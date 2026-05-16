# Percept — High-Level Design

## 1. Goals
- Aggregate heterogeneous sensor streams (MQTT, HTTP/gRPC push, WebSocket, ROS2/Foxglove, BLE, wired) into one uniform store.
- Serve two question shapes equally well: **"what is true now?"** and **"what happened in [time range]?"**
- Expose the store to LLMs through a small, stable **MCP tool surface** — no bespoke per-deployment glue.
- Be **sensor-agnostic**: the `semantic` payload is free JSON; ontology lives in descriptors, not code.
- Be **locally deployable** on a single small box, and **scale-out** to hub-and-spoke or federated topologies without changing the data model.

## 2. Non-goals
- Not a general-purpose TSDB or analytics warehouse. No SQL surface for end users.
- Not a perception/inference engine. Producers do their own ML; Percept stores results.
- Not a device-management plane (provisioning, OTA, calibration).
- No multi-tenant ACLs in v1 — single trust domain per instance.
- No built-in LLM. The LLM is a client; Percept only serves grounded context to it.

## 3. Data model

### 3.1 Event record (the only thing on the hot path)
CloudEvents-compatible, deliberately minimal:

```
Event {
  source_id:   string        // stable instance id, e.g. "cam.front_door"
  kind:        string        // ontology tag, e.g. "object_detected", "temperature"
  ts_ms_utc:   i64           // event time, not ingest time
  semantic:    json          // free-form payload
  links?:      [Link]        // optional blob refs (image, clip, raw frame)
}
Link { rel: string, uri: string, mime?: string, bytes?: i64 }
```

Ingest-time fields (`ingest_ts_ms_utc`, `seq`, optional `producer_id`) are attached by Percept and stored alongside but not part of the producer contract.

### 3.2 SourceDescriptor (per instance — "this camera, this thermometer")
Self-description for the LLM. Registered/updated by the producer or pinned in config.

```
SourceDescriptor {
  source_id,
  kinds:            [string]
  description:      string   // what this sensor observes
  usage:            string   // how an LLM should interpret/query it
  caveats:          string   // gotchas: mounting, drift, false-positive modes, missing-data semantics
  semantic_schema?: json     // JSON Schema for `semantic` (optional, per-kind map allowed)
  units?, sampling_hint_ms?, freshness_ttl_ms?
  location?:        string   // human label, e.g. "kitchen"
  updated_ts_ms_utc
}
```

### 3.3 KindDescriptor (per ontology tag — "what `temperature` means here")
Shared semantics across sources. Source-level fields override kind-level fields.

```
KindDescriptor {
  kind,
  description, usage, caveats,
  semantic_schema?, units?,
  updated_ts_ms_utc
}
```

The LLM always sees the **merged view** (kind defaults + source overrides). Producers don't have to know which is which.

### 3.4 Versioning
- `kind` strings are versioned by convention (`object_detected.v1`) when payload shape changes incompatibly. Descriptors carry `semantic_schema`; readers tolerate unknown fields.

## 4. Architecture

```
                       ┌────────────────────────────────────────────┐
producers ──MQTT──▶    │                                            │
producers ──HTTP──▶    │  Ingest adapters  ─▶  Normalizer  ─▶  Bus  │
producers ──WS────▶    │                                            │
producers ──BLE───▶    └──────────────┬──────────────┬──────────────┘
                                      │              │
                              ┌───────▼──────┐  ┌────▼─────────┐
                              │ Hot ring buf │  │ Cold writer  │
                              │ (per source) │  │  (batched)   │
                              └───────┬──────┘  └────┬───────┬─┘
                                      │              │       │
                                      │      ┌───────▼──┐ ┌──▼────────┐
                                      │      │ Event    │ │ Vector    │
                                      │      │ store    │ │ index     │
                                      │      │ (cols)   │ │ (embeds)  │
                                      │      └─────┬────┘ └──┬────────┘
                                      │            │         │
                              ┌───────▼────────────▼─────────▼─────────┐
                              │              Query layer               │
                              └──────────────────┬─────────────────────┘
                                                 │
                                          ┌──────▼──────┐
                                          │ MCP server  │ ◀── LLM client
                                          └─────────────┘
```

Internal bus is a typed async channel (single-process) or NATS/Redis Streams (multi-process). The cold writer and the vector indexer are independent consumers, so either can fall behind without blocking ingest.

## 5. Ingestion

### 5.1 Adapters
Each adapter is a thin shim that emits canonical `Event`s onto the bus.

- **MQTT subscriber** — topic→`source_id` mapping via config; payload assumed JSON unless `content_type` indicates otherwise.
- **HTTP/gRPC push** — `POST /ingest` for single or batched events; gRPC for high-rate producers.
- **WebSocket** — long-lived producer streams; same shape as HTTP batch.
- **ROS2 / Foxglove bridge** — subscribes to topics, maps msg types to `kind`.
- **BLE scanner** — advertisements become events with `kind=ble.advert`; explicit pairings get richer kinds.
- **Producer SDK** — thin client lib that handles batching, retries, descriptor registration. Same wire format as HTTP.

### 5.2 Normalizer
- Validates required fields, assigns `ingest_ts_ms_utc` / `seq`.
- Enforces clock sanity (rejects or clamps wildly future `ts_ms_utc`).
- Optionally validates `semantic` against the descriptor's `semantic_schema` — soft-fail (tag and store, don't drop) by default.
- Computes an embedding for searchable kinds (configurable per kind/source).

### 5.3 Backpressure
- Adapter → bus is bounded; on overflow, drop with a counter, never block.
- Hot ring is fixed-size per source (drop oldest).
- Cold writer batches by time + size; lag is observable.

## 6. Storage

### 6.1 Hot path (the "now" answer)
- Per-`source_id` **ring buffer** in memory (configurable depth, e.g. last N events or last T seconds).
- Lookup is O(1) per source; no I/O.
- Survives nothing — restart re-fills from producers.

### 6.2 Cold store (the "what happened" answer)
- Columnar event log partitioned by day and `source_id`.
- Two profiles, same schema:
  - **Edge profile:** embedded analytical engine over local Parquet files. Single binary, no daemon.
  - **Server profile:** a relational TSDB with native time partitioning and vector extension co-located.
- Retention is per-source/per-kind policy (count, age, or size).
- Blob payloads (images, clips) live behind `links` in object storage; Percept stores the reference, not the bytes.

### 6.3 Vector index
- One embedding per stored event for kinds flagged `searchable: true`.
- Index supports filtered ANN: `(time_range, source_filter, kind_filter)` ∧ vector similarity.
- Embedding model is **local by default**, swappable; model id is stored with the vector so re-indexes are safe.

### 6.4 Descriptors
- Stored in a small KV table; cached in memory.
- Versioned by `updated_ts_ms_utc`; historical queries get the descriptor that was current at event time (optional v2).

## 7. Query surface — MCP tools

Deliberately small. The LLM's job is reasoning; ours is to make context easy to fetch.

| Tool | Purpose |
|------|---------|
| `describe_sources(filter?)` | Returns merged Source+Kind descriptors. **This is how the LLM learns what's available, what it means, and what to watch out for.** |
| `get_current_state(source_filter?, kind_filter?)` | Latest event per matching source from the hot ring. |
| `get_window(start_ms, end_ms, source_filter?, kind_filter?, limit?)` | Time-range scan from the cold store; paginated. |
| `search_events(query, time_range?, source_filter?, kind_filter?, limit?)` | Semantic search via vector index, with structured filters. |

Design rules:
- Every tool returns canonical `Event` records plus a `cursor` if truncated.
- Filters are uniform across tools.
- Time is always UTC ms. No timezone math in the tool surface.
- Tool responses include the descriptor snippet for each `source_id` they return (cheap, makes single-turn answers possible).

## 8. Deployment topologies

All three use the same binary and the same schema.

1. **Single-box edge** — one Percept process; producers reach it over LAN/BLE/wired. Cold store on local disk.
2. **Hub-and-spoke** — edge Percept instances act as ingesters and forward (replicate) to a central Percept over the producer SDK. Local hot ring still answers "now" if WAN is down.
3. **Federated multi-site** — peer Percept instances; `describe_sources()` aggregates across peers; queries fan out with a per-peer timeout. No global write coordination.

## 9. Security & auth (v1 scope)
- Bearer token on the MCP endpoint and on `/ingest`.
- TLS terminated at Percept (or a sidecar).
- One trust domain per instance; producer identity is advisory (`producer_id` field), not enforced.
- Multi-tenant ACLs, signed events, and per-source scopes are explicit non-goals for v1.

## 10. Observability
- Per-source: events/sec, last-seen-age, drop count, schema-validation-fail count.
- Per-consumer (cold writer, vector indexer): lag, batch latency.
- MCP: tool call counts, latencies, result sizes.
- A `/healthz` endpoint and a `/metrics` Prometheus surface.

## 11. Performance targets & limits

### 11.1 Latency
Targets are end-to-end (producer emit → result visible to LLM), p99, on edge-profile hardware (Pi 5 class) under nominal load.

| Path | Target (p99) | Notes |
|------|--------------|-------|
| Ingest → visible in `get_current_state` | **≤ 100 ms** | hot ring is in-memory; this is dominated by transport. |
| Ingest → visible in `get_window` | **≤ 5 s** | cold writer batches by time/size; this is the batch-flush bound. |
| Ingest → visible in `search_events` | **≤ 10 s** | embedding + vector index update can lag the cold writer. |
| `get_current_state` call | **≤ 50 ms** | memory lookup. |
| `get_window` call (≤ 10k events returned) | **≤ 500 ms** | filtered scan over Parquet / TSDB. |
| `search_events` call (top-k ≤ 50) | **≤ 300 ms** | filtered ANN. |
| `describe_sources` call | **≤ 50 ms** | cached. |

Sustained ingest rate, edge profile (Pi 5 class): target **≥ 1k events/sec aggregate** across all sources without dropping. Server profile: target **≥ 20k events/sec**, bounded by the TSDB.

### 11.2 Event size
Bytes refer to the serialized `semantic` JSON. Blob payloads (images, clips, raw frames) **must** go to object storage and be referenced via `links` — never inlined.

| Tier | Size | Behavior |
|------|------|----------|
| Typical | ≤ 2 KB | scalars, small object lists, short transcripts, BLE adverts |
| Soft cap | 16 KB | accepted, counter incremented, debug-logged |
| Hard cap | **64 KB** | rejected with `payload_too_large`; producer must move bulk to `links` |

Rationale for 64 KB: an LLM call typically wants tens of recent events in its context. At 64 KB max each, 50 events ≈ 3 MB raw — fits in modern long-context windows after JSON compaction, with headroom for descriptors and reasoning. Anything bigger is almost certainly a blob that the LLM doesn't want to read inline anyway — it wants a summary plus a link.

The hard cap is **configurable**, but raising it is discouraged; the right fix is `links`.

### 11.3 Retention
Fully configurable. Policies are evaluated by a background sweeper.

Policy model, composable per (source_id ∪ kind):
```
RetentionPolicy {
  max_age?:        duration   // e.g. "30d"
  max_count?:      i64        // keep last N per source
  max_bytes?:      i64        // cap on-disk footprint per source
  vector_max_age?: duration   // embeddings can outlive or underlive raw events
}
```
Resolution order: source-level policy → kind-level policy → global default. First match wins per dimension.

Suggested defaults (configurable, not hardcoded):

| Class | Example kinds | Default retention |
|-------|---------------|-------------------|
| Low-rate, high-value | `object_detected`, `person_present`, `alert` | 30 days |
| Mid-rate observations | `temperature`, `humidity`, `door_state` | 14 days |
| High-rate / noisy | `ble.advert`, `frame_seen` | 24 hours |
| Embeddings | (any searchable kind) | match raw, configurable longer |

Mechanics:
- Sweeper runs on a configurable cadence (default 1 h); drops whole day-partitions where possible to avoid rewrites.
- Blob lifecycle is **not** Percept's responsibility — the producer (or a separate GC) owns it; we only drop the `links` reference.
- Retention changes are non-destructive until the next sweeper pass, so config mistakes have a recovery window.
- `describe_sources()` surfaces the effective policy per source so the LLM (and operators) know how far back it can ask.
- No event-level "pinned" / "do-not-evict" flag in v1 — retention is purely policy-driven. Producers can re-emit important events under a higher-retention `kind`.

## 12. Open questions / decisions deferred
- **Descriptor lifecycle:** producer-pushed vs. config-pinned vs. both. Leaning both, with config winning on conflict.
- **Schema enforcement:** soft-fail vs. quarantine queue for invalid `semantic` payloads.
- **Embedding policy:** which kinds embed by default; truncation rules for large payloads.
- **Replication semantics** for hub-and-spoke: at-least-once with idempotent `(source_id, ts_ms_utc, seq)` dedupe is the working assumption.
- **Descriptor history:** do `get_window` results carry the descriptor that was live at event time, or the current one? v1: current; v2: optional point-in-time.
- **Blob lifecycle:** Percept manages references only; GC of orphaned blobs is the producer's problem unless we add a sweeper.

---

## Appendix A: Reference implementation stack
- **Runtime:** Rust + tokio
- **MQTT:** `rumqttc`
- **HTTP ingest + MCP transport:** `axum`
- **Cold store (edge):** DuckDB + Parquet
- **Cold store (server):** TimescaleDB + pgvector
- **Vector index (edge):** LanceDB
- **Embeddings:** FastEmbed-rs (local, swappable)
- **MCP server:** `rmcp` (HTTP/SSE)
- **Result:** one ~15 MB static binary; same binary in all three topologies.

## Appendix B: Pi 5 + OpenClaw demo
Reference deployment used to validate the design end-to-end.

- **Hardware:** Pi 5 (+ optional Hailo AI HAT+ for local vision).
- **Co-located processes:** Mosquitto (MQTT broker), Percept (Rust binary), OpenClaw (Node.js agent gateway).
- **LLM:** Claude / GPT via API — local LLMs are too slow on the Pi (per OpenClaw's own guidance).
- **Wiring:** OpenClaw discovers Percept via its `mcpServers` config:
  ```json
  {
    "mcpServers": {
      "percept": {
        "url": "http://127.0.0.1:7878/mcp",
        "headers": { "Authorization": "Bearer <local-token>" }
      }
    }
  }
  ```
- **Demo flow:** user (Telegram / web) → OpenClaw → Claude API → MCP tool call → Percept → cold/hot/vector → events back → Claude grounds its answer.
- **Gotcha noted:** OpenClaw's MCP bridge strips `NODE_OPTIONS`, `PYTHONSTARTUP`, etc. on stdio servers. Percept uses HTTP/SSE, so this doesn't apply.
- **Why this demo:** exercises all four MCP tools, three ingest paths (MQTT, BLE, HTTP), and both hot and cold queries on hardware representative of the target edge tier.
