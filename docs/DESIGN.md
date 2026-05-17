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
- **Multi-engine cold store is out of scope for v1.** Only the edge profile (embedded analytical engine + local Parquet + on-disk vector index) ships. A server profile (relational TSDB + pgvector) is v2 work and requires a query-layer IR that does not exist yet — see §13.

## 3. Data model

### 3.1 Event record
CloudEvents-compatible, deliberately minimal:

```
Event {
  event_id:    ulid          // canonical id, primary dedupe key, sortable by encoded time
  source_id:   string        // stable instance id, e.g. "cam.front_door"
  kind:        string        // ontology tag; optionally version-pinned, e.g. "object_detected@v2"
  ts_ms_utc:   i64           // event time, not ingest time
  semantic:    json          // free-form payload
  links?:      [Link]        // optional blob refs (image, clip, raw frame)
  trace_id?:   string        // cross-component correlation; attached by adapter if absent
}
Link { rel: string, uri: string, mime?: string, bytes?: i64, sha256?: string }
```

Server-attached fields (stored alongside, not part of the producer contract):
- `ingest_ts_ms_utc`
- `seq` — **per-`(source_id, process)` monotonic** counter; resets on Percept restart. Useful for in-process ordering only; **not** a cross-process dedupe key. Dedupe is on `event_id`.
- `producer_id?` — advisory, not enforced.
- `_schema_invalid?: bool` — set when `semantic` fails validation against the descriptor's `semantic_schema`. See §5.2.

### 3.2 SourceDescriptor (per instance — "this camera, this thermometer")
Self-description for the LLM.

**Lifecycle (v1):** descriptors are **pinned in config only**. Producer-side registration is deferred — see §13.

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

`describe_sources()` attaches a `recent_errors` summary per source at read time (see §7 and §10) — it is not part of the persisted descriptor.

### 3.3 KindDescriptor (per ontology tag — "what `temperature` means here")
Keyed by `(kind, version)`. Default `version = "v1"`.

```
KindDescriptor {
  kind,                      // bare name, e.g. "object_detected"
  version:          string   // "v1", "v2", ...
  description, usage, caveats,
  semantic_schema?, units?,
  updated_ts_ms_utc
}
```

**Schema merge semantics:** if a SourceDescriptor defines `semantic_schema`, it **fully replaces** the KindDescriptor's `semantic_schema` for that source. No per-field merge — JSON Schema has no canonical merge and trying to invent one leaks edge cases.

The LLM sees the **merged view** for non-schema fields (kind defaults + source overrides). Producers don't have to know which is which.

### 3.4 Versioning
- Incompatible payload shape → register a new `(kind, version)`; both versions coexist as separate KindDescriptors.
- Producers can pin a version with `kind = "object_detected@v2"`. An unversioned `kind` resolves to the latest registered version.
- Adding optional fields does **not** require a new version; readers tolerate unknown fields.

## 4. Architecture

```
                       ┌──────────────────────────────────────────────┐
producers ──MQTT──▶    │                                              │
producers ──HTTP──▶    │   Ingest adapters  ─▶  Normalizer  ─▶  Bus   │
producers ──WS────▶    │                                              │
producers ──BLE───▶    └─────┬──────────┬──────────┬─────────────┬────┘
                             │          │          │             │
                       ┌─────▼──────┐ ┌─▼────────┐ ┌▼──────────┐
                       │ Hot rings  │ │  Cold    │ │ Embedder  │
                       │ per        │ │  writer  │ │ → vector  │
                       │(src,kind)  │ │ (batched)│ │   index   │
                       └─────┬──────┘ └─────┬────┘ └─────┬─────┘
                             │              │            │
                             │       ┌──────▼─────┐ ┌────▼──────┐
                             │       │ Event      │ │ Vector    │
                             │       │ store      │ │ index     │
                             │       │ (Parquet)  │ │ (ANN)     │
                             │       └──────┬─────┘ └─────┬─────┘
                             │              │             │
                       ┌─────▼──────────────▼─────────────▼─────┐
                       │              Query layer               │
                       └──────────────────┬─────────────────────┘
                                          │
                                   ┌──────▼──────┐
                                   │ MCP server  │ ◀── LLM client
                                   └─────────────┘
```

- The **bus** is an in-process typed async channel (tokio mpsc). v1 is single-binary; no external bus.
- The hot rings, cold writer, and embedder are **independent consumers** of the bus. Any one can fall behind without blocking ingest.
- The embedder is asynchronous by design — embedding is too expensive to do on the ingest path (see §5.2 and §11.1).

## 5. Ingestion

### 5.1 Adapters
Each adapter is a thin shim that emits canonical `Event`s onto the bus.

- **MQTT subscriber** — topic→`source_id` mapping via config; payload assumed JSON unless `content_type` indicates otherwise.
- **HTTP/gRPC push** — `POST /ingest` for single or batched events; gRPC for high-rate producers.
- **WebSocket** — long-lived producer streams; same shape as HTTP batch.
- **ROS2 / Foxglove bridge** — subscribes to topics, maps msg types to `kind`.
- **BLE scanner** — advertisements become events with `kind=ble.advert`; bonded devices in GATT mode emit richer kinds.
- **Producer SDK** — thin client lib that handles batching, retries (with `Retry-After`), and ingest auth. Same wire format as HTTP.

### 5.2 Normalizer
- Validates required fields; assigns `event_id` (ULID), `ingest_ts_ms_utc`, `seq`.
- Enforces clock sanity (rejects or clamps wildly future `ts_ms_utc`).
- Resolves `kind` version (latest if unversioned, explicit if `kind@vN`).
- Optionally validates `semantic` against the descriptor's `semantic_schema` — soft-fail: the event is stored with `_schema_invalid = true` and a counter is incremented. Soft-fail is the default because schemas are often imperfect and dropping data hides upstream bugs.
- Propagates `trace_id` from the producer; attaches a new one if absent.

**Embedding is not done here.** The embedder is a downstream bus consumer (§6.3). This is the only way the §11.1 throughput target is achievable on edge hardware.

### 5.3 Backpressure
- Adapter → bus is bounded; on overflow, drop with a counter, never block.
- Hot rings are fixed-size per `(source_id, kind)` (drop oldest within a kind, see §6.1).
- Cold writer batches by time + size; lag is observable.
- **Producer-visible shed:** HTTP/gRPC ingest returns `429 Too Many Requests` with a `Retry-After` header and an `X-Percept-Shed-Reason` header (`bus_full`, `rate_limit`, `unauthorized`, `payload_too_large`, `unresolved_kind`) when an event is dropped at the boundary. SDK clients honor `Retry-After`. Per-token shed counters are exported on `/metrics`.

## 6. Storage

### 6.1 Hot path (the "now" answer)
- **Ring buffer per `(source_id, kind)`**, not per source. A high-rate `ble.advert` from a node cannot displace its rare `door_state=open` event — the rings are independent.
- Each ring has configurable depth (last N events or last T seconds).
- Lookup is O(1) per `(source, kind)`; no I/O.
- Survives nothing — restart re-fills from producers. `get_current_state` provides a cold-store fallback for the post-restart gap (§7).

### 6.2 Cold store (v1: edge profile only)
- Embedded analytical engine over local Parquet files. Single binary, no daemon.
- Columnar event log partitioned by day and `source_id`.
- Retention per-source/per-kind policy (§11.3).
- Blob payloads (images, clips) live behind `links` in object storage; Percept stores the reference, not the bytes.

**v2 (not in scope):** a server profile based on a relational TSDB with native time partitioning and a co-located vector extension. Shipping it honestly requires a small query-layer IR so the MCP surface stays engine-agnostic; that IR is open work — see §13.

### 6.3 Vector index
- One embedding per stored event for kinds flagged `searchable: true`.
- **Asynchronous:** an embedder consumer reads from the bus, computes embeddings, and writes to the vector index. It can lag the cold writer without blocking ingest.
- Filtered ANN: `(time_range, source_filter, kind_filter)` ∧ vector similarity.
- Embedding model id is stored with the vector so re-indexes are safe.
- **Retention constraint:** `vector_max_age > raw retention` is a config error rejected at startup — vector entries cannot outlive their source events (would create dangling references).

### 6.4 Descriptors
- Stored in a small KV table; cached in memory.
- Versioned by `updated_ts_ms_utc`. **Point-in-time descriptor history** at query time — see §13.

## 7. Query surface — MCP tools

Deliberately small. The LLM's job is reasoning; ours is to make context easy to fetch.

| Tool | Purpose |
|------|---------|
| `describe_sources(filter?)` | Returns merged Source+Kind descriptors, plus a per-source `recent_errors` summary (drop counters by reason, last-error timestamp). **This is how the LLM learns what's available, what it means, what to watch out for, and which sources are currently misbehaving.** |
| `get_current_state(source_filter?, kind_filter?)` | Latest event per matching `(source_id, kind)` from the hot ring. On hot-ring miss (e.g. shortly after restart), falls back to a `latest_per_(source,kind)` cold scan; those entries are tagged `from_cold = true`. Each entry carries `age_ms = now - ts_ms_utc` and a `stale = (age_ms > freshness_ttl_ms)` flag derived from the descriptor. |
| `get_window(start_ms, end_ms, source_filter?, kind_filter?, limit?)` | Time-range scan from the cold store; paginated. |
| `search_events(query, time_range?, source_filter?, kind_filter?, limit?)` | Semantic search via vector index, with structured filters. |

Design rules:
- Every tool returns canonical `Event` records (including `event_id`) plus a `cursor` if truncated.
- **Cursor format** is opaque to the LLM. Internally `{partition_key, offset}`; the API treats it as a string and validates that it matches the originating query's filter set.
- Filters are uniform across tools.
- Time is always UTC ms. No timezone math in the tool surface.
- Tool responses include the descriptor snippet for each `source_id` they return.
- **Aggregations** (count, avg, max-over-time) are intentionally pushed to the LLM. Tools return events; the LLM does the math. Keeps the surface small and explicit.

## 8. Deployment topologies

All topologies run the same single binary; no external bus.

1. **Single-box edge** — one Percept process; producers reach it over LAN/BLE/wired. Cold store on local disk.
2. **Hub-and-spoke** — edge Percepts act as ingesters and forward to a central Percept over the producer SDK. **`source_id` rewrite on egress is mandatory:** the forwarder prefixes `<peer_id>.` to every outgoing `source_id` (and to descriptor entries). The hub sees `kitchen.temp.fridge`, never bare `temp.fridge`, so two edges cannot collide. Local hot rings still answer "now" if WAN is down.
3. **Federated multi-site** — peer Percepts; `describe_sources()` aggregates across peers; queries fan out with a per-peer timeout. Responses include a per-peer status (`ok` / `timeout` / `error`) so the LLM can reason about partial coverage. No global write coordination.

## 9. Security & auth (v1 scope)
- Bearer token on the MCP endpoint and on `/ingest`.
- TLS terminated at Percept (or a sidecar).
- One trust domain per instance; producer identity (`producer_id`) is advisory, not enforced.
- **Token rotation:** the config supports multiple active tokens per scope. Operational pattern is roll-over-then-remove. v1 requires a restart to remove a token; v2 hot-reload will lift that.
- Multi-tenant ACLs, signed events, and per-source scopes are explicit non-goals for v1.

## 10. Observability
- Per-source: events/sec, last-seen-age, drop count by reason, schema-validation-fail count. `describe_sources()` surfaces a digest of these as `recent_errors`.
- Per-consumer (cold writer, embedder): lag, batch latency.
- Per-token (HTTP ingest): accepted, shed, 429 count.
- MCP: tool call counts, latencies, result sizes.
- **Tracing:** `trace_id` propagates through the canonical Event from adapter → bus → cold writer → MCP response. Adapters attach a fresh `trace_id` when the producer didn't supply one. Full OpenTelemetry integration is deferred; the field is the substrate.
- `/healthz` endpoint and a `/metrics` Prometheus surface.

## 11. Performance targets & limits

### 11.1 Latency
Targets are end-to-end (producer emit → result visible to LLM), p99, on edge-profile hardware (Pi 5 class) under nominal load, **with embedding disabled by default**. Enabling embedding on a `kind` adds load proportional to that kind's event rate (FastEmbed-rs single-thread is roughly 10–50 ms/event on Pi-5 class CPUs); see §5.2 / §6.3.

| Path | Target (p99) | Notes |
|------|--------------|-------|
| Ingest → visible in `get_current_state` | **≤ 100 ms** | hot ring is in-memory; dominated by transport. |
| Ingest → visible in `get_window` | **≤ 5 s** | cold writer batches by time/size. |
| Ingest → visible in `search_events` | **≤ 10 s** | embedder + vector index update lag the cold writer. |
| `get_current_state` call | **≤ 50 ms** | memory lookup; cold-fallback path on miss can be slower (one cold partition scan per missing source/kind). |
| `get_window` call (≤ 10k events returned) | **≤ 500 ms** | filtered scan over Parquet. |
| `search_events` call (top-k ≤ 50) | **≤ 300 ms** | filtered ANN over LanceDB ≤ 1M-vector index. Larger indices require re-tuning. |
| `describe_sources` call | **≤ 50 ms** | cached; `recent_errors` digest comes from in-memory counters. |

Sustained ingest rate, edge profile (Pi 5 class): target **≥ 1k events/sec aggregate** across all sources without dropping, with embedding off. With embedding on a high-rate kind, throughput is bounded by the embedder's per-event cost, not the bus.

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
  vector_max_age?: duration   // embeddings can underlive raw events; cannot outlive (§6.3)
}
```
Resolution order: source-level policy → kind-level policy → global default. First match wins per dimension.

Suggested defaults (configurable, not hardcoded):

| Class | Example kinds | Default retention |
|-------|---------------|-------------------|
| Low-rate, high-value | `object_detected`, `person_present`, `alert` | 30 days |
| Mid-rate observations | `temperature`, `humidity`, `door_state` | 14 days |
| High-rate / noisy | `ble.advert`, `frame_seen` | 24 hours |
| Embeddings | (any searchable kind) | match raw, configurable shorter |

Mechanics:
- Sweeper runs on a configurable cadence (default 1 h).
- **`max_age` is cheap** — implemented as whole day-partition drops. No rewrites.
- **`max_count` and `max_bytes` are best-effort and expensive** — they require in-partition delete-and-rewrite on Parquet. Avoid them on high-rate sources; prefer `max_age`. The sweeper logs a warning when a `max_count` / `max_bytes` policy is bound to a source whose rate makes per-partition rewrites unavoidable.
- Blob lifecycle is **not** Percept's responsibility — the producer (or a separate GC) owns it; we only drop the `links` reference.
- Retention changes are non-destructive until the next sweeper pass, so config mistakes have a recovery window.
- `describe_sources()` surfaces the effective policy per source so the LLM (and operators) know how far back it can ask.
- No event-level "pinned" / "do-not-evict" flag in v1 — retention is purely policy-driven. Producers can re-emit important events under a higher-retention `kind`.

## 12. Configuration

### 12.1 Format and layering
- **Format:** TOML.
- **Layering:** single `percept.toml` for simple deployments; `conf.d/*.toml` files are merged (later files win) for larger ones.
- **Secrets:** referenced via `*_env` or `*_file`. Inline credentials are a config error.
- **Validation:** `percept config check` validates without starting the server; runtime startup also fails fast with line numbers.
- **Reload:** v1 is restart-only. No hot reload.

### 12.2 Top-level layout
```toml
[server]                # data_dir, profile
[mcp]                   # listener + auth
[storage]               # sweeper cadence, embedding defaults
[[retention]]           # repeatable, matched by source_id / kind

[[mqtt]]                # one block per broker
[[mqtt.subscription]]   # nested

[[ble]]                 # one block per HCI adapter
[[ble.device]]          # known / bonded device

[[http_token]]          # per-token ingest scopes

[[ros2]]                # ROS2 bridge

[[source]]              # SourceDescriptor (pinned)
[[kind]]                # KindDescriptor (pinned)
```

### 12.3 Server, MCP, storage
```toml
[server]
data_dir = "/var/lib/percept"
profile  = "edge"                       # v1: only "edge" is accepted

[mcp]
listen    = "0.0.0.0:7878"
transport = "http-streamable"           # MCP Streamable HTTP (default). "sse" supported as a legacy fallback.
auth      = { token_env = "PERCEPT_MCP_TOKEN" }

[storage]
sweeper_interval = "1h"
embed_default    = false                # opt-in per kind/source

[[retention]]
match.kind = "ble.advert"
max_age    = "24h"

[[retention]]
match.source_id = "cam.front_door"
max_age         = "30d"
```

### 12.4 MQTT
Multiple brokers; each has nested subscriptions.

```toml
[[mqtt]]
id        = "house-broker"
url       = "mqtts://broker.lan:8883"
client_id = "percept-1"
credentials = { user = "percept", password_env = "MQTT_PASS" }
tls         = { ca_file = "/etc/percept/ca.pem", insecure = false }

[[mqtt.subscription]]
topic              = "home/+/temp"
source_id_template = "temp.{+1}"            # {+1}, {+2}, ... = ordered `+` captures
kind               = "temperature"          # static OR omit and use kind_field
payload            = "json"                 # "json" | "raw" | "hex" | "csv"
qos                = 1

[[mqtt.subscription]]
topic              = "cams/+/events"
source_id_template = "cam.{+1}"
kind_field         = "$.event_type"         # JSONPath into the decoded payload
payload            = "json"
```

Rules:
- **Topic captures** use template syntax only: `{+1}`, `{+2}`, ... for `+` wildcards in order; `{#}` for the `#` tail. No regex.
- **`kind` resolution order:** explicit `kind` > `kind_field` JSONPath > descriptor default. If none resolves, event is dropped + counted.
- **JSONPath dialect:** **RFC 9535**. Goessner-style expressions that don't conform are rejected at config-check time.
- **Built-in decoders (v1):** `json`, `raw`, `hex`, `csv`. Plugin surface is deferred.

### 12.5 BLE
Two modes.

**Scan (passive)** — adverts become events. No pairing.
```toml
[[ble]]
adapter    = "hci0"
mode       = "scan"
duplicates = true

[[ble.device]]
mac        = "AA:BB:CC:DD:EE:FF"
source_id  = "weight.bathroom"
kind       = "weight"
decoder    = "miscale-v2"
```
Unknown MACs land under `source_id=ble.<mac>`, `kind=ble.advert`.

**GATT (bonded)** — connect to already-bonded devices and subscribe to characteristics.
```toml
[[ble]]
adapter = "hci0"
mode    = "gatt"

[[ble.device]]
mac          = "C0:FF:EE:00:11:22"
source_id    = "hrm.chest"
kind         = "heart_rate"
require_bond = true
poll         = "notify"

[[ble.device.gatt]]
service_uuid = "0000180d-0000-1000-8000-00805f9b34fb"
char_uuid    = "00002a37-0000-1000-8000-00805f9b34fb"
decoder      = "ble-hrm-standard"
```
**Pairing is out of scope for Percept.** Bond the device via the OS (`bluetoothctl` on Linux/BlueZ); in GATT mode with `require_bond = true`, Percept refuses to connect to an unbonded device. A helper command `percept ble pair <mac>` is a thin wrapper around the OS pairing tool and writes nothing to Percept's own state.

### 12.6 HTTP / WebSocket ingest
Single listener. Per-token scopes gate which `source_id`s and `kind`s a producer may write. Default-deny — a token with no allowlist writes nothing.

```toml
[[http_token]]
name             = "front-door-cam"
token_env        = "PERCEPT_TOKEN_FRONT_DOOR"
allow_source_ids = ["cam.front_door", "cam.front_door.*"]
allow_kinds      = ["object_detected", "scene_description"]
rate_limit       = "100/s"
```

- `allow_source_ids` and `allow_kinds` use **shell-style `*` globs**. (Different from MQTT topic capture syntax `{+N}` deliberately — captures and matches are different problems: one extracts substrings, one tests membership.)
- On rate-limit or denial, ingest returns `429` with `Retry-After` and `X-Percept-Shed-Reason`. Per-token counters on `/metrics`.
- Tokens **cannot push SourceDescriptors** in v1 — descriptor pinning is config-only. (A future opt-in flag will unlock producer-side descriptor registration; see §13.)

### 12.7 ROS2 / Foxglove
```toml
[[ros2]]
node_name = "percept_bridge"
domain_id = 42

[[ros2.subscription]]
topic              = "/camera/detections"
msg_type           = "vision_msgs/msg/Detection2DArray"
source_id_template = "ros.{topic_basename}"
kind               = "object_detected"
```

### 12.8 Descriptor pinning
Pinned descriptors live in config and are the only source in v1. Producer-pushed descriptors are deferred (see §12.6 and §13).

```toml
[[kind]]
name         = "temperature"
version      = "v1"
units        = "celsius"
description  = "Ambient temperature reading."
usage        = "Use for 'is it cold/hot' questions; cross-check with humidity if present."
caveats      = "Some sources report Fahrenheit despite this kind; check the source override."

[[source]]
id          = "cam.front_door"
kinds       = ["object_detected", "scene_description"]
description = "Reolink camera mounted above front porch, 1080p, ~110° FOV."
usage       = "Use for 'who/what is at the front door' and recent activity."
caveats     = "Heavy false positives at dusk; rain drops trigger 'person' occasionally."
location    = "front porch"
freshness_ttl_ms = 60000
```

### 12.9 Failure modes
- **Decoding error / unresolved `kind` / unauthorized token / payload-too-large:** event dropped, counter incremented (per source, per reason). HTTP/gRPC producers receive `429` with `X-Percept-Shed-Reason`. No quarantine table in v1.
- **Schema validation against `semantic_schema`:** soft-fail — event is stored with `_schema_invalid = true` so the LLM/operator can still see what arrived. (See §5.2.)
- **All failure counters** are exposed via `/metrics` and surfaced in `describe_sources()` as a `recent_errors` digest.

## 13. Open questions / decisions deferred
- **Embedding policy:** which kinds embed by default; truncation rules for large payloads.
- **Replication semantics** for hub-and-spoke: at-least-once with idempotent `event_id` dedupe is the working assumption.
- **Descriptor history:** do `get_window` results carry the descriptor that was live at event time, or the current one? v1: current; v2: optional point-in-time.
- **Blob lifecycle:** Percept manages references only; GC of orphaned blobs is the producer's problem unless we add a sweeper.
- **Producer-side descriptor registration:** deferred from v1 (see §3.2 / §12.6). A future opt-in token flag will let producers push their own SourceDescriptors.
- **Server-profile cold store:** requires a query-layer IR to abstract the MCP surface over the edge engine and a TSDB+pgvector backend. Not in v1; the IR design is open work.

---

## Appendix A: Reference implementation stack (v1, edge profile)
- **Runtime:** Rust + tokio
- **MQTT:** `rumqttc`
- **HTTP ingest + MCP transport:** `axum`, Streamable HTTP (SSE fallback)
- **Cold store:** DuckDB + Parquet (v1 ships with bundled SQLite via
  `rusqlite` — DuckDB-bundled was untenable for CI disk budget and
  compile time; SQLite meets the slice-3 acceptance target. Switch
  remains a v2 perf concern, see `PLAN.md` slice 3.)
- **Vector index:** LanceDB
- **Embeddings:** FastEmbed-rs (local, swappable)
- **MCP server:** `rmcp`
- **Result:** one ~15 MB static binary, **edge profile only**. The "single binary" claim applies to v1; a v2 server profile would add an external TSDB dependency.

## Appendix B: Pi 5 + OpenClaw demo
Reference deployment used to validate the design end-to-end.

- **Hardware:** Pi 5 (+ optional Hailo AI HAT+ for local vision).
- **Co-located processes:** Mosquitto (MQTT broker), Percept (Rust binary), OpenClaw (Node.js agent gateway).
- **LLM:** Claude / GPT via API — local LLMs are too slow on the Pi (per OpenClaw's own guidance).
- **Wiring:** OpenClaw discovers Percept via its `mcpServers` config (Streamable HTTP transport):
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
- **Gotcha noted:** OpenClaw's MCP bridge strips `NODE_OPTIONS`, `PYTHONSTARTUP`, etc. on stdio servers. Percept uses Streamable HTTP, so this doesn't apply.
- **Why this demo:** exercises all four MCP tools, three ingest paths (MQTT, BLE, HTTP), and both hot and cold queries on hardware representative of the target edge tier.
