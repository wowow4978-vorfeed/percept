# Percept — Implementation Decisions (Addendum to DESIGN.md)

**Status:** proposed, awaiting review. Each item is the concrete pick I'd make
to start implementation. Reject any with a one-liner and I'll revise before
writing code. Section numbers reference DESIGN.md.

## 1. Workspace layout
Four crates in a Cargo workspace:

| Crate | Responsibility | Depends on |
|---|---|---|
| `percept-core` | `Event`, `Link`, `SourceDescriptor`, `KindDescriptor`, error/time/trace types. No tokio, no I/O. | — |
| `percept-store` | Hot rings, cold writer (DuckDB + Parquet), vector index (LanceDB), embedder (FastEmbed-rs). Defines the bus type alias. | `percept-core` |
| `percept-ingest` | Adapters (MQTT, HTTP/WS, ROS2 stub, BLE scan/GATT), normalizer, per-token authn/scope checks. | `percept-core` |
| `percept` | Binary. Config loader, wiring, `rmcp` MCP server, `/healthz`, `/metrics`, CLI. | all three |

Rationale: smallest split that keeps `percept-core` dependency-light (useful
for tests and possible future bindings) without over-fragmenting.

## 2. Embedding defaults (closes part of §13)
- `embed_default = false` globally. Opt in per kind (`[[kind]] embed = true`)
  or per source (`[[source]] embed = true`); source override wins.
- **Truncation rule:** serialize `semantic` to compact JSON, truncate to
  **2048 bytes** on a UTF-8 boundary before embedding. Vectors carry a
  `truncated: bool` flag; `search_events` surfaces it in the result so the
  LLM knows the match was on a prefix.
- **Default model:** `bge-small-en-v1.5` via FastEmbed-rs. The model id is
  stored on each vector row so re-index is safe (§6.3).

## 3. Cursor encoding (§7)
- Internal payload: `{ partition_key: String, anchor: (ts_ms_utc, event_id), filter_hash: [u8; 16] }`.
- `filter_hash = BLAKE3-128(canonical_filters)` — keyed inputs sorted; UTC ms
  for time ranges.
- Wire format: base64url(CBOR(payload)). Opaque to the LLM.
- Validation on resume: recompute `filter_hash` from the new request;
  mismatch → `cursor_filter_mismatch` error. No server secret needed —
  tampering only lets a caller scan a partition they could have requested
  directly.

## 4. `get_current_state` cold fallback (§7)
- Maintain a **`latest` table** in DuckDB keyed by `(source_id, kind)`,
  updated by the cold writer at each batch commit (single upsert per
  `(source, kind)` in the batch, amortized).
- Hot-ring miss → point-lookup on `latest` → result tagged `from_cold = true`.
- `latest` is a derived index: rebuildable from the event log on demand;
  not part of the durability contract.

## 5. Hot-ring sizing defaults
- Default per `(source_id, kind)`: `max(256 events, 60 s)` — whichever cap
  hits first, the older end is dropped.
- Overridable: `[[kind]] hot_ring = { max_events = N, max_age = "Ts" }`,
  with per-source override on top.
- Worst-case footprint: 256 × 64 KB ≈ 16 MB per ring. Documented so operators
  sizing for thousands of rings see the upper bound; defaults are well below
  that in practice.

## 6. Bus channel sizing & overflow
- One `tokio::sync::mpsc` per consumer (cold writer, embedder, hot fan-out),
  fed by a fan-out task that drains the normalizer's output and writes to each
  consumer's channel.
- Default depth per consumer: **4096 events**. Overflow: drop with per-consumer
  counter (`bus_drop_total{consumer=...}`), never block the normalizer.
- Hot-ring fan-out runs **inline** in the fan-out task (in-memory ring push is
  O(1)); it cannot lag, so it doesn't need a queue.

## 7. `seq` and normalizer concurrency
- **One normalizer task per Percept process** in v1. Adapters push raw
  `IngestEnvelope`s onto a single MPSC; the normalizer drains, validates,
  assigns `event_id`/`ingest_ts_ms_utc`/`seq`, and emits canonical Events.
- This makes `seq` per-`(source_id, process)` monotonic with no atomics and
  satisfies §3.1 trivially.
- If profiling later shows the normalizer is the bottleneck, shard it by
  `hash(source_id) mod N`. Each shard keeps its own counter; the invariant
  still holds because a given `source_id` lives on exactly one shard.

## 8. MCP transport
- Default: **Streamable HTTP** at `POST /mcp`, served by `rmcp` mounted on
  the axum router that also exposes `/healthz` and `/metrics`.
- `transport = "sse"` switches to the legacy SSE endpoint at `GET /sse` for
  the whole `[mcp]` block. No mixed-mode; operator picks one.

## 9. `get_window` ordering & pagination
- **Order:** `(ts_ms_utc ASC, event_id ASC)`. `event_id` is a ULID, so the
  tiebreaker embeds time and is deterministic.
- Cursor anchor = the `(ts, event_id)` of the last returned row; resume
  scans `WHERE (ts, event_id) > anchor` within the cursor's `partition_key`.
- Per-call hard limit: `min(requested_limit, 10_000)`. Above the §11.1 target
  so paginated callers can hit the perf number; below memory-blowout range.

## 10. CLI surface (v1)
```
percept serve [--config <path>]              # default; merges <path> + <path>.d/*.toml
percept config check [--config <path>]       # validate, exit nonzero on error
percept retention dry-run [--config <path>]  # show what the next sweeper pass would drop
percept ble pair <mac>                       # thin wrapper over bluetoothctl
percept version                              # build + git sha
```
No `migrate` in v1: Parquet partitions are self-describing; the `latest`
table is created on first run.

## 11. First implementation slice
On go-ahead I'd land, in one branch:
1. Workspace `Cargo.toml` + four crate manifests.
2. `percept-core` with `Event`, `Link`, `SourceDescriptor`, `KindDescriptor`,
   ULID helpers, plus unit tests for serialization round-trips.
3. `percept` binary that loads `percept.toml` + `conf.d/*.toml`, resolves the
   descriptor map, and prints it. `percept config check` works.

No adapters, no storage, no MCP yet — those land in subsequent slices, each
small enough to review independently. Suggested next slices, in order:
HTTP `/ingest` → normalizer → bus → hot rings → MCP `get_current_state`
(end-to-end thin slice); then cold writer + `get_window`; then embedder +
`search_events`; then MQTT and BLE adapters; finally hub-and-spoke
forwarding.

## 12. Still deferred (explicitly not blocking the first slice)
- Hub-and-spoke forwarder behavior beyond the `source_id` prefix rule (§8).
- ROS2 adapter wiring (`ros2_rust` setup is out of scope for the scaffold).
- Descriptor history (§13) — DESIGN already picks "current-only for v1".
- Producer-side descriptor registration (§13).
- Server-profile cold store / query IR (§13).
