# Percept — Implementation Plan

**Status:** living tracker. Updated each time a slice lands or scope shifts.
Companion to `DESIGN.md`, `DECISIONS.md`, `CI.md`.

## How this works

Implementation lands as a sequence of small, independently reviewable slices.
Each slice has a **goal**, an explicit **in/out scope**, and **acceptance
criteria**. A slice is "done" when:

- Its acceptance criteria are met
- CI is green (fmt, clippy, test, check-arm, deny)
- The slice's PR is merged to `main`

Cross-cutting concerns (observability, error handling, docs) are woven into
each slice — not deferred to a hypothetical cleanup slice.

## Slice status legend
- ☐ not started
- ◐ in progress
- ☑ landed in `main`

## Integration-test coverage per slice

Every slice has at least one integration test that exercises its
acceptance path end-to-end, in addition to per-module unit tests:

| Slice | Integration test |
|---|---|
| 0 — scaffold | `crates/percept/tests/config_load.rs` |
| 1 — ingest pipeline | `crates/percept-ingest/tests/http_e2e.rs` |
| 2 — MCP server | `crates/percept/tests/mcp_e2e.rs` (initialize, tools/list, describe_sources, get_current_state) |
| 3 — cold store + get_window | `crates/percept/tests/mcp_e2e.rs` (get_window section) |
| 4 — vector search | `crates/percept/tests/mcp_e2e.rs` (search_events section) |
| 5 — retention sweeper | `crates/percept/tests/retention_e2e.rs` — drives the live background `Sweeper` task |
| 6 — MQTT + WebSocket | `crates/percept-ingest/tests/ws_e2e.rs` (live WebSocket); MQTT live broker test pulled (see slice 6) — unit tests in `mqtt::{decode,topic,router,subscriber}` cover the routing logic |
| 7 — producer SDK | `crates/percept-client/tests/round_trip.rs` |
| 8 — federation | `crates/percept/tests/federation_e2e.rs` |
| 9 — container | operator-side via the release pipeline; no in-process test |
- ⊘ descoped / superseded

---

## Slice 0 — Workspace scaffold + core types
**Goal:** turn a docs-only repo into a buildable Rust workspace; freeze the
canonical type surface used by everything downstream.

**In scope:**
- Workspace `Cargo.toml` with 4 member crates under `crates/`:
  `percept-core`, `percept-store`, `percept-ingest`, `percept` (binary).
- `percept-core`: `Event`, `Link`, `SourceDescriptor`, `KindDescriptor`,
  `KindRef` (parses `kind@vN`), ULID helpers, time helpers, error type.
- Config loader: TOML, `percept.toml` + `conf.d/*.toml` merge (later files
  win), `*_env` / `*_file` secret indirection.
- Descriptor resolution: merge SourceDescriptor over KindDescriptor for
  non-schema fields; source's `semantic_schema` fully replaces kind's.
- CLI scaffold via `clap`: `percept serve`, `percept config check`,
  `percept version`.

**Out of scope (this slice):**
- No adapters, no storage, no MCP server.
- `serve` just logs the resolved descriptor map and exits.

**Acceptance:**
- `cargo build --workspace` succeeds.
- `cargo test --workspace` runs ≥ 10 unit tests covering serde round-trips,
  config merge, kind version resolution.
- `percept config check` rejects (with line numbers): inline secrets,
  unknown TOML keys, unresolvable `*_env`, duplicate `source_id`,
  retention with `vector_max_age > raw retention`.
- CI (now non-vacuous) green.

**Status:** ◐ (this PR)

---

## Slice 1 — Ingest pipeline (no MCP yet)
**Goal:** events from HTTP land in the hot ring; throughput is observable.

**In scope:**
- HTTP listener (axum): `POST /ingest` single + batched.
- Per-token authn; `allow_source_ids` / `allow_kinds` shell-glob scope check.
- Normalizer (single task per process, DECISIONS §7): assigns
  `event_id` (ULID), `ingest_ts_ms_utc`, `seq`; resolves `kind` version;
  soft-fails on `semantic_schema` validation with `_schema_invalid`.
- Bus: tokio mpsc fan-out, default depth 4096 per consumer.
- Hot rings per `(source_id, kind)`, default `max(256 events, 60 s)`.
- 429 + `X-Percept-Shed-Reason` on rejection; `Retry-After` on rate-limit.
- `/metrics` (Prometheus) exposing accepted / shed / rate-limit counters.
- `/healthz` (liveness).

**Out of scope:**
- No MCP server.
- No cold writer, no embedder.
- Sharded normalizer fallback (DECISIONS §7) — single task only.

**Acceptance:**
- End-to-end test: HTTP POST → event visible in the hot ring within 100 ms p99.
- Shed counters increment on overload; producer receives 429.
- 64 KB hard cap enforced (returns `payload_too_large`).
- Soft cap (16 KB) increments a counter, still accepts the event.

**Status:** ☑ (PRs #5 + slice-1 follow-up). Schema validation runtime
wired via `jsonschema` in the slice-1 follow-up: SchemaIndex compiles
every `semantic_schema` at startup; the normalizer marks
`_schema_invalid = true` and bumps the counter on payloads that fail
their resolved schema (source override > kind+version, with unversioned
kinds resolving to the latest registered version).

---

## Slice 2 — MCP server + hot-path tools
**Goal:** LLM can ask "what's available?" and "what is true now?"

**In scope:**
- `rmcp` server mounted at `POST /mcp` (Streamable HTTP).
- Bearer token auth on the MCP endpoint.
- Tools: `describe_sources(filter?)`, `get_current_state(...)`.
- `describe_sources` returns merged Source+Kind plus a `recent_errors`
  digest (drop counters, last-error timestamp) sourced from in-memory
  counters from Slice 1.
- `get_current_state` reads hot rings only (cold fallback in Slice 3).
- `stale` flag derived from `freshness_ttl_ms`.
- MCP-side tool-call counters and latency on `/metrics`.

**Out of scope:**
- No SSE transport fallback in v1 (config rejects with a clear error).
- Cold-store fallback for `get_current_state` (Slice 3).

**Acceptance:**
- An MCP client can call both tools and receives canonical Event-shape JSON.
- `describe_sources` ≤ 50 ms p99 on a 100-source config.

**Status:** ☑ (this PR). MCP server is hand-rolled (JSON-RPC over HTTP)
rather than `rmcp`-backed — the wire surface we need is small enough that
tracking the `rmcp` crate's API churn isn't worth it. Swap is mechanical
when sampling / SSE-streamed responses become needed.

---

## Slice 3 — Cold store + window queries
**Goal:** durability and "what happened in [time range]".

**In scope:**
- ~~DuckDB + Parquet, partitioned by day and `source_id`~~ — v1 ships
  with rusqlite (bundled SQLite) instead. DuckDB-bundled produces ~4 GB
  per build directory and a 9-minute first compile, which CI runners
  can't sustain. SQLite + the same SQL surface (cursor-friendly
  indexes on `(ts_ms_utc, event_id)` and `(source_id, kind, ts_ms_utc)`)
  meets the slice-3 acceptance target. DuckDB stays as a v2 perf
  concern when Parquet portability and >10 M-event scans become real
  requirements. `day` column is in place for the partition-drop work in
  slice 5; Parquet export joins it.
- Cold writer consumer: batches by time + size; lag observable.
- `latest` table maintained at each batch commit (DECISIONS §4).
- `get_current_state` cold fallback on hot-ring miss; results tagged
  `from_cold = true`.
- `get_window(start_ms, end_ms, ...)` MCP tool with cursor pagination
  (CBOR + base64url + BLAKE3 filter hash per DECISIONS §3).
- Per-call hard limit 10 000 events; ordering `(ts_ms_utc, event_id) ASC`.

**Out of scope:**
- Retention sweeper (Slice 5).
- Vector search (Slice 4).

**Acceptance:**
- 1 M-event window scan returns the first 10 k within 500 ms p99.
- Cursor resume returns disjoint, ordered results; a tampered cursor returns
  `cursor_filter_mismatch`.
- Cold writer lag exposed on `/metrics`.

**Status:** ☑ (this PR). DuckDB → rusqlite deviation noted above and in
DESIGN Appendix A.

---

## Slice 4 — Vector search
**Goal:** semantic queries over recorded events.

**In scope:**
- Embedder consumer task with time/size batch flush, lag observable on
  `/metrics`.
- Vector index (SQLite-persisted, in-memory float32 mirror for slice 4;
  brute-force cosine — see "Engine deviation" below). Embedding model id
  stored per vector; mismatch at startup rejects with a clear error.
- Truncation rule: 2048 bytes on UTF-8 boundary; `truncated` flag on the
  vector row and propagated to `search_events` results.
- `search_events(query, time_range?, source_filter?, kind_filter?, limit?)`
  MCP tool. Top-k cap 50 (DESIGN §11.1).
- Startup check: `vector_max_age > raw retention` already rejected by the
  slice-0 validator.
- Per-kind / per-source opt-in via `embed = true` (TOML). Default off
  (`embed_default = false`).

**Engine deviation (v1):**
- ~~FastEmbed-rs + `bge-small-en-v1.5`~~ → `HashEmbedder` placeholder
  (deterministic 64-dim hash projection). The ONNX model fetch needs
  network access we don't have in CI/sandbox, and the `ort` toolchain
  has the same compile/disk profile that pushed us off DuckDB. The
  `Embedder` trait is the swap point — slice-4 follow-up wires the
  production model the same way slice-1 follow-up wired the schema runtime.
- ~~LanceDB~~ → SQLite-backed table + in-memory float32 brute-force
  cosine. Matches the slice-3 storage strategy; fast enough for the edge
  profile up to ~100 k vectors × 64 dims (a few ms per query). ANN
  (HNSW / LanceDB) lands when vector counts or production dim push
  memory.

**Out of scope:**
- Real embedding model (slice-4 follow-up).
- Re-index command (deferred to ops work).
- ANN engine (lands with the production embedder).
- Multiple embedding models per kind (v2).

**Acceptance:**
- Top-50 search over a 1 M-vector index returns ≤ 300 ms p99.
  *(Validated end-to-end for the slice-4 wiring with the placeholder
  embedder; production-scale validation is a follow-up.)*
- Embedder lag observable; doesn't block ingest under load.

**Status:** ☑ (this PR). FastEmbed/LanceDB swap-in deferred to a
slice-4 follow-up per "Engine deviation".

---

## Slice 5 — Retention sweeper
**Goal:** disk doesn't grow forever; policy is auditable.

**In scope:**
- Background sweeper (default 1 h cadence; configurable via
  `[storage] sweeper_interval`).
- `max_age` as a `(source_id, kind, ts_ms_utc)` DELETE against the cold
  store — uses the `events_by_source_kind` index slice 3 already
  established. (Whole-Parquet-file unlinks are a v2 follow-up alongside
  Parquet export, see DESIGN Appendix A.)
- `max_count` / `max_bytes` via in-partition rewrite; warning logged when
  bound to a `(source, kind)` exceeding `EXPENSIVE_REWRITE_THRESHOLD`
  events.
- Source > kind > global resolution order (first-match-wins per
  dimension).
- Vector pruning by `vector_max_age`. (Cross-DB orphan reconciliation —
  vectors whose `event_id` was just dropped from the cold store — is a
  slice-6 follow-up; until then, `vector_max_age` is the operator's
  knob for keeping the vector index bounded.)
- `describe_sources()` surfaces `effective_retention` per source.
- `percept retention dry-run --config <path>` CLI prints a JSON
  `SweepReport` of what the next sweep would drop.

**Out of scope:**
- Blob GC (producer's problem, DESIGN §11.3).
- Event-level "pinned" flag (explicit non-goal).

**Acceptance:**
- Sweeper drops events older than `max_age` cheaply (single indexed
  DELETE); dry-run accurately predicts the drop without modifying state.
- Per-source effective policy reflected in `describe_sources()`.

**Integration test:** `crates/percept/tests/retention_e2e.rs` drives
the live background `Sweeper` task: appends events spanning the
cut-off, lets the sweeper fire on its cadence, asserts the right
rows survive.

**Status:** ☑. Cross-DB orphan reconciliation between vectors
and the cold store is the one slice-5 follow-up; the rest of the in-scope
list ships here.

---

## Slice 6 — More adapters
**Goal:** real producers (beyond raw HTTP) can reach Percept.

**In scope:**
- MQTT subscriber (`rumqttc`): topic-capture template `{+N}` / `{#}`,
  JSONPath `kind_field` (via `jsonpath-rust`, RFC 9535-compliant),
  payload decoders `json` / `raw` / `hex` / `csv`. Pure
  `(topic, payload, subs) → IngestEvent` routing layer, separately
  unit-tested from the rumqttc transport.
- WebSocket ingest at `/ingest/ws` (same `IngestPayload` wire shape as
  HTTP `/ingest`; bearer token via query string since browser WS
  clients can't set headers).
- `percept ble pair <mac>` wrapper over `bluetoothctl` (MAC validation,
  exits non-zero on failure).

**Engine deviation (v1):**
- ~~`serde_json_path` for JSONPath~~ → `jsonpath-rust`. The
  `serde_json_path` 0.6.x line has an internal crate-split version
  mismatch with its macros crate that the resolver can't untangle.
  Both are RFC 9535 implementations; the swap is cosmetic.

**Slice 6 follow-up (separate PR):**
- BLE scan (`btleplug`) + GATT mode. Defer because the sandbox has no
  BLE hardware to validate against, and `btleplug` brings in heavy
  platform-specific native deps (BlueZ/CoreBluetooth/WinRT) that need
  the same kind of compile/disk audit DuckDB / FastEmbed got. Gated
  behind a `ble` feature when it lands.

**Out of scope:**
- ROS2 / Foxglove bridge (out of v1).
- BLE decoder plugin surface (built-in decoders only).
- TLS for MQTT (`mqtts://` rejects at startup; use `mqtt://` on a
  private network until v1.x ships proper TLS).

**Acceptance:**
- MQTT: 28 unit tests across decode/topic/router/subscriber covering
  route-table, JSONPath kind resolution, raw/hex/csv decoders,
  first-match wins, decode-failure counter, and bus-full →
  consumer_drop. A live `rumqttd`-broker e2e was tried and pulled —
  rumqttd 0.19's `Broker::start()` is blocking and never returns,
  which blocks tokio runtime drop indefinitely. The rumqttc
  EventLoop wrapper is a thin pass-through into the unit-tested
  `route()`; a real broker test belongs in a `cargo run --example
  mqtt_smoke` harness that the operator runs against the Pi 5 demo.
- WebSocket: 5 e2e tests (round-trip into hot ring, invalid bearer,
  scope deny → shed_reason, batch form, malformed JSON).
- Unknown MQTT payload type → `messages_decode_failed` increments, no
  panic.

**Status:** ☑. BLE adapters deferred to slice-6 follow-up per
"Slice 6 follow-up" above; live MQTT broker test belongs in the
operator-side smoke harness, not CI.

---

## Slice 7 — Producer SDK (Rust)
**Goal:** thin client library so producers don't reimplement batching.

**In scope:**
- New crate `percept-client`: async `Client::send_batch` with retry on
  `429`/`503` honouring `Retry-After`, bearer-token auth, gzip on the
  wire (default). Maps `X-Percept-Shed-Reason: unauthorized` /
  `payload_too_large` to `ClientError::ScopeDeny` / `PayloadTooLarge`
  immediately rather than burning retries on a permanent error.
- `Batcher` wrapper with background flush task (size + time triggers)
  for the chatty-producer case.
- `BlockingClient` for embedded producers without a tokio runtime,
  gated behind the `blocking` cargo feature so async-only consumers
  don't pay the cost.
- Server-side: `tower-http::decompression` layer added to the ingest
  router so gzipped request bodies transparently decompress before
  hitting the handler.

**Out of scope:**
- Non-Rust SDKs (community / later).
- Forwarder behaviour (Slice 8 builds on this).

**Acceptance:**
- Round-trip test: SDK → HTTP ingest → hot ring (verified via the
  in-process server harness with `Pipeline`).
- `429 + Retry-After` honoured under simulated rate-limit (test
  configures a `1/s` scope, fires two requests, asserts the second
  succeeds only after a backoff sleep).

**Status:** ☑ (this PR).

---

## Slice 8 — Hub-and-spoke + federation
**Goal:** the topology stories from DESIGN §8 work end-to-end.

**In scope:**
- Forwarder: edge → hub via `percept-client` with mandatory `<peer_id>.`
  source_id prefix on egress. Hooks into the normalizer's fan-out as a
  fourth optional sink (alongside cold writer + embedder); when the
  hub is down, `try_send` drops with a `consumer_drops{consumer="forwarder"}`
  increment so local ingest never blocks.
- Local hot rings answer "now" when WAN is down (covered by the same
  fan-out architecture — the forwarder is decoupled from the hot ring
  write path).
- Federation: `describe_sources()` and `get_current_state()` aggregate
  across peers. Each peer query runs concurrently with its own
  `timeout_ms`; `peer_status` is reported per-peer (`ok` / `timeout` /
  `error{message}`). Every row in `sources` / `states` carries a
  `peer_id` (`null` for local) so the LLM can attribute results.

**Out of scope:**
- Cross-peer write coordination (explicit non-goal, DESIGN §8).
- Federation of `get_window` / `search_events` — cursor + ANN
  pagination across peers is meaningfully harder and is a slice-8
  follow-up.

**Acceptance:**
- Two-edge demo: edges A and B both have `temp.fridge`; hub sees
  `A.temp.fridge` and `B.temp.fridge`; unprefixed `temp.fridge` is
  absent from the hub.
- WAN-down test: edge's local `get_current_state` still works while
  the forwarder's `send_errors` counter ticks.
- Federation: `describe_sources` / `get_current_state` aggregate
  across peers with `peer_status: ok`; an unreachable peer reports
  `timeout` / `error` without taking down the local result.

**Status:** ☑ (this PR). `get_window` / `search_events` federation is
a slice-8 follow-up.

---

## Slice 9 — Container + v0.1.0 release
**Goal:** first tagged release with a working image.

**In scope:**
- `Dockerfile` (multi-stage: `rust:1.85-bookworm` builder →
  `debian:bookworm-slim` runtime; tini as PID 1; non-root user
  `percept` (uid 1000); CA certs for outbound TLS from the forwarder
  and federation peers).
- Sample `percept.toml` shipped at `/etc/percept/percept.toml`; covers
  [server] + [mcp] + [storage] + a starter `[[http_token]]` so the
  default image doesn't reject every ingest out of the box.
- Tag `v0.1.0` triggers `release.yml`: native-arch tarballs
  (x86_64 + aarch64) + multi-arch GHCR image (`linux/amd64`,
  `linux/arm64`).

**Slice-4 follow-up makes this simpler:**
- ~~ONNX Runtime shared libs bundled~~ — slice 4 shipped a deterministic
  `HashEmbedder` placeholder, so the runtime image needs zero ONNX
  libs. The Dockerfile shrinks to ca-certificates + tini on top of
  bookworm-slim (~30 MB base).

**Out of scope:**
- Distroless image (later — re-evaluate once the real embedder lands
  and we know its native-deps story).

**Acceptance:**
- `docker run ghcr.io/wowow4978-vorfeed/percept:v0.1.0 \
      percept config-check --config /etc/percept/percept.toml`
  exits 0 when `PERCEPT_MCP_TOKEN` and `PERCEPT_INGEST_TOKEN` are
  passed in.
- Pi 5 reference deployment (DESIGN Appendix B) runs the image
  end-to-end (operator-side validation; sandbox can't reach a Pi).

**Status:** ☑ (this PR). Tagging `v0.1.0` is the operator's call once
the PR merges to `main`; CI does the rest.

---

## Cross-cutting (woven through every slice)

- **Tracing:** `trace_id` propagated adapter → bus → cold writer → MCP
  response from Slice 1 onward.
- **Observability:** `/metrics` and `/healthz` exist from Slice 1; every
  later slice adds its own counters / gauges.
- **Errors:** `thiserror` in libs, `anyhow` in the binary; canonical
  error type defined in `percept-core` from Slice 0.
- **Tests:** every slice ships unit tests + ≥ 1 integration test
  exercising its acceptance criteria.
- **Docs:** any user-visible change updates `DESIGN.md` (if the contract
  shifts) or this plan (if scope shifts). The shifted state lands in the
  same PR.

---

## Deferred to v2 (not on this plan)
- Server-profile cold store (relational TSDB + pgvector); needs the
  query-layer IR open work (DESIGN §13).
- Producer-side descriptor registration.
- Multi-tenant ACLs, signed events, per-source scopes.
- Hot reload of config.
- ROS2 / Foxglove bridge.
- Code coverage in CI (CI.md §3).
- Non-Rust producer SDKs.
- Descriptor history (point-in-time) on `get_window` results.
