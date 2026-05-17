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

**Status:** ☐

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

**Status:** ☐

---

## Slice 3 — Cold store + window queries
**Goal:** durability and "what happened in [time range]".

**In scope:**
- DuckDB + Parquet, partitioned by day and `source_id`.
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

**Status:** ☐

---

## Slice 4 — Vector search
**Goal:** semantic queries over recorded events.

**In scope:**
- Embedder consumer (FastEmbed-rs, default `bge-small-en-v1.5`).
- LanceDB index; embedding model id stored per vector.
- Truncation rule: 2048 bytes on UTF-8 boundary; `truncated` flag on the
  vector row and propagated to results.
- `search_events(query, ...)` MCP tool with structured filters.
- Startup check: reject config where `vector_max_age > raw retention`.

**Out of scope:**
- Re-index command (deferred to ops work).
- Multiple embedding models per kind (v2).

**Acceptance:**
- Top-50 search over a 1 M-vector index returns ≤ 300 ms p99.
- Embedder lag observable; doesn't block ingest under load.

**Status:** ☐

---

## Slice 5 — Retention sweeper
**Goal:** disk doesn't grow forever; policy is auditable.

**In scope:**
- Background sweeper (default 1 h cadence).
- `max_age` via whole day-partition drops.
- `max_count` / `max_bytes` via in-partition rewrite; warning logged when
  bound to a high-rate source.
- Source > kind > global resolution order.
- Vector index pruning coupled to raw retention.
- `describe_sources()` surfaces effective policy per source.
- `percept retention dry-run` CLI.

**Out of scope:**
- Blob GC (producer's problem, DESIGN §11.3).
- Event-level "pinned" flag (explicit non-goal).

**Acceptance:**
- Sweeper drops a day-partition without rewriting; dry-run accurately
  predicts the drop.
- Per-source effective policy reflected in `describe_sources()`.

**Status:** ☐

---

## Slice 6 — More adapters
**Goal:** real producers (beyond raw HTTP) can reach Percept.

**In scope:**
- MQTT subscriber (`rumqttc`): topic-capture template `{+N}`, JSONPath
  (RFC 9535) `kind_field`, payload decoders `json` / `raw` / `hex` / `csv`.
- WebSocket ingest (same shape as HTTP batch).
- BLE scan mode (`btleplug` or equivalent): adverts → `ble.advert`.
- BLE GATT mode for bonded devices.
- `percept ble pair <mac>` wrapper over `bluetoothctl`.

**Out of scope:**
- ROS2 / Foxglove bridge (out of v1).
- BLE decoder plugin surface.

**Acceptance:**
- Each adapter passes an integration test against a local stand-in
  (mosquitto in CI; mock GATT for BLE).
- Unknown MQTT payload type increments a drop counter, no panic.

**Status:** ☐

---

## Slice 7 — Producer SDK (Rust)
**Goal:** thin client library so producers don't reimplement batching.

**In scope:**
- New crate `percept-client`: batched send, retry honouring `Retry-After`,
  bearer token, gzip when accepted.
- Logs `X-Percept-Shed-Reason` on shed.
- Optional sync variant for embedded producers.

**Out of scope:**
- Non-Rust SDKs (community / later).
- Forwarder behaviour (Slice 8 builds on this).

**Acceptance:**
- Round-trip test: SDK → HTTP ingest → hot ring → MCP query.
- 429 + `Retry-After` honoured under simulated rate-limit.

**Status:** ☐

---

## Slice 8 — Hub-and-spoke + federation
**Goal:** the topology stories from DESIGN §8 work end-to-end.

**In scope:**
- Forwarder: edge → hub via `percept-client` with mandatory `<peer_id>.`
  source_id prefix on egress; descriptors prefixed too.
- Local hot rings answer "now" when WAN is down.
- Federation: `describe_sources()` aggregates across peers; queries fan out
  with per-peer timeout; per-peer status (`ok` / `timeout` / `error`) in
  responses.

**Out of scope:**
- Cross-peer write coordination (explicit non-goal, DESIGN §8).

**Acceptance:**
- Two-edge demo: edges A and B both have `temp.fridge`; hub sees
  `A.temp.fridge` and `B.temp.fridge`; no collision.
- WAN-down test: edge's local `get_current_state` still works.

**Status:** ☐

---

## Slice 9 — Container + v0.1.0 release
**Goal:** first tagged release with a working image.

**In scope:**
- `Dockerfile` (multi-stage: rust builder → `debian:bookworm-slim`
  runtime; ONNX Runtime shared libs bundled).
- Sample `percept.toml` shipped at `/etc/percept/percept.toml` in the image.
- Tag `v0.1.0` triggers `release.yml`: tarballs (x86_64 + aarch64) + GHCR
  multi-arch image.

**Out of scope:**
- Distroless image (later — needs ONNX Runtime musl story).

**Acceptance:**
- `docker run ghcr.io/wowow4978-vorfeed/percept:v0.1.0 percept config check`
  exits 0 on the sample config.
- Pi 5 reference deployment (DESIGN Appendix B) runs the image end-to-end.

**Status:** ☐

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
