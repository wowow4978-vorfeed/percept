# Percept

[![CI](https://github.com/wowow4978-vorfeed/percept/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wowow4978-vorfeed/percept/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/wowow4978-vorfeed/percept?sort=semver&include_prereleases&label=release)](https://github.com/wowow4978-vorfeed/percept/releases)

Local-first event-aggregation server for LLM agents. Producers (sensors,
services, scripts) push structured events; Percept normalises and retains
them across a hot recency window, a cold SQL store, and an optional vector
index; LLM agents read the aggregate through a small **MCP** tool surface.

The point is to give an assistant a faithful, queryable picture of "what
is going on" without each producer needing to know about each consumer.

## Top-level architecture

```
        producers                            consumers
  ┌───────────────────┐                ┌───────────────────┐
  │ HTTP POST /ingest │                │  LLM agents       │
  │ WebSocket  /ws    │                │  (OpenClaw,       │
  │ MQTT subscriber   │                │   Claude, …)      │
  └─────────┬─────────┘                └─────────▲─────────┘
            │ batched events                     │ JSON-RPC
            ▼                                    │
      ┌──────────┐                       ┌───────┴────────┐
      │normalizer│  ──── ULID, seq ───▶  │   MCP server   │
      └────┬─────┘                       │ describe_sources
           │ fan-out (non-blocking)      │ get_current_state
           ├────────────▶ hot ring  ◀────│ get_window
           ├────────────▶ cold (SQLite)◀─│ search_events
           ├────────────▶ embedder ───▶ vector index
           └────────────▶ forwarder ──▶ hub Percept (federation)
```

- **Hot ring** — bounded in-memory window per `(source_id, kind)`; serves
  `get_current_state`.
- **Cold store** — append-only SQLite with cursor-based pagination; serves
  `get_window`. A background sweeper enforces per-kind / per-source
  retention.
- **Vector index** — opt-in per source/kind; serves `search_events` via
  brute-force cosine kNN over the embedded `semantic` payload.
- **Forwarder / peers** — an edge Percept can forward its stream to a hub
  (prefixing `source_id` with `<peer_id>.`); a hub fans MCP queries out to
  its peers in parallel.

The full surface lives in [`docs/DESIGN.md`](docs/DESIGN.md); slice-by-slice
implementation notes are in [`docs/PLAN.md`](docs/PLAN.md).

## Quick start — Percept as OpenClaw's sensor backend

The goal: an [OpenClaw](https://github.com/openclaw/openclaw) assistant
that can answer questions about events from three producers — an HTTP
client, a browser WebSocket, and an MQTT-speaking device.

### 1. Run Percept

```sh
# Once a v* tag has been published, the image is on GHCR:
docker run -d --name percept \
  -p 7878:7878 \
  -v percept-data:/var/lib/percept \
  -e PERCEPT_MCP_TOKEN=$(openssl rand -hex 32) \
  -e PERCEPT_INGEST_TOKEN=$(openssl rand -hex 32) \
  ghcr.io/wowow4978-vorfeed/percept:latest

# Until then, build locally from the repo:
docker build -t percept:dev . && docker run -d --name percept \
  -p 7878:7878 -v percept-data:/var/lib/percept \
  -e PERCEPT_MCP_TOKEN=... -e PERCEPT_INGEST_TOKEN=... percept:dev
```

The shipped `/etc/percept/percept.toml` (see
[`docs/sample.percept.toml`](docs/sample.percept.toml)) reads both tokens
from the environment, exposes MCP on `:7878`, and accepts ingest from any
source on a single starter token. Replace it with a bind-mounted config
once you want per-producer scopes — the format is in `DESIGN.md` §12.

Keep the two token values handy — the producers below need the ingest
token, OpenClaw needs the MCP token.

### 2. Send events from each supported producer

**HTTP — the `percept-client` SDK** (any service running Rust):

```rust
use std::sync::Arc;
use percept_client::{Batcher, BatcherConfig, Client, Event};
use percept_core::now_ms_utc;

let client = Arc::new(Client::new(
    "http://percept.lan:7878",
    std::env::var("PERCEPT_INGEST_TOKEN")?,
));
let batcher = Batcher::spawn(client, BatcherConfig::default());

batcher.enqueue(Event::new(
    "door.front",
    "door.state",
    now_ms_utc(),
    serde_json::json!({ "open": true }),
)).await.ok();
```

Or with plain `curl` for shell producers:

```sh
curl -X POST http://percept.lan:7878/ingest \
  -H "Authorization: Bearer $PERCEPT_INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"events":[{
        "source_id":"door.front",
        "kind":"door.state",
        "ts_ms_utc": '"$(date +%s%3N)"',
        "semantic":{"open":true}
      }]}'
```

**WebSocket — `/ingest/ws`** (good fit for browsers and long-lived
producers that want backpressure). Browsers can't set custom headers on a
`WebSocket`, so the bearer goes in the `?token=` query string:

```js
const ws = new WebSocket(
  `ws://percept.lan:7878/ingest/ws?token=${PERCEPT_INGEST_TOKEN}`,
);
ws.onopen = () => ws.send(JSON.stringify({ events: [{
  source_id: "browser.tab.123",
  kind: "page.scroll",
  ts_ms_utc: Date.now(),
  semantic: { y: window.scrollY },
}]}));
```

**MQTT — broker subscription** (devices that already speak MQTT, e.g.
Zigbee2MQTT, ESPHome). Add a broker block to `percept.toml` and restart;
Percept subscribes and routes matching messages through the normaliser:

```toml
[[mqtt]]
id  = "home"
url = "tcp://mosquitto.lan:1883"
credentials = { user = "percept", password_env = "PERCEPT_MQTT_PASS" }

[[mqtt.subscription]]
topic              = "zigbee2mqtt/+/state"
source_id_template = "z2m.{+1}"    # `{+1}` = the first `+` capture
kind               = "device.state"
payload            = "json"
```

Then any device publishing to `zigbee2mqtt/kitchen-light/state` shows up
in Percept under `source_id = "z2m.kitchen-light"`. Test it with
`mosquitto_pub -t zigbee2mqtt/kitchen-light/state -m '{"on":true}'`.

### 3. Point OpenClaw at Percept's MCP

In OpenClaw's MCP Registry, add Percept as an HTTP-Streamable MCP server:

```yaml
servers:
  - name: percept
    transport: http-streamable
    url: http://percept.lan:7878/mcp
    auth:
      type: bearer
      token: "${PERCEPT_MCP_TOKEN}"
```

OpenClaw will discover four tools — `describe_sources`,
`get_current_state`, `get_window`, `search_events` — and the assistant
can now answer prompts like *"did the front door open while I was on the
call?"* by querying `get_window` over the call's time range, or *"what
am I currently doing?"* by reading the hot-ring snapshot.

## Repository layout

```
crates/
  core/       canonical Event / descriptor types
  store/      hot ring, cold SQLite store, vector index, retention sweeper
  ingest/     HTTP / WS / MQTT adapters, normaliser, auth + rate-limit
  client/     producer SDK (async + blocking)
  percept/    the binary: config loader, MCP server, federation, sweeper
docs/         DESIGN.md (architecture), PLAN.md (slice tracker),
              DECISIONS.md, CI.md, sample.percept.toml
```

## License

MIT — see [`LICENSE`](LICENSE).
