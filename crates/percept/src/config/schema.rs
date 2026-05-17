//! TOML schema for `percept.toml`. Mirrors DESIGN.md §12.
//!
//! `deny_unknown_fields` is set on every struct so unknown keys (including
//! inline `password = "..."` instead of `password_env = "..."`) are
//! rejected at parse time with the TOML source location.

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: Option<Server>,
    #[serde(default)]
    pub mcp: Option<Mcp>,
    #[serde(default)]
    pub storage: Option<Storage>,

    #[serde(default, rename = "retention")]
    pub retention: Vec<RetentionEntry>,

    #[serde(default, rename = "mqtt")]
    pub mqtt: Vec<MqttBroker>,

    #[serde(default, rename = "ble")]
    pub ble: Vec<BleAdapter>,

    #[serde(default, rename = "http_token")]
    pub http_tokens: Vec<HttpToken>,

    #[serde(default, rename = "ros2")]
    pub ros2: Vec<Ros2Bridge>,

    #[serde(default, rename = "forwarder")]
    pub forwarders: Vec<ForwarderEntry>,

    #[serde(default, rename = "peer")]
    pub peers: Vec<PeerEntry>,

    #[serde(default, rename = "source")]
    pub sources: Vec<SourceEntry>,

    #[serde(default, rename = "kind")]
    pub kinds: Vec<KindEntry>,
}

impl Config {
    /// Merge `overlay` into `self`. Scalar sections — later wins.
    /// Array-of-tables sections — entries accumulate.
    pub fn merge(&mut self, overlay: Self) {
        if overlay.server.is_some() {
            self.server = overlay.server;
        }
        if overlay.mcp.is_some() {
            self.mcp = overlay.mcp;
        }
        if overlay.storage.is_some() {
            self.storage = overlay.storage;
        }
        self.retention.extend(overlay.retention);
        self.mqtt.extend(overlay.mqtt);
        self.ble.extend(overlay.ble);
        self.http_tokens.extend(overlay.http_tokens);
        self.ros2.extend(overlay.ros2);
        self.forwarders.extend(overlay.forwarders);
        self.peers.extend(overlay.peers);
        self.sources.extend(overlay.sources);
        self.kinds.extend(overlay.kinds);
    }
}

/// `[[forwarder]]` — push events from this edge to a hub. DESIGN §8.
/// Source IDs are mandatorily prefixed with `peer_id` on egress.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForwarderEntry {
    /// Prefix the forwarder prepends to every source_id on egress.
    pub peer_id: String,
    /// Base URL of the hub Percept (e.g. `http://hub.lan:7878`).
    pub hub_url: String,
    /// Bearer token the hub accepts (via env var ref).
    #[serde(default)]
    pub hub_token_env: Option<String>,
    #[serde(default)]
    pub hub_token_file: Option<String>,
    /// Populated by `secrets::resolve` after the env/file lookup.
    #[serde(skip)]
    pub resolved_hub_token: Option<String>,
}

/// `[[peer]]` — a remote Percept queried as part of federation.
/// DESIGN §8: `describe_sources` and `get_current_state` fan out to
/// these alongside the local query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerEntry {
    /// Logical name attached to results aggregated from this peer.
    pub id: String,
    /// Full URL of the peer's MCP endpoint (e.g.
    /// `http://kitchen.lan:7878/mcp`).
    pub mcp_url: String,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default)]
    pub token_file: Option<String>,
    /// Per-peer timeout for the fan-out call; default is 1s when unset.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(skip)]
    pub resolved_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub data_dir: String,
    #[serde(default = "default_profile")]
    pub profile: String,
}

fn default_profile() -> String {
    "edge".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mcp {
    pub listen: String,
    #[serde(default)]
    pub transport: Option<String>,
    pub auth: SecretRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    #[serde(default)]
    pub sweeper_interval: Option<String>,
    #[serde(default)]
    pub embed_default: Option<bool>,
}

/// Indirect credential reference. Exactly one of `*_env` or `*_file` must be
/// set; a bare `token = "..."` field would be an unknown key and rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default)]
    pub token_file: Option<String>,
    /// Populated by `secrets::resolve` after env/file lookup; never read from TOML.
    #[serde(skip)]
    pub resolved: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionEntry {
    pub r#match: RetentionMatch,
    #[serde(default)]
    pub max_age: Option<String>,
    #[serde(default)]
    pub max_count: Option<i64>,
    #[serde(default)]
    pub max_bytes: Option<i64>,
    #[serde(default)]
    pub vector_max_age: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionMatch {
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttBroker {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub credentials: Option<MqttCredentials>,
    #[serde(default)]
    pub tls: Option<MqttTls>,
    #[serde(default, rename = "subscription")]
    pub subscriptions: Vec<MqttSubscription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttCredentials {
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    #[serde(default)]
    pub password_file: Option<String>,
    #[serde(skip)]
    pub resolved_password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttTls {
    #[serde(default)]
    pub ca_file: Option<String>,
    #[serde(default)]
    pub insecure: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSubscription {
    pub topic: String,
    pub source_id_template: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub kind_field: Option<String>,
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default)]
    pub qos: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BleAdapter {
    pub adapter: String,
    pub mode: String,
    #[serde(default)]
    pub duplicates: Option<bool>,
    #[serde(default, rename = "device")]
    pub devices: Vec<BleDevice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BleDevice {
    pub mac: String,
    pub source_id: String,
    pub kind: String,
    #[serde(default)]
    pub decoder: Option<String>,
    #[serde(default)]
    pub require_bond: Option<bool>,
    #[serde(default)]
    pub poll: Option<String>,
    #[serde(default, rename = "gatt")]
    pub gatt: Vec<BleGatt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BleGatt {
    pub service_uuid: String,
    pub char_uuid: String,
    #[serde(default)]
    pub decoder: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpToken {
    pub name: String,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default)]
    pub token_file: Option<String>,
    #[serde(default)]
    pub allow_source_ids: Vec<String>,
    #[serde(default)]
    pub allow_kinds: Vec<String>,
    #[serde(default)]
    pub rate_limit: Option<String>,
    #[serde(skip)]
    pub resolved_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ros2Bridge {
    pub node_name: String,
    #[serde(default)]
    pub domain_id: Option<i64>,
    #[serde(default, rename = "subscription")]
    pub subscriptions: Vec<Ros2Subscription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ros2Subscription {
    pub topic: String,
    pub msg_type: String,
    pub source_id_template: String,
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEntry {
    pub id: String,
    pub kinds: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub usage: Option<String>,
    #[serde(default)]
    pub caveats: Option<String>,
    #[serde(default)]
    pub semantic_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub units: Option<String>,
    #[serde(default)]
    pub sampling_hint_ms: Option<i64>,
    #[serde(default)]
    pub freshness_ttl_ms: Option<i64>,
    #[serde(default)]
    pub location: Option<String>,
    /// Per-source override for the embedder opt-in (DECISIONS §2).
    /// Takes precedence over the kind-level `embed` and the
    /// `[storage].embed_default`.
    #[serde(default)]
    pub embed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindEntry {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub usage: Option<String>,
    #[serde(default)]
    pub caveats: Option<String>,
    #[serde(default)]
    pub semantic_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub units: Option<String>,
    /// Per-kind opt-in for the embedder; overridden by a source-level
    /// `embed` when both are set.
    #[serde(default)]
    pub embed: Option<bool>,
}
