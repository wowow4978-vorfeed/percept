//! Slice 8 acceptance: hub-and-spoke forwarding + federation fan-out.
//!
//! Each test spins up two or three in-process Percept instances and
//! wires them together via the `percept-client` SDK (for forwarders)
//! and reqwest MCP calls (for federation).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use percept::forwarder::{Forwarder, ForwarderConfig, ForwarderMetrics};
use percept::mcp::{DescriptorRegistry, McpState, PeerHandle};
use percept_client::Client as IngestClient;
use percept_core::ResolvedDescriptor;
use percept_ingest::{Auth, Pipeline, PipelineConfig, SchemaIndex, TokenScope};
use serde_json::{json, Value};

struct Node {
    base: String,
    hot_rings: Arc<percept_store::HotRings>,
    _server: tokio::task::JoinHandle<()>,
    forward_rx: Option<tokio::sync::mpsc::Receiver<Arc<percept_core::Event>>>,
    pipeline_metrics: Arc<percept_ingest::Metrics>,
}

const TOKEN: &str = "node-token";

async fn spawn_node(
    descriptors: Vec<ResolvedDescriptor>,
    peers: Vec<PeerHandle>,
    forwarder_enabled: bool,
) -> Node {
    let mut auth = Auth::new();
    auth.insert(
        TOKEN.to_string(),
        TokenScope::build("test", &["*".into()], &["*".into()], None).unwrap(),
    );
    let schemas = Arc::new(SchemaIndex::default());
    let cold_store = Some(Arc::new(
        percept_store::ColdStore::open_in_memory().unwrap(),
    ));
    let mut pipeline = Pipeline::spawn(
        Arc::new(auth),
        schemas,
        cold_store.clone(),
        None,
        forwarder_enabled,
        PipelineConfig::default(),
    );

    let mcp_state = McpState {
        token: Arc::new(TOKEN.to_string()),
        registry: Arc::new(DescriptorRegistry::new(descriptors)),
        hot_rings: pipeline.hot_rings.clone(),
        cold_store,
        vector_index: None,
        embedder: None,
        retention_policies: Arc::new(Vec::new()),
        peers: Arc::new(peers),
        metrics: pipeline.metrics.clone(),
    };

    let forward_rx = pipeline.forward_rx.take();
    let hot_rings = pipeline.hot_rings.clone();
    let pipeline_metrics = pipeline.metrics.clone();
    let app =
        percept_ingest::router(pipeline.http_state.clone()).merge(percept::mcp::router(mcp_state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    Node {
        base: format!("http://{addr}"),
        hot_rings,
        _server: server,
        forward_rx,
        pipeline_metrics,
    }
}

fn rd(src: &str, kind: &str) -> ResolvedDescriptor {
    ResolvedDescriptor {
        source_id: src.to_string(),
        kind: kind.to_string(),
        kind_version: "v1".to_string(),
        description: String::new(),
        usage: String::new(),
        caveats: String::new(),
        semantic_schema: None,
        units: None,
        sampling_hint_ms: None,
        freshness_ttl_ms: None,
        location: None,
    }
}

async fn post_mcp(base: &str, body: Value) -> Value {
    let resp = reqwest::Client::new()
        .post(format!("{base}/mcp"))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    resp.json().await.unwrap()
}

async fn post_ingest(base: &str, body: Value) {
    reqwest::Client::new()
        .post(format!("{base}/ingest"))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn two_edges_forward_with_prefixed_source_ids_no_collision() {
    // Hub first, then two edges pointed at it.
    let hub = spawn_node(vec![], vec![], false).await;

    let mut edge_a = spawn_node(vec![rd("temp.fridge", "temperature")], vec![], true).await;
    let mut edge_b = spawn_node(vec![rd("temp.fridge", "temperature")], vec![], true).await;

    // Spawn one forwarder per edge.
    let client_a = Arc::new(IngestClient::new(&hub.base, TOKEN));
    let forwarder_a = Forwarder::new(
        edge_a.forward_rx.take().unwrap(),
        client_a,
        ForwarderConfig {
            peer_id: "A".into(),
            batch_size: 4,
            batch_age: Duration::from_millis(50),
        },
        Arc::new(ForwarderMetrics::default()),
    );
    tokio::spawn(forwarder_a.run());

    let client_b = Arc::new(IngestClient::new(&hub.base, TOKEN));
    let forwarder_b = Forwarder::new(
        edge_b.forward_rx.take().unwrap(),
        client_b,
        ForwarderConfig {
            peer_id: "B".into(),
            batch_size: 4,
            batch_age: Duration::from_millis(50),
        },
        Arc::new(ForwarderMetrics::default()),
    );
    tokio::spawn(forwarder_b.run());

    // Both edges receive an event for the *same* local source_id.
    post_ingest(
        &edge_a.base,
        json!({
            "source_id": "temp.fridge",
            "kind": "temperature",
            "ts_ms_utc": percept_core::now_ms_utc(),
            "semantic": { "c": 4 }
        }),
    )
    .await;
    post_ingest(
        &edge_b.base,
        json!({
            "source_id": "temp.fridge",
            "kind": "temperature",
            "ts_ms_utc": percept_core::now_ms_utc(),
            "semantic": { "c": 5 }
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Hub should now see both events under their prefixed source_ids.
    assert!(
        hub.hot_rings
            .latest("A.temp.fridge", "temperature")
            .is_some(),
        "hub missing A.temp.fridge"
    );
    assert!(
        hub.hot_rings
            .latest("B.temp.fridge", "temperature")
            .is_some(),
        "hub missing B.temp.fridge"
    );
    // And the unprefixed source_id should NOT exist on the hub (no
    // collision).
    assert!(
        hub.hot_rings.latest("temp.fridge", "temperature").is_none(),
        "hub saw unprefixed temp.fridge — forwarder didn't rewrite"
    );
    // Local edges keep their unprefixed view (so a local LLM query
    // against the edge still sees the canonical source_id).
    assert!(edge_a
        .hot_rings
        .latest("temp.fridge", "temperature")
        .is_some());
    assert!(edge_b
        .hot_rings
        .latest("temp.fridge", "temperature")
        .is_some());
}

#[tokio::test]
async fn edge_keeps_serving_when_hub_is_unreachable() {
    // Edge configured with a forwarder pointed at a bogus hub. The
    // forwarder will fail to send; the edge's local pipeline must keep
    // serving regardless.
    let mut edge = spawn_node(vec![rd("therm.kitchen", "temperature")], vec![], true).await;
    let bogus_client = Arc::new(IngestClient::with_config(
        "http://127.0.0.1:1", // closed port
        TOKEN,
        percept_client::ClientConfig {
            max_attempts: 1,
            timeout: Duration::from_millis(100),
            ..percept_client::ClientConfig::default()
        },
    ));
    let metrics = Arc::new(ForwarderMetrics::default());
    let forwarder = Forwarder::new(
        edge.forward_rx.take().unwrap(),
        bogus_client,
        ForwarderConfig {
            peer_id: "edge".into(),
            batch_size: 1,
            batch_age: Duration::from_millis(50),
        },
        metrics.clone(),
    );
    tokio::spawn(forwarder.run());

    post_ingest(
        &edge.base,
        json!({
            "source_id": "therm.kitchen",
            "kind": "temperature",
            "ts_ms_utc": percept_core::now_ms_utc(),
            "semantic": { "c": 20 }
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The local hot ring is populated even though the forwarder failed.
    assert!(
        edge.hot_rings
            .latest("therm.kitchen", "temperature")
            .is_some(),
        "local hot ring should still answer when WAN is down",
    );
    // And the forwarder's metrics show the failure.
    assert!(
        metrics
            .send_errors
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
    // Drop the unused metrics ref to silence the unused_variables warn
    // some linters apply to last-line bindings.
    let _ = &edge.pipeline_metrics;
}

#[tokio::test]
async fn describe_sources_federates_across_peers() {
    // Peer A and B each have their own descriptors. Hub knows them as
    // peers and aggregates via describe_sources.
    let peer_a = spawn_node(vec![rd("cam.front", "object_detected")], vec![], false).await;
    let peer_b = spawn_node(vec![rd("therm.kitchen", "temperature")], vec![], false).await;

    let peers = vec![
        PeerHandle {
            id: "A".into(),
            mcp_url: format!("{}/mcp", peer_a.base),
            token: TOKEN.into(),
            timeout: Duration::from_secs(2),
        },
        PeerHandle {
            id: "B".into(),
            mcp_url: format!("{}/mcp", peer_b.base),
            token: TOKEN.into(),
            timeout: Duration::from_secs(2),
        },
    ];
    let hub = spawn_node(vec![rd("hub.local", "marker")], peers, false).await;

    let body = post_mcp(
        &hub.base,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": "describe_sources", "arguments": {} }
        }),
    )
    .await;
    let sources = body["result"]["structuredContent"]["sources"]
        .as_array()
        .unwrap();
    // 1 local + 1 from each peer.
    assert_eq!(sources.len(), 3, "got: {body}");
    // Every entry carries a peer_id (null for local, "A"/"B" for peers).
    let peer_ids: Vec<&Value> = sources.iter().map(|s| &s["peer_id"]).collect();
    assert!(peer_ids.iter().any(|p| p.is_null()));
    assert!(peer_ids.iter().any(|p| p == &&Value::String("A".into())));
    assert!(peer_ids.iter().any(|p| p == &&Value::String("B".into())));
    // peer_status reports ok for both peers.
    let status = &body["result"]["structuredContent"]["peer_status"];
    assert_eq!(status["A"]["status"], "ok");
    assert_eq!(status["B"]["status"], "ok");
}

#[tokio::test]
async fn describe_sources_records_peer_timeout() {
    // Peer URL points at a closed port so the dial fails fast. The
    // per-peer timeout still kicks in; the report should mark it.
    let peers = vec![PeerHandle {
        id: "ghost".into(),
        mcp_url: "http://127.0.0.1:1/mcp".into(),
        token: TOKEN.into(),
        timeout: Duration::from_millis(150),
    }];
    let hub = spawn_node(vec![rd("hub.local", "marker")], peers, false).await;

    let body = post_mcp(
        &hub.base,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": "describe_sources", "arguments": {} }
        }),
    )
    .await;
    let status = &body["result"]["structuredContent"]["peer_status"]["ghost"];
    // Either timeout or transport error — both are valid "peer down"
    // signals; assert the variant tag exists and isn't "ok".
    let s = status["status"].as_str().unwrap_or_default();
    assert!(s == "timeout" || s == "error", "got: {status}");
    // The local row is still returned.
    let sources = body["result"]["structuredContent"]["sources"]
        .as_array()
        .unwrap();
    assert_eq!(sources.len(), 1);
}

#[tokio::test]
async fn get_current_state_federates_across_peers() {
    let peer_a = spawn_node(vec![rd("cam.front", "object_detected")], vec![], false).await;
    // Push a real event into peer_a so the hub sees something.
    post_ingest(
        &peer_a.base,
        json!({
            "source_id": "cam.front",
            "kind": "object_detected",
            "ts_ms_utc": percept_core::now_ms_utc(),
            "semantic": { "label": "person" }
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = vec![PeerHandle {
        id: "A".into(),
        mcp_url: format!("{}/mcp", peer_a.base),
        token: TOKEN.into(),
        timeout: Duration::from_secs(2),
    }];
    let hub = spawn_node(vec![], peers, false).await;

    let body = post_mcp(
        &hub.base,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": "get_current_state", "arguments": {} }
        }),
    )
    .await;
    let states = body["result"]["structuredContent"]["states"]
        .as_array()
        .unwrap();
    // Hub itself has no local descriptors → no local states. But the
    // peer's state is aggregated.
    assert_eq!(states.len(), 1, "got: {body}");
    assert_eq!(states[0]["peer_id"], "A");
    assert_eq!(states[0]["event"]["source_id"], "cam.front");
}
