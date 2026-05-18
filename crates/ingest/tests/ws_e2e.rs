//! WebSocket ingest end-to-end tests. Spawns the full ingest pipeline,
//! opens a WebSocket against `/ingest/ws`, and asserts on accepted /
//! shed-reason responses.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use percept_ingest::{router, Auth, Pipeline, PipelineConfig, SchemaIndex, TokenScope};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "ws-token-xyz";

struct Harness {
    base_ws: String,
    hot_rings: Arc<percept_store::HotRings>,
    _server: tokio::task::JoinHandle<()>,
}

async fn spawn(scope: TokenScope) -> Harness {
    let mut auth = Auth::new();
    auth.insert(TOKEN.to_string(), scope);
    let pipeline = Pipeline::spawn(
        Arc::new(auth),
        Arc::new(SchemaIndex::default()),
        None,
        None,
        false,
        PipelineConfig::default(),
    );
    let app = router(pipeline.http_state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Harness {
        base_ws: format!("ws://{addr}"),
        hot_rings: pipeline.hot_rings,
        _server: server,
    }
}

fn permissive() -> TokenScope {
    TokenScope::build("test", &["*".into()], &["*".into()], None).unwrap()
}

#[tokio::test]
async fn websocket_round_trip_lands_in_hot_ring() {
    let h = spawn(permissive()).await;
    let url = format!("{}/ingest/ws?token={TOKEN}", h.base_ws);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let body = json!({
        "source_id": "cam.front",
        "kind": "object_detected",
        "ts_ms_utc": percept_core::now_ms_utc(),
        "semantic": { "label": "person" }
    });
    ws.send(Message::Text(body.to_string())).await.unwrap();

    // First text frame back should be {"accepted": 1}.
    let reply = ws.next().await.unwrap().unwrap();
    let text = reply.into_text().unwrap();
    let parsed: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["accepted"], 1);

    // And the event should be visible in the hot ring.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let event = h.hot_rings.latest("cam.front", "object_detected").unwrap();
    assert_eq!(event.kind, "object_detected");
}

#[tokio::test]
async fn websocket_invalid_bearer_token_is_closed() {
    let h = spawn(permissive()).await;
    let url = format!("{}/ingest/ws?token=wrong", h.base_ws);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    // Server should send a Close frame then drop the connection.
    let next = ws.next().await.unwrap();
    match next {
        Ok(Message::Close(Some(f))) => {
            assert_eq!(
                f.code,
                tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy
            );
        }
        other => panic!("expected Close, got: {other:?}"),
    }
}

#[tokio::test]
async fn websocket_scope_deny_returns_shed_reason() {
    let scope = TokenScope::build(
        "test",
        &["allowed.*".into()],
        &["object_detected".into()],
        None,
    )
    .unwrap();
    let h = spawn(scope).await;
    let url = format!("{}/ingest/ws?token={TOKEN}", h.base_ws);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let body = json!({
        "source_id": "denied.src",
        "kind": "object_detected",
        "ts_ms_utc": 0,
        "semantic": {}
    });
    ws.send(Message::Text(body.to_string())).await.unwrap();
    let reply = ws.next().await.unwrap().unwrap().into_text().unwrap();
    let parsed: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(parsed["shed_reason"], "unauthorized");
}

#[tokio::test]
async fn websocket_batch_form_accepts_all() {
    let h = spawn(permissive()).await;
    let url = format!("{}/ingest/ws?token={TOKEN}", h.base_ws);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let body = json!({
        "events": [
            { "source_id": "a", "kind": "k", "ts_ms_utc": 0, "semantic": {} },
            { "source_id": "b", "kind": "k", "ts_ms_utc": 0, "semantic": {} },
        ]
    });
    ws.send(Message::Text(body.to_string())).await.unwrap();
    let reply = ws.next().await.unwrap().unwrap().into_text().unwrap();
    let parsed: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(parsed["accepted"], 2);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(h.hot_rings.latest("a", "k").is_some());
    assert!(h.hot_rings.latest("b", "k").is_some());
}

#[tokio::test]
async fn websocket_malformed_json_returns_error_frame() {
    let h = spawn(permissive()).await;
    let url = format!("{}/ingest/ws?token={TOKEN}", h.base_ws);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.send(Message::Text("{ not json".into())).await.unwrap();
    let reply = ws.next().await.unwrap().unwrap().into_text().unwrap();
    let parsed: Value = serde_json::from_str(&reply).unwrap();
    assert!(parsed["error"].as_str().unwrap().contains("invalid JSON"));
}
