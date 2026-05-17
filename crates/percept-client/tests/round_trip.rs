//! Slice-7 acceptance: round-trip through the real server.
//!
//! Spawns the live ingest router in-process, builds a `percept-client`
//! pointed at it, posts events, and asserts on (a) the hot-ring landing,
//! (b) auth / shed-reason error mapping, (c) retry-after backoff.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use percept_client::{Batcher, BatcherConfig, Client, ClientConfig, ClientError, Event};
use percept_ingest::{router, Auth, Pipeline, PipelineConfig, SchemaIndex, TokenScope};
use serde_json::json;

const TOKEN: &str = "client-token-xyz";

struct Harness {
    base: String,
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
        PipelineConfig::default(),
    );
    let app = router(pipeline.http_state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Harness {
        base: format!("http://{addr}"),
        hot_rings: pipeline.hot_rings,
        _server: server,
    }
}

fn permissive() -> TokenScope {
    TokenScope::build("test", &["*".into()], &["*".into()], None).unwrap()
}

#[tokio::test]
async fn send_one_lands_in_hot_ring() {
    let h = spawn(permissive()).await;
    let client = Client::new(&h.base, TOKEN);
    let ev = Event::new(
        "cam.front",
        "object_detected",
        percept_core::now_ms_utc(),
        json!({ "label": "person" }),
    );
    let accepted = client.send_one(ev).await.unwrap();
    assert_eq!(accepted, 1);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let event = h.hot_rings.latest("cam.front", "object_detected").unwrap();
    assert_eq!(event.kind, "object_detected");
}

#[tokio::test]
async fn send_batch_accepts_all() {
    let h = spawn(permissive()).await;
    let client = Client::new(&h.base, TOKEN);
    let evs = vec![
        Event::new("a", "k", 0, json!({})),
        Event::new("b", "k", 0, json!({})),
        Event::new("c", "k", 0, json!({})),
    ];
    let accepted = client.send_batch(&evs).await.unwrap();
    assert_eq!(accepted, 3);
}

#[tokio::test]
async fn invalid_token_returns_unauthorized() {
    let h = spawn(permissive()).await;
    let client = Client::new(&h.base, "wrong-token");
    let err = client
        .send_one(Event::new("s", "k", 0, json!({})))
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Unauthorized));
}

#[tokio::test]
async fn scope_deny_returns_scope_deny() {
    let scope = TokenScope::build("test", &["allowed.*".into()], &["k".into()], None).unwrap();
    let h = spawn(scope).await;
    let client = Client::new(&h.base, TOKEN);
    let err = client
        .send_one(Event::new("denied.src", "k", 0, json!({})))
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::ScopeDeny), "got: {err:?}");
}

#[tokio::test]
async fn rate_limit_is_retried_then_succeeds() {
    // 1/s rate-limit: first POST consumes the token, second hits 429
    // with Retry-After. The SDK should sleep and retry until it lands.
    let scope = TokenScope::build("test", &["*".into()], &["*".into()], Some("1/s")).unwrap();
    let h = spawn(scope).await;

    let cfg = ClientConfig {
        max_attempts: 6,
        default_retry_after: Duration::from_millis(100),
        max_retry_after: Duration::from_secs(5),
        ..ClientConfig::default()
    };
    let client = Client::new(&h.base, TOKEN);

    // First call consumes the budget.
    let first = client.send_one(Event::new("s", "k", 0, json!({}))).await;
    assert!(first.is_ok());

    let started = Instant::now();
    let client2 = Client::with_config(&h.base, TOKEN, cfg);
    let second = client2.send_one(Event::new("s", "k", 0, json!({}))).await;
    // Should ultimately succeed after one or more backoff sleeps.
    assert!(second.is_ok(), "second send failed: {second:?}");
    // And it should have taken non-trivial time (at least one backoff).
    assert!(
        started.elapsed() >= Duration::from_millis(80),
        "elapsed {:?} suggests retry didn't happen",
        started.elapsed()
    );
}

#[tokio::test]
async fn batcher_drains_into_hot_ring() {
    let h = spawn(permissive()).await;
    let client = Arc::new(Client::new(&h.base, TOKEN));
    let batcher = Batcher::spawn(
        client,
        BatcherConfig {
            max_batch: 4,
            flush_interval: Duration::from_millis(50),
            queue_depth: 64,
        },
    );

    for i in 0..7 {
        batcher
            .enqueue(Event::new(
                "src",
                "k",
                percept_core::now_ms_utc(),
                json!({ "i": i }),
            ))
            .await
            .unwrap();
    }
    // Wait for the flush ticker.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap = h.hot_rings.snapshot("src", "k");
    assert_eq!(snap.events.len(), 7);
}
