//! End-to-end HTTP tests against the in-process Pipeline.
//!
//! Each test spawns a Pipeline, binds an OS-assigned port, fires real HTTP
//! requests with `reqwest`, and inspects the resulting hot-ring state and
//! metrics counters.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use percept_ingest::{router, Auth, Pipeline, PipelineConfig, SchemaIndex, TokenScope};
use percept_store::HotRings;
use serde_json::json;

const TOKEN: &str = "test-token-abc";

struct Harness {
    base: String,
    hot_rings: Arc<HotRings>,
    metrics: Arc<percept_ingest::Metrics>,
    _server: tokio::task::JoinHandle<()>,
}

async fn spawn_harness(config: PipelineConfig, scope: TokenScope) -> Harness {
    spawn_harness_with_schemas(config, scope, Arc::new(SchemaIndex::default())).await
}

async fn spawn_harness_with_schemas(
    config: PipelineConfig,
    scope: TokenScope,
    schemas: Arc<SchemaIndex>,
) -> Harness {
    let mut auth = Auth::new();
    auth.insert(TOKEN.to_string(), scope);
    // No cold store for slice-1 ingest tests — they only care about the
    // hot-path. Cold-fallback coverage lives in tests/mcp_e2e.rs.
    let pipeline = Pipeline::spawn(Arc::new(auth), schemas, None, config);
    let app = router(pipeline.http_state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    Harness {
        base: format!("http://{addr}"),
        hot_rings: pipeline.hot_rings,
        metrics: pipeline.metrics,
        _server: server,
    }
}

fn permissive_scope() -> TokenScope {
    TokenScope::build("test", &["*".into()], &["*".into()], None).unwrap()
}

fn event_body(source: &str, kind: &str) -> serde_json::Value {
    json!({
        "source_id": source,
        "kind": kind,
        "ts_ms_utc": 1_700_000_000_000_i64,
        "semantic": { "v": 1 }
    })
}

#[tokio::test]
async fn post_event_lands_in_hot_ring_within_100ms() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let client = reqwest::Client::new();

    let start = Instant::now();
    let resp = client
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&event_body("cam.front", "object_detected"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Wait up to 100ms for the normalizer to drain.
    let deadline = start + Duration::from_millis(100);
    loop {
        if let Some(e) = h.hot_rings.latest("cam.front", "object_detected") {
            assert_eq!(e.source_id, "cam.front");
            assert_eq!(e.kind, "object_detected");
            assert!(e.event_id.timestamp_ms() > 0);
            assert!(e.ingest_ts_ms_utc.is_some());
            assert_eq!(e.seq, Some(1));
            assert!(e.trace_id.is_some());
            return;
        }
        if Instant::now() > deadline {
            panic!("event never reached the hot ring within 100ms");
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test]
async fn missing_bearer_token_returns_401() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .json(&event_body("s", "k"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn invalid_bearer_token_returns_401() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth("wrong")
        .json(&event_body("s", "k"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn scope_deny_returns_429_unauthorized() {
    let scope = TokenScope::build(
        "test",
        &["allowed.*".into()],
        &["object_detected".into()],
        None,
    )
    .unwrap();
    let h = spawn_harness(PipelineConfig::default(), scope).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&event_body("denied.source", "object_detected"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 429);
    assert_eq!(
        resp.headers().get("x-percept-shed-reason").unwrap(),
        "unauthorized"
    );
    assert!(h.metrics.shed_count("unauthorized") >= 1);
}

#[tokio::test]
async fn payload_too_large_returns_429() {
    let cfg = PipelineConfig {
        hard_cap_bytes: 1_000,
        ..PipelineConfig::default()
    };
    let h = spawn_harness(cfg, permissive_scope()).await;

    let huge = "x".repeat(2_000);
    let body = json!({
        "source_id": "s",
        "kind": "k",
        "ts_ms_utc": 0_i64,
        "semantic": { "blob": huge }
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 429);
    assert_eq!(
        resp.headers().get("x-percept-shed-reason").unwrap(),
        "payload_too_large"
    );
    assert!(h.metrics.shed_count("payload_too_large") >= 1);
}

#[tokio::test]
async fn soft_cap_accepts_but_increments_counter() {
    let cfg = PipelineConfig {
        soft_cap_bytes: 100,
        hard_cap_bytes: 64 * 1024,
        ..PipelineConfig::default()
    };
    let h = spawn_harness(cfg, permissive_scope()).await;

    let medium = "x".repeat(500);
    let body = json!({
        "source_id": "s",
        "kind": "k",
        "ts_ms_utc": 0_i64,
        "semantic": { "blob": medium }
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert!(
        h.metrics
            .oversized_soft_total
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
}

#[tokio::test]
async fn bus_full_returns_429_with_retry_after() {
    // bus depth 1, no rate limit; queue a backlog with the normalizer paused
    // by holding the runtime busy. Easiest reliable trigger: depth 1 and
    // many concurrent senders.
    let cfg = PipelineConfig {
        bus_depth: 1,
        ..PipelineConfig::default()
    };
    let h = spawn_harness(cfg, permissive_scope()).await;
    let client = reqwest::Client::new();

    // Send a batch large enough to fill the channel inside the handler
    // (the handler does try_send per event; once the channel is full,
    // subsequent events in the same batch return bus_full).
    let events: Vec<_> = (0..256).map(|_| event_body("s", "k")).collect();
    let body = json!({ "events": events });
    let mut saw_bus_full = false;
    for _ in 0..20 {
        let resp = client
            .post(format!("{}/ingest", h.base))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        if resp.status().as_u16() == 429
            && resp
                .headers()
                .get("x-percept-shed-reason")
                .map(|v| v.as_bytes())
                == Some(b"bus_full")
        {
            assert!(resp.headers().contains_key("retry-after"));
            saw_bus_full = true;
            break;
        }
    }
    assert!(saw_bus_full, "expected at least one bus_full 429");
    assert!(h.metrics.shed_count("bus_full") >= 1);
}

#[tokio::test]
async fn rate_limit_returns_429_with_retry_after() {
    let scope = TokenScope::build("test", &["*".into()], &["*".into()], Some("1/s")).unwrap();
    let h = spawn_harness(PipelineConfig::default(), scope).await;
    let client = reqwest::Client::new();

    let mut saw = false;
    for _ in 0..20 {
        let resp = client
            .post(format!("{}/ingest", h.base))
            .bearer_auth(TOKEN)
            .json(&event_body("s", "k"))
            .send()
            .await
            .unwrap();
        if resp.status().as_u16() == 429
            && resp
                .headers()
                .get("x-percept-shed-reason")
                .map(|v| v.as_bytes())
                == Some(b"rate_limit")
        {
            assert!(resp.headers().contains_key("retry-after"));
            saw = true;
            break;
        }
    }
    assert!(saw, "expected a rate_limit 429 after burst");
    assert!(h.metrics.shed_count("rate_limit") >= 1);
}

#[tokio::test]
async fn healthz_returns_ok() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let resp = reqwest::get(format!("{}/healthz", h.base)).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn metrics_exposes_counters() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let client = reqwest::Client::new();
    client
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&event_body("s", "k"))
        .send()
        .await
        .unwrap();
    let body = reqwest::get(format!("{}/metrics", h.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("percept_accepted_total"));
}

#[tokio::test]
async fn batch_via_events_array_accepts_all() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let body = json!({
        "events": [
            event_body("a", "k"),
            event_body("b", "k"),
            event_body("a", "j"),
        ]
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Drain.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(h.hot_rings.latest("a", "k").is_some());
    assert!(h.hot_rings.latest("b", "k").is_some());
    assert!(h.hot_rings.latest("a", "j").is_some());
}

#[tokio::test]
async fn malformed_json_returns_400() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .header("content-type", "application/json")
        .body("not-json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
}

#[tokio::test]
async fn per_source_seq_is_monotonic_per_source() {
    let h = spawn_harness(PipelineConfig::default(), permissive_scope()).await;
    let client = reqwest::Client::new();
    for _ in 0..5 {
        client
            .post(format!("{}/ingest", h.base))
            .bearer_auth(TOKEN)
            .json(&event_body("src.a", "k"))
            .send()
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let snap = h.hot_rings.snapshot("src.a", "k");
    assert_eq!(snap.events.len(), 5);
    for (i, e) in snap.events.iter().enumerate() {
        assert_eq!(e.seq, Some((i + 1) as u64));
    }
}

fn kind_descriptor(
    name: &str,
    version: &str,
    schema: serde_json::Value,
) -> percept_core::KindDescriptor {
    percept_core::KindDescriptor {
        kind: name.to_string(),
        version: version.to_string(),
        description: String::new(),
        usage: String::new(),
        caveats: String::new(),
        semantic_schema: Some(schema),
        units: None,
        updated_ts_ms_utc: 0,
    }
}

#[tokio::test]
async fn invalid_semantic_sets_schema_invalid_and_increments_counter() {
    // Schema requires { "celsius": <number> }; producer sends garbage.
    let kind = kind_descriptor(
        "temperature",
        "v1",
        json!({
            "type": "object",
            "required": ["celsius"],
            "properties": { "celsius": { "type": "number" } }
        }),
    );
    let schemas = Arc::new(SchemaIndex::build(&[], &[kind]).unwrap());
    let h =
        spawn_harness_with_schemas(PipelineConfig::default(), permissive_scope(), schemas).await;

    let bad_body = json!({
        "source_id": "therm.kitchen",
        "kind": "temperature",
        "ts_ms_utc": 0_i64,
        "semantic": { "fahrenheit": 70 }
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&bad_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "invalid schema is soft-fail");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let e = h
        .hot_rings
        .latest("therm.kitchen", "temperature")
        .expect("event landed");
    assert_eq!(
        e.schema_invalid,
        Some(true),
        "_schema_invalid should be set on a payload that violates the schema"
    );
    assert_eq!(
        h.metrics
            .schema_invalid_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn valid_semantic_leaves_schema_invalid_unset() {
    let kind = kind_descriptor(
        "temperature",
        "v1",
        json!({
            "type": "object",
            "required": ["celsius"],
            "properties": { "celsius": { "type": "number" } }
        }),
    );
    let schemas = Arc::new(SchemaIndex::build(&[], &[kind]).unwrap());
    let h =
        spawn_harness_with_schemas(PipelineConfig::default(), permissive_scope(), schemas).await;

    let good = json!({
        "source_id": "therm.kitchen",
        "kind": "temperature",
        "ts_ms_utc": 0_i64,
        "semantic": { "celsius": 20.5 }
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/ingest", h.base))
        .bearer_auth(TOKEN)
        .json(&good)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    tokio::time::sleep(Duration::from_millis(50)).await;
    let e = h.hot_rings.latest("therm.kitchen", "temperature").unwrap();
    assert_eq!(e.schema_invalid, None);
    assert_eq!(
        h.metrics
            .schema_invalid_total
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}
