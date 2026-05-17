//! HTTP listener for `/ingest`, `/healthz`, `/metrics`.
//!
//! Per DESIGN §5.3 a rejected event surfaces an `X-Percept-Shed-Reason`
//! response header. Slice 1 status-code mapping:
//!
//! - `400` malformed JSON
//! - `401` missing/invalid bearer token (no scope to evaluate)
//! - `429` everything else — rate-limit, bus-full, scope-deny (unauthorized),
//!   payload_too_large — with `Retry-After` set when applicable, per
//!   DESIGN §5.3.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::auth::{Auth, ShedReason, TokenScope};
use crate::metrics::Metrics;
use crate::normalizer::IngestEnvelope;
use crate::wire::IngestPayload;

/// 64 KiB per DESIGN §11.2.
pub const DEFAULT_HARD_CAP_BYTES: usize = 64 * 1024;
/// 16 KiB per DESIGN §11.2.
pub const DEFAULT_SOFT_CAP_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct HttpState {
    pub auth: Arc<Auth>,
    pub metrics: Arc<Metrics>,
    pub cold_writer_metrics: Option<Arc<percept_store::ColdWriterMetrics>>,
    pub tx: mpsc::Sender<IngestEnvelope>,
    pub hard_cap_bytes: usize,
    pub soft_cap_bytes: usize,
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/ingest", post(ingest))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn metrics(State(s): State<HttpState>) -> String {
    let mut out = s.metrics.render();
    if let Some(cm) = &s.cold_writer_metrics {
        cm.render_into(&mut out);
    }
    out
}

#[derive(Serialize)]
struct IngestOk {
    accepted: usize,
}

async fn ingest(headers: HeaderMap, State(s): State<HttpState>, body: Bytes) -> Response {
    let scope = match authenticate(&s.auth, &headers) {
        Ok(scope) => scope,
        Err(resp) => return resp,
    };

    let payload: IngestPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")).into_response();
        }
    };
    let events = payload.into_events();
    if events.is_empty() {
        return (StatusCode::BAD_REQUEST, "no events in payload").into_response();
    }

    // Per-event scope + size checks happen first; if any event fails, the
    // whole batch is rejected. (We don't partially-accept — keeps the
    // counter accounting honest for the producer.)
    let now_ms = percept_core::now_ms_utc();
    for e in &events {
        if !scope.allows(&e.source_id, &e.kind) {
            s.metrics
                .inc_shed(ShedReason::Unauthorized.as_str(), Some(&scope.name));
            s.metrics
                .inc_source_error(&e.source_id, ShedReason::Unauthorized.as_str(), now_ms);
            return shed_response(ShedReason::Unauthorized, None);
        }
        let size = approx_semantic_size(&e.semantic);
        if size > s.hard_cap_bytes {
            s.metrics
                .inc_shed(ShedReason::PayloadTooLarge.as_str(), Some(&scope.name));
            s.metrics
                .inc_source_error(&e.source_id, ShedReason::PayloadTooLarge.as_str(), now_ms);
            return shed_response(ShedReason::PayloadTooLarge, None);
        }
        if size > s.soft_cap_bytes {
            s.metrics.inc_oversized_soft();
        }
    }

    // Rate-limit once per batch (cheaper than per-event and a closer match
    // to producer intent: a batch is a single producer action).
    if let Some(wait) = scope.check_rate() {
        s.metrics
            .inc_shed(ShedReason::RateLimit.as_str(), Some(&scope.name));
        for e in &events {
            s.metrics
                .inc_source_error(&e.source_id, ShedReason::RateLimit.as_str(), now_ms);
        }
        return shed_response(ShedReason::RateLimit, Some(wait));
    }

    let mut accepted = 0;
    for e in events {
        let source_id = e.source_id.clone();
        let env = IngestEnvelope {
            event: e,
            token_name: Some(scope.name.clone()),
        };
        match s.tx.try_send(env) {
            Ok(()) => accepted += 1,
            Err(mpsc::error::TrySendError::Full(_)) => {
                s.metrics
                    .inc_shed(ShedReason::BusFull.as_str(), Some(&scope.name));
                s.metrics
                    .inc_source_error(&source_id, ShedReason::BusFull.as_str(), now_ms);
                return shed_response(ShedReason::BusFull, Some(Duration::from_millis(100)));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response();
            }
        }
    }

    (StatusCode::OK, axum::Json(IngestOk { accepted })).into_response()
}

fn authenticate(auth: &Auth, headers: &HeaderMap) -> Result<Arc<TokenScope>, Response> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let Some(token) = value else {
        return Err((StatusCode::UNAUTHORIZED, "missing bearer token").into_response());
    };
    auth.lookup(token)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "unknown bearer token").into_response())
}

fn shed_response(reason: ShedReason, retry_after: Option<Duration>) -> Response {
    let mut resp = (StatusCode::TOO_MANY_REQUESTS, reason.as_str().to_string()).into_response();
    resp.headers_mut().insert(
        "x-percept-shed-reason",
        HeaderValue::from_static(reason.as_str()),
    );
    if let Some(d) = retry_after {
        let secs = d.as_secs().max(1);
        if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
            resp.headers_mut().insert("retry-after", v);
        }
    }
    resp
}

/// Approximate the on-wire size of the `semantic` payload without doing a
/// full round-trip serialization. Counts strings + 2 bytes of quoting,
/// numbers/booleans as ~16 bytes, arrays/objects recursively.
fn approx_semantic_size(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(_) => 5,
        serde_json::Value::Number(_) => 16,
        serde_json::Value::String(s) => s.len() + 2,
        serde_json::Value::Array(a) => a.iter().map(approx_semantic_size).sum::<usize>() + 2,
        serde_json::Value::Object(o) => {
            o.iter()
                .map(|(k, vv)| k.len() + 4 + approx_semantic_size(vv))
                .sum::<usize>()
                + 2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn size_estimator_handles_nested() {
        let v = json!({ "label": "person", "boxes": [[1,2,3,4]] });
        assert!(approx_semantic_size(&v) > 10);
    }

    #[test]
    fn size_estimator_string_includes_quotes() {
        let v = serde_json::Value::String("a".repeat(1000));
        assert!(approx_semantic_size(&v) >= 1000);
    }
}
