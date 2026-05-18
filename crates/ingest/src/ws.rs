//! WebSocket ingest endpoint. Same wire format as HTTP `/ingest` —
//! `IngestPayload` (single object, `{"events": [...]}`, or bare array)
//! per text message. Bearer-token auth via the `Authorization` query
//! parameter or `Sec-WebSocket-Protocol` (WebSocket clients can't set
//! arbitrary headers from a browser).
//!
//! DESIGN §5.1: "WebSocket — long-lived producer streams; same shape as
//! HTTP batch."

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::auth::{ShedReason, TokenScope};
use crate::http::HttpState;
use crate::normalizer::IngestEnvelope;
use crate::wire::IngestPayload;

#[derive(Debug, Deserialize)]
pub struct WsAuth {
    /// Bearer token in the query string — the only auth route a browser
    /// WebSocket client can use without server cooperation.
    pub token: String,
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(auth): Query<WsAuth>,
    State(state): State<HttpState>,
) -> Response {
    let scope = state.auth.lookup(&auth.token);
    ws.on_upgrade(move |socket| async move {
        match scope {
            Some(s) => run_session(socket, state, s).await,
            None => {
                // Best-effort: send a close frame with a 1008 (policy
                // violation) reason before dropping.
                let mut s = socket;
                let _ = s
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1008,
                        reason: "unknown bearer token".into(),
                    })))
                    .await;
            }
        }
    })
}

async fn run_session(mut socket: WebSocket, state: HttpState, scope: Arc<TokenScope>) {
    while let Some(msg) = socket.recv().await {
        let Ok(msg) = msg else {
            // Network error; treat as session end.
            return;
        };
        let payload_bytes = match msg {
            Message::Text(t) => t.into_bytes(),
            Message::Binary(b) => b,
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return,
        };
        let payload: IngestPayload = match serde_json::from_slice(&payload_bytes) {
            Ok(p) => p,
            Err(e) => {
                send_error(&mut socket, &format!("invalid JSON: {e}")).await;
                continue;
            }
        };
        let events = payload.into_events();
        if events.is_empty() {
            send_error(&mut socket, "no events in payload").await;
            continue;
        }
        let now_ms = percept_core::now_ms_utc();
        let mut rejected: Option<ShedReason> = None;
        for e in &events {
            if !scope.allows(&e.source_id, &e.kind) {
                state
                    .metrics
                    .inc_shed(ShedReason::Unauthorized.as_str(), Some(&scope.name));
                state.metrics.inc_source_error(
                    &e.source_id,
                    ShedReason::Unauthorized.as_str(),
                    now_ms,
                );
                rejected = Some(ShedReason::Unauthorized);
                break;
            }
            if approx_semantic_size(&e.semantic) > state.hard_cap_bytes {
                state
                    .metrics
                    .inc_shed(ShedReason::PayloadTooLarge.as_str(), Some(&scope.name));
                state.metrics.inc_source_error(
                    &e.source_id,
                    ShedReason::PayloadTooLarge.as_str(),
                    now_ms,
                );
                rejected = Some(ShedReason::PayloadTooLarge);
                break;
            }
        }
        if let Some(r) = rejected {
            send_shed(&mut socket, r).await;
            continue;
        }
        if let Some(wait) = scope.check_rate() {
            state
                .metrics
                .inc_shed(ShedReason::RateLimit.as_str(), Some(&scope.name));
            for e in &events {
                state.metrics.inc_source_error(
                    &e.source_id,
                    ShedReason::RateLimit.as_str(),
                    now_ms,
                );
            }
            let _ = wait; // No Retry-After on WS; the shed_reason frame is enough.
            send_shed(&mut socket, ShedReason::RateLimit).await;
            continue;
        }
        let mut accepted = 0;
        for e in events {
            let envelope = IngestEnvelope {
                event: e,
                token_name: Some(scope.name.clone()),
            };
            match state.tx.try_send(envelope) {
                Ok(()) => accepted += 1,
                Err(mpsc::error::TrySendError::Full(env)) => {
                    state
                        .metrics
                        .inc_shed(ShedReason::BusFull.as_str(), Some(&scope.name));
                    state.metrics.inc_source_error(
                        &env.event.source_id,
                        ShedReason::BusFull.as_str(),
                        now_ms,
                    );
                    send_shed(&mut socket, ShedReason::BusFull).await;
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
        if accepted > 0 {
            let body = serde_json::json!({ "accepted": accepted });
            let _ = socket.send(Message::Text(body.to_string())).await;
        }
    }
}

async fn send_error(socket: &mut WebSocket, msg: &str) {
    let body = serde_json::json!({ "error": msg });
    let _ = socket.send(Message::Text(body.to_string())).await;
}

async fn send_shed(socket: &mut WebSocket, reason: ShedReason) {
    let body = serde_json::json!({
        "shed_reason": reason.as_str(),
    });
    let _ = socket.send(Message::Text(body.to_string())).await;
}

/// Reuse the HTTP path's size estimator — kept in sync by re-deriving
/// here rather than reaching into a private function.
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
