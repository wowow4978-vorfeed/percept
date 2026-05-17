//! Axum router for `POST /mcp`. Single-response Streamable HTTP — no SSE.

use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use percept_ingest::Metrics;
use percept_store::{ColdStore, Embedder, HotRings, RetentionPolicy, VectorIndex};
use serde_json::json;

use super::peer::PeerHandle;
use super::protocol::{
    CallParams, CallResult, InitializeResult, Request, Response as RpcResponse, RpcError,
    ServerCapabilities, ServerInfo, Tool, ToolsCapability, ToolsListResult, PROTOCOL_VERSION,
};
use super::registry::DescriptorRegistry;
use super::tools;

#[derive(Clone)]
pub struct McpState {
    pub token: Arc<String>,
    pub registry: Arc<DescriptorRegistry>,
    pub hot_rings: Arc<HotRings>,
    pub cold_store: Option<Arc<ColdStore>>,
    pub vector_index: Option<Arc<VectorIndex>>,
    pub embedder: Option<Arc<dyn Embedder>>,
    pub retention_policies: Arc<Vec<RetentionPolicy>>,
    pub peers: Arc<Vec<PeerHandle>>,
    pub metrics: Arc<Metrics>,
}

pub fn router(state: McpState) -> Router {
    Router::new().route("/mcp", post(handle)).with_state(state)
}

async fn handle(headers: HeaderMap, State(s): State<McpState>, body: Bytes) -> Response {
    if let Err(resp) = authenticate(&s.token, &headers) {
        return resp;
    }

    let req: Request = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return jsonrpc(RpcResponse::err(
                None,
                RpcError::parse_error(format!("invalid JSON-RPC envelope: {e}")),
            ));
        }
    };

    if req.jsonrpc != "2.0" {
        return jsonrpc(RpcResponse::err(
            req.id,
            RpcError::invalid_request("jsonrpc must be \"2.0\""),
        ));
    }

    let started = Instant::now();
    let response = dispatch(&s, &req).await;
    record_metrics(&s.metrics, &req.method, started);
    jsonrpc(response)
}

async fn dispatch(s: &McpState, req: &Request) -> RpcResponse {
    let id = req.id.clone();
    match req.method.as_str() {
        "initialize" => {
            let value = serde_json::to_value(InitializeResult {
                protocol_version: PROTOCOL_VERSION,
                capabilities: ServerCapabilities {
                    tools: ToolsCapability::default(),
                },
                server_info: ServerInfo {
                    name: "percept",
                    version: env!("CARGO_PKG_VERSION"),
                },
            })
            .expect("serializable");
            RpcResponse::ok(id, value)
        }
        "tools/list" => {
            let value = serde_json::to_value(ToolsListResult {
                tools: vec![
                    Tool {
                        name: "describe_sources",
                        description: "What sensors are available, what they mean, and \
                             which are currently misbehaving. Returns merged \
                             Source+Kind descriptors plus a recent_errors \
                             digest per source.",
                        input_schema: tools::describe_sources_input_schema(),
                    },
                    Tool {
                        name: "get_current_state",
                        description: "Latest event per (source, kind) from the hot \
                             ring. Each entry carries age_ms and a stale flag \
                             derived from the descriptor's freshness_ttl_ms. \
                             Falls back to the cold store on a hot-ring miss; \
                             those rows are tagged from_cold = true.",
                        input_schema: tools::get_current_state_input_schema(),
                    },
                    Tool {
                        name: "get_window",
                        description: "Time-range scan from the cold store. \
                             Paginated via an opaque cursor. Ordered by \
                             (ts_ms_utc, event_id) ascending. Per-call hard \
                             limit 10,000 events.",
                        input_schema: tools::get_window_input_schema(),
                    },
                    Tool {
                        name: "search_events",
                        description: "Semantic search via the vector index, \
                             with optional time / source / kind filters. \
                             Returns the top-k hits with similarity scores \
                             and the matching events. Requires the embedder \
                             to be configured (see [storage].embed_default \
                             and per-kind / per-source `embed`).",
                        input_schema: tools::search_events_input_schema(),
                    },
                ],
            })
            .expect("serializable");
            RpcResponse::ok(id, value)
        }
        "tools/call" => dispatch_call(s, id, req.params.clone()).await,
        "notifications/initialized" | "notifications/cancelled" => {
            // Notifications carry no id and don't expect a response; we
            // return an empty Ok for transports that send them as a
            // request shape.
            RpcResponse::ok(id, json!(null))
        }
        "ping" => RpcResponse::ok(id, json!({})),
        other => RpcResponse::err(id, RpcError::method_not_found(other)),
    }
}

async fn dispatch_call(
    s: &McpState,
    id: Option<serde_json::Value>,
    params: Option<serde_json::Value>,
) -> RpcResponse {
    let Some(params) = params else {
        return RpcResponse::err(id, RpcError::invalid_params("missing params"));
    };
    let call: CallParams = match serde_json::from_value(params) {
        Ok(c) => c,
        Err(e) => return RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
    };
    let args = call.arguments.unwrap_or_else(|| json!({}));
    match call.name.as_str() {
        "describe_sources" => {
            let parsed = match serde_json::from_value(args) {
                Ok(a) => a,
                Err(e) => return RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            };
            match tools::describe_sources(
                &s.registry,
                &s.metrics,
                &s.retention_policies,
                &s.peers,
                parsed,
            )
            .await
            {
                Ok(v) => RpcResponse::ok(
                    id,
                    serde_json::to_value(CallResult::structured(v)).expect("serializable"),
                ),
                Err(e) => RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            }
        }
        "get_current_state" => {
            let parsed = match serde_json::from_value(args) {
                Ok(a) => a,
                Err(e) => return RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            };
            match tools::get_current_state(
                &s.registry,
                &s.hot_rings,
                s.cold_store.as_deref(),
                &s.peers,
                parsed,
            )
            .await
            {
                Ok(v) => RpcResponse::ok(
                    id,
                    serde_json::to_value(CallResult::structured(v)).expect("serializable"),
                ),
                Err(e) => RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            }
        }
        "get_window" => {
            let parsed = match serde_json::from_value(args) {
                Ok(a) => a,
                Err(e) => return RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            };
            match tools::get_window(s.cold_store.as_deref(), parsed) {
                Ok(v) => RpcResponse::ok(
                    id,
                    serde_json::to_value(CallResult::structured(v)).expect("serializable"),
                ),
                Err(e) => RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            }
        }
        "search_events" => {
            let parsed = match serde_json::from_value(args) {
                Ok(a) => a,
                Err(e) => return RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            };
            match tools::search_events(
                s.vector_index.as_deref(),
                s.embedder.as_deref(),
                s.cold_store.as_deref(),
                parsed,
            ) {
                Ok(v) => RpcResponse::ok(
                    id,
                    serde_json::to_value(CallResult::structured(v)).expect("serializable"),
                ),
                Err(e) => RpcResponse::err(id, RpcError::invalid_params(e.to_string())),
            }
        }
        other => RpcResponse::err(id, RpcError::method_not_found(other)),
    }
}

fn jsonrpc(r: RpcResponse) -> Response {
    (StatusCode::OK, axum::Json(r)).into_response()
}

fn authenticate(expected: &str, headers: &HeaderMap) -> Result<(), Response> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let Some(token) = value else {
        return Err((StatusCode::UNAUTHORIZED, "missing bearer token").into_response());
    };
    if token != expected {
        return Err((StatusCode::UNAUTHORIZED, "unknown bearer token").into_response());
    }
    Ok(())
}

fn record_metrics(metrics: &Metrics, method: &str, started: Instant) {
    let elapsed_ms = started.elapsed().as_millis() as u64;
    // Reuse the per-token-shed shape: each tool/method gets its own
    // counter via the `inc_shed` map (reason = "mcp:<method>"). Crude but
    // works without a new field on Metrics.
    metrics.inc_mcp_call(method, elapsed_ms);
}
