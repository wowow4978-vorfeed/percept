//! Federation peer handle + MCP tool-call client.
//!
//! Each `[[peer]]` config entry resolves to a `PeerHandle`. When the
//! local MCP server runs `describe_sources` or `get_current_state`, it
//! also fans out to every peer in parallel (subject to each peer's
//! `timeout`) and merges the results.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct PeerHandle {
    pub id: String,
    pub mcp_url: String,
    pub token: String,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PeerStatus {
    Ok,
    Timeout,
    Error { message: String },
}

/// Run a `tools/call` against a peer with a hard per-peer timeout.
/// Returns the structured tool result as JSON, or an error variant the
/// caller can fold into the federation report.
pub async fn call_tool(
    client: &reqwest::Client,
    peer: &PeerHandle,
    tool_name: &str,
    arguments: Value,
) -> (PeerStatus, Option<Value>) {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": { "name": tool_name, "arguments": arguments },
    });
    let fut = client
        .post(&peer.mcp_url)
        .bearer_auth(&peer.token)
        .json(&body)
        .send();
    let resp = match tokio::time::timeout(peer.timeout, fut).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            return (
                PeerStatus::Error {
                    message: e.to_string(),
                },
                None,
            )
        }
        Err(_) => return (PeerStatus::Timeout, None),
    };
    if !resp.status().is_success() {
        return (
            PeerStatus::Error {
                message: format!("peer returned {}", resp.status()),
            },
            None,
        );
    }
    // Unwrap the JSON-RPC envelope + the tool's CallResult.
    let envelope: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            return (
                PeerStatus::Error {
                    message: e.to_string(),
                },
                None,
            )
        }
    };
    if let Some(err) = envelope.get("error") {
        return (
            PeerStatus::Error {
                message: err.to_string(),
            },
            None,
        );
    }
    let structured = envelope
        .get("result")
        .and_then(|r| r.get("structuredContent"))
        .cloned();
    (PeerStatus::Ok, structured)
}
