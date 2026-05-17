//! JSON-RPC 2.0 + MCP wire types for the methods we serve.
//!
//! Wire format is camelCase per MCP convention; Rust fields stay snake_case.

use serde::{Deserialize, Serialize};

/// Protocol version we advertise on `initialize`. Matches the spec's
/// 2024-11-05 revision (the current widely-supported version).
pub const PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Debug, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<serde_json::Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
    #[serde(flatten)]
    pub payload: ResponsePayload,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResponsePayload {
    Ok { result: serde_json::Value },
    Err { error: RpcError },
}

#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl RpcError {
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self {
            code: -32_700,
            message: msg.into(),
            data: None,
        }
    }
    pub fn invalid_request(msg: impl Into<String>) -> Self {
        Self {
            code: -32_600,
            message: msg.into(),
            data: None,
        }
    }
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: -32_601,
            message: format!("method not found: {method}"),
            data: None,
        }
    }
    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self {
            code: -32_602,
            message: msg.into(),
            data: None,
        }
    }
}

impl Response {
    pub fn ok(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            payload: ResponsePayload::Ok { result },
        }
    }
    pub fn err(id: Option<serde_json::Value>, error: RpcError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            payload: ResponsePayload::Err { error },
        }
    }
}

// --- initialize ---

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol_version: &'static str,
    pub capabilities: ServerCapabilities,
    pub server_info: ServerInfo,
}

#[derive(Debug, Serialize)]
pub struct ServerCapabilities {
    pub tools: ToolsCapability,
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ToolsCapability {
    pub list_changed: bool,
}

#[derive(Debug, Serialize)]
pub struct ServerInfo {
    pub name: &'static str,
    pub version: &'static str,
}

// --- tools/list ---

#[derive(Debug, Serialize)]
pub struct ToolsListResult {
    pub tools: Vec<Tool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: serde_json::Value,
}

// --- tools/call ---

#[derive(Debug, Deserialize)]
pub struct CallParams {
    pub name: String,
    #[serde(default)]
    pub arguments: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallResult {
    pub content: Vec<ContentBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<serde_json::Value>,
    pub is_error: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ContentBlock {
    Text { text: String },
}

impl CallResult {
    pub fn structured(value: serde_json::Value) -> Self {
        let pretty = serde_json::to_string(&value).unwrap_or_default();
        Self {
            content: vec![ContentBlock::Text { text: pretty }],
            structured_content: Some(value),
            is_error: false,
        }
    }
}
