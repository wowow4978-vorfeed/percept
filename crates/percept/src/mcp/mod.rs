//! Minimal MCP server (Streamable HTTP transport, single-response mode).
//!
//! Hand-rolled rather than via the `rmcp` crate: the wire surface we need
//! is small (`initialize`, `tools/list`, `tools/call`), and `rmcp`'s API has
//! seen non-trivial churn across 0.x releases. The cost of one ~300-line
//! module is lower than the cost of tracking that churn. If we later need
//! SSE-streamed responses, session resumability, or sampling, switching
//! to `rmcp` is mechanical — the tool implementations themselves are
//! transport-agnostic.

mod protocol;
mod registry;
mod router;
mod tools;

pub use registry::DescriptorRegistry;
pub use router::{router, McpState};
