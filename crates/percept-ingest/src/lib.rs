//! Ingest adapters and the normalizer.
//!
//! Slice 1: HTTP `/ingest`, per-token authn + scope, rate limit, normalizer,
//! `/healthz`, `/metrics`. Additional adapters (MQTT, WS, BLE) land in slice 6.

pub mod auth;
pub mod http;
pub mod metrics;
pub mod normalizer;
pub mod pipeline;
pub mod wire;

pub use auth::{Auth, ShedReason, TokenScope};
pub use http::router;
pub use metrics::Metrics;
pub use normalizer::Normalizer;
pub use pipeline::{Pipeline, PipelineConfig};
pub use wire::{IngestEvent, IngestPayload};
