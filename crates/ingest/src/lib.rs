//! Ingest adapters and the normalizer.
//!
//! Slice 1: HTTP `/ingest`, per-token authn + scope, rate limit, normalizer,
//! `/healthz`, `/metrics`.
//! Slice 6: MQTT subscriber, WebSocket ingest endpoint.

pub mod auth;
pub mod http;
pub mod metrics;
pub mod mqtt;
pub mod normalizer;
pub mod pipeline;
pub mod schema;
pub mod wire;
pub mod ws;

pub use auth::{Auth, ShedReason, TokenScope};
pub use http::router;
pub use metrics::{Metrics, RecentErrors};
pub use normalizer::Normalizer;
pub use pipeline::{Pipeline, PipelineConfig};
pub use schema::{SchemaError, SchemaIndex};
pub use wire::{IngestEvent, IngestPayload};
