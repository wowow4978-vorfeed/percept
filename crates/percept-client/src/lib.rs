//! Producer SDK for Percept's HTTP `/ingest` endpoint.
//!
//! - `Client` — async, batched send with retry on `429`/`503` honouring
//!   `Retry-After`, gzip on the wire, bearer-token auth.
//! - `Batcher` — queue + background flush wrapper around `Client`.
//! - `BlockingClient` — blocking variant for embedded producers; gated
//!   behind the `blocking` cargo feature.
//!
//! Same wire shape the server accepts (see DESIGN §3.1 / §5.1). The
//! SDK always uses the `{"events": [...]}` batch form for uniformity.

mod batcher;
#[cfg(feature = "blocking")]
pub mod blocking;
mod client;
mod error;
mod event;

pub use batcher::{Batcher, BatcherConfig};
pub use client::{Client, ClientConfig, SHED_REASON_HEADER};
pub use error::ClientError;
pub use event::{Envelope, Event};
