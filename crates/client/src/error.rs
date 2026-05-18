//! Public error surface.

use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("network: {0}")]
    Network(#[from] reqwest::Error),

    #[error("serialization: {0}")]
    Serialize(#[from] serde_json::Error),

    /// 401 from the server. The bearer token isn't valid — retrying
    /// won't help.
    #[error("unauthorized: bearer token rejected by server")]
    Unauthorized,

    /// 4xx (other than 401/413) — bad request shape. Won't be retried.
    #[error("bad request ({status}): {body}")]
    BadRequest { status: u16, body: String },

    /// 413 Payload Too Large. Producer must move bulk to `links`.
    #[error("payload too large: server rejected the batch")]
    PayloadTooLarge,

    /// 429 with `X-Percept-Shed-Reason: unauthorized`. The token doesn't
    /// cover the (source_id, kind) on this batch — won't be retried.
    #[error("scope deny: token doesn't cover an event in the batch")]
    ScopeDeny,

    /// 429 / 503 after the SDK exhausted its retry budget. `last_status`
    /// is the final status code observed; `attempts` is how many tries
    /// were made.
    #[error("retries exhausted after {attempts} attempts (last status: {last_status})")]
    RetriesExhausted {
        attempts: usize,
        last_status: u16,
        last_retry_after: Option<Duration>,
    },

    /// 5xx that we don't handle (anything but 503).
    #[error("server error ({status}): {body}")]
    ServerError { status: u16, body: String },
}
