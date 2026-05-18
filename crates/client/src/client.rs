//! Async producer client.
//!
//! Wraps `reqwest::Client`. One method to send a batch with built-in
//! retry on `429` / `503` that honours the `Retry-After` header. Logs
//! `X-Percept-Shed-Reason` so operators see why a retry is happening.
//!
//! Gzip is sent when `ClientConfig::gzip` is true (default). The
//! `axum`-based server side decompresses transparently; the producer
//! gets the network-bandwidth saving on chatty batches.

use std::time::Duration;

use reqwest::header::{HeaderValue, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE};
use reqwest::StatusCode;
use std::io::Write;

use crate::error::ClientError;
use crate::event::{Envelope, Event};

/// Header surfaced by the server on every shed (DESIGN §5.3).
pub const SHED_REASON_HEADER: &str = "x-percept-shed-reason";

#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Maximum number of attempts including the initial one.
    pub max_attempts: usize,
    /// Cap on the server-suggested `Retry-After` so a misbehaving
    /// server can't park us forever.
    pub max_retry_after: Duration,
    /// Default backoff when the server returns `429`/`503` without
    /// `Retry-After`.
    pub default_retry_after: Duration,
    /// Gzip the JSON body before send.
    pub gzip: bool,
    /// Overall HTTP timeout per attempt.
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            max_retry_after: Duration::from_secs(30),
            default_retry_after: Duration::from_millis(200),
            gzip: true,
            timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    base_url: String,
    bearer: String,
    http: reqwest::Client,
    config: ClientConfig,
}

impl Client {
    /// `base_url` is the root the server listens on (e.g.
    /// `http://localhost:7878`). The SDK posts to `<base_url>/ingest`.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self::with_config(base_url, token, ClientConfig::default())
    }

    pub fn with_config(
        base_url: impl Into<String>,
        token: impl Into<String>,
        config: ClientConfig,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .expect("reqwest builder defaults");
        Self {
            base_url: base_url.into(),
            bearer: token.into(),
            http,
            config,
        }
    }

    /// Send one event. Wraps `send_batch` for the common case.
    pub async fn send_one(&self, event: Event) -> Result<usize, ClientError> {
        self.send_batch(&[event]).await
    }

    /// Send a batch of events. Retries on `429` / `503`, honouring
    /// `Retry-After`. Returns the server-reported `accepted` count.
    pub async fn send_batch(&self, events: &[Event]) -> Result<usize, ClientError> {
        if events.is_empty() {
            return Ok(0);
        }
        let body_json = serde_json::to_vec(&Envelope { events })?;
        let body = if self.config.gzip {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(&body_json)
                .expect("in-memory write cannot fail");
            enc.finish().expect("in-memory finish cannot fail")
        } else {
            body_json
        };

        let mut last_status = 0;
        let mut last_retry_after: Option<Duration> = None;
        for attempt in 0..self.config.max_attempts {
            let mut req = self
                .http
                .post(format!("{}/ingest", self.base_url))
                .header(AUTHORIZATION, format!("Bearer {}", self.bearer))
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone());
            if self.config.gzip {
                req = req.header(CONTENT_ENCODING, "gzip");
            }
            let resp = req.send().await?;
            let status = resp.status();
            last_status = status.as_u16();
            let shed_reason = header_str(resp.headers().get(SHED_REASON_HEADER));
            let retry_after = parse_retry_after(resp.headers().get("retry-after"));
            last_retry_after = retry_after;

            match status {
                StatusCode::OK => {
                    let parsed: IngestOk = resp.json().await?;
                    return Ok(parsed.accepted);
                }
                StatusCode::UNAUTHORIZED => return Err(ClientError::Unauthorized),
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
                    if attempt + 1 < self.config.max_attempts =>
                {
                    if matches!(shed_reason.as_deref(), Some("unauthorized")) {
                        // Token-scope deny on a specific (source, kind):
                        // retrying with the same payload won't help.
                        return Err(ClientError::ScopeDeny);
                    }
                    if matches!(shed_reason.as_deref(), Some("payload_too_large")) {
                        return Err(ClientError::PayloadTooLarge);
                    }
                    let wait = retry_after
                        .unwrap_or(self.config.default_retry_after)
                        .min(self.config.max_retry_after);
                    tracing::warn!(
                        attempt = attempt + 1,
                        wait_ms = wait.as_millis() as u64,
                        shed_reason = shed_reason.as_deref().unwrap_or("(none)"),
                        "percept ingest backoff",
                    );
                    tokio::time::sleep(wait).await;
                    continue;
                }
                StatusCode::PAYLOAD_TOO_LARGE => return Err(ClientError::PayloadTooLarge),
                code if code.is_client_error() => {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(ClientError::BadRequest {
                        status: code.as_u16(),
                        body,
                    });
                }
                code => {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(ClientError::ServerError {
                        status: code.as_u16(),
                        body,
                    });
                }
            }
        }

        Err(ClientError::RetriesExhausted {
            attempts: self.config.max_attempts,
            last_status,
            last_retry_after,
        })
    }
}

#[derive(serde::Deserialize)]
struct IngestOk {
    accepted: usize,
}

fn header_str(h: Option<&HeaderValue>) -> Option<String> {
    h.and_then(|v| v.to_str().ok()).map(str::to_owned)
}

fn parse_retry_after(h: Option<&HeaderValue>) -> Option<Duration> {
    let raw = h?.to_str().ok()?;
    // Per RFC 9110 §10.2.3, Retry-After is either a delta-seconds or an
    // HTTP-date. Slice 7 only honours delta-seconds; the server itself
    // only emits integer seconds.
    let n: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_delta_seconds() {
        let v = HeaderValue::from_static("5");
        assert_eq!(parse_retry_after(Some(&v)), Some(Duration::from_secs(5)));
    }

    #[test]
    fn ignores_garbage_retry_after() {
        let v = HeaderValue::from_static("Wed, 21 Oct 2026 07:28:00 GMT");
        assert_eq!(parse_retry_after(Some(&v)), None);
    }
}
