//! Blocking (synchronous) variant for embedded producers that don't
//! want a tokio runtime. Same retry semantics as the async `Client`.

use std::io::Write;
use std::time::Duration;

use reqwest::blocking as rblocking;
use reqwest::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE};

use crate::client::{ClientConfig, SHED_REASON_HEADER};
use crate::error::ClientError;
use crate::event::{Envelope, Event};

#[derive(Clone)]
pub struct BlockingClient {
    base_url: String,
    bearer: String,
    http: rblocking::Client,
    config: ClientConfig,
}

impl BlockingClient {
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self::with_config(base_url, token, ClientConfig::default())
    }

    pub fn with_config(
        base_url: impl Into<String>,
        token: impl Into<String>,
        config: ClientConfig,
    ) -> Self {
        let http = rblocking::Client::builder()
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

    pub fn send_batch(&self, events: &[Event]) -> Result<usize, ClientError> {
        if events.is_empty() {
            return Ok(0);
        }
        let body_json = serde_json::to_vec(&Envelope { events })?;
        let body = if self.config.gzip {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(&body_json).expect("in-memory write");
            enc.finish().expect("in-memory finish")
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
            let resp = req.send()?;
            let status = resp.status();
            last_status = status.as_u16();
            let shed_reason = resp
                .headers()
                .get(SHED_REASON_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(Duration::from_secs);
            last_retry_after = retry_after;

            match status.as_u16() {
                200 => return Ok(resp.json::<IngestOk>()?.accepted),
                401 => return Err(ClientError::Unauthorized),
                413 => return Err(ClientError::PayloadTooLarge),
                429 | 503 if attempt + 1 < self.config.max_attempts => {
                    if matches!(shed_reason.as_deref(), Some("unauthorized")) {
                        return Err(ClientError::ScopeDeny);
                    }
                    if matches!(shed_reason.as_deref(), Some("payload_too_large")) {
                        return Err(ClientError::PayloadTooLarge);
                    }
                    let wait = retry_after
                        .unwrap_or(self.config.default_retry_after)
                        .min(self.config.max_retry_after);
                    std::thread::sleep(wait);
                    continue;
                }
                400..=499 => {
                    let body = resp.text().unwrap_or_default();
                    return Err(ClientError::BadRequest {
                        status: status.as_u16(),
                        body,
                    });
                }
                _ => {
                    let body = resp.text().unwrap_or_default();
                    return Err(ClientError::ServerError {
                        status: status.as_u16(),
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

    pub fn send_one(&self, event: Event) -> Result<usize, ClientError> {
        self.send_batch(&[event])
    }
}

#[derive(serde::Deserialize)]
struct IngestOk {
    accepted: usize,
}
