//! Edge → hub forwarder. Drains the normalizer's forward fan-out and
//! pushes events to a hub Percept via `percept-client`.
//!
//! DESIGN §8: `source_id` rewrite on egress is mandatory — every
//! forwarded event has `<peer_id>.` prepended to its `source_id` so two
//! edges with the same local id ("temp.fridge") don't collide on the
//! hub. The hub sees `kitchen.temp.fridge` vs `garage.temp.fridge`.
//!
//! The forwarder runs as its own tokio task. A slow / unavailable hub
//! never blocks ingest: the normalizer's `try_send` drops with a
//! `consumer_drops{consumer="forwarder"}` counter increment, and the
//! local hot ring / cold store / vector index stay current. That's the
//! "WAN-down still answers locally" property §8 calls out.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use percept_client::{Client, Event as ClientEvent};
use percept_core::Event;
use tokio::sync::mpsc;
use tokio::time::interval;

#[derive(Debug, Clone)]
pub struct ForwarderConfig {
    /// Prefix prepended to every forwarded `source_id`.
    pub peer_id: String,
    pub batch_size: usize,
    pub batch_age: Duration,
}

impl Default for ForwarderConfig {
    fn default() -> Self {
        Self {
            peer_id: "edge".to_string(),
            batch_size: 64,
            batch_age: Duration::from_millis(500),
        }
    }
}

#[derive(Default)]
pub struct ForwarderMetrics {
    pub batches_sent: AtomicU64,
    pub events_forwarded: AtomicU64,
    pub events_dropped: AtomicU64,
    pub send_errors: AtomicU64,
}

impl ForwarderMetrics {
    pub fn render_into(&self, peer_id: &str, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(
            out,
            "# HELP percept_forwarder_events_total Events the forwarder sent to the hub."
        );
        let _ = writeln!(out, "# TYPE percept_forwarder_events_total counter");
        let _ = writeln!(
            out,
            "percept_forwarder_events_total{{peer=\"{peer_id}\"}} {}",
            self.events_forwarded.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_forwarder_send_errors_total Forwarder send failures."
        );
        let _ = writeln!(out, "# TYPE percept_forwarder_send_errors_total counter");
        let _ = writeln!(
            out,
            "percept_forwarder_send_errors_total{{peer=\"{peer_id}\"}} {}",
            self.send_errors.load(Ordering::Relaxed)
        );
    }
}

pub struct Forwarder {
    rx: mpsc::Receiver<Arc<Event>>,
    client: Arc<Client>,
    config: ForwarderConfig,
    metrics: Arc<ForwarderMetrics>,
}

impl Forwarder {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<Arc<Event>>,
        client: Arc<Client>,
        config: ForwarderConfig,
        metrics: Arc<ForwarderMetrics>,
    ) -> Self {
        Self {
            rx,
            client,
            config,
            metrics,
        }
    }

    pub async fn run(mut self) {
        tracing::info!(peer = %self.config.peer_id, "forwarder started");
        let mut buf: Vec<ClientEvent> = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(self.config.batch_age);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(event) => {
                            buf.push(to_client_event(&self.config.peer_id, &event));
                            if buf.len() >= self.config.batch_size {
                                self.flush(&mut buf).await;
                            }
                        }
                        None => {
                            if !buf.is_empty() {
                                self.flush(&mut buf).await;
                            }
                            tracing::info!(peer = %self.config.peer_id, "forwarder input closed, exiting");
                            return;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if !buf.is_empty() {
                        self.flush(&mut buf).await;
                    }
                }
            }
        }
    }

    async fn flush(&self, buf: &mut Vec<ClientEvent>) {
        let n = buf.len();
        let batch = std::mem::take(buf);
        match self.client.send_batch(&batch).await {
            Ok(accepted) => {
                self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .events_forwarded
                    .fetch_add(accepted as u64, Ordering::Relaxed);
                if accepted < n {
                    self.metrics
                        .events_dropped
                        .fetch_add((n - accepted) as u64, Ordering::Relaxed);
                }
            }
            Err(e) => {
                self.metrics.send_errors.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .events_dropped
                    .fetch_add(n as u64, Ordering::Relaxed);
                tracing::warn!(peer = %self.config.peer_id, err = %e, dropped = n, "forwarder flush failed; events dropped");
            }
        }
    }
}

/// Translate a local canonical Event into the client wire shape, with
/// the mandatory `<peer_id>.` prefix applied to `source_id`.
fn to_client_event(peer_id: &str, event: &Event) -> ClientEvent {
    let mut out = ClientEvent::new(
        format!("{peer_id}.{}", event.source_id),
        event.kind.clone(),
        event.ts_ms_utc,
        event.semantic.clone(),
    )
    .with_event_id(event.event_id);
    if let Some(trace_id) = &event.trace_id {
        out = out.with_trace_id(trace_id.clone());
    }
    if let Some(producer_id) = &event.producer_id {
        out = out.with_producer_id(producer_id.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use percept_core::Event;
    use serde_json::json;
    use ulid::Ulid;

    fn local(source: &str, kind: &str) -> Event {
        Event {
            event_id: Ulid::new(),
            source_id: source.to_string(),
            kind: kind.to_string(),
            ts_ms_utc: 1700,
            semantic: json!({"x": 1}),
            links: None,
            trace_id: Some("trace-123".into()),
            ingest_ts_ms_utc: Some(1701),
            seq: Some(7),
            producer_id: Some("edge-producer".into()),
            schema_invalid: None,
        }
    }

    #[test]
    fn source_id_is_prefixed_with_peer_id() {
        let e = local("temp.fridge", "temperature");
        let out = to_client_event("kitchen", &e);
        // Serializes to the wire shape; verify the rewritten field.
        let v = serde_json::to_value(&out).unwrap();
        assert_eq!(v["source_id"], "kitchen.temp.fridge");
        assert_eq!(v["kind"], "temperature");
        assert_eq!(v["trace_id"], "trace-123");
        assert_eq!(v["producer_id"], "edge-producer");
        // event_id is preserved so the hub's idempotent dedupe works.
        assert_eq!(v["event_id"], e.event_id.to_string());
    }
}
