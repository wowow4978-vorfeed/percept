//! Single-task normalizer: drains `IngestEvent` envelopes, assigns
//! server-side fields, and pushes canonical `Event`s into the hot rings.
//!
//! `seq` is a per-`source_id` monotonic counter — trivially correct because
//! the normalizer runs as one task (DECISIONS §7).
//!
//! Schema validation is soft-fail (DESIGN §5.2): a payload that doesn't
//! conform to its descriptor's `semantic_schema` is stored with
//! `_schema_invalid = true` and the counter is incremented; the event still
//! lands in the hot ring.

use std::collections::HashMap;
use std::sync::Arc;

use percept_core::{new_event_id, now_ms_utc, Event};
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::schema::SchemaIndex;
use percept_store::HotRings;

#[derive(Debug, Clone)]
pub struct IngestEnvelope {
    pub event: crate::wire::IngestEvent,
    pub token_name: Option<String>,
}

pub struct Normalizer {
    rx: mpsc::Receiver<IngestEnvelope>,
    hot_rings: Arc<HotRings>,
    metrics: Arc<Metrics>,
    schemas: Arc<SchemaIndex>,
    cold_tx: Option<mpsc::Sender<Arc<Event>>>,
    seq_by_source: HashMap<String, u64>,
}

impl Normalizer {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<IngestEnvelope>,
        hot_rings: Arc<HotRings>,
        metrics: Arc<Metrics>,
        schemas: Arc<SchemaIndex>,
        cold_tx: Option<mpsc::Sender<Arc<Event>>>,
    ) -> Self {
        Self {
            rx,
            hot_rings,
            metrics,
            schemas,
            cold_tx,
            seq_by_source: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        tracing::debug!("normalizer started");
        while let Some(envelope) = self.rx.recv().await {
            let event = Arc::new(self.normalize(envelope.event));
            self.hot_rings.push(event.clone());
            if let Some(tx) = &self.cold_tx {
                match tx.try_send(event) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.metrics.inc_consumer_drop("cold_writer");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Cold writer task exited; treat as no cold sink.
                    }
                }
            }
            self.metrics.inc_accepted(envelope.token_name.as_deref());
        }
        tracing::info!("normalizer: input closed, exiting");
    }

    fn normalize(&mut self, e: crate::wire::IngestEvent) -> Event {
        let seq_entry = self.seq_by_source.entry(e.source_id.clone()).or_insert(0);
        *seq_entry += 1;
        let seq = *seq_entry;

        let event_id = e.event_id.unwrap_or_else(new_event_id);
        let ingest_ts = now_ms_utc();
        let trace_id = Some(e.trace_id.unwrap_or_else(|| new_event_id().to_string()));

        let validation = self.schemas.validate(&e.source_id, &e.kind, &e.semantic);
        if validation == Some(true) {
            self.metrics.inc_schema_invalid();
            self.metrics
                .inc_source_error(&e.source_id, "schema_invalid", ingest_ts);
        }
        // Only set `_schema_invalid` when validation actually failed; absent
        // when the payload passed or when no schema applied.
        let schema_invalid = if validation == Some(true) {
            Some(true)
        } else {
            None
        };

        // Kind version canonicalization is intentionally not done here —
        // Event.kind is preserved as-sent (DESIGN §3.1 permits either form).
        // Schema lookup already resolves "latest" internally.

        Event {
            event_id,
            source_id: e.source_id,
            kind: e.kind,
            ts_ms_utc: e.ts_ms_utc,
            semantic: e.semantic,
            links: e.links,
            trace_id,
            ingest_ts_ms_utc: Some(ingest_ts),
            seq: Some(seq),
            producer_id: e.producer_id,
            schema_invalid,
        }
    }
}
