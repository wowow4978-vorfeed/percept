//! Single-task normalizer: drains `IngestEvent` envelopes, assigns
//! server-side fields, and pushes canonical `Event`s into the hot rings.
//!
//! `seq` is a per-`source_id` monotonic counter — trivially correct because
//! the normalizer runs as one task (DECISIONS §7).
//!
//! Schema validation (DESIGN §5.2) is wired structurally — `_schema_invalid`
//! and the counter exist — but the JSON Schema runtime is descoped from
//! Slice 1. See the TODO in `validate_semantic`.

use std::collections::HashMap;
use std::sync::Arc;

use percept_core::{new_event_id, now_ms_utc, Event};
use tokio::sync::mpsc;

use crate::metrics::Metrics;
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
    seq_by_source: HashMap<String, u64>,
}

impl Normalizer {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<IngestEnvelope>,
        hot_rings: Arc<HotRings>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            rx,
            hot_rings,
            metrics,
            seq_by_source: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        tracing::debug!("normalizer started");
        while let Some(envelope) = self.rx.recv().await {
            let event = self.normalize(envelope.event);
            self.hot_rings.push(Arc::new(event));
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

        let schema_invalid = validate_semantic(&e.kind, &e.semantic);
        if schema_invalid == Some(true) {
            self.metrics.inc_schema_invalid();
        }

        // Kind version resolution against the DescriptorIndex lands with
        // the MCP work in slice 2; Slice 1 keeps `e.kind` as-sent.

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

/// Slice 1 placeholder: returns `None` (validation skipped).
///
/// TODO Slice 2/3: wire `jsonschema` against the resolved descriptor's
/// `semantic_schema`. The `_schema_invalid` field and counter are already
/// in place so the upgrade is one function.
fn validate_semantic(_kind: &str, _semantic: &serde_json::Value) -> Option<bool> {
    None
}
