//! Embedder task: drains canonical Events the normalizer fanned out for
//! embedding, computes vectors, persists them.
//!
//! Same shape as `ColdWriter` — single async task, time/size batch flush,
//! counters surfaced to `/metrics`. The selector check lives upstream in
//! the normalizer so this task only sees events that should be embedded.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use percept_core::Event;
use tokio::sync::mpsc;
use tokio::time::interval;

use super::embedder::SharedEmbedder;
use super::index::{VectorIndex, VectorRecord};
use super::truncate::truncate_utf8;

/// 2048 bytes per DECISIONS §2.
pub const EMBED_TRUNCATE_BYTES: usize = 2048;

#[derive(Debug, Clone, Copy)]
pub struct EmbedderConfig {
    pub batch_size: usize,
    pub batch_age: Duration,
}

impl Default for EmbedderConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            batch_age: Duration::from_millis(500),
        }
    }
}

#[derive(Default)]
pub struct EmbedderMetrics {
    pub batches_committed: AtomicU64,
    pub events_embedded: AtomicU64,
    pub events_truncated: AtomicU64,
    pub commit_errors: AtomicU64,
    pub pending_buffer: AtomicU64,
}

impl EmbedderMetrics {
    pub fn render_into(&self, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(
            out,
            "# HELP percept_embedder_batches_committed_total Embedder batches committed."
        );
        let _ = writeln!(
            out,
            "# TYPE percept_embedder_batches_committed_total counter"
        );
        let _ = writeln!(
            out,
            "percept_embedder_batches_committed_total {}",
            self.batches_committed.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_embedder_events_embedded_total Embedder events committed."
        );
        let _ = writeln!(out, "# TYPE percept_embedder_events_embedded_total counter");
        let _ = writeln!(
            out,
            "percept_embedder_events_embedded_total {}",
            self.events_embedded.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_embedder_events_truncated_total Embedder events whose semantic was truncated before embedding."
        );
        let _ = writeln!(
            out,
            "# TYPE percept_embedder_events_truncated_total counter"
        );
        let _ = writeln!(
            out,
            "percept_embedder_events_truncated_total {}",
            self.events_truncated.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_embedder_commit_errors_total Embedder commit failures."
        );
        let _ = writeln!(out, "# TYPE percept_embedder_commit_errors_total counter");
        let _ = writeln!(
            out,
            "percept_embedder_commit_errors_total {}",
            self.commit_errors.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_embedder_pending_buffer Embedder in-memory pending events."
        );
        let _ = writeln!(out, "# TYPE percept_embedder_pending_buffer gauge");
        let _ = writeln!(
            out,
            "percept_embedder_pending_buffer {}",
            self.pending_buffer.load(Ordering::Relaxed)
        );
    }
}

pub struct EmbedderTask {
    rx: mpsc::Receiver<Arc<Event>>,
    embedder: SharedEmbedder,
    index: Arc<VectorIndex>,
    metrics: Arc<EmbedderMetrics>,
    config: EmbedderConfig,
}

impl EmbedderTask {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<Arc<Event>>,
        embedder: SharedEmbedder,
        index: Arc<VectorIndex>,
        metrics: Arc<EmbedderMetrics>,
        config: EmbedderConfig,
    ) -> Self {
        Self {
            rx,
            embedder,
            index,
            metrics,
            config,
        }
    }

    pub async fn run(mut self) {
        tracing::debug!("embedder task started");
        let mut buf: Vec<Arc<Event>> = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(self.config.batch_age);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            tokio::select! {
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(event) => {
                            buf.push(event);
                            self.metrics.pending_buffer.store(buf.len() as u64, Ordering::Relaxed);
                            if buf.len() >= self.config.batch_size {
                                self.flush(&mut buf);
                            }
                        }
                        None => {
                            if !buf.is_empty() {
                                self.flush(&mut buf);
                            }
                            tracing::info!("embedder task: input closed, exiting");
                            return;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if !buf.is_empty() {
                        self.flush(&mut buf);
                    }
                }
            }
        }
    }

    fn flush(&self, buf: &mut Vec<Arc<Event>>) {
        let model_id = self.embedder.model_id().to_string();
        let records: Vec<VectorRecord> = buf
            .iter()
            .filter_map(|e| build_record(e, &*self.embedder, &model_id, &self.metrics))
            .collect();
        let n = records.len();

        match self.index.append(&records) {
            Ok(()) => {
                self.metrics
                    .batches_committed
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .events_embedded
                    .fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => {
                self.metrics.commit_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!(err = %e, batch_size = n, "embedder commit failed");
            }
        }
        buf.clear();
        self.metrics.pending_buffer.store(0, Ordering::Relaxed);
    }
}

fn build_record(
    e: &Event,
    embedder: &dyn super::embedder::Embedder,
    model_id: &str,
    metrics: &EmbedderMetrics,
) -> Option<VectorRecord> {
    let serialized = serde_json::to_string(&e.semantic).ok()?;
    let (input, truncated) = truncate_utf8(&serialized, EMBED_TRUNCATE_BYTES);
    if truncated {
        metrics.events_truncated.fetch_add(1, Ordering::Relaxed);
    }
    let vector = embedder.embed(input);
    Some(VectorRecord {
        event_id: e.event_id,
        source_id: e.source_id.clone(),
        kind: e.kind.clone(),
        ts_ms_utc: e.ts_ms_utc,
        truncated,
        model_id: model_id.to_string(),
        vector,
    })
}
