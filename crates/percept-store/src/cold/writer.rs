//! Cold writer task. Drains canonical Events from a bounded mpsc, batches
//! by time + size, commits to the ColdStore.
//!
//! Lag is visible to callers via `pending()` and the `batch_lag` counter the
//! caller hands in.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use percept_core::Event;
use tokio::sync::mpsc;
use tokio::time::interval;

use super::store::ColdStore;

#[derive(Debug, Clone, Copy)]
pub struct ColdWriterConfig {
    pub batch_size: usize,
    pub batch_age: Duration,
}

impl Default for ColdWriterConfig {
    fn default() -> Self {
        Self {
            batch_size: 256,
            batch_age: Duration::from_millis(500),
        }
    }
}

/// Counters surfaced to `/metrics`. The caller owns the `Arc` and renders
/// it alongside the other metrics; this writer just bumps them.
#[derive(Default)]
pub struct ColdWriterMetrics {
    pub batches_committed: AtomicU64,
    pub events_committed: AtomicU64,
    pub events_dropped: AtomicU64,
    pub commit_errors: AtomicU64,
    pub pending_buffer: AtomicU64,
}

impl ColdWriterMetrics {
    /// Append Prometheus-text counters to `out`. The caller is responsible
    /// for stitching this into the full `/metrics` body.
    pub fn render_into(&self, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(
            out,
            "# HELP percept_cold_batches_committed_total Cold writer batches committed."
        );
        let _ = writeln!(out, "# TYPE percept_cold_batches_committed_total counter");
        let _ = writeln!(
            out,
            "percept_cold_batches_committed_total {}",
            self.batches_committed.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_cold_events_committed_total Cold writer events committed."
        );
        let _ = writeln!(out, "# TYPE percept_cold_events_committed_total counter");
        let _ = writeln!(
            out,
            "percept_cold_events_committed_total {}",
            self.events_committed.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_cold_commit_errors_total Cold writer commit failures."
        );
        let _ = writeln!(out, "# TYPE percept_cold_commit_errors_total counter");
        let _ = writeln!(
            out,
            "percept_cold_commit_errors_total {}",
            self.commit_errors.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_cold_events_dropped_total Cold writer events dropped (e.g. commit failures)."
        );
        let _ = writeln!(out, "# TYPE percept_cold_events_dropped_total counter");
        let _ = writeln!(
            out,
            "percept_cold_events_dropped_total {}",
            self.events_dropped.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_cold_pending_buffer Cold writer in-memory pending events."
        );
        let _ = writeln!(out, "# TYPE percept_cold_pending_buffer gauge");
        let _ = writeln!(
            out,
            "percept_cold_pending_buffer {}",
            self.pending_buffer.load(Ordering::Relaxed)
        );
    }
}

pub struct ColdWriter {
    rx: mpsc::Receiver<Arc<Event>>,
    store: Arc<ColdStore>,
    metrics: Arc<ColdWriterMetrics>,
    config: ColdWriterConfig,
}

impl ColdWriter {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<Arc<Event>>,
        store: Arc<ColdStore>,
        metrics: Arc<ColdWriterMetrics>,
        config: ColdWriterConfig,
    ) -> Self {
        Self {
            rx,
            store,
            metrics,
            config,
        }
    }

    /// Drain `rx` until closed, flushing whenever the batch fills or ages out.
    pub async fn run(mut self) {
        tracing::debug!("cold writer started");
        let mut buf: Vec<Arc<Event>> = Vec::with_capacity(self.config.batch_size);
        let mut ticker = interval(self.config.batch_age);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick.
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
                            tracing::info!("cold writer: input closed, exiting");
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
        let n = buf.len();
        match self.store.append(buf) {
            Ok(()) => {
                self.metrics
                    .batches_committed
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .events_committed
                    .fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(e) => {
                self.metrics.commit_errors.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .events_dropped
                    .fetch_add(n as u64, Ordering::Relaxed);
                tracing::error!(err = %e, batch_size = n, "cold writer commit failed");
            }
        }
        buf.clear();
        self.metrics.pending_buffer.store(0, Ordering::Relaxed);
    }
}
