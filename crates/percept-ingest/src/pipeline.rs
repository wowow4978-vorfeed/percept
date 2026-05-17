//! Wires the ingest path: HTTP -> bounded mpsc -> normalizer task ->
//! { hot rings (inline sync), cold writer (try_send) }.
//!
//! DECISIONS §6: per-consumer channels, normalizer fans out. Hot-ring
//! fan-out runs inline; cold writer is its own task with a bounded mpsc.

use std::sync::Arc;

use percept_core::Event;
use percept_store::{
    ColdStore, ColdWriter, ColdWriterConfig, ColdWriterMetrics, HotRingConfig, HotRings,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::auth::Auth;
use crate::http::{HttpState, DEFAULT_HARD_CAP_BYTES, DEFAULT_SOFT_CAP_BYTES};
use crate::metrics::Metrics;
use crate::normalizer::{IngestEnvelope, Normalizer};
use crate::schema::SchemaIndex;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub bus_depth: usize,
    pub hot_ring: HotRingConfig,
    pub hard_cap_bytes: usize,
    pub soft_cap_bytes: usize,
    pub cold_writer: ColdWriterConfig,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            bus_depth: 4096,
            hot_ring: HotRingConfig::default(),
            hard_cap_bytes: DEFAULT_HARD_CAP_BYTES,
            soft_cap_bytes: DEFAULT_SOFT_CAP_BYTES,
            cold_writer: ColdWriterConfig::default(),
        }
    }
}

pub struct Pipeline {
    pub http_state: HttpState,
    pub hot_rings: Arc<HotRings>,
    pub metrics: Arc<Metrics>,
    pub cold_store: Option<Arc<ColdStore>>,
    pub cold_writer_metrics: Option<Arc<ColdWriterMetrics>>,
    pub normalizer_handle: JoinHandle<()>,
    pub cold_writer_handle: Option<JoinHandle<()>>,
}

impl Pipeline {
    /// Spawns the normalizer task and (if `cold_store` is `Some`) the cold
    /// writer task. Returns the HTTP state plus handles.
    pub fn spawn(
        auth: Arc<Auth>,
        schemas: Arc<SchemaIndex>,
        cold_store: Option<Arc<ColdStore>>,
        config: PipelineConfig,
    ) -> Self {
        let metrics = Arc::new(Metrics::new());
        let hot_rings = Arc::new(HotRings::new(config.hot_ring));
        let (tx, rx) = mpsc::channel::<IngestEnvelope>(config.bus_depth);

        let (cold_tx, cold_handle, cold_metrics) = if let Some(store) = cold_store.clone() {
            let (cold_tx, cold_rx) = mpsc::channel::<Arc<Event>>(config.bus_depth);
            let cm = Arc::new(ColdWriterMetrics::default());
            let writer = ColdWriter::new(cold_rx, store, cm.clone(), config.cold_writer);
            let handle = tokio::spawn(writer.run());
            (Some(cold_tx), Some(handle), Some(cm))
        } else {
            (None, None, None)
        };

        let normalizer = Normalizer::new(rx, hot_rings.clone(), metrics.clone(), schemas, cold_tx);
        let normalizer_handle = tokio::spawn(normalizer.run());

        let http_state = HttpState {
            auth,
            metrics: metrics.clone(),
            cold_writer_metrics: cold_metrics.clone(),
            tx,
            hard_cap_bytes: config.hard_cap_bytes,
            soft_cap_bytes: config.soft_cap_bytes,
        };

        Self {
            http_state,
            hot_rings,
            metrics,
            cold_store,
            cold_writer_metrics: cold_metrics,
            normalizer_handle,
            cold_writer_handle: cold_handle,
        }
    }
}
