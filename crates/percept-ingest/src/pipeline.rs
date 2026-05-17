//! Wires the ingest path: HTTP -> bounded mpsc -> normalizer task ->
//! { hot rings (inline sync), cold writer (try_send), embedder (try_send) }.
//!
//! DECISIONS §6: per-consumer channels, normalizer fans out. Hot-ring
//! fan-out runs inline; cold writer and embedder each get their own
//! bounded mpsc and async task. The embedder's selector check fires in
//! the normalizer so events that shouldn't be embedded never enqueue.

use std::sync::Arc;

use percept_core::Event;
use percept_store::{
    ColdStore, ColdWriter, ColdWriterConfig, ColdWriterMetrics, EmbedSelector, EmbedderConfig,
    EmbedderMetrics, EmbedderTask, HotRingConfig, HotRings, SharedEmbedder, VectorIndex,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::auth::Auth;
use crate::http::{HttpState, DEFAULT_HARD_CAP_BYTES, DEFAULT_SOFT_CAP_BYTES};
use crate::metrics::Metrics;
use crate::normalizer::{EmbedSink, IngestEnvelope, Normalizer};
use crate::schema::SchemaIndex;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub bus_depth: usize,
    pub hot_ring: HotRingConfig,
    pub hard_cap_bytes: usize,
    pub soft_cap_bytes: usize,
    pub cold_writer: ColdWriterConfig,
    pub embedder: EmbedderConfig,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            bus_depth: 4096,
            hot_ring: HotRingConfig::default(),
            hard_cap_bytes: DEFAULT_HARD_CAP_BYTES,
            soft_cap_bytes: DEFAULT_SOFT_CAP_BYTES,
            cold_writer: ColdWriterConfig::default(),
            embedder: EmbedderConfig::default(),
        }
    }
}

/// Optional vector subsystem handed to the pipeline at spawn time. When
/// `None`, no embedder task spawns and the normalizer's vector fan-out is
/// disabled.
pub struct VectorSubsystem {
    pub embedder: SharedEmbedder,
    pub index: Arc<VectorIndex>,
    pub selector: Arc<EmbedSelector>,
}

pub struct Pipeline {
    pub http_state: HttpState,
    pub hot_rings: Arc<HotRings>,
    pub metrics: Arc<Metrics>,
    pub cold_store: Option<Arc<ColdStore>>,
    pub cold_writer_metrics: Option<Arc<ColdWriterMetrics>>,
    pub vector_index: Option<Arc<VectorIndex>>,
    pub embedder_metrics: Option<Arc<EmbedderMetrics>>,
    /// Slice 8: receiver end of the forwarder fan-out. `None` when no
    /// `[[forwarder]]` is configured; the binary drains this and posts
    /// to a hub via percept-client.
    pub forward_rx: Option<mpsc::Receiver<Arc<Event>>>,
    pub normalizer_handle: JoinHandle<()>,
    pub cold_writer_handle: Option<JoinHandle<()>>,
    pub embedder_handle: Option<JoinHandle<()>>,
}

impl Pipeline {
    pub fn spawn(
        auth: Arc<Auth>,
        schemas: Arc<SchemaIndex>,
        cold_store: Option<Arc<ColdStore>>,
        vector: Option<VectorSubsystem>,
        forwarder_enabled: bool,
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

        let (embed_sink, embedder_handle, embedder_metrics, vector_index) = if let Some(v) = vector
        {
            let (embed_tx, embed_rx) = mpsc::channel::<Arc<Event>>(config.bus_depth);
            let em = Arc::new(EmbedderMetrics::default());
            let task = EmbedderTask::new(
                embed_rx,
                v.embedder,
                v.index.clone(),
                em.clone(),
                config.embedder,
            );
            let handle = tokio::spawn(task.run());
            (
                Some(EmbedSink {
                    tx: embed_tx,
                    selector: v.selector,
                }),
                Some(handle),
                Some(em),
                Some(v.index),
            )
        } else {
            (None, None, None, None)
        };

        let (forward_tx, forward_rx) = if forwarder_enabled {
            let (t, r) = mpsc::channel::<Arc<Event>>(config.bus_depth);
            (Some(t), Some(r))
        } else {
            (None, None)
        };

        let normalizer = Normalizer::new(
            rx,
            hot_rings.clone(),
            metrics.clone(),
            schemas,
            cold_tx,
            embed_sink,
            forward_tx,
        );
        let normalizer_handle = tokio::spawn(normalizer.run());

        let http_state = HttpState {
            auth,
            metrics: metrics.clone(),
            cold_writer_metrics: cold_metrics.clone(),
            embedder_metrics: embedder_metrics.clone(),
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
            vector_index,
            embedder_metrics,
            forward_rx,
            normalizer_handle,
            cold_writer_handle: cold_handle,
            embedder_handle,
        }
    }
}
