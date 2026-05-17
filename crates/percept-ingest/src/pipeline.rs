//! Wires the ingest path: HTTP -> bounded mpsc -> normalizer task -> hot rings.
//!
//! Slice 1 has a single consumer (hot rings), so the fan-out step from
//! DECISIONS §6 is degenerate and lives inside the normalizer's loop. Slice
//! 3 will split it out when the cold writer joins.

use std::sync::Arc;

use percept_store::{HotRingConfig, HotRings};
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
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            bus_depth: 4096,
            hot_ring: HotRingConfig::default(),
            hard_cap_bytes: DEFAULT_HARD_CAP_BYTES,
            soft_cap_bytes: DEFAULT_SOFT_CAP_BYTES,
        }
    }
}

pub struct Pipeline {
    pub http_state: HttpState,
    pub hot_rings: Arc<HotRings>,
    pub metrics: Arc<Metrics>,
    pub normalizer_handle: JoinHandle<()>,
}

impl Pipeline {
    /// Spawns the normalizer task and returns the HTTP state plus handles.
    pub fn spawn(auth: Arc<Auth>, schemas: Arc<SchemaIndex>, config: PipelineConfig) -> Self {
        let metrics = Arc::new(Metrics::new());
        let hot_rings = Arc::new(HotRings::new(config.hot_ring));
        let (tx, rx) = mpsc::channel::<IngestEnvelope>(config.bus_depth);

        let normalizer = Normalizer::new(rx, hot_rings.clone(), metrics.clone(), schemas);
        let handle = tokio::spawn(normalizer.run());

        let http_state = HttpState {
            auth,
            metrics: metrics.clone(),
            tx,
            hard_cap_bytes: config.hard_cap_bytes,
            soft_cap_bytes: config.soft_cap_bytes,
        };

        Self {
            http_state,
            hot_rings,
            metrics,
            normalizer_handle: handle,
        }
    }
}
