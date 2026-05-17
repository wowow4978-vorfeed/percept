//! Background retention sweeper task. Spawns inside the tokio runtime
//! alongside the cold writer / embedder; runs at the configured cadence
//! (default 1 h per DESIGN §11.3).

use std::sync::Arc;
use std::time::Duration;

use percept_store::{sweep, ColdStore, RetentionPolicy, VectorIndex};
use tokio::time::interval;

#[derive(Debug, Clone)]
pub struct SweeperConfig {
    pub cadence: Duration,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            cadence: Duration::from_secs(3600),
        }
    }
}

pub struct Sweeper {
    cold: Arc<ColdStore>,
    vector: Option<Arc<VectorIndex>>,
    policies: Arc<Vec<RetentionPolicy>>,
    cadence: Duration,
}

impl Sweeper {
    #[must_use]
    pub fn new(
        cold: Arc<ColdStore>,
        vector: Option<Arc<VectorIndex>>,
        policies: Arc<Vec<RetentionPolicy>>,
        config: SweeperConfig,
    ) -> Self {
        Self {
            cold,
            vector,
            policies,
            cadence: config.cadence,
        }
    }

    pub async fn run(self) {
        if self.policies.is_empty() {
            tracing::info!("retention sweeper: no [[retention]] entries — exiting");
            return;
        }
        tracing::info!(
            cadence_secs = self.cadence.as_secs(),
            "retention sweeper started"
        );
        let mut ticker = interval(self.cadence);
        // First tick fires immediately; skip it so we don't sweep on
        // startup before any events exist.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let now_ms = percept_core::now_ms_utc();
            let cold = self.cold.clone();
            let vector = self.vector.clone();
            let policies = self.policies.clone();
            // Sweep can do meaningful work (DELETE in a transaction);
            // run it on a blocking pool so we don't tie up the runtime.
            let result = tokio::task::spawn_blocking(move || {
                sweep(&cold, vector.as_deref(), &policies, now_ms, false)
            })
            .await;
            match result {
                Ok(Ok(report)) => {
                    if report.events_dropped > 0 || report.vectors_dropped > 0 {
                        tracing::info!(
                            events = report.events_dropped,
                            vectors = report.vectors_dropped,
                            "retention sweep applied",
                        );
                    }
                    for w in &report.warnings {
                        tracing::warn!("retention sweep: {w}");
                    }
                }
                Ok(Err(e)) => tracing::error!(err = %e, "retention sweep failed"),
                Err(e) => tracing::error!(err = %e, "retention sweep panicked"),
            }
        }
    }
}
