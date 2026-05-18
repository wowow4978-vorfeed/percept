//! Retention sweeper.
//!
//! DESIGN §11.3:
//! - `max_age` is the cheap dimension — a single DELETE filtered by
//!   `(source_id, kind, ts_ms_utc)` (the indexes already cover this).
//!   The Parquet day-partition unlink optimisation is a v2 follow-up.
//! - `max_count` / `max_bytes` are best-effort, in-partition rewrites.
//!   We emit a warning when a (source, kind) above
//!   `EXPENSIVE_REWRITE_THRESHOLD` events is bound by either.
//! - Vector pruning is coupled to raw retention by event_id, plus the
//!   optional `vector_max_age`. Slice-0 already rejects
//!   `vector_max_age > max_age`.
//!
//! The sweeper produces a `SweepReport`; in dry-run mode it counts but
//! doesn't execute.

use serde::Serialize;

use super::policy::{resolve_effective, EffectiveRetention, RetentionPolicy};
use crate::cold::{ColdError, ColdStore};
use crate::vector::VectorIndex;

/// Threshold above which a `max_count` / `max_bytes` policy bound to a
/// `(source_id, kind)` triggers the "expensive policy" warning DESIGN
/// §11.3 calls for.
pub const EXPENSIVE_REWRITE_THRESHOLD: i64 = 1_000;

#[derive(Debug, Clone, Default, Serialize)]
pub struct SweepReport {
    pub per_pair: Vec<PerPairReport>,
    pub events_dropped: u64,
    pub vectors_dropped: u64,
    pub warnings: Vec<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerPairReport {
    pub source_id: String,
    pub kind: String,
    pub effective: EffectiveRetention,
    pub events_dropped: u64,
    pub vectors_dropped: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SweepError {
    #[error(transparent)]
    Cold(#[from] ColdError),
    #[error(transparent)]
    Vector(#[from] crate::vector::VectorError),
}

/// Run one sweep pass. `now_ms` is the reference clock for age cut-offs;
/// taking it as an argument keeps the function deterministic in tests.
pub fn sweep(
    cold: &ColdStore,
    vector: Option<&VectorIndex>,
    policies: &[RetentionPolicy],
    now_ms: i64,
    dry_run: bool,
) -> Result<SweepReport, SweepError> {
    let mut report = SweepReport {
        dry_run,
        ..Default::default()
    };

    let pairs = cold.distinct_source_kind_pairs()?;
    for (source_id, kind) in pairs {
        let eff = resolve_effective(policies, &source_id, &kind);
        if eff.is_empty() {
            continue;
        }

        let mut events_dropped: u64 = 0;
        let mut vectors_dropped: u64 = 0;

        if let Some(max_age_ms) = eff.max_age_ms {
            let cutoff_ms = now_ms.saturating_sub(max_age_ms);
            events_dropped += cold.sweep_max_age(&source_id, &kind, cutoff_ms, dry_run)?;
        }
        if let Some(max_count) = eff.max_count {
            let before = cold.count_for_pair(&source_id, &kind)?;
            if before > EXPENSIVE_REWRITE_THRESHOLD {
                report.warnings.push(format!(
                    "max_count on ({source_id:?}, {kind:?}) is expensive: \
                     {before} events touched per sweep",
                ));
            }
            events_dropped += cold.sweep_max_count(&source_id, &kind, max_count, dry_run)?;
        }
        if let Some(max_bytes) = eff.max_bytes {
            let before = cold.count_for_pair(&source_id, &kind)?;
            if before > EXPENSIVE_REWRITE_THRESHOLD {
                report.warnings.push(format!(
                    "max_bytes on ({source_id:?}, {kind:?}) is expensive: \
                     {before} events touched per sweep",
                ));
            }
            events_dropped += cold.sweep_max_bytes(&source_id, &kind, max_bytes, dry_run)?;
        }

        if let Some(idx) = vector {
            if let Some(vmax) = eff.vector_max_age_ms {
                let cutoff_ms = now_ms.saturating_sub(vmax);
                vectors_dropped += idx.sweep_max_age(&source_id, &kind, cutoff_ms, dry_run)?;
            }
        }

        report.events_dropped += events_dropped;
        report.vectors_dropped += vectors_dropped;
        if events_dropped > 0 || vectors_dropped > 0 {
            report.per_pair.push(PerPairReport {
                source_id,
                kind,
                effective: eff,
                events_dropped,
                vectors_dropped,
            });
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold::ColdStore;
    use crate::vector::{Embedder, HashEmbedder, VectorIndex, VectorRecord};
    use percept_core::Event;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use ulid::Ulid;

    fn ev(source: &str, kind: &str, ts: i64) -> Arc<Event> {
        Arc::new(Event {
            event_id: Ulid::new(),
            source_id: source.to_string(),
            kind: kind.to_string(),
            ts_ms_utc: ts,
            semantic: json!({ "v": ts }),
            links: None,
            trace_id: None,
            ingest_ts_ms_utc: Some(ts),
            seq: Some(1),
            producer_id: None,
            schema_invalid: None,
        })
    }

    #[test]
    fn dry_run_predicts_the_drop_but_does_not_execute() {
        let cold = ColdStore::open_in_memory().unwrap();
        let evs: Vec<_> = (0..10).map(|i| ev("s", "k", i * 10)).collect();
        cold.append(&evs).unwrap();

        let policies = vec![RetentionPolicy {
            match_source_id: Some("s".into()),
            max_age: Some(Duration::from_millis(55)),
            ..Default::default()
        }];

        // now_ms = 100 → cutoff = 45 → drop ts < 45 → 0, 10, 20, 30, 40 = 5 events
        let report = sweep(&cold, None, &policies, 100, true).unwrap();
        assert!(report.dry_run);
        assert_eq!(report.events_dropped, 5);
        // Cold store untouched.
        assert_eq!(cold.event_count().unwrap(), 10);
    }

    #[test]
    fn execute_drops_events_older_than_max_age() {
        let cold = ColdStore::open_in_memory().unwrap();
        let evs: Vec<_> = (0..10).map(|i| ev("s", "k", i * 10)).collect();
        cold.append(&evs).unwrap();

        let policies = vec![RetentionPolicy {
            match_source_id: Some("s".into()),
            max_age: Some(Duration::from_millis(55)),
            ..Default::default()
        }];
        let report = sweep(&cold, None, &policies, 100, false).unwrap();
        assert!(!report.dry_run);
        assert_eq!(report.events_dropped, 5);
        assert_eq!(cold.event_count().unwrap(), 5);
    }

    #[test]
    fn max_count_keeps_last_n() {
        let cold = ColdStore::open_in_memory().unwrap();
        let evs: Vec<_> = (0..10).map(|i| ev("s", "k", i * 10)).collect();
        cold.append(&evs).unwrap();

        let policies = vec![RetentionPolicy {
            match_source_id: Some("s".into()),
            max_count: Some(3),
            ..Default::default()
        }];
        let report = sweep(&cold, None, &policies, 100, false).unwrap();
        assert_eq!(report.events_dropped, 7);
        assert_eq!(cold.event_count().unwrap(), 3);
    }

    #[test]
    fn vector_max_age_prunes_index() {
        let embedder = HashEmbedder::new(32);
        let idx = VectorIndex::open_in_memory(embedder.model_id(), embedder.dim()).unwrap();
        let cold = ColdStore::open_in_memory().unwrap();

        // Insert events + matching vectors at two different times.
        for ts in [10_i64, 200] {
            let e = ev("s", "k", ts);
            cold.append(&[e.clone()]).unwrap();
            idx.append(&[VectorRecord {
                event_id: e.event_id,
                source_id: e.source_id.clone(),
                kind: e.kind.clone(),
                ts_ms_utc: e.ts_ms_utc,
                truncated: false,
                model_id: embedder.model_id().to_string(),
                vector: embedder.embed("x"),
            }])
            .unwrap();
        }

        // Policy: vector_max_age = 100 ms. now_ms = 300 -> cutoff = 200.
        // Vector at ts=10 should be dropped; ts=200 retained (strictly <).
        let policies = vec![RetentionPolicy {
            match_source_id: Some("s".into()),
            vector_max_age: Some(Duration::from_millis(100)),
            ..Default::default()
        }];
        let report = sweep(&cold, Some(&idx), &policies, 300, false).unwrap();
        assert_eq!(report.vectors_dropped, 1);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn warns_when_max_count_bound_to_hot_pair() {
        let cold = ColdStore::open_in_memory().unwrap();
        // Insert above the warn threshold for the pair.
        let evs: Vec<_> = (0..(EXPENSIVE_REWRITE_THRESHOLD + 5))
            .map(|i| ev("hot.source", "noisy", i))
            .collect();
        cold.append(&evs).unwrap();
        let policies = vec![RetentionPolicy {
            match_source_id: Some("hot.source".into()),
            max_count: Some(100),
            ..Default::default()
        }];
        let report = sweep(&cold, None, &policies, 999_999, false).unwrap();
        assert!(report.warnings.iter().any(|w| w.contains("expensive")));
    }
}
