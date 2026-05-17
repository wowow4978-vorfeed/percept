//! Hand-rolled Prometheus-text metrics for Slice 1.
//!
//! Counters only — gauges/histograms can be added in later slices when
//! there's something to measure. Keeps the dep footprint at zero.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

#[derive(Default)]
pub struct Metrics {
    pub accepted_total: AtomicU64,
    pub oversized_soft_total: AtomicU64,
    pub schema_invalid_total: AtomicU64,
    shed_by_reason: RwLock<HashMap<String, AtomicU64>>,
    per_token_accepted: RwLock<HashMap<String, AtomicU64>>,
    per_token_shed: RwLock<HashMap<String, AtomicU64>>,
}

impl Metrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_accepted(&self, token_name: Option<&str>) {
        self.accepted_total.fetch_add(1, Ordering::Relaxed);
        if let Some(name) = token_name {
            self.inc_in_map(&self.per_token_accepted, name);
        }
    }

    pub fn inc_oversized_soft(&self) {
        self.oversized_soft_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_schema_invalid(&self) {
        self.schema_invalid_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_shed(&self, reason: &str, token_name: Option<&str>) {
        self.inc_in_map(&self.shed_by_reason, reason);
        if let Some(name) = token_name {
            self.inc_in_map(&self.per_token_shed, name);
        }
    }

    fn inc_in_map(&self, map: &RwLock<HashMap<String, AtomicU64>>, key: &str) {
        {
            let read = map.read();
            if let Some(c) = read.get(key) {
                c.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let mut write = map.write();
        write
            .entry(key.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Render in Prometheus text exposition format.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP percept_accepted_total Events accepted at the ingest boundary."
        );
        let _ = writeln!(out, "# TYPE percept_accepted_total counter");
        let _ = writeln!(
            out,
            "percept_accepted_total {}",
            self.accepted_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP percept_oversized_soft_total Events larger than the soft size cap."
        );
        let _ = writeln!(out, "# TYPE percept_oversized_soft_total counter");
        let _ = writeln!(
            out,
            "percept_oversized_soft_total {}",
            self.oversized_soft_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP percept_schema_invalid_total Events that failed semantic_schema validation."
        );
        let _ = writeln!(out, "# TYPE percept_schema_invalid_total counter");
        let _ = writeln!(
            out,
            "percept_schema_invalid_total {}",
            self.schema_invalid_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP percept_shed_total Events shed at the ingest boundary by reason."
        );
        let _ = writeln!(out, "# TYPE percept_shed_total counter");
        for (reason, c) in self.shed_by_reason.read().iter() {
            let _ = writeln!(
                out,
                "percept_shed_total{{reason=\"{reason}\"}} {}",
                c.load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP percept_token_accepted_total Per-token accepted counter."
        );
        let _ = writeln!(out, "# TYPE percept_token_accepted_total counter");
        for (name, c) in self.per_token_accepted.read().iter() {
            let _ = writeln!(
                out,
                "percept_token_accepted_total{{token=\"{name}\"}} {}",
                c.load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            out,
            "# HELP percept_token_shed_total Per-token shed counter."
        );
        let _ = writeln!(out, "# TYPE percept_token_shed_total counter");
        for (name, c) in self.per_token_shed.read().iter() {
            let _ = writeln!(
                out,
                "percept_token_shed_total{{token=\"{name}\"}} {}",
                c.load(Ordering::Relaxed)
            );
        }
        out
    }

    #[must_use]
    pub fn shed_count(&self, reason: &str) -> u64 {
        self.shed_by_reason
            .read()
            .get(reason)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = Metrics::new();
        m.inc_accepted(Some("t"));
        m.inc_accepted(Some("t"));
        m.inc_shed("bus_full", Some("t"));
        assert_eq!(m.accepted_total.load(Ordering::Relaxed), 2);
        assert_eq!(m.shed_count("bus_full"), 1);
    }

    #[test]
    fn render_contains_expected_lines() {
        let m = Metrics::new();
        m.inc_accepted(None);
        m.inc_shed("rate_limit", None);
        let text = m.render();
        assert!(text.contains("percept_accepted_total 1"));
        assert!(text.contains("percept_shed_total{reason=\"rate_limit\"} 1"));
    }
}
