//! Hand-rolled Prometheus-text metrics for Slice 1.
//!
//! Counters only — gauges/histograms can be added in later slices when
//! there's something to measure. Keeps the dep footprint at zero.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use parking_lot::RwLock;

#[derive(Default)]
pub struct Metrics {
    pub accepted_total: AtomicU64,
    pub oversized_soft_total: AtomicU64,
    pub schema_invalid_total: AtomicU64,
    shed_by_reason: RwLock<HashMap<String, AtomicU64>>,
    per_token_accepted: RwLock<HashMap<String, AtomicU64>>,
    per_token_shed: RwLock<HashMap<String, AtomicU64>>,
    per_source_errors: RwLock<HashMap<String, SourceErrors>>,
    mcp_calls: RwLock<HashMap<String, McpMethodStats>>,
    consumer_drops: RwLock<HashMap<String, AtomicU64>>,
}

#[derive(Default)]
struct McpMethodStats {
    count: AtomicU64,
    /// Sum of observed latencies in ms; combine with `count` for a mean.
    /// True percentiles need a histogram — defer until slice budget allows.
    total_latency_ms: AtomicU64,
}

#[derive(Default)]
struct SourceErrors {
    by_reason: HashMap<String, AtomicU64>,
    last_error_ts_ms_utc: AtomicI64,
}

/// Per-source digest surfaced by `describe_sources` in the MCP layer.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RecentErrors {
    pub counters: HashMap<String, u64>,
    pub last_error_ts_ms_utc: i64,
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

    /// Event dropped on the way to a downstream consumer (e.g. cold writer
    /// channel full).
    pub fn inc_consumer_drop(&self, consumer: &str) {
        self.inc_in_map(&self.consumer_drops, consumer);
    }

    #[must_use]
    pub fn consumer_drop_count(&self, consumer: &str) -> u64 {
        self.consumer_drops
            .read()
            .get(consumer)
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Record an error (shed or schema-fail) against a known `source_id`.
    /// Called only when the source_id parsed from the request — auth
    /// failures on a malformed payload don't have one.
    pub fn inc_source_error(&self, source_id: &str, reason: &str, now_ms: i64) {
        // Fast path: source + reason both already known.
        {
            let read = self.per_source_errors.read();
            if let Some(s) = read.get(source_id) {
                if let Some(c) = s.by_reason.get(reason) {
                    c.fetch_add(1, Ordering::Relaxed);
                    s.last_error_ts_ms_utc.store(now_ms, Ordering::Relaxed);
                    return;
                }
            }
        }
        // Slow path: insert source or reason (needs `&mut` on the inner map).
        let mut write = self.per_source_errors.write();
        let entry = write.entry(source_id.to_string()).or_default();
        let counter = entry
            .by_reason
            .entry(reason.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed);
        entry.last_error_ts_ms_utc.store(now_ms, Ordering::Relaxed);
    }

    /// Record an MCP tool/method call and its observed wall-time.
    pub fn inc_mcp_call(&self, method: &str, latency_ms: u64) {
        {
            let read = self.mcp_calls.read();
            if let Some(s) = read.get(method) {
                s.count.fetch_add(1, Ordering::Relaxed);
                s.total_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
                return;
            }
        }
        let mut write = self.mcp_calls.write();
        let entry = write.entry(method.to_string()).or_default();
        entry.count.fetch_add(1, Ordering::Relaxed);
        entry
            .total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
    }

    #[must_use]
    pub fn mcp_call_count(&self, method: &str) -> u64 {
        self.mcp_calls
            .read()
            .get(method)
            .map_or(0, |s| s.count.load(Ordering::Relaxed))
    }

    /// Digest used by `describe_sources` for the `recent_errors` field.
    #[must_use]
    pub fn recent_errors(&self, source_id: &str) -> Option<RecentErrors> {
        let map = self.per_source_errors.read();
        let s = map.get(source_id)?;
        let counters = s
            .by_reason
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .filter(|(_, n)| *n > 0)
            .collect::<HashMap<_, _>>();
        if counters.is_empty() {
            return None;
        }
        Some(RecentErrors {
            counters,
            last_error_ts_ms_utc: s.last_error_ts_ms_utc.load(Ordering::Relaxed),
        })
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

        let _ = writeln!(out, "# HELP percept_mcp_calls_total MCP method call count.");
        let _ = writeln!(out, "# TYPE percept_mcp_calls_total counter");
        let _ = writeln!(
            out,
            "# HELP percept_mcp_latency_ms_sum Sum of MCP method wall-time in ms."
        );
        let _ = writeln!(out, "# TYPE percept_mcp_latency_ms_sum counter");
        for (method, s) in self.mcp_calls.read().iter() {
            let _ = writeln!(
                out,
                "percept_mcp_calls_total{{method=\"{method}\"}} {}",
                s.count.load(Ordering::Relaxed)
            );
            let _ = writeln!(
                out,
                "percept_mcp_latency_ms_sum{{method=\"{method}\"}} {}",
                s.total_latency_ms.load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP percept_consumer_drops_total Events dropped on the way to a downstream consumer."
        );
        let _ = writeln!(out, "# TYPE percept_consumer_drops_total counter");
        for (consumer, c) in self.consumer_drops.read().iter() {
            let _ = writeln!(
                out,
                "percept_consumer_drops_total{{consumer=\"{consumer}\"}} {}",
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
