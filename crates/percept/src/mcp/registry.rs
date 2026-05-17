//! Built from `Config` at startup; serves the descriptor data the MCP
//! tools return. Stores the eager (source, kind) -> ResolvedDescriptor
//! cross-product so `describe_sources` is a flat scan with glob filters.

use std::collections::HashMap;

use globset::{Glob, GlobSet, GlobSetBuilder};
use percept_core::ResolvedDescriptor;

pub struct DescriptorRegistry {
    /// One row per (source_id, kind) pair declared by config.
    rows: Vec<ResolvedDescriptor>,
    /// Quick lookup for `get_current_state`'s per-result descriptor and
    /// `freshness_ttl_ms` derivation.
    by_pair: HashMap<(String, String), usize>,
}

impl DescriptorRegistry {
    #[must_use]
    pub fn new(rows: Vec<ResolvedDescriptor>) -> Self {
        let by_pair = rows
            .iter()
            .enumerate()
            .map(|(i, r)| ((r.source_id.clone(), r.kind.clone()), i))
            .collect();
        Self { rows, by_pair }
    }

    #[must_use]
    pub fn all(&self) -> &[ResolvedDescriptor] {
        &self.rows
    }

    #[must_use]
    pub fn lookup(&self, source_id: &str, kind: &str) -> Option<&ResolvedDescriptor> {
        let idx = self
            .by_pair
            .get(&(source_id.to_string(), kind.to_string()))?;
        Some(&self.rows[*idx])
    }

    /// Filter rows by optional source / kind globs. An empty filter matches
    /// everything; otherwise at least one glob must match.
    pub fn filter(
        &self,
        source_filter: Option<&[String]>,
        kind_filter: Option<&[String]>,
    ) -> Result<Vec<&ResolvedDescriptor>, globset::Error> {
        let src = compile(source_filter)?;
        let kind = compile(kind_filter)?;
        Ok(self
            .rows
            .iter()
            .filter(|r| matches_or_empty(src.as_ref(), &r.source_id))
            .filter(|r| matches_or_empty(kind.as_ref(), &r.kind))
            .collect())
    }
}

fn compile(patterns: Option<&[String]>) -> Result<Option<GlobSet>, globset::Error> {
    let Some(patterns) = patterns else {
        return Ok(None);
    };
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        b.add(Glob::new(p)?);
    }
    Ok(Some(b.build()?))
}

fn matches_or_empty(set: Option<&GlobSet>, s: &str) -> bool {
    set.is_none_or(|g| g.is_match(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rd(src: &str, kind: &str) -> ResolvedDescriptor {
        ResolvedDescriptor {
            source_id: src.to_string(),
            kind: kind.to_string(),
            kind_version: "v1".to_string(),
            description: String::new(),
            usage: String::new(),
            caveats: String::new(),
            semantic_schema: None,
            units: None,
            sampling_hint_ms: None,
            freshness_ttl_ms: None,
            location: None,
        }
    }

    #[test]
    fn lookup_returns_match() {
        let r = DescriptorRegistry::new(vec![rd("a", "k"), rd("b", "k")]);
        assert!(r.lookup("a", "k").is_some());
        assert!(r.lookup("a", "missing").is_none());
    }

    #[test]
    fn filter_no_filters_returns_all() {
        let r = DescriptorRegistry::new(vec![rd("a", "k"), rd("b", "j")]);
        assert_eq!(r.filter(None, None).unwrap().len(), 2);
    }

    #[test]
    fn filter_source_glob_matches() {
        let r = DescriptorRegistry::new(vec![
            rd("cam.front", "k"),
            rd("cam.back", "k"),
            rd("therm.kitchen", "k"),
        ]);
        let matched = r.filter(Some(&["cam.*".to_string()]), None).unwrap();
        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn filter_kind_glob_matches() {
        let r = DescriptorRegistry::new(vec![
            rd("a", "ble.advert"),
            rd("a", "ble.sample"),
            rd("a", "temperature"),
        ]);
        let matched = r.filter(None, Some(&["ble.*".to_string()])).unwrap();
        assert_eq!(matched.len(), 2);
    }
}
