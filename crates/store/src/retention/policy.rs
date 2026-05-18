//! Retention policy types + resolver.
//!
//! DESIGN §11.3: policies are composable per (source ∪ kind); resolution
//! is source-level → kind-level → global default with first-match-wins per
//! dimension. Slice-0 already rejects `vector_max_age > max_age` at config
//! load.

use std::time::Duration;

/// One configured `[[retention]]` block.
#[derive(Debug, Clone, Default)]
pub struct RetentionPolicy {
    pub match_source_id: Option<String>,
    pub match_kind: Option<String>,
    pub max_age: Option<Duration>,
    pub max_count: Option<i64>,
    pub max_bytes: Option<i64>,
    pub vector_max_age: Option<Duration>,
}

impl RetentionPolicy {
    /// Does this entry apply to events from `(source_id, kind)`?
    #[must_use]
    pub fn matches(&self, source_id: &str, kind: &str) -> bool {
        let source_ok = self
            .match_source_id
            .as_deref()
            .is_none_or(|s| s == source_id);
        let kind_match = self.match_kind.as_deref();
        let kind_ok = kind_match.is_none_or(|k| {
            // Slice 4 introduced versioned kinds ("name@vN"). Match by
            // bare name so a retention rule for "ble.advert" still picks
            // up "ble.advert@v2".
            let bare = kind.split_once('@').map_or(kind, |(n, _)| n);
            k == bare || k == kind
        });
        source_ok && kind_ok
    }

    /// Specificity tier for ordering at resolution time: lower = more
    /// specific. source-level (any source_id match) beats kind-level beats
    /// global default.
    #[must_use]
    pub fn tier(&self) -> u8 {
        match (self.match_source_id.is_some(), self.match_kind.is_some()) {
            (true, _) => 0,
            (false, true) => 1,
            (false, false) => 2,
        }
    }
}

/// The resolved per-(source, kind) policy after walking the configured
/// list. Each dimension is whatever the first matching, dimension-set
/// policy provided.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub struct EffectiveRetention {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_max_age_ms: Option<i64>,
}

impl EffectiveRetention {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.max_age_ms.is_none()
            && self.max_count.is_none()
            && self.max_bytes.is_none()
            && self.vector_max_age_ms.is_none()
    }
}

#[must_use]
pub fn resolve_effective(
    policies: &[RetentionPolicy],
    source_id: &str,
    kind: &str,
) -> EffectiveRetention {
    let mut matching: Vec<&RetentionPolicy> = policies
        .iter()
        .filter(|p| p.matches(source_id, kind))
        .collect();
    matching.sort_by_key(|p| p.tier());

    let mut eff = EffectiveRetention::default();
    for p in matching {
        if eff.max_age_ms.is_none() {
            eff.max_age_ms = p.max_age.map(duration_to_ms);
        }
        if eff.max_count.is_none() {
            eff.max_count = p.max_count;
        }
        if eff.max_bytes.is_none() {
            eff.max_bytes = p.max_bytes;
        }
        if eff.vector_max_age_ms.is_none() {
            eff.vector_max_age_ms = p.vector_max_age.map(duration_to_ms);
        }
    }
    eff
}

fn duration_to_ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(src: Option<&str>, kind: Option<&str>, max_age_secs: Option<u64>) -> RetentionPolicy {
        RetentionPolicy {
            match_source_id: src.map(String::from),
            match_kind: kind.map(String::from),
            max_age: max_age_secs.map(Duration::from_secs),
            ..Default::default()
        }
    }

    #[test]
    fn matches_source_only() {
        let pol = p(Some("cam.front"), None, Some(86_400));
        assert!(pol.matches("cam.front", "anything"));
        assert!(!pol.matches("cam.back", "anything"));
    }

    #[test]
    fn matches_kind_only() {
        let pol = p(None, Some("ble.advert"), Some(86_400));
        assert!(pol.matches("any.source", "ble.advert"));
        assert!(!pol.matches("any.source", "temperature"));
    }

    #[test]
    fn matches_kind_ignores_version_suffix() {
        let pol = p(None, Some("ble.advert"), Some(86_400));
        assert!(pol.matches("any", "ble.advert@v2"));
    }

    #[test]
    fn resolve_picks_source_over_kind() {
        let policies = vec![
            p(None, Some("temperature"), Some(7 * 86_400)),
            p(Some("therm.kitchen"), None, Some(30 * 86_400)),
        ];
        let eff = resolve_effective(&policies, "therm.kitchen", "temperature");
        // Source wins for max_age.
        assert_eq!(eff.max_age_ms, Some(30 * 86_400 * 1000));
    }

    #[test]
    fn resolve_falls_back_to_kind_when_source_does_not_set_dimension() {
        let mut src_pol = p(Some("therm.kitchen"), None, None);
        src_pol.max_count = Some(100);
        let kind_pol = p(None, Some("temperature"), Some(86_400));
        let eff = resolve_effective(&[src_pol, kind_pol], "therm.kitchen", "temperature");
        assert_eq!(eff.max_count, Some(100));
        assert_eq!(eff.max_age_ms, Some(86_400_000));
    }

    #[test]
    fn resolve_global_default_picks_up_unset_dimensions() {
        let global = p(None, None, Some(3600));
        let eff = resolve_effective(&[global], "any", "any");
        assert_eq!(eff.max_age_ms, Some(3_600_000));
    }

    #[test]
    fn empty_policies_yields_empty_effective() {
        let eff = resolve_effective(&[], "any", "any");
        assert!(eff.is_empty());
    }
}
