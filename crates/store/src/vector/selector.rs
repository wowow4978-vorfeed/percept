//! Decides whether a given `(source_id, kind)` should be embedded.
//!
//! DECISIONS §2: opt-in per kind / source; source overrides kind; default
//! is `embed_default = false`.

use std::collections::HashMap;

pub struct EmbedSelector {
    embed_default: bool,
    by_kind: HashMap<String, bool>,
    by_source: HashMap<String, bool>,
}

impl EmbedSelector {
    #[must_use]
    pub fn new(embed_default: bool) -> Self {
        Self {
            embed_default,
            by_kind: HashMap::new(),
            by_source: HashMap::new(),
        }
    }

    pub fn set_kind(&mut self, kind: impl Into<String>, embed: bool) {
        self.by_kind.insert(kind.into(), embed);
    }

    pub fn set_source(&mut self, source_id: impl Into<String>, embed: bool) {
        self.by_source.insert(source_id.into(), embed);
    }

    #[must_use]
    pub fn should_embed(&self, source_id: &str, kind: &str) -> bool {
        if let Some(v) = self.by_source.get(source_id) {
            return *v;
        }
        // Treat `kind@vN` and the bare name interchangeably for opt-in.
        let bare_kind = kind.split_once('@').map_or(kind, |(n, _)| n);
        if let Some(v) = self.by_kind.get(bare_kind) {
            return *v;
        }
        self.embed_default
    }
}

impl Default for EmbedSelector {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_off() {
        let s = EmbedSelector::default();
        assert!(!s.should_embed("any", "any"));
    }

    #[test]
    fn default_on_picks_everything() {
        let s = EmbedSelector::new(true);
        assert!(s.should_embed("any", "any"));
    }

    #[test]
    fn kind_override_wins_over_default() {
        let mut s = EmbedSelector::new(false);
        s.set_kind("scene", true);
        assert!(s.should_embed("any", "scene"));
        assert!(!s.should_embed("any", "temperature"));
    }

    #[test]
    fn source_override_wins_over_kind() {
        let mut s = EmbedSelector::new(false);
        s.set_kind("scene", true);
        s.set_source("cam.silent", false);
        // kind says yes, but source override says no.
        assert!(!s.should_embed("cam.silent", "scene"));
    }

    #[test]
    fn versioned_kind_resolves_to_bare_name() {
        let mut s = EmbedSelector::new(false);
        s.set_kind("scene", true);
        assert!(s.should_embed("any", "scene@v2"));
    }
}
