//! Compiled JSON Schema lookup for `semantic` payload validation.
//!
//! DESIGN §3.3 / §5.2: a SourceDescriptor's `semantic_schema` fully replaces
//! the KindDescriptor's. The lookup order is:
//!
//! 1. Source override `(source_id, kind_name)` — full replace if present.
//! 2. Kind+version `(kind_name, version)` — version is the producer's pin
//!    or the latest registered version.
//!
//! Slice 1.5 supports a single schema per source / per kind; the per-kind
//! map shape DESIGN §3.2 hints at is deferred.

use std::collections::HashMap;
use std::sync::Arc;

use jsonschema::JSONSchema;
use percept_core::{KindDescriptor, KindRef, SourceDescriptor};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("source {source_id:?}: invalid JSON Schema: {detail}")]
    InvalidSourceSchema { source_id: String, detail: String },
    #[error("kind {kind:?}@{version}: invalid JSON Schema: {detail}")]
    InvalidKindSchema {
        kind: String,
        version: String,
        detail: String,
    },
}

#[derive(Default)]
pub struct SchemaIndex {
    /// Source-level override, full replace of the kind's schema for any
    /// kind this source emits.
    source: HashMap<String, Arc<JSONSchema>>,
    /// `(kind_name, version)` → schema.
    kind: HashMap<(String, String), Arc<JSONSchema>>,
    /// `kind_name` → latest registered version (string-sorted descending).
    latest_version: HashMap<String, String>,
}

impl SchemaIndex {
    /// Build by compiling every `semantic_schema` in the supplied descriptors.
    /// Returns the first compilation error; configs are validated at startup,
    /// so a malformed schema is an operator bug, not a runtime concern.
    pub fn build(
        sources: &[SourceDescriptor],
        kinds: &[KindDescriptor],
    ) -> Result<Self, SchemaError> {
        let mut idx = Self::default();

        for s in sources {
            if let Some(schema_value) = &s.semantic_schema {
                let compiled = JSONSchema::compile(schema_value).map_err(|e| {
                    SchemaError::InvalidSourceSchema {
                        source_id: s.source_id.clone(),
                        detail: e.to_string(),
                    }
                })?;
                idx.source.insert(s.source_id.clone(), Arc::new(compiled));
            }
        }

        for k in kinds {
            if let Some(schema_value) = &k.semantic_schema {
                let compiled = JSONSchema::compile(schema_value).map_err(|e| {
                    SchemaError::InvalidKindSchema {
                        kind: k.kind.clone(),
                        version: k.version.clone(),
                        detail: e.to_string(),
                    }
                })?;
                idx.kind
                    .insert((k.kind.clone(), k.version.clone()), Arc::new(compiled));
            }
            // Track the lexicographically-greatest version per kind. Versions
            // are short strings like "v1", "v2", so string ordering works
            // (until v10 — defer the natural-sort fix until we see it).
            let prev = idx.latest_version.get(&k.kind).cloned();
            if prev.as_deref().is_none_or(|p| p < k.version.as_str()) {
                idx.latest_version.insert(k.kind.clone(), k.version.clone());
            }
        }

        Ok(idx)
    }

    /// Resolve `(source_id, raw_kind)` to a compiled schema if any applies.
    /// `raw_kind` is the producer-supplied `kind` string ("name" or "name@vN").
    #[must_use]
    pub fn lookup(&self, source_id: &str, raw_kind: &str) -> Option<Arc<JSONSchema>> {
        // Source override wins unconditionally.
        if let Some(s) = self.source.get(source_id) {
            return Some(Arc::clone(s));
        }
        let kref: KindRef = raw_kind.parse().ok()?;
        let version = kref
            .version
            .clone()
            .or_else(|| self.latest_version.get(&kref.name).cloned())?;
        self.kind.get(&(kref.name, version)).map(Arc::clone)
    }

    /// `Some(true)` if a schema applied and the payload failed; `Some(false)`
    /// if a schema applied and the payload passed; `None` if no schema
    /// applies to this `(source_id, raw_kind)`.
    #[must_use]
    pub fn validate(
        &self,
        source_id: &str,
        raw_kind: &str,
        semantic: &serde_json::Value,
    ) -> Option<bool> {
        let schema = self.lookup(source_id, raw_kind)?;
        Some(!schema.is_valid(semantic))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn kind(name: &str, version: &str, schema: Option<serde_json::Value>) -> KindDescriptor {
        KindDescriptor {
            kind: name.to_string(),
            version: version.to_string(),
            description: String::new(),
            usage: String::new(),
            caveats: String::new(),
            semantic_schema: schema,
            units: None,
            updated_ts_ms_utc: 0,
        }
    }

    fn source(id: &str, kinds: &[&str], schema: Option<serde_json::Value>) -> SourceDescriptor {
        SourceDescriptor {
            source_id: id.to_string(),
            kinds: kinds.iter().map(|s| (*s).to_string()).collect(),
            description: String::new(),
            usage: String::new(),
            caveats: String::new(),
            semantic_schema: schema,
            units: None,
            sampling_hint_ms: None,
            freshness_ttl_ms: None,
            location: None,
            updated_ts_ms_utc: 0,
        }
    }

    #[test]
    fn no_schema_means_no_check() {
        let idx = SchemaIndex::build(&[], &[]).unwrap();
        assert_eq!(idx.validate("s", "k", &json!({})), None);
    }

    #[test]
    fn kind_schema_applied_when_no_source_override() {
        let k = kind(
            "temperature",
            "v1",
            Some(json!({ "type": "object", "required": ["celsius"] })),
        );
        let idx = SchemaIndex::build(&[], &[k]).unwrap();
        assert_eq!(
            idx.validate("any-src", "temperature", &json!({ "celsius": 20 })),
            Some(false)
        );
        assert_eq!(
            idx.validate("any-src", "temperature", &json!({})),
            Some(true)
        );
    }

    #[test]
    fn source_schema_fully_replaces_kind_schema() {
        let k = kind(
            "temperature",
            "v1",
            Some(json!({ "type": "object", "required": ["celsius"] })),
        );
        let s = source(
            "therm.kitchen",
            &["temperature"],
            Some(json!({ "type": "object", "required": ["fahrenheit"] })),
        );
        let idx = SchemaIndex::build(&[s], &[k]).unwrap();

        // Source's schema (fahrenheit required) is the only one consulted.
        assert_eq!(
            idx.validate("therm.kitchen", "temperature", &json!({ "fahrenheit": 70 })),
            Some(false)
        );
        // Payload that would have passed kind's schema (has celsius) still
        // fails because source schema demands fahrenheit.
        assert_eq!(
            idx.validate("therm.kitchen", "temperature", &json!({ "celsius": 20 })),
            Some(true)
        );
    }

    #[test]
    fn unversioned_kind_resolves_to_latest_version() {
        let v1 = kind("k", "v1", Some(json!({ "const": "old" })));
        let v2 = kind("k", "v2", Some(json!({ "const": "new" })));
        let idx = SchemaIndex::build(&[], &[v1, v2]).unwrap();
        // Unversioned -> v2 -> requires "new"
        assert_eq!(idx.validate("any", "k", &json!("new")), Some(false));
        assert_eq!(idx.validate("any", "k", &json!("old")), Some(true));
    }

    #[test]
    fn versioned_kind_pins_to_exact_version() {
        let v1 = kind("k", "v1", Some(json!({ "const": "old" })));
        let v2 = kind("k", "v2", Some(json!({ "const": "new" })));
        let idx = SchemaIndex::build(&[], &[v1, v2]).unwrap();
        assert_eq!(idx.validate("any", "k@v1", &json!("old")), Some(false));
        assert_eq!(idx.validate("any", "k@v1", &json!("new")), Some(true));
    }

    #[test]
    fn malformed_schema_fails_at_build_time() {
        let bad = kind("k", "v1", Some(json!({ "type": 42 })));
        match SchemaIndex::build(&[], &[bad]) {
            Err(SchemaError::InvalidKindSchema { .. }) => {}
            other => panic!(
                "expected InvalidKindSchema, got {}",
                if other.is_ok() {
                    "Ok"
                } else {
                    "different error"
                }
            ),
        }
    }
}
