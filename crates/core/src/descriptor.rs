use serde::{Deserialize, Serialize};

/// Per-instance descriptor: "this camera, this thermometer".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDescriptor {
    pub source_id: String,
    pub kinds: Vec<String>,
    pub description: String,
    pub usage: String,
    pub caveats: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_schema: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub units: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_hint_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_ttl_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    pub updated_ts_ms_utc: i64,
}

/// Per-ontology-tag descriptor: "what `temperature` means here".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KindDescriptor {
    pub kind: String,
    pub version: String,
    pub description: String,
    pub usage: String,
    pub caveats: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_schema: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub units: Option<String>,

    pub updated_ts_ms_utc: i64,
}

/// Merged view shown to the LLM for a given `(source, kind)` pair.
///
/// Merge rule (DESIGN §3.3): source overrides kind for non-schema fields.
/// `semantic_schema` is **full replace** — if the source defines one it
/// replaces the kind's outright; no per-field merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedDescriptor {
    pub source_id: String,
    pub kind: String,
    pub kind_version: String,
    pub description: String,
    pub usage: String,
    pub caveats: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_schema: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub units: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling_hint_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness_ttl_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// Merge a SourceDescriptor and a KindDescriptor into the LLM-visible view.
///
/// Non-schema fields: source's value wins when non-empty (`description`,
/// `usage`, `caveats`, `units`); otherwise the kind's value is used.
/// `semantic_schema`: source's, if present, fully replaces kind's.
#[must_use]
pub fn resolve(source: &SourceDescriptor, kind: &KindDescriptor) -> ResolvedDescriptor {
    fn pick(src: &str, fallback: &str) -> String {
        if src.is_empty() {
            fallback.to_string()
        } else {
            src.to_string()
        }
    }

    ResolvedDescriptor {
        source_id: source.source_id.clone(),
        kind: kind.kind.clone(),
        kind_version: kind.version.clone(),
        description: pick(&source.description, &kind.description),
        usage: pick(&source.usage, &kind.usage),
        caveats: pick(&source.caveats, &kind.caveats),
        semantic_schema: source
            .semantic_schema
            .clone()
            .or_else(|| kind.semantic_schema.clone()),
        units: source.units.clone().or_else(|| kind.units.clone()),
        sampling_hint_ms: source.sampling_hint_ms,
        freshness_ttl_ms: source.freshness_ttl_ms,
        location: source.location.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn kind(name: &str) -> KindDescriptor {
        KindDescriptor {
            kind: name.to_string(),
            version: "v1".to_string(),
            description: "kind-desc".to_string(),
            usage: "kind-usage".to_string(),
            caveats: "kind-caveats".to_string(),
            semantic_schema: Some(json!({ "type": "number" })),
            units: Some("kelvin".to_string()),
            updated_ts_ms_utc: 0,
        }
    }

    fn source(id: &str) -> SourceDescriptor {
        SourceDescriptor {
            source_id: id.to_string(),
            kinds: vec!["temperature".into()],
            description: "src-desc".to_string(),
            usage: "src-usage".to_string(),
            caveats: "src-caveats".to_string(),
            semantic_schema: None,
            units: None,
            sampling_hint_ms: None,
            freshness_ttl_ms: Some(60_000),
            location: Some("kitchen".to_string()),
            updated_ts_ms_utc: 0,
        }
    }

    #[test]
    fn source_overrides_non_schema_fields() {
        let r = resolve(&source("s"), &kind("temperature"));
        assert_eq!(r.description, "src-desc");
        assert_eq!(r.usage, "src-usage");
        assert_eq!(r.caveats, "src-caveats");
    }

    #[test]
    fn falls_back_to_kind_when_source_is_empty() {
        let mut s = source("s");
        s.description.clear();
        s.usage.clear();
        s.caveats.clear();
        let r = resolve(&s, &kind("temperature"));
        assert_eq!(r.description, "kind-desc");
        assert_eq!(r.usage, "kind-usage");
        assert_eq!(r.caveats, "kind-caveats");
    }

    #[test]
    fn source_schema_replaces_kind_schema() {
        let mut s = source("s");
        s.semantic_schema = Some(json!({ "type": "string" }));
        let r = resolve(&s, &kind("temperature"));
        assert_eq!(r.semantic_schema, Some(json!({ "type": "string" })));
    }

    #[test]
    fn kind_schema_used_when_source_has_none() {
        let r = resolve(&source("s"), &kind("temperature"));
        assert_eq!(r.semantic_schema, Some(json!({ "type": "number" })));
    }

    #[test]
    fn source_units_override_kind_units() {
        let mut s = source("s");
        s.units = Some("celsius".to_string());
        let r = resolve(&s, &kind("temperature"));
        assert_eq!(r.units.as_deref(), Some("celsius"));
    }

    #[test]
    fn descriptor_roundtrip() {
        let s = source("cam.front");
        let blob = serde_json::to_string(&s).unwrap();
        let back: SourceDescriptor = serde_json::from_str(&blob).unwrap();
        assert_eq!(s, back);

        let k = kind("object_detected");
        let blob = serde_json::to_string(&k).unwrap();
        let back: KindDescriptor = serde_json::from_str(&blob).unwrap();
        assert_eq!(k, back);
    }
}
