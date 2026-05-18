//! Pure routing: (topic, payload) → IngestEvent for a configured set of
//! subscriptions. No I/O — the subscriber task wires this to the live
//! rumqttc stream.

use percept_core::Link;
use serde_json::Value;

use crate::wire::IngestEvent;

use super::decode::{decode, DecodeError, PayloadFormat};
use super::topic::{render, TopicMatcher};

#[derive(Debug, Clone)]
pub struct Subscription {
    pub topic_filter: String,
    pub source_id_template: String,
    /// Static kind for every message on this subscription.
    pub kind: Option<String>,
    /// Or a JSONPath into the decoded payload (RFC 9535).
    pub kind_field: Option<String>,
    pub payload: PayloadFormat,
}

pub struct CompiledSubscription {
    pub topic_filter: String,
    pub source_id_template: String,
    pub kind: Option<String>,
    /// Compiled JSONPath (parsed at config-load) plus the original
    /// source string for diagnostics.
    pub kind_field: Option<(jsonpath_rust::JsonPath, String)>,
    pub payload: PayloadFormat,
    matcher: TopicMatcher,
}

#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error("invalid JSONPath {0:?}: {1}")]
    BadJsonPath(String, String),
}

impl CompiledSubscription {
    pub fn compile(sub: Subscription) -> Result<Self, CompileError> {
        let matcher = TopicMatcher::new(&sub.topic_filter);
        let kind_field = match sub.kind_field {
            Some(p) => {
                let parsed = jsonpath_rust::JsonPath::try_from(p.as_str())
                    .map_err(|e| CompileError::BadJsonPath(p.clone(), e.to_string()))?;
                Some((parsed, p))
            }
            None => None,
        };
        Ok(Self {
            topic_filter: sub.topic_filter,
            source_id_template: sub.source_id_template,
            kind: sub.kind,
            kind_field,
            payload: sub.payload,
            matcher,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("topic {0:?} matched no subscription")]
    NoMatch(String),
    #[error("template render failed: {0}")]
    Template(#[from] super::topic::TemplateError),
    #[error("payload decode failed: {0}")]
    Decode(#[from] DecodeError),
    #[error("kind unresolved: no static kind and JSONPath {0:?} produced no string value")]
    UnresolvedKind(String),
}

/// Find the first subscription whose filter matches `topic` and build an
/// `IngestEvent` from `(topic, payload)`. Returns `RouteError::NoMatch`
/// when no subscription covers the topic — the caller increments
/// `unresolved_kind` / drops with the appropriate metric.
pub fn route(
    subs: &[CompiledSubscription],
    topic: &str,
    payload: &[u8],
    ts_ms_utc: i64,
    producer_id: Option<&str>,
) -> Result<IngestEvent, RouteError> {
    for sub in subs {
        let Some(captures) = sub.matcher.captures(topic) else {
            continue;
        };
        let source_id = render(&sub.source_id_template, &captures)?;
        let semantic = decode(sub.payload, payload)?;
        let kind = resolve_kind(sub, &semantic)?;
        return Ok(IngestEvent {
            event_id: None,
            source_id,
            kind,
            ts_ms_utc,
            semantic,
            links: Option::<Vec<Link>>::None,
            trace_id: None,
            producer_id: producer_id.map(String::from),
        });
    }
    Err(RouteError::NoMatch(topic.to_string()))
}

fn resolve_kind(sub: &CompiledSubscription, semantic: &Value) -> Result<String, RouteError> {
    if let Some(k) = &sub.kind {
        return Ok(k.clone());
    }
    if let Some((path, raw)) = &sub.kind_field {
        let found = path.find_slice(semantic);
        for node in found {
            if let Some(v) = jsonpath_value_as_value(&node) {
                if let Some(s) = v.as_str() {
                    return Ok(s.to_string());
                }
            }
        }
        return Err(RouteError::UnresolvedKind(raw.clone()));
    }
    // Neither static kind nor kind_field: caller should have rejected at
    // config-load. Treat as unresolved at runtime to avoid panic.
    Err(RouteError::UnresolvedKind(String::new()))
}

/// Extract a `&serde_json::Value` from a `JsonPathValue`. Borrowed slices
/// are the common case; constructed values (the spec's `Filter` etc.)
/// don't come up for our string-extraction use.
fn jsonpath_value_as_value<'a>(
    node: &'a jsonpath_rust::JsonPathValue<'a, Value>,
) -> Option<&'a Value> {
    match node {
        jsonpath_rust::JsonPathValue::Slice(v, _) => Some(v),
        jsonpath_rust::JsonPathValue::NewValue(v) => Some(v),
        jsonpath_rust::JsonPathValue::NoValue => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compile(sub: Subscription) -> CompiledSubscription {
        CompiledSubscription::compile(sub).unwrap()
    }

    fn sub_default() -> Subscription {
        Subscription {
            topic_filter: "home/+/temp".into(),
            source_id_template: "temp.{+1}".into(),
            kind: Some("temperature".into()),
            kind_field: None,
            payload: PayloadFormat::Json,
        }
    }

    #[test]
    fn routes_with_static_kind() {
        let subs = vec![compile(sub_default())];
        let ev = route(&subs, "home/kitchen/temp", br#"{"c": 20}"#, 1700, None).unwrap();
        assert_eq!(ev.source_id, "temp.kitchen");
        assert_eq!(ev.kind, "temperature");
        assert_eq!(ev.semantic["c"], 20);
        assert_eq!(ev.ts_ms_utc, 1700);
    }

    #[test]
    fn unmatched_topic_errors() {
        let subs = vec![compile(sub_default())];
        let err = route(&subs, "weather/outside", b"{}", 0, None).unwrap_err();
        assert!(matches!(err, RouteError::NoMatch(_)));
    }

    #[test]
    fn resolves_kind_from_jsonpath() {
        let mut s = sub_default();
        s.topic_filter = "cams/+/events".into();
        s.source_id_template = "cam.{+1}".into();
        s.kind = None;
        s.kind_field = Some("$.event_type".into());
        let subs = vec![compile(s)];
        let ev = route(
            &subs,
            "cams/front/events",
            br#"{"event_type": "object_detected"}"#,
            0,
            None,
        )
        .unwrap();
        assert_eq!(ev.kind, "object_detected");
    }

    #[test]
    fn missing_kind_field_errors_at_route_time() {
        let mut s = sub_default();
        s.topic_filter = "cams/+/events".into();
        s.source_id_template = "cam.{+1}".into();
        s.kind = None;
        s.kind_field = Some("$.event_type".into());
        let subs = vec![compile(s)];
        let err = route(&subs, "cams/front/events", b"{}", 0, None).unwrap_err();
        assert!(matches!(err, RouteError::UnresolvedKind(_)));
    }

    #[test]
    fn raw_payload_decodes_to_base64_shape() {
        let mut s = sub_default();
        s.payload = PayloadFormat::Raw;
        let subs = vec![compile(s)];
        let ev = route(&subs, "home/kitchen/temp", &[0xff, 0x00], 0, None).unwrap();
        assert_eq!(ev.semantic["encoding"], "raw");
    }

    #[test]
    fn invalid_jsonpath_rejected_at_compile_time() {
        let s = Subscription {
            topic_filter: "x".into(),
            source_id_template: "x".into(),
            kind: None,
            kind_field: Some("not-a-jsonpath".into()),
            payload: PayloadFormat::Json,
        };
        match CompiledSubscription::compile(s) {
            Err(CompileError::BadJsonPath(_, _)) => {}
            Ok(_) => panic!("expected compile error, got Ok"),
        }
    }

    #[test]
    fn first_subscription_wins_on_overlap() {
        let a = compile(Subscription {
            topic_filter: "a/+".into(),
            source_id_template: "first.{+1}".into(),
            kind: Some("k1".into()),
            kind_field: None,
            payload: PayloadFormat::Json,
        });
        let b = compile(Subscription {
            topic_filter: "+/+".into(),
            source_id_template: "second.{+1}.{+2}".into(),
            kind: Some("k2".into()),
            kind_field: None,
            payload: PayloadFormat::Json,
        });
        let ev = route(&[a, b], "a/x", b"{}", 0, None).unwrap();
        assert_eq!(ev.source_id, "first.x");
        assert_eq!(ev.kind, "k1");
    }

    #[test]
    fn kind_field_must_resolve_to_string_not_number() {
        let mut s = sub_default();
        s.topic_filter = "k/+".into();
        s.source_id_template = "s.{+1}".into();
        s.kind = None;
        s.kind_field = Some("$.k".into());
        let subs = vec![compile(s)];
        let err = route(&subs, "k/x", br#"{"k": 42}"#, 0, None).unwrap_err();
        assert!(matches!(err, RouteError::UnresolvedKind(_)));
    }

    #[test]
    fn ts_and_producer_propagate() {
        let subs = vec![compile(sub_default())];
        let ev = route(
            &subs,
            "home/kitchen/temp",
            br#"{}"#,
            999,
            Some("mqtt-bridge"),
        )
        .unwrap();
        assert_eq!(ev.ts_ms_utc, 999);
        assert_eq!(ev.producer_id.as_deref(), Some("mqtt-bridge"));
    }
}
