//! Producer-facing wire types for HTTP `/ingest`.
//!
//! `IngestPayload` is `untagged`, so producers can send a single event as a
//! JSON object or a batch as `{"events": [...]}`. Bare arrays are also
//! accepted to match the convenience that DESIGN §5.1 calls out.

use percept_core::Link;
use serde::Deserialize;
use ulid::Ulid;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestEvent {
    #[serde(default)]
    pub event_id: Option<Ulid>,
    pub source_id: String,
    pub kind: String,
    pub ts_ms_utc: i64,
    pub semantic: serde_json::Value,
    #[serde(default)]
    pub links: Option<Vec<Link>>,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub producer_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IngestPayload {
    Batch { events: Vec<IngestEvent> },
    Array(Vec<IngestEvent>),
    Single(IngestEvent),
}

impl IngestPayload {
    #[must_use]
    pub fn into_events(self) -> Vec<IngestEvent> {
        match self {
            Self::Batch { events } | Self::Array(events) => events,
            Self::Single(e) => vec![e],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn single_event_json() -> serde_json::Value {
        json!({
            "source_id": "cam.front",
            "kind": "object_detected",
            "ts_ms_utc": 1_700_000_000_000_i64,
            "semantic": { "label": "person" }
        })
    }

    #[test]
    fn parses_single_object() {
        let p: IngestPayload = serde_json::from_value(single_event_json()).unwrap();
        let events = p.into_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_id, "cam.front");
    }

    #[test]
    fn parses_batch_with_events_key() {
        let p: IngestPayload = serde_json::from_value(json!({
            "events": [ single_event_json(), single_event_json() ]
        }))
        .unwrap();
        assert_eq!(p.into_events().len(), 2);
    }

    #[test]
    fn parses_bare_array() {
        let p: IngestPayload = serde_json::from_value(json!([single_event_json()])).unwrap();
        assert_eq!(p.into_events().len(), 1);
    }

    #[test]
    fn rejects_unknown_field_on_event() {
        let bad = json!({
            "source_id": "x", "kind": "y", "ts_ms_utc": 0,
            "semantic": {}, "bogus": 1
        });
        assert!(serde_json::from_value::<IngestPayload>(bad).is_err());
    }
}
