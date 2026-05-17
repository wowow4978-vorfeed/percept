//! Producer-facing event type. Serializes to the same wire shape the
//! server expects on `POST /ingest` (DESIGN §3.1 minus server-attached
//! fields).

use percept_core::Link;
use serde::Serialize;
use ulid::Ulid;

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Ulid>,
    pub source_id: String,
    pub kind: String,
    pub ts_ms_utc: i64,
    pub semantic: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<Vec<Link>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_id: Option<String>,
}

impl Event {
    /// Minimal constructor — fill in any optional fields after.
    pub fn new(
        source_id: impl Into<String>,
        kind: impl Into<String>,
        ts_ms_utc: i64,
        semantic: serde_json::Value,
    ) -> Self {
        Self {
            event_id: None,
            source_id: source_id.into(),
            kind: kind.into(),
            ts_ms_utc,
            semantic,
            links: None,
            trace_id: None,
            producer_id: None,
        }
    }

    #[must_use]
    pub fn with_producer_id(mut self, producer_id: impl Into<String>) -> Self {
        self.producer_id = Some(producer_id.into());
        self
    }

    #[must_use]
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    #[must_use]
    pub fn with_event_id(mut self, event_id: Ulid) -> Self {
        self.event_id = Some(event_id);
        self
    }
}

/// Wire envelope: `{"events": [...]}`. The server also accepts a single
/// event or a bare array; the SDK always uses the batch form for
/// uniformity.
#[derive(Debug, Serialize)]
pub struct Envelope<'a> {
    pub events: &'a [Event],
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialised_event_matches_wire_shape() {
        let e = Event::new("cam.front", "object_detected", 1700, json!({ "x": 1 }))
            .with_producer_id("rust-sdk");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["source_id"], "cam.front");
        assert_eq!(v["kind"], "object_detected");
        assert_eq!(v["producer_id"], "rust-sdk");
        // Unset optionals are omitted (server's `deny_unknown_fields` is
        // happy when we don't send `event_id`, `trace_id`, etc.).
        assert!(v.get("event_id").is_none());
        assert!(v.get("trace_id").is_none());
    }
}
