use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub event_id: Ulid,
    pub source_id: String,
    pub kind: String,
    pub ts_ms_utc: i64,
    pub semantic: serde_json::Value,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<Vec<Link>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest_ts_ms_utc: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_id: Option<String>,

    #[serde(
        default,
        rename = "_schema_invalid",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_invalid: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub rel: String,
    pub uri: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_event() -> Event {
        Event {
            event_id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            source_id: "cam.front_door".to_string(),
            kind: "object_detected".to_string(),
            ts_ms_utc: 1_700_000_000_000,
            semantic: json!({ "label": "person", "confidence": 0.92 }),
            links: None,
            trace_id: None,
            ingest_ts_ms_utc: None,
            seq: None,
            producer_id: None,
            schema_invalid: None,
        }
    }

    #[test]
    fn event_roundtrip_minimal() {
        let e = sample_event();
        let s = serde_json::to_string(&e).unwrap();
        let back: Event = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn event_roundtrip_with_links_and_server_fields() {
        let mut e = sample_event();
        e.links = Some(vec![Link {
            rel: "frame".to_string(),
            uri: "s3://bucket/k.jpg".to_string(),
            mime: Some("image/jpeg".to_string()),
            bytes: Some(123_456),
            sha256: Some("deadbeef".to_string()),
        }]);
        e.trace_id = Some("trace-123".to_string());
        e.ingest_ts_ms_utc = Some(1_700_000_000_500);
        e.seq = Some(42);
        e.schema_invalid = Some(true);

        let s = serde_json::to_string(&e).unwrap();
        assert!(s.contains("_schema_invalid"));
        let back: Event = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn link_roundtrip_minimal() {
        let l = Link {
            rel: "clip".to_string(),
            uri: "file:///tmp/c.mp4".to_string(),
            mime: None,
            bytes: None,
            sha256: None,
        };
        let s = serde_json::to_string(&l).unwrap();
        assert!(!s.contains("mime"));
        let back: Link = serde_json::from_str(&s).unwrap();
        assert_eq!(l, back);
    }

    #[test]
    fn event_tolerates_unknown_fields() {
        let raw = r#"{
            "event_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "source_id": "x",
            "kind": "y",
            "ts_ms_utc": 0,
            "semantic": {},
            "future_field": "ignored"
        }"#;
        let _: Event = serde_json::from_str(raw).unwrap();
    }
}
