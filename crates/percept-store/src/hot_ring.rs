//! In-memory ring buffer per `(source_id, kind)`.
//!
//! Each ring is bounded by both a max event count and a max age — eviction
//! happens on push, oldest first, until both invariants hold.
//!
//! DECISIONS §5: default per-ring cap `max(256 events, 60 s)`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use percept_core::Event;

#[derive(Debug, Clone, Copy)]
pub struct HotRingConfig {
    pub max_events: usize,
    pub max_age: Duration,
}

impl Default for HotRingConfig {
    fn default() -> Self {
        Self {
            max_events: 256,
            max_age: Duration::from_secs(60),
        }
    }
}

/// A consistent view of one ring at the moment `snapshot` was taken.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub events: Vec<Arc<Event>>,
}

/// Map of `(source_id, kind)` to a bounded ring. Lookups and inserts are
/// O(1) on the outer map; per-ring writes take a per-ring lock.
pub struct HotRings {
    config: HotRingConfig,
    rings: RwLock<HashMap<RingKey, Arc<RwLock<Ring>>>>,
}

type RingKey = (String, String);

struct Ring {
    events: VecDeque<Arc<Event>>,
}

impl HotRings {
    #[must_use]
    pub fn new(config: HotRingConfig) -> Self {
        Self {
            config,
            rings: RwLock::new(HashMap::new()),
        }
    }

    /// Push an event into its `(source_id, kind)` ring. Returns the number of
    /// events evicted by this push (0 in the steady state).
    pub fn push(&self, event: Arc<Event>) -> usize {
        let key = (event.source_id.clone(), event.kind.clone());
        let ring = {
            let map = self.rings.read();
            map.get(&key).cloned()
        };
        let ring = if let Some(r) = ring {
            r
        } else {
            let mut map = self.rings.write();
            map.entry(key)
                .or_insert_with(|| {
                    Arc::new(RwLock::new(Ring {
                        events: VecDeque::new(),
                    }))
                })
                .clone()
        };

        let now = percept_core::now_ms_utc();
        let mut r = ring.write();
        r.events.push_back(event);

        let mut evicted = 0;
        while r.events.len() > self.config.max_events {
            r.events.pop_front();
            evicted += 1;
        }
        // Age is measured against ingest time (set by the normalizer);
        // ts_ms_utc is the producer's event time and may be stale
        // (e.g. for a replay), which shouldn't insta-evict.
        let max_age_ms = i64::try_from(self.config.max_age.as_millis()).unwrap_or(i64::MAX);
        let cutoff = now.saturating_sub(max_age_ms);
        while let Some(front) = r.events.front() {
            let age_ref = front.ingest_ts_ms_utc.unwrap_or(front.ts_ms_utc);
            if age_ref < cutoff {
                r.events.pop_front();
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }

    /// Latest event for `(source_id, kind)`, or `None` if the ring is empty.
    #[must_use]
    pub fn latest(&self, source_id: &str, kind: &str) -> Option<Arc<Event>> {
        let map = self.rings.read();
        let ring = map.get(&(source_id.to_string(), kind.to_string()))?.clone();
        drop(map);
        let r = ring.read();
        r.events.back().cloned()
    }

    /// Snapshot the events currently in the ring for `(source_id, kind)`.
    #[must_use]
    pub fn snapshot(&self, source_id: &str, kind: &str) -> Snapshot {
        let map = self.rings.read();
        let Some(ring) = map.get(&(source_id.to_string(), kind.to_string())).cloned() else {
            return Snapshot::default();
        };
        drop(map);
        let r = ring.read();
        Snapshot {
            events: r.events.iter().cloned().collect(),
        }
    }

    /// All `(source_id, kind)` pairs currently tracked, in unspecified order.
    #[must_use]
    pub fn keys(&self) -> Vec<RingKey> {
        self.rings.read().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use ulid::Ulid;

    fn event(source: &str, kind: &str, ts_ms: i64) -> Arc<Event> {
        Arc::new(Event {
            event_id: Ulid::new(),
            source_id: source.to_string(),
            kind: kind.to_string(),
            ts_ms_utc: ts_ms,
            semantic: json!({}),
            links: None,
            trace_id: None,
            ingest_ts_ms_utc: None,
            seq: None,
            producer_id: None,
            schema_invalid: None,
        })
    }

    #[test]
    fn separate_kinds_dont_displace() {
        let r = HotRings::new(HotRingConfig {
            max_events: 2,
            max_age: Duration::from_secs(3600),
        });
        let now = percept_core::now_ms_utc();
        r.push(event("node", "ble.advert", now));
        r.push(event("node", "ble.advert", now));
        r.push(event("node", "ble.advert", now)); // evicts first ble.advert
        r.push(event("node", "door_state", now)); // separate ring; not displaced

        assert!(r.latest("node", "ble.advert").is_some());
        assert!(r.latest("node", "door_state").is_some());
        assert_eq!(r.snapshot("node", "ble.advert").events.len(), 2);
        assert_eq!(r.snapshot("node", "door_state").events.len(), 1);
    }

    #[test]
    fn max_events_evicts_oldest() {
        let r = HotRings::new(HotRingConfig {
            max_events: 3,
            max_age: Duration::from_secs(3600),
        });
        let now = percept_core::now_ms_utc();
        for _ in 0..5 {
            r.push(event("s", "k", now));
        }
        assert_eq!(r.snapshot("s", "k").events.len(), 3);
    }

    #[test]
    fn max_age_evicts_old_events() {
        let r = HotRings::new(HotRingConfig {
            max_events: 100,
            max_age: Duration::from_millis(50),
        });
        let now = percept_core::now_ms_utc();
        r.push(event("s", "k", now - 10_000)); // very old
        r.push(event("s", "k", now));
        let snap = r.snapshot("s", "k");
        assert_eq!(snap.events.len(), 1);
        assert!(snap.events[0].ts_ms_utc >= now - 10);
    }

    #[test]
    fn latest_returns_most_recent_push() {
        let r = HotRings::new(HotRingConfig::default());
        let now = percept_core::now_ms_utc();
        let first = event("s", "k", now);
        let second = event("s", "k", now);
        r.push(first.clone());
        r.push(second.clone());
        assert_eq!(r.latest("s", "k").unwrap().event_id, second.event_id);
    }

    #[test]
    fn keys_lists_all_pairs() {
        let r = HotRings::new(HotRingConfig::default());
        let now = percept_core::now_ms_utc();
        r.push(event("a", "k", now));
        r.push(event("b", "k", now));
        r.push(event("a", "j", now));
        let mut keys = r.keys();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                ("a".to_string(), "j".to_string()),
                ("a".to_string(), "k".to_string()),
                ("b".to_string(), "k".to_string()),
            ]
        );
    }
}
