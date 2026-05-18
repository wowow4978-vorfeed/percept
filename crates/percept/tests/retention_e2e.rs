//! Slice 5 acceptance: live retention sweeper.
//!
//! Counterpart to the unit tests in
//! `percept-store::retention::sweeper` (which drive the sweep
//! function directly). This test exercises the
//! `percept::sweeper::Sweeper` background task end-to-end: pipeline
//! ingests events, sweeper task fires on its cadence, expired rows
//! disappear from the cold store.

use std::sync::Arc;
use std::time::Duration;

use percept::sweeper::{Sweeper, SweeperConfig};
use percept_core::{new_event_id, now_ms_utc, Event};
use percept_store::{ColdStore, RetentionPolicy};

#[tokio::test]
async fn background_sweeper_drops_expired_events() {
    let cold = Arc::new(ColdStore::open_in_memory().unwrap());

    // Seed 6 events: 4 well past the 2s cut-off, 2 well inside it.
    let now = now_ms_utc();
    let timestamps: [i64; 6] = [
        now - 60_000, // 60s ago — drop
        now - 30_000, // 30s ago — drop
        now - 10_000, // 10s ago — drop
        now - 5_000,  // 5s ago  — drop
        now - 500,    // 0.5s ago — keep
        now,          // now      — keep
    ];
    let events: Vec<_> = timestamps
        .iter()
        .enumerate()
        .map(|(i, ts)| {
            Arc::new(Event {
                event_id: new_event_id(),
                source_id: "s".into(),
                kind: "k".into(),
                ts_ms_utc: *ts,
                semantic: serde_json::json!({ "i": i }),
                links: None,
                trace_id: None,
                ingest_ts_ms_utc: Some(*ts),
                seq: Some(i as u64),
                producer_id: None,
                schema_invalid: None,
            })
        })
        .collect();
    cold.append(&events).unwrap();
    assert_eq!(cold.event_count().unwrap(), 6);

    // max_age = 2s. With cadence 80ms and the first tick skipped, the
    // first execute fires at ~160ms; cutoff = now+160ms - 2s ≈ -1.84s
    // → drops the four older rows, keeps the two within ~1s.
    let policies = Arc::new(vec![RetentionPolicy {
        match_source_id: Some("s".into()),
        max_age: Some(Duration::from_secs(2)),
        ..Default::default()
    }]);

    let sweeper = Sweeper::new(
        cold.clone(),
        None,
        policies,
        SweeperConfig {
            cadence: Duration::from_millis(80),
        },
    );
    tokio::spawn(sweeper.run());
    tokio::time::sleep(Duration::from_millis(300)).await;

    let after = cold.event_count().unwrap();
    // Strict expectation: exactly the 2 recent rows survive.
    assert_eq!(after, 2, "expected the 2 within-cutoff rows to survive");
}

#[tokio::test]
async fn sweeper_no_op_when_no_policies_configured() {
    let cold = Arc::new(ColdStore::open_in_memory().unwrap());
    let now = now_ms_utc();
    cold.append(&[Arc::new(Event {
        event_id: new_event_id(),
        source_id: "s".into(),
        kind: "k".into(),
        ts_ms_utc: now - 10_000,
        semantic: serde_json::json!({}),
        links: None,
        trace_id: None,
        ingest_ts_ms_utc: Some(now - 10_000),
        seq: Some(1),
        producer_id: None,
        schema_invalid: None,
    })])
    .unwrap();

    let sweeper = Sweeper::new(
        cold.clone(),
        None,
        Arc::new(Vec::new()), // no policies
        SweeperConfig {
            cadence: Duration::from_millis(50),
        },
    );
    let handle = tokio::spawn(sweeper.run());

    tokio::time::sleep(Duration::from_millis(200)).await;
    // No policies → sweeper exits immediately; the row stays.
    assert_eq!(cold.event_count().unwrap(), 1);
    // The task should have exited cleanly (no panic).
    assert!(!handle.is_finished() || handle.await.is_ok());
}
