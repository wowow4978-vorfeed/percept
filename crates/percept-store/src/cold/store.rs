//! SQLite-backed cold store.
//!
//! Two tables behind one persistent SQLite connection:
//!
//! - `events` — append-only canonical event log, indexed by `ts_ms_utc`
//!   and `(source_id, kind)`. The `day` column is the per-row partition
//!   key used for retention drops in slice 5.
//! - `latest` — one row per `(source_id, kind)`, upserted on every batch
//!   commit, used as the cold fallback for `get_current_state`.
//!
//! Engine choice: DESIGN.md Appendix A names DuckDB+Parquet for the cold
//! store. v1 ships with rusqlite (bundled SQLite) — same SQL surface for
//! cursor-paginated time-range scans, ~30 s compile, a few MB on disk vs
//! DuckDB-bundled's 9 min and ~4 GB per build directory. The DuckDB switch
//! is a v2 perf concern; SQLite handles the slice-3 acceptance target.
//! Parquet export is deferred to slice 5 along with day-partition drops.

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use percept_core::Event;
use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension};
use ulid::Ulid;

use super::cursor::Anchor;

#[derive(Debug, thiserror::Error)]
pub enum ColdError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("serializing event: {0}")]
    Encode(String),
    #[error("decoding stored row: {0}")]
    Decode(String),
}

#[derive(Debug, Clone, Default)]
pub struct WindowFilter {
    pub start_ms: i64,
    pub end_ms: i64,
    pub source_filter: Option<Vec<String>>,
    pub kind_filter: Option<Vec<String>>,
    pub limit: u32,
}

pub struct ColdStore {
    conn: Arc<Mutex<Connection>>,
}

impl ColdStore {
    /// Open or create the cold store at `<data_dir>/cold.sqlite3`.
    pub fn open(data_dir: &Path) -> Result<Self, ColdError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("cold.sqlite3");
        let conn = Connection::open(&path)?;
        configure(&conn)?;
        migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// In-memory store for tests.
    pub fn open_in_memory() -> Result<Self, ColdError> {
        let conn = Connection::open_in_memory()?;
        configure(&conn)?;
        migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Append a batch of events. Updates `latest` for each `(source_id, kind)`
    /// in a single transaction.
    pub fn append(&self, events: &[Arc<Event>]) -> Result<(), ColdError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        {
            let mut insert = tx.prepare_cached(
                "INSERT OR IGNORE INTO events
                 (event_id, source_id, kind, ts_ms_utc, ingest_ts_ms_utc,
                  seq, producer_id, trace_id, semantic, links,
                  schema_invalid, day)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for e in events {
                let semantic = serde_json::to_string(&e.semantic)
                    .map_err(|err| ColdError::Encode(err.to_string()))?;
                let links = match &e.links {
                    Some(l) => Some(
                        serde_json::to_string(l)
                            .map_err(|err| ColdError::Encode(err.to_string()))?,
                    ),
                    None => None,
                };
                insert.execute(params![
                    e.event_id.to_string(),
                    e.source_id,
                    e.kind,
                    e.ts_ms_utc,
                    e.ingest_ts_ms_utc.unwrap_or(e.ts_ms_utc),
                    e.seq.map(|s| s as i64),
                    e.producer_id,
                    e.trace_id,
                    semantic,
                    links,
                    e.schema_invalid,
                    day_of(e.ts_ms_utc),
                ])?;
            }
        }

        // Upsert `latest`: only overwrite when the incoming row is newer by
        // ts_ms_utc. SQLite supports ON CONFLICT ... DO UPDATE since 3.24.
        {
            let mut upsert = tx.prepare_cached(
                "INSERT INTO latest
                 (source_id, kind, event_id, ts_ms_utc, ingest_ts_ms_utc,
                  seq, producer_id, trace_id, semantic, links, schema_invalid)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(source_id, kind) DO UPDATE SET
                   event_id = excluded.event_id,
                   ts_ms_utc = excluded.ts_ms_utc,
                   ingest_ts_ms_utc = excluded.ingest_ts_ms_utc,
                   seq = excluded.seq,
                   producer_id = excluded.producer_id,
                   trace_id = excluded.trace_id,
                   semantic = excluded.semantic,
                   links = excluded.links,
                   schema_invalid = excluded.schema_invalid
                 WHERE excluded.ts_ms_utc > latest.ts_ms_utc",
            )?;
            // Collapse the batch to per-(source, kind) latest so we run one
            // upsert per pair, not per event.
            let mut latest_in_batch: std::collections::HashMap<(String, String), &Arc<Event>> =
                std::collections::HashMap::new();
            for e in events {
                let key = (e.source_id.clone(), e.kind.clone());
                latest_in_batch
                    .entry(key)
                    .and_modify(|cur| {
                        if e.ts_ms_utc > cur.ts_ms_utc {
                            *cur = e;
                        }
                    })
                    .or_insert(e);
            }
            for e in latest_in_batch.values() {
                let semantic = serde_json::to_string(&e.semantic)
                    .map_err(|err| ColdError::Encode(err.to_string()))?;
                let links = match &e.links {
                    Some(l) => Some(
                        serde_json::to_string(l)
                            .map_err(|err| ColdError::Encode(err.to_string()))?,
                    ),
                    None => None,
                };
                upsert.execute(params![
                    e.source_id,
                    e.kind,
                    e.event_id.to_string(),
                    e.ts_ms_utc,
                    e.ingest_ts_ms_utc.unwrap_or(e.ts_ms_utc),
                    e.seq.map(|s| s as i64),
                    e.producer_id,
                    e.trace_id,
                    semantic,
                    links,
                    e.schema_invalid,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Latest cached `(source_id, kind)` row, used by the cold fallback in
    /// `get_current_state`.
    pub fn latest(&self, source_id: &str, kind: &str) -> Result<Option<Event>, ColdError> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare_cached(
            "SELECT event_id, source_id, kind, ts_ms_utc, ingest_ts_ms_utc,
                    seq, producer_id, trace_id, semantic, links, schema_invalid
               FROM latest
              WHERE source_id = ? AND kind = ?",
        )?;
        let row = stmt
            .query_row(params![source_id, kind], row_to_event)
            .optional()?;
        Ok(row)
    }

    /// Time-range scan with cursor-based pagination.
    ///
    /// Resumption is `(ts_ms_utc, event_id) > anchor` in the stable
    /// `(ts_ms_utc, event_id)` ascending order.
    pub fn query_window(
        &self,
        filter: &WindowFilter,
        anchor: Option<Anchor>,
    ) -> Result<Vec<Event>, ColdError> {
        let limit = filter.limit.min(MAX_WINDOW_LIMIT);
        let mut sql = String::from(
            "SELECT event_id, source_id, kind, ts_ms_utc, ingest_ts_ms_utc,
                    seq, producer_id, trace_id, semantic, links, schema_invalid
               FROM events
              WHERE ts_ms_utc >= ? AND ts_ms_utc < ?",
        );
        let mut params: Vec<Value> = vec![filter.start_ms.into(), filter.end_ms.into()];
        if let Some(globs) = &filter.source_filter {
            sql.push_str(&glob_clause("source_id", globs.len()));
            for g in globs {
                params.push(glob_to_like(g).into());
            }
        }
        if let Some(globs) = &filter.kind_filter {
            sql.push_str(&glob_clause("kind", globs.len()));
            for g in globs {
                params.push(glob_to_like(g).into());
            }
        }
        if let Some(a) = &anchor {
            sql.push_str(" AND (ts_ms_utc > ? OR (ts_ms_utc = ? AND event_id > ?))");
            params.push(a.ts_ms_utc.into());
            params.push(a.ts_ms_utc.into());
            params.push(a.event_id.to_string().into());
        }
        sql.push_str(" ORDER BY ts_ms_utc ASC, event_id ASC LIMIT ?");
        params.push(i64::from(limit).into());

        let conn = self.conn.lock();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params), row_to_event)?;
        let mut out = Vec::with_capacity(limit as usize);
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Total events in the store. Used for tests and the lag metric.
    pub fn event_count(&self) -> Result<u64, ColdError> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        Ok(u64::try_from(n).unwrap_or(0))
    }
}

/// Per-call hard limit per DECISIONS §9.
pub const MAX_WINDOW_LIMIT: u32 = 10_000;

const MIGRATIONS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS events (
        event_id           TEXT PRIMARY KEY,
        source_id          TEXT NOT NULL,
        kind               TEXT NOT NULL,
        ts_ms_utc          INTEGER NOT NULL,
        ingest_ts_ms_utc   INTEGER NOT NULL,
        seq                INTEGER,
        producer_id        TEXT,
        trace_id           TEXT,
        semantic           TEXT NOT NULL,
        links              TEXT,
        schema_invalid     INTEGER,
        day                TEXT NOT NULL
     ) STRICT",
    "CREATE INDEX IF NOT EXISTS events_by_time ON events(ts_ms_utc, event_id)",
    "CREATE INDEX IF NOT EXISTS events_by_source_kind ON events(source_id, kind, ts_ms_utc)",
    "CREATE INDEX IF NOT EXISTS events_by_day ON events(day)",
    "CREATE TABLE IF NOT EXISTS latest (
        source_id          TEXT NOT NULL,
        kind               TEXT NOT NULL,
        event_id           TEXT NOT NULL,
        ts_ms_utc          INTEGER NOT NULL,
        ingest_ts_ms_utc   INTEGER NOT NULL,
        seq                INTEGER,
        producer_id        TEXT,
        trace_id           TEXT,
        semantic           TEXT NOT NULL,
        links              TEXT,
        schema_invalid     INTEGER,
        PRIMARY KEY (source_id, kind)
     ) STRICT",
];

fn configure(conn: &Connection) -> Result<(), ColdError> {
    // WAL improves concurrent reads while the writer commits. The cold
    // writer is single-threaded but MCP queries read concurrently.
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn migrate(conn: &Connection) -> Result<(), ColdError> {
    for sql in MIGRATIONS {
        conn.execute_batch(sql)?;
    }
    Ok(())
}

fn day_of(ts_ms_utc: i64) -> String {
    // UTC day key, e.g. "2026-05-17". Used for partition pruning in
    // queries and for retention drops (slice 5).
    let secs = ts_ms_utc / 1000;
    let days_since_epoch = secs / 86_400;
    let (y, m, d) = civil_date(days_since_epoch);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_date(days: i64) -> (i32, u32, u32) {
    // Howard Hinnant's algorithm — days since 1970-01-01 → (y, m, d).
    let days = days + 719_468;
    let era = if days >= 0 {
        days / 146_097
    } else {
        (days - 146_096) / 146_097
    };
    let doe = (days - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y_full = if m <= 2 { y + 1 } else { y };
    (i32::try_from(y_full).unwrap_or(0), m as u32, d as u32)
}

fn glob_clause(col: &str, n: usize) -> String {
    if n == 0 {
        return String::new();
    }
    let conds: Vec<String> = (0..n).map(|_| format!("{col} LIKE ?")).collect();
    format!(" AND ({})", conds.join(" OR "))
}

/// Translate a shell-style `*` glob to a SQL LIKE pattern. We support `*`
/// only (no `?`); LIKE's `%` is the equivalent. Caller already validated
/// the glob via the configuration loader.
fn glob_to_like(g: &str) -> String {
    g.replace('%', r"\%").replace('_', r"\_").replace('*', "%")
}

fn row_to_event(row: &rusqlite::Row<'_>) -> Result<Event, rusqlite::Error> {
    let event_id_str: String = row.get(0)?;
    let event_id = Ulid::from_string(&event_id_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let source_id: String = row.get(1)?;
    let kind: String = row.get(2)?;
    let ts_ms_utc: i64 = row.get(3)?;
    let ingest_ts_ms_utc: Option<i64> = row.get(4)?;
    let seq: Option<i64> = row.get(5)?;
    let producer_id: Option<String> = row.get(6)?;
    let trace_id: Option<String> = row.get(7)?;
    let semantic_str: String = row.get(8)?;
    let semantic: serde_json::Value = serde_json::from_str(&semantic_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let links_str: Option<String> = row.get(9)?;
    let links = match links_str {
        Some(s) => Some(
            serde_json::from_str::<Vec<percept_core::Link>>(&s).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?,
        ),
        None => None,
    };
    let schema_invalid: Option<bool> = row.get(10)?;

    Ok(Event {
        event_id,
        source_id,
        kind,
        ts_ms_utc,
        semantic,
        links,
        trace_id,
        ingest_ts_ms_utc,
        seq: seq.map(|s| s as u64),
        producer_id,
        schema_invalid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(source: &str, kind: &str, ts_ms: i64) -> Arc<Event> {
        Arc::new(Event {
            event_id: Ulid::new(),
            source_id: source.to_string(),
            kind: kind.to_string(),
            ts_ms_utc: ts_ms,
            semantic: json!({ "v": ts_ms }),
            links: None,
            trace_id: None,
            ingest_ts_ms_utc: Some(ts_ms),
            seq: Some(1),
            producer_id: None,
            schema_invalid: None,
        })
    }

    #[test]
    fn append_then_latest_returns_newest() {
        let s = ColdStore::open_in_memory().unwrap();
        s.append(&[
            event("cam", "k", 100),
            event("cam", "k", 200),
            event("cam", "k", 150),
        ])
        .unwrap();
        let latest = s.latest("cam", "k").unwrap().unwrap();
        assert_eq!(latest.ts_ms_utc, 200);
    }

    #[test]
    fn latest_handles_unknown_pair() {
        let s = ColdStore::open_in_memory().unwrap();
        assert!(s.latest("missing", "k").unwrap().is_none());
    }

    #[test]
    fn window_filters_time_range_and_orders() {
        let s = ColdStore::open_in_memory().unwrap();
        let evs: Vec<_> = (0..10).map(|i| event("s", "k", i * 10)).collect();
        s.append(&evs).unwrap();
        let result = s
            .query_window(
                &WindowFilter {
                    start_ms: 20,
                    end_ms: 60,
                    source_filter: None,
                    kind_filter: None,
                    limit: 100,
                },
                None,
            )
            .unwrap();
        assert_eq!(result.len(), 4); // 20, 30, 40, 50
        for w in result.windows(2) {
            assert!(w[0].ts_ms_utc <= w[1].ts_ms_utc);
        }
    }

    #[test]
    fn window_glob_filter() {
        let s = ColdStore::open_in_memory().unwrap();
        s.append(&[
            event("cam.front", "k", 1),
            event("cam.back", "k", 2),
            event("therm.kitchen", "k", 3),
        ])
        .unwrap();
        let result = s
            .query_window(
                &WindowFilter {
                    start_ms: 0,
                    end_ms: 100,
                    source_filter: Some(vec!["cam.*".into()]),
                    kind_filter: None,
                    limit: 10,
                },
                None,
            )
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn window_resumes_via_anchor() {
        let s = ColdStore::open_in_memory().unwrap();
        let evs: Vec<_> = (0..10).map(|i| event("s", "k", i * 10)).collect();
        s.append(&evs).unwrap();
        let page1 = s
            .query_window(
                &WindowFilter {
                    start_ms: 0,
                    end_ms: 1000,
                    source_filter: None,
                    kind_filter: None,
                    limit: 3,
                },
                None,
            )
            .unwrap();
        assert_eq!(page1.len(), 3);
        let last = page1.last().unwrap();
        let page2 = s
            .query_window(
                &WindowFilter {
                    start_ms: 0,
                    end_ms: 1000,
                    source_filter: None,
                    kind_filter: None,
                    limit: 3,
                },
                Some(Anchor {
                    ts_ms_utc: last.ts_ms_utc,
                    event_id: last.event_id,
                }),
            )
            .unwrap();
        assert_eq!(page2.len(), 3);
        assert!(page2[0].ts_ms_utc > last.ts_ms_utc);
    }

    #[test]
    fn day_of_known_dates() {
        // 2024-01-01 00:00:00 UTC = 1_704_067_200_000 ms
        assert_eq!(day_of(1_704_067_200_000), "2024-01-01");
    }

    #[test]
    fn glob_to_like_translates_star() {
        assert_eq!(glob_to_like("cam.*"), "cam.%");
        assert_eq!(glob_to_like("a*b*c"), "a%b%c");
    }
}
