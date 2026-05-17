//! SQLite-persisted vector store with an in-memory mirror.
//!
//! Persistence keeps vectors across restarts (and lets retention drop them
//! by event_id in slice 5). The in-memory mirror is what `search_kn` reads
//! from — brute-force cosine over a contiguous Vec<f32>. For the edge
//! profile (≤ 1M vectors × 64 dims = 256 MB for the placeholder embedder)
//! this clears the slice-4 latency target by a wide margin.
//!
//! ANN (HNSW or LanceDB) is the right call once vector counts grow or
//! production embedding dims push memory pressure. The `Embedder` trait
//! is the swap point; this index will follow it.

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rusqlite::{params, Connection};
use ulid::Ulid;

use super::embedder::cosine_similarity;

#[derive(Debug, thiserror::Error)]
pub enum VectorError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("vector dim mismatch: index has {expected}, embedder has {actual}")]
    DimMismatch { expected: usize, actual: usize },
    #[error("vector model mismatch: index has {expected:?}, embedder has {actual:?}")]
    ModelMismatch { expected: String, actual: String },
    #[error("decoding stored vector blob: {0}")]
    Decode(String),
}

#[derive(Debug, Clone)]
pub struct VectorRecord {
    pub event_id: Ulid,
    pub source_id: String,
    pub kind: String,
    pub ts_ms_utc: i64,
    pub truncated: bool,
    pub model_id: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub event_id: Ulid,
    pub source_id: String,
    pub kind: String,
    pub ts_ms_utc: i64,
    pub truncated: bool,
    pub score: f32,
}

#[derive(Debug, Clone, Default)]
pub struct VectorFilter {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub source_filter: Option<Vec<String>>,
    pub kind_filter: Option<Vec<String>>,
}

struct InMemoryRow {
    event_id: Ulid,
    source_id: String,
    kind: String,
    ts_ms_utc: i64,
    truncated: bool,
    vector: Vec<f32>,
}

struct Inner {
    /// Compiled glob sets stay external (built per query); here we keep
    /// the raw rows.
    rows: Vec<InMemoryRow>,
}

pub struct VectorIndex {
    conn: Arc<parking_lot::Mutex<Connection>>,
    inner: Arc<RwLock<Inner>>,
}

const MIGRATIONS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS vectors (
        event_id    TEXT PRIMARY KEY,
        source_id   TEXT NOT NULL,
        kind        TEXT NOT NULL,
        ts_ms_utc   INTEGER NOT NULL,
        truncated   INTEGER NOT NULL,
        model_id    TEXT NOT NULL,
        vector      BLOB NOT NULL
     ) STRICT",
    "CREATE INDEX IF NOT EXISTS vectors_by_time ON vectors(ts_ms_utc)",
    "CREATE INDEX IF NOT EXISTS vectors_by_source_kind ON vectors(source_id, kind)",
];

impl VectorIndex {
    /// Open or create the vector index at `<data_dir>/vectors.sqlite3`,
    /// then load every stored row into memory. Returns an error when the
    /// stored model_id / dim doesn't match the running embedder.
    pub fn open(
        data_dir: &Path,
        expected_model_id: &str,
        expected_dim: usize,
    ) -> Result<Self, VectorError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("vectors.sqlite3");
        let conn = Connection::open(&path)?;
        configure(&conn)?;
        migrate(&conn)?;
        Self::from_connection(conn, expected_model_id, expected_dim)
    }

    /// In-memory variant for tests.
    pub fn open_in_memory(model_id: &str, dim: usize) -> Result<Self, VectorError> {
        let conn = Connection::open_in_memory()?;
        configure(&conn)?;
        migrate(&conn)?;
        Self::from_connection(conn, model_id, dim)
    }

    fn from_connection(
        conn: Connection,
        expected_model_id: &str,
        expected_dim: usize,
    ) -> Result<Self, VectorError> {
        let inner = load_rows(&conn, expected_model_id, expected_dim)?;
        Ok(Self {
            conn: Arc::new(parking_lot::Mutex::new(conn)),
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub fn append(&self, records: &[VectorRecord]) -> Result<(), VectorError> {
        if records.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO vectors
                   (event_id, source_id, kind, ts_ms_utc, truncated, model_id, vector)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )?;
            for r in records {
                let blob = vector_to_bytes(&r.vector);
                stmt.execute(params![
                    r.event_id.to_string(),
                    r.source_id,
                    r.kind,
                    r.ts_ms_utc,
                    i64::from(r.truncated),
                    r.model_id,
                    blob,
                ])?;
            }
        }
        tx.commit()?;

        // Append to the in-memory mirror under a write lock.
        let mut inner = self.inner.write();
        for r in records {
            // INSERT OR REPLACE on storage => replace in memory too. Linear
            // dedupe is fine for the slice-4 sizes; an event_id -> idx map
            // is a follow-up if writes get hot.
            if let Some(idx) = inner.rows.iter().position(|x| x.event_id == r.event_id) {
                inner.rows[idx] = InMemoryRow {
                    event_id: r.event_id,
                    source_id: r.source_id.clone(),
                    kind: r.kind.clone(),
                    ts_ms_utc: r.ts_ms_utc,
                    truncated: r.truncated,
                    vector: r.vector.clone(),
                };
            } else {
                inner.rows.push(InMemoryRow {
                    event_id: r.event_id,
                    source_id: r.source_id.clone(),
                    kind: r.kind.clone(),
                    ts_ms_utc: r.ts_ms_utc,
                    truncated: r.truncated,
                    vector: r.vector.clone(),
                });
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop vectors for `(source_id, kind)` older than `cutoff_ms`. With
    /// `dry_run = true` returns how many would be dropped without
    /// modifying anything (used by the retention sweeper).
    pub fn sweep_max_age(
        &self,
        source_id: &str,
        kind: &str,
        cutoff_ms: i64,
        dry_run: bool,
    ) -> Result<u64, VectorError> {
        let conn = self.conn.lock();
        if dry_run {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM vectors
                  WHERE source_id = ? AND kind = ? AND ts_ms_utc < ?",
                params![source_id, kind, cutoff_ms],
                |r| r.get(0),
            )?;
            return Ok(u64::try_from(n).unwrap_or(0));
        }
        let affected = conn.execute(
            "DELETE FROM vectors
              WHERE source_id = ? AND kind = ? AND ts_ms_utc < ?",
            params![source_id, kind, cutoff_ms],
        )?;
        drop(conn);
        // Mirror the delete in memory so search_kn reflects it.
        let mut inner = self.inner.write();
        inner
            .rows
            .retain(|r| !(r.source_id == source_id && r.kind == kind && r.ts_ms_utc < cutoff_ms));
        Ok(affected as u64)
    }

    /// Filtered kNN by cosine. Returns at most `top_k` hits, sorted by
    /// descending similarity.
    pub fn search_kn(
        &self,
        query: &[f32],
        top_k: usize,
        filter: &VectorFilter,
    ) -> Result<Vec<SearchHit>, VectorError> {
        let source_globs = build_globs(filter.source_filter.as_deref())?;
        let kind_globs = build_globs(filter.kind_filter.as_deref())?;
        let inner = self.inner.read();

        let mut heap: Vec<SearchHit> = Vec::with_capacity(top_k + 1);
        for row in &inner.rows {
            if let Some(s) = filter.start_ms {
                if row.ts_ms_utc < s {
                    continue;
                }
            }
            if let Some(e) = filter.end_ms {
                if row.ts_ms_utc >= e {
                    continue;
                }
            }
            if let Some(g) = &source_globs {
                if !g.is_match(&row.source_id) {
                    continue;
                }
            }
            if let Some(g) = &kind_globs {
                if !g.is_match(&row.kind) {
                    continue;
                }
            }
            let score = cosine_similarity(query, &row.vector);
            insert_top_k(
                &mut heap,
                SearchHit {
                    event_id: row.event_id,
                    source_id: row.source_id.clone(),
                    kind: row.kind.clone(),
                    ts_ms_utc: row.ts_ms_utc,
                    truncated: row.truncated,
                    score,
                },
                top_k,
            );
        }
        heap.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(heap)
    }
}

fn insert_top_k(heap: &mut Vec<SearchHit>, hit: SearchHit, top_k: usize) {
    if heap.len() < top_k {
        heap.push(hit);
        return;
    }
    let (worst_idx, worst_score) = heap
        .iter()
        .enumerate()
        .min_by(|a, b| {
            a.1.score
                .partial_cmp(&b.1.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(i, h)| (i, h.score))
        .unwrap();
    if hit.score > worst_score {
        heap[worst_idx] = hit;
    }
}

fn build_globs(patterns: Option<&[String]>) -> Result<Option<globset::GlobSet>, VectorError> {
    let Some(p) = patterns else { return Ok(None) };
    if p.is_empty() {
        return Ok(None);
    }
    let mut b = globset::GlobSetBuilder::new();
    for pat in p {
        b.add(globset::Glob::new(pat).map_err(|e| VectorError::Decode(e.to_string()))?);
    }
    b.build()
        .map(Some)
        .map_err(|e| VectorError::Decode(e.to_string()))
}

fn configure(conn: &Connection) -> Result<(), VectorError> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

fn migrate(conn: &Connection) -> Result<(), VectorError> {
    for sql in MIGRATIONS {
        conn.execute_batch(sql)?;
    }
    Ok(())
}

fn load_rows(
    conn: &Connection,
    expected_model_id: &str,
    expected_dim: usize,
) -> Result<Inner, VectorError> {
    let mut stmt = conn.prepare(
        "SELECT event_id, source_id, kind, ts_ms_utc, truncated, model_id, vector FROM vectors",
    )?;
    let mut rows = stmt.query([])?;
    let mut out: Vec<InMemoryRow> = Vec::new();
    let mut seen_model: Option<String> = None;
    let mut seen_dim: Option<usize> = None;
    while let Some(row) = rows.next()? {
        let event_id_s: String = row.get(0)?;
        let event_id =
            Ulid::from_string(&event_id_s).map_err(|e| VectorError::Decode(e.to_string()))?;
        let source_id: String = row.get(1)?;
        let kind: String = row.get(2)?;
        let ts_ms_utc: i64 = row.get(3)?;
        let truncated: i64 = row.get(4)?;
        let model_id: String = row.get(5)?;
        let blob: Vec<u8> = row.get(6)?;
        let vector = bytes_to_vector(&blob)?;

        if seen_model.is_none() {
            seen_model = Some(model_id.clone());
            seen_dim = Some(vector.len());
        }
        out.push(InMemoryRow {
            event_id,
            source_id,
            kind,
            ts_ms_utc,
            truncated: truncated != 0,
            vector,
        });
    }
    if let Some(m) = &seen_model {
        if m != expected_model_id {
            return Err(VectorError::ModelMismatch {
                expected: m.clone(),
                actual: expected_model_id.to_string(),
            });
        }
    }
    if let Some(d) = seen_dim {
        if d != expected_dim {
            return Err(VectorError::DimMismatch {
                expected: d,
                actual: expected_dim,
            });
        }
    }
    let _ = (expected_dim, expected_model_id); // model/dim checks already done above
    Ok(Inner { rows: out })
}

fn vector_to_bytes(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn bytes_to_vector(b: &[u8]) -> Result<Vec<f32>, VectorError> {
    if b.len() % 4 != 0 {
        return Err(VectorError::Decode(format!(
            "vector blob length {} is not a multiple of 4",
            b.len()
        )));
    }
    let mut out = Vec::with_capacity(b.len() / 4);
    for chunk in b.chunks_exact(4) {
        let bytes: [u8; 4] = chunk.try_into().unwrap();
        out.push(f32::from_le_bytes(bytes));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::embedder::{Embedder, HashEmbedder};

    fn record(
        e: &HashEmbedder,
        event_id: Ulid,
        src: &str,
        kind: &str,
        ts: i64,
        text: &str,
    ) -> VectorRecord {
        VectorRecord {
            event_id,
            source_id: src.to_string(),
            kind: kind.to_string(),
            ts_ms_utc: ts,
            truncated: false,
            model_id: e.model_id().to_string(),
            vector: e.embed(text),
        }
    }

    #[test]
    fn append_persists_and_search_returns_topk() {
        let e = HashEmbedder::new(64);
        let idx = VectorIndex::open_in_memory(e.model_id(), e.dim()).unwrap();
        idx.append(&[
            record(
                &e,
                Ulid::new(),
                "cam.front",
                "scene",
                1,
                "person near the door",
            ),
            record(&e, Ulid::new(), "cam.front", "scene", 2, "porch is empty"),
            record(
                &e,
                Ulid::new(),
                "therm.kitchen",
                "temp",
                3,
                "twenty degrees celsius",
            ),
        ])
        .unwrap();

        let q = e.embed("someone at the door");
        let hits = idx.search_kn(&q, 2, &VectorFilter::default()).unwrap();
        assert_eq!(hits.len(), 2);
        // Best match should be the "person near the door" row.
        assert_eq!(hits[0].source_id, "cam.front");
    }

    #[test]
    fn search_respects_time_filter() {
        let e = HashEmbedder::new(32);
        let idx = VectorIndex::open_in_memory(e.model_id(), e.dim()).unwrap();
        idx.append(&[
            record(&e, Ulid::new(), "s", "k", 10, "person"),
            record(&e, Ulid::new(), "s", "k", 200, "person"),
        ])
        .unwrap();
        let q = e.embed("person");
        let hits = idx
            .search_kn(
                &q,
                10,
                &VectorFilter {
                    start_ms: Some(100),
                    end_ms: Some(300),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].ts_ms_utc, 200);
    }

    #[test]
    fn search_respects_source_glob() {
        let e = HashEmbedder::new(32);
        let idx = VectorIndex::open_in_memory(e.model_id(), e.dim()).unwrap();
        idx.append(&[
            record(&e, Ulid::new(), "cam.front", "k", 1, "x"),
            record(&e, Ulid::new(), "cam.back", "k", 2, "x"),
            record(&e, Ulid::new(), "therm.kitchen", "k", 3, "x"),
        ])
        .unwrap();
        let q = e.embed("x");
        let hits = idx
            .search_kn(
                &q,
                10,
                &VectorFilter {
                    source_filter: Some(vec!["cam.*".into()]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn append_then_reload_preserves_rows() {
        let e = HashEmbedder::new(32);
        // First open: write some rows.
        let dir = tempfile::tempdir().unwrap();
        let idx = VectorIndex::open(dir.path(), e.model_id(), e.dim()).unwrap();
        idx.append(&[record(&e, Ulid::new(), "s", "k", 1, "hello world")])
            .unwrap();
        drop(idx);

        // Reopen: rows should be loaded back into memory.
        let idx = VectorIndex::open(dir.path(), e.model_id(), e.dim()).unwrap();
        assert_eq!(idx.len(), 1);
        let q = e.embed("hello world");
        let hits = idx.search_kn(&q, 1, &VectorFilter::default()).unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn reopen_with_wrong_model_id_rejects() {
        let e = HashEmbedder::new(32);
        let dir = tempfile::tempdir().unwrap();
        let idx = VectorIndex::open(dir.path(), e.model_id(), e.dim()).unwrap();
        idx.append(&[record(&e, Ulid::new(), "s", "k", 1, "x")])
            .unwrap();
        drop(idx);

        match VectorIndex::open(dir.path(), "different-model", e.dim()) {
            Err(VectorError::ModelMismatch { .. }) => {}
            Err(other) => panic!("expected ModelMismatch, got: {other}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn insert_top_k_keeps_best() {
        let mut heap: Vec<SearchHit> = vec![];
        for (i, s) in [(0.1, 0), (0.5, 1), (0.3, 2)].iter().enumerate() {
            insert_top_k(
                &mut heap,
                SearchHit {
                    event_id: Ulid::new(),
                    source_id: "x".into(),
                    kind: "y".into(),
                    ts_ms_utc: i as i64,
                    truncated: false,
                    score: s.0 as f32,
                },
                2,
            );
        }
        assert_eq!(heap.len(), 2);
        let scores: Vec<f32> = heap.iter().map(|h| h.score).collect();
        assert!(scores.contains(&0.5));
        assert!(scores.contains(&0.3));
    }
}
