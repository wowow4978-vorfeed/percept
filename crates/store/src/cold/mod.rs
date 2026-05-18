//! Cold store: durable event log + `latest` cache for cold-fallback lookups.
//!
//! Slice 3 backs everything with a single DuckDB file (`<data_dir>/cold.duckdb`).
//! Parquet partition export is deferred to slice 5 (retention) — DuckDB's
//! own columnar storage covers the slice-3 acceptance criteria.

pub mod cursor;
mod store;
mod writer;

pub use cursor::{filter_hash, Anchor, Cursor, CursorError};
pub use store::{ColdError, ColdStore, WindowFilter, MAX_WINDOW_LIMIT};
pub use writer::{ColdWriter, ColdWriterConfig, ColdWriterMetrics};
