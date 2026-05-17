//! Storage layer for Percept.
//!
//! Slice 1: in-memory hot rings only. Cold store and vector index land in
//! slices 3 and 4 respectively.

pub mod cold;
mod hot_ring;

pub use cold::{
    filter_hash, Anchor, ColdError, ColdStore, ColdWriter, ColdWriterConfig, ColdWriterMetrics,
    Cursor, CursorError, WindowFilter, MAX_WINDOW_LIMIT,
};
pub use hot_ring::{HotRingConfig, HotRings, Snapshot};
