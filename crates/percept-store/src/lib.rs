//! Storage layer for Percept.
//!
//! Slice 1: in-memory hot rings only. Cold store and vector index land in
//! slices 3 and 4 respectively.

pub mod cold;
mod hot_ring;
pub mod retention;
pub mod vector;

pub use cold::{
    filter_hash, Anchor, ColdError, ColdStore, ColdWriter, ColdWriterConfig, ColdWriterMetrics,
    Cursor, CursorError, WindowFilter, MAX_WINDOW_LIMIT,
};
pub use hot_ring::{HotRingConfig, HotRings, Snapshot};
pub use retention::{
    resolve_effective, sweep, EffectiveRetention, PerPairReport, RetentionPolicy, SweepError,
    SweepReport, EXPENSIVE_REWRITE_THRESHOLD,
};
pub use vector::{
    cosine_similarity, truncate_utf8, EmbedSelector, Embedder, EmbedderConfig, EmbedderMetrics,
    EmbedderTask, HashEmbedder, SearchHit, SharedEmbedder, VectorError, VectorFilter, VectorIndex,
    VectorRecord, EMBED_TRUNCATE_BYTES,
};
