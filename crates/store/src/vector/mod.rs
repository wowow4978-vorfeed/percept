//! Vector index: embedding generation + SQLite-persisted store +
//! brute-force cosine kNN.
//!
//! Slice 4 ships the wiring with a deterministic `HashEmbedder` so the
//! `search_events` MCP tool, the truncation rule, and the in-memory mirror
//! are all exercised end-to-end. The real embedder (FastEmbed-rs +
//! `bge-small-en-v1.5`) lands in a slice-4 follow-up — same swap point
//! as the slice-1 schema runtime.

mod embedder;
mod index;
mod selector;
mod task;
mod truncate;

pub use embedder::{cosine_similarity, Embedder, HashEmbedder, SharedEmbedder};
pub use index::{SearchHit, VectorError, VectorFilter, VectorIndex, VectorRecord};
pub use selector::EmbedSelector;
pub use task::{EmbedderConfig, EmbedderMetrics, EmbedderTask, EMBED_TRUNCATE_BYTES};
pub use truncate::truncate_utf8;
