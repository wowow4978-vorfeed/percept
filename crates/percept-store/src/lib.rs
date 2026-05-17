//! Storage layer for Percept.
//!
//! Slice 1: in-memory hot rings only. Cold store and vector index land in
//! slices 3 and 4 respectively.

mod hot_ring;

pub use hot_ring::{HotRingConfig, HotRings, Snapshot};
