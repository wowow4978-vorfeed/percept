//! Retention policy resolution + sweeper.
//!
//! Slice 5: DESIGN §11.3 policy model with source > kind > global
//! resolution per dimension; sweeper executes deletes against the cold
//! store and vector index. Background scheduler lives in
//! percept-ingest::pipeline (so it shares the runtime with the rest).

mod policy;
mod sweeper;

pub use policy::{resolve_effective, EffectiveRetention, RetentionPolicy};
pub use sweeper::{sweep, PerPairReport, SweepError, SweepReport, EXPENSIVE_REWRITE_THRESHOLD};
