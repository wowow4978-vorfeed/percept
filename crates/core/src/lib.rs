//! Canonical types and helpers for Percept.
//!
//! See `docs/DESIGN.md` §3 for the data-model contract.

mod descriptor;
mod error;
mod event;
mod id;
mod kind_ref;
mod time;

pub use descriptor::{resolve, KindDescriptor, ResolvedDescriptor, SourceDescriptor};
pub use error::Error;
pub use event::{Event, Link};
pub use id::new_event_id;
pub use kind_ref::KindRef;
pub use time::now_ms_utc;
