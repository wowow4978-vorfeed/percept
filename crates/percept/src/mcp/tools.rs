//! Tool implementations: `describe_sources`, `get_current_state`, `get_window`.
//!
//! Tool surface mirrors DESIGN §7. `*_filter` args are optional shell-style
//! glob arrays; an empty / absent filter means "all".

use std::sync::Arc;

use percept_ingest::Metrics;
use percept_store::{
    filter_hash, resolve_effective, Anchor, ColdStore, Cursor, CursorError, EffectiveRetention,
    Embedder, HotRings, RetentionPolicy, VectorFilter, VectorIndex, WindowFilter,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::registry::DescriptorRegistry;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeSourcesArgs {
    #[serde(default)]
    pub source_filter: Option<Vec<String>>,
    #[serde(default)]
    pub kind_filter: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetCurrentStateArgs {
    #[serde(default)]
    pub source_filter: Option<Vec<String>>,
    #[serde(default)]
    pub kind_filter: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetWindowArgs {
    pub start_ms: i64,
    pub end_ms: i64,
    #[serde(default)]
    pub source_filter: Option<Vec<String>>,
    #[serde(default)]
    pub kind_filter: Option<Vec<String>>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DescribeSourcesEntry {
    #[serde(flatten)]
    pub descriptor: percept_core::ResolvedDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_errors: Option<percept_ingest::RecentErrors>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_retention: Option<EffectiveRetention>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct CurrentStateEntry {
    pub event: percept_core::Event,
    pub age_ms: i64,
    pub stale: bool,
    pub from_cold: bool,
    pub descriptor: percept_core::ResolvedDescriptor,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchEventsArgs {
    pub query: String,
    #[serde(default)]
    pub time_range: Option<TimeRange>,
    #[serde(default)]
    pub source_filter: Option<Vec<String>>,
    #[serde(default)]
    pub kind_filter: Option<Vec<String>>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(thiserror::Error, Debug)]
pub enum ToolError {
    #[error("invalid glob: {0}")]
    Glob(#[from] globset::Error),
    #[error("cold store unavailable: configure [server].data_dir")]
    NoColdStore,
    #[error("vector index unavailable: enable embedding via [storage].embed_default or per-kind / per-source `embed`")]
    NoVectorIndex,
    #[error("cold store error: {0}")]
    Cold(#[from] percept_store::ColdError),
    #[error("vector error: {0}")]
    Vector(#[from] percept_store::VectorError),
    #[error("invalid time range: end_ms ({end}) <= start_ms ({start})")]
    InvalidRange { start: i64, end: i64 },
    #[error("query string is empty")]
    EmptyQuery,
    #[error("cursor_filter_mismatch")]
    CursorFilterMismatch,
    #[error("cursor is malformed")]
    CursorMalformed,
}

pub fn describe_sources(
    registry: &DescriptorRegistry,
    metrics: &Metrics,
    retention_policies: &[RetentionPolicy],
    args: DescribeSourcesArgs,
) -> Result<serde_json::Value, ToolError> {
    let rows = registry.filter(args.source_filter.as_deref(), args.kind_filter.as_deref())?;
    let entries: Vec<DescribeSourcesEntry> = rows
        .into_iter()
        .map(|d| {
            let eff = resolve_effective(retention_policies, &d.source_id, &d.kind);
            DescribeSourcesEntry {
                recent_errors: metrics.recent_errors(&d.source_id),
                effective_retention: if eff.is_empty() { None } else { Some(eff) },
                descriptor: d.clone(),
            }
        })
        .collect();
    Ok(json!({ "sources": entries }))
}

pub fn get_current_state(
    registry: &DescriptorRegistry,
    hot_rings: &HotRings,
    cold_store: Option<&ColdStore>,
    args: GetCurrentStateArgs,
) -> Result<serde_json::Value, ToolError> {
    let rows = registry.filter(args.source_filter.as_deref(), args.kind_filter.as_deref())?;
    let now = percept_core::now_ms_utc();
    let mut entries: Vec<CurrentStateEntry> = Vec::new();
    for d in rows {
        let (event, from_cold) = match hot_rings.latest(&d.source_id, &d.kind) {
            Some(event) => (Arc::unwrap_or_clone(event), false),
            None => match cold_store {
                Some(store) => match store.latest(&d.source_id, &d.kind)? {
                    Some(e) => (e, true),
                    None => continue,
                },
                None => continue,
            },
        };
        let age_ms = now - event.ts_ms_utc;
        let stale = d.freshness_ttl_ms.is_some_and(|ttl| age_ms > ttl);
        entries.push(CurrentStateEntry {
            event,
            age_ms,
            stale,
            from_cold,
            descriptor: d.clone(),
        });
    }
    Ok(json!({ "states": entries }))
}

pub fn get_window(
    cold_store: Option<&ColdStore>,
    args: GetWindowArgs,
) -> Result<serde_json::Value, ToolError> {
    let store = cold_store.ok_or(ToolError::NoColdStore)?;
    if args.end_ms <= args.start_ms {
        return Err(ToolError::InvalidRange {
            start: args.start_ms,
            end: args.end_ms,
        });
    }
    let limit = args
        .limit
        .unwrap_or(percept_store::MAX_WINDOW_LIMIT)
        .min(percept_store::MAX_WINDOW_LIMIT);

    let hash = filter_hash(
        args.start_ms,
        args.end_ms,
        args.source_filter.as_deref(),
        args.kind_filter.as_deref(),
        limit,
    );

    let anchor = if let Some(c) = &args.cursor {
        match Cursor::decode(c, &hash) {
            Ok(cur) => Some(cur.anchor),
            Err(CursorError::FilterMismatch) => return Err(ToolError::CursorFilterMismatch),
            Err(CursorError::Malformed) => return Err(ToolError::CursorMalformed),
        }
    } else {
        None
    };

    let filter = WindowFilter {
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        source_filter: args.source_filter.clone(),
        kind_filter: args.kind_filter.clone(),
        limit,
    };
    let events = store.query_window(&filter, anchor)?;

    // A short page (fewer than `limit`) means we exhausted the window;
    // omit the cursor to signal "done".
    let next_cursor = if u32::try_from(events.len()).is_ok_and(|n| n >= limit) {
        events.last().map(|e| {
            Cursor {
                anchor: Anchor {
                    ts_ms_utc: e.ts_ms_utc,
                    event_id: e.event_id,
                },
                filter_hash: hash,
            }
            .encode()
        })
    } else {
        None
    };

    Ok(json!({
        "events": events,
        "cursor": next_cursor,
    }))
}

/// Default top-k for `search_events` when the caller omits `limit`.
pub const DEFAULT_SEARCH_LIMIT: u32 = 10;
/// Per-call hard cap, mirrors DESIGN §11.1 (`top-k ≤ 50`).
pub const MAX_SEARCH_LIMIT: u32 = 50;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SearchHitOut {
    pub event: percept_core::Event,
    pub score: f32,
    pub truncated: bool,
}

pub fn search_events(
    vector_index: Option<&VectorIndex>,
    embedder: Option<&dyn Embedder>,
    cold_store: Option<&ColdStore>,
    args: SearchEventsArgs,
) -> Result<serde_json::Value, ToolError> {
    let index = vector_index.ok_or(ToolError::NoVectorIndex)?;
    let embedder = embedder.ok_or(ToolError::NoVectorIndex)?;
    let store = cold_store.ok_or(ToolError::NoColdStore)?;

    if args.query.trim().is_empty() {
        return Err(ToolError::EmptyQuery);
    }
    let (start_ms, end_ms) = if let Some(t) = &args.time_range {
        if t.end_ms <= t.start_ms {
            return Err(ToolError::InvalidRange {
                start: t.start_ms,
                end: t.end_ms,
            });
        }
        (Some(t.start_ms), Some(t.end_ms))
    } else {
        (None, None)
    };

    let top_k = args
        .limit
        .unwrap_or(DEFAULT_SEARCH_LIMIT)
        .clamp(1, MAX_SEARCH_LIMIT) as usize;

    let query_vec = embedder.embed(&args.query);
    let filter = VectorFilter {
        start_ms,
        end_ms,
        source_filter: args.source_filter.clone(),
        kind_filter: args.kind_filter.clone(),
    };
    let hits = index.search_kn(&query_vec, top_k, &filter)?;

    // Hydrate each hit with the full event from cold storage. A missing
    // event (vector exists but cold row was dropped — possible once
    // retention lands) is skipped, not surfaced.
    let mut out: Vec<SearchHitOut> = Vec::with_capacity(hits.len());
    for hit in hits {
        if let Some(event) = store.get_by_id(&hit.event_id)? {
            out.push(SearchHitOut {
                event,
                score: hit.score,
                truncated: hit.truncated,
            });
        }
    }

    Ok(json!({ "hits": out }))
}

pub fn describe_sources_input_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "source_filter": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Shell-style globs against source_id. Omit for all."
            },
            "kind_filter": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Shell-style globs against kind. Omit for all."
            }
        }
    })
}

pub fn get_current_state_input_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "source_filter": {
                "type": "array",
                "items": { "type": "string" }
            },
            "kind_filter": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

pub fn get_window_input_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["start_ms", "end_ms"],
        "properties": {
            "start_ms": {
                "type": "integer",
                "description": "UTC ms, inclusive."
            },
            "end_ms": {
                "type": "integer",
                "description": "UTC ms, exclusive."
            },
            "source_filter": { "type": "array", "items": { "type": "string" } },
            "kind_filter": { "type": "array", "items": { "type": "string" } },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10000
            },
            "cursor": {
                "type": "string",
                "description": "Opaque cursor returned by a previous get_window call."
            }
        }
    })
}

pub fn search_events_input_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "description": "Free-text query to embed and ANN-search the vector index."
            },
            "time_range": {
                "type": "object",
                "additionalProperties": false,
                "required": ["start_ms", "end_ms"],
                "properties": {
                    "start_ms": { "type": "integer" },
                    "end_ms": { "type": "integer" }
                }
            },
            "source_filter": { "type": "array", "items": { "type": "string" } },
            "kind_filter": { "type": "array", "items": { "type": "string" } },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "description": "Top-k cap (default 10, hard max 50 per DESIGN §11.1)."
            }
        }
    })
}
