//! Tool implementations: `describe_sources` and `get_current_state`.
//!
//! Tool surface mirrors DESIGN §7. Both take optional `source_filter` /
//! `kind_filter` glob arrays; an empty / absent filter means "all".

use std::sync::Arc;

use percept_ingest::Metrics;
use percept_store::HotRings;
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

#[derive(Debug, Serialize)]
pub struct DescribeSourcesEntry {
    #[serde(flatten)]
    pub descriptor: percept_core::ResolvedDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_errors: Option<percept_ingest::RecentErrors>,
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

#[derive(thiserror::Error, Debug)]
pub enum ToolError {
    #[error("invalid glob: {0}")]
    Glob(#[from] globset::Error),
}

pub fn describe_sources(
    registry: &DescriptorRegistry,
    metrics: &Metrics,
    args: DescribeSourcesArgs,
) -> Result<serde_json::Value, ToolError> {
    let rows = registry.filter(args.source_filter.as_deref(), args.kind_filter.as_deref())?;
    let entries: Vec<DescribeSourcesEntry> = rows
        .into_iter()
        .map(|d| DescribeSourcesEntry {
            recent_errors: metrics.recent_errors(&d.source_id),
            descriptor: d.clone(),
        })
        .collect();
    Ok(json!({ "sources": entries }))
}

pub fn get_current_state(
    registry: &DescriptorRegistry,
    hot_rings: &HotRings,
    args: GetCurrentStateArgs,
) -> Result<serde_json::Value, ToolError> {
    let rows = registry.filter(args.source_filter.as_deref(), args.kind_filter.as_deref())?;
    let now = percept_core::now_ms_utc();
    let mut entries: Vec<CurrentStateEntry> = Vec::new();
    for d in rows {
        let Some(event) = hot_rings.latest(&d.source_id, &d.kind) else {
            continue;
        };
        let event = Arc::unwrap_or_clone(event);
        let age_ms = now - event.ts_ms_utc;
        let stale = d.freshness_ttl_ms.is_some_and(|ttl| age_ms > ttl);
        entries.push(CurrentStateEntry {
            event,
            age_ms,
            stale,
            from_cold: false,
            descriptor: d.clone(),
        });
    }
    Ok(json!({ "states": entries }))
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
