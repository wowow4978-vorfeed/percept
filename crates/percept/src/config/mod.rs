//! Configuration loader: TOML, `conf.d/*.toml` merge, secret resolution,
//! and the validation rules called out in `docs/PLAN.md` Slice 0.

pub mod schema;
mod secrets;
mod validate;

use std::path::{Path, PathBuf};

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use percept_core::{resolve, KindDescriptor, ResolvedDescriptor, SourceDescriptor};
use percept_store::RetentionPolicy;

pub use schema::{Config, HttpToken, KindEntry, RetentionEntry, SourceEntry};

/// Load configuration from `path`, then merge any `<path>.d/*.toml` files
/// (later filenames win for scalar fields; array-of-tables entries
/// accumulate). Resolves `*_env` / `*_file` secret indirections and runs
/// the validation rules in `validate`.
pub fn load(path: &Path) -> Result<Config> {
    let primary = read_toml(path)?;
    let mut cfg = primary;

    let confd: PathBuf = {
        let mut p = path.as_os_str().to_owned();
        p.push(".d");
        PathBuf::from(p)
    };
    if confd.is_dir() {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&confd)
            .with_context(|| format!("reading {}", confd.display()))?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "toml"))
            .collect();
        entries.sort();
        for entry in entries {
            let overlay = read_toml(&entry)?;
            cfg.merge(overlay);
        }
    }

    secrets::resolve(&mut cfg)?;
    validate::check(&cfg)?;
    Ok(cfg)
}

fn read_toml(path: &Path) -> Result<Config> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let cfg: Config = toml::from_str(&text).map_err(|e| anyhow!("{}: {}", path.display(), e))?;
    Ok(cfg)
}

/// Resolve every `(source, kind)` pair into the LLM-visible merged view.
/// A source listing a kind with no matching `KindDescriptor` falls back to
/// a synthetic kind with empty defaults — the resolution still produces a
/// row so the LLM sees the source.
#[must_use]
pub fn resolve_descriptors(cfg: &Config) -> Vec<ResolvedDescriptor> {
    let mut out = Vec::new();
    for s in &cfg.sources {
        let source = source_descriptor(s);
        for kind_name in &s.kinds {
            let kind_desc = cfg
                .kinds
                .iter()
                .find(|k| &k.name == kind_name)
                .map_or_else(|| synthetic_kind(kind_name), kind_descriptor);
            out.push(resolve(&source, &kind_desc));
        }
    }
    out
}

/// Build the canonical `SourceDescriptor` list from `[[source]]` entries.
#[must_use]
pub fn build_source_descriptors(cfg: &Config) -> Vec<SourceDescriptor> {
    cfg.sources.iter().map(source_descriptor).collect()
}

/// Build the canonical `KindDescriptor` list from `[[kind]]` entries.
#[must_use]
pub fn build_kind_descriptors(cfg: &Config) -> Vec<KindDescriptor> {
    cfg.kinds.iter().map(kind_descriptor).collect()
}

/// Translate `[[retention]]` entries into the storage-layer policy type.
/// Durations are parsed by the slice-0 validator at config-load; an
/// unparseable duration here is an internal bug (validate would have
/// errored already).
pub fn build_retention_policies(cfg: &Config) -> Result<Vec<RetentionPolicy>> {
    let mut out = Vec::new();
    for r in &cfg.retention {
        let max_age = match &r.max_age {
            Some(s) => Some(parse_duration_str(s).with_context(|| format!("max_age = {s:?}"))?),
            None => None,
        };
        let vector_max_age = match &r.vector_max_age {
            Some(s) => {
                Some(parse_duration_str(s).with_context(|| format!("vector_max_age = {s:?}"))?)
            }
            None => None,
        };
        out.push(RetentionPolicy {
            match_source_id: r.r#match.source_id.clone(),
            match_kind: r.r#match.kind.clone(),
            max_age,
            max_count: r.max_count,
            max_bytes: r.max_bytes,
            vector_max_age,
        });
    }
    Ok(out)
}

/// Same duration grammar as `validate::parse_duration` — kept private
/// to that module, mirrored here so descriptor-building doesn't drag in
/// the validate internals.
fn parse_duration_str(s: &str) -> Result<Duration> {
    let s = s.trim();
    let (num_str, unit) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| s.split_at(i))
        .ok_or_else(|| anyhow!("missing unit suffix in {s:?}"))?;
    if num_str.is_empty() {
        return Err(anyhow!("missing numeric value in {s:?}"));
    }
    let n: u64 = num_str
        .parse()
        .map_err(|_| anyhow!("invalid number in {s:?}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n.checked_mul(60).ok_or_else(|| anyhow!("overflow"))?,
        "h" => n.checked_mul(3600).ok_or_else(|| anyhow!("overflow"))?,
        "d" => n.checked_mul(86_400).ok_or_else(|| anyhow!("overflow"))?,
        other => return Err(anyhow!("unknown unit {other:?} (expected s/m/h/d)")),
    };
    Ok(Duration::from_secs(secs))
}

fn source_descriptor(s: &SourceEntry) -> SourceDescriptor {
    SourceDescriptor {
        source_id: s.id.clone(),
        kinds: s.kinds.clone(),
        description: s.description.clone().unwrap_or_default(),
        usage: s.usage.clone().unwrap_or_default(),
        caveats: s.caveats.clone().unwrap_or_default(),
        semantic_schema: s.semantic_schema.clone(),
        units: s.units.clone(),
        sampling_hint_ms: s.sampling_hint_ms,
        freshness_ttl_ms: s.freshness_ttl_ms,
        location: s.location.clone(),
        updated_ts_ms_utc: 0,
    }
}

fn kind_descriptor(k: &KindEntry) -> KindDescriptor {
    KindDescriptor {
        kind: k.name.clone(),
        version: k.version.clone().unwrap_or_else(|| "v1".to_string()),
        description: k.description.clone().unwrap_or_default(),
        usage: k.usage.clone().unwrap_or_default(),
        caveats: k.caveats.clone().unwrap_or_default(),
        semantic_schema: k.semantic_schema.clone(),
        units: k.units.clone(),
        updated_ts_ms_utc: 0,
    }
}

fn synthetic_kind(name: &str) -> KindDescriptor {
    KindDescriptor {
        kind: name.to_string(),
        version: "v1".to_string(),
        description: String::new(),
        usage: String::new(),
        caveats: String::new(),
        semantic_schema: None,
        units: None,
        updated_ts_ms_utc: 0,
    }
}
