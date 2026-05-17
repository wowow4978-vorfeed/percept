//! Binds the configured listener and runs the ingest + MCP pipeline until
//! shut down.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use percept_ingest::pipeline::VectorSubsystem;
use percept_ingest::{Auth, Pipeline, PipelineConfig, SchemaIndex, TokenScope};
use percept_store::{ColdStore, EmbedSelector, Embedder, HashEmbedder, VectorIndex};

use crate::config::{self, Config, HttpToken};
use crate::mcp::{DescriptorRegistry, McpState};
use crate::sweeper::{Sweeper, SweeperConfig};

/// Build the ingest + MCP pipeline from a loaded config, bind the configured
/// listener, and serve until SIGINT.
pub async fn run(cfg: Config) -> Result<()> {
    let mcp_cfg = cfg
        .mcp
        .as_ref()
        .ok_or_else(|| anyhow!("[mcp] section required to bind a listener"))?;
    let addr: SocketAddr = mcp_cfg.listen.parse().with_context(|| {
        format!(
            "[mcp] listen = {:?} is not a valid socket address",
            mcp_cfg.listen
        )
    })?;
    if let Some(t) = &mcp_cfg.transport {
        if t != "http-streamable" {
            bail!("[mcp] transport = {t:?}: only \"http-streamable\" is supported in v1");
        }
    }
    let mcp_token = mcp_cfg
        .auth
        .resolved
        .clone()
        .ok_or_else(|| anyhow!("[mcp].auth token not resolved"))?;

    let auth = build_auth(&cfg.http_tokens)?;
    if auth.is_empty() {
        tracing::warn!("no [[http_token]] entries — /ingest will reject every request");
    }

    let sources = config::build_source_descriptors(&cfg);
    let kinds = config::build_kind_descriptors(&cfg);
    let schemas = SchemaIndex::build(&sources, &kinds).context("compiling semantic_schema")?;
    let registry = DescriptorRegistry::new(config::resolve_descriptors(&cfg));

    let cold_store = match cfg.server.as_ref().map(|s| s.data_dir.clone()) {
        Some(dir) => Some(Arc::new(
            ColdStore::open(std::path::Path::new(&dir)).context("opening cold store")?,
        )),
        None => {
            tracing::warn!("no [server].data_dir — cold store disabled");
            None
        }
    };

    let vector = build_vector_subsystem(&cfg).context("opening vector index")?;
    let vector_index = vector.as_ref().map(|v| v.index.clone());

    let pipeline = Pipeline::spawn(
        Arc::new(auth),
        Arc::new(schemas),
        cold_store.clone(),
        vector,
        PipelineConfig::default(),
    );

    // Retention policies + sweeper. Wires only if there's a cold store
    // (without one, there's nothing to sweep).
    let retention_policies =
        Arc::new(config::build_retention_policies(&cfg).context("parsing [[retention]]")?);
    if let Some(cold) = &cold_store {
        let sweeper = Sweeper::new(
            cold.clone(),
            vector_index.clone(),
            retention_policies.clone(),
            SweeperConfig {
                cadence: cfg
                    .storage
                    .as_ref()
                    .and_then(|s| s.sweeper_interval.as_deref())
                    .and_then(parse_cadence_or_warn)
                    .unwrap_or_else(|| std::time::Duration::from_secs(3600)),
            },
        );
        tokio::spawn(sweeper.run());
    }

    let mcp_state = McpState {
        token: Arc::new(mcp_token),
        registry: Arc::new(registry),
        hot_rings: pipeline.hot_rings.clone(),
        cold_store,
        vector_index,
        embedder: pipeline.vector_index.as_ref().map(|_| current_embedder()),
        retention_policies,
        metrics: pipeline.metrics.clone(),
    };

    let app =
        percept_ingest::router(pipeline.http_state.clone()).merge(crate::mcp::router(mcp_state));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(?addr, "ingest + MCP listener bound");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;

    // Drop the sender to close the normalizer's input channel and wait for
    // it to drain.
    drop(pipeline.http_state);
    let _ = pipeline.normalizer_handle.await;
    Ok(())
}

fn build_auth(tokens: &[HttpToken]) -> Result<Auth> {
    let mut auth = Auth::new();
    for t in tokens {
        let resolved = t
            .resolved_token
            .as_ref()
            .ok_or_else(|| anyhow!("[[http_token]] {:?}: token not resolved", t.name))?;
        let scope = TokenScope::build(
            &t.name,
            &t.allow_source_ids,
            &t.allow_kinds,
            t.rate_limit.as_deref(),
        )
        .with_context(|| format!("[[http_token]] {:?}", t.name))?;
        auth.insert(resolved.clone(), scope);
    }
    Ok(auth)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

/// Best-effort cadence parse for `[storage] sweeper_interval = "1h"`. A
/// bad value falls back to the default with a warning rather than
/// failing startup, since the field is operational.
fn parse_cadence_or_warn(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    let (num_str, unit) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| s.split_at(i))?;
    let n: u64 = num_str.parse().ok()?;
    let secs = match unit {
        "s" => n,
        "m" => n.checked_mul(60)?,
        "h" => n.checked_mul(3600)?,
        "d" => n.checked_mul(86_400)?,
        other => {
            tracing::warn!(
                "[storage] sweeper_interval = {s:?}: unknown unit {other:?}, using default"
            );
            return None;
        }
    };
    Some(std::time::Duration::from_secs(secs))
}

/// Build the v1 placeholder embedder. Returns `Arc<dyn Embedder>`. The
/// FastEmbed/`bge-small-en-v1.5` swap-in lands in a slice-4 follow-up.
fn current_embedder() -> Arc<dyn Embedder> {
    Arc::new(HashEmbedder::new(64))
}

/// Resolve config into a VectorSubsystem. Returns `None` when there's no
/// cold store data_dir (the vector index needs disk) or when no
/// `[storage] embed_default = true` / per-source / per-kind opt-in.
fn build_vector_subsystem(cfg: &Config) -> Result<Option<VectorSubsystem>> {
    // Need a data_dir to persist vectors. Without it, the index would be
    // RAM-only and lose state across restarts — not in scope for v1.
    let Some(data_dir) = cfg.server.as_ref().map(|s| s.data_dir.clone()) else {
        return Ok(None);
    };

    let embed_default = cfg
        .storage
        .as_ref()
        .and_then(|s| s.embed_default)
        .unwrap_or(false);
    let mut selector = EmbedSelector::new(embed_default);
    for k in &cfg.kinds {
        if let Some(v) = k.embed {
            selector.set_kind(k.name.clone(), v);
        }
    }
    for s in &cfg.sources {
        if let Some(v) = s.embed {
            selector.set_source(s.id.clone(), v);
        }
    }
    let any_enabled = embed_default
        || cfg.kinds.iter().any(|k| k.embed == Some(true))
        || cfg.sources.iter().any(|s| s.embed == Some(true));
    if !any_enabled {
        tracing::info!("no kinds or sources opted into embedding — vector subsystem disabled");
        return Ok(None);
    }

    let embedder = current_embedder();
    let index = Arc::new(
        VectorIndex::open(
            std::path::Path::new(&data_dir),
            embedder.model_id(),
            embedder.dim(),
        )
        .context("opening vector index")?,
    );

    Ok(Some(VectorSubsystem {
        embedder,
        index,
        selector: Arc::new(selector),
    }))
}
