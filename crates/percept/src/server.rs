//! Binds the configured listener and runs the ingest + MCP pipeline until
//! shut down.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use percept_ingest::{Auth, Pipeline, PipelineConfig, SchemaIndex, TokenScope};

use crate::config::{self, Config, HttpToken};
use crate::mcp::{DescriptorRegistry, McpState};

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

    let pipeline = Pipeline::spawn(Arc::new(auth), Arc::new(schemas), PipelineConfig::default());

    let mcp_state = McpState {
        token: Arc::new(mcp_token),
        registry: Arc::new(registry),
        hot_rings: pipeline.hot_rings.clone(),
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
