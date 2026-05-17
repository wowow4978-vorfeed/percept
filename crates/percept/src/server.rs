//! Binds the configured listener and runs the ingest pipeline until shut down.
//!
//! Slice 1 wires HTTP only — MCP joins in slice 2.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use percept_ingest::{Auth, Pipeline, PipelineConfig, TokenScope};

use crate::config::{Config, HttpToken};

/// Build the ingest pipeline from a loaded config, bind the configured MCP
/// listen address (Slice 1 reuses it for the HTTP ingest), and serve until
/// SIGINT.
pub async fn run(cfg: Config) -> Result<()> {
    let mcp = cfg
        .mcp
        .as_ref()
        .ok_or_else(|| anyhow!("[mcp] section required to bind a listener"))?;
    let addr: SocketAddr = mcp.listen.parse().with_context(|| {
        format!(
            "[mcp] listen = {:?} is not a valid socket address",
            mcp.listen
        )
    })?;

    let auth = build_auth(&cfg.http_tokens)?;
    if auth.is_empty() {
        tracing::warn!("no [[http_token]] entries — /ingest will reject every request");
    }

    let pipeline = Pipeline::spawn(Arc::new(auth), PipelineConfig::default());
    let app = percept_ingest::router(pipeline.http_state.clone());

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(?addr, "ingest listener bound");

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
