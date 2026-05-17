//! Resolve `*_env` / `*_file` indirections after parsing.
//!
//! Exactly one of the two variants must be set per secret site; both-set
//! or neither-set is a config error.

use std::path::Path;

use anyhow::{bail, Context, Result};

use super::schema::{Config, HttpToken, MqttCredentials, SecretRef};

pub fn resolve(cfg: &mut Config) -> Result<()> {
    if let Some(mcp) = &mut cfg.mcp {
        resolve_secret(&mut mcp.auth, "[mcp.auth] token")?;
    }
    for broker in &mut cfg.mqtt {
        if let Some(creds) = &mut broker.credentials {
            resolve_mqtt_password(creds, &broker.id)?;
        }
    }
    for token in &mut cfg.http_tokens {
        resolve_http_token(token)?;
    }
    Ok(())
}

fn resolve_secret(s: &mut SecretRef, site: &str) -> Result<()> {
    let value = read_one_of(&s.token_env, &s.token_file, site, "token")?;
    s.resolved = Some(value);
    Ok(())
}

fn resolve_mqtt_password(c: &mut MqttCredentials, broker_id: &str) -> Result<()> {
    let site = format!("[mqtt.{broker_id}.credentials] password");
    let value = read_one_of(&c.password_env, &c.password_file, &site, "password")?;
    c.resolved_password = Some(value);
    Ok(())
}

fn resolve_http_token(t: &mut HttpToken) -> Result<()> {
    let site = format!("[http_token \"{}\"] token", t.name);
    let value = read_one_of(&t.token_env, &t.token_file, &site, "token")?;
    t.resolved_token = Some(value);
    Ok(())
}

fn read_one_of(
    env: &Option<String>,
    file: &Option<String>,
    site: &str,
    kind: &str,
) -> Result<String> {
    match (env, file) {
        (Some(_), Some(_)) => bail!("{site}: both {kind}_env and {kind}_file set; choose one"),
        (None, None) => bail!("{site}: neither {kind}_env nor {kind}_file set"),
        (Some(var), None) => {
            std::env::var(var).with_context(|| format!("{site}: env var {var} is not set"))
        }
        (None, Some(path)) => std::fs::read_to_string(Path::new(path))
            .map(|s| s.trim_end_matches(['\r', '\n']).to_string())
            .with_context(|| format!("{site}: reading {path}")),
    }
}
