//! Validation rules called out in PLAN.md Slice 0 acceptance.
//!
//! - duplicate `source_id`
//! - retention with `vector_max_age > max_age`
//! - profile must be "edge" in v1
//! - retention match must specify at least one of `source_id` / `kind`
//!
//! Inline-secret rejection and unknown-key rejection happen at parse time
//! (via `deny_unknown_fields`); unresolvable `*_env` errors happen during
//! secret resolution. Those checks live next to the relevant code, not
//! here.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{bail, Result};

use super::schema::Config;

pub fn check(cfg: &Config) -> Result<()> {
    if let Some(server) = &cfg.server {
        if server.profile != "edge" {
            bail!(
                "[server] profile = {:?}: only \"edge\" is accepted in v1",
                server.profile
            );
        }
    }

    let mut seen = HashSet::new();
    for s in &cfg.sources {
        if !seen.insert(s.id.clone()) {
            bail!("duplicate [[source]] id = {:?}", s.id);
        }
    }

    for (i, r) in cfg.retention.iter().enumerate() {
        if r.r#match.source_id.is_none() && r.r#match.kind.is_none() {
            bail!("[[retention]] #{}: match must set source_id or kind", i);
        }
        if let (Some(max_age), Some(vmax)) = (&r.max_age, &r.vector_max_age) {
            let raw = parse_duration(max_age)
                .map_err(|e| anyhow::anyhow!("[[retention]] #{i}: max_age: {e}"))?;
            let vec = parse_duration(vmax)
                .map_err(|e| anyhow::anyhow!("[[retention]] #{i}: vector_max_age: {e}"))?;
            if vec > raw {
                bail!(
                    "[[retention]] #{i}: vector_max_age ({vmax}) > max_age ({max_age}); \
                     vectors cannot outlive their source events"
                );
            }
        }
    }

    Ok(())
}

/// Minimal duration parser for the units DESIGN.md uses in examples: `s`,
/// `m`, `h`, `d`. No fractional values; integer scalar + unit suffix.
fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num_str, unit) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| s.split_at(i))
        .ok_or_else(|| format!("missing unit suffix in {s:?}"))?;
    if num_str.is_empty() {
        return Err(format!("missing numeric value in {s:?}"));
    }
    let n: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid number in {s:?}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n.checked_mul(60).ok_or("overflow")?,
        "h" => n.checked_mul(3600).ok_or("overflow")?,
        "d" => n.checked_mul(86_400).ok_or("overflow")?,
        other => return Err(format!("unknown unit {other:?} (expected s/m/h/d)")),
    };
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_units() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(30 * 86_400)
        );
    }

    #[test]
    fn duration_rejects_garbage() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("10y").is_err());
        assert!(parse_duration("").is_err());
    }
}
