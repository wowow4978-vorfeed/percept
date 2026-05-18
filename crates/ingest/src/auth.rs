//! Bearer-token authn + glob scope checks + per-token rate limiting.
//!
//! See DESIGN §12.6: `allow_source_ids` / `allow_kinds` are shell-style
//! globs; rate_limit is `"N/s"`. Tokens with no allowlist write nothing
//! (default-deny).

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use globset::{Glob, GlobSet, GlobSetBuilder};
use governor::clock::{Clock, DefaultClock};
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use thiserror::Error;

/// Reason an event was shed at the ingest boundary. Goes into the
/// `X-Percept-Shed-Reason` response header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedReason {
    BusFull,
    RateLimit,
    Unauthorized,
    PayloadTooLarge,
    UnresolvedKind,
}

impl ShedReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BusFull => "bus_full",
            Self::RateLimit => "rate_limit",
            Self::Unauthorized => "unauthorized",
            Self::PayloadTooLarge => "payload_too_large",
            Self::UnresolvedKind => "unresolved_kind",
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("invalid glob {pattern:?}: {source}")]
    InvalidGlob {
        pattern: String,
        #[source]
        source: globset::Error,
    },
    #[error("invalid rate_limit {0:?}: expected \"N/s\"")]
    InvalidRateLimit(String),
}

type DirectLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

pub struct TokenScope {
    pub name: String,
    pub source_globs: GlobSet,
    pub kind_globs: GlobSet,
    pub limiter: Option<Arc<DirectLimiter>>,
}

impl TokenScope {
    pub fn build(
        name: impl Into<String>,
        source_patterns: &[String],
        kind_patterns: &[String],
        rate_limit: Option<&str>,
    ) -> Result<Self, AuthError> {
        Ok(Self {
            name: name.into(),
            source_globs: build_globs(source_patterns)?,
            kind_globs: build_globs(kind_patterns)?,
            limiter: rate_limit.map(parse_rate_limit).transpose()?,
        })
    }

    #[must_use]
    pub fn allows(&self, source_id: &str, kind: &str) -> bool {
        self.source_globs.is_match(source_id) && self.kind_globs.is_match(kind)
    }

    /// Returns `Some(retry_after)` when rate-limited; `None` when allowed.
    pub fn check_rate(&self) -> Option<Duration> {
        let limiter = self.limiter.as_ref()?;
        match limiter.check() {
            Ok(()) => None,
            Err(neg) => Some(neg.wait_time_from(DefaultClock::default().now())),
        }
    }
}

fn build_globs(patterns: &[String]) -> Result<GlobSet, AuthError> {
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        let g = Glob::new(p).map_err(|source| AuthError::InvalidGlob {
            pattern: p.clone(),
            source,
        })?;
        b.add(g);
    }
    b.build().map_err(|source| AuthError::InvalidGlob {
        pattern: patterns.join(","),
        source,
    })
}

fn parse_rate_limit(s: &str) -> Result<Arc<DirectLimiter>, AuthError> {
    let (n_str, unit) = s
        .split_once('/')
        .ok_or_else(|| AuthError::InvalidRateLimit(s.to_string()))?;
    let n: u32 = n_str
        .trim()
        .parse()
        .map_err(|_| AuthError::InvalidRateLimit(s.to_string()))?;
    let n = NonZeroU32::new(n).ok_or_else(|| AuthError::InvalidRateLimit(s.to_string()))?;
    let quota = match unit.trim() {
        "s" => Quota::per_second(n),
        "m" => Quota::per_minute(n),
        "h" => Quota::per_hour(n),
        _ => return Err(AuthError::InvalidRateLimit(s.to_string())),
    };
    Ok(Arc::new(RateLimiter::direct(quota)))
}

/// Map of resolved bearer-token value -> scope.
pub struct Auth {
    by_token: HashMap<String, Arc<TokenScope>>,
}

impl Auth {
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_token: HashMap::new(),
        }
    }

    pub fn insert(&mut self, token_value: impl Into<String>, scope: TokenScope) {
        self.by_token.insert(token_value.into(), Arc::new(scope));
    }

    #[must_use]
    pub fn lookup(&self, token_value: &str) -> Option<Arc<TokenScope>> {
        self.by_token.get(token_value).cloned()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_token.is_empty()
    }
}

impl Default for Auth {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_allows_matching_source_and_kind() {
        let s = TokenScope::build(
            "t",
            &["cam.front_door".into(), "cam.front_door.*".into()],
            &["object_detected".into(), "scene_description".into()],
            None,
        )
        .unwrap();
        assert!(s.allows("cam.front_door", "object_detected"));
        assert!(s.allows("cam.front_door.zone1", "object_detected"));
        assert!(!s.allows("cam.back_yard", "object_detected"));
        assert!(!s.allows("cam.front_door", "ble.advert"));
    }

    #[test]
    fn empty_allowlist_is_default_deny() {
        let s = TokenScope::build("t", &[], &[], None).unwrap();
        assert!(!s.allows("anything", "anything"));
    }

    #[test]
    fn parses_rate_limit_syntax() {
        assert!(parse_rate_limit("100/s").is_ok());
        assert!(parse_rate_limit("10/m").is_ok());
        assert!(parse_rate_limit("1/h").is_ok());
        assert!(parse_rate_limit("0/s").is_err());
        assert!(parse_rate_limit("garbage").is_err());
        assert!(parse_rate_limit("10/d").is_err());
    }

    #[test]
    fn shed_reason_strings() {
        assert_eq!(ShedReason::BusFull.as_str(), "bus_full");
        assert_eq!(ShedReason::Unauthorized.as_str(), "unauthorized");
    }
}
