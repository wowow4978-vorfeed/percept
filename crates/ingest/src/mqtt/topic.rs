//! MQTT topic-filter matching + capture-template substitution.
//!
//! DESIGN §12.4 uses two distinct grammars:
//! - Wildcards in the **MQTT filter** are MQTT's standard `+` (one level)
//!   and `#` (rest); literal levels are exact-match.
//! - Capture templates in `source_id_template` use `{+1}`, `{+2}`, ... for
//!   the ordered `+` captures and `{#}` for the `#` tail. No regex.

#[derive(Debug, Clone)]
pub struct TopicMatcher {
    filter: Vec<TopicSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TopicSegment {
    Literal(String),
    SinglePlus,
    Hash,
}

impl TopicMatcher {
    pub fn new(filter: &str) -> Self {
        let mut out = Vec::new();
        for part in filter.split('/') {
            out.push(match part {
                "+" => TopicSegment::SinglePlus,
                "#" => TopicSegment::Hash,
                s => TopicSegment::Literal(s.to_string()),
            });
        }
        Self { filter: out }
    }

    /// Try to match `topic` against the filter. Returns the ordered list of
    /// `+`-captured segments plus the optional `#` tail.
    #[must_use]
    pub fn captures(&self, topic: &str) -> Option<TopicCaptures> {
        let parts: Vec<&str> = topic.split('/').collect();
        let mut pluses = Vec::new();
        let mut idx = 0;
        for seg in &self.filter {
            match seg {
                TopicSegment::Hash => {
                    let tail = parts[idx..].join("/");
                    return Some(TopicCaptures {
                        pluses,
                        hash: Some(tail),
                    });
                }
                TopicSegment::SinglePlus => {
                    if idx >= parts.len() {
                        return None;
                    }
                    pluses.push(parts[idx].to_string());
                    idx += 1;
                }
                TopicSegment::Literal(s) => {
                    if idx >= parts.len() || parts[idx] != s {
                        return None;
                    }
                    idx += 1;
                }
            }
        }
        if idx != parts.len() {
            return None;
        }
        Some(TopicCaptures { pluses, hash: None })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicCaptures {
    pub pluses: Vec<String>,
    pub hash: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("unknown placeholder {0:?} in template (expected {{+N}} or {{#}})")]
    UnknownPlaceholder(String),
    #[error("placeholder {{+{0}}} is out of range for {1} `+` captures")]
    OutOfRange(usize, usize),
    #[error("template references {{#}} but the topic filter has no `#`")]
    NoHash,
}

/// Substitute `{+1}`, `{+2}`, … and `{#}` placeholders in `template`
/// against the captures from `topic`.
pub fn render(template: &str, captures: &TopicCaptures) -> Result<String, TemplateError> {
    let mut out = String::with_capacity(template.len());
    let mut iter = template.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        if c != '{' {
            out.push(c);
            continue;
        }
        // Scan to matching '}'.
        let rest = &template[i + 1..];
        let Some(end_rel) = rest.find('}') else {
            return Err(TemplateError::UnknownPlaceholder(rest.to_string()));
        };
        let body = &rest[..end_rel];
        match body {
            "#" => match &captures.hash {
                Some(s) => out.push_str(s),
                None => return Err(TemplateError::NoHash),
            },
            other => {
                let Some(idx_str) = other.strip_prefix('+') else {
                    return Err(TemplateError::UnknownPlaceholder(other.to_string()));
                };
                let Ok(n) = idx_str.parse::<usize>() else {
                    return Err(TemplateError::UnknownPlaceholder(other.to_string()));
                };
                if n == 0 || n > captures.pluses.len() {
                    return Err(TemplateError::OutOfRange(n, captures.pluses.len()));
                }
                out.push_str(&captures.pluses[n - 1]);
            }
        }
        // Advance past the closing '}'.
        for _ in 0..=end_rel {
            iter.next();
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_filter_matches_exact_topic() {
        let m = TopicMatcher::new("home/kitchen/temp");
        let c = m.captures("home/kitchen/temp").unwrap();
        assert!(c.pluses.is_empty());
        assert!(c.hash.is_none());
        assert!(m.captures("home/kitchen/humidity").is_none());
    }

    #[test]
    fn plus_captures_one_level() {
        let m = TopicMatcher::new("home/+/temp");
        let c = m.captures("home/kitchen/temp").unwrap();
        assert_eq!(c.pluses, vec!["kitchen".to_string()]);
        // Does not match additional levels.
        assert!(m.captures("home/kitchen/sub/temp").is_none());
    }

    #[test]
    fn hash_captures_rest() {
        let m = TopicMatcher::new("home/#");
        let c = m.captures("home/kitchen/temp/x").unwrap();
        assert_eq!(c.hash.as_deref(), Some("kitchen/temp/x"));
    }

    #[test]
    fn template_substitutes_plus_captures() {
        let captures = TopicCaptures {
            pluses: vec!["kitchen".into(), "fridge".into()],
            hash: None,
        };
        let s = render("temp.{+1}.{+2}", &captures).unwrap();
        assert_eq!(s, "temp.kitchen.fridge");
    }

    #[test]
    fn template_substitutes_hash() {
        let captures = TopicCaptures {
            pluses: vec![],
            hash: Some("kitchen/temp".into()),
        };
        let s = render("home.{#}", &captures).unwrap();
        assert_eq!(s, "home.kitchen/temp");
    }

    #[test]
    fn template_out_of_range_errors() {
        let captures = TopicCaptures {
            pluses: vec!["only".into()],
            hash: None,
        };
        let err = render("temp.{+2}", &captures).unwrap_err();
        assert!(matches!(err, TemplateError::OutOfRange(2, 1)));
    }

    #[test]
    fn template_unknown_placeholder_errors() {
        let captures = TopicCaptures::default();
        let err = render("x.{foo}", &captures).unwrap_err();
        assert!(matches!(err, TemplateError::UnknownPlaceholder(_)));
    }

    #[test]
    fn template_passes_literals_through() {
        let captures = TopicCaptures {
            pluses: vec!["a".into()],
            hash: None,
        };
        let s = render("source.{+1}.suffix", &captures).unwrap();
        assert_eq!(s, "source.a.suffix");
    }
}
