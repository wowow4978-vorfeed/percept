use std::fmt;
use std::str::FromStr;

use crate::error::Error;

/// A reference to a `(kind, version)` pair, as accepted in producer-supplied
/// `kind` fields. `version = None` means "latest registered version".
///
/// Wire form: `"name"` or `"name@vN"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KindRef {
    pub name: String,
    pub version: Option<String>,
}

impl KindRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
        }
    }

    pub fn with_version(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: Some(version.into()),
        }
    }
}

impl FromStr for KindRef {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidKindRef("empty string".into()));
        }
        match s.split_once('@') {
            None => Ok(Self {
                name: s.to_string(),
                version: None,
            }),
            Some((name, version)) => {
                if name.is_empty() {
                    return Err(Error::InvalidKindRef(format!("missing name in {s:?}")));
                }
                if version.is_empty() {
                    return Err(Error::InvalidKindRef(format!("missing version in {s:?}")));
                }
                Ok(Self {
                    name: name.to_string(),
                    version: Some(version.to_string()),
                })
            }
        }
    }
}

impl fmt::Display for KindRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.version {
            None => f.write_str(&self.name),
            Some(v) => write!(f, "{}@{}", self.name, v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_name() {
        let k: KindRef = "object_detected".parse().unwrap();
        assert_eq!(k.name, "object_detected");
        assert_eq!(k.version, None);
        assert_eq!(k.to_string(), "object_detected");
    }

    #[test]
    fn parses_versioned() {
        let k: KindRef = "object_detected@v2".parse().unwrap();
        assert_eq!(k.name, "object_detected");
        assert_eq!(k.version.as_deref(), Some("v2"));
        assert_eq!(k.to_string(), "object_detected@v2");
    }

    #[test]
    fn rejects_empty() {
        assert!("".parse::<KindRef>().is_err());
    }

    #[test]
    fn rejects_missing_name() {
        assert!("@v2".parse::<KindRef>().is_err());
    }

    #[test]
    fn rejects_missing_version() {
        assert!("name@".parse::<KindRef>().is_err());
    }
}
