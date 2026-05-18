//! MQTT-payload decoders. DESIGN §12.4 enumerates four:
//! `json` / `raw` / `hex` / `csv`. The producer chooses one per
//! subscription; the decoder takes raw bytes and returns the
//! `semantic` Value the normalizer ships downstream.

use serde_json::{json, Value};

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    Json,
    Raw,
    Hex,
    Csv,
}

impl PayloadFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "json" => Some(Self::Json),
            "raw" => Some(Self::Raw),
            "hex" => Some(Self::Hex),
            "csv" => Some(Self::Csv),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Raw => "raw",
            Self::Hex => "hex",
            Self::Csv => "csv",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid JSON payload: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid UTF-8 in payload: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

/// Decode `bytes` according to `format` into the `semantic` JSON shape
/// the normalizer ships downstream.
///
/// Shapes:
/// - `json`  → whatever the payload parses to
/// - `raw`   → `{"encoding": "raw", "bytes_base64": "..."}`
/// - `hex`   → `{"encoding": "hex", "hex": "..."}`
/// - `csv`   → `{"encoding": "csv", "fields": [...]}`
pub fn decode(format: PayloadFormat, bytes: &[u8]) -> Result<Value, DecodeError> {
    match format {
        PayloadFormat::Json => Ok(serde_json::from_slice(bytes)?),
        PayloadFormat::Raw => Ok(json!({
            "encoding": "raw",
            "bytes_base64": STANDARD_NO_PAD.encode(bytes),
        })),
        PayloadFormat::Hex => {
            let mut s = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                use std::fmt::Write;
                let _ = write!(s, "{b:02x}");
            }
            Ok(json!({ "encoding": "hex", "hex": s }))
        }
        PayloadFormat::Csv => {
            let text = std::str::from_utf8(bytes)?;
            // Single-row CSV per message: split on the first newline, then
            // by comma. Quoted strings supported (one level, no escape).
            let row = text.lines().next().unwrap_or("");
            let fields = parse_csv_row(row);
            Ok(json!({ "encoding": "csv", "fields": fields }))
        }
    }
}

fn parse_csv_row(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if !in_quotes && cur.is_empty() => in_quotes = true,
            '"' if in_quotes => {
                if matches!(chars.peek(), Some('"')) {
                    // Escaped double-quote.
                    chars.next();
                    cur.push('"');
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_passes_through() {
        let v = decode(PayloadFormat::Json, br#"{"k": 1}"#).unwrap();
        assert_eq!(v["k"], 1);
    }

    #[test]
    fn raw_emits_base64() {
        let v = decode(PayloadFormat::Raw, &[0xde, 0xad, 0xbe, 0xef]).unwrap();
        assert_eq!(v["encoding"], "raw");
        assert!(v["bytes_base64"].is_string());
    }

    #[test]
    fn hex_emits_lowercase_hex() {
        let v = decode(PayloadFormat::Hex, &[0xab, 0xcd, 0x01]).unwrap();
        assert_eq!(v["encoding"], "hex");
        assert_eq!(v["hex"], "abcd01");
    }

    #[test]
    fn csv_splits_on_comma() {
        let v = decode(PayloadFormat::Csv, b"a,b,c").unwrap();
        assert_eq!(
            v["fields"],
            serde_json::json!(["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn csv_respects_quoted_field_with_comma() {
        let v = decode(PayloadFormat::Csv, br#""a,b",c"#).unwrap();
        assert_eq!(v["fields"][0], "a,b");
        assert_eq!(v["fields"][1], "c");
    }

    #[test]
    fn csv_escaped_quote() {
        let v = decode(PayloadFormat::Csv, br#""a""b",c"#).unwrap();
        assert_eq!(v["fields"][0], r#"a"b"#);
    }

    #[test]
    fn malformed_json_errors() {
        let r = decode(PayloadFormat::Json, b"{");
        assert!(r.is_err());
    }
}
