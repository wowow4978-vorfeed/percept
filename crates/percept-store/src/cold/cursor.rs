//! Opaque cursor for `get_window` / `get_current_state` pagination.
//!
//! Encoded as `base64url(CBOR(payload))`. The payload carries:
//!
//! - an `anchor` = `(ts_ms_utc, event_id)` of the last returned row, so
//!   resumption is `(ts, event_id) > anchor` in stable order;
//! - a `filter_hash` (BLAKE3-128) over the canonicalized filter set, so
//!   reusing a cursor with a different filter is rejected with
//!   `cursor_filter_mismatch`.
//!
//! No server secret is needed — tampering only lets a caller scan
//! a different partition of their own data, which they could request
//! directly. The hash is integrity-checked, not authenticated.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    pub ts_ms_utc: i64,
    pub event_id: Ulid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CursorPayload {
    /// Logical partition key (slice 3: always empty — single store).
    /// Reserved so the encoded shape doesn't change when partition
    /// pruning lands in slice 5.
    p: String,
    /// `(ts_ms_utc, event_id)` of the last returned row.
    a: Anchor,
    /// BLAKE3-128 of the canonicalized filter set.
    h: [u8; 16],
}

#[derive(Debug, Clone)]
pub struct Cursor {
    pub anchor: Anchor,
    pub filter_hash: [u8; 16],
}

impl Cursor {
    /// Encode for the wire. Opaque to the LLM.
    #[must_use]
    pub fn encode(&self) -> String {
        let payload = CursorPayload {
            p: String::new(),
            a: self.anchor.clone(),
            h: self.filter_hash,
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&payload, &mut buf).expect("cbor encode");
        URL_SAFE_NO_PAD.encode(buf)
    }

    /// Decode and verify the filter hash matches `expected`. Returns
    /// `Err(CursorError::FilterMismatch)` when the caller resumed with a
    /// different filter from the originating query.
    pub fn decode(s: &str, expected_hash: &[u8; 16]) -> Result<Self, CursorError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(s.as_bytes())
            .map_err(|_| CursorError::Malformed)?;
        let payload: CursorPayload =
            ciborium::from_reader(bytes.as_slice()).map_err(|_| CursorError::Malformed)?;
        if &payload.h != expected_hash {
            return Err(CursorError::FilterMismatch);
        }
        Ok(Self {
            anchor: payload.a,
            filter_hash: payload.h,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("cursor is malformed")]
    Malformed,
    #[error("cursor_filter_mismatch")]
    FilterMismatch,
}

/// Compute the canonical filter hash. Inputs are sorted before hashing so
/// equivalent filter sets produce the same hash regardless of input order.
#[must_use]
pub fn filter_hash(
    start_ms: i64,
    end_ms: i64,
    source_filter: Option<&[String]>,
    kind_filter: Option<&[String]>,
    limit: u32,
) -> [u8; 16] {
    let mut h = blake3::Hasher::new();
    h.update(b"percept-cursor-v1");
    h.update(&start_ms.to_le_bytes());
    h.update(&end_ms.to_le_bytes());
    h.update(&limit.to_le_bytes());

    let mut srcs = source_filter.unwrap_or(&[]).to_vec();
    srcs.sort();
    h.update(&u32::try_from(srcs.len()).unwrap_or(u32::MAX).to_le_bytes());
    for s in &srcs {
        h.update(&u32::try_from(s.len()).unwrap_or(u32::MAX).to_le_bytes());
        h.update(s.as_bytes());
    }
    let mut kinds = kind_filter.unwrap_or(&[]).to_vec();
    kinds.sort();
    h.update(&u32::try_from(kinds.len()).unwrap_or(u32::MAX).to_le_bytes());
    for k in &kinds {
        h.update(&u32::try_from(k.len()).unwrap_or(u32::MAX).to_le_bytes());
        h.update(k.as_bytes());
    }

    let full = h.finalize();
    let bytes = full.as_bytes();
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes[..16]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_preserves_anchor_and_hash() {
        let hash = filter_hash(0, 1_000, Some(&["a".into()]), None, 10);
        let c = Cursor {
            anchor: Anchor {
                ts_ms_utc: 42,
                event_id: Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            },
            filter_hash: hash,
        };
        let encoded = c.encode();
        let decoded = Cursor::decode(&encoded, &hash).unwrap();
        assert_eq!(decoded.anchor, c.anchor);
        assert_eq!(decoded.filter_hash, hash);
    }

    #[test]
    fn filter_hash_is_order_independent() {
        let h1 = filter_hash(0, 100, Some(&["a".into(), "b".into()]), None, 10);
        let h2 = filter_hash(0, 100, Some(&["b".into(), "a".into()]), None, 10);
        assert_eq!(h1, h2);
    }

    #[test]
    fn filter_hash_differs_for_different_filters() {
        let h1 = filter_hash(0, 100, Some(&["a".into()]), None, 10);
        let h2 = filter_hash(0, 100, Some(&["b".into()]), None, 10);
        assert_ne!(h1, h2);
    }

    #[test]
    fn filter_hash_differs_for_different_time_range() {
        let h1 = filter_hash(0, 100, None, None, 10);
        let h2 = filter_hash(0, 200, None, None, 10);
        assert_ne!(h1, h2);
    }

    #[test]
    fn decode_with_wrong_hash_rejects() {
        let h = filter_hash(0, 100, None, None, 10);
        let c = Cursor {
            anchor: Anchor {
                ts_ms_utc: 0,
                event_id: Ulid::new(),
            },
            filter_hash: h,
        };
        let encoded = c.encode();
        let other = filter_hash(0, 200, None, None, 10);
        let err = Cursor::decode(&encoded, &other).unwrap_err();
        assert!(matches!(err, CursorError::FilterMismatch));
    }

    #[test]
    fn decode_with_garbage_rejects() {
        let h = filter_hash(0, 100, None, None, 10);
        let err = Cursor::decode("not-base64!", &h).unwrap_err();
        assert!(matches!(err, CursorError::Malformed));
    }
}
