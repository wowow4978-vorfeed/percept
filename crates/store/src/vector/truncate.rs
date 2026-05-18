//! UTF-8-safe byte truncation for embedder input.
//!
//! Per DECISIONS §2: serialize `semantic` to compact JSON, truncate to
//! 2048 bytes on a UTF-8 boundary. Vectors carry a `truncated` flag so
//! search results can surface it.

/// Returns `(prefix, truncated)`. `truncated` is `true` when the original
/// input was longer than `max_bytes`.
#[must_use]
pub fn truncate_utf8(s: &str, max_bytes: usize) -> (&str, bool) {
    if s.len() <= max_bytes {
        return (s, false);
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_cap_returns_input() {
        assert_eq!(truncate_utf8("hello", 100), ("hello", false));
    }

    #[test]
    fn over_cap_truncates_and_flags() {
        let s = "a".repeat(3000);
        let (out, t) = truncate_utf8(&s, 2048);
        assert_eq!(out.len(), 2048);
        assert!(t);
    }

    #[test]
    fn cut_walks_back_to_utf8_boundary() {
        // The 3-byte chars push a cut at byte 5 onto a non-boundary; the
        // truncator must walk back to byte 3.
        let s = "aaa日本";
        // "aaa" = 3 bytes, "日" = 3 bytes, "本" = 3 bytes; total 9.
        // max_bytes = 5 lands inside "日"; expected output is "aaa".
        let (out, t) = truncate_utf8(s, 5);
        assert_eq!(out, "aaa");
        assert!(t);
        // Sanity: the slice is a valid &str (no panic).
        assert_eq!(out.chars().count(), 3);
    }
}
