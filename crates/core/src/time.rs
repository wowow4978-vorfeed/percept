use std::time::{SystemTime, UNIX_EPOCH};

/// Current UTC time as milliseconds since the Unix epoch.
///
/// Panics if the system clock is before 1970, which is not a recoverable
/// condition for an event store.
#[must_use]
pub fn now_ms_utc() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970");
    i64::try_from(dur.as_millis()).expect("epoch ms overflows i64 (year ~292M AD)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_after_2024() {
        // 2024-01-01 UTC
        assert!(now_ms_utc() > 1_704_067_200_000);
    }
}
