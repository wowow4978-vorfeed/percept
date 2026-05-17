use ulid::Ulid;

/// Generate a fresh ULID. Distinct from a generic constructor name so the
/// intent ("a new event id") is visible at the call site.
#[must_use]
pub fn new_event_id() -> Ulid {
    Ulid::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ids_are_distinct_and_sortable() {
        let a = new_event_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = new_event_id();
        assert_ne!(a, b);
        assert!(a < b, "ULIDs should sort by encoded time");
    }
}
