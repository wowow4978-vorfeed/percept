//! End-to-end tests for `percept::config::load`.
//!
//! Each test writes a real TOML file (and optionally a `conf.d/` directory)
//! to a tempdir, then calls `load` and asserts on the result. Tempdirs are
//! cleaned up by Drop on `tempfile::TempDir`.

use std::fs;
use std::path::Path;

use percept::config;

fn write(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&p, body).unwrap();
    p
}

fn td() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[test]
fn minimal_config_loads() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [server]
            data_dir = "/var/lib/percept"
        "#,
    );
    let cfg = config::load(&path).unwrap();
    assert_eq!(cfg.server.as_ref().unwrap().profile, "edge");
}

#[test]
fn rejects_unknown_top_level_key() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [server]
            data_dir = "/x"

            [bogus]
            anything = 1
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(
        err.contains("bogus") || err.contains("unknown field"),
        "got: {err}"
    );
}

#[test]
fn rejects_inline_password() {
    let dir = td();
    // `password` (bare) is not a field of MqttCredentials — only
    // `password_env` and `password_file` are. deny_unknown_fields catches it.
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [[mqtt]]
            id = "b"
            url = "mqtts://x:8883"
            credentials = { user = "u", password = "leaked" }
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(err.to_lowercase().contains("password"), "got: {err}");
}

#[test]
fn rejects_inline_token_on_http_token() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [[http_token]]
            name = "x"
            token = "leaked"
            allow_source_ids = ["a"]
            allow_kinds = ["b"]
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("token") || err.contains("unknown"),
        "got: {err}"
    );
}

#[test]
fn rejects_unresolvable_env() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [mcp]
            listen = "0.0.0.0:7878"
            auth = { token_env = "PERCEPT_NONEXISTENT_TOKEN_VAR_XYZ" }
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(
        err.contains("PERCEPT_NONEXISTENT_TOKEN_VAR_XYZ"),
        "got: {err}"
    );
}

#[test]
fn rejects_both_env_and_file() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [mcp]
            listen = "0.0.0.0:7878"
            auth = { token_env = "X", token_file = "/x" }
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(err.to_lowercase().contains("both"), "got: {err}");
}

#[test]
fn rejects_neither_env_nor_file() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [mcp]
            listen = "0.0.0.0:7878"
            auth = {}
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(err.to_lowercase().contains("neither"), "got: {err}");
}

#[test]
fn rejects_duplicate_source_id() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [[source]]
            id = "cam.front"
            kinds = ["object_detected"]

            [[source]]
            id = "cam.front"
            kinds = ["scene_description"]
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(
        err.contains("duplicate") && err.contains("cam.front"),
        "got: {err}"
    );
}

#[test]
fn rejects_vector_max_age_greater_than_max_age() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [[retention]]
            match.kind = "temperature"
            max_age = "1d"
            vector_max_age = "30d"
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(err.contains("vector_max_age"), "got: {err}");
}

#[test]
fn rejects_non_edge_profile() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [server]
            data_dir = "/x"
            profile = "server"
        "#,
    );
    let err = config::load(&path).unwrap_err().to_string();
    assert!(err.contains("edge"), "got: {err}");
}

#[test]
fn confd_overlay_merges_later_wins_for_scalar() {
    let dir = td();
    let primary = write(
        dir.path(),
        "percept.toml",
        r#"
            [server]
            data_dir = "/old"
        "#,
    );
    write(
        dir.path(),
        "percept.toml.d/10-override.toml",
        r#"
            [server]
            data_dir = "/new"
        "#,
    );
    let cfg = config::load(&primary).unwrap();
    assert_eq!(cfg.server.as_ref().unwrap().data_dir, "/new");
}

#[test]
fn confd_array_entries_accumulate() {
    let dir = td();
    let primary = write(
        dir.path(),
        "percept.toml",
        r#"
            [[source]]
            id = "a"
            kinds = ["k"]
        "#,
    );
    write(
        dir.path(),
        "percept.toml.d/10-extra.toml",
        r#"
            [[source]]
            id = "b"
            kinds = ["k"]
        "#,
    );
    let cfg = config::load(&primary).unwrap();
    assert_eq!(cfg.sources.len(), 2);
    assert_eq!(cfg.sources[0].id, "a");
    assert_eq!(cfg.sources[1].id, "b");
}

#[test]
fn confd_files_load_in_sorted_order() {
    let dir = td();
    let primary = write(
        dir.path(),
        "percept.toml",
        r#"
            [server]
            data_dir = "/0"
        "#,
    );
    // Write out-of-order; loader must sort by filename.
    write(
        dir.path(),
        "percept.toml.d/20-b.toml",
        r#"[server]
data_dir = "/20""#,
    );
    write(
        dir.path(),
        "percept.toml.d/10-a.toml",
        r#"[server]
data_dir = "/10""#,
    );
    let cfg = config::load(&primary).unwrap();
    assert_eq!(cfg.server.as_ref().unwrap().data_dir, "/20");
}

#[test]
fn resolves_descriptors_falls_back_to_synthetic_kind() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [[source]]
            id = "cam.front"
            kinds = ["object_detected"]
            description = "porch cam"
            usage = "front door"
            caveats = "dusk false positives"
        "#,
    );
    let cfg = config::load(&path).unwrap();
    let resolved = config::resolve_descriptors(&cfg);
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].source_id, "cam.front");
    assert_eq!(resolved[0].kind, "object_detected");
    assert_eq!(resolved[0].kind_version, "v1");
    assert_eq!(resolved[0].description, "porch cam");
}

#[test]
fn resolves_descriptors_uses_kind_when_source_fields_empty() {
    let dir = td();
    let path = write(
        dir.path(),
        "percept.toml",
        r#"
            [[kind]]
            name = "temperature"
            description = "ambient temp"
            usage = "cold/hot questions"
            caveats = "fahrenheit sometimes"

            [[source]]
            id = "therm.kitchen"
            kinds = ["temperature"]
        "#,
    );
    let cfg = config::load(&path).unwrap();
    let resolved = config::resolve_descriptors(&cfg);
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].description, "ambient temp");
    assert_eq!(resolved[0].usage, "cold/hot questions");
}
