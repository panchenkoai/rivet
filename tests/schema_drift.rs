//! Drift guard for `schemas/rivet.schema.json` (v0.7.3 P0.1).
//!
//! The checked-in schema artifact MUST equal the value the running
//! binary's `JsonSchema` derives produce.  When a Config field is added
//! / removed / re-typed without refreshing the artifact, this test
//! fails with a clear "regenerate via `rivet schema config`" hint so
//! the editor-validation experience never silently drifts from the
//! grammar this version accepts.

use rivet::config::generate_config_schema_pretty;

const TRACKED_PATH: &str = "schemas/rivet.schema.json";
const LATEST_PATH: &str = "schemas/latest/rivet.schema.json";

#[test]
fn checked_in_schema_matches_runtime_schema() {
    let generated = generate_config_schema_pretty().expect("schema generation must succeed");
    let on_disk = std::fs::read_to_string(TRACKED_PATH).unwrap_or_else(|e| {
        panic!(
            "{TRACKED_PATH} missing or unreadable ({e}). \
             Run `cargo run --bin rivet -- schema config > {TRACKED_PATH}` to (re)create it.",
        )
    });
    assert_eq!(
        on_disk, generated,
        "\n\n\
         schemas/rivet.schema.json is out of date with the running binary's Config types.\n\
         Regenerate it with:\n\
         \n    cargo run --bin rivet -- schema config > schemas/rivet.schema.json\n    cp schemas/rivet.schema.json schemas/latest/rivet.schema.json\n\
         \nCI will keep failing until both files match.\n",
    );
}

#[test]
fn latest_schema_mirror_matches_tracked_path() {
    // `schemas/latest/rivet.schema.json` is the stable URL CI users
    // pin via `# yaml-language-server: $schema=…/latest/…` — it MUST
    // be byte-identical to the per-version `schemas/rivet.schema.json`.
    let primary = std::fs::read_to_string(TRACKED_PATH)
        .unwrap_or_else(|e| panic!("{TRACKED_PATH} missing or unreadable ({e})."));
    let latest = std::fs::read_to_string(LATEST_PATH).unwrap_or_else(|e| {
        panic!(
            "{LATEST_PATH} missing or unreadable ({e}). \
             Run `cp {TRACKED_PATH} {LATEST_PATH}` to re-mirror it.",
        )
    });
    assert_eq!(
        primary, latest,
        "schemas/latest/ must be a byte-for-byte mirror of {TRACKED_PATH}",
    );
}

#[test]
fn schema_title_carries_current_version() {
    // Cross-check the schema is the one this binary just built — a
    // forgotten `cp` to schemas/latest/ from a stale checkout would
    // otherwise sneak through.
    let on_disk = std::fs::read_to_string(TRACKED_PATH).unwrap();
    let expected = format!("\"Rivet config (rivet-cli {})\"", env!("CARGO_PKG_VERSION"));
    assert!(
        on_disk.contains(&expected),
        "{TRACKED_PATH} title must embed CARGO_PKG_VERSION ({}); \
         regenerate after every version bump.",
        env!("CARGO_PKG_VERSION"),
    );
}
