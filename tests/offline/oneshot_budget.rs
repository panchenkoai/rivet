//! Offline guard for the `oneshot_budget_mb` destination key (PR review: the
//! "New config keys carry tests" gate wants a test that NAMES the key and
//! exercises an interaction — not just an inline `#[cfg(test)]` in the source).
//!
//! Parsing + business-rule validation are both offline (`Config::from_yaml`),
//! so this runs in CI without a database or network. It pins the config-level
//! surface of the knob; the memory-behaviour half (shared process-wide pool vs
//! a per-destination private pool) is unit-tested in
//! `destination::cloud::tests` and exercised live in `tests/live/`.

#[test]
fn oneshot_budget_mb_resolves_through_the_public_config_surface() {
    // The key is a legal, known destination key (a config that sets it parses
    // and validates), and the resolved value is reachable through the public
    // `Config` -> export -> destination surface.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://u:p@localhost/db"
exports:
  - name: t
    query: "SELECT 1"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: b
      oneshot_budget_mb: 128
"#;
    let cfg = rivet::config::Config::from_yaml(yaml).expect("config with the key must parse");
    assert_eq!(
        cfg.exports[0].destination.oneshot_budget_mb,
        Some(128),
        "the configured megabytes must be the resolved value"
    );

    // Second-run: parsing the same config again resolves identically (the
    // field is deterministic across loads, not stateful).
    let again = rivet::config::Config::from_yaml(yaml).expect("re-load must parse");
    assert_eq!(
        again.exports[0].destination.oneshot_budget_mb,
        Some(128),
        "re-loading the config must not change the resolved budget"
    );
}

#[test]
fn oneshot_budget_mb_omitted_defaults_to_none_and_zero_is_legal() {
    // Empty case: a config without the key keeps the historical default — the
    // field resolves to `None` (which the loader maps to the shared process-wide
    // pool; see cloud.rs). `0` is a legal explicit value: it disables one-shot
    // uploads, so a destination must parse with it rather than reject it.
    let bare = r#"
source:
  type: postgres
  url: "postgresql://u:p@localhost/db"
exports:
  - name: t
    query: "SELECT 1"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: b
"#;
    let cfg = rivet::config::Config::from_yaml(bare).expect("legacy config must parse");
    assert_eq!(
        cfg.exports[0].destination.oneshot_budget_mb, None,
        "omitting the key must leave the default (shared process-wide pool) intact"
    );

    let zero = bare.replace(
        "      type: s3\n      bucket: b\n",
        "      type: s3\n      bucket: b\n      oneshot_budget_mb: 0\n",
    );
    let cfg = rivet::config::Config::from_yaml(&zero).expect("0 is a valid disable value");
    assert_eq!(cfg.exports[0].destination.oneshot_budget_mb, Some(0));
}

#[test]
fn oneshot_budget_mb_rejects_a_negative_value() {
    // Crash case: the field is `u64` (memory is not a signable quantity), so a
    // negative budget must fail to parse rather than wrap or be silently
    // accepted.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://u:p@localhost/db"
exports:
  - name: t
    query: "SELECT 1"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: b
      oneshot_budget_mb: -1
"#;
    let err =
        rivet::config::Config::from_yaml(yaml).expect_err("a negative MB budget must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("oneshot_budget_mb"),
        "the error must name the offending key, got: {msg}"
    );
}
