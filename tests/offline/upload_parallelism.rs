//! Offline guard for the `upload_parallelism` export key (the "New config keys
//! carry tests" gate wants a test that NAMES the key and exercises an
//! interaction — not just an inline `#[cfg(test)]` in the source).
//!
//! Parsing + business-rule validation are both offline (`Config::from_yaml`),
//! so this runs in CI without a database or network. It pins the config-level
//! surface of the knob; the concurrent-upload behaviour half is unit-tested in
//! `pipeline::single::tests` and covered live in `tests/live/`.

#[test]
fn upload_parallelism_resolves_through_the_public_config_surface() {
    // The key is a legal, known export key (a config that sets it parses and
    // validates), and the resolved value is reachable through the public
    // `Config` -> export surface.
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
      type: local
      path: /tmp/out
    upload_parallelism: 4
"#;
    let cfg = rivet::config::Config::from_yaml(yaml).expect("config with the key must parse");
    assert_eq!(
        cfg.exports[0].upload_parallelism, 4,
        "the configured parallelism must be the resolved value"
    );
}

#[test]
fn upload_parallelism_defaults_to_sequential() {
    // A legacy config without the key keeps the historical behaviour: the
    // field resolves to the default of 1 (the strictly-sequential upload loop).
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
      type: local
      path: /tmp/out
"#;
    let cfg = rivet::config::Config::from_yaml(bare).expect("legacy config must parse");
    assert_eq!(
        cfg.exports[0].upload_parallelism, 1,
        "omitting the key must keep the sequential default"
    );
}

#[test]
fn upload_parallelism_rejects_a_negative_value() {
    // The field is `usize` (a concurrency count is not a signable quantity),
    // so a negative value must fail to parse rather than wrap or be silently
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
      type: local
      path: /tmp/out
    upload_parallelism: -1
"#;
    let err = rivet::config::Config::from_yaml(yaml)
        .expect_err("a negative parallelism must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("upload_parallelism"),
        "the error must name the offending key, got: {msg}"
    );
}
