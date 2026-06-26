//! Error-message quality and secret redaction contract tests.
//!
//! QA backlog Task 5.4. The `redact_for_artifact` round-trip is covered by
//! unit tests in `src/config/tests.rs`.  This file asserts the complementary
//! property: when config parsing or placeholder resolution *fails*, the
//! plaintext password must not surface in the resulting `anyhow::Error`
//! Display output.
//!
//! These checks protect CLI output, logs, and Slack/webhook payloads that
//! render `error_message` verbatim.

use rivet::config::{Config, resolve_vars};

/// Magic marker string used as a stand-in for a real secret.  Any test that
/// fails because this marker appears in an error message indicates a leak.
const SECRET_MARKER: &str = "TOP_SECRET_DO_NOT_LEAK_42";

#[test]
fn misplaced_tuning_error_does_not_leak_plaintext_password_in_url() {
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "postgresql://user:{SECRET_MARKER}@host:5432/db"
  batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#
    );
    let err = Config::from_yaml(&yaml).expect_err("misplaced batch_size must be rejected");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains(SECRET_MARKER),
        "error message must not leak URL-embedded password: {msg}"
    );
}

#[test]
fn validation_error_does_not_leak_plaintext_password_field() {
    let yaml = format!(
        r#"
source:
  type: postgres
  host: db.example.com
  user: rivet
  password: "{SECRET_MARKER}"
  password_env: DB_PASS
  database: prod
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#
    );
    let err = Config::from_yaml(&yaml).expect_err("password + password_env must be rejected");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains(SECRET_MARKER),
        "validation error must not echo plaintext password: {msg}"
    );
}

#[test]
fn missing_env_var_error_names_var_but_does_not_leak_surrounding_context() {
    // `resolve_vars` is called by `Config::load_with_params` before the YAML
    // parser sees any input.  A missing env var must be named (actionable)
    // but the resolver must not echo the full surrounding context — a secret
    // downstream of the placeholder would leak.
    unsafe {
        std::env::remove_var("RIVET_QA_MISSING_VAR_5_4");
    }
    let input =
        format!("url: postgresql://user:${{RIVET_QA_MISSING_VAR_5_4}}@host/db_{SECRET_MARKER}");
    let err = resolve_vars(&input, None).expect_err("missing env var must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("RIVET_QA_MISSING_VAR_5_4"),
        "error must name the missing variable (actionable): {msg}"
    );
    assert!(
        !msg.contains(SECRET_MARKER),
        "error must not leak surrounding URL/db name: {msg}"
    );
}

#[test]
fn validation_error_for_exports_is_actionable() {
    // When validation fails for an export, the message must mention the
    // export name (operators run configs with dozens of exports).
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: critical_orders_export
    query: "SELECT * FROM orders"
    mode: incremental
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).expect_err("incremental without cursor_column is invalid");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("critical_orders_export"),
        "validation error must mention the failing export name: {msg}"
    );
    assert!(
        msg.contains("cursor_column"),
        "validation error must point at the missing field: {msg}"
    );
}
