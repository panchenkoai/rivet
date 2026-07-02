//! Validation tests — invalid YAML combinations, error-message shape, query_file checks.

use super::*;

// ─── misplaced tuning field detection ────────────────────────

#[test]
fn misplaced_batch_size_in_source_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("batch_size"),
        "expected batch_size mention: {msg}"
    );
    assert!(
        msg.contains("source.tuning"),
        "expected hint about tuning: {msg}"
    );
}

#[test]
fn misplaced_profile_in_source_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  profile: fast
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("profile"), "expected profile mention: {msg}");
    assert!(msg.contains("source.tuning"), "expected hint: {msg}");
}

#[test]
fn misplaced_multiple_tuning_fields_in_source_all_listed() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  batch_size: 500
  throttle_ms: 100
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("batch_size"), "missing batch_size: {msg}");
    assert!(msg.contains("throttle_ms"), "missing throttle_ms: {msg}");
}

#[test]
fn misplaced_batch_size_in_export_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    query: "SELECT 1"
    format: csv
    batch_size: 2000
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("batch_size"), "expected batch_size: {msg}");
    assert!(msg.contains("orders"), "expected export name: {msg}");
    assert!(msg.contains("exports[].tuning"), "expected hint: {msg}");
}

#[test]
fn correct_tuning_placement_accepted() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tuning:
    profile: fast
    batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
    tuning:
      throttle_ms: 50
"#;
    Config::from_yaml(yaml).unwrap();
}

// =============================================================================
// Invalid config combinations — QA backlog Task 5.1
// =============================================================================

#[test]
fn empty_exports_list_is_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports: []
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("export"),
        "expected error mentioning 'export' for empty exports list: {msg}"
    );
}

#[test]
fn duplicate_export_names_are_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
  - name: orders
    query: "SELECT 2"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("duplicate") || msg.contains("orders"),
        "expected duplicate-name error mentioning 'orders': {msg}"
    );
}

#[test]
fn chunked_with_parallel_zero_is_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT 1"
    mode: chunked
    chunk_column: id
    parallel: 0
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    assert!(
        format!("{err:#}").contains("parallel"),
        "validation must mention 'parallel'"
    );
}

#[test]
fn chunked_with_chunk_size_zero_is_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT 1"
    mode: chunked
    chunk_column: id
    chunk_size: 0
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    assert!(
        format!("{err:#}").contains("chunk_size"),
        "validation must mention 'chunk_size'"
    );
}

// ── v0.7.4: first-user-friendly error messages ───────────────────────
//
// These tests pin the *content* of the error messages a first-time
// operator sees when they make the most common config mistakes.  The
// exact wording is allowed to evolve, but every test asserts at least
// one Hint phrase the README / getting-started docs reference, so
// dropping the hint accidentally trips the test.
//
// Note: the no-source-block-at-all path is covered by
// `no_url_at_all_rejected` above (updated in v0.7.4 to also assert
// the new url_env + DATABASE_URL guidance).

#[test]
fn structured_source_missing_host_includes_actionable_hint() {
    let yaml = r#"
source:
  type: postgres
  user: rivet
  database: rivetdb
exports:
  - name: u
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("host"), "must name 'host': {msg}");
    assert!(
        msg.contains("Hint") && msg.contains("localhost"),
        "must include 'Hint:' with a concrete suggestion (localhost): {msg}"
    );
    assert!(
        msg.contains("url_env") || msg.contains("DATABASE_URL"),
        "must offer the URL-based alternative: {msg}"
    );
}

#[test]
fn url_env_missing_var_lists_export_command_and_alternative() {
    // The most common first-run failure: env var simply not exported.
    // The error must show *the exact shell command* to fix it.
    unsafe { std::env::remove_var("RIVET_DOCTEST_DATABASE_URL_X") };
    let yaml = r#"
source:
  type: postgres
  url_env: RIVET_DOCTEST_DATABASE_URL_X
exports:
  - name: u
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let err = cfg.source.resolve_url().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("RIVET_DOCTEST_DATABASE_URL_X") && msg.contains("not set"),
        "must name the missing var: {msg}"
    );
    assert!(
        msg.contains("export"),
        "must show the `export VAR=...` shell hint: {msg}"
    );
    assert!(
        msg.contains("postgresql://"),
        "must include a concrete URL example: {msg}"
    );
}

#[test]
fn password_env_missing_var_includes_export_hint() {
    unsafe { std::env::remove_var("RIVET_DOCTEST_PASSWORD_Y") };
    let yaml = r#"
source:
  type: postgres
  host: localhost
  user: rivet
  database: rivetdb
  password_env: RIVET_DOCTEST_PASSWORD_Y
exports:
  - name: u
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let err = cfg.source.resolve_url().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("RIVET_DOCTEST_PASSWORD_Y") && msg.contains("password_env"),
        "must name the missing var + the field that referenced it: {msg}"
    );
    assert!(
        msg.contains("export"),
        "must show the `export VAR=...` shell hint: {msg}"
    );
}

#[test]
fn mixed_url_and_structured_fields_error_explains_choice() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  host: otherhost
  user: u
  database: d
exports:
  - name: u
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Hint") && msg.contains("ambiguous"),
        "must explain why mixing is rejected: {msg}"
    );
}

// ── F-NEW (cfg_matrix Issue 3): query_file syntactic validation at load ──
//
// Before this regression, `query_file: ../../../../etc/passwd` (or any
// absolute path) passed `Config::validate` and `rivet check`, and was only
// rejected at plan-build time when `ExportConfig::resolve_query` was
// called.  Operators running `rivet check` saw rc=0 and assumed the YAML
// was safe.  These tests pin the eager check.

#[test]
fn query_file_traversal_rejected_at_load() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: u
    query_file: ../../../../etc/passwd
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("must not contain '..'") && msg.contains("query_file"),
        "expected traversal-rejection at config-load; got: {msg}"
    );
}

#[test]
fn query_file_absolute_path_rejected_at_load() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: u
    query_file: /etc/passwd
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("must be a relative path") && msg.contains("query_file"),
        "expected absolute-path rejection at config-load; got: {msg}"
    );
}

#[test]
fn query_file_relative_path_accepted_at_load() {
    // Sanity: a sane relative path passes the syntactic check even when
    // the file doesn't exist on disk (the I/O is deferred to plan time).
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: u
    query_file: queries/users.sql
    format: csv
    destination:
      type: local
      path: ./out
"#;
    Config::from_yaml(yaml).expect("relative path must pass validation");
}

#[test]
fn tab_indentation_gets_a_spaces_hint() {
    // A tab in indentation is the classic beginner YAML mistake.
    let yaml = "source:\n  type: postgres\n\tbad: tab\nexports: []\n";
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("TAB") && msg.contains("spaces"),
        "expected a tab→spaces hint, got: {msg}"
    );
}

#[test]
fn missing_source_field_gets_a_remediation_hint() {
    let yaml = "exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: csv\n    destination: {type: local, path: /tmp}\n";
    let err = Config::from_yaml(yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("missing field `source`"), "got: {msg}");
    assert!(
        msg.contains("rivet init"),
        "expected a `rivet init` remediation hint, got: {msg}"
    );
}

// ── CDC resource conflicts (slot / server_id / checkpoint) ────────────────────
//
// RED tests for the sharpest multi-table CDC edge: the per-engine stream
// resources are per-export, and their DEFAULTS collide. Two PostgreSQL cdc
// exports without an explicit `slot:` both resolve to `rivet_slot` — each ack
// advances `confirmed_flush_lsn` past changes the other never read (mutual,
// silent data loss). Two MySQL exports both default to `server_id: 4271` — the
// server kills the older replica connection. A shared `checkpoint:` path makes
// the exports overwrite each other's resume position on any engine.

fn cdc_pair_yaml(source: &str, cdc_a: &str, cdc_b: &str) -> String {
    format!(
        r#"
source:
  type: {source}
  url: "{source}://localhost/test"
exports:
  - name: orders_cdc
    table: orders
    mode: cdc
    format: parquet
    {cdc_a}
    destination:
      type: local
      path: ./out/orders
  - name: users_cdc
    table: users
    mode: cdc
    format: parquet
    {cdc_b}
    destination:
      type: local
      path: ./out/users
"#
    )
}

#[test]
fn two_pg_cdc_exports_defaulting_to_the_same_slot_are_rejected() {
    let yaml = cdc_pair_yaml("postgres", "", "");
    let err = Config::from_yaml(&yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("rivet_slot") && msg.contains("slot"),
        "expected a same-slot conflict naming the defaulted 'rivet_slot': {msg}"
    );
}

#[test]
fn two_cdc_exports_with_the_same_explicit_slot_are_rejected() {
    let yaml = cdc_pair_yaml(
        "postgres",
        "cdc: { slot: shared_slot }",
        "cdc: { slot: shared_slot }",
    );
    let err = Config::from_yaml(&yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("shared_slot"),
        "expected a same-slot conflict naming 'shared_slot': {msg}"
    );
}

#[test]
fn two_mysql_cdc_exports_defaulting_to_the_same_server_id_are_rejected() {
    let yaml = cdc_pair_yaml("mysql", "", "");
    let err = Config::from_yaml(&yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("4271") && msg.contains("server_id"),
        "expected a same-server_id conflict naming the defaulted 4271: {msg}"
    );
}

#[test]
fn two_cdc_exports_sharing_a_checkpoint_path_are_rejected() {
    let yaml = cdc_pair_yaml(
        "postgres",
        "cdc: { slot: slot_a, checkpoint: /var/lib/rivet/same.ckpt }",
        "cdc: { slot: slot_b, checkpoint: /var/lib/rivet/same.ckpt }",
    );
    let err = Config::from_yaml(&yaml).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("same.ckpt") && msg.contains("checkpoint"),
        "expected a shared-checkpoint conflict naming the path: {msg}"
    );
}

#[test]
fn cdc_exports_with_distinct_resources_validate_fine() {
    let yaml = cdc_pair_yaml(
        "postgres",
        "cdc: { slot: slot_orders, checkpoint: /var/lib/rivet/orders.ckpt }",
        "cdc: { slot: slot_users, checkpoint: /var/lib/rivet/users.ckpt }",
    );
    Config::from_yaml(&yaml).expect("distinct slots + checkpoints must validate");
}

#[test]
fn mysql_cdc_exports_with_distinct_server_ids_validate_fine() {
    let yaml = cdc_pair_yaml(
        "mysql",
        "cdc: { server_id: 4271 }",
        "cdc: { server_id: 4272 }",
    );
    Config::from_yaml(&yaml).expect("distinct server_ids must validate");
}
