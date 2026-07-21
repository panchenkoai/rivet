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

// ─── round-2 audit #14: chunk knobs are chunked-only ─────────────────────
// A config that sets a chunk knob but omits `mode: chunked` silently degrades
// to a single unbounded snapshot (the source-pressure footgun chunked mode
// exists to prevent). Gate it at config-load, like chunk_dense/chunk_by_days.
// RED before the validate_export guard: these all passed validation.

#[test]
fn chunk_column_without_chunked_mode_rejected() {
    // `mode` omitted ⇒ defaults to Full; chunk_column is then silently dropped.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    table: events
    chunk_column: id
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("chunk_column"), "names the knob: {msg}");
    assert!(msg.contains("mode: chunked"), "points at the fix: {msg}");
}

#[test]
fn nondefault_chunk_size_without_chunked_mode_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: full
    table: events
    chunk_size: 5000
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("chunk_size"), "names the knob: {msg}");
    assert!(msg.contains("mode: chunked"), "points at the fix: {msg}");
}

#[test]
fn chunk_knobs_accepted_under_chunked_mode() {
    // The guard must NOT false-positive: the exact same knobs are valid when
    // mode IS chunked, and the default chunk_size under a non-chunked mode is
    // fine (it is indistinguishable from unset and harmless).
    let chunked = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: chunked
    table: events
    chunk_column: id
    chunk_size: 5000
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    assert!(
        Config::from_yaml(chunked).is_ok(),
        "chunk knobs must be accepted under mode: chunked"
    );

    let default_size_full = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: full
    table: events
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    assert!(
        Config::from_yaml(default_size_full).is_ok(),
        "a full export that never touches chunk knobs must stay valid"
    );
}

// ─── round-2 audit #5/#6/#15/#16/#17: config-load hoists ─────────────────

#[test]
fn cdc_tables_entry_with_traversal_rejected_at_load() {
    // #5: a `tables:` entry becomes a destination path segment — `..` escapes
    // the configured tree. RED before the is_filename_safe_name gate on tables.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: leak
    mode: cdc
    tables: ["../../evil"]
    format: parquet
    cdc:
      slot: s
    destination:
      type: local
      path: ./out
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("filename-safe"), "names the class: {msg}");
}

#[test]
fn partition_by_with_cdc_mode_rejected_at_load() {
    // #15: partition_by + mode:cdc previously passed `check`, then died at run
    // after a live probe with a misleading "requires table:". Reject statically.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: cdc
    table: events
    partition_by: created_at
    format: parquet
    cdc:
      slot: s
    destination:
      type: local
      path: ./out
      prefix: "p/{partition}/"
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("partition_by"), "names the knob: {msg}");
    assert!(
        msg.to_lowercase().contains("cdc"),
        "names the incompatible mode: {msg}"
    );
}

#[test]
fn partition_by_column_name_traversal_rejected_at_load() {
    // #6: the partition column name becomes the Hive `col=value` segment.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: full
    table: events
    partition_by: "../x"
    format: parquet
    destination:
      type: local
      path: ./out
      prefix: "p/{partition}/"
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("filename-safe"), "names the class: {msg}");
}

#[test]
fn partition_by_without_token_rejected_at_check_time() {
    // #16: the {partition}-token rule was enforced only in the run pipeline —
    // `rivet check` gave a false green. Now caught at config-load.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: full
    table: events
    partition_by: created_at
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(
        msg.contains("{partition}"),
        "names the missing token: {msg}"
    );
}

#[test]
fn chunk_size_and_memory_mb_both_set_rejected() {
    // #17: documented mutually exclusive, previously unenforced — chunk_size
    // was silently dropped in favour of the memory budget.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    mode: chunked
    table: events
    chunk_column: id
    chunk_size: 5000
    chunk_size_memory_mb: 256
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("chunk_size"), "names the knob: {msg}");
    assert!(
        msg.contains("mutually exclusive"),
        "explains the rule: {msg}"
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

// ── CDC multi-table `tables:` shape ───────────────────────────────────────────

#[test]
fn cdc_table_and_tables_are_mutually_exclusive() {
    let yaml = r#"
source: { type: postgres, url: "postgresql://localhost/test" }
exports:
  - name: app_cdc
    table: orders
    tables: [orders, users]
    mode: cdc
    format: parquet
    cdc: { slot: s1 }
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("mutually exclusive"), "{msg}");
}

#[test]
fn cdc_tables_must_be_nonempty_and_unique() {
    let empty = r#"
source: { type: postgres, url: "postgresql://localhost/test" }
exports:
  - name: app_cdc
    tables: []
    mode: cdc
    format: parquet
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(empty).unwrap_err());
    assert!(msg.contains("at least one table"), "{msg}");

    let dup = r#"
source: { type: postgres, url: "postgresql://localhost/test" }
exports:
  - name: app_cdc
    tables: [orders, orders]
    mode: cdc
    format: parquet
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(dup).unwrap_err());
    assert!(msg.contains("duplicate table"), "{msg}");
}

#[test]
fn cdc_tables_on_mssql_is_rejected_for_now() {
    let yaml = r#"
source: { type: mssql, url: "sqlserver://sa:x@localhost:1434/rivet" }
exports:
  - name: app_cdc
    tables: [orders, users]
    mode: cdc
    format: parquet
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("not yet supported for SQL Server"), "{msg}");
}

#[test]
fn cdc_tables_multi_table_stream_validates_on_postgres_and_mysql() {
    for source in ["postgres", "mysql"] {
        let yaml = format!(
            r#"
source: {{ type: {source}, url: "{source}://localhost/test" }}
exports:
  - name: app_cdc
    tables: [orders, users]
    mode: cdc
    format: parquet
    cdc: {{ slot: s1, server_id: 5000, checkpoint: /tmp/app.ckpt }}
    destination: {{ type: local, path: ./out }}
"#
        );
        Config::from_yaml(&yaml)
            .unwrap_or_else(|e| panic!("{source} multi-table cdc must validate: {e:#}"));
    }
}

// `validate_non_sql_source_modes` (config/mod.rs) is what makes the SQL-only
// builders' `unreachable!` arms provably unreachable for a Mongo source — but
// it was itself untested (the behaviour-matrix `mode_incremental`/`time_window`
// Mongo `na`s rest entirely on it). A mutant relaxing `matches!(mode, Full|Cdc)`
// would let `mode: incremental` reach a panic; a mutant dropping the parallel+
// resume guard would silently re-read the whole collection every run.
#[test]
fn mongo_non_full_or_cdc_mode_is_rejected() {
    for (mode, extra) in [
        ("incremental", "\n    cursor_column: updated_at"),
        ("chunked", ""),
    ] {
        let yaml = format!(
            r#"
source: {{ type: mongo, url: "mongodb://localhost:27017/testdb" }}
exports:
  - name: t
    table: t
    mode: {mode}{extra}
    format: parquet
    destination: {{ type: local, path: ./out }}
"#
        );
        let msg = format!("{:#}", Config::from_yaml(&yaml).unwrap_err());
        assert!(
            msg.contains("MongoDB has no SQL") || msg.contains("supports `mode: full`"),
            "mongo mode:{mode} must be rejected with the mode-unsupported message, got: {msg}"
        );
    }
}

#[test]
fn mongo_parallel_plus_resume_is_rejected() {
    // The parallel `_id`-range fan-out keeps NO keyset checkpoint, so `resume: true`
    // with `parallel > 1` would be silently ignored (whole collection re-read every
    // run — a bug-hunt find). The guard must bail loud naming the silent-ignore risk.
    let yaml = r#"
source:
  type: mongo
  url: "mongodb://localhost:27017/testdb"
  mongo:
    resume: true
exports:
  - name: t
    table: t
    mode: full
    parallel: 2
    format: parquet
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(
        msg.contains("resume") && msg.contains("parallel") && msg.contains("silently ignored"),
        "mongo parallel+resume must be rejected naming the silent-ignore risk, got: {msg}"
    );
}

#[test]
fn cdc_until_current_defaults_to_true_not_the_streaming_daemon() {
    // Omitting `until_current` must default to TRUE (bounded / scheduler), never
    // the continuous-streaming daemon. A hand-written CDC config that forgot the
    // field used to silently start a never-terminating stream (`#[serde(default)]`
    // = false); the default is now `default_true`.
    let yaml = r#"
source: { type: postgres, url: "postgresql://localhost/test" }
exports:
  - name: t
    table: t
    mode: cdc
    format: parquet
    cdc: { slot: s1 }
    destination: { type: local, path: ./out }
"#;
    let cfg = Config::from_yaml(yaml).expect("valid cdc config");
    assert!(
        cfg.exports[0]
            .cdc
            .as_ref()
            .expect("cdc block")
            .until_current,
        "omitting until_current must default to true (bounded), not the streaming daemon"
    );
}

#[test]
fn cdc_export_config_rust_default_is_bounded_not_a_daemon() {
    // The serde default (test above) is NOT enough: `cdc_job` builds the effective
    // CDC config with `export.cdc.clone().unwrap_or_default()`, so a `mode: cdc`
    // export that omits the whole `cdc:` block (valid for PG/MySQL) reaches the
    // RUST `Default`, not the serde one. A *derived* Default yields
    // `until_current = false` (bool::default) = `DrainMode::Continuous` — a
    // never-terminating daemon on a minimal config. RED before the hand-written
    // `impl Default`; audit finding.
    use crate::config::export::CdcExportConfig;
    use crate::source::cdc::DrainMode;
    let d = CdcExportConfig::default();
    assert!(
        d.until_current,
        "CdcExportConfig::default().until_current must be true (bounded), not the daemon default"
    );
    assert!(
        matches!(
            DrainMode::from_until_current(d.until_current),
            DrainMode::BoundedAtOpen
        ),
        "an omitted cdc: block (unwrap_or_default) must resolve to a bounded drain"
    );
}

#[test]
fn tables_outside_cdc_mode_is_rejected() {
    let yaml = r#"
source: { type: postgres, url: "postgresql://localhost/test" }
exports:
  - name: orders
    tables: [orders, users]
    mode: full
    format: parquet
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("only valid with `mode: cdc`"), "{msg}");
}

// ── Table-qualified `columns:` override keys ─────────────────────────────────
// Bare keys apply to every table of a multi-table export (the blast radius the
// schema-wide user worries about); `"table.column"` targets one table and wins
// over the bare key. A qualified key naming a table the export does NOT
// capture is a config error — a typo must fail at load, never silently miss.

#[test]
fn qualified_override_for_unknown_table_is_rejected() {
    let yaml = r#"
source: { type: mysql, url: "mysql://localhost/test" }
exports:
  - name: app_cdc
    tables: [orders, users]
    mode: cdc
    format: parquet
    columns: { "ghost.v": "decimal(20,0)" }
    cdc: { checkpoint: /tmp/a.ckpt, server_id: 5001 }
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(
        msg.contains("ghost") && msg.contains("does not capture"),
        "a qualified override must name a captured table: {msg}"
    );
}

#[test]
fn qualified_override_on_query_export_is_rejected() {
    let yaml = r#"
source: { type: postgres, url: "postgresql://localhost/test" }
exports:
  - name: orders
    query: "SELECT 1 AS v"
    mode: full
    format: parquet
    columns: { "orders.v": "int8" }
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(
        msg.contains("query"),
        "qualified overrides need a table-shaped export: {msg}"
    );
}

#[test]
fn qualified_override_matching_the_export_table_validates() {
    let yaml = r#"
source: { type: mysql, url: "mysql://localhost/test" }
exports:
  - name: app_cdc
    tables: [orders, users]
    mode: cdc
    format: parquet
    columns: { "orders.v": "decimal(20,0)", v: text }
    cdc: { checkpoint: /tmp/a.ckpt, server_id: 5001 }
    destination: { type: local, path: ./out }
"#;
    Config::from_yaml(yaml).expect("qualified + bare keys must validate");
}

// `initial: snapshot` reserves the `snapshot/` sub-prefix — a captured table
// literally named "snapshot" would collide with it (roast finding).
#[test]
fn initial_snapshot_rejects_a_table_named_snapshot() {
    let yaml = r#"
source: { type: mysql, url: "mysql://localhost/test" }
exports:
  - name: app_cdc
    tables: [orders, snapshot]
    mode: cdc
    format: parquet
    cdc: { initial: snapshot, checkpoint: /tmp/a.ckpt, server_id: 5001 }
    destination: { type: local, path: ./out }
"#;
    let msg = format!("{:#}", Config::from_yaml(yaml).unwrap_err());
    assert!(msg.contains("snapshot"), "must name the collision: {msg}");
}

// Negative family #3 (hostile configs), generative flavour: arbitrary
// bytes/fragments through the whole config parse+validate pipeline must
// yield Ok or a graceful Err — never a panic. serde almost certainly holds;
// the net is for OUR validation layers on top of it.
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        cases: 128, ..Default::default()
    })]

    #[test]
    fn config_parse_is_total_over_arbitrary_yaml(junk in ".{0,300}") {
        let _ = Config::from_yaml(&junk);
    }

    #[test]
    fn config_parse_is_total_over_structured_hostility(
        name in ".{0,30}",
        table in ".{0,30}",
        colkey in ".{0,20}",
        colval in ".{0,20}",
    ) {
        let yaml = format!(
            "source: {{ type: mysql, url: \"mysql://u:p@h/db\" }}\nexports:\n  - name: {name:?}\n    table: {table:?}\n    mode: cdc\n    format: parquet\n    columns: {{ {colkey:?}: {colval:?} }}\n    cdc: {{ checkpoint: /tmp/x, server_id: 5001 }}\n    destination: {{ type: local, path: ./out }}\n"
        );
        let _ = Config::from_yaml(&yaml);
    }
}
