//! Gremlin tests — pipeline-level fault injection and edge-case gates.
//!
//! Covers scenarios that sit between unit-level mock tests (`sink.rs`) and
//! the crash-recovery matrix (`live_crash_recovery.rs`):
//!
//! | ID | Scenario                                                                  |
//! |----|---------------------------------------------------------------------------|
//! | G1 | `row_count_min` fires on a genuinely empty source table                   |
//! | G2 | `row_count_min` fires when cursor has already consumed all available rows  |
//! | G3 | `unique_max_entries` cap hit emits Warn, not Fail — export still succeeds  |
//! | G4 | `auto_shrink` splits preserve total row count under a `row_count_min` gate |
//! | G5 | Crash at `after_source_read` + quality config: recovery re-evaluates gate  |
//!
//! G1 and G2 are regression tests for the bug where `run_quality_checks()` was
//! called after the `if sink.total_rows == 0 { return Ok(()); }` early return
//! in `pipeline/single.rs`, silently bypassing `row_count_min` when the source
//! returned zero rows.  The fix moved the quality-check call before that return.
//!
//! All tests require the docker-compose Postgres stack:
//!
//! ```bash
//! docker compose up -d postgres
//! cargo test --test gremlin -- --include-ignored
//! ```

mod common;

use common::*;

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Write `yaml` to a fresh tempdir and return `(dir_guard, config_path)`.
/// The `TempDir` must be kept alive for the duration of the test; dropping it
/// deletes the config file and the state DB that rivet writes next to it.
fn make_cfg(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

/// Read `last_cursor_value` for `export` from the state DB next to `cfg_path`.
fn read_cursor(cfg_path: &std::path::Path, export: &str) -> Option<String> {
    let db = cfg_path.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(db).ok()?;
    conn.query_row(
        "SELECT last_cursor_value FROM export_state WHERE export_name = ?1",
        [export],
        |r| r.get::<_, Option<String>>(0),
    )
    .unwrap_or(None)
}

// ─── G1: row_count_min fires on an empty source table ────────────────────────

/// Regression: `row_count_min` must fire even when the source returns 0 rows.
///
/// Before the fix, `run_quality_checks()` was called after the early return for
/// empty exports; the gate was silently bypassed and the export exited with `Ok`.
/// After the fix the call is before the early return.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_row_count_min_fires_on_empty_source_table() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(0); // table created, zero rows inserted
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_empty");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 1
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "row_count_min=1 on empty table must fail the export; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("row_count") || stderr.contains("quality") || stderr.contains("minimum"),
        "error message must mention row count or quality; got:\n{stderr}"
    );
    // No file should have been written (0 rows → early return before write).
    assert!(
        files_with_extension(out.path(), "parquet").is_empty(),
        "empty export must not produce an output file"
    );
}

// ─── G2: row_count_min fires on an exhausted incremental cursor ───────────────

/// Regression: `row_count_min` must fire on an incremental run that returns 0
/// new rows because the cursor is already past the latest row.
///
/// Pattern: first run (no quality gate) consumes all rows and advances the
/// cursor; second run (with `row_count_min: 1`) issues the incremental query and
/// gets an empty result set.  The gate must fire on that second run.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_row_count_min_fires_on_exhausted_incremental_cursor() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10); // 10 rows, all in the past
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_incr");

    // First run: no quality gate.  Consumes all 10 rows, cursor advances to max.
    let yaml_run1 = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, created_at FROM {table_name}"
    mode: incremental
    cursor_column: created_at
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (cfgdir, cfgpath) = make_cfg(&yaml_run1);
    let run1 = run_rivet_export(&cfgpath, &export_name);
    assert!(run1.status.success(), "first run must succeed");
    assert!(
        read_cursor(&cfgpath, &export_name).is_some(),
        "cursor must be set after first run"
    );

    // Second run: identical query + row_count_min=1.  Cursor is already at max →
    // zero new rows → quality gate must fire.
    let yaml_run2 = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, created_at FROM {table_name}"
    mode: incremental
    cursor_column: created_at
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 1
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    // Overwrite the config in the same tempdir so the existing state DB is reused.
    let cfgpath2 = write_config(&cfgdir, &yaml_run2);

    let run2 = run_rivet_export(&cfgpath2, &export_name);
    assert!(
        !run2.status.success(),
        "row_count_min=1 on exhausted cursor must fail the export; stderr:\n{}",
        String::from_utf8_lossy(&run2.stderr)
    );
    let stderr = String::from_utf8_lossy(&run2.stderr);
    assert!(
        stderr.contains("row_count") || stderr.contains("quality") || stderr.contains("minimum"),
        "error must mention row count or quality; got:\n{stderr}"
    );
}

// ─── G3: unique_max_entries cap hit → Warn, not Fail ─────────────────────────

/// When `unique_max_entries` is set below the actual distinct count the
/// uniqueness check is bypassed with a Warn (the hash set filled up before all
/// rows were seen).  The export must still succeed — it is a data-volume signal,
/// not a uniqueness violation.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_unique_max_entries_cap_emits_warn_and_export_succeeds() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(100); // 100 distinct IDs
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_ucap");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      unique_columns: [id]
      unique_max_entries: 10
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    // RUST_LOG=warn surfaces the quality Warn message in stderr.
    let result = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfgpath.to_str().unwrap(),
        "--export",
        &export_name,
    ]);
    assert!(
        result.status.success(),
        "unique cap hit must not fail the export; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("capped") || stderr.contains("uniqueness") || stderr.contains("unique"),
        "cap-hit warning must appear in stderr; got:\n{stderr}"
    );
    // File must still have been produced.
    assert!(
        !files_with_extension(out.path(), "parquet").is_empty(),
        "export with cap-hit warn must produce an output file"
    );
}

// ─── G4: auto_shrink splits preserve total_rows under row_count_min ──────────

/// Wide rows + a tight memory cap trigger `auto_shrink`, which recursively
/// splits each Arrow batch and writes multiple partial files.  The `total_rows`
/// counter in the sink must accumulate all rows across splits so that the
/// `row_count_min` gate sees the true row count — not the count of the last
/// (potentially tiny) split.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_auto_shrink_total_rows_correct_under_quality_gate() {
    require_alive(LiveService::Postgres);
    // 2000 rows × 600-char payload ≈ 1.2 MB Arrow batch — exceeds max_batch_memory_mb: 1.
    let table = seed_pg_wide_table(2000, 0);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_shrink");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, payload FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning:
      max_batch_memory_mb: 1
      on_batch_memory_exceeded: auto_shrink
    quality:
      row_count_min: 2000
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "auto_shrink + row_count_min=2000 on 2000-row table must pass; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        !files_with_extension(out.path(), "parquet").is_empty(),
        "auto_shrink export must produce at least one output file"
    );
}

// ─── G5: crash at after_source_read + quality config → recovery re-evaluates ─

/// Crash at the `after_source_read` fault point happens before quality checks
/// run.  The post-crash state is clean (no file, no manifest entry, no cursor).
/// A recovery run must re-read the source, re-evaluate the quality gate, and
/// succeed — i.e. the gate is not frozen or skipped because an earlier run
/// crashed mid-flight.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_crash_before_quality_check_recovery_re_evaluates_gate() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_qcrash");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, created_at FROM {table_name}"
    mode: incremental
    cursor_column: created_at
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 5
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    // Injected crash: source was read but writer was not finalised and quality
    // checks were not evaluated.  Post-crash state must be completely clean.
    let crash_out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfgpath.to_str().unwrap(),
            "--export",
            &export_name,
        ])
        .env("RIVET_TEST_PANIC_AT", "after_source_read")
        .output()
        .expect("spawn rivet (crash run)");
    assert!(
        !crash_out.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash_out.stderr)
    );

    // Post-crash: no file on disk, no cursor advance.
    assert!(
        files_with_extension(out.path(), "parquet").is_empty(),
        "after_source_read crash must not produce an output file"
    );
    assert_eq!(
        read_cursor(&cfgpath, &export_name),
        None,
        "after_source_read crash must not advance the cursor"
    );

    // Recovery run: no crash injection, quality gate must pass on the 10 rows.
    let rec = run_rivet_export(&cfgpath, &export_name);
    assert!(
        rec.status.success(),
        "recovery run must succeed (quality gate: 10 rows >= row_count_min 5); stderr:\n{}",
        String::from_utf8_lossy(&rec.stderr)
    );
    assert!(
        read_cursor(&cfgpath, &export_name).is_some(),
        "cursor must be advanced after recovery run"
    );
    assert_eq!(
        files_with_extension(out.path(), "parquet").len(),
        1,
        "recovery run must produce exactly one output file"
    );
}
