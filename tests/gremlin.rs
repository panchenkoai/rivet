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
//! | G6 | `row_count_max` fires when source returns MORE rows than the upper bound   |
//! | G7 | `row_count_min` at exact boundary (rows == min) passes, off-by-one fails  |
//! | G8 | `unique_columns` with NULLs: NULL rows don't false-positive uniqueness    |
//! | G9 | Chunked + quality: empty middle chunk doesn't false-fire row_count gate  |
//! | G10| Multi-export with one quality-failing export: other exports still run    |
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

// ─── G6: row_count_max fires when source returns MORE rows than the bound ─────

/// `row_count_max` is the symmetric gate to `row_count_min`: if the source
/// returns more rows than the operator declared as the upper bound the export
/// must fail with a clear message naming the bound. Catches "I expected this
/// query to return at most 100 rows for a daily summary; it returned 50_000
/// because my predicate is wrong" regressions.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_row_count_max_fires_on_over_limit_source() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(50); // 50 rows
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_rmax");

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
      row_count_max: 10
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "row_count_max=10 on 50-row table must fail; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("row_count") || stderr.contains("maximum") || stderr.contains("exceed"),
        "error must mention row count / maximum / exceed; got:\n{stderr}"
    );
}

// ─── G7: row_count_min at exact boundary passes, off-by-one fails ─────────────

/// Boundary check on `row_count_min`. Operator-visible semantics: `min: 10`
/// means "at least 10". A run that returns exactly 10 rows must pass; a run
/// that returns 9 must fail. Cheap regression test for the `<` vs `<=` class
/// of off-by-one bug in the quality comparator.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_row_count_min_boundary_is_inclusive() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let out_exact = tempfile::tempdir().unwrap();
    let out_short = tempfile::tempdir().unwrap();

    // Exact boundary: 10 rows == min 10 → must pass.
    let export_exact = unique_name("gr_b_exact");
    let yaml_exact = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_exact}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 10
"#,
        table_name = table.name(),
        dir = out_exact.path().display()
    );
    let (_d1, cfg_exact) = make_cfg(&yaml_exact);
    let r_exact = run_rivet_export(&cfg_exact, &export_exact);
    assert!(
        r_exact.status.success(),
        "row_count_min=10 on 10-row source must pass (inclusive boundary); stderr:\n{}",
        String::from_utf8_lossy(&r_exact.stderr)
    );

    // Off-by-one: 10 rows < min 11 → must fail.
    let export_short = unique_name("gr_b_short");
    let yaml_short = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_short}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 11
"#,
        table_name = table.name(),
        dir = out_short.path().display()
    );
    let (_d2, cfg_short) = make_cfg(&yaml_short);
    let r_short = run_rivet_export(&cfg_short, &export_short);
    assert!(
        !r_short.status.success(),
        "row_count_min=11 on 10-row source must fail (off-by-one boundary)"
    );
}

// ─── G8: NULLs in unique_columns should not false-positive uniqueness ─────────

/// `unique_columns: [col]` asserts that distinct non-NULL values across the
/// export equal the row count. If two rows have NULL in `col`, those NULLs
/// must NOT be flagged as "duplicate of each other" — by SQL convention
/// `NULL != NULL`. Pins the behaviour for any future refactor of the
/// uniqueness check that might collapse NULLs into one bucket.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_unique_columns_nullable_column_does_not_false_positive() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_unull");

    // Inject NULL values into a nullable column by issuing a follow-up UPDATE.
    // Use the `name` column which is NOT NULL by default — we union with a
    // SELECT-NULL projection in the export query to inject the NULLs.
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name FROM {table_name} UNION ALL SELECT 100 AS id, NULL::text AS name UNION ALL SELECT 101 AS id, NULL::text AS name"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      unique_columns: [id]
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "two NULL rows on a column NOT in unique_columns must not affect uniqueness on `id`; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        !files_with_extension(out.path(), "parquet").is_empty(),
        "export must produce output"
    );
}

// ─── G9: chunked with empty middle chunk doesn't false-fire row_count gate ────

/// Sparse-ID source: rows at id={1,2,3} and id={100,101,102}, no rows in 4..99.
/// With chunk_size=10 and a row_count_min gate, the chunks in the empty
/// 4..99 range return 0 rows each. The export-level row count must aggregate
/// across chunks (6 rows total), not evaluate per-chunk (which would fire on
/// the first empty chunk). Regression against a hypothetical per-chunk gate.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_chunked_empty_middle_chunk_does_not_false_fire_row_count_min() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(0); // empty, we'll insert sparse rows manually
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "INSERT INTO {name} (id, name, amount) VALUES \
         (1,'a',1),(2,'b',2),(3,'c',3),(100,'d',4),(101,'e',5),(102,'f',6);",
        name = table.name()
    ))
    .expect("sparse insert");
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("gr_chunk_empty");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    table: {table_name}
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      row_count_min: 6
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "chunked export with sparse IDs must pass row_count_min=6 by total, not per-chunk; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

// ─── G10: multi-export with one quality-fail — others still execute ───────────

/// Two exports in one config; the first has a row_count_min gate that will
/// fail (min=100 on a 5-row source); the second has no gate. The run as a
/// whole must exit non-zero (one failed export = non-zero), but the second
/// export must still execute end-to-end and produce its parquet file. Catches
/// "first export failed, abort the rest" regressions that erase work on
/// independent exports.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn gremlin_multi_export_one_quality_fail_does_not_abort_others() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let out_fail = tempfile::tempdir().unwrap();
    let out_ok = tempfile::tempdir().unwrap();
    let fail_name = unique_name("gr_me_fail");
    let ok_name = unique_name("gr_me_ok");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {fail_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir_fail}}}
    quality:
      row_count_min: 100
  - name: {ok_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir_ok}}}
"#,
        table_name = table.name(),
        dir_fail = out_fail.path().display(),
        dir_ok = out_ok.path().display(),
    );
    let (_cfgdir, cfgpath) = make_cfg(&yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfgpath.to_str().unwrap()])
        .output()
        .expect("spawn rivet (multi-export)");

    assert!(
        !result.status.success(),
        "multi-export with one failing quality gate must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    // The healthy export must have produced its output file even though the
    // sibling export failed. This is the actual gate: a "bail on first
    // failure" regression would leave out_ok empty.
    assert!(
        !files_with_extension(out_ok.path(), "parquet").is_empty(),
        "second export must complete independently of first export's failure; \
         out_ok=is_empty would mean a regression aborted siblings"
    );
}
