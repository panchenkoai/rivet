//! E2E tests for batch memory cap policies (`warn`, `fail`, `auto_shrink`).
//!
//! Roadmap Phase 1 §3.1 — Correctness Lockdown for batch memory policies.
//!
//! All tests in this file require the docker-compose Postgres stack.
//! Run with: `cargo test --test batch_memory_policy -- --include-ignored`
//!
//! ## What these tests verify
//!
//! - `warn`: export succeeds, warning is emitted to stderr, output is complete.
//! - `fail`: export exits non-zero before writing inconsistent data.
//! - `auto_shrink` + Parquet: output file exists, is non-empty, export exits 0.
//! - `auto_shrink` + CSV: output file exists, is non-empty, export exits 0.
//! - `auto_shrink` + incremental: cursor state is correct — second run sees no
//!   new rows after the full table has been exported once.
//! - Batch below cap: export succeeds silently with no oversized-batch warning.

use crate::common::*;

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Write a config YAML to a tempdir and return (dir_guard, path_to_yaml).
fn cfg(rig: &Rig) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = rig.config_in(d.path());
    (d, p)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn batch_below_cap_succeeds_silently() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(50); // tiny — will never exceed 1 MB
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("bmp_below_cap");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, name, amount FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line(r#"columns: { amount: "decimal(12,2)" }"#)
        .export_line("tuning:")
        .export_line("  max_batch_memory_mb: 512")
        .export_line("  on_batch_memory_exceeded: warn")
        .dest_path(out.path().to_path_buf());
    let (_cfgdir, cfgpath) = cfg(&rig);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "export below cap must succeed; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        !stderr.contains("exceeds max_batch_memory_mb"),
        "no oversized warning expected for small batch; stderr:\n{stderr}"
    );
    let files = files_with_extension(out.path(), "parquet");
    assert!(!files.is_empty(), "must produce at least one parquet file");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn warn_policy_succeeds_and_emits_warning() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_wide_table(2000, 0);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("bmp_warn");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, payload FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  batch_size: 2000")
        .export_line("  max_batch_memory_mb: 1")
        .export_line("  on_batch_memory_exceeded: warn")
        .dest_path(out.path().to_path_buf());
    let (_cfgdir, cfgpath) = cfg(&rig);

    // Use RUST_LOG=warn so that log::warn! output is visible in stderr.
    let result = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfgpath.to_str().unwrap(),
        "--export",
        &export_name,
    ]);
    assert!(
        result.status.success(),
        "warn policy must not fail the export; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("exceeds max_batch_memory_mb"),
        "warn policy must emit the oversized-batch warning; stderr:\n{stderr}"
    );
    let files = files_with_extension(out.path(), "parquet");
    assert!(!files.is_empty(), "warn policy must still write output");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn fail_policy_exits_nonzero_when_batch_exceeds_cap() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_wide_table(2000, 0);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("bmp_fail");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, payload FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  batch_size: 2000")
        .export_line("  max_batch_memory_mb: 1")
        .export_line("  on_batch_memory_exceeded: fail")
        .dest_path(out.path().to_path_buf());
    let (_cfgdir, cfgpath) = cfg(&rig);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "fail policy must exit non-zero when batch exceeds cap"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("exceeds") || stderr.contains("max_batch_memory"),
        "fail policy error message must mention the cap; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn auto_shrink_parquet_output_is_complete_and_readable() {
    // auto_shrink splits oversized batches recursively and writes sub-batches.
    // Definition of done: export succeeds, parquet file is non-empty.
    require_alive(LiveService::Postgres);
    const ROWS: usize = 2000;
    let table = seed_pg_wide_table(ROWS as i64, 0);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("bmp_shrink_pq");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, payload FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  batch_size: 2000")
        .export_line("  max_batch_memory_mb: 1")
        .export_line("  on_batch_memory_exceeded: auto_shrink")
        .dest_path(out.path().to_path_buf());
    let (_cfgdir, cfgpath) = cfg(&rig);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "auto_shrink must not fail the export; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let files = files_with_extension(out.path(), "parquet");
    assert!(
        !files.is_empty(),
        "auto_shrink must produce at least one parquet file"
    );
    // The rows are the point, and until 2026-08-17 nothing read them: the test
    // was named `..._is_complete_and_readable` and its body never opened a file
    // (`metadata(f).len() > 0`). `auto_shrink` SPLITS an oversized batch
    // recursively, so its failure mode is a sub-batch that never gets written —
    // and a splitter that drops one leaves every remaining file non-empty. The
    // name promised what the body could not check.
    //
    // DuckDB, not the parquet crate rivet writes with: a symmetric encode/decode
    // fault cancels out in the shared path.
    assert_eq!(
        duckdb_total_parquet_rows(out.path()),
        ROWS,
        "auto_shrink must deliver every source row — a dropped sub-batch leaves \
         the remaining files non-empty and this is the only assertion that sees it"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn auto_shrink_csv_output_is_complete_and_valid() {
    require_alive(LiveService::Postgres);
    const ROWS: usize = 2000;
    let table = seed_pg_wide_table(ROWS as i64, 0);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("bmp_shrink_csv");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, payload FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .with_format("csv")
        .export_line("tuning:")
        .export_line("  batch_size: 2000")
        .export_line("  max_batch_memory_mb: 1")
        .export_line("  on_batch_memory_exceeded: auto_shrink")
        .dest_path(out.path().to_path_buf());
    let (_cfgdir, cfgpath) = cfg(&rig);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "auto_shrink CSV must not fail; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let files = files_with_extension(out.path(), "csv");
    assert!(
        !files.is_empty(),
        "auto_shrink CSV must produce at least one file"
    );
    // `lines.len() >= 2` per file passes when 2000 rows deliver ONE. Count the
    // data rows across every part and compare to what was seeded — the CSV
    // splitter drops a sub-batch the same way the parquet one does.
    let delivered: usize = files
        .iter()
        .map(|f| {
            let content = std::fs::read_to_string(f).expect("read csv");
            // A header-only part (the splitter's failure signature) is 1 line.
            content.lines().count().saturating_sub(1)
        })
        .sum();
    assert_eq!(
        delivered,
        ROWS,
        "auto_shrink CSV must deliver every source row across its parts; got \
         {delivered} data rows in {} file(s)",
        files.len()
    );
    for f in &files {
        let content = std::fs::read_to_string(f).expect("read csv");
        // Header + at least one data row means the file has >= 2 lines.
        let lines: Vec<&str> = content.lines().collect();
        assert!(
            lines.len() >= 2,
            "CSV output must have header + data rows; got {} lines in {}",
            lines.len(),
            f.display()
        );
        // Verify the header contains expected columns.
        assert!(
            lines[0].contains("id") && lines[0].contains("payload"),
            "CSV header must contain id and payload columns; got: {}",
            lines[0]
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn auto_shrink_incremental_cursor_is_correct() {
    // Contract: when auto_shrink splits batches, the cursor value written to
    // state must be the last value from the ORIGINAL batch, not from a sub-batch.
    // Verification: two runs on an unchanged source — second run must produce
    // no new files (cursor was set correctly after run #1).
    require_alive(LiveService::Postgres);

    let table_name = unique_name("bmp_inc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            payload TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
         );
         INSERT INTO {table_name} (id, payload, updated_at)
         SELECT g, repeat('x', 600), now() - (interval '1 second') * (2001 - g)
         FROM generate_series(1, 2000) g;"
    ))
    .unwrap();

    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = Cleanup(table_name.clone());

    let export_name = unique_name("bmp_inc_exp");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, payload, updated_at FROM {table_name}"#
        ))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .export_line("tuning:")
        .export_line("  batch_size: 2000")
        .export_line("  max_batch_memory_mb: 1")
        .export_line("  on_batch_memory_exceeded: auto_shrink")
        .dest_path(out.path().to_path_buf());
    let (_cfgdir, cfgpath) = cfg(&rig);

    // Run #1 — must export all 2000 rows.
    let r1 = run_rivet_export(&cfgpath, &export_name);
    assert!(
        r1.status.success(),
        "auto_shrink incremental run #1 failed; stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let files_after_first = files_with_extension(out.path(), "parquet").len();
    assert_eq!(
        files_after_first, 1,
        "first incremental run must produce exactly one file"
    );

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    // Run #2 — source unchanged, cursor should be at last updated_at → zero new rows → no new file.
    let r2 = run_rivet_export(&cfgpath, &export_name);
    assert!(
        r2.status.success(),
        "auto_shrink incremental run #2 (no new rows) failed; stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let files_after_second = files_with_extension(out.path(), "parquet").len();
    assert_eq!(
        files_after_second, files_after_first,
        "second run on unchanged source must not produce duplicates — \
         auto_shrink cursor must match the last row of the original batch; \
         got {} files after run #2, expected {}",
        files_after_second, files_after_first
    );
}
