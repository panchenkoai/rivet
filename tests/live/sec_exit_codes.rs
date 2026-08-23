//! Live E2E tests for the **exit-code taxonomy** (see `crate::error::ExitClass`).
//!
//! An unattended scheduler branches on the process exit code instead of grepping
//! stderr:
//!
//!   * `0` success
//!   * `1` generic / config / usage error
//!   * `2` retryable (transient — safe to retry the same command)
//!   * `3` data-integrity (quality gate / reconcile mismatch / duplicate-guard /
//!     manifest inconsistency — STOP, data may be wrong)
//!   * `4` schema-drift (`on_schema_drift: fail` tripped — needs review)
//!
//! The class boundaries themselves are pinned by fast unit tests in
//! `src/error.rs` (transient→2, syntax→1, schema-drift→4, data-integrity→3) and
//! `src/pipeline/reconcile_cmd.rs` (a mismatch classifies to 3). This file proves
//! the *end-to-end wiring*: a real `rivet run` / `rivet reconcile` against live
//! Postgres returns the data-integrity code `3` — i.e. `main` actually routes the
//! failure through `classify_exit` and exits with the class, via the typed marker
//! (no string matching).
//!
//! Run with: `cargo test --test sec_exit_codes -- --include-ignored`

use crate::common::*;

/// A quality-gate failure (`row_count_min` above the real row count) must exit
/// with the **data-integrity** code `3` — the scheduler's signal to STOP, not
/// retry. The export wrote a (potentially wrong-shaped) dataset; blindly
/// retrying would re-produce the same failing result.
#[test]
#[ignore = "live: postgres"]
fn quality_gate_failure_exits_data_integrity_3() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5); // only 5 rows
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("xc_quality_3");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id FROM {}", table.name()))
        .export_line("quality:")
        .export_line("  row_count_min: 100")
        .dest_path(out.path().to_path_buf());

    let result = rig.run_args(&["--export", &export_name]);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert_eq!(
        result.status.code(),
        Some(3),
        "quality-gate failure must exit 3 (data-integrity); stderr:\n{stderr}"
    );
    // The human message is unchanged — the marker only sets the exit class.
    assert!(
        stderr.contains("quality check(s) failed"),
        "operator-facing quality message must be preserved verbatim; stderr:\n{stderr}"
    );
}

/// #9 e2e: the `row_count_min` tripwire (exit 3) must ALSO fire on the KEYSET
/// runner (which backs parallel-Mongo too). It was Chunked-only, so a truncated
/// keyset/parallel extract — the runners auto-selected for LARGE tables, where
/// completeness matters most — exited 0/success with the quality gate silently
/// disarmed. A TEXT primary key + `chunk_by_key` routes to the keyset runner.
#[test]
#[ignore = "live: postgres"]
fn keyset_quality_gate_failure_exits_data_integrity_3() {
    require_alive(LiveService::Postgres);
    let table_name = unique_name("keyset_quality_3");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (k TEXT PRIMARY KEY, v INT NOT NULL);
         INSERT INTO {table_name} (k, v) VALUES ('a', 1), ('b', 2), ('c', 3);"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table_name.clone());

    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("keyset_quality_3_exp");
    let rig = Rig::pg_batch(&table_name)
        .export_named(&export_name)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 2")
        .export_line("quality:")
        .export_line("  row_count_min: 100")
        .dest_path(out.path().to_path_buf());

    let result = rig.run_args(&["--export", &export_name]);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert_eq!(
        result.status.code(),
        Some(3),
        "keyset quality-gate failure must exit 3 — the row_count_min tripwire must fire on the \
         keyset runner, not silently pass (3 rows < min 100); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("quality check(s) failed"),
        "operator-facing quality message must be present; stderr:\n{stderr}"
    );
}

/// A `rivet reconcile` that finds a partition disagreeing with the source must
/// exit with the **data-integrity** code `3` (the taxonomy's "reconcile
/// mismatch" row), so a CI gate `rivet reconcile && <deploy>` stops on divergent
/// data instead of sailing past. Regression guard for the honesty gap where the
/// reconcile bail was an un-typed string → classified generic (exit 1).
#[test]
#[ignore = "live: postgres"]
fn reconcile_mismatch_exits_data_integrity_3() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(150); // ids 0..149 → chunks [0..49],[50..99],[100..149]
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("xc_reconcile_3");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());

    // Export records each chunk's row count in the manifest.
    let run = rig.run_args(&["--export", &export_name]);
    assert!(
        run.status.success(),
        "setup export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Mutate the SOURCE so a fresh per-chunk recount disagrees with the manifest:
    // drop one row inside chunk 0 (the recount falls 50→49 while the manifest
    // still says 50).
    {
        let mut c = pg_connect();
        c.execute(&format!("DELETE FROM {} WHERE id = 1", table.name()), &[])
            .expect("delete one source row to force a reconcile mismatch");
    }

    let result = rig.cli(&["reconcile", "--export", &export_name]);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert_eq!(
        result.status.code(),
        Some(3),
        "a reconcile mismatch must exit 3 (data-integrity), not 1; stderr:\n{stderr}"
    );
}

/// A clean export still exits `0` — the taxonomy must not regress the success
/// path. Guards against `classify_exit` (or the markers) accidentally tagging a
/// successful run.
#[test]
#[ignore = "live: postgres"]
fn clean_export_still_exits_0() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("xc_clean_0");

    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id FROM {}", table.name()))
        .export_line("quality:")
        .export_line("  row_count_min: 1")
        .dest_path(out.path().to_path_buf());

    let result = rig.run_args(&["--export", &export_name]);
    assert_eq!(
        result.status.code(),
        Some(0),
        "a clean export must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

/// A config / usage error that is *not* transient must exit `1` (generic), not
/// `2`/`3`/`4`. A missing config file is the simplest deterministic generic
/// error and needs no live infrastructure — but it shares this file because it
/// pins the same `main`→`classify_exit` wiring. (Not `#[ignore]`d: no DB.)
#[test]
fn missing_config_exits_generic_1() {
    let result = run_rivet(&[
        "run",
        "--config",
        "/nonexistent/rivet-does-not-exist.yaml",
        "--export",
        "anything",
    ]);
    assert_eq!(
        result.status.code(),
        Some(1),
        "a missing-config error is generic (exit 1), not retryable/data-integrity; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
