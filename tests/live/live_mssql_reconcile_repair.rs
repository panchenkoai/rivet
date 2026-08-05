//! Live E2E tests for `rivet reconcile` and `rivet repair` against SQL Server —
//! SQL Server twin of `tests/live_mysql_reconcile_repair.rs` /
//! `tests/live_reconcile_repair.rs`.
//!
//! Reconcile/repair logic lives in `pipeline::reconcile_cmd` /
//! `pipeline::repair_cmd` and is driver-agnostic at the orchestration layer,
//! but it issues per-partition `COUNT(*)` against the source driver. This
//! file mirrors the RR1-RR6 matrix against MSSQL to ensure the
//! chunked-aware partition-counting and JSON contract both hold there.
//!
//! ## Test symmetry with `live_mysql_reconcile_repair.rs`
//!
//! | MySQL twin                                                            | MSSQL twin (this file)                                                |
//! |-----------------------------------------------------------------------|-----------------------------------------------------------------------|
//! | `mysql_reconcile_all_match_exits_zero_with_pretty_output`             | `mssql_reconcile_all_match_exits_zero_with_pretty_output`             |
//! | `mysql_reconcile_format_json_emits_valid_json_report`                 | `mssql_reconcile_format_json_emits_valid_json_report`                 |
//! | `mysql_reconcile_format_json_output_flag_writes_report_to_file`       | `mssql_reconcile_format_json_output_flag_writes_report_to_file`       |
//! | `mysql_repair_report_flag_uses_precomputed_reconcile_json`            | `mssql_repair_report_flag_uses_precomputed_reconcile_json`            |
//! | `mysql_repair_dryrun_exits_zero_when_nothing_to_repair`               | `mssql_repair_dryrun_exits_zero_when_nothing_to_repair`               |
//! | `mysql_repair_execute_flag_exits_zero_when_nothing_to_repair`         | `mssql_repair_execute_flag_exits_zero_when_nothing_to_repair`         |
//! | `mysql_repair_format_json_emits_valid_json_plan`                      | `mssql_repair_format_json_emits_valid_json_plan`                      |

use crate::common::*;

/// Seed a row_count-row MSSQL table, write a chunked config with checkpoint,
/// run the export, then return the config path (plus the table RAII guard).
fn seed_and_run_chunked(row_count: i64, chunk_size: u32) -> (MssqlTable, Rig) {
    let table = seed_mssql_numeric_table(row_count);
    // The rig owns its config and destination dirs, so the two TempDirs this
    // helper returned purely to keep alive are gone with the tuple.
    let rig = Rig::mssql_batch(table.name())
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        // Braces here were INTERPOLATION in the old format!; emitting them as a
        // plain literal makes the YAML invalid — the trap that broke two MySQL
        // chunked tests silently.
        .export_line(&format!("chunk_size: {chunk_size}"))
        .export_line("chunk_checkpoint: true");

    let run_out = rig.run_args(&["--export", table.name()]);
    assert!(
        run_out.status.success(),
        "setup export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run_out.stderr)
    );
    (table, rig)
}

// ─── RR1: reconcile pretty — all partitions match ────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_reconcile_all_match_exits_zero_with_pretty_output() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);

    let result = rig.cli(&["reconcile", "--export", table.name()]);

    assert!(
        result.status.success(),
        "reconcile must exit 0 when all partitions match; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("match") || stdout.contains("Match"),
        "reconcile output must report matching partitions; got:\n{stdout}"
    );
    assert!(
        stdout.contains("100"),
        "reconcile output must mention row count 100; got:\n{stdout}"
    );
    // Independent oracle: rivet's reconcile verdict is rivet's OWN counter
    // chain. Re-read the destination parquet and count physically (matrix
    // audit: self-oracle class) — the verdict and the physical rows must agree.
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        100,
        "destination parquet must physically hold every source row, independent of the reconcile verdict"
    );
}

// ─── RR2: reconcile --format json ────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_reconcile_format_json_emits_valid_json_report() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);

    let result = rig.cli(&["reconcile", "--export", table.name(), "--format", "json"]);

    assert!(
        result.status.success(),
        "reconcile --format json must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("reconcile --format json must be valid JSON");

    assert_eq!(
        json["export_name"].as_str().unwrap_or(""),
        table.name(),
        "JSON report must have correct export_name"
    );
    assert!(
        json["partitions"].is_array(),
        "JSON report must have 'partitions' array"
    );

    let partitions = json["partitions"].as_array().unwrap();
    assert_eq!(
        partitions.len(),
        2,
        "100 rows / chunk_size 50 → 2 partitions; got {partitions:?}"
    );
    for p in partitions {
        assert_eq!(
            p["status"].as_str().unwrap_or(""),
            "match",
            "all partitions must match after a clean run; partition:\n{p}"
        );
    }
}

// ─── RR3: reconcile --format json --output FILE ──────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_reconcile_format_json_output_flag_writes_report_to_file() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);
    let report_dir = tempfile::tempdir().unwrap();
    let report_path = report_dir.path().join("reconcile_report.json");

    let result = rig.cli(&[
        "reconcile",
        "--export",
        table.name(),
        "--format",
        "json",
        "--output",
        report_path.to_str().unwrap(),
    ]);

    assert!(
        result.status.success(),
        "reconcile --output must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        report_path.exists(),
        "reconcile --output must write the report file"
    );

    let raw = std::fs::read_to_string(&report_path).expect("read report file");
    let json: serde_json::Value =
        serde_json::from_str(raw.trim()).expect("report must be valid JSON");
    assert!(
        json["partitions"].is_array(),
        "written report must have partitions array"
    );
}

// ─── RR4b: repair --report uses precomputed reconcile JSON ───────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_repair_report_flag_uses_precomputed_reconcile_json() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);
    let report_dir = tempfile::tempdir().unwrap();
    let reconcile_path = report_dir.path().join("reconcile.json");

    let rec_out = rig.cli(&[
        "reconcile",
        "--export",
        table.name(),
        "--format",
        "json",
        "--output",
        reconcile_path.to_str().unwrap(),
    ]);

    assert!(
        rec_out.status.success(),
        "reconcile must succeed before repair --report; stderr:\n{}",
        String::from_utf8_lossy(&rec_out.stderr)
    );
    assert!(
        reconcile_path.exists(),
        "reconcile must write the report file"
    );

    let repair_out = rig.cli(&[
        "repair",
        "--export",
        table.name(),
        "--report",
        reconcile_path.to_str().unwrap(),
    ]);

    assert!(
        repair_out.status.success(),
        "repair --report must exit 0 when all partitions match; stderr:\n{}",
        String::from_utf8_lossy(&repair_out.stderr)
    );

    let stdout = String::from_utf8_lossy(&repair_out.stdout);
    assert!(
        stdout.contains("0") || stdout.contains("nothing") || stdout.contains("match"),
        "repair --report must report 0 actions for a clean run; got:\n{stdout}"
    );
}

// ─── RR4: repair dry-run — no mismatches ─────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_repair_dryrun_exits_zero_when_nothing_to_repair() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);

    let result = rig.cli(&["repair", "--export", table.name()]);

    assert!(
        result.status.success(),
        "repair dry-run must exit 0 when no mismatches; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(
        stdout.contains("0") || stdout.contains("nothing"),
        "repair dry-run must report 0 actions when partitions all match; got:\n{stdout}"
    );
}

// ─── RR5: repair --execute — no mismatches ───────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_repair_execute_flag_exits_zero_when_nothing_to_repair() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);

    let result = rig.cli(&["repair", "--export", table.name(), "--execute"]);

    assert!(
        result.status.success(),
        "repair --execute must exit 0 when nothing to repair; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

// ─── RR6: repair --format json dry-run ───────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_repair_format_json_emits_valid_json_plan() {
    require_alive(LiveService::Mssql);

    let (table, rig) = seed_and_run_chunked(100, 50);

    let result = rig.cli(&["repair", "--export", table.name(), "--format", "json"]);

    assert!(
        result.status.success(),
        "repair --format json must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("repair --format json must be valid JSON");
    assert!(
        json.get("actions").is_some() || json.get("export_name").is_some(),
        "repair JSON must have 'actions' or 'export_name' field; got:\n{stdout}"
    );
}
