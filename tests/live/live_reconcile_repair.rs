//! Live E2E tests for `rivet reconcile` and `rivet repair` commands.
//!
//! Both commands require a chunked export previously run with
//! `chunk_checkpoint: true`.
//!
//! | ID | Scenario | Contract |
//! |---|---|---|
//! | RR1 | `reconcile` pretty — all partitions match | RC1 — counts match → exit 0 |
//! | RR2 | `reconcile --format json` | RC1 — JSON report with partitions array |
//! | RR3 | `reconcile --format json --output FILE` | RC1 — report written to file |
//! | RR4 | `repair` dry-run — no mismatches | RR1 — 0 actions, exit 0 |
//! | RR4b | `repair --report` uses precomputed reconcile JSON | RR1 — skips live reconcile |
//! | RR5 | `repair --execute` — no mismatches | RR1 — execute with 0 repairs, exit 0 |
//! | RR6 | `repair --format json` dry-run | RR1 — JSON plan output |

use crate::common::*;

// ─── setup helper ─────────────────────────────────────────────────────────────

/// Seed a 100-row table, write a chunked config with checkpoint, run the
/// export, then return the config path (and the table RAII guard).
fn seed_and_run_chunked(
    row_count: i64,
    chunk_size: u32,
) -> (
    PgTable,
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let table = seed_pg_numeric_table(row_count);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: {chunk_size}
    chunk_checkpoint: true
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out_dir.path().display()
    );

    let cfg = write_config(&cfg_dir, &yaml);

    let run_out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet run (setup)");
    assert!(
        run_out.status.success(),
        "setup export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run_out.stderr)
    );

    (table, out_dir, cfg_dir, cfg)
}

// ─── RR1: reconcile pretty — all partitions match ─────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn reconcile_all_match_exits_zero_with_pretty_output() {
    require_alive(LiveService::Postgres);

    let (table, _out, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "reconcile",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet reconcile");

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
    // source and exported counts must both be 100.
    assert!(
        stdout.contains("100"),
        "reconcile output must mention row count 100; got:\n{stdout}"
    );
}

// ─── RR2: reconcile --format json ─────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn reconcile_format_json_emits_valid_json_report() {
    require_alive(LiveService::Postgres);

    let (table, _out, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "reconcile",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
        ])
        .output()
        .expect("spawn rivet reconcile --format json");

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
    // All partitions must be "match".
    for p in partitions {
        assert_eq!(
            p["status"].as_str().unwrap_or(""),
            "match",
            "all partitions must match after a clean run; partition:\n{p}"
        );
    }
}

// ─── RR3: reconcile --format json --output FILE ───────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn reconcile_format_json_output_flag_writes_report_to_file() {
    require_alive(LiveService::Postgres);

    let (table, _out, cfg_dir, cfg) = seed_and_run_chunked(100, 50);
    let report_path = cfg_dir.path().join("reconcile_report.json");

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "reconcile",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            report_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet reconcile --format json --output");

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

// ─── RR4b: repair --report uses precomputed reconcile JSON ────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn repair_report_flag_uses_precomputed_reconcile_json() {
    require_alive(LiveService::Postgres);

    let (table, _out, cfg_dir, cfg) = seed_and_run_chunked(100, 50);
    let reconcile_path = cfg_dir.path().join("reconcile.json");

    // Step 1: generate reconcile report.
    let rec_out = std::process::Command::new(RIVET_BIN)
        .args([
            "reconcile",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
            "--output",
            reconcile_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet reconcile --format json --output");

    assert!(
        rec_out.status.success(),
        "reconcile must succeed before repair --report; stderr:\n{}",
        String::from_utf8_lossy(&rec_out.stderr)
    );
    assert!(
        reconcile_path.exists(),
        "reconcile must write the report file"
    );

    // Step 2: repair dry-run using the precomputed reconcile JSON.
    let repair_out = std::process::Command::new(RIVET_BIN)
        .args([
            "repair",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--report",
            reconcile_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet repair --report");

    assert!(
        repair_out.status.success(),
        "repair --report must exit 0 when all partitions match; stderr:\n{}",
        String::from_utf8_lossy(&repair_out.stderr)
    );

    let stdout = String::from_utf8_lossy(&repair_out.stdout);
    // With all partitions matching, repair has nothing to do.
    assert!(
        stdout.contains("0") || stdout.contains("nothing") || stdout.contains("match"),
        "repair --report must report 0 actions for a clean run; got:\n{stdout}"
    );
}

// ─── RR4: repair dry-run — no mismatches ──────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn repair_dryrun_exits_zero_when_nothing_to_repair() {
    require_alive(LiveService::Postgres);

    let (table, _out, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "repair",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet repair (dry-run)");

    assert!(
        result.status.success(),
        "repair dry-run must exit 0 when no mismatches; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // When nothing needs repair, the output says "0 actions" or "nothing to repair".
    assert!(
        stdout.contains("0") || stdout.contains("nothing"),
        "repair dry-run must report 0 actions when partitions all match; got:\n{stdout}"
    );
}

// ─── RR5: repair --execute — no mismatches ────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn repair_execute_flag_exits_zero_when_nothing_to_repair() {
    require_alive(LiveService::Postgres);

    let (table, _out, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "repair",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--execute",
        ])
        .output()
        .expect("spawn rivet repair --execute");

    assert!(
        result.status.success(),
        "repair --execute must exit 0 when nothing to repair; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

// ─── RR6: repair --format json dry-run ────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn repair_format_json_emits_valid_json_plan() {
    require_alive(LiveService::Postgres);

    let (table, _out, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "repair",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
        ])
        .output()
        .expect("spawn rivet repair --format json");

    assert!(
        result.status.success(),
        "repair --format json must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    // Repair JSON plan must be valid JSON with an "actions" field.
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("repair --format json must be valid JSON");
    assert!(
        json.get("actions").is_some() || json.get("export_name").is_some(),
        "repair JSON must have 'actions' or 'export_name' field; got:\n{stdout}"
    );
}

// ─── RR7: reconcile exits non-zero on a real mismatch (gate, not just report) ──
//
// Regression: `reconcile` printed `N mismatch` but returned Ok → exit 0, so
// `rivet reconcile && <next>` proceeded on disagreeing data. The audit must gate.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn reconcile_exits_nonzero_on_mismatch() {
    require_alive(LiveService::Postgres);
    let (table, _out, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    // Drift the source under chunk 0 [1..50]: delete 20 rows → source 30 vs exported 50.
    let mut c = pg_connect();
    c.batch_execute(&format!("DELETE FROM {} WHERE id <= 20", table.name()))
        .expect("drift source");

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "reconcile",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .output()
        .expect("spawn rivet reconcile");

    assert!(
        !out.status.success(),
        "reconcile must exit non-zero on a detected mismatch; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("disagree with the source"),
        "must name the mismatch in the error; stderr:\n{stderr}"
    );
}

// ─── RR8: repair --execute re-exports a real mismatch AND preserves the original ──
//
// The repair path was only covered for the no-mismatch case. This pins the real
// recovery + ADR-0009 RR5: the re-exported chunk lands as a NEW file alongside
// the original (collision-proof naming), and the original is never overwritten.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn repair_execute_reexports_mismatch_and_preserves_original_file() {
    require_alive(LiveService::Postgres);
    let (table, out_dir, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    let chunk0 = |dir: &std::path::Path| -> Vec<std::path::PathBuf> {
        files_with_extension(dir, "parquet")
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .map(|n| n.to_string_lossy().contains("chunk0"))
                    .unwrap_or(false)
            })
            .collect()
    };

    let before = chunk0(out_dir.path());
    assert_eq!(before.len(), 1, "exactly one chunk0 file after export");
    let original = before[0].clone();
    let original_size = std::fs::metadata(&original).unwrap().len();

    // Drift source under chunk 0 → mismatch that repair will re-export.
    let mut c = pg_connect();
    c.batch_execute(&format!("DELETE FROM {} WHERE id <= 20", table.name()))
        .expect("drift source");

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "repair",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--execute",
        ])
        .output()
        .expect("spawn rivet repair --execute");
    assert!(
        out.status.success(),
        "repair --execute must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );

    // RR5: the original file survives, byte-for-byte unchanged.
    assert!(
        original.exists(),
        "RR5: repair must NOT delete the original chunk0 file"
    );
    assert_eq!(
        std::fs::metadata(&original).unwrap().len(),
        original_size,
        "RR5: repair must NOT overwrite the original chunk0 file"
    );
    // The re-export landed as a distinct new file alongside it.
    assert_eq!(
        chunk0(out_dir.path()).len(),
        2,
        "repair must add a new chunk0 file alongside the original (collision-proof naming)"
    );
}
