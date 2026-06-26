//! AUDIT-RED (cluster repair-trust): the reconcile → repair → reconcile loop
//! does not close, and `repair --execute` leaves the dataset in a state that
//! `rivet validate` flags.
//!
//! Two findings, both observed live against the docker stack:
//!
//! #7  `repair --execute` re-exports a mismatched chunk and prints
//!     "executed 1 · failed 0" (exit 0), but it drives the *stateless*
//!     chunked path (`pipeline::chunked::exec::run_chunked_sequential`),
//!     which never touches `chunk_task`. The stored `rows_written` for the
//!     chunk is left exactly as it was, so the very next `reconcile` recounts
//!     the source, compares against the *same* stale `rows_written`, and
//!     reports the *same* mismatch (exit 1). The reconcile → repair →
//!     reconcile loop therefore never converges.
//!
//! #8  The repair re-export lands a new parquet file alongside the originals
//!     (collision-proof naming, ADR-0009 RR5) but `manifest.json` is never
//!     updated to record it. A subsequent `rivet validate` lists the prefix
//!     and flags the repair-written file as an `untracked_object`.
//!
//! Both tests assert the CORRECT behavior (loop converges; manifest tracks the
//! repair file) and are expected to FAIL until the repair path updates state
//! and the manifest.

use crate::common::*;

// ─── setup helper ─────────────────────────────────────────────────────────────

/// Seed a `row_count`-row table, write a chunked-checkpoint config, run the
/// export, and return the table guard plus the output dir, config dir, and
/// config path. The state DB lands at `<config_dir>/.rivet_state.db`.
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

/// Run a SQL statement against the on-disk SQLite state DB via the `sqlite3`
/// CLI. The audit forced a chunk mismatch exactly this way (a direct
/// `UPDATE chunk_task ...`) — it is the most faithful reproduction because it
/// guarantees the mismatch survives a re-export that never touches state.
fn sqlite3_exec(state_db: &std::path::Path, sql: &str) {
    let out = std::process::Command::new("sqlite3")
        .arg(state_db)
        .arg(sql)
        .output()
        .expect("spawn sqlite3 (state DB edit)");
    assert!(
        out.status.success(),
        "sqlite3 edit failed: {sql}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `rivet reconcile --format json` → (exit success, parsed report).
fn reconcile_json(cfg: &std::path::Path, export: &str) -> (bool, serde_json::Value) {
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "reconcile",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            export,
            "--format",
            "json",
        ])
        .output()
        .expect("spawn rivet reconcile --format json");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "reconcile --format json must emit valid JSON (err: {e}); stdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    });
    (out.status.success(), json)
}

// ─── Finding #7: reconcile → repair → reconcile must converge ─────────────────

// AUDIT-RED repair-trust: `repair --execute` re-exports a chunk but never
// updates chunk_task.rows_written, so the next reconcile reports the SAME
// mismatch — the trust loop never converges. Asserts CORRECT behavior;
// expected to FAIL until fixed.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn audit_repair_then_reconcile_converges() {
    require_alive(LiveService::Postgres);
    let (table, _out, cfg_dir, cfg) = seed_and_run_chunked(100, 50);
    let state_db = cfg_dir.path().join(".rivet_state.db");

    // Force a chunk mismatch the way the audit did: corrupt the stored
    // exported count for chunk 0 directly in the state DB. The source still
    // holds 50 rows in [1..50], so reconcile will see source=50 vs exported=999.
    sqlite3_exec(
        &state_db,
        "UPDATE chunk_task SET rows_written = 999 WHERE chunk_index = 0;",
    );

    // Precondition: reconcile detects the mismatch and gates (exit non-zero).
    let (ok_before, before) = reconcile_json(&cfg, table.name());
    assert!(
        !ok_before,
        "precondition: reconcile must exit non-zero on the forced mismatch; report:\n{before:#}"
    );
    assert_eq!(
        before["summary"]["mismatches"].as_u64(),
        Some(1),
        "precondition: exactly one chunk must be mismatched before repair; report:\n{before:#}"
    );

    // Repair the mismatch.
    let repair = std::process::Command::new(RIVET_BIN)
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
        repair.status.success(),
        "repair --execute must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&repair.stderr)
    );

    // CORRECT behavior: having repaired the only mismatched chunk, a fresh
    // reconcile must now report a clean MATCH and exit 0 — the loop converges.
    let (ok_after, after) = reconcile_json(&cfg, table.name());
    let mismatches = after["summary"]["mismatches"].as_u64().unwrap_or(u64::MAX);
    assert_eq!(
        mismatches, 0,
        "BROKEN TRUST LOOP: after `repair --execute` reconcile still reports {mismatches} \
         mismatch(es) (chunk_task.rows_written was never updated by the repair re-export); \
         report:\n{after:#}"
    );
    assert!(
        ok_after,
        "BROKEN TRUST LOOP: reconcile must exit 0 after a successful repair, but it still \
         gates non-zero — reconcile → repair → reconcile never converges; report:\n{after:#}"
    );
}

// ─── Finding #8: `repair --execute` must keep `rivet validate` clean ──────────

// AUDIT-RED repair-trust: the repair re-export lands a new file but never
// updates manifest.json, so `rivet validate` flags it as an untracked object.
// Asserts CORRECT behavior (manifest tracks the repair file); expected to FAIL
// until fixed.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn audit_repair_keeps_validate_clean() {
    require_alive(LiveService::Postgres);
    let (table, out_dir, _cfg_dir, cfg) = seed_and_run_chunked(100, 50);

    // After the clean run, manifest.json + _SUCCESS + 2 chunk parts are at the
    // prefix; validate should be clean.
    let count_parquet = |dir: &std::path::Path| files_with_extension(dir, "parquet").len();
    let parts_before = count_parquet(out_dir.path());
    assert_eq!(
        parts_before, 2,
        "100 rows / chunk_size 50 → 2 chunk parts after the initial export"
    );

    // Drift the source under chunk 0 [1..50]: delete 20 rows → a real
    // mismatch (source 30 vs exported 50) that repair will re-export.
    let mut c = pg_connect();
    c.batch_execute(&format!("DELETE FROM {} WHERE id <= 20", table.name()))
        .expect("drift source");

    // Repair: re-exports chunk 0 as a NEW file alongside the originals.
    let repair = std::process::Command::new(RIVET_BIN)
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
        repair.status.success(),
        "repair --execute must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&repair.stderr)
    );

    // Sanity: the repair actually wrote a new parquet file at the prefix —
    // that file is the candidate the manifest must learn about.
    let parts_after = count_parquet(out_dir.path());
    assert!(
        parts_after > parts_before,
        "repair --execute must write a new chunk file at the prefix (before {parts_before}, \
         after {parts_after})"
    );

    // Validate the prefix as a machine-readable report.
    let validate = std::process::Command::new(RIVET_BIN)
        .args([
            "validate",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
        ])
        .output()
        .expect("spawn rivet validate --format json");
    let stdout = String::from_utf8_lossy(&validate.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "validate --format json must emit valid JSON (err: {e}); stdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&validate.stderr)
        )
    });

    // CORRECT behavior: every part under the prefix is tracked by the
    // manifest, so validate surfaces NO `untracked_object` failure. Today the
    // repair-written file is untracked because the manifest was never updated.
    let failures = json["exports"][0]["verification"]["failures"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let untracked: Vec<&serde_json::Value> = failures
        .iter()
        .filter(|f| f["kind"].as_str() == Some("untracked_object"))
        .collect();
    assert!(
        untracked.is_empty(),
        "UNTRACKED REPAIR FILE: `repair --execute` wrote a new part but did not update \
         manifest.json, so `rivet validate` flags it as an untracked object: {untracked:#?}\n\
         full verification:\n{}",
        json["exports"][0]["verification"]
    );
}
