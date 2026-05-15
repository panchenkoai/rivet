//! Crash-point recovery tests for the chunked export pipeline.
//!
//! These tests inject process panics at specific points in the sequential
//! checkpoint loop and verify that `--resume` produces correct final state.
//!
//! ## Fault points
//!
//! | Point | When | Post-crash chunk task status |
//! |---|---|---|
//! | `after_chunk_file:N` | after file written + manifest entry, before `complete_chunk_task` | `running` |
//! | `after_chunk_complete:N` | after `complete_chunk_task` succeeds | `completed` |
//!
//! ## Tests
//!
//! | ID | Scenario | Key invariant |
//! |---|---|---|
//! | C1 | Crash after chunk 0 complete → resume | Chunk 0 not re-run; total rows == seeded |
//! | C2 | Crash after chunk 0 file (before commit) → resume | Chunk 0 re-runs (at-least-once); manifest grows |
//!
//! ## Related
//!
//! F5 in `tests/recovery.rs` covers the state-layer invariant (completed chunk
//! data preserved after `reset_stale_running_chunk_tasks`) without a live DB.

mod common;

use common::*;

// ─── Test helpers ─────────────────────────────────────────────────────────────

fn open_state_db(cfg: &std::path::Path) -> rusqlite::Connection {
    let db = cfg.parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(db).expect("open state db")
}

/// Most-recently created `chunk_run.run_id` for the given export.
fn chunk_run_id(cfg: &std::path::Path, export: &str) -> Option<String> {
    open_state_db(cfg)
        .query_row(
            "SELECT run_id FROM chunk_run WHERE export_name = ?1 ORDER BY created_at DESC LIMIT 1",
            [export],
            |r| r.get(0),
        )
        .ok()
}

fn chunk_task_status(cfg: &std::path::Path, run_id: &str, chunk_index: i64) -> Option<String> {
    open_state_db(cfg)
        .query_row(
            "SELECT status FROM chunk_task WHERE run_id = ?1 AND chunk_index = ?2",
            rusqlite::params![run_id, chunk_index],
            |r| r.get(0),
        )
        .ok()
}

fn chunk_run_status(cfg: &std::path::Path, export: &str) -> Option<String> {
    open_state_db(cfg)
        .query_row(
            "SELECT status FROM chunk_run WHERE export_name = ?1 ORDER BY created_at DESC LIMIT 1",
            [export],
            |r| r.get(0),
        )
        .ok()
}

fn manifest_count(cfg: &std::path::Path, export: &str) -> i64 {
    open_state_db(cfg)
        .query_row(
            "SELECT COUNT(*) FROM file_manifest WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

fn manifest_total_rows(cfg: &std::path::Path, export: &str) -> i64 {
    open_state_db(cfg)
        .query_row(
            "SELECT COALESCE(SUM(row_count), 0) FROM file_manifest WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

// ─── C1: crash after chunk 0 complete → resume skips chunk 0 ─────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_crash_after_first_chunk_complete_resume_finishes_export() {
    // Crash after `complete_chunk_task` marks chunk 0 as done, before the loop
    // advances to chunk 1.  On resume, chunk 0 must NOT re-run; total row count
    // must equal the seeded count with no duplicates.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(150);
    let export = unique_name("c1_crash_complete");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // ── Crash run ────────────────────────────────────────────────────────────
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")
        .output()
        .expect("spawn rivet");
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    // ── Post-crash state assertions ───────────────────────────────────────────
    let run_id = chunk_run_id(&cfg, &export).expect("chunk_run must exist after crash");
    assert_eq!(
        chunk_task_status(&cfg, &run_id, 0).as_deref(),
        Some("completed"),
        "chunk 0 must be 'completed' in state DB after crash at after_chunk_complete:0"
    );
    assert_eq!(
        chunk_task_status(&cfg, &run_id, 1).as_deref(),
        Some("pending"),
        "chunk 1 must still be 'pending' after crash"
    );

    // ── Resume run ────────────────────────────────────────────────────────────
    let resume = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--resume",
        ])
        .output()
        .expect("spawn rivet resume");
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // ── Final state: chunk_run completed, 3 manifest entries, 150 total rows ─
    assert_eq!(
        chunk_run_status(&cfg, &export).as_deref(),
        Some("completed"),
        "chunk_run must be 'completed' after successful resume"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        3,
        "3 chunks × 1 run each = 3 manifest entries; chunk 0 must not be re-run"
    );
    assert_eq!(
        manifest_total_rows(&cfg, &export),
        150,
        "total manifest rows must equal seeded row count (no duplicates)"
    );
}

// ─── C2: crash after chunk 0 file (before commit) → resume re-runs chunk 0 ───

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_crash_after_chunk_file_before_commit_resume_reruns_chunk_atleastonce() {
    // Crash after the file for chunk 0 is written to the destination AND the
    // manifest entry is recorded, but BEFORE `complete_chunk_task` marks the
    // task as done.  The chunk_task status is left as 'running'.
    //
    // On resume, `reset_stale_running_chunk_tasks` resets chunk 0 → 'pending',
    // causing it to re-run.  This is the at-least-once delivery guarantee for
    // the chunk: the output for chunk 0 appears in 2 manifest entries (the
    // original pre-crash write and the post-resume write).
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(150);
    let export = unique_name("c2_crash_file");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: 50
    chunk_checkpoint: true
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // ── Crash run ────────────────────────────────────────────────────────────
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "after_chunk_file:0")
        .output()
        .expect("spawn rivet");
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    // ── Post-crash state: chunk 0 stuck in 'running' ─────────────────────────
    let run_id = chunk_run_id(&cfg, &export).expect("chunk_run must exist after crash");
    assert_eq!(
        chunk_task_status(&cfg, &run_id, 0).as_deref(),
        Some("running"),
        "chunk 0 must be 'running' after crash at after_chunk_file:0 (complete_chunk_task never called)"
    );
    // Manifest already has the pre-crash entry because record_file was called
    // before the panic point.
    assert_eq!(
        manifest_count(&cfg, &export),
        1,
        "exactly 1 manifest entry must exist after crash (chunk 0 file was recorded)"
    );

    // Sleep so that the resumed chunk 0 write gets a distinct filename timestamp.
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // ── Resume run ────────────────────────────────────────────────────────────
    let resume = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--resume",
        ])
        .output()
        .expect("spawn rivet resume");
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // ── Final state: at-least-once for chunk 0 → 4 manifest entries ──────────
    //
    // chunk 0 ran twice (pre-crash + post-resume), chunks 1 and 2 ran once each.
    // Total manifest entries = 2 + 1 + 1 = 4.
    // Total rows = 50 (chunk 0 first) + 50 (chunk 0 retry) + 50 (chunk 1) + 50 (chunk 2) = 200.
    assert_eq!(
        chunk_run_status(&cfg, &export).as_deref(),
        Some("completed"),
        "chunk_run must be 'completed' after resume"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        4,
        "chunk 0 ran twice (at-least-once) + chunks 1 and 2 = 4 manifest entries"
    );
    assert_eq!(
        manifest_total_rows(&cfg, &export),
        200,
        "50 rows × 4 manifest entries = 200 (chunk 0 counted twice due to at-least-once)"
    );
    // All files on disk: 3 chunks, but chunk 0 may have been overwritten if
    // both runs happened within the same second.  At a minimum, files for all
    // 3 chunks must be present (the final state is always correct).
    let parquet_files = files_with_extension(out.path(), "parquet");
    assert!(
        parquet_files.len() >= 3,
        "at least 3 parquet files must exist (one per chunk); found: {parquet_files:?}"
    );
}
