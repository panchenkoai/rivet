//! Crash-point recovery tests for the chunked export pipeline — SQL Server
//! (MSSQL) twin of `tests/live_mysql_chunked_recovery.rs`.
//!
//! The chunked checkpoint state machine lives in `src/pipeline/chunked/mod.rs`
//! and the fault-injection hooks (`after_chunk_file:N`, `after_chunk_complete:N`)
//! are driver-agnostic. This file pairs each MySQL chunked-recovery test with a
//! SQL Server twin so any regression of the chunked recovery semantics on the
//! MSSQL driver is caught with the same coverage as on MySQL/PG.
//!
//! ## Test symmetry with `live_mysql_chunked_recovery.rs`
//!
//! | MySQL twin | MSSQL twin (this file) |
//! |---|---|
//! | `mysql_chunked_crash_after_first_chunk_complete_resume_finishes_export` (C1)             | `mssql_chunked_crash_after_first_chunk_complete_resume_finishes_export`             |
//! | `mysql_chunked_crash_after_chunk_file_before_commit_resume_reruns_chunk_atleastonce` (C2) | `mssql_chunked_crash_after_chunk_file_before_commit_resume_reruns_chunk_atleastonce` |
//! | `mysql_parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates` (C3) | `mssql_parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates` |
//! | `mysql_parallel_chunked_crash_after_chunk_file_stuck_running_resume_reruns_chunk` (C4)    | `mssql_parallel_chunked_crash_after_chunk_file_stuck_running_resume_reruns_chunk`    |
//!
//! Test bodies are intentionally near-identical to the MySQL/PG suite so any
//! deviation is easy to spot in review. The only differences are the source
//! driver, fixture seeding (MSSQL `seed_mssql_numeric_table`), and the
//! YAML's `source` block (MSSQL forces TLS with a self-signed dev cert).

use crate::common::*;

// ─── State-DB inspection helpers (identical to the MySQL/PG twin) ────────────

fn open_state_db(cfg: &std::path::Path) -> rusqlite::Connection {
    let db = cfg.parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(db).expect("open state db")
}

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
            "SELECT COUNT(*) FROM file_log WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

fn manifest_total_rows(cfg: &std::path::Path, export: &str) -> i64 {
    open_state_db(cfg)
        .query_row(
            "SELECT COALESCE(SUM(row_count), 0) FROM file_log WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

fn chunk_task_status_counts(cfg: &std::path::Path, run_id: &str) -> (i64, i64, i64, i64) {
    open_state_db(cfg)
        .query_row(
            "SELECT
                COUNT(*) FILTER (WHERE status = 'completed'),
                COUNT(*) FILTER (WHERE status = 'running'),
                COUNT(*) FILTER (WHERE status = 'pending'),
                COUNT(*) FILTER (WHERE status = 'failed')
             FROM chunk_task WHERE run_id = ?1",
            [run_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .expect("aggregate chunk_task statuses")
}

// ─── C1: sequential — crash after chunk 0 complete → resume skips chunk 0 ────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_chunked_crash_after_first_chunk_complete_resume_finishes_export() {
    require_alive(LiveService::Mssql);

    let table = seed_mssql_numeric_table(150);
    let export = unique_name("ms_c1_crash_complete");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
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
    // Destination re-read: chunk 0 not re-run → exactly 150 rows, 150 distinct ids.
    assert_eq!(
        total_parquet_rows(out.path()),
        150,
        "destination must hold exactly 150 rows (no re-run, no dup)"
    );
    assert_eq!(
        dir_parquet_id_set(out.path()).len(),
        150,
        "150 distinct source ids must be present at the destination"
    );
}

// ─── C2: sequential — crash after chunk 0 file (before commit) → re-runs ─────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_chunked_crash_after_chunk_file_before_commit_resume_reruns_chunk_atleastonce() {
    require_alive(LiveService::Mssql);

    let table = seed_mssql_numeric_table(150);
    let export = unique_name("ms_c2_crash_file");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
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

    let run_id = chunk_run_id(&cfg, &export).expect("chunk_run must exist after crash");
    assert_eq!(
        chunk_task_status(&cfg, &run_id, 0).as_deref(),
        Some("running"),
        "chunk 0 must be 'running' after crash at after_chunk_file:0 (complete_chunk_task never called)"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        1,
        "exactly 1 manifest entry must exist after crash (chunk 0 file was recorded)"
    );

    std::thread::sleep(std::time::Duration::from_millis(1100));

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

    assert_eq!(
        chunk_run_status(&cfg, &export).as_deref(),
        Some("completed"),
        "chunk_run must be 'completed' after resume"
    );
    // chunk 0 ran twice (pre-crash + post-resume), chunks 1 and 2 once each:
    // 2 + 1 + 1 = 4 manifest entries; 50 * 4 = 200 rows (chunk 0 counted twice).
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
    let parquet_files = files_with_extension(out.path(), "parquet");
    assert!(
        parquet_files.len() >= 3,
        "at least 3 parquet files must exist (one per chunk); found: {parquet_files:?}"
    );
    // Destination re-read: every source id present (no loss) despite the
    // at-least-once chunk-0 re-run; physical rows >= source (the dup is surplus).
    assert_eq!(
        dir_parquet_id_set(out.path()).len(),
        150,
        "every seeded id must survive the at-least-once re-run (no loss)"
    );
    assert!(
        total_parquet_rows(out.path()) as i64 >= 150,
        "at-least-once: physical destination rows must be >= source (150)"
    );
}

// ─── C3: parallel checkpoint — panic during worker → clean resume ────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates() {
    // MSSQL twin of `mysql_parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates`.
    // See the PG twin's module doc-comment for the load-bearing invariants
    // (std::thread::scope panic propagation, non-deterministic pre-crash
    // distribution, decisive post-resume manifest assertions).
    require_alive(LiveService::Mssql);

    const ROW_COUNT: i64 = 400;
    const CHUNK_SIZE: i64 = 50;
    const EXPECTED_CHUNKS: i64 = ROW_COUNT / CHUNK_SIZE;

    let table = seed_mssql_numeric_table(ROW_COUNT);
    let export = unique_name("ms_c3_parallel_crash_complete");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: {CHUNK_SIZE}
    chunk_checkpoint: true
    parallel: 4
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

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
        "crash run must exit non-zero (worker panic propagated through scope); stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    let run_id = chunk_run_id(&cfg, &export).expect("chunk_run must exist after crash");
    assert_eq!(
        chunk_task_status(&cfg, &run_id, 0).as_deref(),
        Some("completed"),
        "chunk 0 must be 'completed' — panic fires AFTER complete_chunk_task succeeded"
    );
    assert_ne!(
        chunk_run_status(&cfg, &export).as_deref(),
        Some("completed"),
        "chunk_run must NOT be 'completed' — finalize_chunk_run_completed never ran"
    );
    let (_completed_pre, _running_pre, _pending_pre, failed_pre) =
        chunk_task_status_counts(&cfg, &run_id);
    assert_eq!(
        failed_pre, 0,
        "no chunk_task should be 'failed' from a panic; got failed={failed_pre}"
    );

    std::thread::sleep(std::time::Duration::from_millis(1100));

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

    assert_eq!(
        chunk_run_status(&cfg, &export).as_deref(),
        Some("completed"),
        "chunk_run must be 'completed' after parallel resume finalizes it"
    );

    let (completed_post, running_post, pending_post, failed_post) =
        chunk_task_status_counts(&cfg, &run_id);
    assert_eq!(
        completed_post, EXPECTED_CHUNKS,
        "after resume all {EXPECTED_CHUNKS} chunk_tasks must be 'completed' (got completed={completed_post}, running={running_post}, pending={pending_post}, failed={failed_post})"
    );
    assert_eq!(running_post, 0, "no chunk_task should remain 'running'");
    assert_eq!(pending_post, 0, "no chunk_task should remain 'pending'");
    assert_eq!(failed_post, 0, "no chunk_task should be 'failed'");

    assert_eq!(
        manifest_total_rows(&cfg, &export),
        ROW_COUNT,
        "total manifest rows must equal seeded count — no double-counting of completed chunks"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        EXPECTED_CHUNKS,
        "exactly {EXPECTED_CHUNKS} manifest entries expected: no chunk re-recorded"
    );

    // Destination re-read: no parallel double-write → exactly ROW_COUNT rows + distinct.
    assert_eq!(
        total_parquet_rows(out.path()) as i64,
        ROW_COUNT,
        "destination must hold exactly ROW_COUNT rows — no parallel double-write"
    );
    assert_eq!(
        dir_parquet_id_set(out.path()).len() as i64,
        ROW_COUNT,
        "ROW_COUNT distinct source ids must be present"
    );

    let parquet_files = files_with_extension(out.path(), "parquet");
    assert!(
        parquet_files.len() >= EXPECTED_CHUNKS as usize,
        "at least {EXPECTED_CHUNKS} parquet files must exist; found {}: {parquet_files:?}",
        parquet_files.len()
    );
}

// ─── C4: parallel — stuck-running recovery via at-least-once ─────────────────

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_parallel_chunked_crash_after_chunk_file_stuck_running_resume_reruns_chunk() {
    require_alive(LiveService::Mssql);

    const ROW_COUNT: i64 = 150;
    const CHUNK_SIZE: i64 = 50;
    const EXPECTED_CHUNKS: i64 = ROW_COUNT / CHUNK_SIZE;

    let table = seed_mssql_numeric_table(ROW_COUNT);
    let export = unique_name("ms_c4_parallel_stuck_running");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: {CHUNK_SIZE}
    chunk_checkpoint: true
    parallel: 1
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

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

    let run_id = chunk_run_id(&cfg, &export).expect("chunk_run must exist after crash");
    assert_eq!(
        chunk_task_status(&cfg, &run_id, 0).as_deref(),
        Some("running"),
        "chunk 0 must be 'running' after crash at after_chunk_file:0 (complete_chunk_task never called)"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        1,
        "exactly 1 manifest entry must exist after crash (chunk 0 file was recorded before panic)"
    );

    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Resume with parallel: 2 — proves reset works regardless of worker
    // count change between crash and resume.
    let yaml_resume = yaml.replace("parallel: 1", "parallel: 2");
    std::fs::write(&cfg, yaml_resume).expect("rewrite config for resume");

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

    assert_eq!(
        chunk_run_status(&cfg, &export).as_deref(),
        Some("completed"),
        "chunk_run must be 'completed' after resume"
    );

    let (completed_post, running_post, pending_post, failed_post) =
        chunk_task_status_counts(&cfg, &run_id);
    assert_eq!(
        completed_post, EXPECTED_CHUNKS,
        "all {EXPECTED_CHUNKS} chunk_tasks must be 'completed' (got completed={completed_post}, running={running_post}, pending={pending_post}, failed={failed_post})"
    );
    assert_eq!(running_post, 0);
    assert_eq!(pending_post, 0);
    assert_eq!(failed_post, 0);

    assert_eq!(
        manifest_count(&cfg, &export),
        EXPECTED_CHUNKS + 1,
        "{} entries expected (chunk 0 recorded twice due to at-least-once)",
        EXPECTED_CHUNKS + 1
    );
    assert_eq!(
        manifest_total_rows(&cfg, &export),
        ROW_COUNT + CHUNK_SIZE,
        "total manifest rows must equal seeded + one chunk (chunk 0 counted twice)"
    );

    // Destination re-read: every source id survives the stuck-running reset +
    // at-least-once re-run (no loss); physical rows >= source (dup is surplus).
    assert_eq!(
        dir_parquet_id_set(out.path()).len() as i64,
        ROW_COUNT,
        "every seeded id must land after stuck-running reset + at-least-once re-run"
    );
    assert!(
        total_parquet_rows(out.path()) as i64 >= ROW_COUNT,
        "at-least-once: physical destination rows must be >= ROW_COUNT"
    );

    let parquet_files = files_with_extension(out.path(), "parquet");
    assert!(
        parquet_files.len() >= EXPECTED_CHUNKS as usize,
        "at least {EXPECTED_CHUNKS} parquet files must exist; found {}: {parquet_files:?}",
        parquet_files.len()
    );
}
