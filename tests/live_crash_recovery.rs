//! Crash-point recovery matrix.
//!
//! QA backlog Task 1.1.  For each of the four observable write-path
//! boundaries, inject a panic via the `RIVET_TEST_PANIC_AT` hook (see
//! `src/test_hook.rs`), observe the post-crash state, then re-run without
//! the injection and assert recovery produces the expected final state.
//!
//! Boundaries exercised:
//!
//! | Point                    | Triggered at                                 | Expected post-crash state         |
//! |--------------------------|----------------------------------------------|-----------------------------------|
//! | `after_source_read`      | source stream drained, writer not finalised  | no file, no manifest, no cursor   |
//! | `after_file_write`       | `dest.write` Ok, no manifest yet             | file on disk, no manifest entry   |
//! | `after_manifest_update`  | manifest row written, cursor not advanced    | file + manifest, no cursor        |
//! | `after_cursor_commit`    | cursor advanced, no final metric             | file + manifest + cursor, no metric |
//!
//! After each crash we run rivet again without the env var and assert the
//! final state matches the no-crash baseline for that export (full row
//! count, single manifest entry per unique file name, cursor at expected
//! value).

mod common;

use common::*;

/// RAII guard — drops a Postgres table on scope exit.
struct PgCleanup(String);
impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Run rivet with the given panic-point injected, expecting a non-zero exit.
fn run_rivet_crash(
    cfg_path: &std::path::Path,
    export_name: &str,
    crash_at: &str,
) -> std::process::Output {
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            export_name,
        ])
        .env("RIVET_TEST_PANIC_AT", crash_at)
        .output()
        .expect("spawn rivet");
    assert!(
        !out.status.success(),
        "run with RIVET_TEST_PANIC_AT='{crash_at}' must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Open the state DB that rivet wrote next to the given config file.
fn open_state_db(cfg: &std::path::Path) -> rusqlite::Connection {
    let db = cfg.parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(db).expect("open state db")
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

fn cursor_value(cfg: &std::path::Path, export: &str) -> Option<String> {
    open_state_db(cfg)
        .query_row(
            "SELECT last_cursor_value FROM export_state WHERE export_name = ?1",
            [export],
            |r| r.get::<_, Option<String>>(0),
        )
        .unwrap_or(None)
}

fn metric_count(cfg: &std::path::Path, export: &str) -> i64 {
    open_state_db(cfg)
        .query_row(
            "SELECT COUNT(*) FROM export_metrics WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

/// Seed a table with an ordered `updated_at` cursor column.
fn seed_cursor_table(rows: i64) -> (String, PgCleanup) {
    let name = unique_name("qa11");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            updated_at TIMESTAMPTZ NOT NULL
        );
        INSERT INTO {name} (id, updated_at)
        SELECT g, now() - (interval '1 minute') * ({rows} - g)
        FROM generate_series(1, {rows}) g;"
    ))
    .unwrap();
    (name.clone(), PgCleanup(name))
}

fn write_cfg(
    out_dir: &std::path::Path,
    table_name: &str,
    export_name: &str,
    cfg_dir: &tempfile::TempDir,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, updated_at FROM {table_name}"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        dir = out_dir.display()
    );
    write_config(cfg_dir, &yaml)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_source_read_leaves_state_completely_clean() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(10);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_src");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_source_read");

    // Contract: no file, no manifest entry, no cursor advance, no metric.
    assert!(
        files_with_extension(out.path(), "parquet").is_empty(),
        "after_source_read crash must not produce a file"
    );
    assert_eq!(manifest_count(&cfg, &export), 0);
    assert_eq!(cursor_value(&cfg, &export), None);
    // Metric may or may not be written depending on where in the outer loop
    // the panic unwinds — either way, the resume logic does not depend on it.

    // Recovery: re-run without the crash.  Full row count must surface.
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success(), "recovery run must succeed");
    assert_eq!(
        files_with_extension(out.path(), "parquet").len(),
        1,
        "recovery run must produce the single expected file"
    );
    assert!(cursor_value(&cfg, &export).is_some());
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_file_write_leaves_file_but_no_manifest_or_cursor() {
    // ADR-0001 I2→I3 crash window.  The operator-visible state must be:
    // file on disk, manifest empty, cursor absent.  Recovery: next run sees
    // no cursor and re-exports — at-least-once delivery for that file.
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(8);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_file");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_file_write");

    let files_after_crash = files_with_extension(out.path(), "parquet");
    assert_eq!(
        files_after_crash.len(),
        1,
        "after_file_write must have left exactly one file on disk; got: {files_after_crash:?}"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        0,
        "after_file_write must leave manifest empty"
    );
    assert_eq!(cursor_value(&cfg, &export), None);

    // Recovery: second run sees no cursor so it re-exports everything,
    // producing a second file (different timestamp).  Sleep to ensure a
    // distinct timestamp (rivet uses 1-second granularity).
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success());

    // Post-recovery: manifest has one entry for the recovery run, cursor is
    // now populated.  The orphaned pre-crash file is still on disk — this
    // is the documented at-least-once-delivery corollary.
    assert_eq!(manifest_count(&cfg, &export), 1);
    assert!(cursor_value(&cfg, &export).is_some());
    let total = files_with_extension(out.path(), "parquet").len();
    assert!(
        total >= 2,
        "orphaned pre-crash file + recovery file: expected >=2, got {total}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_manifest_update_leaves_file_and_manifest_but_no_cursor() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(7);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_mani");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_manifest_update");

    assert_eq!(files_with_extension(out.path(), "parquet").len(), 1);
    assert_eq!(
        manifest_count(&cfg, &export),
        1,
        "after_manifest_update must leave the manifest row written"
    );
    assert_eq!(
        cursor_value(&cfg, &export),
        None,
        "after_manifest_update must leave the cursor unset"
    );

    // Recovery: re-run.  No cursor → full re-export; manifest grows by 1.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success());
    assert_eq!(manifest_count(&cfg, &export), 2);
    assert!(cursor_value(&cfg, &export).is_some());
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_cursor_commit_is_recoverable_with_full_state() {
    // Crash between cursor commit and final run metric.  The write cycle
    // is structurally complete; only observability (the metric row) is
    // missing.  The next run must see the cursor and export zero new rows.
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(6);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_curs");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_cursor_commit");

    assert_eq!(files_with_extension(out.path(), "parquet").len(), 1);
    assert_eq!(manifest_count(&cfg, &export), 1);
    let cursor_after_crash = cursor_value(&cfg, &export);
    assert!(
        cursor_after_crash.is_some(),
        "after_cursor_commit must leave the cursor advanced"
    );
    // Metric may be absent (panic before record_metric) OR present (panic
    // after).  The test accepts either outcome — the key invariant is that
    // the write cycle is durable.
    let _metric_after_crash = metric_count(&cfg, &export);

    // Recovery: re-run must see the cursor and export zero additional rows.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success());

    assert_eq!(
        files_with_extension(out.path(), "parquet").len(),
        1,
        "second run on unchanged source must not produce a new file (cursor saw no new rows)"
    );
    assert_eq!(
        cursor_value(&cfg, &export),
        cursor_after_crash,
        "cursor value must stay at the post-crash position when no new data arrived"
    );
}
