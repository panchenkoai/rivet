//! Crash-point recovery matrix — MySQL twin of `tests/live_crash_recovery.rs`.
//!
//! The four pipeline boundaries (`after_source_read`, `after_file_write`,
//! `after_manifest_update`, `after_cursor_commit`) live in `src/pipeline/single.rs`
//! and are driver-agnostic — they fire from the pipeline orchestration layer,
//! not from the source driver. This file pairs each PG crash-recovery test
//! with a MySQL twin so any future regression of the recovery semantics in
//! one driver is caught for the other as well.
//!
//! ## Test symmetry with `live_crash_recovery.rs`
//!
//! | PG test                                                       | MySQL twin (this file)                                              |
//! |---------------------------------------------------------------|---------------------------------------------------------------------|
//! | `crash_after_source_read_leaves_state_completely_clean`       | `mysql_crash_after_source_read_leaves_state_completely_clean`       |
//! | `crash_after_file_write_leaves_file_but_no_manifest_or_cursor`| `mysql_crash_after_file_write_leaves_file_but_no_manifest_or_cursor`|
//! | `crash_after_manifest_update_leaves_file_and_manifest_but_no_cursor` | `mysql_crash_after_manifest_update_leaves_file_and_manifest_but_no_cursor` |
//! | `crash_after_cursor_commit_is_recoverable_with_full_state`    | `mysql_crash_after_cursor_commit_is_recoverable_with_full_state`    |
//!
//! Test bodies are intentionally near-identical so deviations are easy to
//! spot in review. The only real differences are the cursor column shape
//! (MySQL `DATETIME` with second granularity vs. PG `TIMESTAMPTZ`) and
//! the seeding SQL.

mod common;

use common::*;
use mysql::prelude::Queryable;

// ─── State-DB inspection helpers (identical to PG twin) ──────────────────────

/// Open the state DB that rivet wrote next to the given config file.
fn open_state_db(cfg: &std::path::Path) -> rusqlite::Connection {
    let db = cfg.parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(db).expect("open state db")
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

// ─── MySQL-specific seeding + helpers ────────────────────────────────────────

/// RAII drop guard for the MySQL fixture table created by
/// `seed_mysql_cursor_table`. `common::MysqlTable` exists, but its field is
/// private to the seeding helpers in `common/mod.rs`; re-defining a tiny
/// local guard keeps this file self-contained and avoids widening the public
/// surface of `common` just for one extra cursor-table seeder.
struct MysqlCursorTable {
    name: String,
}

impl MysqlCursorTable {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for MysqlCursorTable {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.name));
        }
    }
}

/// Seed a MySQL table with `(id BIGINT PK, updated_at DATETIME)` rows ordered
/// monotonically — same logical shape as the PG twin's `seed_cursor_table`.
/// A flat `INSERT ... VALUES` is faster than `INSERT ... SELECT` for the
/// small row counts these tests use, and is easier to reason about.
fn seed_mysql_cursor_table(rows: i64) -> MysqlCursorTable {
    let name = unique_name("qa11_mysql");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB;"
    ))
    .expect("create mysql qa11 table");

    if rows > 0 {
        let mut sql = format!("INSERT INTO {name} (id, updated_at) VALUES ");
        for i in 1..=rows {
            if i > 1 {
                sql.push_str(", ");
            }
            // Each row's updated_at is (now - (rows-i) seconds) so the cursor
            // walk is deterministic and rows export in id order.
            sql.push_str(&format!(
                "({i}, DATE_SUB(NOW(), INTERVAL {} SECOND))",
                rows - i
            ));
        }
        c.query_drop(sql).expect("seed mysql qa11 rows");
    }
    MysqlCursorTable { name }
}

fn write_cfg(
    out_dir: &std::path::Path,
    table_name: &str,
    export_name: &str,
    cfg_dir: &tempfile::TempDir,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"
source: {{type: mysql, url: "{MYSQL_URL}"}}
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
        "rivet run with RIVET_TEST_PANIC_AT={crash_at} must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

// ─── MySQL crash-recovery matrix ─────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_crash_after_source_read_leaves_state_completely_clean() {
    // ADR-0001 pre-I1 crash window. Source stream drained but the writer is
    // not finalised, so the destination never sees a file. Contract: no
    // file, no manifest entry, no cursor advance.
    require_alive(LiveService::Mysql);
    let table = seed_mysql_cursor_table(10);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11my_src");
    let cfg = write_cfg(out.path(), table.name(), &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_source_read");

    assert!(
        files_with_extension(out.path(), "parquet").is_empty(),
        "after_source_read crash must not produce a file"
    );
    assert_eq!(manifest_count(&cfg, &export), 0);
    assert_eq!(cursor_value(&cfg, &export), None);
    // Metric may or may not be written depending on where in the outer loop
    // the panic unwinds — either way, the resume logic does not depend on it.

    // Recovery: re-run without the crash. Full row count must surface.
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
#[ignore = "live: requires docker compose mysql"]
fn mysql_crash_after_file_write_leaves_file_but_no_manifest_or_cursor() {
    // ADR-0001 I2→I3 crash window. The operator-visible state must be:
    // file on disk, manifest empty, cursor absent. Recovery: next run sees
    // no cursor and re-exports — at-least-once delivery for that file.
    require_alive(LiveService::Mysql);
    let table = seed_mysql_cursor_table(8);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11my_file");
    let cfg = write_cfg(out.path(), table.name(), &export, &cfg_dir);

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
    // producing a second file (different timestamp). Sleep to ensure a
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
    // now populated. The orphaned pre-crash file is still on disk — this is
    // the documented at-least-once-delivery corollary.
    assert_eq!(manifest_count(&cfg, &export), 1);
    assert!(cursor_value(&cfg, &export).is_some());
    let total = files_with_extension(out.path(), "parquet").len();
    assert!(
        total >= 2,
        "orphaned pre-crash file + recovery file: expected >=2, got {total}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_crash_after_manifest_update_leaves_file_and_manifest_but_no_cursor() {
    require_alive(LiveService::Mysql);
    let table = seed_mysql_cursor_table(7);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11my_mani");
    let cfg = write_cfg(out.path(), table.name(), &export, &cfg_dir);

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

    // Recovery: re-run. No cursor → full re-export; manifest grows by 1.
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
#[ignore = "live: requires docker compose mysql"]
fn mysql_crash_after_cursor_commit_is_recoverable_with_full_state() {
    // Crash between cursor commit and final run metric. The write cycle is
    // structurally complete; only observability (the metric row) is missing.
    // The next run must see the cursor and export zero new rows.
    require_alive(LiveService::Mysql);
    let table = seed_mysql_cursor_table(6);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11my_curs");
    let cfg = write_cfg(out.path(), table.name(), &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_cursor_commit");

    assert_eq!(files_with_extension(out.path(), "parquet").len(), 1);
    assert_eq!(manifest_count(&cfg, &export), 1);
    let cursor_after_crash = cursor_value(&cfg, &export);
    assert!(
        cursor_after_crash.is_some(),
        "after_cursor_commit must leave the cursor advanced"
    );
    // Metric may be absent (panic before record_metric) OR present (panic
    // after). The test accepts either outcome — the key invariant is that
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
