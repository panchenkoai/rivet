//! Idempotent resume across export modes — MySQL twin of `tests/live_resume.rs`.
//!
//! Mirrors the QA-1.2 resume matrix against the MySQL driver: the resume
//! semantics (manifest accumulation in full mode, cursor advance + zero-new-rows
//! in incremental mode, `--resume` flag validation in chunked mode) all live in
//! the pipeline orchestration layer and are driver-agnostic, but the live
//! contract still must hold on both sources or "MySQL incremental works the
//! same way as PG" is just an aspiration.
//!
//! ## Test symmetry with `live_resume.rs`
//!
//! | PG test                                                              | MySQL twin (this file)                                                     |
//! |----------------------------------------------------------------------|----------------------------------------------------------------------------|
//! | `full_mode_repeated_run_accumulates_manifest_entries`                | `mysql_full_mode_repeated_run_accumulates_manifest_entries`                |
//! | `incremental_second_run_on_unchanged_source_exports_zero_new_rows`   | `mysql_incremental_second_run_on_unchanged_source_exports_zero_new_rows`   |
//! | `incremental_third_run_picks_up_newly_inserted_rows`                 | `mysql_incremental_third_run_picks_up_newly_inserted_rows`                 |
//! | `chunked_resume_without_prior_run_fails_with_actionable_message`     | `mysql_chunked_resume_without_prior_run_fails_with_actionable_message`     |
//! | `chunked_resume_with_completed_run_gives_actionable_message`         | `mysql_chunked_resume_with_completed_run_gives_actionable_message`         |
//! | `full_mode_resume_flag_is_rejected`                                  | `mysql_full_mode_resume_flag_is_rejected`                                  |

use crate::common::*;
use mysql::prelude::Queryable;

fn cfg_dir_with(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

/// Local RAII guard for ad-hoc MySQL tables that don't fit the shape of
/// `common::seed_mysql_numeric_table` (the incremental tests want a narrow
/// `(id, updated_at)` cursor table).
struct MysqlCleanup(String);
impl Drop for MysqlCleanup {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

/// Seed a `(id BIGINT PK, updated_at DATETIME)` MySQL table with N
/// monotonically-increasing rows.
fn seed_cursor_table(rows: i64) -> (String, MysqlCleanup) {
    let name = unique_name("qa12my_inc");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB;"
    ))
    .expect("create mysql qa12 cursor table");
    if rows > 0 {
        let mut sql = format!("INSERT INTO {name} (id, updated_at) VALUES ");
        for i in 1..=rows {
            if i > 1 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, DATE_SUB(NOW(), INTERVAL {} MINUTE))",
                rows - i + 1
            ));
        }
        c.query_drop(sql).expect("seed mysql cursor rows");
    }
    (name.clone(), MysqlCleanup(name))
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_full_mode_repeated_run_accumulates_manifest_entries() {
    require_alive(LiveService::Mysql);
    let table = seed_mysql_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("qa12my_full");

    let yaml = Rig::mysql_batch(&export_name)
        .query(&format!(
            r#"SELECT id, name, amount FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line(r#"columns: { amount: "decimal(12,2)" }"#)
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let r1 = run_rivet_export(&cfg, &export_name);
    assert!(r1.status.success(), "first full run failed");

    // Sleep so each run produces a distinct timestamped filename (rivet uses
    // 1-second granularity in `<export>_<YYYYMMDD_HHMMSS>.parquet`).
    std::thread::sleep(std::time::Duration::from_millis(1100));

    let r2 = run_rivet_export(&cfg, &export_name);
    assert!(r2.status.success(), "second full run failed");

    let files = files_with_extension(out.path(), "parquet");
    assert_eq!(
        files.len(),
        2,
        "full mode must produce one file per run; got {files:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_incremental_second_run_on_unchanged_source_exports_zero_new_rows() {
    require_alive(LiveService::Mysql);

    let (table_name, _guard) = seed_cursor_table(15);
    let export_name = unique_name("qa12my_inc_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .query(&format!(r#"SELECT id, updated_at FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let r1 = run_rivet_export(&cfg, &export_name);
    assert!(
        r1.status.success(),
        "first incremental run failed; stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let files_after_first = files_with_extension(out.path(), "parquet").len();
    assert_eq!(files_after_first, 1, "first run must produce one file");

    let r2 = run_rivet_export(&cfg, &export_name);
    assert!(
        r2.status.success(),
        "second incremental run (no new rows) must still exit 0; stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let files_after_second = files_with_extension(out.path(), "parquet").len();
    assert_eq!(
        files_after_second, files_after_first,
        "incremental second run on unchanged source must not produce duplicates"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_incremental_third_run_picks_up_newly_inserted_rows() {
    require_alive(LiveService::Mysql);

    let (table_name, _guard) = seed_cursor_table(5);
    let export_name = unique_name("qa12my_inc2_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .query(&format!(r#"SELECT id, updated_at FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let files_1 = files_with_extension(out.path(), "parquet").len();

    // Insert new rows with higher updated_at than any existing row.
    let mut c = mysql_connect();
    c.query_drop(format!(
        "INSERT INTO {table_name} (id, updated_at)
         VALUES (6, NOW()), (7, NOW()), (8, NOW()), (9, NOW()), (10, NOW());"
    ))
    .expect("insert new mysql rows");

    std::thread::sleep(std::time::Duration::from_millis(1100));

    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let files_2 = files_with_extension(out.path(), "parquet").len();
    assert_eq!(
        files_2,
        files_1 + 1,
        "incremental must produce one additional file for new rows"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_chunked_resume_without_prior_run_fails_with_actionable_message() {
    require_alive(LiveService::Mysql);
    let table = seed_mysql_numeric_table(20);
    let export_name = unique_name("qa12my_chunk");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 5")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let out = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
        "--resume",
    ]);
    assert!(
        !out.status.success(),
        "--resume without prior in-progress run must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("resume") || stderr.contains("in-progress") || stderr.contains("chunk"),
        "stderr must explain the resume requirement; got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_chunked_resume_with_completed_run_gives_actionable_message() {
    require_alive(LiveService::Mysql);
    let table = seed_mysql_numeric_table(20);
    let export_name = unique_name("qa12my_resume_done");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 5")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let first_run = run_rivet_export(&cfg, &export_name);
    assert!(
        first_run.status.success(),
        "initial chunked run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first_run.stderr)
    );

    let resume_run = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
        "--resume",
    ]);
    assert!(
        !resume_run.status.success(),
        "--resume on a completed export must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&resume_run.stderr);
    assert!(
        stderr.contains("resume")
            || stderr.contains("in-progress")
            || stderr.contains("completed")
            || stderr.contains("chunk"),
        "stderr must tell the operator why resume failed; got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_full_mode_resume_flag_is_rejected() {
    require_alive(LiveService::Mysql);
    let table = seed_mysql_numeric_table(10);
    let export_name = unique_name("qa12my_full_resume");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let result = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
        "--resume",
    ]);
    // Plan validator emits a Warning for resume-no-checkpoint; the export
    // itself may succeed but the operator must be informed.
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("resume") || stderr.contains("checkpoint") || stderr.contains("warn"),
        "--resume on full-mode export must produce a diagnostic; stderr:\n{stderr}"
    );
}
