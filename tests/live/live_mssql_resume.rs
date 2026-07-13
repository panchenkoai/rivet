//! Idempotent resume across export modes — SQL Server twin of
//! `tests/live_resume.rs` / `tests/live_mysql_resume.rs`.
//!
//! Mirrors the QA-1.2 resume matrix against the MSSQL driver: the resume
//! semantics (manifest accumulation in full mode, cursor advance + zero-new-rows
//! in incremental mode, `--resume` flag validation in chunked mode) live in the
//! driver-agnostic pipeline orchestration layer, but the live contract still
//! must hold on SQL Server or "MSSQL incremental works the same way as PG/MySQL"
//! is just an aspiration.
//!
//! ## Test symmetry with `live_mysql_resume.rs`
//!
//! | MySQL twin                                                              | MSSQL twin (this file)                                                     |
//! |-------------------------------------------------------------------------|----------------------------------------------------------------------------|
//! | `mysql_full_mode_repeated_run_accumulates_manifest_entries`             | `mssql_full_mode_repeated_run_accumulates_manifest_entries`                |
//! | `mysql_incremental_second_run_on_unchanged_source_exports_zero_new_rows`| `mssql_incremental_second_run_on_unchanged_source_exports_zero_new_rows`   |
//! | `mysql_incremental_third_run_picks_up_newly_inserted_rows`              | `mssql_incremental_third_run_picks_up_newly_inserted_rows`                 |
//! | `mysql_chunked_resume_without_prior_run_fails_with_actionable_message`  | `mssql_chunked_resume_without_prior_run_fails_with_actionable_message`     |
//! | `mysql_chunked_resume_with_completed_run_gives_actionable_message`      | `mssql_chunked_resume_with_completed_run_gives_actionable_message`         |
//! | `mysql_full_mode_resume_flag_is_rejected`                               | `mssql_full_mode_resume_flag_is_rejected`                                  |

use crate::common::*;

fn cfg_dir_with(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

/// Local RAII guard for the ad-hoc `(id, updated_at)` cursor table.
struct MssqlCursorCleanup(String);
impl Drop for MssqlCursorCleanup {
    fn drop(&mut self) {
        mssql_drop_table(&self.0);
    }
}

/// Seed a `(id BIGINT PK, updated_at DATETIME2)` SQL Server table with N
/// monotonically-increasing rows (descending `updated_at` by id).
fn seed_cursor_table(rows: i64) -> (String, MssqlCursorCleanup) {
    let name = unique_name("qa12ms_inc");
    mssql_drop_table(&name);
    // DATETIME2(6) = microsecond, matching rivet's Timestamp(Micro) mapping.
    // Bare DATETIME2 defaults to 7 digits (100 ns) which rivet truncates to
    // microsecond, dropping the captured cursor below the source max so the
    // boundary row re-exports each run (known gap, docs/type-mapping.md).
    mssql_exec(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            updated_at DATETIME2(6) NOT NULL
        );"
    ));
    if rows > 0 {
        let mut sql = format!("INSERT INTO {name} (id, updated_at) VALUES ");
        for i in 1..=rows {
            if i > 1 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, DATEADD(MINUTE, -{}, SYSUTCDATETIME()))",
                rows - i + 1
            ));
        }
        mssql_exec(&sql);
    }
    (name.clone(), MssqlCursorCleanup(name))
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_full_mode_repeated_run_accumulates_manifest_entries() {
    require_alive(LiveService::Mssql);
    let table = seed_mssql_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("qa12ms_full");

    let yaml = Rig::mssql_batch(&export_name)
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

    // No sleep: `single.rs` stamps parts at millisecond granularity (`%3f`), so
    // two back-to-back sub-second runs still get distinct names — that they do
    // not clobber IS the run-uniqueness contract. A `sleep(1s)` here would only
    // document the second-granularity gap the `%3f` fix already closed.
    let r2 = run_rivet_export(&cfg, &export_name);
    assert!(r2.status.success(), "second full run failed");

    let files = files_with_extension(out.path(), "parquet");
    assert_eq!(
        files.len(),
        2,
        "full mode must produce one file per run; got {files:?}"
    );
    // Files-count alone is the weakest oracle: assert the payload (both runs'
    // rows survive) AND the dest manifest COPIES (`manifest-<run>.json`, what
    // reconcile / a warehouse autoloader read) — a parquet re-read passes even
    // when the manifest sidecar clobbers; the copies' summed row_count is RED
    // pre-fix (one clobbered manifest.json held only the last run's rows).
    assert_eq!(
        total_parquet_rows(out.path()),
        20,
        "two full runs of 10 rows must materialise 20 physical rows"
    );
    assert_eq!(
        dir_manifest_copy_total_rows(out.path()),
        20,
        "run-unique manifest copies must sum both runs' rows; a clobbered manifest is silent to a parquet re-read"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_incremental_second_run_on_unchanged_source_exports_zero_new_rows() {
    require_alive(LiveService::Mssql);

    let (table_name, _guard) = seed_cursor_table(15);
    let export_name = unique_name("qa12ms_inc_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mssql_batch(&export_name)
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
#[ignore = "live: requires docker compose mssql"]
fn mssql_incremental_third_run_picks_up_newly_inserted_rows() {
    require_alive(LiveService::Mssql);

    let (table_name, _guard) = seed_cursor_table(5);
    let export_name = unique_name("qa12ms_inc2_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mssql_batch(&export_name)
        .query(&format!(r#"SELECT id, updated_at FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let files_1 = files_with_extension(out.path(), "parquet").len();

    // Insert new rows with higher updated_at than any existing row.
    mssql_exec(&format!(
        "INSERT INTO {table_name} (id, updated_at)
         VALUES (6, SYSUTCDATETIME()), (7, SYSUTCDATETIME()), (8, SYSUTCDATETIME()),
                (9, SYSUTCDATETIME()), (10, SYSUTCDATETIME());"
    ));

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
#[ignore = "live: requires docker compose mssql"]
fn mssql_chunked_resume_without_prior_run_fails_with_actionable_message() {
    require_alive(LiveService::Mssql);
    let table = seed_mssql_numeric_table(20);
    let export_name = unique_name("qa12ms_chunk");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mssql_batch(&export_name)
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
#[ignore = "live: requires docker compose mssql"]
fn mssql_chunked_resume_with_completed_run_gives_actionable_message() {
    require_alive(LiveService::Mssql);
    let table = seed_mssql_numeric_table(20);
    let export_name = unique_name("qa12ms_resume_done");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mssql_batch(&export_name)
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
#[ignore = "live: requires docker compose mssql"]
fn mssql_full_mode_resume_flag_is_rejected() {
    require_alive(LiveService::Mssql);
    let table = seed_mssql_numeric_table(10);
    let export_name = unique_name("qa12ms_full_resume");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::mssql_batch(&export_name)
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
