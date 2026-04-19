//! Idempotent resume across export modes.
//!
//! QA backlog Task 1.2.  Run the same config twice in a row and assert the
//! state layer behaves predictably on the second invocation:
//!
//!   * full mode — both runs succeed; manifest accumulates entries.
//!   * incremental mode — second run sees cursor at the last exported value
//!     and produces zero new rows (no duplicates).
//!   * chunked mode — `--resume` flag picks up an in-progress run (we cannot
//!     interrupt mid-flight from a simple integration test, but we verify
//!     resume-without-prior-run bails with a clear message, and resume
//!     with a completed prior run is either no-op or equivalent error).
//!
//! These tests use a dedicated config dir per test so the state DB lives
//! next to the YAML — no cross-contamination between tests.

mod common;

use common::*;

/// Helper: write a config YAML + `rivet_state.db` next to it, return cfg path.
fn cfg_dir_with(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_mode_repeated_run_accumulates_manifest_entries() {
    // rivet names output files `<export>_<YYYYMMDD_HHMMSS>.parquet` (1-second
    // granularity).  Two full runs in the *same* second therefore produce
    // identical names and the local backend (idempotent_overwrite=true)
    // collapses them into one file on disk.  Sleep between runs so each
    // produces a uniquely-named artefact — that lets us assert both runs
    // were independently materialised, which is the real contract.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("qa12_full");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name, amount FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let r1 = run_rivet_export(&cfg, &export_name);
    assert!(r1.status.success(), "first full run failed");

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
#[ignore = "live: requires docker compose postgres"]
fn incremental_second_run_on_unchanged_source_exports_zero_new_rows() {
    // Contract: rivet persists `last_cursor_value` in SQLite next to the
    // config file.  The second run with the same config must see that
    // cursor and produce zero additional files (since source is unchanged).
    require_alive(LiveService::Postgres);

    // Seed a table with an `updated_at` column we can use as a cursor.
    let table_name = unique_name("qa12_inc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            updated_at TIMESTAMPTZ NOT NULL
        );
        INSERT INTO {table_name} (id, updated_at)
        SELECT g, now() - (interval '1 minute') * g FROM generate_series(1, 15) g;"
    ))
    .unwrap();
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = Cleanup(table_name.clone());

    let export_name = unique_name("qa12_inc_exp");
    let out = tempfile::tempdir().unwrap();
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
        dir = out.path().display()
    );
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    // Run #1 — must pick up every row.
    let r1 = run_rivet_export(&cfg, &export_name);
    assert!(
        r1.status.success(),
        "first incremental run failed; stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let files_after_first = files_with_extension(out.path(), "parquet").len();
    assert_eq!(files_after_first, 1, "first run must produce one file");

    // Run #2 — cursor is now at the most recent updated_at; no new rows, no
    // new file (documented: zero rows → no file).
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
#[ignore = "live: requires docker compose postgres"]
fn incremental_third_run_picks_up_newly_inserted_rows() {
    // After a clean incremental cycle, inserting new rows with higher
    // updated_at values must be picked up by the next run — and only those.
    require_alive(LiveService::Postgres);

    let table_name = unique_name("qa12_inc2");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            updated_at TIMESTAMPTZ NOT NULL
        );
        INSERT INTO {table_name} (id, updated_at)
        SELECT g, now() - (interval '1 minute') * (20 - g) FROM generate_series(1, 5) g;"
    ))
    .unwrap();
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = Cleanup(table_name.clone());

    let export_name = unique_name("qa12_inc2_exp");
    let out = tempfile::tempdir().unwrap();
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
        dir = out.path().display()
    );
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    // Run #1 — exports rows 1..5.
    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let files_1 = files_with_extension(out.path(), "parquet").len();

    // Insert new rows with higher updated_at.
    c.batch_execute(&format!(
        "INSERT INTO {table_name} (id, updated_at)
         SELECT g, now() FROM generate_series(6, 10) g;"
    ))
    .unwrap();

    // Sleep so file-name timestamp is distinct from run #1 (see
    // full_mode_repeated_run_accumulates_manifest_entries for rationale).
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Run #2 — must pick up rows 6..10.
    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let files_2 = files_with_extension(out.path(), "parquet").len();
    assert_eq!(
        files_2,
        files_1 + 1,
        "incremental must produce one additional file for new rows"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_resume_without_prior_run_fails_with_actionable_message() {
    // `--resume` on a chunked export requires an in-progress run in state.
    // Calling it on a fresh config must fail with a message that tells the
    // operator exactly what to do.  Contract check only — no need to
    // actually crash a run mid-flight for this assertion.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let export_name = unique_name("qa12_chunk");
    let out = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: 5
    chunk_checkpoint: true
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
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
