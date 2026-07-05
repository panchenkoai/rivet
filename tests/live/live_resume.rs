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

use crate::common::*;

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

    let yaml = Rig::pg_batch(&export_name)
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

    std::thread::sleep(std::time::Duration::from_millis(1100));

    let r2 = run_rivet_export(&cfg, &export_name);
    assert!(r2.status.success(), "second full run failed");

    let files = files_with_extension(out.path(), "parquet");
    assert_eq!(
        files.len(),
        2,
        "full mode must produce one file per run; got {files:?}"
    );
    // Re-read the destination (not rivet's file count): two full runs of 10 rows
    // each → 20 physical rows, distinct ids 0..10. Proves both re-exports
    // materialised every row, not just that two files appeared.
    assert_eq!(
        total_parquet_rows(out.path()),
        20,
        "two full runs must each materialise all 10 rows"
    );
    assert_eq!(
        dir_parquet_id_set(out.path()),
        (0..10).collect::<std::collections::BTreeSet<i64>>(),
        "full re-export must hold every source id (0..10)"
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
    let _guard = PgTable::adopt(table_name.clone());

    let export_name = unique_name("qa12_inc_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(r#"SELECT id, updated_at FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
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
    // Re-read the destination: the "picks up every row" claim must be backed by
    // the actual rows, not just one file appearing.
    assert_eq!(
        total_parquet_rows(out.path()),
        15,
        "first incremental run must export all 15 seeded rows"
    );
    assert_eq!(
        dir_parquet_id_set(out.path()),
        (1..=15).collect::<std::collections::BTreeSet<i64>>(),
        "first incremental run must contain ids 1..=15"
    );

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
    let _guard = PgTable::adopt(table_name.clone());

    let export_name = unique_name("qa12_inc2_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(r#"SELECT id, updated_at FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
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
    // Re-read the destination: after both runs the union must hold ids 1..=10
    // exactly — proves the second run picked up the newly-inserted rows 6..10
    // and the first run's rows are still present (no loss across runs).
    assert_eq!(
        dir_parquet_id_set(out.path()),
        (1..=10).collect::<std::collections::BTreeSet<i64>>(),
        "after the second run the destination must hold ids 1..=10 exactly"
    );
    assert_eq!(
        total_parquet_rows(out.path()),
        10,
        "two incremental runs must export each of the 10 rows exactly once"
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
    let yaml = Rig::pg_batch(&export_name)
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
#[ignore = "live: requires docker compose postgres"]
fn chunked_resume_with_completed_run_gives_actionable_message() {
    // After a chunked export completes normally, calling `--resume` should exit
    // non-zero with an actionable message.  A completed run is NOT the same as
    // an in-progress one — the operator must use `rivet run` (without --resume)
    // to start a fresh run.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let export_name = unique_name("qa12_resume_done");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
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

    // First: run the full export to completion.
    let first_run = run_rivet_export(&cfg, &export_name);
    assert!(
        first_run.status.success(),
        "initial chunked run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first_run.stderr)
    );

    // Now try to resume the completed export.  This should fail with an actionable
    // message rather than starting a silent fresh run or panicking.
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
#[ignore = "live: requires docker compose postgres"]
fn full_mode_resume_flag_is_rejected() {
    // `--resume` is only meaningful for chunked mode.  For full/incremental
    // exports, the flag must produce a diagnostic rather than silently ignoring it.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let export_name = unique_name("qa12_full_resume");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
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
    // The plan validator emits a Warning for resume-no-checkpoint; it should not
    // silently succeed as if resume had an effect.  The export itself may succeed
    // (the warning does not block execution), but the operator must be informed.
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("resume") || stderr.contains("checkpoint") || stderr.contains("warn"),
        "--resume on full-mode export must produce a diagnostic; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn chunked_resume_force_overrides_success_gate() {
    // ADR-0013 / ADR-0012 M8: `--resume` against a completed run (`_SUCCESS`
    // present) refuses; `--resume --force` overrides the gate and proceeds. The
    // refuse direction is `chunked_resume_with_completed_run_gives_actionable_message`;
    // this pins the override — the previously-untested direction of `--force`.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let export_name = unique_name("qa12_resume_force");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
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

    // Run to completion → `_SUCCESS` written, every chunk completed.
    let first = run_rivet_export(&cfg, &export_name);
    assert!(
        first.status.success(),
        "initial chunked run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first.stderr)
    );

    // Plain `--resume` is refused by the `_SUCCESS` gate (the prefix already
    // has a `_SUCCESS` marker from the completed run).
    let refused = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
        "--resume",
    ]);
    assert!(!refused.status.success(), "plain --resume must refuse");
    let refused_err = String::from_utf8_lossy(&refused.stderr);
    assert!(
        refused_err.contains("_SUCCESS"),
        "plain --resume must be refused *by the _SUCCESS gate*; stderr:\n{refused_err}"
    );

    // `--force` overrides that gate. We prove the override by the failure
    // *reason changing*: with `--force` the run gets PAST the `_SUCCESS` gate
    // (no `_SUCCESS` refusal) and only then hits the legitimate "nothing
    // in-progress to resume" state of a cleanly-completed run. (A success
    // outcome isn't reachable here precisely because a completed run has no
    // outstanding chunks — that is a different, correct refusal, not the gate.)
    let forced = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
        "--resume",
        "--force",
    ]);
    let forced_err = String::from_utf8_lossy(&forced.stderr);
    assert!(
        !forced_err.contains("_SUCCESS"),
        "--force must bypass the _SUCCESS gate (no _SUCCESS refusal expected); stderr:\n{forced_err}"
    );
    assert!(
        forced_err.contains("in-progress") || forced_err.contains("reset-chunks"),
        "--force should reach the real 'nothing to resume' state past the gate; stderr:\n{forced_err}"
    );
}

/// The manifest's extraction section must ship the cursor RANGE and prove
/// continuity across runs: run N+1's cursor_low equals run N's cursor_high
/// (a non-contiguous low would be a silently-skipped range). Ported from the
/// pip_db_replicator meta-catalog idea.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn incremental_manifest_ships_contiguous_cursor_range() {
    require_alive(LiveService::Postgres);
    let table_name = unique_name("qa12_curmeta");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, v INT NOT NULL);
         INSERT INTO {table_name} SELECT g, g*10 FROM generate_series(1,3) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table_name.clone());
    let export_name = unique_name("qa12_curmeta_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(r#"SELECT id, v FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: id")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    let read_ex = || -> serde_json::Value {
        let m: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(out.path().join("manifest.json")).unwrap(),
        )
        .unwrap();
        m["source"]["extraction"].clone()
    };

    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let e1 = read_ex();
    assert_eq!(e1["strategy"], "incremental");
    assert_eq!(e1["cursor_column"], "id");
    assert_eq!(e1["cursor_high"], "3", "run 1 covered up to id 3");

    c.batch_execute(&format!(
        "INSERT INTO {table_name} SELECT g, g*10 FROM generate_series(4,7) g;"
    ))
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let e2 = read_ex();
    assert_eq!(
        e2["cursor_low"], e1["cursor_high"],
        "continuity: run 2's low must equal run 1's high (no skipped range)"
    );
    assert_eq!(e2["cursor_high"], "7", "run 2 covered up to id 7");
}
