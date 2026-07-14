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
fn incremental_export_with_unwritable_state_db_fails_loud_not_silent() {
    // A state-store write failure must fail the run LOUDLY — never a silent
    // success that skips persisting the cursor (that would break at-least-once:
    // the operator believes the cursor advanced when it did not). rivet writes
    // rivet_state.db next to the config; a read-only config dir makes the store
    // unwritable. The state store is engine-agnostic (SQLite), so this is pinned
    // once on Postgres.
    require_alive(LiveService::Postgres);
    let table_name = unique_name("state_wf");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL); \
         INSERT INTO {table_name} \
           SELECT g, now() - (interval '1 min') * g FROM generate_series(1,5) g"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table_name.clone());

    let export_name = unique_name("state_wf_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id, updated_at FROM {table_name}"))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let cfgdir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfgdir, &yaml);

    // Make the state-DB directory unwritable — rivet can neither create nor write
    // rivet_state.db, and must surface that loudly, not swallow it.
    let mut ro = std::fs::metadata(cfgdir.path()).unwrap().permissions();
    ro.set_readonly(true);
    std::fs::set_permissions(cfgdir.path(), ro).unwrap();

    let res = run_rivet_export(&cfg, &export_name);

    // Restore write perms BEFORE asserting so TempDir cleanup always succeeds.
    use std::os::unix::fs::PermissionsExt;
    let mut rw = std::fs::metadata(cfgdir.path()).unwrap().permissions();
    rw.set_mode(0o755);
    std::fs::set_permissions(cfgdir.path(), rw).unwrap();

    assert!(
        !res.status.success(),
        "an unwritable state store must fail the run loudly, not silently succeed"
    );
    let stderr = String::from_utf8_lossy(&res.stderr).to_lowercase();
    assert!(
        stderr.contains("state")
            || stderr.contains("readonly")
            || stderr.contains("read-only")
            || stderr.contains("permission")
            || stderr.contains("denied"),
        "the error must name the state store / the write failure:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_mode_repeated_run_accumulates_manifest_entries() {
    // rivet names output files `<export>_<YYYYMMDD_HHMMSS_mmm>.parquet` with
    // MILLISECOND granularity, so two back-to-back full runs get distinct names
    // and neither clobbers the other (LocalDestination idempotent_overwrite would
    // otherwise collapse a same-name pair). No sleep needed — that this passes
    // rapid-fire is the run-uniqueness contract (regressed for months at
    // second-granularity; see roast_rapid_incremental_runs_...).
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
    // The parquet re-read above passes even if the manifest sidecar clobbered.
    // Assert the dest manifest COPIES (`manifest-<run>.json`, what reconcile / a
    // warehouse autoloader read) sum BOTH runs: pre-fix a shared prefix held one
    // clobbered `manifest.json` (last run's rows only) → RED. This is the oracle
    // the cell's name ("accumulates manifest entries") demands — and it is
    // fix-sensitive, unlike the `file_log` ledger which accumulates regardless.
    assert_eq!(
        dir_manifest_copy_total_rows(out.path()),
        20,
        "run-unique manifest copies must sum both runs' rows; a clobbered manifest is silent to the parquet re-read"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn roast_rapid_incremental_runs_into_same_prefix_must_not_clobber_prior_parts() {
    // Run-uniqueness for the batch incremental path (the src/pipeline/single.rs
    // part-name stamp). Several incremental runs land in the same wall-clock
    // SECOND; each exports a different delta (one new row). If the stamp is
    // second-granularity the later run's file OVERWRITES the earlier's
    // (LocalDestination idempotent_overwrite) and the earlier delta's rows are
    // silently LOST — the CDC/keyset/mongo_parallel paths already use a
    // millisecond stamp to prevent exactly this. N rapid runs cannot each occupy
    // a distinct second, so a second-granularity stamp is guaranteed to lose at
    // least one delta. Assert the union of every run's rows survives.
    require_alive(LiveService::Postgres);
    const N: i64 = 6;
    let table_name = unique_name("clobber_inc");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL)"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table_name.clone());

    let export_name = unique_name("clobber_inc_exp");
    let out = tempfile::tempdir().unwrap();
    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(r#"SELECT id, updated_at FROM {table_name}"#))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfg) = cfg_dir_with(&yaml);

    // Each run inserts one new row (strictly increasing cursor) then exports just
    // that delta — back to back, no sleep, so multiple runs share a second.
    for k in 0..N {
        c.execute(
            &format!(
                "INSERT INTO {table_name} (id, updated_at) \
                 VALUES ({k}, now() + interval '{k} seconds')"
            ),
            &[],
        )
        .unwrap();
        let r = run_rivet_export(&cfg, &export_name);
        assert!(
            r.status.success(),
            "incremental run {k} failed:\n{}",
            String::from_utf8_lossy(&r.stderr)
        );
    }

    // The union of all deltas must be every id — no run's part was clobbered.
    assert_eq!(
        dir_parquet_id_set(out.path()),
        (0..N).collect::<std::collections::BTreeSet<i64>>(),
        "every incremental delta must survive; a clobbered part is silent data loss"
    );
    // Data survival above is a proxy: a manifest clobber leaves the parquet but
    // loses the sidecar a warehouse autoloader reads. Sum the dest manifest
    // copies — N rapid runs, 1 row each → N. Pre-fix the shared prefix held one
    // clobbered manifest → RED.
    assert_eq!(
        dir_manifest_copy_total_rows(out.path()),
        N,
        "run-unique manifest copies must sum every rapid run's row; a clobbered manifest is silent to the parquet re-read"
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
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

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
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).
    assert!(run_rivet_export(&cfg, &export_name).status.success());
    let e2 = read_ex();
    assert_eq!(
        e2["cursor_low"], e1["cursor_high"],
        "continuity: run 2's low must equal run 1's high (no skipped range)"
    );
    assert_eq!(e2["cursor_high"], "7", "run 2 covered up to id 7");
}
