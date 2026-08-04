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
//! | C1 | Sequential: crash after chunk 0 complete → resume | Chunk 0 not re-run; total rows == seeded |
//! | C2 | Sequential: crash after chunk 0 file (before commit) → resume | Chunk 0 re-runs (at-least-once); manifest grows |
//! | C3 | Parallel (4 workers): panic after chunk 0 complete → resume | chunk_run not 'completed' until resume finalizes; manifest rows == seeded; no chunk re-recorded |
//! | C4 | Parallel (1 worker → resume with 2): panic at `after_chunk_file:0` leaves chunk 0 'running' → resume | `reset_stale_running_chunk_tasks` recovers; chunk 0 at-least-once; resume worker count may differ from crash worker count |
//!
//! ## Related
//!
//! F5 in `tests/recovery.rs` covers the state-layer invariant (completed chunk
//! data preserved after `reset_stale_running_chunk_tasks`) without a live DB.
//!
//! The parallel checkpoint path injects panic hooks at the same fault-point
//! names as the sequential one (`after_chunk_file:N`, `after_chunk_complete:N`,
//! see `src/pipeline/chunked/mod.rs` and `src/test_hook.rs`), so C3 exercises
//! recovery semantics across both paths with a single matrix.

use crate::common::*;

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

/// Every `run_status` row still marked `running`, newest first — the liveness
/// signal `gc_orphans` and the load's consumed-set both read.
fn running_run_status_rows(cfg: &std::path::Path) -> Vec<String> {
    let db = open_state_db(cfg);
    let mut st = db
        .prepare("SELECT run_id FROM run_status WHERE status = 'running' ORDER BY started_at DESC")
        .expect("run_status must exist");
    st.query_map([], |r| r.get::<_, String>(0))
        .expect("query run_status")
        .filter_map(|r| r.ok())
        .collect()
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

/// Physical row total and distinct `id` count across every Parquet part at the
/// destination — an EXTERNAL oracle that re-reads the files rather than trusting
/// rivet's own `file_log`. `physical` pins the at-least-once duplication count
/// against what the manifest claims (catching a nonce regression → same-second
/// overwrite, or an additive-within-chunk commit, that the file_log assertions
/// alone would wave through); `distinct` proves the de-duplicated logical total.
fn parquet_physical_and_distinct_ids(dir: &std::path::Path) -> (i64, i64) {
    (
        total_parquet_rows(dir) as i64,
        dir_parquet_id_set(dir).len() as i64,
    )
}

/// `validated` flag from the latest `export_metrics` row (NULL → None).
fn latest_metric_validated(cfg: &std::path::Path, export: &str) -> Option<bool> {
    open_state_db(cfg)
        .query_row(
            "SELECT validated FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT 1",
            [export],
            |r| r.get::<_, Option<bool>>(0),
        )
        .ok()
        .flatten()
}

/// `total_rows` from the latest `export_metrics` row — the summary's reported row
/// count (finding #9). On a checkpoint RESUME this must be the CUMULATIVE total
/// (rehydrated pre-crash base + this run), not just this invocation's rows.
fn latest_metric_total_rows(cfg: &std::path::Path, export: &str) -> Option<i64> {
    open_state_db(cfg)
        .query_row(
            "SELECT total_rows FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT 1",
            [export],
            |r| r.get::<_, i64>(0),
        )
        .ok()
}

/// The destination manifest's `column_checksums` (Form B), or `None` when the
/// key is absent/null (suppressed). Empty array also reads as "no Form B".
fn manifest_column_checksums(dir: &std::path::Path) -> Option<Vec<serde_json::Value>> {
    let raw = std::fs::read_to_string(dir.join("manifest.json")).ok()?;
    let json: serde_json::Value = serde_json::from_str(&raw).ok()?;
    match json.get("column_checksums") {
        Some(serde_json::Value::Array(a)) if !a.is_empty() => Some(a.clone()),
        _ => None,
    }
}

// ─── C1: crash after chunk 0 complete → resume skips chunk 0 ─────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_crash_after_first_chunk_complete_resume_finishes_export() {
    // Crash after `complete_chunk_task` marks chunk 0 as done, before the loop
    // advances to chunk 1.  On resume, chunk 0 must NOT re-run; total row count
    // must equal the seeded count with no duplicates.
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table = seed_pg_numeric_table(150);
    let export = unique_name("c1_crash_complete");
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .duckdb_oracle();
    let cfg = rig.config_path();

    // ── Crash run ────────────────────────────────────────────────────────────
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")],
    );
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
    let resume = rig.run_args(&["--export", &export, "--resume"]);
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // ── Final state ──────────────────────────────────────────────────────────
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
    // The DESTINATION is the oracle for rows. `manifest_total_rows` reads rivet's
    // own ledger, so a run that miscounts agrees with itself; and a re-read
    // through the parquet crate rivet writes with shares its codec. DuckDB shares
    // neither. Distinct rides along because "no duplicates" is half the claim
    // this test's own comment makes.
    rig.assert_complete(
        "id",
        150,
        "chunked crash+resume must deliver every seeded row exactly once",
    );
}

/// Round-4: the DESTINATION manifest.json (the manifest-authoritative `rivet load`
/// view) must declare EVERY committed chunk after a crash+resume — not just the
/// resume-processed ones. The sibling test above asserts on file_log + a parquet
/// glob, which both retain chunk 0 and so MASK the orphan; this reads the actual
/// destination manifest.json. RED before rehydrate_manifest_parts_from_completed_
/// chunks (the resume manifest omitted chunk 0 → a silent ~33% loss under load).
#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_crash_resume_writes_a_complete_destination_manifest() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(150);
    let export = unique_name("c1_manifest_complete");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());

    // Crash after chunk 0 commits (before any destination manifest is written).
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")],
    );
    assert!(!crash.status.success(), "crash run must exit non-zero");

    // Resume completes.
    let resume = rig.run_args(&["--export", &export, "--resume"]);
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // MANIFEST-DRIVEN assertion: the destination manifest.json must declare all 3
    // chunks / 150 rows — chunk 0 must not be orphaned from the loader's view.
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.path().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    assert_eq!(
        m["row_count"].as_i64(),
        Some(150),
        "destination manifest must declare all 150 rows (chunk 0 not orphaned); got {}",
        m["row_count"]
    );
    assert_eq!(
        m["part_count"].as_u64(),
        Some(3),
        "destination manifest must declare all 3 chunk parts; got {}",
        m["part_count"]
    );
    // And the physical parts on disk match the manifest's declared count.
    assert_eq!(
        files_with_extension(out.path(), "parquet").len(),
        3,
        "3 physical parquet parts"
    );
}

// ─── C2: crash after chunk 0 file (before commit) → resume re-runs chunk 0 ───

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn chunk_checkpoint_resume_closes_the_ledger_row_it_opened() {
    // A chunk-checkpoint RESUME adopts the prior run's id: `ledger_begin_run`
    // opens a `running` row under the FRESH id, then chunked/mod.rs rewrites
    // `summary.run_id` to the adopted one, and `finish_run` is a bare UPDATE —
    // so the close matched no row and the fresh id stayed `running` forever.
    //
    // Not cosmetic. `has_active_run_on_prefix` keeps answering true, so
    // `gc_orphans` defers cleanup indefinitely, and since the load now excludes
    // ACTIVE runs from its consumed set, every later load on that prefix
    // re-appends instead of recording progress — until some unrelated newer run
    // of the same export happens to supersede the leak.
    //
    // Observes the LEDGER after the resume, not the id-handling logic: the leak
    // is exactly a value one layer hands another, which a test that supplies its
    // own ids cannot see.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(200);
    let export = unique_name("ckpt_ledger");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    // Crash mid-chunk so the next run RESUMES (and therefore adopts the id).
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_file:0")],
    );
    assert!(!crash.status.success(), "the crash run must exit non-zero");
    // A hard panic leaves its own row running — that is the crash, not the leak.
    let after_crash = running_run_status_rows(&cfg);

    // Clean resume: it adopts the prior run_id, so the row opened for THIS run
    // is the one at risk.
    let ok = rig.run_args(&["--export", &export, "--resume"]);
    assert!(
        ok.status.success(),
        "the resume run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&ok.stderr)
    );

    // A SUCCESSFUL run leaves nothing running — that is the whole contract, and
    // it is what a row count cannot express: the resume ADOPTS the crashed run's
    // id, so "one row before, one row after" holds whether the leaked row is the
    // crashed run (fine, it gets closed) or the fresh one (the leak). Counting
    // passed against the unfixed product; asserting EMPTY is what bites.
    let after_resume = running_run_status_rows(&cfg);
    assert!(
        after_resume.is_empty(),
        "a completed resume must leave NO `running` row — the run opened one under \
         a fresh id, then adopted the prior one, and `finish_run` is a bare UPDATE, \
         so the row it opened stayed running forever (blocking gc and, since the \
         load excludes active runs from its consumed set, every later load on this \
         prefix). before: {after_crash:?}, still running: {after_resume:?}"
    );
}

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
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    // ── Crash run ────────────────────────────────────────────────────────────
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_file:0")],
    );
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

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    // ── Resume run ────────────────────────────────────────────────────────────
    let resume = rig.run_args(&["--export", &export, "--resume"]);
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

    // Re-read the DESTINATION (not file_log): the 1.1s sleep gives chunk 0's two
    // writes distinct filenames, so the physical row total must equal what the
    // manifest claims (200, chunk 0 counted twice), and the DISTINCT id set must
    // equal the de-duplicated source (150). Together this proves the orphan is
    // the only surplus and a manifest-aware reader recovers exactly-once — the
    // file_log↔Parquet coherence the file_log-only assertions above cannot see.
    let mtr = manifest_total_rows(&cfg, &export);
    let (physical, distinct) = parquet_physical_and_distinct_ids(out.path());
    assert_eq!(
        physical, mtr,
        "destination physical rows ({physical}) must equal manifest_total_rows ({mtr}) — \
         file_log claims rows that must physically exist at the destination"
    );
    assert_eq!(
        distinct, 150,
        "150 distinct source ids must survive de-duplication (got {distinct}) — a hole here is row LOSS"
    );
}

// ─── Helpers shared with C3 ──────────────────────────────────────────────────

/// Count chunk_task rows by status for a given run.  Returns
/// `(completed, running, pending, failed)`.
///
/// Splitting the four counts into one query keeps the post-crash snapshot
/// race-free (a single SQLite transaction sees a consistent view) and avoids
/// rebinding the same `?1` across four separate `query_row` calls.
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

// ─── C3: parallel checkpoint — panic during worker → clean resume ────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_chunked_crash_after_chunk_complete_resume_finishes_with_no_duplicates() {
    // Parallel-checkpoint variant of C1: same `after_chunk_complete:0` fault
    // point, but the run uses `parallel: 4` so the panic fires from a worker
    // thread inside `std::thread::scope`.
    //
    // Important semantic note: `std::thread::scope` defers panic propagation
    // until the scope joins, so the other workers continue until they exit
    // their work loop naturally.  In practice this means by the time the
    // process actually aborts, the surviving workers will usually have drained
    // every remaining chunk — the pre-crash distribution of statuses is
    // therefore *non-deterministic*.  We assert only what we can guarantee:
    //
    //   * Process exits non-zero (the panic was re-raised after join).
    //   * Chunk 0 is 'completed' (the panic fires AFTER complete_chunk_task).
    //   * `chunk_run` is NOT 'completed' (finalize_chunk_run_completed never
    //     ran), so the resume code path actually has work to do — even if
    //     every individual chunk_task happens to be done.
    //
    // The decisive invariant lives on the *resume* side: the resume must
    // finalize the run and produce a manifest whose row total exactly equals
    // the seeded count, with no double-counted completed chunks.
    require_alive(LiveService::Postgres);

    const ROW_COUNT: i64 = 400;
    const CHUNK_SIZE: i64 = 50;
    const EXPECTED_CHUNKS: i64 = ROW_COUNT / CHUNK_SIZE;

    let table = seed_pg_numeric_table(ROW_COUNT);
    let export = unique_name("c3_parallel_crash_complete");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!(r#"chunk_size: {CHUNK_SIZE}"#))
        .export_line("chunk_checkpoint: true")
        .export_line("parallel: 4")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    // ── Crash run ────────────────────────────────────────────────────────────
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")],
    );
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero (worker panic propagated through scope); stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    // ── Post-crash snapshot: what we *can* guarantee ─────────────────────────
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

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    // ── Resume run ────────────────────────────────────────────────────────────
    let resume = rig.run_args(&["--export", &export, "--resume"]);
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // ── Final invariants — these are the load-bearing ones ───────────────────
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

    // The decisive invariant: already-completed chunks (including chunk 0)
    // are NOT re-run on resume.  Total manifest rows == seeded count, and
    // manifest entry count == EXPECTED_CHUNKS exactly.
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

    let parquet_files = files_with_extension(out.path(), "parquet");
    assert!(
        parquet_files.len() >= EXPECTED_CHUNKS as usize,
        "at least {EXPECTED_CHUNKS} parquet files must exist; found {}: {parquet_files:?}",
        parquet_files.len()
    );
}

// ─── Finding #9: parallel-checkpoint resume reports the CUMULATIVE row total ──

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_chunked_resume_reports_cumulative_total_rows_not_just_this_run() {
    // A parallel-checkpoint crash before finalize leaves the manifest MISSING, so
    // resume rehydrates the pre-crash parts from file_log — bumping total_rows,
    // files, bytes, and manifest_parts together. The parallel runner then landed
    // its worker rows with `summary.total_rows = agg` (assign), CLOBBERING the
    // rehydrated base, so the reported total under-counted (only this run's rows,
    // often 0 when the surviving workers had already drained every chunk). The
    // manifest's own row_count (sum of parts) stayed correct — this is the SUMMARY
    // total, recorded to export_metrics. The fix accumulates onto the base.
    require_alive(LiveService::Postgres);

    const ROW_COUNT: i64 = 400;
    const CHUNK_SIZE: i64 = 50;

    let table = seed_pg_numeric_table(ROW_COUNT);
    let export = unique_name("f9_parallel_total_rows");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!("chunk_size: {CHUNK_SIZE}"))
        .export_line("chunk_checkpoint: true")
        .export_line("parallel: 4")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")],
    );
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    let resume = rig.run_args(&["--export", &export, "--resume"]);
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // The manifest (file_log sum) is correct regardless of the bug — the pin.
    assert_eq!(
        manifest_total_rows(&cfg, &export),
        ROW_COUNT,
        "manifest row total (file_log sum) must equal the seeded count"
    );
    // The SUMMARY's reported total_rows must ALSO be the cumulative total. RED
    // before the fix: the clobber reported only this invocation's re-exported rows.
    assert_eq!(
        latest_metric_total_rows(&cfg, &export),
        Some(ROW_COUNT),
        "resume must report the CUMULATIVE row total (rehydrated base + this run), \
         not just the rows re-exported this invocation"
    );
}

// ─── Finding #10: a rehydrating resume SUPPRESSES Form B so validate can't lie ─

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_checkpoint_resume_suppresses_form_b_so_validate_does_not_false_fail() {
    // On a checkpoint resume the manifest is rebuilt to list ALL parts, but the
    // rehydrated pre-crash parts carry no per-column checksum (file_log stores
    // none). The run-wide Form B XOR this run harvests would then cover only the
    // re-exported parts while the manifest lists every part — a partial XOR that
    // `rivet validate` (Full) re-reads and reports as a FALSE mismatch. The fix
    // suppresses Form B entirely on such a resume (absent, not partial), so
    // validate size-verifies the parts and skips only the value re-read.
    require_alive(LiveService::Postgres);

    const ROW_COUNT: i64 = 400;
    const CHUNK_SIZE: i64 = 50;

    // Both source tables are created HERE so their drop guards outlive the runs
    // (a table created inside the closure would drop — and DROP the table — before
    // the export executes).
    let base_table = seed_pg_numeric_table(ROW_COUNT);
    let res_table = seed_pg_numeric_table(ROW_COUNT);
    // `parallel: 1` for the resume path is deliberate: with 4 workers + a deferred
    // scope-panic the surviving workers drain EVERY chunk before the abort, so the
    // resume re-exports nothing and the harvest accumulator is empty — the
    // suppression path (which only matters when SOME parts re-export while others
    // rehydrate) never fires. One worker completing only chunk 0 leaves the rest
    // pending, so resume rehydrates chunk 0 (no checksum) AND re-exports the rest
    // (with checksums) — the exact mixed case Form B suppression guards.
    let run_checkpoint = |export: &str,
                          table_name: &str,
                          out: &std::path::Path,
                          cfg_dir: &tempfile::TempDir,
                          parallel: u32| {
        // Caller-owned dir: a Rig owns its tempdir, so returning rig.config_path()
        // from this closure would drop the rig and delete the config. Keep yaml().
        let yaml = Rig::pg_batch(export)
            .query(&format!("SELECT id, name FROM {table_name}"))
            .mode("chunked")
            .export_line("chunk_column: id")
            .export_line(&format!("chunk_size: {CHUNK_SIZE}"))
            .export_line("chunk_checkpoint: true")
            .export_line(&format!("parallel: {parallel}"))
            .dest_path(out.to_path_buf())
            .yaml();
        write_config(cfg_dir, &yaml)
    };
    let validate = |cfg: &std::path::Path, export: &str| {
        run_rivet_env(
            &[
                "validate",
                "--config",
                cfg.to_str().unwrap(),
                "--export",
                export,
            ],
            &[],
        )
    };

    // ── Baseline: a CLEAN parallel-checkpoint run RECORDS Form B, and validate
    //    (Full) re-reads + verifies it → passes. Proves Form B is active on this
    //    path, so the suppression below is not vacuous.
    let base_export = unique_name("f10_baseline");
    let base_out = tempfile::tempdir().unwrap();
    let base_cfg_dir = tempfile::tempdir().unwrap();
    let base_cfg = run_checkpoint(
        &base_export,
        base_table.name(),
        base_out.path(),
        &base_cfg_dir,
        4,
    );
    let base_run = run_rivet_env(
        &[
            "run",
            "--config",
            base_cfg.to_str().unwrap(),
            "--export",
            &base_export,
        ],
        &[],
    );
    assert!(
        base_run.status.success(),
        "baseline run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&base_run.stderr)
    );
    assert!(
        manifest_column_checksums(base_out.path()).is_some(),
        "a clean parallel-checkpoint run must record Form B column_checksums"
    );
    let base_v = validate(&base_cfg, &base_export);
    assert!(
        base_v.status.success(),
        "clean-run validate (Full) must pass; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&base_v.stdout),
        String::from_utf8_lossy(&base_v.stderr)
    );

    // ── Resume: crash before finalize (manifest missing) → resume rehydrates.
    let res_export = unique_name("f10_resume");
    let res_out = tempfile::tempdir().unwrap();
    let res_cfg_dir = tempfile::tempdir().unwrap();
    let res_cfg = run_checkpoint(
        &res_export,
        res_table.name(),
        res_out.path(),
        &res_cfg_dir,
        1,
    );
    let crash = run_rivet_env(
        &[
            "run",
            "--config",
            res_cfg.to_str().unwrap(),
            "--export",
            &res_export,
        ],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")],
    );
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );
    let resume = run_rivet_env(
        &[
            "run",
            "--config",
            res_cfg.to_str().unwrap(),
            "--export",
            &res_export,
            "--resume",
        ],
        &[],
    );
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );
    // Form B suppressed: the manifest lists every part but records NO Form B,
    // because the rehydrated pre-crash parts have no per-column checksum.
    assert!(
        manifest_column_checksums(res_out.path()).is_none(),
        "a rehydrating resume must SUPPRESS Form B (absent), never record a partial XOR"
    );
    // validate (Full): with Form B absent it size-verifies + skips the value
    // re-read → PASSES. RED before the fix: the partial XOR made validate FALSE-fail.
    let res_v = validate(&res_cfg, &res_export);
    assert!(
        res_v.status.success(),
        "validate must PASS on a correctly-resumed run — no false Form B mismatch; \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&res_v.stdout),
        String::from_utf8_lossy(&res_v.stderr)
    );
}

// ─── C4: parallel checkpoint — stuck-running recovery via at-least-once ──────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_chunked_crash_after_chunk_file_stuck_running_resume_reruns_chunk() {
    // Parallel-checkpoint variant of C2: panic at `after_chunk_file:0` leaves
    // chunk 0 in 'running' status (the panic fires AFTER record_file but
    // BEFORE complete_chunk_task).  We use `parallel: 1` so this scenario is
    // *deterministic* — with N workers, surviving workers might race past the
    // pending tasks and finish them before the scope re-raises the panic, and
    // the test's value comes from the stuck-running recovery, not from
    // multi-worker concurrency.  Multi-worker concurrency is covered by C3.
    //
    // What we're actually testing in this code path:
    //
    //   * The parallel worker loop runs even when `parallel: 1` (no
    //     accidental fall-back to the sequential checkpoint code path).
    //   * `reset_stale_running_chunk_tasks` correctly recovers the 'running'
    //     chunk 0 left behind by the panic.
    //   * At-least-once semantics: chunk 0's pre-crash manifest entry is
    //     preserved AND the resumed write produces a fresh manifest entry, so
    //     row totals grow by exactly one chunk's worth of rows.
    require_alive(LiveService::Postgres);

    const ROW_COUNT: i64 = 150;
    const CHUNK_SIZE: i64 = 50;
    const EXPECTED_CHUNKS: i64 = ROW_COUNT / CHUNK_SIZE;

    let table = seed_pg_numeric_table(ROW_COUNT);
    let export = unique_name("c4_parallel_stuck_running");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!(r#"chunk_size: {CHUNK_SIZE}"#))
        .export_line("chunk_checkpoint: true")
        .export_line("parallel: 1")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    // ── Crash run ────────────────────────────────────────────────────────────
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_chunk_file:0")],
    );
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    // ── Post-crash: chunk 0 stuck in 'running', manifest already has its row ─
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

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    // ── Resume with parallel: 2 (exercises both reset path and parallel workers)
    //
    // Rewrite the config to use parallel: 2 on resume; this proves that
    // reset_stale_running_chunk_tasks works regardless of whether the resume
    // worker count matches the crash worker count.
    // yaml() borrows, so the variant config comes from the rig itself.
    let yaml_resume = rig.yaml().replace("parallel: 1", "parallel: 2");
    std::fs::write(&cfg, yaml_resume).expect("rewrite config for resume");

    let resume = rig.run_args(&["--export", &export, "--resume"]);
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // ── Final state ──────────────────────────────────────────────────────────
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
    assert_eq!(running_post, 0, "no chunk_task should remain 'running'");
    assert_eq!(pending_post, 0, "no chunk_task should remain 'pending'");
    assert_eq!(failed_post, 0, "no chunk_task should be 'failed'");

    // At-least-once for chunk 0: pre-crash manifest entry + post-resume entry
    // for the same chunk index, plus one entry each for chunks 1 and 2.
    //
    // Total manifest entries = 2 + 1 + 1 = 4.
    // Total rows = 50 (chunk 0 first run) + 50 (chunk 0 retry) + 50 + 50 = 200.
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

    let parquet_files = files_with_extension(out.path(), "parquet");
    assert!(
        parquet_files.len() >= EXPECTED_CHUNKS as usize,
        "at least {EXPECTED_CHUNKS} parquet files must exist; found {}: {parquet_files:?}",
        parquet_files.len()
    );

    // Re-read the DESTINATION (not file_log): physical rows must equal what the
    // manifest claims (ROW_COUNT + one re-run chunk), and DISTINCT ids must equal
    // the de-duplicated source — proving the orphan is the only surplus and a
    // manifest-aware reader recovers exactly-once (file_log↔Parquet coherence).
    let mtr = manifest_total_rows(&cfg, &export);
    let (physical, distinct) = parquet_physical_and_distinct_ids(out.path());
    assert_eq!(
        physical, mtr,
        "destination physical rows ({physical}) must equal manifest_total_rows ({mtr})"
    );
    assert_eq!(
        distinct, ROW_COUNT,
        "{ROW_COUNT} distinct source ids must survive de-duplication (got {distinct}) — a hole is row LOSS"
    );
}

// ─── C5: clean sequential checkpoint run records `validated` ──────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_sequential_checkpoint_validate_records_validated_metric() {
    // Regression: the sequential checkpoint path ran `validate_output` but never
    // set `summary.validated`, so chunked + `chunk_checkpoint: true` + default
    // `parallel: 1` runs stored `validated = NULL` in `export_metrics` and the
    // summary block dropped the `validated: pass` line — even though every chunk
    // file was validated.  The other three chunked paths (exec, parallel
    // checkpoint) set the flag; this pins the sequential one to match.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(150);
    let export = unique_name("c5_seq_validated");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    let run = rig.run_args(&["--export", &export, "--validate"]);
    assert!(
        run.status.success(),
        "clean validated run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    assert_eq!(
        latest_metric_validated(&cfg, &export),
        Some(true),
        "sequential checkpoint + --validate must record validated=true in export_metrics"
    );
}

/// A chunk that FAILS (not crashes) must fail the run — never a success whose
/// output is quietly short.
///
/// The checkpoint runner marks a failed chunk `failed` and keeps going, so the
/// only thing standing between a partial export and a green `_SUCCESS` is the
/// end-of-loop guard that counts chunk tasks which never completed. Disabling
/// that guard (`pending > 0` → `pending < 0`) left the ENTIRE live suite green —
/// 9 chunked-recovery, 1 crash-soak and 4 chaos tests all passed while a run
/// missing a whole chunk reported success. Every existing test injects a PANIC,
/// and a panic never reaches the guard: the guard only runs when the loop
/// finishes, which a crashed process never does.
///
/// So this injects an ERROR instead (`RIVET_TEST_ERROR_AT=chunk_export:1`): the
/// chunk returns, the loop completes, and the guard is the only thing that can
/// notice. The oracle is the exported ROW COUNT as well as the exit code —
/// exiting non-zero for some other reason would satisfy a status-only assertion
/// without the guard existing at all.
#[test]
#[ignore = "live: postgres"]
fn a_failed_chunk_must_fail_the_run_not_ship_a_short_export() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(150);
    let export = unique_name("chunk_fail_guard");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());

    let run = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_ERROR_AT", "chunk_export:1")],
    );
    let stderr = String::from_utf8_lossy(&run.stderr).into_owned();

    // The fixture must not be inert: the OTHER chunks have to have run, or the
    // run failed for an unrelated reason and proves nothing about the guard.
    let rows = total_parquet_rows(out.path());
    assert!(
        rows > 0 && rows < 150,
        "fixture is inert — expected a PARTIAL export (some chunks written, one failed), \
         got {rows} of 150 rows; stderr:\n{stderr}"
    );

    assert!(
        !run.status.success(),
        "a run that exported {rows} of 150 rows reported SUCCESS — the chunk-completion guard \
         is the only thing that notices a failed chunk, and it did not; stderr:\n{stderr}"
    );
    assert!(
        !out.path().join("_SUCCESS").exists(),
        "a short export must not leave a _SUCCESS marker for a downstream loader to trust"
    );
}

/// The PARALLEL checkpoint runner's half of the same contract — and it needed
/// its own test for the reason this repo keeps re-learning: a guard wired into
/// one runner says nothing about the others.
///
/// Here TWO guards stand between a partial export and a green `_SUCCESS`: the
/// collected worker errors, and the chunk-completion count behind it. Disabling
/// BOTH left 56 live tests green — chunked-recovery, chunked-dense, cli-flags and
/// crash-soak — while a run missing a whole chunk reported success.
///
/// So this injects an ERROR instead (`RIVET_TEST_ERROR_AT=chunk_export:1`): the
/// chunk returns, the loop completes, and the guard is the only thing that can
/// notice. The oracle is the exported ROW COUNT as well as the exit code —
/// exiting non-zero for some other reason would satisfy a status-only assertion
/// without the guard existing at all.
#[test]
#[ignore = "live: postgres"]
fn a_failed_chunk_must_fail_the_parallel_run_not_ship_a_short_export() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(150);
    let export = unique_name("chunk_fail_guard_par");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(&export)
        .query(&format!(
            r#"SELECT id, name FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .export_line("parallel: 4")
        .dest_path(out.path().to_path_buf());
    let cfg = rig.config_path();

    let run = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_ERROR_AT", "chunk_export:1")],
    );
    let stderr = String::from_utf8_lossy(&run.stderr).into_owned();

    // Raw parquet on disk is NOT the oracle here, and measuring it was the first
    // draft's mistake: a worker retries its chunk internally, and each attempt
    // writes a distinct part before the injected error discards its record — so
    // the prefix legitimately holds MORE rows than the table (measured: 303 files
    // for 299 recorded). Those are unmanifested parts of a FAILED run, which is
    // exactly what `gc_orphans` is for, not a loss. The ledger is what a
    // downstream reader would trust, so the ledger is what this asserts.
    let committed = {
        let db = StateDb::next_to_config(&cfg);
        let run_id = db.latest_run_id(&export);
        db.metrics_row(&run_id).files_committed.unwrap_or(0)
    };
    assert!(
        committed > 0,
        "fixture is inert — no chunk completed, so the run failed for an unrelated reason and \
         proves nothing about the guard; stderr:\n{stderr}"
    );

    assert!(
        !run.status.success(),
        "a run that recorded only {committed} chunk part(s) of a 3-chunk export reported \
         SUCCESS — the worker-error and chunk-completion guards are the only things that \
         notice a failed chunk, and neither did; stderr:\n{stderr}"
    );
    assert!(
        !out.path().join("_SUCCESS").exists(),
        "a short export must not leave a _SUCCESS marker for a downstream loader to trust"
    );
}
