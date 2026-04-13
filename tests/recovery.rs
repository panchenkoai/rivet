//! Recovery and Partial Failure Tests — Epic 13
//!
//! Each test targets a specific failure boundary in the pipeline write sequence
//! (ADR-0001):
//!
//!   dest.write() → record_file() → state.update() → record_metric()
//!
//! F1: Failure before file finalization        → no state written
//! F2: Failure after write but before cursor   → manifest present, cursor absent
//! F3: Failure after cursor but before metric  → write cycle complete, verdict absent
//! F4: Retry after partial progress            → state written exactly once
//! F5: Resume after interrupted chunked run    → completed chunks preserved, stale reset
//!
//! ## Scope
//!
//! Tests operate directly on the `StateStore` API in isolation — no live database
//! connection or pipeline execution is required.  This design makes them deterministic
//! and fast, but it means they verify that the **state layer behaves correctly when
//! called in the right order**, not that **the pipeline calls the state layer in that
//! order**.
//!
//! Pipeline-level ADR-0001 ordering (e.g. `record_file` is only called after
//! `dest.write()` returns `Ok`) is enforced by code structure in
//! `pipeline/single.rs` and `pipeline/chunked.rs` and verified by code inspection.
//! A future epic may add pipeline-level ordering tests via a mock destination.
//!
//! ## Known gap
//!
//! F5 covers the sequential checkpoint path only (`run_chunked_sequential_checkpoint`).
//! The parallel checkpoint path (`run_chunked_parallel_checkpoint`) uses the same
//! `StateStore` methods but with concurrent workers; its recovery behaviour under
//! mid-run process failure is not yet covered by automated tests.

use rivet::state::StateStore;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// File-backed store for tests that use `claim_next_chunk_task`.
/// That method opens a fresh connection to `self.db_path`; an in-memory store
/// would create a separate empty DB on each fresh-connection call.
fn file_store() -> (StateStore, tempfile::NamedTempFile) {
    let f = tempfile::NamedTempFile::new().unwrap();
    let store = StateStore::open_at_path(f.path()).unwrap();
    (store, f)
}

/// Shorthand: record a parquet file entry in the manifest.
fn record_file(state: &StateStore, run_id: &str, export: &str, file: &str, rows: i64) {
    state
        .record_file(
            run_id,
            export,
            file,
            rows,
            rows * 100,
            "parquet",
            Some("zstd"),
        )
        .unwrap();
}

// ─── F1: Failure before file finalization ─────────────────────────────────────

/// ADR-0001 I2 — `record_file` is called only after `dest.write()` returns `Ok(())`.
/// If the export fails before the destination write completes, the manifest must
/// remain empty.  Simulated by not calling `record_file` (the code path on failure).
#[test]
fn f1_pre_write_failure_leaves_manifest_clean() {
    let state = StateStore::open_in_memory().unwrap();

    // dest.write() returned Err — record_file is never called.

    let files = state.get_files(Some("orders"), 10).unwrap();
    assert!(
        files.is_empty(),
        "manifest must be empty when dest.write() never succeeded"
    );
}

/// ADR-0001 I3 — If the export fails before writing, the incremental cursor must
/// remain at its prior value so the next run re-exports from the same position.
/// Simulated by not calling `state.update()` (the code path on failure).
#[test]
fn f1_pre_write_failure_leaves_cursor_at_prior_value() {
    let state = StateStore::open_in_memory().unwrap();

    // Cursor was advanced by a previous successful run.
    state.update("orders", "2024-01-31T00:00:00Z").unwrap();

    // Export fails — state.update() is never called again.

    let cursor = state.get("orders").unwrap();
    assert_eq!(
        cursor.last_cursor_value.as_deref(),
        Some("2024-01-31T00:00:00Z"),
        "cursor must remain at the previous run's value after a failed export"
    );
}

// ─── F2: Failure after file write but before cursor advance ───────────────────

/// ADR-0001 I2→I3 crash window — `dest.write()` succeeded and `record_file()`
/// was called, but the process crashed before `state.update()` ran.
///
/// Observable state: manifest has the file; cursor is absent.
/// Recovery implication: the next incremental run sees no cursor and will
/// re-export from the beginning, producing rows that overlap with the file
/// that was already written (at-least-once delivery for that file).
#[test]
fn f2_crash_window_manifest_written_cursor_absent() {
    let state = StateStore::open_in_memory().unwrap();

    // dest.write() + record_file() succeeded, then crash before state.update().
    record_file(&state, "run-a", "orders", "orders_20240601.parquet", 1_000);

    // state.update() never called.

    let files = state.get_files(Some("orders"), 10).unwrap();
    assert_eq!(
        files.len(),
        1,
        "manifest must show the file written before the crash"
    );
    assert_eq!(files[0].file_name, "orders_20240601.parquet");

    let cursor = state.get("orders").unwrap();
    assert!(
        cursor.last_cursor_value.is_none(),
        "cursor must be absent — crash prevented the advance after record_file"
    );
}

/// F2 — The I2→I3 crash window is detectable by inspecting state.
/// manifest non-empty + cursor absent = file produced but cursor not advanced.
/// An operator or recovery procedure can identify this condition and decide
/// to either reset state for a clean re-run or rely on idempotent overwrite
/// at the destination.
#[test]
fn f2_crash_window_is_detectable_via_state_inspection() {
    let state = StateStore::open_in_memory().unwrap();

    record_file(&state, "run-b", "events", "events_20240601.parquet", 500);
    // state.update() not called — crash window

    let files = state.get_files(Some("events"), 10).unwrap();
    let cursor = state.get("events").unwrap();

    let manifest_has_file = !files.is_empty();
    let cursor_absent = cursor.last_cursor_value.is_none();

    assert!(
        manifest_has_file && cursor_absent,
        "crash window state must be detectable: manifest non-empty but cursor absent"
    );

    // A recovery procedure can reset cursor state to force a clean re-run.
    state.reset("events").unwrap();
    let after = state.get("events").unwrap();
    assert!(
        after.last_cursor_value.is_none(),
        "reset is idempotent when cursor is already absent"
    );
}

// ─── F3: Failure after cursor advance but before final verdict ────────────────

/// ADR-0001 I4 — If the process crashes after `state.update()` but before
/// `state.record_metric()`, the write phase is complete: the file exists at the
/// destination, the cursor is advanced, but no metric row exists for this run.
///
/// Recovery implication: the write was successful.  The next incremental run
/// will start from the correct cursor position.  No data re-export is needed.
/// The only gap is the observability record (missing metric row).
#[test]
fn f3_write_cycle_complete_without_metric_is_recoverable() {
    let state = StateStore::open_in_memory().unwrap();

    // Full write cycle: file written, cursor advanced.
    record_file(&state, "run-c", "users", "users_20240601.parquet", 200);
    state.update("users", "2024-06-01T00:00:00Z").unwrap();

    // state.record_metric() never called — process crashed before final verdict.

    // Write phase is intact:
    let files = state.get_files(Some("users"), 10).unwrap();
    assert_eq!(
        files.len(),
        1,
        "file must be recorded in the manifest despite the missing metric"
    );

    let cursor = state.get("users").unwrap();
    assert_eq!(
        cursor.last_cursor_value.as_deref(),
        Some("2024-06-01T00:00:00Z"),
        "cursor must be advanced to the correct position"
    );

    // Verdict is missing:
    let metrics = state.get_metrics(Some("users"), 10).unwrap();
    assert!(
        metrics.is_empty(),
        "no metric row — process crashed before record_metric was called"
    );

    // The next incremental run will start correctly from the advanced cursor.
}

// ─── F4: Retry after partial progress ────────────────────────────────────────

/// F4 — The file manifest is NOT idempotent: calling `record_file` twice for
/// the same file name inserts two distinct rows.  This documents why the
/// pipeline must gate `record_file` on `dest.write()` returning `Ok(())` —
/// if `record_file` were called on every retry attempt, the manifest would
/// accumulate duplicate entries and double-count rows and bytes.
#[test]
fn f4_manifest_not_idempotent_duplicate_calls_create_duplicate_entries() {
    let state = StateStore::open_in_memory().unwrap();

    // Bug simulation: record_file called on each of three attempts.
    record_file(&state, "run-d", "sales", "sales_20240601.parquet", 100);
    record_file(&state, "run-d", "sales", "sales_20240601.parquet", 100); // retry 1
    record_file(&state, "run-d", "sales", "sales_20240601.parquet", 100); // retry 2

    let files = state.get_files(Some("sales"), 10).unwrap();
    assert_eq!(
        files.len(),
        3,
        "manifest is NOT idempotent — each record_file call inserts a new row; \
         calling it on each attempt would produce 3 entries for 1 file"
    );
}

/// F4 — Correct retry path: `record_file` is called exactly once, only after
/// the final successful `dest.write()`.  After N-1 failed attempts followed by
/// one success, the manifest must contain exactly 1 entry.
#[test]
fn f4_state_written_exactly_once_after_retries() {
    let state = StateStore::open_in_memory().unwrap();

    // Attempt 1: dest.write() → Err → record_file NOT called.
    // Attempt 2: dest.write() → Err → record_file NOT called.
    // Attempt 3: dest.write() → Ok  → record_file called once.
    record_file(&state, "run-e", "sales", "sales_20240601.parquet", 100);

    let files = state.get_files(Some("sales"), 10).unwrap();
    assert_eq!(
        files.len(),
        1,
        "exactly 1 manifest entry after 2 failed attempts + 1 success"
    );
    assert_eq!(files[0].file_name, "sales_20240601.parquet");
}

/// F4 — The retry count is captured in the final metric, supporting post-mortem
/// analysis of how many attempts were needed and whether retries are trending up.
#[test]
fn f4_retry_count_captured_in_metric() {
    let state = StateStore::open_in_memory().unwrap();

    state
        .record_metric(
            "sales",
            "run-f",
            8_500,  // duration_ms (across 3 attempts)
            10_000, // total_rows
            None,   // peak_rss_mb
            "success",
            None, // error_message
            None, // tuning_profile
            Some("parquet"),
            Some("incremental"),
            1,       // files_produced
            409_600, // bytes_written
            2,       // retries = 2 failed attempts before success
            None,
            None,
        )
        .unwrap();

    let metrics = state.get_metrics(Some("sales"), 1).unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(
        metrics[0].retries, 2,
        "metric must record the number of retry attempts for post-mortem analysis"
    );
    assert_eq!(metrics[0].status, "success");
}

// ─── F5: Resume after interrupted chunked run ─────────────────────────────────

/// F5 — After a crash during a chunked run, the state of completed chunks
/// (rows_written, file_name) must be preserved when `reset_stale_running_chunk_tasks`
/// resets in-flight tasks to `pending`.  Only tasks in `running` status are reset.
///
/// Invariant I5: completed tasks are never re-claimed or overwritten.
#[test]
fn f5_resume_preserves_completed_chunk_data_after_stale_reset() {
    let (state, _f) = file_store();

    state
        .create_chunk_run("run-r1", "events", "hash-abc", 3)
        .unwrap();
    state
        .insert_chunk_tasks("run-r1", &[(0, 100), (100, 200), (200, 300)])
        .unwrap();

    // Chunk 0: complete successfully.
    let (idx, _, _) = state.claim_next_chunk_task("run-r1").unwrap().unwrap();
    assert_eq!(idx, 0);
    state
        .complete_chunk_task("run-r1", 0, 150, Some("events_chunk0.parquet"))
        .unwrap();

    // Chunk 1: claim but crash — left in "running".
    let (idx, _, _) = state.claim_next_chunk_task("run-r1").unwrap().unwrap();
    assert_eq!(idx, 1);
    // Process crash — complete_chunk_task never called for chunk 1.

    // Chunk 2: still pending (never claimed).

    // Resume: reset stale running tasks.
    let reset_count = state.reset_stale_running_chunk_tasks("run-r1").unwrap();
    assert_eq!(
        reset_count, 1,
        "exactly one stale task (chunk 1) must be reset"
    );

    // Inspect post-reset state.
    let tasks = state.list_chunk_tasks_for_run("run-r1").unwrap();
    assert_eq!(tasks.len(), 3);

    // Chunk 0: completed data must be preserved.
    assert_eq!(tasks[0].chunk_index, 0);
    assert_eq!(
        tasks[0].status, "completed",
        "chunk 0 must remain completed after stale reset"
    );
    assert_eq!(
        tasks[0].rows_written,
        Some(150),
        "chunk 0 rows_written must be preserved (not reset to None)"
    );
    assert_eq!(
        tasks[0].file_name.as_deref(),
        Some("events_chunk0.parquet"),
        "chunk 0 file_name must be preserved"
    );

    // Chunk 1: reset from running → pending.
    assert_eq!(tasks[1].chunk_index, 1);
    assert_eq!(
        tasks[1].status, "pending",
        "chunk 1 must be reset to pending after stale reset"
    );
    assert!(
        tasks[1].rows_written.is_none(),
        "chunk 1 rows_written must be None (was never completed)"
    );

    // Chunk 2: untouched — still pending.
    assert_eq!(tasks[2].chunk_index, 2);
    assert_eq!(tasks[2].status, "pending", "chunk 2 must remain pending");
}

/// F5 — Full resume sequence: detect the interrupted run, reset stale tasks,
/// re-claim and complete remaining chunks, then finalize the run.
///
/// Invariants tested:
///   I5 — completed tasks are not re-claimed after reset
///   I6 — finalization gate requires all tasks to reach `completed` status
#[test]
fn f5_full_resume_sequence_completes_interrupted_run() {
    let (state, _f) = file_store();

    state
        .create_chunk_run("run-r2", "logs", "hash-xyz", 3)
        .unwrap();
    state
        .insert_chunk_tasks("run-r2", &[(0, 50), (50, 100), (100, 150)])
        .unwrap();

    // First partial run: complete chunk 0, crash during chunk 1.
    let (i0, _, _) = state.claim_next_chunk_task("run-r2").unwrap().unwrap();
    assert_eq!(i0, 0);
    state.complete_chunk_task("run-r2", 0, 50, None).unwrap();

    let (i1, _, _) = state.claim_next_chunk_task("run-r2").unwrap().unwrap();
    assert_eq!(i1, 1);
    // Crash — chunk 1 left in "running", chunk 2 still pending.

    // Resume: detect in-progress run.
    let run_info = state.find_in_progress_chunk_run("logs").unwrap();
    assert!(
        run_info.is_some(),
        "find_in_progress_chunk_run must detect the interrupted run"
    );
    let (found_run_id, _plan_hash) = run_info.unwrap();
    assert_eq!(found_run_id, "run-r2");

    // Reset stale running tasks.
    let reset = state.reset_stale_running_chunk_tasks("run-r2").unwrap();
    assert_eq!(
        reset, 1,
        "one stale task (chunk 1) must be reset to pending"
    );

    // Re-claim: chunk 1 is the lowest-indexed pending task (chunk 0 is completed).
    let (r1, _, _) = state.claim_next_chunk_task("run-r2").unwrap().unwrap();
    assert_eq!(
        r1, 1,
        "first re-claim after reset must return chunk 1 (completed chunk 0 is skipped)"
    );
    state.complete_chunk_task("run-r2", 1, 50, None).unwrap();

    // Claim and complete chunk 2.
    let (r2, _, _) = state.claim_next_chunk_task("run-r2").unwrap().unwrap();
    assert_eq!(r2, 2);
    state.complete_chunk_task("run-r2", 2, 50, None).unwrap();

    // No more tasks — queue is drained.
    assert!(
        state.claim_next_chunk_task("run-r2").unwrap().is_none(),
        "all tasks completed — claim queue must be empty"
    );

    // I6 gate passes — safe to finalize.
    let remaining = state.count_chunk_tasks_not_completed("run-r2").unwrap();
    assert_eq!(
        remaining, 0,
        "I6: all tasks completed — finalization is safe"
    );
    state.finalize_chunk_run_completed("run-r2").unwrap();

    // Verify final run status.
    let run = state.get_latest_chunk_run("logs").unwrap().unwrap();
    assert_eq!(
        run.2, "completed",
        "chunk run status must be 'completed' after finalization"
    );
}
