//! State update invariant tests — ADR-0001
//!
//! Each test below encodes one invariant from docs/adr/0001-state-update-invariants.md.
//! The invariant ID (I1–I7) is noted in each test name.

use rivet::state::StateStore;

/// File-backed store for tests that use `claim_next_chunk_task`.
/// `claim_next_chunk_task` opens a *new* connection to the DB path;
/// an in-memory store would create a separate empty DB each time.
fn file_store() -> (StateStore, tempfile::NamedTempFile) {
    let f = tempfile::NamedTempFile::new().unwrap();
    let store = StateStore::open_at_path(f.path()).unwrap();
    (store, f) // keep _f alive so the temp file isn't deleted
}

// ─── I5: Chunk Task Acyclicity ────────────────────────────────────────────────

/// I5 — A completed task is never re-claimed.
#[test]
fn i5_completed_chunk_task_is_not_reclaimed() {
    let (state, _f) = file_store();
    state
        .create_chunk_run("run-1", "export", "hash", 3)
        .unwrap();
    state
        .insert_chunk_tasks("run-1", &[(0, 100), (100, 200)])
        .unwrap();

    // Claim and complete chunk 0.
    let (idx, _, _) = state.claim_next_chunk_task("run-1").unwrap().unwrap();
    assert_eq!(idx, 0);
    state.complete_chunk_task("run-1", 0, 50, None).unwrap();

    // Next claim must be chunk 1, not chunk 0 again.
    let (idx2, _, _) = state.claim_next_chunk_task("run-1").unwrap().unwrap();
    assert_eq!(idx2, 1, "completed chunk 0 must not be re-claimed");

    state.complete_chunk_task("run-1", 1, 50, None).unwrap();

    // No more tasks.
    let none = state.claim_next_chunk_task("run-1").unwrap();
    assert!(none.is_none(), "all tasks completed — queue must be empty");
}

/// I5 — A failed task can be retried while attempts < max_chunk_attempts.
#[test]
fn i5_failed_chunk_task_retryable_within_max_attempts() {
    let (state, _f) = file_store();
    state
        .create_chunk_run("run-2", "export", "hash", 3)
        .unwrap();
    state.insert_chunk_tasks("run-2", &[(0, 100)]).unwrap();

    // Attempt 1: claim → fail.
    let (idx, _, _) = state.claim_next_chunk_task("run-2").unwrap().unwrap();
    assert_eq!(idx, 0);
    state
        .fail_chunk_task("run-2", 0, "transient network error")
        .unwrap();

    // Attempt 2: still claimable (failed but attempts=1 < max=3).
    let claimed = state.claim_next_chunk_task("run-2").unwrap();
    assert!(
        claimed.is_some(),
        "failed task within max_attempts must be reclaimable"
    );
    state.complete_chunk_task("run-2", 0, 10, None).unwrap();

    // After completion: no more tasks.
    assert!(state.claim_next_chunk_task("run-2").unwrap().is_none());
}

/// I5 — A failed task that has exhausted max_chunk_attempts is not re-claimed.
#[test]
fn i5_failed_chunk_task_not_retryable_beyond_max_attempts() {
    let (state, _f) = file_store();
    // max_chunk_attempts = 1 means only 1 attempt allowed.
    state
        .create_chunk_run("run-3", "export", "hash", 1)
        .unwrap();
    state.insert_chunk_tasks("run-3", &[(0, 100)]).unwrap();

    // Only attempt: claim → fail.
    state.claim_next_chunk_task("run-3").unwrap().unwrap();
    state.fail_chunk_task("run-3", 0, "fatal error").unwrap();

    // Must not be reclaimable.
    let none = state.claim_next_chunk_task("run-3").unwrap();
    assert!(
        none.is_none(),
        "task with exhausted attempts must not be re-claimed"
    );
}

// ─── I5 / Recovery: Stale Running Tasks ──────────────────────────────────────

/// I5 (recovery) — Tasks left in `running` after a crash are reset to `pending`
/// before resume, making them reclaimable again.
#[test]
fn i5_stale_running_tasks_reset_to_pending_on_resume() {
    let (state, _f) = file_store();
    state
        .create_chunk_run("run-4", "export", "hash", 3)
        .unwrap();
    state.insert_chunk_tasks("run-4", &[(0, 100)]).unwrap();

    // Simulate crash: task is claimed (status=running) but never completed.
    state.claim_next_chunk_task("run-4").unwrap().unwrap();

    // Nothing is claimable right now (running, not pending/failed).
    // (The running task won't be re-issued by normal claim.)
    // On resume, stale running tasks are explicitly reset.
    let reset = state.reset_stale_running_chunk_tasks("run-4").unwrap();
    assert_eq!(reset, 1, "one stale running task must be reset");

    // Now reclaimable.
    let reclaimed = state.claim_next_chunk_task("run-4").unwrap();
    assert!(reclaimed.is_some(), "reset task must be reclaimable");
}

// ─── I6: Finalize After All Complete ─────────────────────────────────────────

/// I6 — `count_chunk_tasks_not_completed` reaches 0 only after all tasks complete,
/// gating the `finalize_chunk_run_completed` call.
#[test]
fn i6_finalize_gate_requires_all_tasks_complete() {
    let (state, _f) = file_store();
    state
        .create_chunk_run("run-5", "export", "hash", 3)
        .unwrap();
    state
        .insert_chunk_tasks("run-5", &[(0, 100), (100, 200), (200, 300)])
        .unwrap();

    // Initially all 3 tasks are not completed.
    assert_eq!(
        state.count_chunk_tasks_not_completed("run-5").unwrap(),
        3,
        "all tasks start as not-completed"
    );

    // Complete tasks one by one; gate must not pass until all done.
    for i in 0..3i64 {
        let (idx, _, _) = state.claim_next_chunk_task("run-5").unwrap().unwrap();
        state.complete_chunk_task("run-5", idx, 50, None).unwrap();

        let remaining = state.count_chunk_tasks_not_completed("run-5").unwrap();
        let expected = 2 - i;
        assert_eq!(
            remaining, expected,
            "after completing chunk {idx}, {expected} tasks must remain"
        );
    }

    // Gate passes — safe to finalize.
    assert_eq!(state.count_chunk_tasks_not_completed("run-5").unwrap(), 0);
    state.finalize_chunk_run_completed("run-5").unwrap();
}

// ─── I3: Write Before Cursor (state layer) ───────────────────────────────────

/// I3 (state layer) — Cursor starts absent; after update it reflects the new value.
/// A failed write that returns before `st.update()` leaves the cursor unchanged.
#[test]
fn i3_cursor_absent_until_explicitly_updated() {
    let state = StateStore::open_in_memory().unwrap();

    let before = state.get("my_export").unwrap();
    assert!(
        before.last_cursor_value.is_none(),
        "cursor must be absent before any run"
    );

    state.update("my_export", "2024-06-01T00:00:00Z").unwrap();

    let after = state.get("my_export").unwrap();
    assert_eq!(
        after.last_cursor_value.as_deref(),
        Some("2024-06-01T00:00:00Z"),
        "cursor must reflect the updated value"
    );
}

/// I3 — A second update overwrites the first; the cursor only ever holds the
/// most recent committed value (monotone advance is the caller's responsibility).
#[test]
fn i3_cursor_update_is_last_write_wins() {
    let state = StateStore::open_in_memory().unwrap();
    state.update("exp", "2024-01-01T00:00:00Z").unwrap();
    state.update("exp", "2024-06-15T00:00:00Z").unwrap();

    let val = state.get("exp").unwrap().last_cursor_value.unwrap();
    assert_eq!(
        val, "2024-06-15T00:00:00Z",
        "cursor must hold the last committed value"
    );
}

// ─── I4: Metric After Verdict ─────────────────────────────────────────────────

/// I4 — Metric always records the final terminal status, never an intermediate one.
#[test]
fn i4_metric_records_terminal_status_success() {
    let state = StateStore::open_in_memory().unwrap();
    state
        .record_metric(
            "exp",
            "run-a",
            1000,
            500,
            None,
            "success",
            None,
            None,
            Some("parquet"),
            Some("full"),
            1,
            4096,
            0,
            None,
            None,
        )
        .unwrap();

    let m = &state.get_metrics(Some("exp"), 1).unwrap()[0];
    assert_eq!(m.status, "success");
    assert_eq!(m.total_rows, 500);
    assert_ne!(
        m.status, "running",
        "metric must never record intermediate state"
    );
}

/// I4 — A failed run records `"failed"` status with an error message.
#[test]
fn i4_metric_records_terminal_status_failed() {
    let state = StateStore::open_in_memory().unwrap();
    state
        .record_metric(
            "exp",
            "run-b",
            200,
            0,
            None,
            "failed",
            Some("connection refused"),
            None,
            Some("parquet"),
            Some("full"),
            0,
            0,
            2,
            None,
            None,
        )
        .unwrap();

    let m = &state.get_metrics(Some("exp"), 1).unwrap()[0];
    assert_eq!(m.status, "failed");
    assert_eq!(m.error_message.as_deref(), Some("connection refused"));
    assert_ne!(m.status, "running");
}

// ─── I2: Write Before Manifest (state layer) ─────────────────────────────────

/// I2 (state layer) — `record_file` creates exactly one manifest entry per call.
/// A file not yet written to the destination must have no manifest entry.
#[test]
fn i2_manifest_absent_before_record_file_is_called() {
    let state = StateStore::open_in_memory().unwrap();

    // Before any record_file call, manifest is empty.
    let files = state.get_files(Some("exp"), 10).unwrap();
    assert!(
        files.is_empty(),
        "manifest must be empty before any write is recorded"
    );

    state
        .record_file(
            "run-x",
            "exp",
            "exp_20240601.parquet",
            100,
            4096,
            "parquet",
            Some("zstd"),
        )
        .unwrap();

    let files = state.get_files(Some("exp"), 10).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_name, "exp_20240601.parquet");
}

/// I7 (structural) — Each run produces at most one manifest entry per file written.
/// Idempotent record_file calls with distinct file names produce distinct entries.
#[test]
fn i7_manifest_entry_per_file_no_duplication() {
    let state = StateStore::open_in_memory().unwrap();

    state
        .record_file(
            "run-y",
            "exp",
            "exp_part0.parquet",
            50,
            2048,
            "parquet",
            None,
        )
        .unwrap();
    state
        .record_file(
            "run-y",
            "exp",
            "exp_part1.parquet",
            50,
            2048,
            "parquet",
            None,
        )
        .unwrap();

    let files = state.get_files(Some("exp"), 10).unwrap();
    assert_eq!(
        files.len(),
        2,
        "two distinct files must produce two distinct entries"
    );
}
