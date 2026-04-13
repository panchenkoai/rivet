//! RunJournal ordering and query-method invariants — Epic 10 / ADR-0001
//!
//! `RunJournal` is the canonical in-memory record of a pipeline run introduced in Epic 10.
//! These tests verify that its query methods correctly filter the event log and that
//! the ordering guarantees hold.  Each test names the property it protects.

use rivet::pipeline::journal::{PlanSnapshot, RunEvent, RunJournal};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn make_journal() -> RunJournal {
    RunJournal::new("run-test-001", "my_export")
}

fn plan_snapshot(name: &str) -> PlanSnapshot {
    PlanSnapshot {
        export_name: name.to_string(),
        base_query: "SELECT 1".to_string(),
        strategy: "full".to_string(),
        format: "parquet".to_string(),
        compression: "zstd".to_string(),
        destination_type: "local".to_string(),
        tuning_profile: "balanced".to_string(),
        batch_size: 10_000,
        validate: false,
        reconcile: false,
        resume: false,
    }
}

fn file_written(name: &str, part: usize) -> RunEvent {
    RunEvent::FileWritten {
        file_name: name.to_string(),
        rows: 100,
        bytes: 4096,
        part_index: part,
    }
}

fn run_completed(status: &str) -> RunEvent {
    RunEvent::RunCompleted {
        status: status.to_string(),
        error_message: None,
        duration_ms: 50,
    }
}

// ─── Construction ─────────────────────────────────────────────────────────────

/// A new journal has no entries, and its identity fields are set correctly.
#[test]
fn journal_starts_empty_on_new() {
    let j = make_journal();
    assert!(j.entries.is_empty(), "new journal must have no entries");
    assert_eq!(j.run_id, "run-test-001");
    assert_eq!(j.export_name, "my_export");
}

// ─── plan_snapshot() ──────────────────────────────────────────────────────────

/// `plan_snapshot()` returns `None` before any `PlanResolved` event is recorded.
#[test]
fn plan_snapshot_absent_when_no_plan_resolved() {
    let j = make_journal();
    assert!(
        j.plan_snapshot().is_none(),
        "plan_snapshot() must be None on an empty journal"
    );
}

/// `plan_snapshot()` returns the **first** `PlanResolved` event if multiple are recorded.
/// This makes it stable even if the journal is somehow appended to after the fact.
#[test]
fn plan_snapshot_returns_first_plan_resolved() {
    let mut j = make_journal();
    j.record(RunEvent::PlanResolved(plan_snapshot("export_a")));
    j.record(RunEvent::PlanResolved(plan_snapshot("export_b")));

    let snap = j.plan_snapshot().expect("plan_snapshot must be Some");
    assert_eq!(
        snap.export_name, "export_a",
        "plan_snapshot() must return the first PlanResolved, not a later one"
    );
}

/// `plan_snapshot()` returns the correct fields from the recorded snapshot.
#[test]
fn plan_snapshot_fields_match_recorded_values() {
    let mut j = make_journal();
    j.record(RunEvent::PlanResolved(PlanSnapshot {
        export_name: "orders".into(),
        base_query: "SELECT * FROM orders".into(),
        strategy: "incremental".into(),
        format: "csv".into(),
        compression: "none".into(),
        destination_type: "s3".into(),
        tuning_profile: "high_throughput".into(),
        batch_size: 50_000,
        validate: true,
        reconcile: false,
        resume: true,
    }));

    let snap = j.plan_snapshot().unwrap();
    assert_eq!(snap.export_name, "orders");
    assert_eq!(snap.strategy, "incremental");
    assert_eq!(snap.batch_size, 50_000);
    assert!(snap.validate);
    assert!(!snap.reconcile);
    assert!(snap.resume);
}

// ─── final_outcome() ──────────────────────────────────────────────────────────

/// `final_outcome()` returns `None` before any `RunCompleted` event is recorded.
#[test]
fn final_outcome_absent_when_no_run_completed() {
    let mut j = make_journal();
    j.record(file_written("f.parquet", 0));
    assert!(
        j.final_outcome().is_none(),
        "final_outcome() must be None when no RunCompleted event exists"
    );
}

/// `final_outcome()` returns the **last** `RunCompleted` event.
/// This handles the edge case where a run records multiple completion events.
#[test]
fn final_outcome_returns_last_run_completed() {
    let mut j = make_journal();
    j.record(RunEvent::RunCompleted {
        status: "success".into(),
        error_message: None,
        duration_ms: 100,
    });
    // Simulate an event recorded after the first completion (unusual but must be handled).
    j.record(file_written("extra.parquet", 0));
    j.record(RunEvent::RunCompleted {
        status: "failed".into(),
        error_message: Some("post-write error".into()),
        duration_ms: 200,
    });

    let outcome = j.final_outcome().expect("final_outcome must be Some");
    let RunEvent::RunCompleted { status, .. } = &outcome.event else {
        panic!("final_outcome must return a RunCompleted entry");
    };
    assert_eq!(
        status, "failed",
        "final_outcome() must return the LAST RunCompleted, not the first"
    );
}

/// A single `RunCompleted` is returned by `final_outcome()`.
#[test]
fn final_outcome_returns_the_only_run_completed() {
    let mut j = make_journal();
    j.record(run_completed("success"));

    let outcome = j.final_outcome().unwrap();
    let RunEvent::RunCompleted { status, .. } = &outcome.event else {
        panic!("must be RunCompleted");
    };
    assert_eq!(status, "success");
}

// ─── files() ──────────────────────────────────────────────────────────────────

/// `files()` returns exactly the `FileWritten` entries, in insertion order.
#[test]
fn files_returns_only_file_written_events() {
    let mut j = make_journal();
    j.record(RunEvent::RetryAttempted {
        attempt: 1,
        reason: "timeout".into(),
        backoff_ms: 1_000,
    });
    j.record(file_written("a.parquet", 0));
    j.record(run_completed("success"));
    j.record(file_written("b.parquet", 1));

    let files = j.files();
    assert_eq!(
        files.len(),
        2,
        "files() must return exactly the FileWritten entries"
    );
    for e in &files {
        assert!(
            matches!(e.event, RunEvent::FileWritten { .. }),
            "files() must only contain FileWritten entries"
        );
    }
}

/// `files()` is empty when no file was written (e.g., skipped run).
#[test]
fn files_empty_when_no_file_written() {
    let mut j = make_journal();
    j.record(run_completed("skipped"));
    assert!(
        j.files().is_empty(),
        "files() must be empty when no FileWritten events exist"
    );
}

// ─── retries() ────────────────────────────────────────────────────────────────

/// `retries()` returns only `RetryAttempted` entries.
#[test]
fn retries_returns_only_retry_attempted_events() {
    let mut j = make_journal();
    j.record(RunEvent::RetryAttempted {
        attempt: 1,
        reason: "connection reset".into(),
        backoff_ms: 500,
    });
    j.record(file_written("f.parquet", 0));
    j.record(RunEvent::RetryAttempted {
        attempt: 2,
        reason: "broken pipe".into(),
        backoff_ms: 1_000,
    });

    let retries = j.retries();
    assert_eq!(retries.len(), 2);
    for e in &retries {
        assert!(
            matches!(e.event, RunEvent::RetryAttempted { .. }),
            "retries() must only contain RetryAttempted entries"
        );
    }
}

/// `retries()` is empty when the run completed without retrying.
#[test]
fn retries_empty_on_clean_run() {
    let mut j = make_journal();
    j.record(file_written("f.parquet", 0));
    assert!(j.retries().is_empty());
}

// ─── quality_issues() ─────────────────────────────────────────────────────────

/// `quality_issues()` returns both FAIL and WARN quality events, and nothing else.
#[test]
fn quality_issues_returns_only_quality_issue_events() {
    let mut j = make_journal();
    j.record(RunEvent::QualityIssue {
        severity: "WARN".into(),
        message: "row count below minimum".into(),
    });
    j.record(RunEvent::QualityIssue {
        severity: "FAIL".into(),
        message: "null ratio exceeded threshold".into(),
    });
    j.record(RunEvent::Warning {
        context: "schema".into(),
        message: "unrelated non-quality warning".into(),
    });

    let issues = j.quality_issues();
    assert_eq!(
        issues.len(),
        2,
        "quality_issues() must return both FAIL and WARN quality events"
    );
    for e in &issues {
        assert!(
            matches!(e.event, RunEvent::QualityIssue { .. }),
            "quality_issues() must not include Warning or other event types"
        );
    }
}

// ─── schema_changes() ─────────────────────────────────────────────────────────

/// `schema_changes()` returns only `SchemaChanged` entries.
#[test]
fn schema_changes_returns_only_schema_changed_events() {
    let mut j = make_journal();
    j.record(RunEvent::SchemaChanged {
        added: vec!["phone (Utf8)".into()],
        removed: vec!["age".into()],
        type_changed: vec![("balance".into(), "Float64".into(), "Utf8".into())],
    });
    j.record(RunEvent::QualityIssue {
        severity: "WARN".into(),
        message: "unrelated".into(),
    });

    let changes = j.schema_changes();
    assert_eq!(changes.len(), 1);
    assert!(
        matches!(changes[0].event, RunEvent::SchemaChanged { .. }),
        "schema_changes() must only return SchemaChanged entries"
    );
}

/// `schema_changes()` carries the correct diff fields.
#[test]
fn schema_changes_fields_are_correct() {
    let mut j = make_journal();
    j.record(RunEvent::SchemaChanged {
        added: vec!["new_col (Utf8)".into()],
        removed: vec!["old_col".into()],
        type_changed: vec![("price".into(), "Float32".into(), "Float64".into())],
    });

    let changes = j.schema_changes();
    let RunEvent::SchemaChanged {
        added,
        removed,
        type_changed,
    } = &changes[0].event
    else {
        panic!("expected SchemaChanged");
    };
    assert_eq!(added, &["new_col (Utf8)"]);
    assert_eq!(removed, &["old_col"]);
    assert_eq!(
        type_changed[0],
        ("price".into(), "Float32".into(), "Float64".into())
    );
}

// ─── warnings() ───────────────────────────────────────────────────────────────

/// `warnings()` includes both `Warning` and `PlanWarning` variants, and nothing else.
#[test]
fn warnings_includes_both_warning_and_plan_warning() {
    let mut j = make_journal();
    j.record(RunEvent::PlanWarning {
        rule: "stdout_manifest".into(),
        message: "manifest skipped for stdout destination".into(),
    });
    j.record(RunEvent::Warning {
        context: "schema".into(),
        message: "column type widened".into(),
    });
    j.record(file_written("f.parquet", 0));

    let warnings = j.warnings();
    assert_eq!(
        warnings.len(),
        2,
        "warnings() must include both Warning and PlanWarning"
    );
    for e in &warnings {
        assert!(
            matches!(
                e.event,
                RunEvent::Warning { .. } | RunEvent::PlanWarning { .. }
            ),
            "warnings() must not include FileWritten or other non-warning entries"
        );
    }
}

/// `warnings()` does not include `QualityIssue` events — those have their own method.
#[test]
fn warnings_does_not_include_quality_issues() {
    let mut j = make_journal();
    j.record(RunEvent::QualityIssue {
        severity: "WARN".into(),
        message: "should not appear in warnings()".into(),
    });
    j.record(RunEvent::Warning {
        context: "ctx".into(),
        message: "this should".into(),
    });

    let warnings = j.warnings();
    assert_eq!(
        warnings.len(),
        1,
        "warnings() must not include QualityIssue"
    );
    assert!(matches!(warnings[0].event, RunEvent::Warning { .. }));
}

// ─── chunk_events() ───────────────────────────────────────────────────────────

/// `chunk_events()` returns all three chunk lifecycle variants.
#[test]
fn chunk_events_includes_all_three_chunk_variants() {
    let mut j = make_journal();
    j.record(RunEvent::ChunkStarted {
        chunk_index: 0,
        start_key: "0".into(),
        end_key: "100".into(),
    });
    j.record(RunEvent::ChunkFailed {
        chunk_index: 0,
        error: "connection reset".into(),
        attempt: 1,
    });
    j.record(RunEvent::ChunkStarted {
        chunk_index: 0,
        start_key: "0".into(),
        end_key: "100".into(),
    });
    j.record(RunEvent::ChunkCompleted {
        chunk_index: 0,
        rows: 50,
        file_name: Some("part0.parquet".into()),
    });
    // FileWritten is NOT a chunk event.
    j.record(file_written("part0.parquet", 0));

    let chunks = j.chunk_events();
    assert_eq!(
        chunks.len(),
        4,
        "chunk_events() must include ChunkStarted, ChunkFailed, and ChunkCompleted"
    );
    assert!(
        !chunks
            .iter()
            .any(|e| matches!(e.event, RunEvent::FileWritten { .. })),
        "chunk_events() must not include FileWritten"
    );
}

/// `chunk_events()` is empty when no chunk events were recorded (single-path run).
#[test]
fn chunk_events_empty_on_single_path_run() {
    let mut j = make_journal();
    j.record(file_written("full.parquet", 0));
    j.record(run_completed("success"));
    assert!(
        j.chunk_events().is_empty(),
        "chunk_events() must be empty when no chunk lifecycle events were recorded"
    );
}

// ─── Insertion order and timestamps ───────────────────────────────────────────

/// Events are stored in insertion order; reading `entries` directly reflects the
/// sequence in which `record()` was called.
#[test]
fn entries_preserved_in_insertion_order() {
    let mut j = make_journal();
    j.record(RunEvent::PlanResolved(plan_snapshot("exp")));
    j.record(RunEvent::RetryAttempted {
        attempt: 1,
        reason: "r".into(),
        backoff_ms: 100,
    });
    j.record(file_written("f.parquet", 0));
    j.record(run_completed("success"));

    assert_eq!(j.entries.len(), 4);
    assert!(matches!(j.entries[0].event, RunEvent::PlanResolved(_)));
    assert!(matches!(
        j.entries[1].event,
        RunEvent::RetryAttempted { .. }
    ));
    assert!(matches!(j.entries[2].event, RunEvent::FileWritten { .. }));
    assert!(matches!(j.entries[3].event, RunEvent::RunCompleted { .. }));
}

/// `recorded_at` timestamps are non-decreasing — later entries are never earlier
/// than earlier entries.  Clock skew can make them equal but never reversed.
#[test]
fn recorded_at_timestamps_are_non_decreasing() {
    let mut j = make_journal();
    for i in 0u64..8 {
        j.record(RunEvent::FileWritten {
            file_name: format!("part{i}.parquet"),
            rows: i as i64 * 100,
            bytes: i * 1024,
            part_index: i as usize,
        });
    }

    let times: Vec<_> = j.entries.iter().map(|e| e.recorded_at).collect();
    for window in times.windows(2) {
        assert!(
            window[0] <= window[1],
            "journal timestamps must be non-decreasing: {:?} > {:?}",
            window[0],
            window[1]
        );
    }
}

// ─── ValidationResult and ReconciliationResult ────────────────────────────────

/// `ValidationResult` events pass through `entries` and are not filtered by
/// any query method — callers iterate entries directly.
#[test]
fn validation_result_recorded_in_entries() {
    let mut j = make_journal();
    j.record(RunEvent::ValidationResult { passed: true });
    j.record(RunEvent::ValidationResult { passed: false });

    let results: Vec<_> = j
        .entries
        .iter()
        .filter(|e| matches!(e.event, RunEvent::ValidationResult { .. }))
        .collect();
    assert_eq!(results.len(), 2);
    let RunEvent::ValidationResult { passed } = results[0].event else {
        panic!("expected ValidationResult");
    };
    assert!(passed, "first ValidationResult must have passed=true");
}

/// `ReconciliationResult` captures source count, exported rows, and match status.
#[test]
fn reconciliation_result_fields_are_correct() {
    let mut j = make_journal();
    j.record(RunEvent::ReconciliationResult {
        source_count: 1_000,
        exported_rows: 1_000,
        matched: true,
    });
    j.record(RunEvent::ReconciliationResult {
        source_count: 500,
        exported_rows: 499,
        matched: false,
    });

    let results: Vec<_> = j
        .entries
        .iter()
        .filter(|e| matches!(e.event, RunEvent::ReconciliationResult { .. }))
        .collect();
    assert_eq!(results.len(), 2);

    let RunEvent::ReconciliationResult {
        source_count,
        exported_rows,
        matched,
    } = results[1].event
    else {
        panic!("expected ReconciliationResult");
    };
    assert_eq!(source_count, 500);
    assert_eq!(exported_rows, 499);
    assert!(!matched, "mismatch must set matched=false");
}
