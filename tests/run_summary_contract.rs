//! `RunSummary` structural contract tests.
//!
//! QA backlog Task 8.1.  The run summary is the machine- and human-readable
//! verdict surface consumed by notifications (Slack/webhook), CI schedulers,
//! and operators inspecting logs.  Silent field renames or removed metrics
//! break downstream automation.  These tests pin the contract.
//!
//! Scope:
//!   * Every required field exists on the struct and round-trips as expected.
//!   * The journal captures a `PlanResolved` event and exposes a
//!     `plan_snapshot()` helper.
//!   * Structural invariants across terminal states (success / failed /
//!     degraded) — any field promoted to optional must stay optional.
//!
//! We exercise the struct via direct construction (all fields are `pub`)
//! rather than `RunSummary::new` (which is `pub(super)` for the pipeline
//! builders).  The real builder is unit-tested in `src/pipeline/mod.rs`.

use rivet::pipeline::RunSummary;
use rivet::pipeline::journal::{PlanSnapshot, RunEvent, RunJournal};

/// Create a minimally-populated summary for contract checks.
fn stub_summary(status: &str) -> RunSummary {
    RunSummary {
        run_id: "test_run".into(),
        export_name: "orders".into(),
        status: status.into(),
        total_rows: 100,
        files_produced: 1,
        bytes_written: 1024,
        duration_ms: 500,
        peak_rss_mb: 10,
        retries: 0,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message: None,
        tuning_profile: "balanced".into(),
        batch_size: 10_000,
        batch_size_memory_mb: None,
        format: "parquet".into(),
        mode: "full".into(),
        compression: "zstd".into(),
        source_count: None,
        reconciled: None,
        journal: RunJournal::new("test_run", "orders"),
    }
}

// ─── Required fields contract ────────────────────────────────

#[test]
fn success_summary_has_all_required_identification_fields() {
    let s = stub_summary("success");
    assert!(!s.run_id.is_empty(), "run_id is required for correlation");
    assert!(!s.export_name.is_empty(), "export_name is required");
    assert_eq!(s.status, "success");
    assert!(!s.format.is_empty());
    assert!(!s.mode.is_empty());
    assert!(!s.tuning_profile.is_empty());
}

#[test]
fn success_summary_carries_volume_metrics_as_non_negative_integers() {
    let s = stub_summary("success");
    assert!(s.total_rows >= 0);
    assert!(s.duration_ms >= 0);
    assert!(s.peak_rss_mb >= 0);
    // `files_produced`, `bytes_written`, `retries` are unsigned at the type
    // level so the compiler already enforces non-negativity; this test
    // documents that contract for reviewers.
    let _: usize = s.files_produced;
    let _: u64 = s.bytes_written;
    let _: u32 = s.retries;
}

#[test]
fn optional_verdict_fields_default_to_none_when_unset() {
    // Notifications must be able to distinguish "not applicable" (None) from
    // "ran and failed" (Some(false)).  Silently initialising these to
    // Some(false) would produce false alarms in dashboards.
    let s = stub_summary("success");
    assert!(s.validated.is_none(), "validated: None = not requested");
    assert!(
        s.schema_changed.is_none(),
        "schema_changed: None = no schema snapshot yet"
    );
    assert!(
        s.quality_passed.is_none(),
        "quality_passed: None = no quality config"
    );
    assert!(
        s.reconciled.is_none(),
        "reconciled: None = reconcile not requested"
    );
    assert!(s.source_count.is_none());
    assert!(
        s.error_message.is_none(),
        "success must leave error_message unset"
    );
}

#[test]
fn failed_summary_must_carry_error_message_for_observability() {
    let mut s = stub_summary("failed");
    s.error_message = Some("connection refused".into());
    assert_eq!(s.status, "failed");
    assert!(
        s.error_message.is_some(),
        "failed status must ship with an error_message so notifications are actionable"
    );
}

// ─── Journal contract ────────────────────────────────────────

#[test]
fn journal_run_id_and_export_name_match_summary() {
    let s = stub_summary("success");
    assert_eq!(s.journal.run_id, s.run_id);
    assert_eq!(s.journal.export_name, s.export_name);
}

#[test]
fn journal_plan_snapshot_helper_returns_the_first_plan_resolved_event() {
    let mut j = RunJournal::new("run-x", "orders");
    // Empty journal → no plan snapshot available.
    assert!(j.plan_snapshot().is_none());

    // Recording a PlanResolved event makes plan_snapshot() return it.
    j.record(RunEvent::PlanResolved(PlanSnapshot {
        export_name: "orders".into(),
        base_query: "SELECT * FROM orders".into(),
        strategy: "snapshot".into(),
        format: "parquet".into(),
        compression: "zstd".into(),
        destination_type: "local".into(),
        tuning_profile: "balanced".into(),
        batch_size: 10_000,
        validate: true,
        reconcile: false,
        resume: false,
    }));

    let snap = j
        .plan_snapshot()
        .expect("PlanResolved event must surface via plan_snapshot()");
    assert_eq!(snap.export_name, "orders");
    assert_eq!(snap.base_query, "SELECT * FROM orders");
}

#[test]
fn journal_record_appends_events_in_order() {
    let mut j = RunJournal::new("run-ord", "exp");
    j.record(RunEvent::Warning {
        context: "a".into(),
        message: "first".into(),
    });
    j.record(RunEvent::Warning {
        context: "b".into(),
        message: "second".into(),
    });
    assert_eq!(j.entries.len(), 2);
    if let RunEvent::Warning { message, .. } = &j.entries[0].event {
        assert_eq!(message, "first");
    } else {
        panic!("expected Warning at index 0");
    }
}

#[test]
fn journal_plan_snapshot_returns_none_when_only_non_plan_events_present() {
    let mut j = RunJournal::new("run", "exp");
    j.record(RunEvent::Warning {
        context: "x".into(),
        message: "y".into(),
    });
    assert!(j.plan_snapshot().is_none());
}
