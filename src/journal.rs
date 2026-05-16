//! **Layer: Observability**
// Query methods and event fields are intentionally defined for future consumers
// (storage, CLI inspection, notifications).  Suppress dead_code for this module.
#![allow(dead_code)]
//!
//! `RunJournal` is the canonical in-memory record of everything that happened
//! during a pipeline run.  It accumulates typed, timestamped events and can
//! answer the four observability questions from the Epic 10 DoD:
//!
//! | Question               | Method               |
//! |------------------------|----------------------|
//! | What was planned?      | `plan_snapshot()`    |
//! | What happened?         | `files()`, `retries()`, `chunk_events()` |
//! | What degraded?         | `quality_issues()`, `schema_changes()`, `warnings()` |
//! | What was the outcome?  | `final_outcome()`    |
//!
//! `RunJournal` is currently embedded in `RunSummary` so that all pipeline
//! modules — which already hold `&mut RunSummary` — can record events without
//! signature changes.  A future epic will invert the relationship so that
//! `RunSummary` is derived from `RunJournal`.

//!
//! This module is the canonical home for journal types. It deliberately has no
//! dependencies on `plan`, `state`, or `pipeline` so that storage (state) and
//! orchestration (pipeline) can both depend on it without creating a cycle.
//! The `From<&ResolvedRunPlan>` conversion lives in `pipeline/summary.rs`
//! beside the call site that needs it.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ─── Plan snapshot ───────────────────────────────────────────────────────────

/// Owned, serialisable snapshot of the resolved execution plan, captured at
/// the moment a run starts.  Answers "what was planned?" without requiring the
/// original `ResolvedRunPlan` to remain in scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanSnapshot {
    pub export_name: String,
    pub base_query: String,
    pub strategy: String,
    pub format: String,
    pub compression: String,
    pub destination_type: String,
    pub tuning_profile: String,
    pub batch_size: usize,
    pub validate: bool,
    pub reconcile: bool,
    pub resume: bool,
}

// ─── Events ──────────────────────────────────────────────────────────────────

/// A single typed event emitted during a pipeline run.
///
/// Variants are grouped by DoD question:
/// - *Planned* — `PlanResolved`, `PlanWarning`
/// - *Happened* — `FileWritten`, `ChunkStarted`, `ChunkCompleted`, `ChunkFailed`, `RetryAttempted`
/// - *Degraded* — `QualityIssue`, `SchemaChanged`, `Warning`
/// - *Succeeded* — `ValidationResult`, `ReconciliationResult`
/// - *Outcome* — `RunCompleted`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunEvent {
    // ── Planned ──────────────────────────────────────────────
    /// Emitted once at the start of a run with a snapshot of the resolved plan.
    PlanResolved(PlanSnapshot),
    /// A plan validation diagnostic at `Warning` or `Degraded` level.
    PlanWarning { rule: String, message: String },

    // ── Happened ─────────────────────────────────────────────
    /// One output file was successfully written to the destination.
    FileWritten {
        file_name: String,
        rows: i64,
        bytes: u64,
        part_index: usize,
    },
    /// A chunk task transitioned from `pending` to `running`.
    ChunkStarted {
        chunk_index: i64,
        start_key: String,
        end_key: String,
    },
    /// A chunk task completed successfully.
    ChunkCompleted {
        chunk_index: i64,
        rows: i64,
        file_name: Option<String>,
    },
    /// A chunk task failed (may be retried up to `max_chunk_attempts`).
    ChunkFailed {
        chunk_index: i64,
        error: String,
        attempt: i64,
    },
    /// The pipeline is about to retry after a transient error.
    RetryAttempted {
        attempt: u32,
        reason: String,
        backoff_ms: u64,
    },

    // ── Degraded ─────────────────────────────────────────────
    /// One quality rule fired (severity: `"FAIL"` or `"WARN"`).
    QualityIssue { severity: String, message: String },
    /// The output schema differs from the previously stored snapshot.
    SchemaChanged {
        /// Columns added since the last run, formatted as `"name (type)"`.
        added: Vec<String>,
        /// Column names removed since the last run.
        removed: Vec<String>,
        /// `(column, old_type, new_type)` for columns whose type changed.
        type_changed: Vec<(String, String, String)>,
    },
    /// A non-fatal warning that does not fit another variant.
    Warning { context: String, message: String },

    // ── Succeeded ────────────────────────────────────────────
    /// Output file row-count validation completed.
    ValidationResult { passed: bool },
    /// Source COUNT(*) reconciliation completed.
    ReconciliationResult {
        source_count: i64,
        exported_rows: i64,
        matched: bool,
    },

    // ── Outcome ──────────────────────────────────────────────
    /// Terminal event — the run has reached its final state.
    RunCompleted {
        status: String,
        error_message: Option<String>,
        duration_ms: i64,
    },
}

// ─── Journal entry ───────────────────────────────────────────────────────────

/// A timestamped wrapper around a `RunEvent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub recorded_at: DateTime<Utc>,
    pub event: RunEvent,
}

// ─── RunJournal ──────────────────────────────────────────────────────────────

/// Canonical in-memory record of a pipeline run.
///
/// Accumulated during execution via `record()`.  Query methods let callers
/// answer the four DoD questions without iterating `entries` directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunJournal {
    pub run_id: String,
    pub export_name: String,
    /// All events in insertion order.
    pub entries: Vec<JournalEntry>,
}

impl RunJournal {
    pub fn new(run_id: impl Into<String>, export_name: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            export_name: export_name.into(),
            entries: Vec::new(),
        }
    }

    /// Append an event with the current UTC timestamp.
    pub fn record(&mut self, event: RunEvent) {
        self.entries.push(JournalEntry {
            recorded_at: Utc::now(),
            event,
        });
    }

    // ── What was planned? ─────────────────────────────────────

    /// Returns the plan snapshot recorded at the start of the run.
    pub fn plan_snapshot(&self) -> Option<&PlanSnapshot> {
        self.entries.iter().find_map(|e| {
            if let RunEvent::PlanResolved(s) = &e.event {
                Some(s)
            } else {
                None
            }
        })
    }

    // ── What happened? ────────────────────────────────────────

    /// All `FileWritten` entries, in the order files were committed.
    pub fn files(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.event, RunEvent::FileWritten { .. }))
            .collect()
    }

    /// All `RetryAttempted` entries.
    pub fn retries(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.event, RunEvent::RetryAttempted { .. }))
            .collect()
    }

    /// All chunk lifecycle entries (`ChunkStarted`, `ChunkCompleted`, `ChunkFailed`).
    pub fn chunk_events(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| {
                matches!(
                    e.event,
                    RunEvent::ChunkStarted { .. }
                        | RunEvent::ChunkCompleted { .. }
                        | RunEvent::ChunkFailed { .. }
                )
            })
            .collect()
    }

    // ── What degraded? ────────────────────────────────────────

    /// All `QualityIssue` entries (both FAIL and WARN severity).
    pub fn quality_issues(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.event, RunEvent::QualityIssue { .. }))
            .collect()
    }

    /// All `SchemaChanged` entries.
    pub fn schema_changes(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.event, RunEvent::SchemaChanged { .. }))
            .collect()
    }

    /// All `Warning` and `PlanWarning` entries.
    pub fn warnings(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| {
                matches!(
                    e.event,
                    RunEvent::Warning { .. } | RunEvent::PlanWarning { .. }
                )
            })
            .collect()
    }

    // ── What was the final outcome? ───────────────────────────

    /// The last `RunCompleted` entry, or `None` if the run has not yet finished.
    pub fn final_outcome(&self) -> Option<&JournalEntry> {
        self.entries
            .iter()
            .rev()
            .find(|e| matches!(e.event, RunEvent::RunCompleted { .. }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal() -> RunJournal {
        RunJournal::new("run-1", "orders")
    }

    fn snap() -> PlanSnapshot {
        PlanSnapshot {
            export_name: "orders".into(),
            base_query: "SELECT 1".into(),
            strategy: "snapshot".into(),
            format: "parquet".into(),
            compression: "zstd".into(),
            destination_type: "local".into(),
            tuning_profile: "balanced".into(),
            batch_size: 1000,
            validate: false,
            reconcile: false,
            resume: false,
        }
    }

    // ── construction ────────────────────────────────────────────────────────

    #[test]
    fn new_journal_is_empty() {
        let j = journal();
        assert_eq!(j.run_id, "run-1");
        assert_eq!(j.export_name, "orders");
        assert!(j.entries.is_empty());
    }

    // ── record ───────────────────────────────────────────────────────────────

    #[test]
    fn record_appends_entry() {
        let mut j = journal();
        j.record(RunEvent::Warning {
            context: "test".into(),
            message: "w".into(),
        });
        assert_eq!(j.entries.len(), 1);
    }

    #[test]
    fn record_multiple_entries_in_order() {
        let mut j = journal();
        j.record(RunEvent::Warning {
            context: "a".into(),
            message: "1".into(),
        });
        j.record(RunEvent::Warning {
            context: "b".into(),
            message: "2".into(),
        });
        assert_eq!(j.entries.len(), 2);
    }

    // ── plan_snapshot ────────────────────────────────────────────────────────

    #[test]
    fn plan_snapshot_none_when_empty() {
        assert!(journal().plan_snapshot().is_none());
    }

    #[test]
    fn plan_snapshot_returns_first_resolved() {
        let mut j = journal();
        j.record(RunEvent::PlanResolved(snap()));
        let s = j.plan_snapshot().unwrap();
        assert_eq!(s.export_name, "orders");
        assert_eq!(s.batch_size, 1000);
    }

    // ── files ────────────────────────────────────────────────────────────────

    #[test]
    fn files_empty_when_no_file_written() {
        let mut j = journal();
        j.record(RunEvent::Warning {
            context: "x".into(),
            message: "y".into(),
        });
        assert!(j.files().is_empty());
    }

    #[test]
    fn files_returns_file_written_entries() {
        let mut j = journal();
        j.record(RunEvent::FileWritten {
            file_name: "f.parquet".into(),
            rows: 100,
            bytes: 4096,
            part_index: 0,
        });
        j.record(RunEvent::Warning {
            context: "x".into(),
            message: "y".into(),
        });
        j.record(RunEvent::FileWritten {
            file_name: "g.parquet".into(),
            rows: 50,
            bytes: 2048,
            part_index: 1,
        });
        assert_eq!(j.files().len(), 2);
    }

    // ── retries ──────────────────────────────────────────────────────────────

    #[test]
    fn retries_empty_when_none_recorded() {
        assert!(journal().retries().is_empty());
    }

    #[test]
    fn retries_returns_retry_attempted_entries() {
        let mut j = journal();
        j.record(RunEvent::RetryAttempted {
            attempt: 1,
            reason: "timeout".into(),
            backoff_ms: 500,
        });
        j.record(RunEvent::RetryAttempted {
            attempt: 2,
            reason: "timeout".into(),
            backoff_ms: 1000,
        });
        assert_eq!(j.retries().len(), 2);
    }

    // ── chunk_events ─────────────────────────────────────────────────────────

    #[test]
    fn chunk_events_collects_all_three_variant_types() {
        let mut j = journal();
        j.record(RunEvent::ChunkStarted {
            chunk_index: 0,
            start_key: "0".into(),
            end_key: "100".into(),
        });
        j.record(RunEvent::ChunkCompleted {
            chunk_index: 0,
            rows: 100,
            file_name: None,
        });
        j.record(RunEvent::ChunkFailed {
            chunk_index: 1,
            error: "err".into(),
            attempt: 1,
        });
        j.record(RunEvent::Warning {
            context: "x".into(),
            message: "y".into(),
        });
        assert_eq!(j.chunk_events().len(), 3);
    }

    // ── quality_issues ───────────────────────────────────────────────────────

    #[test]
    fn quality_issues_filters_correctly() {
        let mut j = journal();
        j.record(RunEvent::QualityIssue {
            severity: "FAIL".into(),
            message: "null check".into(),
        });
        j.record(RunEvent::Warning {
            context: "x".into(),
            message: "y".into(),
        });
        assert_eq!(j.quality_issues().len(), 1);
    }

    // ── schema_changes ───────────────────────────────────────────────────────

    #[test]
    fn schema_changes_filters_correctly() {
        let mut j = journal();
        j.record(RunEvent::SchemaChanged {
            added: vec!["new_col (Int64)".into()],
            removed: vec![],
            type_changed: vec![],
        });
        assert_eq!(j.schema_changes().len(), 1);
    }

    // ── warnings ─────────────────────────────────────────────────────────────

    #[test]
    fn warnings_includes_both_warning_and_plan_warning() {
        let mut j = journal();
        j.record(RunEvent::Warning {
            context: "ctx".into(),
            message: "w1".into(),
        });
        j.record(RunEvent::PlanWarning {
            rule: "r".into(),
            message: "w2".into(),
        });
        j.record(RunEvent::QualityIssue {
            severity: "WARN".into(),
            message: "q".into(),
        });
        assert_eq!(j.warnings().len(), 2);
    }

    // ── final_outcome ─────────────────────────────────────────────────────────

    #[test]
    fn final_outcome_none_when_not_completed() {
        let mut j = journal();
        j.record(RunEvent::Warning {
            context: "x".into(),
            message: "y".into(),
        });
        assert!(j.final_outcome().is_none());
    }

    #[test]
    fn final_outcome_returns_last_run_completed() {
        let mut j = journal();
        j.record(RunEvent::RunCompleted {
            status: "success".into(),
            error_message: None,
            duration_ms: 1234,
        });
        j.record(RunEvent::Warning {
            context: "x".into(),
            message: "y".into(),
        });
        j.record(RunEvent::RunCompleted {
            status: "failed".into(),
            error_message: Some("err".into()),
            duration_ms: 5678,
        });
        let outcome = j.final_outcome().unwrap();
        if let RunEvent::RunCompleted { status, .. } = &outcome.event {
            assert_eq!(status, "failed");
        } else {
            panic!("expected RunCompleted");
        }
    }
}
