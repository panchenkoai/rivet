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

use chrono::{DateTime, Utc};

use crate::plan::ResolvedRunPlan;

// ─── Plan snapshot ───────────────────────────────────────────────────────────

/// Owned, serialisable snapshot of the resolved execution plan, captured at
/// the moment a run starts.  Answers "what was planned?" without requiring the
/// original `ResolvedRunPlan` to remain in scope.
#[derive(Debug, Clone)]
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

impl From<&ResolvedRunPlan> for PlanSnapshot {
    fn from(plan: &ResolvedRunPlan) -> Self {
        Self {
            export_name: plan.export_name.clone(),
            base_query: plan.base_query.clone(),
            strategy: plan.strategy.mode_label().to_string(),
            format: plan.format.label().to_string(),
            compression: plan.compression.label().to_string(),
            destination_type: plan.destination.destination_type.label().to_string(),
            tuning_profile: plan.tuning_profile_label.clone(),
            batch_size: plan.tuning.batch_size,
            validate: plan.validate,
            reconcile: plan.reconcile,
            resume: plan.resume,
        }
    }
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub recorded_at: DateTime<Utc>,
    pub event: RunEvent,
}

// ─── RunJournal ──────────────────────────────────────────────────────────────

/// Canonical in-memory record of a pipeline run.
///
/// Accumulated during execution via `record()`.  Query methods let callers
/// answer the four DoD questions without iterating `entries` directly.
#[derive(Debug, Clone)]
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
