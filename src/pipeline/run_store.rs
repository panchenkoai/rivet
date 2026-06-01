//! **Layer: Coordinator** — the single home for the runner-level
//! post-finalize state writes.
//!
//! When a runner finishes its work, two `state::*` writes must happen, in
//! a fixed order keyed on ADR-0001 I3 + ADR-0008 PG2:
//!
//! 1. **Cursor** ([`StateStore::update`]) — ADR-0001 I3 (Write Before
//!    Cursor): advances the per-export incremental cursor. **Fatal** on
//!    error — failing to advance the cursor risks re-extracting already-
//!    committed rows on the next run.
//! 2. **Progression** ([`StateStore::record_committed_incremental`] or the
//!    chunked walk in [`super::chunked::record_chunked_commit`]) —
//!    ADR-0008 PG2 (Committed boundary): records the latest committed
//!    point for the observability surface. **Warn-on-fail** per PG7
//!    ("advisory only").
//!
//! Before this module the runner-level call sites in [`super::single`]
//! (cursor + progression, ~25 lines), [`super::chunked::sequential_checkpoint`]
//! (`record_chunked_commit`), and [`super::chunked::parallel_checkpoint`]
//! (same) hand-wrote the sequence. The chunked variants did not even share
//! a function with the incremental variant — progression dispatch was a
//! per-runner concern.
//!
//! The builder makes the ordering an interface property: [`RunStore::commit`]
//! writes cursor (if any) first, then progression (if any). Callers chain
//! only what they have — chunked runners skip `with_cursor`; snapshot
//! runs skip both `with_cursor` and `with_progression` and `commit()`
//! is a no-op.
//!
//! Schema-store remains in [`super::single`] because schema-drift
//! detection + policy (`SchemaDriftPolicy::{Continue, Warn, Fail}`) is a
//! runner-level state machine that does not generalize across modes —
//! putting it here would conflate persistence-layer ordering with
//! runner business logic.

use super::summary::RunSummary;
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// How the run's progression boundary is recorded.
///
/// `Incremental { last_value }` is the cursor value this run reached;
/// recorded as a row with `last_committed_strategy = 'incremental'`.
/// `Chunked` walks `chunk_task` for the current `run_id` and records the
/// highest completed chunk index — the dispatch lives in
/// [`super::chunked::record_chunked_commit`] because the chunk-task walk
/// is chunked-mode-specific.
pub(crate) enum Progression {
    Incremental { last_value: String },
    Chunked,
}

/// Build a post-finalize state-write batch and commit it in the
/// ADR-0001 I3 → ADR-0008 PG2 canonical order.
///
/// See module doc for the per-write failure model.
pub(crate) struct RunStore<'a> {
    state: &'a StateStore,
    plan: &'a ResolvedRunPlan,
    summary: &'a RunSummary,
    cursor: Option<String>,
    progression: Option<Progression>,
}

impl<'a> RunStore<'a> {
    /// Start a finalize batch. Use `with_*` to attach writes, then
    /// `commit()` to apply them in the canonical order.
    pub fn finalize(
        state: &'a StateStore,
        plan: &'a ResolvedRunPlan,
        summary: &'a RunSummary,
    ) -> Self {
        Self {
            state,
            plan,
            summary,
            cursor: None,
            progression: None,
        }
    }

    /// Attach a cursor advance. ADR-0001 I3 — written first in `commit()`,
    /// **fatal** on failure so the next run does not re-extract
    /// already-committed rows.
    pub fn with_cursor(mut self, value: String) -> Self {
        self.cursor = Some(value);
        self
    }

    /// Attach the progression boundary update. ADR-0008 PG2 — written
    /// last in `commit()`, warn-on-fail per PG7.
    pub fn with_progression(mut self, p: Progression) -> Self {
        self.progression = Some(p);
        self
    }

    /// Apply the batched writes in the canonical order.
    ///
    /// Returns the cursor's error (fatal) without attempting the later
    /// writes — they would log against a half-finalized run. Otherwise
    /// returns `Ok(())` even if a warn-on-fail write errored.
    pub fn commit(self) -> Result<()> {
        // ADR-0001 I3 — cursor: fatal on error.
        if let Some(cursor_val) = self.cursor.as_deref() {
            self.state.update(&self.plan.export_name, cursor_val)?;

            // Test fault-point: cursor advanced, but the outer-pipeline
            // record_metric has NOT been recorded. QA backlog Task 1.1.
            // Lives here so every runner that uses RunStore inherits the
            // hook (previously inline in single.rs only).
            crate::test_hook::maybe_panic_at("after_cursor_commit");

            log::info!(
                "export '{}': cursor updated to '{}'",
                self.plan.export_name,
                cursor_val
            );
        }

        // ADR-0008 PG2 — progression: warn-on-fail per PG7.
        if let Some(p) = self.progression {
            match p {
                Progression::Incremental { last_value } => {
                    if let Err(e) = self.state.record_committed_incremental(
                        &self.plan.export_name,
                        &last_value,
                        &self.summary.run_id,
                    ) {
                        log::warn!(
                            "export '{}': committed boundary update failed: {:#}",
                            self.plan.export_name,
                            e
                        );
                    }
                }
                Progression::Chunked => {
                    // Chunked progression: walks chunk_task to find the
                    // highest completed chunk_index, then records it.
                    // Logic is chunked-mode-specific so the dispatch
                    // lives in `super::chunked::record_chunked_commit`.
                    super::chunked::record_chunked_commit(
                        self.state,
                        &self.plan.export_name,
                        &self.summary.run_id,
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
    };
    use crate::plan::{ExtractionStrategy, ResolvedRunPlan};
    use crate::tuning::SourceTuning;

    fn test_plan(export_name: &str) -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: export_name.into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn test_summary(plan: &ResolvedRunPlan, run_id: &str) -> RunSummary {
        let mut s = RunSummary::stub_for_testing(run_id, plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "snapshot".into();
        s.compression = "none".into();
        s
    }

    #[test]
    fn finalize_with_no_writes_is_a_noop() {
        // Snapshot mode hits this path: no cursor, no progression.
        // `commit()` must succeed without touching state.
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan("orders");
        let summary = test_summary(&plan, "run-noop");

        RunStore::finalize(&state, &plan, &summary)
            .commit()
            .unwrap();
    }

    #[test]
    fn finalize_with_cursor_only_advances_state_cursor() {
        // Incremental cursor with no progression (rare but valid:
        // single.rs's incremental path technically supports this when
        // the progression write is skipped — e.g., a future flag).
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan("orders");
        let summary = test_summary(&plan, "run-1");

        RunStore::finalize(&state, &plan, &summary)
            .with_cursor("2026-05-30T12:00:00Z".into())
            .commit()
            .unwrap();

        let cursor = state.get("orders").unwrap();
        assert_eq!(
            cursor.last_cursor_value.as_deref(),
            Some("2026-05-30T12:00:00Z"),
            "I3: state.update must persist the cursor value"
        );
    }

    #[test]
    fn finalize_with_progression_incremental_writes_progression_row() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan("orders");
        let summary = test_summary(&plan, "run-2");

        RunStore::finalize(&state, &plan, &summary)
            .with_progression(Progression::Incremental {
                last_value: "42".into(),
            })
            .commit()
            .unwrap();

        let prog = state.get_progression("orders").unwrap();
        let boundary = prog.committed.expect("committed boundary written");
        assert_eq!(boundary.strategy, "incremental");
        assert_eq!(boundary.cursor.as_deref(), Some("42"));
        assert_eq!(boundary.run_id.as_deref(), Some("run-2"));
    }

    #[test]
    fn finalize_with_progression_chunked_picks_highest_completed_chunk_index() {
        // Mirrors `chunked::tests::record_chunked_commit_picks_highest_completed_chunk`
        // but exercises the call through the RunStore facade — proving
        // the Chunked variant dispatches through the same chunked walk.
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan("orders");
        let summary = test_summary(&plan, "run-3");

        state.create_chunk_run("run-3", "orders", "h", 3).unwrap();
        state
            .insert_chunk_tasks("run-3", &[(1, 10), (11, 20), (21, 30)])
            .unwrap();
        state
            .complete_chunk_task("run-3", 0, 10, Some("c0.parquet"))
            .unwrap();
        state
            .complete_chunk_task("run-3", 2, 30, Some("c2.parquet"))
            .unwrap();
        // chunk_task 1 stays pending — must not be picked.

        RunStore::finalize(&state, &plan, &summary)
            .with_progression(Progression::Chunked)
            .commit()
            .unwrap();

        let prog = state.get_progression("orders").unwrap();
        let boundary = prog.committed.expect("committed boundary written");
        assert_eq!(boundary.strategy, "chunked");
        assert_eq!(
            boundary.chunk_index,
            Some(2),
            "must pick the highest *completed* chunk_index, not the highest claimed"
        );
        assert_eq!(boundary.run_id.as_deref(), Some("run-3"));
    }

    #[test]
    fn finalize_with_cursor_and_progression_writes_both() {
        // The canonical incremental finalize: cursor + progression in
        // one batch. Both side effects must be observable on state
        // after commit().
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan("orders");
        let summary = test_summary(&plan, "run-4");

        RunStore::finalize(&state, &plan, &summary)
            .with_cursor("99".into())
            .with_progression(Progression::Incremental {
                last_value: "99".into(),
            })
            .commit()
            .unwrap();

        assert_eq!(
            state.get("orders").unwrap().last_cursor_value.as_deref(),
            Some("99")
        );
        let prog = state.get_progression("orders").unwrap();
        let boundary = prog.committed.expect("committed boundary written");
        assert_eq!(boundary.cursor.as_deref(), Some("99"));
    }

    #[test]
    fn finalize_chunked_with_no_completed_tasks_is_a_silent_noop() {
        // Mirrors `chunked::tests::record_chunked_commit_with_no_completed_tasks_is_noop`
        // — exercised through the RunStore facade. Required because a
        // resume run that finds every chunk already completed in a prior
        // run legitimately reaches commit() with no fresh chunk_task
        // completions; the progression update must not fail or write a
        // bogus boundary.
        let state = StateStore::open_in_memory().unwrap();
        let plan = test_plan("orders");
        let summary = test_summary(&plan, "run-empty");

        state
            .create_chunk_run("run-empty", "orders", "h", 3)
            .unwrap();
        state
            .insert_chunk_tasks("run-empty", &[(1, 10), (11, 20)])
            .unwrap();

        RunStore::finalize(&state, &plan, &summary)
            .with_progression(Progression::Chunked)
            .commit()
            .unwrap();

        let prog = state.get_progression("orders").unwrap();
        assert!(
            prog.committed.is_none(),
            "empty chunk_task → no committed boundary"
        );
    }
}
