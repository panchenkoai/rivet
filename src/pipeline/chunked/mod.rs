//! **Layer: Execution** (with bounded persistence writes)
//!
//! Implements chunked extraction in three variants: sequential (no
//! checkpoint), parallel-simple (no checkpoint), and the two checkpoint
//! runners that drive the resumable `chunk_checkpoint: true` flow. The
//! module is split into focused siblings:
//!
//! - `math` — pure functions: date parsing, range generation, SQL building, fingerprinting.
//! - `detect` — chunk-boundary detection via source queries.
//! - `exec` — stateless sequential and parallel execution (no checkpoint).
//! - `sequential_checkpoint` — single-threaded chunk-checkpoint runner.
//! - `parallel_checkpoint` — multi-worker chunk-checkpoint runner.
//! - `mod` (this file) — shared orchestration helpers (`chunked_plan`,
//!   `config_hint`, `ensure_chunk_checkpoint_plan`, `record_chunked_commit`),
//!   the [`ChunkSource`] enum, public re-exports for callers in
//!   `pipeline::*`, and unit tests for the checkpoint state-machine helpers.
//!
//! The split keeps each checkpoint runner in a focused ~150–350 LoC file
//! and concentrates the helpers they share in one place rather than
//! duplicating them.

mod detect;
mod exec;
pub(crate) mod math;
mod parallel_checkpoint;
mod sequential_checkpoint;

// ─── Re-exports for callers in pipeline:: ────────────────────────────────────

pub(crate) use detect::detect_and_generate_chunks;
pub(crate) use exec::run_chunked_parallel;
pub(crate) use exec::run_chunked_sequential;
pub use math::generate_chunks;
pub(crate) use math::{
    RIVET_CHUNK_RN_COL, build_chunk_query_sql, chunk_plan_fingerprint, strip_select_star_from,
};
pub(crate) use parallel_checkpoint::run_chunked_parallel_checkpoint;
pub(crate) use sequential_checkpoint::run_chunked_sequential_checkpoint;

// ─── Chunk source selection ───────────────────────────────────────────────────

/// Determines how chunk ranges are obtained at execution time.
///
/// `rivet run` always uses `Detect` — runs `SELECT min/max` against the source
/// to compute boundaries live.  `rivet apply` passes `Precomputed` ranges from
/// a previously-generated `PlanArtifact`, skipping the detection queries
/// entirely and replaying the same boundaries that were fixed at plan time.
pub(crate) enum ChunkSource {
    /// Query the source for min/max and compute boundaries at execution time.
    Detect,
    /// Replay pre-computed boundaries from a `PlanArtifact`.
    Precomputed(Vec<(i64, i64)>),
}

// ─── Shared checkpoint-orchestration helpers ─────────────────────────────────

use super::RunSummary;
use crate::error::Result;
use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
use crate::state::StateStore;

/// Extract the `ChunkedPlan` from a `ResolvedRunPlan`. Panics if the strategy
/// is not `Chunked` — all callers in this module only run for chunked plans.
pub(super) fn chunked_plan(plan: &ResolvedRunPlan) -> &ChunkedPlan {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => cp,
        _ => unreachable!("chunked runner called with non-chunked plan"),
    }
}

/// Render the `--config <path>` argument used in user-facing recovery hints,
/// or a `<CONFIG>` placeholder when the config path is not available (e.g.
/// the `rivet apply` path which only knows the plan file).
pub(super) fn config_hint(config_path: &str) -> String {
    if config_path.is_empty() {
        "--config <CONFIG>".to_string()
    } else {
        format!("--config {}", config_path)
    }
}

pub(super) fn ensure_chunk_checkpoint_plan(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    cp: &ChunkedPlan,
    summary: &mut RunSummary,
    chunks: &[(i64, i64)],
    config_path: &str,
) -> Result<String> {
    let plan_hash = chunk_plan_fingerprint(
        &plan.base_query,
        &cp.column,
        cp.chunk_size,
        cp.chunk_count,
        cp.dense,
        cp.by_days,
    );
    let max_att = cp.max_attempts;

    if plan.resume {
        match state.find_in_progress_chunk_run(&plan.export_name)? {
            Some((rid, stored_hash)) => {
                if stored_hash != plan_hash {
                    anyhow::bail!(
                        "export '{}': chunk plan fingerprint mismatch (query, chunk_column, chunk_size, or chunk_dense changed); cannot resume",
                        plan.export_name
                    );
                }
                summary.run_id = rid.clone();
                let n = state.reset_stale_running_chunk_tasks(&rid)?;
                if n > 0 {
                    log::warn!(
                        "export '{}': reset {} stale 'running' chunk task(s) after resume",
                        plan.export_name,
                        n
                    );
                }
                return Ok(rid);
            }
            None => {
                anyhow::bail!(
                    "export '{}': --resume but no in-progress chunk checkpoint; \
                     run without --resume first or `rivet state reset-chunks --config <cfg> --export {}`",
                    plan.export_name,
                    plan.export_name
                );
            }
        }
    }

    if let Some((rid, _)) = state.find_in_progress_chunk_run(&plan.export_name)? {
        anyhow::bail!(
            "export '{}': chunk checkpoint run '{}' still in progress; use `rivet run {} --export {} --resume` or `rivet state reset-chunks {} --export {}`",
            plan.export_name,
            rid,
            config_hint(config_path),
            plan.export_name,
            config_hint(config_path),
            plan.export_name
        );
    }

    state.create_chunk_run(&summary.run_id, &plan.export_name, &plan_hash, max_att)?;
    state.insert_chunk_tasks(&summary.run_id, chunks)?;
    log::info!(
        "export '{}': chunk checkpoint: {} tasks saved (run_id={})",
        plan.export_name,
        chunks.len(),
        summary.run_id
    );
    Ok(summary.run_id.clone())
}

/// Epic G: record the highest completed `chunk_index` for this run as the new
/// committed boundary. Failures are logged — progression is observational and
/// must not fail the pipeline.
pub(super) fn record_chunked_commit(state: &StateStore, export_name: &str, run_id: &str) {
    let tasks = match state.list_chunk_tasks_for_run(run_id) {
        Ok(t) => t,
        Err(e) => {
            log::warn!(
                "export '{}': committed boundary: could not read chunk tasks: {:#}",
                export_name,
                e
            );
            return;
        }
    };
    let highest = tasks
        .iter()
        .filter(|t| t.status == "completed")
        .map(|t| t.chunk_index)
        .max();
    if let Some(idx) = highest
        && let Err(e) = state.record_committed_chunked(export_name, idx, run_id)
    {
        log::warn!(
            "export '{}': committed boundary update failed: {:#}",
            export_name,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    //! Unit coverage for the checkpoint state machine in this module.
    //!
    //! These tests use `StateStore::open_in_memory()` so they exercise the
    //! real SQLite schema and the real `state::checkpoint::*` methods —
    //! no mocks, no docker. They are *intentionally* the only unit cover
    //! for this file's recovery logic; everything that touches a live
    //! `Source` is exercised by `tests/live_*.rs` instead.
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
    use crate::tuning::SourceTuning;

    fn make_plan(export_name: &str) -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: export_name.into(),
            base_query: "SELECT id FROM orders".into(),
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                chunk_count: None,
                parallel: 1,
                dense: false,
                by_days: None,
                checkpoint: true,
                max_attempts: 3,
            }),
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("/tmp".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
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
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn make_summary(plan: &ResolvedRunPlan, run_id: &str) -> RunSummary {
        let mut s = RunSummary::stub_for_testing(run_id, plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "chunked".into();
        s.compression = "none".into();
        s
    }

    // ── config_hint ────────────────────────────────────────────────────────

    #[test]
    fn config_hint_uses_explicit_path_when_set() {
        assert_eq!(config_hint("rivet.yaml"), "--config rivet.yaml");
    }

    #[test]
    fn config_hint_uses_placeholder_when_empty() {
        assert_eq!(config_hint(""), "--config <CONFIG>");
    }

    // ── ensure_chunk_checkpoint_plan: 5 state-machine transitions ──────────

    /// Fresh run (no resume, no prior checkpoint): must create chunk_run +
    /// insert tasks, and the returned run_id matches summary.run_id.
    #[test]
    fn ensure_chunk_checkpoint_fresh_run_creates_state() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan, "run-fresh");
        let chunks = vec![(1, 100), (101, 200), (201, 300)];

        let rid =
            ensure_chunk_checkpoint_plan(&state, &plan, &cp, &mut summary, &chunks, "rivet.yaml")
                .expect("fresh run must succeed");
        assert_eq!(rid, "run-fresh");

        let total = state.count_chunk_tasks_total(&rid).unwrap();
        assert_eq!(total, 3, "all 3 chunk tasks must be persisted");
    }

    /// `--resume` with no in-progress run → actionable error.
    #[test]
    fn ensure_chunk_checkpoint_resume_without_prior_run_bails() {
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = make_plan("orders");
        plan.resume = true;
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan, "run-x");

        let err = ensure_chunk_checkpoint_plan(&state, &plan, &cp, &mut summary, &[], "rivet.yaml")
            .expect_err("resume without prior run must error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("--resume but no in-progress chunk checkpoint"),
            "got: {msg}"
        );
        assert!(
            msg.contains("reset-chunks"),
            "error must point to recovery command"
        );
    }

    /// `--resume` with a prior run that has a *different* plan fingerprint
    /// (e.g. chunk_size changed) → bail with fingerprint-mismatch error.
    #[test]
    fn ensure_chunk_checkpoint_resume_with_hash_mismatch_bails() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        state
            .create_chunk_run("run-old", "orders", "DIFFERENT_HASH", 3)
            .unwrap();

        let mut plan_resume = plan.clone();
        plan_resume.resume = true;
        let cp = match &plan_resume.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan_resume, "run-new");

        let err = ensure_chunk_checkpoint_plan(
            &state,
            &plan_resume,
            &cp,
            &mut summary,
            &[],
            "rivet.yaml",
        )
        .expect_err("hash mismatch must error");
        let msg = format!("{:#}", err);
        assert!(msg.contains("fingerprint mismatch"), "got: {msg}");
        assert!(msg.contains("cannot resume"), "got: {msg}");
    }

    /// `--resume` with a prior run whose fingerprint matches: must adopt the
    /// existing run_id, *not* create a new one.
    #[test]
    fn ensure_chunk_checkpoint_resume_matching_hash_adopts_old_run_id() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let expected_hash = chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.chunk_count,
            cp.dense,
            cp.by_days,
        );
        state
            .create_chunk_run("run-prior", "orders", &expected_hash, 3)
            .unwrap();

        let mut plan_resume = plan.clone();
        plan_resume.resume = true;
        let mut summary = make_summary(&plan_resume, "run-this-attempt");

        let rid = ensure_chunk_checkpoint_plan(
            &state,
            &plan_resume,
            &cp,
            &mut summary,
            &[],
            "rivet.yaml",
        )
        .expect("matching resume must succeed");
        assert_eq!(
            rid, "run-prior",
            "must adopt the prior run_id, not create new"
        );
        assert_eq!(
            summary.run_id, "run-prior",
            "summary.run_id must also be rewritten so downstream writes target the existing run"
        );
    }

    /// Existing in-progress run *without* `--resume` → bail with a message
    /// that tells the operator exactly which recovery command to run.
    #[test]
    fn ensure_chunk_checkpoint_existing_run_without_resume_bails() {
        let state = StateStore::open_in_memory().unwrap();
        state
            .create_chunk_run("run-stuck", "orders", "ANY_HASH", 3)
            .unwrap();

        let plan = make_plan("orders");
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan, "run-new");

        let err = ensure_chunk_checkpoint_plan(&state, &plan, &cp, &mut summary, &[], "rivet.yaml")
            .expect_err("existing run without resume must error");
        let msg = format!("{:#}", err);
        assert!(msg.contains("still in progress"), "got: {msg}");
        assert!(msg.contains("--resume"), "must hint at --resume");
        assert!(msg.contains("reset-chunks"), "must hint at reset-chunks");
    }

    // ── record_chunked_commit ─────────────────────────────────────────────

    /// Picks the highest `chunk_index` among completed tasks and persists it
    /// as the committed boundary.
    #[test]
    fn record_chunked_commit_picks_highest_completed_chunk() {
        let state = StateStore::open_in_memory().unwrap();
        state.create_chunk_run("run-a", "orders", "h", 3).unwrap();
        state
            .insert_chunk_tasks("run-a", &[(1, 10), (11, 20), (21, 30)])
            .unwrap();
        state
            .complete_chunk_task("run-a", 0, 10, Some("c0.parquet"))
            .unwrap();
        state
            .complete_chunk_task("run-a", 2, 30, Some("c2.parquet"))
            .unwrap();

        record_chunked_commit(&state, "orders", "run-a");

        let p = state.get_progression("orders").unwrap();
        let boundary = p.committed.expect("must have committed boundary");
        assert_eq!(boundary.strategy, "chunked");
        assert_eq!(
            boundary.chunk_index,
            Some(2),
            "committed boundary must be the highest completed chunk index"
        );
    }

    /// No completed tasks at all: function must be a silent no-op
    /// (must NOT crash, must NOT write a bogus -1 sentinel).
    #[test]
    fn record_chunked_commit_with_no_completed_tasks_is_noop() {
        let state = StateStore::open_in_memory().unwrap();
        state.create_chunk_run("run-b", "orders", "h", 3).unwrap();
        state
            .insert_chunk_tasks("run-b", &[(1, 10), (11, 20)])
            .unwrap();

        record_chunked_commit(&state, "orders", "run-b");

        let p = state.get_progression("orders").unwrap();
        assert!(
            p.committed.is_none(),
            "no completed tasks → no committed boundary"
        );
    }
}
