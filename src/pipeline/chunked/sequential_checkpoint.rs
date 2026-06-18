//! Single-threaded chunk-checkpoint runner.
//!
//! `run_chunked_sequential_checkpoint` claims chunk tasks from the state DB
//! one at a time, runs each through `export_one_chunk_range` (with its own
//! retry wrapper `run_chunk_with_source_retries`), and records progress in
//! the manifest + chunk_task tables. This is the path used when `parallel: 1`
//! or when `chunk_checkpoint: true` is set on a non-parallel export.
//!
//! Shared chunked-orchestration helpers (`chunked_plan`, `config_hint`,
//! `ensure_chunk_checkpoint_plan`, `record_chunked_commit`) live in
//! [`super`]. The parallel-worker checkpoint loop lives in
//! [`super::parallel_checkpoint`].

use std::time::Duration;

use super::super::{
    RunSummary,
    progress::ChunkProgress,
    retry::{RetryClass, classify_error},
    sink::ExportSink,
};
use super::{ChunkSource, chunked_plan, config_hint, ensure_chunk_checkpoint_plan};
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::{ChunkedPlan, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::{destination, format, resource};

use super::math::build_chunk_query_sql;

/// Returns `(rows, parts)` — one [`PartRecord`] per written part: empty for
/// an empty chunk, several when `max_file_size` rotation split the chunk.
/// Uses [`commit::write_sink_parts`] for the worker-safe I1 → dest.write →
/// fingerprint half; the caller threads each record through
/// [`commit::record_part`].
#[allow(clippy::too_many_arguments)] // chunk + plan + summary threading is the actual arity here
fn export_one_chunk_range(
    src: &mut dyn Source,
    base_query: &str,
    cp: &ChunkedPlan,
    start: i64,
    end: i64,
    chunk_index: i64,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<(usize, Vec<super::super::commit::PartRecord>)> {
    let chunk_query = build_chunk_query_sql(
        base_query,
        &cp.column,
        start,
        end,
        cp.dense,
        cp.by_days.is_some(),
        plan.source.source_type,
    );

    let mut sink = ExportSink::new(plan)?;
    src.export(
        &source::ExportRequest::wrapped(
            &chunk_query,
            base_query,
            &plan.tuning,
            &plan.column_overrides,
        ),
        &mut sink,
    )?;
    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }
    // ADR-0012 M3: capture the dest schema fingerprint as soon as the sink
    // resolves it.  Idempotent: the helper no-ops once `summary` already
    // carries one, and the schema is identical across chunks of one run.
    if let Some(s) = sink.dest_schema.as_deref() {
        super::super::manifest_writer::record_run_schema_fingerprint(summary, s);
    }

    if sink.total_rows == 0 {
        return Ok((0, Vec::new()));
    }

    let fmt = format::create_format(plan.format, plan.compression, plan.compression_level, None);
    let base = super::chunk_part_filename(&plan.export_name, chunk_index, fmt.file_extension());
    let dest = destination::create_destination(&plan.destination)?;
    // Worker-safe half of commit (I1 + dest.write + fingerprint), draining
    // every part the sink produced (max_file_size rotation included).
    let recs = super::super::commit::write_sink_parts(
        dest.as_ref(),
        &mut sink,
        plan.validate.then_some(plan.format),
        |idx, count| super::super::commit::part_indexed_name(&base, idx, count),
    )?;
    if plan.validate {
        summary.validated = Some(true);
    }

    Ok((sink.total_rows, recs))
}

#[allow(clippy::too_many_arguments)] // mirrors export_one_chunk_range's arity for retry wrapping
fn run_chunk_with_source_retries(
    base_query: &str,
    cp: &ChunkedPlan,
    start: i64,
    end: i64,
    chunk_index: i64,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<(usize, Vec<super::super::commit::PartRecord>)> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=plan.tuning.max_retries {
        if attempt > 0 {
            // Bump the per-run retry counter on every retry attempt so the
            // console summary card and `rivet metrics` show how often the
            // export had to re-try (it would otherwise stay at 0, masking
            // a flaky link that worked only because backoff covered for it).
            summary.retries = summary.retries.saturating_add(1);
            let class = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or(RetryClass::Permanent);
            let backoff =
                plan.tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + class.extra_delay_ms();
            log::warn!(
                "export '{}': chunk {} retry {}/{} in {}ms",
                plan.export_name,
                chunk_index,
                attempt,
                plan.tuning.max_retries,
                backoff
            );
            std::thread::sleep(Duration::from_millis(backoff));
        }

        let mut src = match source::create_source(&plan.source) {
            Ok(s) => s,
            Err(e) => {
                if attempt < plan.tuning.max_retries && classify_error(&e).is_transient() {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        };

        match export_one_chunk_range(
            &mut *src,
            base_query,
            cp,
            start,
            end,
            chunk_index,
            plan,
            summary,
        ) {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt < plan.tuning.max_retries && classify_error(&e).is_transient() {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk export failed after retries")))
}

pub(crate) fn run_chunked_sequential_checkpoint(
    src: &mut dyn Source,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    config_path: &str,
    chunk_source: ChunkSource,
) -> Result<()> {
    let cp = chunked_plan(plan);

    let chunks = if plan.resume {
        vec![]
    } else {
        match chunk_source {
            // Detect: compute ranges + run the pre-chunk drift check (ADR-0021).
            ChunkSource::Detect => super::prepare_chunk_plan(src, plan, Some(state), summary)?,
            ChunkSource::Precomputed(ranges) => ranges,
        }
    };

    let run_id = ensure_chunk_checkpoint_plan(state, plan, cp, summary, &chunks, config_path)?;

    // ADR-0012 M8: same manifest-aware preamble as the parallel runner —
    // reconcile destination state with chunk_task table before claiming work.
    if plan.resume {
        let _stats = super::apply_m8_resume_decisions(state, &run_id, plan, summary)?;
    }

    let total_tasks = state.count_chunk_tasks_total(&run_id).unwrap_or(1);
    let pb = ChunkProgress::new(&plan.export_name, total_tasks);

    if !plan.resume && !resource::check_memory(plan.tuning.memory_threshold_mb) {
        log::warn!("memory threshold exceeded before chunk export; pausing 5s");
        std::thread::sleep(Duration::from_secs(5));
    }

    while let Some((chunk_index, sk, ek)) = state.claim_next_chunk_task(&run_id)? {
        if !resource::check_memory(plan.tuning.memory_threshold_mb) {
            log::warn!(
                "memory threshold exceeded, pausing 5s before chunk {}",
                chunk_index
            );
            std::thread::sleep(Duration::from_secs(5));
        }

        let start: i64 = sk
            .parse()
            .map_err(|_| anyhow::anyhow!("chunk {}: invalid start_key {:?}", chunk_index, sk))?;
        let end: i64 = ek
            .parse()
            .map_err(|_| anyhow::anyhow!("chunk {}: invalid end_key {:?}", chunk_index, ek))?;

        log::info!(
            "export '{}': checkpoint chunk {} ({}..{})",
            plan.export_name,
            chunk_index,
            start,
            end
        );

        summary.journal.record(RunEvent::ChunkStarted {
            chunk_index,
            start_key: sk.clone(),
            end_key: ek.clone(),
        });

        match run_chunk_with_source_retries(
            &plan.base_query,
            cp,
            start,
            end,
            chunk_index,
            plan,
            summary,
        ) {
            Ok((rows, parts)) => {
                summary.total_rows += rows as i64;
                pb.inc(summary.total_rows);
                // Shared commit path for the non-empty branch (I2/M1 + counters
                // + ChunkCompleted journal + I7 file-log + fault hooks).
                // Empty chunks have no file to record but still need to journal
                // completion + finalize the chunk_task so the run accounts for
                // every claimed index.
                let fname: Option<String> = if parts.is_empty() {
                    summary.journal.record(RunEvent::ChunkCompleted {
                        chunk_index,
                        rows: 0,
                        file_name: None,
                    });
                    None
                } else {
                    for rec in &parts {
                        super::super::commit::record_part(
                            plan,
                            summary,
                            Some(state),
                            rec,
                            super::super::commit::PartKind::Chunk {
                                chunk_index,
                                start_key: start,
                                end_key: end,
                            },
                        );
                    }
                    // chunk_task carries one file name; for a rotation-split
                    // chunk store the first sibling. The manifest records all
                    // siblings, so a missing one fails destination verification
                    // loudly instead of being silently skipped on resume.
                    Some(parts[0].file_name.clone())
                };
                crate::test_hook::maybe_panic_at_chunk("after_chunk_file", chunk_index);
                state.complete_chunk_task(&run_id, chunk_index, rows as i64, fname.as_deref())?;
                crate::test_hook::maybe_panic_at_chunk("after_chunk_complete", chunk_index);
            }
            Err(e) => {
                let msg = crate::redact::redact_error(&e);
                log::error!(
                    "export '{}': chunk {} failed: {}",
                    plan.export_name,
                    chunk_index,
                    msg
                );
                summary.journal.record(RunEvent::ChunkFailed {
                    chunk_index,
                    error: msg.clone(),
                    attempt: 1,
                });
                state.fail_chunk_task(&run_id, chunk_index, &msg)?;
            }
        }
    }

    let pending = state.count_chunk_tasks_not_completed(&run_id)?;
    if pending > 0 {
        anyhow::bail!(
            "export '{}': chunk checkpoint incomplete ({} tasks not completed); fix errors and `rivet run {} --export {} --resume` or `rivet state reset-chunks {} --export {}`",
            plan.export_name,
            pending,
            config_hint(config_path),
            plan.export_name,
            config_hint(config_path),
            plan.export_name
        );
    }

    pb.finish(summary.total_rows);
    state.finalize_chunk_run_completed(&run_id)?;
    // ADR-0008 PG2 committed boundary via the shared finalize seam — the
    // chunked variant walks chunk_task internally. See
    // `super::super::run_store::RunStore`.
    super::super::run_store::RunStore::finalize(state, plan, summary)
        .with_progression(super::super::run_store::Progression::Chunked)
        .commit()?;
    log::info!(
        "export '{}': chunk checkpoint run completed (run_id={})",
        plan.export_name,
        run_id
    );
    Ok(())
}
