//! **Layer: Execution** (with bounded persistence writes)
//!
//! Implements chunked extraction in three variants: sequential, parallel-simple,
//! and parallel-checkpoint. The module is split into focused sub-modules:
//!
//! - `math`    — pure functions: date parsing, range generation, SQL building
//! - `detect`  — chunk boundary detection via source queries
//! - `exec`    — stateless sequential and parallel execution
//! - `mod`     — checkpoint orchestration (this file)

mod detect;
mod exec;
pub(crate) mod math;

// ─── Re-exports for callers in pipeline:: ────────────────────────────────────

pub(crate) use detect::detect_and_generate_chunks;
pub(crate) use exec::run_chunked_parallel;
pub(crate) use exec::run_chunked_sequential;
pub use math::generate_chunks;
pub(crate) use math::{RIVET_CHUNK_RN_COL, build_chunk_query_sql, chunk_plan_fingerprint};

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

// ─── Checkpoint orchestration ────────────────────────────────────────────────

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::{
    RunSummary, journal::RunEvent, progress::ChunkProgress, retry::classify_error,
    sink::ExportSink, validate::validate_output,
};
use crate::error::Result;
use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::{destination, format, resource};

/// Extract the `ChunkedPlan` from a `ResolvedRunPlan`. Panics if the strategy
/// is not `Chunked` — all callers in this module only run for chunked plans.
fn chunked_plan(plan: &ResolvedRunPlan) -> &ChunkedPlan {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => cp,
        _ => unreachable!("chunked runner called with non-chunked plan"),
    }
}

fn ensure_chunk_checkpoint_plan(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    cp: &ChunkedPlan,
    summary: &mut RunSummary,
    chunks: &[(i64, i64)],
) -> Result<String> {
    let plan_hash = chunk_plan_fingerprint(
        &plan.base_query,
        &cp.column,
        cp.chunk_size,
        cp.dense,
        cp.by_days,
    );
    let max_att = cp.max_attempts;

    if plan.resume {
        let Some((rid, stored_hash)) = state.find_in_progress_chunk_run(&plan.export_name)? else {
            anyhow::bail!(
                "export '{}': --resume but no in-progress chunk checkpoint; run without --resume first or `rivet state reset-chunks --export {}`",
                plan.export_name,
                plan.export_name
            );
        };
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

    if let Some((rid, _)) = state.find_in_progress_chunk_run(&plan.export_name)? {
        anyhow::bail!(
            "export '{}': chunk checkpoint run '{}' still in progress; use `rivet run --resume` or `rivet state reset-chunks --export {}`",
            plan.export_name,
            rid,
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

fn export_one_chunk_range(
    src: &mut dyn Source,
    base_query: &str,
    cp: &ChunkedPlan,
    start: i64,
    end: i64,
    chunk_index: i64,
    plan: &ResolvedRunPlan,
) -> Result<(usize, Option<String>, u64)> {
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
    src.export(&chunk_query, None, None, &plan.tuning, &mut sink)?;
    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    if sink.total_rows == 0 {
        return Ok((0, None, 0));
    }

    if plan.validate {
        validate_output(sink.tmp.path(), plan.format, sink.total_rows)?;
    }
    let file_bytes = std::fs::metadata(sink.tmp.path())
        .map(|m| m.len())
        .unwrap_or(0);

    let fmt = format::create_format(plan.format, plan.compression, plan.compression_level);
    let file_name = format!(
        "{}_{}_chunk{}.{}",
        plan.export_name,
        chrono::Utc::now().format("%Y%m%d_%H%M%S"),
        chunk_index,
        fmt.file_extension()
    );
    let dest = destination::create_destination(&plan.destination)?;
    dest.write(sink.tmp.path(), &file_name)?;

    Ok((sink.total_rows, Some(file_name), file_bytes))
}

fn run_chunk_with_source_retries(
    base_query: &str,
    cp: &ChunkedPlan,
    start: i64,
    end: i64,
    chunk_index: i64,
    plan: &ResolvedRunPlan,
) -> Result<(usize, Option<String>, u64)> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=plan.tuning.max_retries {
        if attempt > 0 {
            let (_, _needs_reconnect, extra_delay) = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or((false, false, 0));
            let backoff = plan.tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + extra_delay;
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
                let (transient, _, _) = classify_error(&e);
                if attempt < plan.tuning.max_retries && transient {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        };

        match export_one_chunk_range(&mut *src, base_query, cp, start, end, chunk_index, plan) {
            Ok(v) => return Ok(v),
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < plan.tuning.max_retries && transient {
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
    _config_path: &str,
    chunk_source: ChunkSource,
) -> Result<()> {
    let cp = chunked_plan(plan);

    let chunks = if plan.resume {
        vec![]
    } else {
        match chunk_source {
            ChunkSource::Detect => detect_and_generate_chunks(
                src,
                &plan.base_query,
                &cp.column,
                cp.chunk_size,
                &plan.export_name,
                cp.dense,
                cp.by_days,
                plan.source.source_type,
            )?,
            ChunkSource::Precomputed(ranges) => ranges,
        }
    };

    let run_id = ensure_chunk_checkpoint_plan(state, plan, cp, summary, &chunks)?;

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

        match run_chunk_with_source_retries(&plan.base_query, cp, start, end, chunk_index, plan) {
            Ok((rows, fname, file_bytes)) => {
                summary.total_rows += rows as i64;
                pb.inc(summary.total_rows);
                if rows > 0 {
                    summary.bytes_written += file_bytes;
                    summary.files_produced += 1;
                    if let Some(name) = &fname
                        && let Err(e) = state.record_file(
                            &summary.run_id,
                            &plan.export_name,
                            name,
                            rows as i64,
                            file_bytes as i64,
                            plan.format.label(),
                            Some(plan.compression.label()),
                        )
                    {
                        log::warn!(
                            "export '{}': manifest write failed for checkpoint chunk '{}' (file was produced): {:#}",
                            plan.export_name,
                            name,
                            e
                        );
                    }
                }
                summary.journal.record(RunEvent::ChunkCompleted {
                    chunk_index,
                    rows: rows as i64,
                    file_name: fname.clone(),
                });
                state.complete_chunk_task(&run_id, chunk_index, rows as i64, fname.as_deref())?;
            }
            Err(e) => {
                let msg = format!("{:#}", e);
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
            "export '{}': chunk checkpoint incomplete ({} tasks not completed); fix errors and `rivet run --resume` or `rivet state reset-chunks`",
            plan.export_name,
            pending
        );
    }

    pb.finish(summary.total_rows);
    state.finalize_chunk_run_completed(&run_id)?;
    log::info!(
        "export '{}': chunk checkpoint run completed (run_id={})",
        plan.export_name,
        run_id
    );
    Ok(())
}

pub(super) fn run_chunked_parallel_checkpoint(
    config_path: &str,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    chunk_source: ChunkSource,
) -> Result<()> {
    let cp = chunked_plan(plan);

    let chunks = if plan.resume {
        vec![]
    } else {
        match chunk_source {
            ChunkSource::Detect => {
                let mut src = source::create_source(&plan.source)?;
                detect_and_generate_chunks(
                    &mut *src,
                    &plan.base_query,
                    &cp.column,
                    cp.chunk_size,
                    &plan.export_name,
                    cp.dense,
                    cp.by_days,
                    plan.source.source_type,
                )?
            }
            ChunkSource::Precomputed(ranges) => ranges,
        }
    };

    let run_id = ensure_chunk_checkpoint_plan(state, plan, cp, summary, &chunks)?;

    let total_tasks = {
        let tasks = state.list_chunk_tasks_for_run(&run_id)?;
        tasks.len().max(1)
    };
    let parallel = cp.parallel.min(total_tasks);
    let pb_cp = ChunkProgress::new(&plan.export_name, total_tasks);
    let pb_cp_arc = pb_cp.arc();
    log::info!(
        "export '{}': chunk checkpoint parallel: {} workers, run_id={}",
        plan.export_name,
        parallel,
        run_id
    );

    let db_path = state.database_path().to_path_buf();
    let run_id_arc = std::sync::Arc::new(run_id.clone());
    let agg_rows = std::sync::atomic::AtomicI64::new(0);
    let agg_bytes = std::sync::atomic::AtomicU64::new(0);
    let agg_files = std::sync::atomic::AtomicUsize::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());

    let plan_for_workers = plan.clone();
    let cp_for_workers = cp.clone();
    let config_path_owned = config_path.to_string();
    let fmt_label = plan.format.label();
    let comp_label = plan.compression.label();

    std::thread::scope(|s| {
        for _ in 0..parallel {
            let db_path = db_path.clone();
            let run_id_arc = std::sync::Arc::clone(&run_id_arc);
            let agg_rows = &agg_rows;
            let agg_bytes = &agg_bytes;
            let agg_files = &agg_files;
            let errors = &errors;
            let plan_w = plan_for_workers.clone();
            let cp_w = cp_for_workers.clone();
            let config_path_w = config_path_owned.clone();
            let fmt_label_w = fmt_label;
            let comp_label_w = comp_label;
            let pb_w = std::sync::Arc::clone(&pb_cp_arc);

            s.spawn(move || {
                loop {
                    let claimed = match StateStore::claim_next_chunk_task_at_path(
                        &db_path,
                        run_id_arc.as_str(),
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            errors
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push(format!("claim error: {:#}", e));
                            break;
                        }
                    };
                    let Some((chunk_index, sk, ek)) = claimed else {
                        break;
                    };

                    if !resource::check_memory(plan_w.tuning.memory_threshold_mb) {
                        log::warn!("memory threshold exceeded in worker; pausing 2s");
                        std::thread::sleep(Duration::from_secs(2));
                    }

                    let start: i64 = match sk.parse() {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = StateStore::fail_chunk_task_at_path(
                                &db_path,
                                run_id_arc.as_str(),
                                chunk_index,
                                "invalid start_key",
                            );
                            continue;
                        }
                    };
                    let end: i64 = match ek.parse() {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = StateStore::fail_chunk_task_at_path(
                                &db_path,
                                run_id_arc.as_str(),
                                chunk_index,
                                "invalid end_key",
                            );
                            continue;
                        }
                    };

                    let chunk_query = build_chunk_query_sql(
                        &plan_w.base_query,
                        &cp_w.column,
                        start,
                        end,
                        cp_w.dense,
                        cp_w.by_days.is_some(),
                        plan_w.source.source_type,
                    );

                    let result = (|| -> Result<(usize, Option<String>, u64)> {
                        let mut last_err: Option<anyhow::Error> = None;
                        for attempt in 0..=plan_w.tuning.max_retries {
                            if attempt > 0 {
                                let (_, _, extra_delay) = last_err
                                    .as_ref()
                                    .map(classify_error)
                                    .unwrap_or((false, false, 0));
                                let backoff = plan_w.tuning.retry_backoff_ms
                                    * 2u64.pow(attempt - 1)
                                    + extra_delay;
                                std::thread::sleep(Duration::from_millis(backoff));
                            }

                            let mut thread_src = match source::create_source(&plan_w.source) {
                                Ok(s) => s,
                                Err(e) => {
                                    let (transient, _, _) = classify_error(&e);
                                    if attempt < plan_w.tuning.max_retries && transient {
                                        last_err = Some(e);
                                        continue;
                                    }
                                    return Err(e);
                                }
                            };

                            let mut sink = ExportSink::new(&plan_w)?;

                            let export_attempt = (|| -> Result<(usize, Option<String>, u64)> {
                                thread_src.export(
                                    &chunk_query,
                                    None,
                                    None,
                                    &plan_w.tuning,
                                    &mut sink,
                                )?;
                                if let Some(w) = sink.writer.take() {
                                    w.finish()?;
                                }
                                if sink.total_rows == 0 {
                                    return Ok((0, None, 0));
                                }
                                if plan_w.validate {
                                    validate_output(
                                        sink.tmp.path(),
                                        plan_w.format,
                                        sink.total_rows,
                                    )?;
                                }
                                let file_bytes = std::fs::metadata(sink.tmp.path())
                                    .map(|m| m.len())
                                    .unwrap_or(0);
                                let fmt = format::create_format(
                                    plan_w.format,
                                    plan_w.compression,
                                    plan_w.compression_level,
                                );
                                let file_name = format!(
                                    "{}_{}_chunk{}.{}",
                                    plan_w.export_name,
                                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                                    chunk_index,
                                    fmt.file_extension()
                                );
                                let dest = destination::create_destination(&plan_w.destination)?;
                                dest.write(sink.tmp.path(), &file_name)?;
                                Ok((sink.total_rows, Some(file_name), file_bytes))
                            })();

                            match export_attempt {
                                Ok(v) => return Ok(v),
                                Err(e) => {
                                    let (transient, _, _) = classify_error(&e);
                                    if attempt < plan_w.tuning.max_retries && transient {
                                        last_err = Some(e);
                                        continue;
                                    }
                                    return Err(e);
                                }
                            }
                        }
                        Err(last_err
                            .unwrap_or_else(|| anyhow::anyhow!("chunk failed after retries")))
                    })();

                    match result {
                        Ok((rows, fname, file_bytes)) => {
                            agg_rows.fetch_add(rows as i64, Ordering::Relaxed);
                            if rows > 0 {
                                agg_bytes.fetch_add(file_bytes, Ordering::Relaxed);
                                agg_files.fetch_add(1, Ordering::Relaxed);
                                if let Some(name) = fname.as_ref() {
                                    match StateStore::open(&config_path_w) {
                                        Ok(store) => {
                                            if let Err(e) = store.record_file(
                                                run_id_arc.as_str(),
                                                &plan_w.export_name,
                                                name,
                                                rows as i64,
                                                file_bytes as i64,
                                                fmt_label_w,
                                                Some(comp_label_w),
                                            ) {
                                                log::warn!(
                                                    "export '{}': manifest write failed for parallel checkpoint chunk '{}' (file was produced): {:#}",
                                                    plan_w.export_name,
                                                    name,
                                                    e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            log::warn!(
                                                "export '{}': could not open state DB for manifest write of '{}': {:#}",
                                                plan_w.export_name,
                                                name,
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            let _ = StateStore::complete_chunk_task_at_path(
                                &db_path,
                                run_id_arc.as_str(),
                                chunk_index,
                                rows as i64,
                                fname.as_deref(),
                            );
                            pb_w.set_message(format!("{} rows", agg_rows.load(Ordering::Relaxed)));
                            pb_w.inc(1);
                        }
                        Err(e) => {
                            let msg = format!("{:#}", e);
                            let _ = StateStore::fail_chunk_task_at_path(
                                &db_path,
                                run_id_arc.as_str(),
                                chunk_index,
                                &msg,
                            );
                            errors
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push(format!("chunk {}: {}", chunk_index, msg));
                        }
                    }
                }
            });
        }
    });

    summary.total_rows = agg_rows.load(Ordering::Relaxed);
    summary.bytes_written = agg_bytes.load(Ordering::Relaxed);
    summary.files_produced = agg_files.load(Ordering::Relaxed);
    pb_cp.finish(summary.total_rows);
    if plan.validate {
        summary.validated = Some(true);
    }

    let errs = errors.into_inner().unwrap_or_else(|e| e.into_inner());
    if !errs.is_empty() {
        anyhow::bail!(
            "export '{}': parallel checkpoint worker errors:\n{}",
            plan.export_name,
            errs.join("\n")
        );
    }

    let pending = state.count_chunk_tasks_not_completed(&run_id)?;
    if pending > 0 {
        anyhow::bail!(
            "export '{}': {} chunk task(s) not completed; `rivet run --resume` or inspect `rivet state chunks --export {}`",
            plan.export_name,
            pending,
            plan.export_name
        );
    }

    state.finalize_chunk_run_completed(&run_id)?;
    log::info!(
        "export '{}': chunk checkpoint parallel run completed",
        plan.export_name
    );
    Ok(())
}
