//! Multi-worker chunk-checkpoint runner — twin of `sequential_checkpoint.rs`.
//!
//! `run_chunked_parallel_checkpoint` spawns `cp.parallel` worker threads
//! inside a `std::thread::scope` and lets them race to claim chunk tasks
//! from the state DB. Each worker opens its own source connection (ADR-0011
//! `Source: Send` not `Sync`), runs the chunk with per-attempt retry, and
//! commits per-chunk state directly via the `StateStore::*_at_ref`
//! ref-based helpers so all workers share a single SQLite connection.
//!
//! Same fault-injection hooks (`after_chunk_file:N`,
//! `after_chunk_complete:N`) as the sequential path — see the doc-comment
//! inside the worker closure. Live coverage: `live_chunked_recovery.rs` C3
//! and C4 (`mysql_chunked_recovery` mirrors).
//!
//! Shared chunked-orchestration helpers (`chunked_plan`, `config_hint`,
//! `ensure_chunk_checkpoint_plan`, `record_chunked_commit`) live in
//! [`super`]. The sequential runner lives in [`super::sequential_checkpoint`].

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::{
    RunSummary, progress::ChunkProgress, retry::classify_error, sink::ExportSink,
    validate::validate_output,
};
use super::{
    ChunkSource, chunked_plan, config_hint, detect_and_generate_chunks,
    ensure_chunk_checkpoint_plan, record_chunked_commit,
};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::source;
use crate::state::StateStore;
use crate::{destination, format, resource};

use super::math::build_chunk_query_sql;

pub(crate) fn run_chunked_parallel_checkpoint(
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
                    cp.chunk_count,
                    &plan.export_name,
                    cp.dense,
                    cp.by_days,
                    plan.source.source_type,
                )?
            }
            ChunkSource::Precomputed(ranges) => ranges,
        }
    };

    let run_id = ensure_chunk_checkpoint_plan(state, plan, cp, summary, &chunks, config_path)?;

    // ADR-0012 M8: when resuming a chunked run, reconcile the destination's
    // prior-run manifest with the local chunk_task state.  Parts whose
    // manifest entry diverges from what's actually on the destination
    // (missing object, size drift) get their chunk_task reset to `pending`
    // so the worker loop below re-exports them.  No-op for fresh prefixes
    // and pre-0.7.0 destinations.  See `pipeline/chunked/resume_m8.rs`.
    if plan.resume {
        let _stats = super::apply_m8_resume_decisions(state, &run_id, plan, summary)?;
    }

    let total_tasks = {
        let tasks = state.list_chunk_tasks_for_run(&run_id)?;
        tasks.len().max(1)
    };
    let parallel = cp.parallel.min(total_tasks);
    let pb_cp = ChunkProgress::new(&plan.export_name, total_tasks);
    let pb_cp_handle = pb_cp.handle();
    log::info!(
        "export '{}': chunk checkpoint parallel: {} workers, run_id={}",
        plan.export_name,
        parallel,
        run_id
    );

    let state_ref = state.state_ref().clone();
    let run_id_arc = std::sync::Arc::new(run_id.clone());
    let agg_rows = std::sync::atomic::AtomicI64::new(0);
    let agg_bytes = std::sync::atomic::AtomicU64::new(0);
    let agg_files = std::sync::atomic::AtomicUsize::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());
    // ADR-0012 M3: schema fingerprint captured once across workers.  None
    // until any worker exports a non-empty chunk and resolves the dest schema.
    let shared_fingerprint: std::sync::OnceLock<String> = std::sync::OnceLock::new();

    let plan_for_workers = plan.clone();
    let cp_for_workers = cp.clone();
    let config_path_owned = config_path.to_string();
    let fmt_label = plan.format.label();
    let comp_label = plan.compression.label();

    let shared_destination =
        std::sync::Arc::new(destination::create_destination(&plan.destination)?);
    destination::log_capabilities(
        &plan.export_name,
        &**shared_destination,
        plan.tuning.max_retries,
    );

    std::thread::scope(|s| {
        for _ in 0..parallel {
            let state_ref = state_ref.clone();
            let shared_destination = std::sync::Arc::clone(&shared_destination);
            let run_id_arc = std::sync::Arc::clone(&run_id_arc);
            let agg_rows = &agg_rows;
            let agg_bytes = &agg_bytes;
            let agg_files = &agg_files;
            let errors = &errors;
            let shared_fingerprint = &shared_fingerprint;
            let plan_w = plan_for_workers.clone();
            let cp_w = cp_for_workers.clone();
            let config_path_w = config_path_owned.clone();
            let fmt_label_w = fmt_label;
            let comp_label_w = comp_label;
            let pb_w = pb_cp_handle.clone();

            s.spawn(move || {
                let shared_destination = shared_destination;
                loop {
                    let claimed = match StateStore::claim_next_chunk_task_at_ref(
                        &state_ref,
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
                            let _ = StateStore::fail_chunk_task_at_ref(
                                &state_ref,
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
                            let _ = StateStore::fail_chunk_task_at_ref(
                                &state_ref,
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
                                let extra_delay = last_err
                                    .as_ref()
                                    .map(classify_error)
                                    .map(|c| c.extra_delay_ms())
                                    .unwrap_or(0);
                                let backoff = plan_w.tuning.retry_backoff_ms
                                    * 2u64.pow(attempt - 1)
                                    + extra_delay;
                                std::thread::sleep(Duration::from_millis(backoff));
                            }

                            let mut thread_src = match source::create_source(&plan_w.source) {
                                Ok(s) => s,
                                Err(e) => {
                                    if attempt < plan_w.tuning.max_retries
                                        && classify_error(&e).is_transient()
                                    {
                                        last_err = Some(e);
                                        continue;
                                    }
                                    return Err(e);
                                }
                            };

                            let mut sink = ExportSink::new(&plan_w)?;

                            let export_attempt = (|| -> Result<(usize, Option<String>, u64)> {
                                thread_src.export(
                                    &source::ExportRequest {
                                        query: &chunk_query,
                                        incremental: None,
                                        cursor: None,
                                        tuning: &plan_w.tuning,
                                        column_overrides: &plan_w.column_overrides,
                                    },
                                    &mut sink,
                                )?;
                                if let Some(w) = sink.writer.take() {
                                    w.finish()?;
                                }
                                // ADR-0012 M3: fingerprint the schema as soon
                                // as the sink resolves it.  Race-free across
                                // workers thanks to OnceLock::set semantics.
                                if let Some(s) = sink.dest_schema.as_deref() {
                                    let columns = crate::state::arrow_schema_to_columns(s);
                                    let _ = shared_fingerprint
                                        .set(crate::state::schema_fingerprint(&columns));
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
                                    None,
                                );
                                let file_name = format!(
                                    "{}_{}_chunk{}.{}",
                                    plan_w.export_name,
                                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                                    chunk_index,
                                    fmt.file_extension()
                                );
                                shared_destination.write(sink.tmp.path(), &file_name)?;
                                Ok((sink.total_rows, Some(file_name), file_bytes))
                            })();

                            match export_attempt {
                                Ok(v) => return Ok(v),
                                Err(e) => {
                                    if attempt < plan_w.tuning.max_retries
                                        && classify_error(&e).is_transient()
                                    {
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
                            // Mirror of the sequential checkpoint hooks (search for
                            // `maybe_panic_at_chunk` in sequential_checkpoint.rs): same
                            // fault-point names so `RIVET_TEST_PANIC_AT=after_chunk_file:N`
                            // and `…=after_chunk_complete:N` exercise both code paths.
                            // The panic propagates through `std::thread::scope`'s join,
                            // crashing the process and leaving any in-flight workers'
                            // chunk_task rows as 'running' for the resume path to reset.
                            crate::test_hook::maybe_panic_at_chunk("after_chunk_file", chunk_index);
                            let _ = StateStore::complete_chunk_task_at_ref(
                                &state_ref,
                                run_id_arc.as_str(),
                                chunk_index,
                                rows as i64,
                                fname.as_deref(),
                            );
                            crate::test_hook::maybe_panic_at_chunk(
                                "after_chunk_complete",
                                chunk_index,
                            );
                            pb_w.inc(agg_rows.load(Ordering::Relaxed));
                        }
                        Err(e) => {
                            let msg = format!("{:#}", e);
                            let _ = StateStore::fail_chunk_task_at_ref(
                                &state_ref,
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
    if let Some(fp) = shared_fingerprint.into_inner() {
        summary.schema_fingerprint = Some(fp);
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
            "export '{}': {} chunk task(s) not completed; `rivet run {} --export {} --resume` or inspect `rivet state chunks {} --export {}`",
            plan.export_name,
            pending,
            config_hint(config_path),
            plan.export_name,
            config_hint(config_path),
            plan.export_name
        );
    }

    state.finalize_chunk_run_completed(&run_id)?;
    record_chunked_commit(state, &plan.export_name, &run_id);
    log::info!(
        "export '{}': chunk checkpoint parallel run completed",
        plan.export_name
    );
    Ok(())
}
