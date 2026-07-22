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

use super::super::{RunSummary, progress::ChunkProgress, retry::classify_error, sink::ExportSink};
use super::poison;
use super::{ChunkSource, chunked_plan, config_hint, ensure_chunk_checkpoint_plan};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::source;
use crate::state::StateStore;
use crate::{destination, format, resource};

use super::math::build_chunk_query_sql;

/// One chunk's worker result: (rows, part records, this chunk's Form B per-column
/// checksums, the checksum key-column name). Named so the retry/export closures
/// don't trip clippy::type_complexity on the 4-tuple.
type ChunkOutcome = (
    usize,
    Vec<super::super::commit::PartRecord>,
    std::collections::BTreeMap<String, u64>,
    Option<String>,
);

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
            // Detect: a short-lived connection computes ranges + runs the
            // pre-chunk drift check (ADR-0021), then closes before workers spawn.
            ChunkSource::Detect => super::prepare_chunk_plan_fresh(plan, state, summary)?,
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
    // Rows streamed across ALL tasks (completed + in-flight) — drives the
    // per-batch progress feed so the bar ticks during a chunk's read.
    let streamed_rows = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    // Per-attempt retry counter bumped from inside each worker's retry
    // loop and folded into `summary.retries` after the scope joins, so the
    // console summary card / `rivet metrics` / `export_metrics.retries`
    // reflect chunked-parallel retries the same way the sequential path
    // already does.
    let agg_retries = std::sync::atomic::AtomicU32::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());
    // PartRecords pushed by workers, drained post-scope through
    // `commit::record_part` so I2/M1 + counters + journal + I7 + fault hooks
    // live once in the seam. Before this, workers opened a fresh StateStore
    // per chunk just to call `record_file` and *no one* populated
    // `summary.manifest_parts` — so the cloud manifest (ADR-0012 M1) was
    // silently empty for every `parallel>1 + chunk_checkpoint:true` run.
    // Tuple second is the chunk_index used by the ChunkCompleted journal.
    let file_records: std::sync::Mutex<Vec<(super::super::commit::PartRecord, i64)>> =
        std::sync::Mutex::new(Vec::new());
    // Form B: each worker pushes its chunk's checksums here; the parent XOR-combines
    // them run-wide post-join and harvests once — so the CHECKPOINT parallel path
    // records Form B like exec.rs's parallel path (graph-surfaced runner-bypass).
    #[allow(clippy::type_complexity)]
    let checksums_shared: std::sync::Mutex<
        Vec<(std::collections::BTreeMap<String, u64>, Option<String>)>,
    > = std::sync::Mutex::new(Vec::new());
    // ADR-0012 M3: schema fingerprint captured once across workers.  None
    // until any worker exports a non-empty chunk and resolves the dest schema.
    let shared_fingerprint: std::sync::OnceLock<String> = std::sync::OnceLock::new();

    let plan_for_workers = plan.clone();
    let cp_for_workers = cp.clone();
    // Per-chunk file_log writes need a state path + label strings the workers
    // capture into their thread closure. Each worker opens its own StateStore
    // per chunk against the same on-disk SQLite (the connection is not Sync).
    // The post-scope drain uses the main-thread `state` reference + state=None
    // to commit::record_part so file_log is not double-written.
    let config_path_owned = config_path.to_string();
    let fmt_label = plan.format.label();
    let comp_label = plan.compression.label();

    let shared_destination =
        std::sync::Arc::new(destination::create_destination(&plan.destination)?);
    destination::log_capabilities(
        &plan.export_name,
        &**shared_destination,
        plan.destination.destination_type,
        plan.tuning.max_retries,
    );

    std::thread::scope(|s| {
        for _ in 0..parallel {
            let state_ref = state_ref.clone();
            let shared_destination = std::sync::Arc::clone(&shared_destination);
            let run_id_arc = std::sync::Arc::clone(&run_id_arc);
            let agg_rows = &agg_rows;
            let agg_retries = &agg_retries;
            let errors = &errors;
            let file_records = &file_records;
            let checksums_shared = &checksums_shared;
            let shared_fingerprint = &shared_fingerprint;
            let plan_w = plan_for_workers.clone();
            let cp_w = cp_for_workers.clone();
            let config_path_w = config_path_owned.clone();
            let fmt_label_w = fmt_label;
            let comp_label_w = comp_label;
            let pb_w = pb_cp_handle.clone();
            let streamed_rows = std::sync::Arc::clone(&streamed_rows);

            s.spawn(move || {
                let shared_destination = shared_destination;
                loop {
                    let claimed = match StateStore::claim_next_chunk_task_at_ref(
                        &state_ref,
                        run_id_arc.as_str(),
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            poison::lock_recover(errors)
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

                    let result = (|| -> Result<ChunkOutcome> {
                        let mut last_err: Option<anyhow::Error> = None;
                        for attempt in 0..=plan_w.tuning.max_retries {
                            if attempt > 0 {
                                // Bump the shared retry counter so summary
                                // card + metrics see chunked-parallel retries
                                // (sequential path bumps `summary.retries`
                                // directly; here we go through the atomic
                                // and fold once after the scope joins).
                                agg_retries
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                                    // Round-2 audit #3/#4: carry the doctor/auth-TLS
                                    // connect hint on the final (non-transient) worker
                                    // connect failure, matching single.rs:93.
                                    return Err(crate::pipeline::single::attach_connect_hint(
                                        e,
                                        &plan_w.source,
                                    ));
                                }
                            };

                            let mut sink = ExportSink::new(&plan_w)?
                                .with_row_progress(
                                    pb_w.clone(),
                                    std::sync::Arc::clone(&streamed_rows),
                                );

                            let export_attempt = (|| -> Result<ChunkOutcome> {
                                thread_src.export(
                                    &source::ExportRequest::wrapped(
                                        &chunk_query,
                                        &plan_w.base_query,
                                        &plan_w.tuning,
                                        &plan_w.column_overrides,
                                    ),
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
                                    return Ok((0, Vec::new(), std::collections::BTreeMap::new(), None));
                                }
                                let fmt = format::create_format(
                                    plan_w.format,
                                    plan_w.compression,
                                    plan_w.compression_level,
                                    None,
                                );
                                let base = super::chunk_part_filename(
                                    &plan_w.export_name,
                                    chunk_index,
                                    fmt.file_extension(),
                                );
                                // Worker-safe half of commit (I1 + dest.write
                                // + fingerprint), draining every part the sink
                                // produced (max_file_size rotation included).
                                // The parent drains each PartRecord through
                                // commit::record_part post-scope.
                                let recs = super::super::commit::write_sink_parts(
                                    &**shared_destination,
                                    &mut sink,
                                    plan_w.validate.then_some(plan_w.format),
                                    |idx, count| {
                                        super::super::commit::part_indexed_name(&base, idx, count)
                                    },
                                )?;
                                let key = sink.checksum_key_col.and(sink.cursor_column.clone());
                                Ok((
                                    sink.total_rows,
                                    recs,
                                    std::mem::take(&mut sink.column_checksums),
                                    key,
                                ))
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
                        Ok((rows, parts, chunk_checksums, chunk_key)) => {
                            agg_rows.fetch_add(rows as i64, Ordering::Relaxed);
                            // Non-empty chunk: write file_log NOW (per-chunk
                            // durable manifest — the recovery flows in
                            // live_chunked_recovery.rs C3 read it after a
                            // mid-run crash to reconstruct the manifest) AND
                            // push each PartRecord for the parent drain. The
                            // drain calls commit::record_part(state=None) so
                            // file_log is not double-written; manifest_parts +
                            // counters + journal still get populated for the
                            // cloud manifest M1 contract.
                            let fname_for_state: Option<String> = if parts.is_empty() {
                                None
                            } else {
                                match StateStore::open(&config_path_w) {
                                    Ok(store) => {
                                        for rec in &parts {
                                            if let Err(e) = store.record_file(
                                                run_id_arc.as_str(),
                                                &plan_w.export_name,
                                                &rec.file_name,
                                                rec.rows,
                                                rec.bytes as i64,
                                                fmt_label_w,
                                                Some(comp_label_w),
                                            ) {
                                                log::warn!(
                                                    "export '{}': file_log write failed for parallel checkpoint chunk '{}' (file was produced): {:#}",
                                                    plan_w.export_name,
                                                    rec.file_name,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "export '{}': could not open state DB for file_log write of chunk {}: {:#}",
                                            plan_w.export_name,
                                            chunk_index,
                                            e
                                        );
                                    }
                                }
                                // chunk_task carries one file name; for a
                                // rotation-split chunk store the first sibling.
                                // The manifest records all siblings, so a
                                // missing one fails destination verification
                                // loudly instead of being silently skipped on
                                // resume.
                                let first = parts[0].file_name.clone();
                                let mut records = poison::lock_recover(file_records);
                                for rec in parts {
                                    records.push((rec, chunk_index));
                                }
                                drop(records);
                                // Form B: hand this chunk's checksums to the parent.
                                poison::lock_recover(checksums_shared)
                                    .push((chunk_checksums, chunk_key));
                                Some(first)
                            };
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
                                fname_for_state.as_deref(),
                            );
                            crate::test_hook::maybe_panic_at_chunk(
                                "after_chunk_complete",
                                chunk_index,
                            );
                            pb_w.inc(streamed_rows.load(Ordering::Relaxed));
                        }
                        Err(e) => {
                            let msg = crate::redact::redact_error(&e);
                            let _ = StateStore::fail_chunk_task_at_ref(
                                &state_ref,
                                run_id_arc.as_str(),
                                chunk_index,
                                &msg,
                            );
                            poison::lock_recover(errors)
                                .push(format!("chunk {}: {}", chunk_index, msg));
                        }
                    }
                }
            });
        }
    });

    summary.total_rows = agg_rows.load(Ordering::Relaxed);
    summary.retries = summary
        .retries
        .saturating_add(agg_retries.load(Ordering::Relaxed));
    pb_cp.finish(summary.total_rows);
    if plan.validate {
        summary.validated = Some(true);
    }
    if let Some(fp) = shared_fingerprint.into_inner() {
        summary.schema_fingerprint = Some(fp);
    }

    // Drain each worker-written part through the shared commit path: bumps
    // bytes/files counters, adds the manifest part (ADR-0012 M1), journals
    // ChunkCompleted, fires the after_file_write / after_manifest_update
    // fault hooks. `state=None` because workers already wrote each chunk's
    // file_log entry synchronously (the per-chunk durable manifest the
    // recovery tests depend on); calling state.record_file again here would
    // double-insert.
    //
    // Before this migration nothing populated `summary.manifest_parts` for
    // parallel_checkpoint runs at all — the cloud manifest M1 contract was
    // silently empty for every `parallel>1 + chunk_checkpoint:true` run.
    for (rec, chunk_index) in poison::into_recover(file_records) {
        super::super::commit::record_part(
            plan,
            summary,
            None,
            &rec,
            super::super::commit::PartKind::Chunk { chunk_index },
        );
    }

    let errs = poison::into_recover(errors);
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

    // Form B: XOR-combine every worker's chunk checksums run-wide + harvest once,
    // before finalize writes the manifest.
    let mut checksums_acc: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut checksum_key_column: Option<String> = None;
    for (part, key) in poison::into_recover(checksums_shared) {
        super::super::commit::accumulate_column_checksums(&mut checksums_acc, &part);
        if checksum_key_column.is_none() {
            checksum_key_column = key;
        }
    }
    super::super::commit::harvest_column_checksums(summary, checksums_acc, checksum_key_column);

    state.finalize_chunk_run_completed(&run_id)?;
    // ADR-0008 PG2 committed boundary via the shared finalize seam.
    super::super::run_store::RunStore::finalize(state, plan, summary)
        .with_progression(super::super::run_store::Progression::Chunked)
        .commit()?;
    log::info!(
        "export '{}': chunk checkpoint parallel run completed",
        plan.export_name
    );
    Ok(())
}
