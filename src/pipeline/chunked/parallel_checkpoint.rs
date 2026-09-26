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
use super::{ChunkSource, chunked_plan, config_hint, ensure_chunk_checkpoint_plan};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::source;
use crate::state::StateStore;
use crate::{format, resource};

use super::math::build_chunk_query_sql;

use super::ChunkOutcome;

pub(crate) fn run_chunked_parallel_checkpoint(
    config_path: &str,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    chunk_source: ChunkSource,
) -> Result<()> {
    // Subject to the per-runner facade contract (ADR-0018) — dispatched directly
    // from job.rs (bypassing run_export), so it sets the flag itself.
    summary.state_backed = true;
    let cp = chunked_plan(plan);

    let chunks = if plan.resume {
        // A resume re-executes the STORED plan, and one thing must still be true
        // for that to be sound — and it was not checked, because this arm
        // skipped the whole match below (round-11 bughunt).
        //
        // The SCHEMA must not have drifted. Through `check_drift_only_FRESH`,
        //    which opens its own short-lived connection — this runner has no
        //    `Source` in scope here, and that seam exists precisely for it. An
        //    earlier pass deferred this half claiming a `Source` would have to be
        //    threaded in; the helper was already there, one line away. `on_schema_drift: fail` was inert
        //    here: DEMONSTRATED — a `DROP COLUMN` between the crash and the resume
        //    produced exit 0, `rows: 300`, and three parts under ONE
        //    `schema_fingerprint` whose schemas disagree. The identical drop without
        //    `--resume` fails loudly. The gap between a crash and its resume is
        //    exactly where a schema change is most likely.
        super::check_drift_only_fresh(plan, state, summary)?;
        vec![]
    } else {
        match chunk_source {
            // Detect: a short-lived connection computes ranges + runs the
            // pre-chunk drift check (ADR-0021), then closes before workers spawn.
            ChunkSource::Detect => super::prepare_chunk_plan_fresh(plan, state, summary)?,
            ChunkSource::Precomputed(ranges) => {
                // Ranges come from the artifact; the DRIFT GATE still runs.
                summary.chunks_precomputed = true;
                // No ranges ⇒ no rows will be read, so there is nothing for the
                // gate to protect and no reason to open a connection to say so.
                if !ranges.is_empty() {
                    super::check_drift_only_fresh(plan, state, summary)?;
                }
                ranges
            }
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
        let stats = super::apply_m8_resume_decisions(state, &run_id, plan, summary)?;
        // #3: crash-recovery resume signal, same as the sequential runner.
        summary.resumed = stats.adopted_prior_work();
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
    // Rows streamed across ALL tasks (completed + in-flight) — drives the
    // per-batch progress feed so the bar ticks during a chunk's read.
    let streamed_rows = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
    // Per-attempt retry counter bumped from inside each worker's retry
    // loop and folded into `summary.retries` after the scope joins, so the
    // console summary card / `rivet metrics` / `export_metrics.retries`
    // reflect chunked-parallel retries the same way the sequential path
    // already does.
    let agg_retries = std::sync::atomic::AtomicU32::new(0);
    // #4: reconnects across worker threads, folded into summary.reconnects after
    // the scope joins — the parallel analogue of the sequential runner's counter.
    let agg_reconnects = std::sync::atomic::AtomicU32::new(0);
    // Parts, shapes, checksums and failures, drained post-join in FanIn's fixed order.
    // The workers are spawned here, not through FanIn::spawn: this runner's crash
    // hooks (`maybe_panic_at_chunk`) must still take the process down.
    let fan = crate::pipeline::fan_in::FanIn::default();
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
    let fmt_label = plan.format.label();
    let comp_label = plan.compression.label();
    let mode_label = plan.strategy.mode_label();

    // The frame IS the fix for the history this file used to narrate here: the
    // two checkpoint runners once bypassed the cross-shape guard entirely
    // (runner-bypass class). Now the guard rides in the only door to a
    // destination.
    let (shared_destination, _frame_ext) = crate::pipeline::frame::RunnerFrame::open_shared(plan)?;

    // OPT-2 adaptive concurrency governor, through the SHARED seam (identical wiring in
    // `chunked/exec.rs` and `keyset.rs`; #152). This runner shipped WITHOUT it, so
    // `tuning.adaptive: true` was a silent no-op on exactly the shape `rivet init` scaffolds
    // (`chunk_checkpoint: true` + `parallel: N`): job.rs dispatches a RESUMABLE chunked plan
    // here, so dropping `chunk_checkpoint` from an otherwise identical config was the
    // difference between a governed run and an ungoverned one — with nothing in the log to
    // tell them apart (bughunt 2026-08-14, finding 0).
    //
    // Pool shape, not spawner shape: `parallel` long-lived workers claim tasks in a loop, so
    // the permit is taken PER CLAIMED TASK (`TaskPermit`, acquired before the claim) and the
    // governor's stop predicate counts WORKERS that exited (`WorkerFinished`), i.e. `total`
    // is the pool size, not the task count. Disarmed (the default) the semaphore starts with
    // one permit per worker and never resizes, so no worker ever parks — the run behaves
    // exactly as it did before.
    let semaphore = resource::Semaphore::new(parallel.max(1));
    let governor = crate::pipeline::governor::GovernorHarness::arm(plan, parallel);

    std::thread::scope(|s| {
        // Governor thread (shared seam): samples source pressure on its own monitoring
        // connection and resizes the permit ceiling within [floor, ceiling], self-terminating
        // once every pool worker has FINISHED (drained, errored, or panicked) so a failing
        // worker can't strand it and deadlock the scope.
        governor.spawn_into(s, &semaphore, fan.finished(), parallel, &plan.export_name);

        for _ in 0..parallel {
            let state_ref = state_ref.clone();
            let shared_destination = std::sync::Arc::clone(&shared_destination);
            let run_id_arc = std::sync::Arc::clone(&run_id_arc);
            let agg_retries = &agg_retries;
            let agg_reconnects = &agg_reconnects;
            let fan_r = &fan;
            let shared_fingerprint = &shared_fingerprint;
            let plan_w = plan_for_workers.clone();
            let cp_w = cp_for_workers.clone();
            let fmt_label_w = fmt_label;
            let comp_label_w = comp_label;
            let mode_label_w = mode_label;
            let pb_w = pb_cp_handle.clone();
            let streamed_rows = std::sync::Arc::clone(&streamed_rows);
            let semaphore = &semaphore;

            s.spawn(move || {
                // Count this worker as FINISHED on every exit path (drained queue, claim
                // error, unwinding panic) — the governor thread's only exit is
                // `finished >= total`, so a missed bump hangs `thread::scope` forever.
                let _finish = crate::pipeline::governor::WorkerFinished::new(fan_r.finished());
                let shared_destination = shared_destination;
                loop {
                    // One permit per claimed task, taken BEFORE the claim: a shed then
                    // parks this worker without leaving a chunk_task pinned `running`
                    // while it waits. The guard releases at the end of THIS iteration on
                    // every path (`break`, `continue`, panic).
                    let _permit = crate::pipeline::governor::TaskPermit::acquire(semaphore);
                    let claimed = match StateStore::claim_next_chunk_task_at_ref(
                        &state_ref,
                        run_id_arc.as_str(),
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            fan_r.fail("claim error", format!("{e:#}"));
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
                            let _ = StateStore::open_at_ref(&state_ref).and_then(|st| {
                                st.fail_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    "invalid start_key",
                                    false, // a malformed key parses the same way every time
                                )
                            });
                            continue;
                        }
                    };
                    let end: i64 = match ek.parse() {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = StateStore::open_at_ref(&state_ref).and_then(|st| {
                                st.fail_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    "invalid end_key",
                                    false, // a malformed key parses the same way every time
                                )
                            });
                            continue;
                        }
                    };

                    let chunk_query = build_chunk_query_sql(
                        &plan_w.base_query,
                        &cp_w.column,
                        start,
                        end,
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
                                let class = last_err.as_ref().map(classify_error);
                                // #4: a reconnect-class retry re-opens the source
                                // below — count it (folded into summary.reconnects).
                                if class.is_some_and(|c| c.needs_reconnect()) {
                                    agg_reconnects
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                let extra_delay =
                                    class.map(|c| c.extra_delay_ms()).unwrap_or(0);
                                let backoff = crate::pipeline::retry::retry_backoff_ms(
                                    plan_w.tuning.retry_backoff_ms,
                                    attempt,
                                    extra_delay,
                                );
                                std::thread::sleep(Duration::from_millis(backoff));
                            }

                            let mut thread_src = match source::create_source(&plan_w.source) {
                                Ok(s) => s,
                                Err(e) => {
                                    if crate::pipeline::retry::should_retry(crate::pipeline::retry::Attempt {
                                        attempt,
                                        max_retries: plan_w.tuning.max_retries,
                                        error: &e,
                                    }) {
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
                                    return Ok((0, Vec::new(), Default::default(), Default::default()));
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
                                Ok((
                                    sink.total_rows,
                                    recs,
                                    sink.take_checksums(),
                                    sink.take_shape(),
                                ))
                            })();

                            match export_attempt {
                                Ok(v) => return Ok(v),
                                Err(e) => {
                                    if crate::pipeline::retry::should_retry(crate::pipeline::retry::Attempt {
                                        attempt,
                                        max_retries: plan_w.tuning.max_retries,
                                        error: &e,
                                    }) {
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

                    // Test-only, mirroring the sequential runner: make ONE chunk
                    // fail without killing the process, so the worker-error and
                    // chunk-completion guards below can be exercised. A panic hook
                    // cannot reach them — they only run once the workers join,
                    // which a crashed process never does.
                    let result = match crate::test_hook::maybe_error_at_index(
                        "chunk_export",
                        chunk_index,
                    ) {
                        Err(msg) => Err(anyhow::anyhow!(msg)),
                        Ok(()) => result,
                    };
                    match result {
                        Ok((rows, parts, chunk_checksums, chunk_shape)) => {
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
                                // Reopen from the REF, not from `config_path`.
                                // `rivet apply` dispatches this runner with an
                                // empty config_path (job.rs — it is a
                                // display-only hint there), and
                                // `StateStore::open("")` resolves to
                                // `./.rivet_state.db` in the process CWD. Every
                                // durable-part row and the running aggregate
                                // landed in a stray database while the real
                                // state DB got none — invisible on a clean run,
                                // and on recovery the resume found the chunks
                                // `completed` with no file_log to rehydrate, so
                                // it declared a manifest with zero parts over
                                // parquet that was already on the destination.
                                match StateStore::open_at_ref(&state_ref) {
                                    Ok(store) => {
                                        for rec in &parts {
                                            if let Err(e) = store.record_durable_part(
                                                crate::state::DurablePart {
                                                    run_id: run_id_arc.as_str(),
                                                    export_name: &plan_w.export_name,
                                                    file_name: &rec.file_name,
                                                    rows: rec.rows,
                                                    bytes: rec.bytes as i64,
                                                    format: fmt_label_w,
                                                    compression: Some(comp_label_w),
                                                    mode: mode_label_w,
                                                    cursor_high: None, // chunked runner, not keyset pages
                                                },
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
                                // ADR-0029: the chunk is the commit unit its parts are
                                // recorded under and its checksums enter with.
                                let unit = super::super::commit::UnitId::Chunk(chunk_index);
                                for rec in parts {
                                    fan_r.part(unit, rec);
                                }
                                fan_r.observe(chunk_shape);
                                fan_r.contribute(unit, chunk_checksums);
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
                            let _ = StateStore::open_at_ref(&state_ref).and_then(|st| {
                                st.complete_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    rows as i64,
                                    fname_for_state.as_deref(),
                                )
                            });
                            crate::test_hook::maybe_panic_at_chunk(
                                "after_chunk_complete",
                                chunk_index,
                            );
                            pb_w.inc(streamed_rows.load(Ordering::Relaxed));
                        }
                        Err(e) => {
                            let msg = crate::redact::redact_error(&e);
                            let _ = StateStore::open_at_ref(&state_ref).and_then(|st| {
                                st.fail_chunk_task(
                                    run_id_arc.as_str(),
                                    chunk_index,
                                    &msg,
                                    crate::pipeline::retry::is_transient(&e),
                                )
                            });
                            fan_r.fail(&format!("chunk {chunk_index}"), msg);
                        }
                    }
                }
            });
        }
    });

    summary.retries = summary
        .retries
        .saturating_add(agg_retries.load(Ordering::Relaxed));
    summary.reconnects = summary
        .reconnects
        .saturating_add(agg_reconnects.load(Ordering::Relaxed));
    if plan.validate {
        summary.validated = Some(true);
    }
    if let Some(fp) = shared_fingerprint.into_inner() {
        summary.schema_fingerprint = Some(fp);
    }

    // `file_log: None`: the workers already wrote each chunk's file_log row
    // synchronously (ADR-0017 — the per-chunk durable record the recovery tests
    // depend on), so the drain must not double-insert it.
    let drained = fan.finish(
        plan,
        summary,
        None,
        Some(governor),
        |_, unit| match unit {
            super::super::commit::UnitId::Chunk(chunk_index) => {
                super::super::commit::PartKind::Chunk { chunk_index }
            }
            other => unreachable!("chunked parts are recorded under a chunk unit, not {other:?}"),
        },
        |errs| {
            anyhow::anyhow!(
                "export '{}': parallel checkpoint worker errors:\n{}",
                plan.export_name,
                errs.join("\n")
            )
        },
    );
    // After the drain: record_part is what counts the rows.
    pb_cp.finish(summary.total_rows);
    drained?;

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
