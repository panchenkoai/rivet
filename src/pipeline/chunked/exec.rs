//! **Layer: Execution** — stateless chunked execution paths.
//!
//! `run_chunked_sequential` and `run_chunked_parallel` execute chunks without
//! owning checkpoint state. They accept an optional `StateStore` reference
//! solely for manifest (file-record) writes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::super::{RunSummary, progress::ChunkProgress, sink::ExportSink};
use super::math::build_chunk_query_sql;
use super::poison;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::tuning::Governor;
use crate::{destination, format, resource};

pub(crate) fn run_chunked_sequential(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    chunk_source: super::ChunkSource,
) -> Result<()> {
    let cp = super::chunked_plan(plan);

    let chunks = match chunk_source {
        // Detect: compute ranges + run the pre-chunk drift check once (ADR-0021).
        super::ChunkSource::Detect => super::prepare_chunk_plan(src, plan, state, summary)?,
        super::ChunkSource::Precomputed(ranges) => ranges,
    };

    let is_date = cp.by_days.is_some();

    log::info!(
        "export '{}': {} chunks to process sequentially",
        plan.export_name,
        chunks.len()
    );

    let pb = ChunkProgress::new(&plan.export_name, chunks.len());

    for (i, (start, end)) in chunks.iter().enumerate() {
        if !resource::check_memory(plan.tuning.memory_threshold_mb) {
            log::warn!("memory threshold exceeded, pausing 5s before chunk {}", i);
            std::thread::sleep(Duration::from_secs(5));
        }

        let chunk_query = build_chunk_query_sql(
            &plan.base_query,
            &cp.column,
            *start,
            *end,
            cp.dense,
            is_date,
            plan.source.source_type,
        );
        log::info!(
            "export '{}': chunk {}/{} ({}..{})",
            plan.export_name,
            i + 1,
            chunks.len(),
            start,
            end
        );

        summary.journal.record(RunEvent::ChunkStarted {
            chunk_index: i as i64,
            start_key: start.to_string(),
            end_key: end.to_string(),
        });

        let mut sink = ExportSink::new(plan)?;
        src.export(
            // `chunk_query` wraps the base in a subquery → resolve NUMERIC
            // catalog hints from the unwrapped base (`wrapped`).
            &source::ExportRequest::wrapped(
                &chunk_query,
                &plan.base_query,
                &plan.tuning,
                &plan.column_overrides,
            ),
            &mut sink,
        )?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
        }
        // ADR-0012 M3: capture the dest schema fingerprint as soon as the
        // sink resolves a schema (first non-empty chunk).  Idempotent across
        // subsequent chunks since the schema is identical run-wide.
        if let Some(s) = sink.dest_schema.as_deref() {
            super::super::manifest_writer::record_run_schema_fingerprint(summary, s);
        }

        summary.total_rows += sink.total_rows as i64;
        pb.inc(summary.total_rows);
        log::info!(
            "export '{}': chunk {} -- {} rows",
            plan.export_name,
            i + 1,
            sink.total_rows
        );

        if sink.total_rows > 0 {
            let fmt =
                format::create_format(plan.format, plan.compression, plan.compression_level, None);
            let base = super::chunk_part_filename(&plan.export_name, i, fmt.file_extension());
            let dest = destination::create_destination(&plan.destination)?;
            // Shared commit path (I1→I2→I7 + counters + journal + fault hooks).
            // write_sink_parts drains every part the sink produced — the
            // final temp file plus anything maybe_split rotated at
            // max_file_size — so rotation cannot drop data.
            let recs = super::super::commit::write_sink_parts(
                dest.as_ref(),
                &mut sink,
                plan.validate.then_some(plan.format),
                |idx, count| super::super::commit::part_indexed_name(&base, idx, count),
            )?;
            if plan.validate {
                summary.validated = Some(true);
            }
            // record_part journals the ChunkCompleted event with file_name=Some.
            for rec in &recs {
                super::super::commit::record_part(
                    plan,
                    summary,
                    state,
                    rec,
                    super::super::commit::PartKind::Chunk {
                        chunk_index: i as i64,
                    },
                );
            }
        } else {
            // Empty chunk: no file, but still journal completion so the run
            // record covers every chunk index. record_part only handles the
            // non-empty case (it always writes a part), so we record inline.
            summary.journal.record(RunEvent::ChunkCompleted {
                chunk_index: i as i64,
                rows: 0,
                file_name: None,
            });
        }
    }

    pb.finish(summary.total_rows);
    log::info!("export '{}': all chunks completed", plan.export_name);
    Ok(())
}

pub(crate) fn run_chunked_parallel(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    chunk_source: super::ChunkSource,
) -> Result<()> {
    let cp = super::chunked_plan(plan);

    let chunks = match chunk_source {
        // Detect: a short-lived connection computes ranges + runs the pre-chunk
        // drift check (ADR-0021), then closes before the workers open theirs.
        super::ChunkSource::Detect => super::prepare_chunk_plan_fresh(plan, state, summary)?,
        super::ChunkSource::Precomputed(ranges) => ranges,
    };

    let is_date = cp.by_days.is_some();

    let total_chunks = chunks.len();
    let parallel = cp.parallel.min(total_chunks);
    log::info!(
        "export '{}': {} chunks, {} parallel threads",
        plan.export_name,
        total_chunks,
        parallel
    );

    let completed = AtomicUsize::new(0);
    // Every worker bumps this exactly once — on success OR failure — so the
    // governor thread can tell when the run is *done* regardless of outcome.
    // `completed` counts only successes (progress bar / summary); using it for
    // the governor's exit condition deadlocks the `thread::scope` whenever a
    // chunk fails (the governor would loop forever waiting for a success count
    // that never arrives).
    let finished = AtomicUsize::new(0);
    let agg_rows = std::sync::atomic::AtomicI64::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());
    // PartRecords pushed by workers, drained into record_part post-scope so the
    // I2/M1 → I7 → counters ordering lives once in commit::record_part. Tuple
    // second is the chunk_index used by the ChunkCompleted journal event. The
    // bytes_written / files_produced / files_committed counters are no longer
    // accumulated worker-side — record_part bumps them in the drain.
    let file_records: std::sync::Mutex<Vec<(super::super::commit::PartRecord, i64)>> =
        std::sync::Mutex::new(Vec::new());
    // Schema fingerprint captured by whichever worker resolves the dest
    // schema first.  ADR-0012 M3 — stays None for empty runs (no chunk
    // produced rows so no schema was seen).  Drained into `summary` after
    // the parallel scope joins.
    let shared_fingerprint: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    // Condvar-backed semaphore: blocked spawners park in the kernel until a
    // worker calls `release()`, instead of polling an atomic every 50 ms. Its
    // permit ceiling is mutable so the OPT-2 governor can back parallelism off
    // and recover it under source pressure.
    let semaphore = resource::Semaphore::new(parallel);
    let pb = ChunkProgress::new(&plan.export_name, total_chunks);
    let pb_handle = pb.handle();

    // OPT-2 adaptive concurrency governor. Armed only when the user opted into
    // adaptation (`tuning.adaptive`) and there is real parallelism to govern
    // (`parallel > 1`). Floor defaults to 1; ceiling is the configured
    // `parallel`. A failed monitoring connection degrades gracefully to static
    // parallelism rather than failing the export.
    // `parallel` can be 0 when there are no chunks; `.max(1)` keeps `clamp`'s
    // bounds valid (the governor is off in that case anyway).
    let governor_floor = plan
        .tuning
        .min_parallel
        .unwrap_or(1)
        .clamp(1, parallel.max(1));
    let governor_on = plan.tuning.adaptive && parallel > 1;
    let governor_monitor: Option<Box<dyn Source>> = if governor_on {
        match source::create_source(&plan.source) {
            Ok(s) => {
                log::info!(
                    "export '{}': adaptive concurrency governor active (parallel {}..{})",
                    plan.export_name,
                    governor_floor,
                    parallel
                );
                Some(s)
            }
            Err(e) => {
                log::warn!(
                    "export '{}': governor monitoring connection failed; parallelism stays static at {}: {:#}",
                    plan.export_name,
                    parallel,
                    e
                );
                None
            }
        }
    } else {
        None
    };
    // Governor decisions buffered here (the journal is not thread-shared) and
    // drained into `summary.journal` after the scope joins.
    let governor_log: std::sync::Mutex<Vec<(usize, usize, String)>> =
        std::sync::Mutex::new(Vec::new());

    // One destination (GCS/S3) instance for the whole export: `create_destination` wires a
    // dedicated Tokio runtime; creating one per chunk caused runtime shutdown races under load
    // (`dispatch task is gone: runtime dropped` from the HTTP client).
    let shared_destination =
        std::sync::Arc::new(destination::create_destination(&plan.destination)?);
    destination::log_capabilities(
        &plan.export_name,
        &**shared_destination,
        plan.destination.destination_type,
        plan.tuning.max_retries,
    );

    std::thread::scope(|s| {
        // Governor thread: samples source pressure on its own monitoring
        // connection and resizes the semaphore within [floor, parallel]. It
        // self-terminates once every chunk worker has finished (success OR
        // failure) — keyed on `finished`, not `completed`, so a failing chunk
        // can't strand it and deadlock the scope. Holds parallelism flat
        // whenever the engine can't sample (`sample_pressure` → None).
        if let Some(mut monitor) = governor_monitor {
            let semaphore = &semaphore;
            let finished = &finished;
            let governor_log = &governor_log;
            let total = total_chunks;
            let ceiling = parallel;
            let floor = governor_floor;
            let export_name = plan.export_name.as_str();
            s.spawn(move || {
                // The decision loop lives in [`tuning::Governor`]; the
                // callback below is the only runner-specific binding
                // (resize the kernel-park semaphore, log the transition,
                // append it to the off-thread decision log so the parent
                // can drain it into the run journal post-scope).
                //
                // `RIVET_GOVERNOR_INTERVAL_MS` is read inside
                // [`Governor::new`]; the poll interval is clamped to the
                // sample interval so a tiny override (deterministic live
                // tests) actually polls that fast.
                let mut gov = Governor::new(ceiling, floor, ceiling);
                gov.run(
                    &mut monitor,
                    || finished.load(Ordering::Relaxed) >= total,
                    |from, to| {
                        semaphore.resize(to);
                        let reason = if to < from {
                            "source pressure rising: backed off"
                        } else {
                            "source pressure eased: recovered"
                        };
                        log::info!(
                            "export '{}': governor parallelism {} → {} ({})",
                            export_name,
                            from,
                            to,
                            reason
                        );
                        poison::lock_recover(governor_log).push((from, to, reason.to_string()));
                    },
                );
            });
        }

        for (i, (start, end)) in chunks.iter().enumerate() {
            // Block (kernel-park) until a worker slot frees up.
            semaphore.acquire();

            // Memory throttle stays a poll loop — RSS rising/falling is not
            // signaled by semaphore release, only observable via the syscall.
            if !resource::check_memory(plan.tuning.memory_threshold_mb) {
                log::warn!("memory threshold exceeded, waiting before chunk {}", i);
                while !resource::check_memory(plan.tuning.memory_threshold_mb) {
                    std::thread::sleep(Duration::from_secs(2));
                }
            }

            let plan_for_worker = plan.clone();
            let export_name = &plan.export_name;
            let base_query = &plan.base_query;
            let col = &cp.column;
            let completed = &completed;
            let finished = &finished;
            let agg_rows = &agg_rows;
            let errors = &errors;
            let file_records = &file_records;
            let shared_fingerprint = &shared_fingerprint;
            let semaphore = &semaphore;
            let pb_thread = pb_handle.clone();
            let start = *start;
            let end = *end;
            let shared_destination = std::sync::Arc::clone(&shared_destination);

            s.spawn(move || {
                let result = (|| -> Result<()> {
                    let chunk_query = build_chunk_query_sql(
                        base_query,
                        col,
                        start,
                        end,
                        cp.dense,
                        is_date,
                        plan_for_worker.source.source_type,
                    );

                    let mut thread_src = source::create_source(&plan_for_worker.source)?;
                    let mut sink = ExportSink::new(&plan_for_worker)?;
                    thread_src.export(
                        &source::ExportRequest::wrapped(
                            &chunk_query,
                            &plan_for_worker.base_query,
                            &plan_for_worker.tuning,
                            &plan_for_worker.column_overrides,
                        ),
                        &mut sink,
                    )?;
                    if let Some(w) = sink.writer.take() {
                        w.finish()?;
                    }
                    // ADR-0012 M3: capture the dest schema fingerprint once
                    // per run.  `OnceLock::set` is a no-op after the first
                    // successful set, so all later workers race-free.
                    if let Some(s) = sink.dest_schema.as_deref() {
                        let columns = crate::state::arrow_schema_to_columns(s);
                        let _ = shared_fingerprint.set(crate::state::schema_fingerprint(&columns));
                    }

                    agg_rows.fetch_add(sink.total_rows as i64, Ordering::Relaxed);

                    if sink.total_rows > 0 {
                        let fmt = format::create_format(
                            plan_for_worker.format,
                            plan_for_worker.compression,
                            plan_for_worker.compression_level,
                            None,
                        );
                        let base = super::chunk_part_filename(export_name, i, fmt.file_extension());
                        // Worker-safe half of commit (I1 + dest.write + fingerprint),
                        // draining every part the sink produced (max_file_size
                        // rotation included). Touches no shared run state;
                        // record_part runs in the drain.
                        let recs = super::super::commit::write_sink_parts(
                            &**shared_destination,
                            &mut sink,
                            plan_for_worker.validate.then_some(plan_for_worker.format),
                            |idx, count| super::super::commit::part_indexed_name(&base, idx, count),
                        )?;
                        let mut records = poison::lock_recover(file_records);
                        for rec in recs {
                            records.push((rec, i as i64));
                        }
                    }

                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    pb_thread.inc(agg_rows.load(Ordering::Relaxed));
                    log::info!(
                        "export '{}': chunk {}/{} done ({} rows)",
                        export_name,
                        done,
                        total_chunks,
                        sink.total_rows
                    );
                    Ok(())
                })();

                semaphore.release();

                if let Err(e) = result {
                    log::error!("export '{}': chunk {} failed: {:#}", export_name, i, e);
                    poison::lock_recover(errors).push(format!("chunk {}: {:#}", i, e));
                }

                // Mark this worker done (success OR failure) so the governor
                // thread can terminate — must run on every exit path.
                finished.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    summary.total_rows = agg_rows.load(Ordering::Relaxed);
    pb.finish(summary.total_rows);

    // Drain governor decisions (recorded off-thread) into the run journal.
    for (from, to, reason) in poison::into_recover(governor_log) {
        summary
            .journal
            .record(RunEvent::ParallelismAdjusted { from, to, reason });
    }
    if plan.validate {
        summary.validated = Some(true);
    }
    // Drain the worker-shared fingerprint into summary.  Stays None for
    // empty runs (no worker saw a schema) — finalize_manifest then falls
    // through to the state lookup / placeholder path for those.
    if let Some(fp) = shared_fingerprint.into_inner() {
        summary.schema_fingerprint = Some(fp);
    }

    // Drain each worker-written part through the shared commit path: I2/M1 +
    // bytes/files counters + ChunkCompleted journal + I7 file-log. All summary
    // and state mutation happens here on the parent thread, post-join.
    for (rec, chunk_index) in poison::into_recover(file_records) {
        super::super::commit::record_part(
            plan,
            summary,
            Some(state),
            &rec,
            super::super::commit::PartKind::Chunk { chunk_index },
        );
    }

    let errs = poison::into_recover(errors);
    if !errs.is_empty() {
        anyhow::bail!(
            "export '{}': {} chunks failed:\n{}",
            plan.export_name,
            errs.len(),
            errs.join("\n")
        );
    }

    log::info!(
        "export '{}': all {} chunks completed",
        plan.export_name,
        total_chunks
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
    use crate::source::BatchSink;
    use crate::state::StateStore;
    use crate::tuning::SourceTuning;

    // ChunkSource is defined in the parent (chunked) module.
    use super::super::ChunkSource;

    // ── EmptySource: never calls on_schema / on_batch ────────────────────────

    struct EmptySource;

    impl crate::source::Source for EmptySource {
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            unimplemented!("not needed in chunked exec tests")
        }
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn BatchSink,
        ) -> crate::error::Result<()> {
            Ok(()) // emits no schema, no batches → total_rows stays 0
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    fn chunked_plan_struct() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "orders".into(),
            base_query: "SELECT id FROM orders".into(),
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                chunk_count: None,
                parallel: 1,
                dense: false,
                by_days: None,
                checkpoint: false,
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

    fn empty_summary(plan: &ResolvedRunPlan) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("test_run", plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "chunked".into();
        s.compression = "none".into();
        s
    }

    // ── sequential ───────────────────────────────────────────────────────────

    #[test]
    fn sequential_empty_precomputed_returns_ok_with_zero_rows() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let mut src = EmptySource;
        run_chunked_sequential(
            &mut src,
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }

    #[test]
    fn sequential_one_chunk_zero_rows_skips_destination_write() {
        // EmptySource returns no rows → sink.total_rows = 0 → no file written
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let mut src = EmptySource;
        run_chunked_sequential(
            &mut src,
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![(1, 100)]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }

    #[test]
    fn sequential_multiple_empty_chunks_all_processed() {
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let mut src = EmptySource;
        // 5 chunks, all empty → no files, no rows, no errors
        run_chunked_sequential(
            &mut src,
            &plan,
            &mut summary,
            None,
            ChunkSource::Precomputed(vec![
                (1, 100),
                (101, 200),
                (201, 300),
                (301, 400),
                (401, 500),
            ]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }

    // ── parallel ─────────────────────────────────────────────────────────────

    #[test]
    fn parallel_empty_precomputed_returns_ok() {
        // With empty Precomputed, creates destination then runs no threads.
        let plan = chunked_plan_struct();
        let mut summary = empty_summary(&plan);
        let state = StateStore::open_in_memory().unwrap();
        run_chunked_parallel(
            &state,
            &plan,
            &mut summary,
            ChunkSource::Precomputed(vec![]),
        )
        .unwrap();
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
    }
}
