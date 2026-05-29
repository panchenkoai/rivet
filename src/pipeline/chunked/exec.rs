//! **Layer: Execution** — stateless chunked execution paths.
//!
//! `run_chunked_sequential` and `run_chunked_parallel` execute chunks without
//! owning checkpoint state. They accept an optional `StateStore` reference
//! solely for manifest (file-record) writes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::super::{
    RunSummary, progress::ChunkProgress, sink::ExportSink, validate::validate_output,
};
use super::detect::detect_and_generate_chunks;
use super::math::build_chunk_query_sql;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::tuning::{GOVERNOR_SAMPLE_INTERVAL_MS, GovernorState};
use crate::{destination, format, resource};

/// How often the governor checks whether the run has finished. Kept much
/// shorter than [`GOVERNOR_SAMPLE_INTERVAL_MS`] so the governor thread exits
/// promptly once the last chunk completes, instead of lingering for a full
/// sample interval.
const GOVERNOR_POLL_MS: u64 = 200;

/// Extract the `ChunkedPlan` from a `ResolvedRunPlan`. Panics if the strategy
/// is not `Chunked` — all callers in this module only run for chunked plans.
fn chunked_plan(plan: &ResolvedRunPlan) -> &ChunkedPlan {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => cp,
        _ => unreachable!("chunked runner called with non-chunked plan"),
    }
}

pub(crate) fn run_chunked_sequential(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    chunk_source: super::ChunkSource,
) -> Result<()> {
    let cp = chunked_plan(plan);

    let chunks = match chunk_source {
        super::ChunkSource::Detect => detect_and_generate_chunks(
            src,
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.chunk_count,
            &plan.export_name,
            cp.dense,
            cp.by_days,
            plan.source.source_type,
        )?,
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
            &source::ExportRequest {
                query: &chunk_query,
                incremental: None,
                cursor: None,
                tuning: &plan.tuning,
                column_overrides: &plan.column_overrides,
                page_limit: None,
            },
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

        let mut chunk_file_name: Option<String> = None;

        if sink.total_rows > 0 {
            if plan.validate {
                validate_output(sink.tmp.path(), plan.format, sink.total_rows)?;
                summary.validated = Some(true);
            }
            let file_bytes = std::fs::metadata(sink.tmp.path())
                .map(|m| m.len())
                .unwrap_or(0);
            summary.bytes_written += file_bytes;
            summary.files_produced += 1;

            let fmt =
                format::create_format(plan.format, plan.compression, plan.compression_level, None);
            let file_name = format!(
                "{}_{}_chunk{}.{}",
                plan.export_name,
                chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                i,
                fmt.file_extension()
            );
            let dest = destination::create_destination(&plan.destination)?;
            dest.write(sink.tmp.path(), &file_name)?;

            // ADR-0012 M1: record the committed chunk part for the manifest.
            super::super::manifest_writer::record_committed_part(
                summary,
                file_name.clone(),
                sink.total_rows as i64,
                file_bytes,
                sink.tmp.path(),
            );

            if let Some(st) = state
                && let Err(e) = st.record_file(
                    &summary.run_id,
                    &plan.export_name,
                    &file_name,
                    sink.total_rows as i64,
                    file_bytes as i64,
                    plan.format.label(),
                    Some(plan.compression.label()),
                )
            {
                log::warn!(
                    "export '{}': file_log write failed for chunk file '{}' (file was produced): {:#}",
                    plan.export_name,
                    file_name,
                    e
                );
            }
            chunk_file_name = Some(file_name);
        }

        summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: i as i64,
            rows: sink.total_rows as i64,
            file_name: chunk_file_name,
        });
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
    let cp = chunked_plan(plan);

    let chunks = match chunk_source {
        super::ChunkSource::Detect => {
            let mut src = source::create_source(&plan.source)?;
            let ranges = detect_and_generate_chunks(
                &mut *src,
                &plan.base_query,
                &cp.column,
                cp.chunk_size,
                cp.chunk_count,
                &plan.export_name,
                cp.dense,
                cp.by_days,
                plan.source.source_type,
            )?;
            drop(src);
            ranges
        }
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
    let agg_bytes = std::sync::atomic::AtomicU64::new(0);
    let agg_files = AtomicUsize::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());
    // (file_name, rows, bytes, content_fingerprint)
    let file_records: std::sync::Mutex<Vec<(String, i64, i64, String)>> =
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
                let mut gov = GovernorState::new(ceiling, floor, ceiling);
                // `RIVET_GOVERNOR_INTERVAL_MS` shrinks the sample cadence for
                // deterministic live tests (same env-seam convention as
                // `RIVET_TEST_PANIC_AT`). Unset → production default. The poll
                // sleep is clamped to the interval so a small override actually
                // samples that fast (otherwise GOVERNOR_POLL_MS would cap it).
                let sample_ms = std::env::var("RIVET_GOVERNOR_INTERVAL_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|&n| n > 0)
                    .unwrap_or(GOVERNOR_SAMPLE_INTERVAL_MS);
                let poll_ms = GOVERNOR_POLL_MS.min(sample_ms);
                let interval = Duration::from_millis(sample_ms);
                let mut last_sample = Instant::now();
                while finished.load(Ordering::Relaxed) < total {
                    std::thread::sleep(Duration::from_millis(poll_ms));
                    if finished.load(Ordering::Relaxed) >= total {
                        break;
                    }
                    if last_sample.elapsed() < interval {
                        continue;
                    }
                    last_sample = Instant::now();
                    if let Some((from, to)) = gov.observe(monitor.sample_pressure()) {
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
                        governor_log
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push((from, to, reason.to_string()));
                    }
                }
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
            let agg_bytes = &agg_bytes;
            let agg_files = &agg_files;
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
                        &source::ExportRequest {
                            query: &chunk_query,
                            incremental: None,
                            cursor: None,
                            tuning: &plan_for_worker.tuning,
                            column_overrides: &plan_for_worker.column_overrides,
                            page_limit: None,
                        },
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
                        let _ = shared_fingerprint
                            .set(crate::state::schema_fingerprint(&columns));
                    }

                    agg_rows.fetch_add(sink.total_rows as i64, Ordering::Relaxed);

                    if sink.total_rows > 0 {
                        if plan_for_worker.validate {
                            validate_output(
                                sink.tmp.path(),
                                plan_for_worker.format,
                                sink.total_rows,
                            )?;
                        }
                        let file_bytes = std::fs::metadata(sink.tmp.path())
                            .map(|m| m.len())
                            .unwrap_or(0);
                        agg_bytes.fetch_add(file_bytes, Ordering::Relaxed);
                        agg_files.fetch_add(1, Ordering::Relaxed);

                        let fmt = format::create_format(
                            plan_for_worker.format,
                            plan_for_worker.compression,
                            plan_for_worker.compression_level,
                            None,
                        );
                        let file_name = format!(
                            "{}_{}_chunk{}.{}",
                            export_name,
                            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                            i,
                            fmt.file_extension()
                        );
                        shared_destination.write(sink.tmp.path(), &file_name)?;
                        // ADR-0012 M3: compute the part fingerprint while the
                        // local tmp file still exists in this worker scope.
                        let fingerprint = super::super::manifest_writer::compute_part_fingerprint(
                            sink.tmp.path(),
                        )
                        .unwrap_or_else(|e| {
                            log::warn!(
                                "export '{}': chunk {} fingerprint failed for '{}' (not fatal): {:#}",
                                export_name,
                                i,
                                file_name,
                                e
                            );
                            "xxh3:0000000000000000".to_string()
                        });
                        file_records.lock().unwrap_or_else(|e| e.into_inner()).push((
                            file_name,
                            sink.total_rows as i64,
                            file_bytes as i64,
                            fingerprint,
                        ));
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
                    errors
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(format!("chunk {}: {:#}", i, e));
                }

                // Mark this worker done (success OR failure) so the governor
                // thread can terminate — must run on every exit path.
                finished.fetch_add(1, Ordering::Relaxed);
            });
        }
    });

    summary.total_rows = agg_rows.load(Ordering::Relaxed);
    summary.bytes_written = agg_bytes.load(Ordering::Relaxed);
    summary.files_produced = agg_files.load(Ordering::Relaxed);
    pb.finish(summary.total_rows);

    // Drain governor decisions (recorded off-thread) into the run journal.
    for (from, to, reason) in governor_log.into_inner().unwrap_or_else(|e| e.into_inner()) {
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

    let fmt_name = plan.format.label();
    let comp_name = plan.compression.label();
    for (fname, rows, bytes, fingerprint) in
        file_records.into_inner().unwrap_or_else(|e| e.into_inner())
    {
        if let Err(e) = state.record_file(
            &summary.run_id,
            &plan.export_name,
            &fname,
            rows,
            bytes,
            fmt_name,
            Some(comp_name),
        ) {
            log::warn!(
                "export '{}': file_log write failed for parallel chunk '{}' (file was produced): {:#}",
                plan.export_name,
                fname,
                e
            );
        }
        // ADR-0012 M1: aggregate the worker's recorded part into the manifest.
        super::super::manifest_writer::record_committed_part_with_fingerprint(
            summary,
            fname,
            rows,
            bytes as u64,
            fingerprint,
        );
    }

    let errs = errors.into_inner().unwrap_or_else(|e| e.into_inner());
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
