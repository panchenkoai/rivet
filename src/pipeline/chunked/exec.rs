//! **Layer: Execution** — stateless chunked execution paths.
//!
//! `run_chunked_sequential` and `run_chunked_parallel` execute chunks without
//! owning checkpoint state. They accept an optional `StateStore` reference
//! solely for manifest (file-record) writes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::super::{
    RunSummary, journal::RunEvent, progress::ChunkProgress, sink::ExportSink,
    validate::validate_output,
};
use super::detect::detect_and_generate_chunks;
use super::math::build_chunk_query_sql;
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
        src.export(&chunk_query, None, None, &plan.tuning, &mut sink)?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
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

            let fmt = format::create_format(plan.format, plan.compression, plan.compression_level);
            let file_name = format!(
                "{}_{}_chunk{}.{}",
                plan.export_name,
                chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                i,
                fmt.file_extension()
            );
            let dest = destination::create_destination(&plan.destination)?;
            dest.write(sink.tmp.path(), &file_name)?;

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
                    "export '{}': manifest write failed for chunk file '{}' (file was produced): {:#}",
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
    let agg_rows = std::sync::atomic::AtomicI64::new(0);
    let agg_bytes = std::sync::atomic::AtomicU64::new(0);
    let agg_files = AtomicUsize::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());
    let file_records: std::sync::Mutex<Vec<(String, i64, i64)>> = std::sync::Mutex::new(Vec::new());
    let semaphore = AtomicUsize::new(0);
    let pb = ChunkProgress::new(&plan.export_name, total_chunks);
    let pb_handle = pb.handle();

    // One destination (GCS/S3) instance for the whole export: `create_destination` wires a
    // dedicated Tokio runtime; creating one per chunk caused runtime shutdown races under load
    // (`dispatch task is gone: runtime dropped` from the HTTP client).
    let shared_destination = std::sync::Arc::new(destination::create_destination(
        &plan.destination,
    )?);

    std::thread::scope(|s| {
        for (i, (start, end)) in chunks.iter().enumerate() {
            while semaphore.load(Ordering::Relaxed) >= parallel {
                std::thread::sleep(Duration::from_millis(50));
            }

            if !resource::check_memory(plan.tuning.memory_threshold_mb) {
                log::warn!("memory threshold exceeded, waiting before chunk {}", i);
                while !resource::check_memory(plan.tuning.memory_threshold_mb) {
                    std::thread::sleep(Duration::from_secs(2));
                }
            }

            semaphore.fetch_add(1, Ordering::Relaxed);

            let plan_for_worker = plan.clone();
            let export_name = &plan.export_name;
            let base_query = &plan.base_query;
            let col = &cp.column;
            let completed = &completed;
            let agg_rows = &agg_rows;
            let agg_bytes = &agg_bytes;
            let agg_files = &agg_files;
            let errors = &errors;
            let file_records = &file_records;
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
                        &chunk_query,
                        None,
                        None,
                        &plan_for_worker.tuning,
                        &mut sink,
                    )?;
                    if let Some(w) = sink.writer.take() {
                        w.finish()?;
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
                        );
                        let file_name = format!(
                            "{}_{}_chunk{}.{}",
                            export_name,
                            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                            i,
                            fmt.file_extension()
                        );
                        shared_destination.write(sink.tmp.path(), &file_name)?;
                        file_records
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push((file_name, sink.total_rows as i64, file_bytes as i64));
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

                semaphore.fetch_sub(1, Ordering::Relaxed);

                if let Err(e) = result {
                    log::error!("export '{}': chunk {} failed: {:#}", export_name, i, e);
                    errors
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(format!("chunk {}: {:#}", i, e));
                }
            });
        }
    });

    summary.total_rows = agg_rows.load(Ordering::Relaxed);
    summary.bytes_written = agg_bytes.load(Ordering::Relaxed);
    summary.files_produced = agg_files.load(Ordering::Relaxed);
    pb.finish(summary.total_rows);
    if plan.validate {
        summary.validated = Some(true);
    }

    let fmt_name = plan.format.label();
    let comp_name = plan.compression.label();
    for (fname, rows, bytes) in file_records.into_inner().unwrap_or_else(|e| e.into_inner()) {
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
                "export '{}': manifest write failed for parallel chunk '{}' (file was produced): {:#}",
                plan.export_name,
                fname,
                e
            );
        }
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
