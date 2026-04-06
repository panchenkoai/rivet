use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::RunSummary;
use super::retry::classify_error;
use super::sink::ExportSink;
use super::validate::validate_output;
use crate::config::{ExportConfig, SourceConfig};
use crate::error::Result;
use crate::preflight::chunk_sparsity_from_counts;
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::tuning::SourceTuning;
use crate::{destination, format, resource};

// ─── Chunk math ──────────────────────────────────────────────

pub fn generate_chunks(min: i64, max: i64, chunk_size: i64) -> Vec<(i64, i64)> {
    if max < min || chunk_size <= 0 {
        return vec![];
    }
    let mut chunks = Vec::new();
    let mut start = min;
    while start <= max {
        let end = (start + chunk_size - 1).min(max);
        chunks.push((start, end));
        start = end + 1;
    }
    chunks
}

/// Synthetic ordinal column for `chunk_dense`; stripped before writing files.
pub(crate) const RIVET_CHUNK_RN_COL: &str = "_rivet_chunk_rn";

pub(crate) fn build_chunk_query_sql(
    base_query: &str,
    order_column: &str,
    start: i64,
    end: i64,
    chunk_dense: bool,
) -> String {
    if chunk_dense {
        format!(
            "SELECT * FROM (SELECT _rivet_i.*, ROW_NUMBER() OVER (ORDER BY _rivet_i.{oc}) AS {rn} FROM ({bq}) AS _rivet_i) AS _rivet_w WHERE _rivet_w.{rn} BETWEEN {s} AND {e}",
            bq = base_query,
            oc = order_column,
            rn = RIVET_CHUNK_RN_COL,
            s = start,
            e = end,
        )
    } else {
        format!(
            "SELECT * FROM ({base}) AS _rivet WHERE {col} BETWEEN {start} AND {end}",
            base = base_query,
            col = order_column,
            start = start,
            end = end,
        )
    }
}

pub(crate) fn chunk_plan_fingerprint(
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
    chunk_dense: bool,
) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    let mut buf = String::with_capacity(base_query.len() + chunk_column.len() + 32);
    buf.push_str(base_query);
    buf.push('\x1f');
    buf.push_str(chunk_column);
    buf.push('\x1f');
    buf.push_str(&chunk_size.to_string());
    buf.push('\x1f');
    buf.push_str(if chunk_dense { "dense_rn" } else { "range" });
    format!("{:016x}", xxh3_64(buf.as_bytes()))
}

// ─── Detection + generation ──────────────────────────────────

fn parse_scalar_i64(raw: &str) -> Result<i64> {
    let t = raw.trim();
    t.parse::<i64>()
        .or_else(|_| t.parse::<f64>().map(|x| x as i64))
        .map_err(|_| anyhow::anyhow!("invalid numeric scalar: {:?}", t))
}

fn query_wrapped_row_count(src: &mut dyn Source, base_query: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM ({}) AS _rivet_rowcnt", base_query);
    let raw = src
        .query_scalar(&sql)?
        .ok_or_else(|| anyhow::anyhow!("COUNT(*) returned no row"))?;
    parse_scalar_i64(&raw)
}

fn log_chunk_boundaries_list(export_name: &str, chunks: &[(i64, i64)]) {
    const HEAD: usize = 8;
    const TAIL: usize = 8;
    if chunks.is_empty() {
        log::info!(
            "export '{}': no BETWEEN windows (empty key range)",
            export_name
        );
        return;
    }
    if chunks.len() <= HEAD + TAIL {
        for (i, (a, b)) in chunks.iter().enumerate() {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
    } else {
        for (i, (a, b)) in chunks.iter().enumerate().take(HEAD) {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
        log::info!(
            "export '{}':   ... {} windows omitted ...",
            export_name,
            chunks.len() - HEAD - TAIL
        );
        for (i, (a, b)) in chunks.iter().enumerate().skip(chunks.len() - TAIL) {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
    }
}

fn log_chunk_sparsity_at_run(
    export_name: &str,
    chunk_column: &str,
    chunk_size: usize,
    min_val: i64,
    max_val: i64,
    chunks: &[(i64, i64)],
    row_count: i64,
) {
    if row_count == 0 {
        log::info!(
            "export '{}': COUNT(*) = 0 — no rows in export query; {} BETWEEN window(s) from `{}` min..max (runs will skip empty chunks)",
            export_name,
            chunks.len(),
            chunk_column
        );
        if !chunks.is_empty() && chunks.len() <= 24 {
            log_chunk_boundaries_list(export_name, chunks);
        }
        return;
    }

    let info = chunk_sparsity_from_counts(row_count, min_val, max_val, chunk_size);
    if info.is_sparse {
        let fill_pct = info.density * 100.0;
        let empty_hint = (1.0 - info.density).clamp(0.0, 1.0) * 100.0;
        log::info!(
            "export '{}': sparse `{}` range — ~{:.2}% of the min..max ID band contains rows (~{:.1}% of logical windows likely empty). \
             rows={}, span≈{}, chunk_size={}, ~{} logical windows, {} BETWEEN chunks. Computed boundaries:",
            export_name,
            chunk_column,
            fill_pct,
            empty_hint,
            info.row_count,
            info.range_span,
            chunk_size,
            info.logical_windows,
            chunks.len(),
        );
        log_chunk_boundaries_list(export_name, chunks);
    } else {
        log::info!(
            "export '{}': `{}` range looks dense enough for BETWEEN chunking (rows={}, span≈{}, density≈{:.6}, {} chunks); continuing",
            export_name,
            chunk_column,
            info.row_count,
            info.range_span,
            info.density,
            chunks.len(),
        );
    }
}

pub(crate) fn detect_and_generate_chunks(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
    export_name: &str,
    chunk_dense: bool,
) -> Result<Vec<(i64, i64)>> {
    if chunk_dense {
        let row_count = query_wrapped_row_count(src, base_query)?;
        log::info!(
            "export '{}': chunk_dense: ROW_NUMBER() OVER (ORDER BY `{}`) — {} row(s), chunk_size={}",
            export_name,
            chunk_column,
            row_count,
            chunk_size
        );
        let chunks = if row_count <= 0 {
            vec![]
        } else {
            generate_chunks(1, row_count, chunk_size as i64)
        };
        log::info!(
            "export '{}': dense chunk plan: {} window(s) on ordinal 1..{}",
            export_name,
            chunks.len(),
            row_count
        );
        return Ok(chunks);
    }

    let min_sql = format!(
        "SELECT min({col}) FROM ({q}) AS _rivet",
        col = chunk_column,
        q = base_query,
    );
    let max_sql = format!(
        "SELECT max({col}) FROM ({q}) AS _rivet",
        col = chunk_column,
        q = base_query,
    );

    let min_val = src
        .query_scalar(&min_sql)?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let max_val = src
        .query_scalar(&max_sql)?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);

    log::info!(
        "export '{}': chunk_column `{}` range {} .. {} (chunk_size={})",
        export_name,
        chunk_column,
        min_val,
        max_val,
        chunk_size
    );

    let chunks = generate_chunks(min_val, max_val, chunk_size as i64);

    match query_wrapped_row_count(src, base_query) {
        Ok(row_count) => {
            log_chunk_sparsity_at_run(
                export_name,
                chunk_column,
                chunk_size,
                min_val,
                max_val,
                &chunks,
                row_count,
            );
        }
        Err(e) => {
            log::warn!(
                "export '{}': could not run COUNT(*) for sparsity diagnostics: {:#}; proceeding with {} windows from min/max only",
                export_name,
                e,
                chunks.len()
            );
            if chunks.len() <= 24 {
                log_chunk_boundaries_list(export_name, &chunks);
            } else {
                log::info!(
                    "export '{}': use `RUST_LOG=info rivet run` after fixing COUNT if you need the full boundary list",
                    export_name
                );
            }
        }
    }

    Ok(chunks)
}

// ─── Chunked Mode (Sequential) ──────────────────────────────

pub(crate) fn run_chunked_sequential(
    src: &mut dyn Source,
    base_query: &str,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
) -> Result<()> {
    let col = export
        .chunk_column
        .as_deref()
        .expect("chunk_column required for chunked mode");
    let chunks = detect_and_generate_chunks(
        src,
        base_query,
        col,
        export.chunk_size,
        &export.name,
        export.chunk_dense,
    )?;

    log::info!(
        "export '{}': {} chunks to process sequentially",
        export.name,
        chunks.len()
    );

    for (i, (start, end)) in chunks.iter().enumerate() {
        if !resource::check_memory(tuning.memory_threshold_mb) {
            log::warn!("memory threshold exceeded, pausing 5s before chunk {}", i);
            std::thread::sleep(Duration::from_secs(5));
        }

        let chunk_query = build_chunk_query_sql(base_query, col, *start, *end, export.chunk_dense);
        log::info!(
            "export '{}': chunk {}/{} ({}..{})",
            export.name,
            i + 1,
            chunks.len(),
            start,
            end
        );

        let mut sink = ExportSink::new(export)?;
        src.export(&chunk_query, None, None, tuning, &mut sink)?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
        }

        summary.total_rows += sink.total_rows as i64;
        log::info!(
            "export '{}': chunk {} -- {} rows",
            export.name,
            i + 1,
            sink.total_rows
        );

        if sink.total_rows > 0 {
            if validate {
                validate_output(sink.tmp.path(), export.format, sink.total_rows)?;
                summary.validated = Some(true);
            }
            let file_bytes = std::fs::metadata(sink.tmp.path())
                .map(|m| m.len())
                .unwrap_or(0);
            summary.bytes_written += file_bytes;
            summary.files_produced += 1;

            let fmt =
                format::create_format(export.format, export.compression, export.compression_level);
            let file_name = format!(
                "{}_{}_chunk{}.{}",
                export.name,
                chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                i,
                fmt.file_extension()
            );
            let dest = destination::create_destination(&export.destination)?;
            dest.write(sink.tmp.path(), &file_name)?;

            if let Some(st) = state {
                let _ = st.record_file(
                    &summary.run_id,
                    &export.name,
                    &file_name,
                    sink.total_rows as i64,
                    file_bytes as i64,
                    &format!("{:?}", export.format).to_lowercase(),
                    Some(&format!("{:?}", export.compression).to_lowercase()),
                );
            }
        }
    }

    log::info!("export '{}': all chunks completed", export.name);
    Ok(())
}

// ─── Chunked Mode (Parallel) ────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn run_chunked_parallel(
    source_config: &SourceConfig,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
    summary: &mut RunSummary,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<()> {
    let base_query = export.resolve_query(config_dir, params)?;
    let col = export
        .chunk_column
        .as_deref()
        .expect("chunk_column required for chunked mode");

    let mut src = source::create_source(source_config)?;
    let chunks = detect_and_generate_chunks(
        &mut *src,
        &base_query,
        col,
        export.chunk_size,
        &export.name,
        export.chunk_dense,
    )?;
    drop(src);

    let total_chunks = chunks.len();
    let parallel = export.parallel.min(total_chunks);
    log::info!(
        "export '{}': {} chunks, {} parallel threads",
        export.name,
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

    std::thread::scope(|s| {
        for (i, (start, end)) in chunks.iter().enumerate() {
            while semaphore.load(Ordering::Relaxed) >= parallel {
                std::thread::sleep(Duration::from_millis(50));
            }

            if !resource::check_memory(tuning.memory_threshold_mb) {
                log::warn!("memory threshold exceeded, waiting before chunk {}", i);
                while !resource::check_memory(tuning.memory_threshold_mb) {
                    std::thread::sleep(Duration::from_secs(2));
                }
            }

            semaphore.fetch_add(1, Ordering::Relaxed);

            let source_config = source_config.clone();
            let tuning = tuning.clone();
            let export_for_sink = export.clone();
            let export_name = &export.name;
            let format_type = export.format;
            let dest_config = &export.destination;
            let base_query = &base_query;
            let completed = &completed;
            let agg_rows = &agg_rows;
            let agg_bytes = &agg_bytes;
            let agg_files = &agg_files;
            let errors = &errors;
            let file_records = &file_records;
            let semaphore = &semaphore;
            let start = *start;
            let end = *end;

            s.spawn(move || {
                let result = (|| -> Result<()> {
                    let chunk_query = build_chunk_query_sql(
                        base_query,
                        col,
                        start,
                        end,
                        export_for_sink.chunk_dense,
                    );

                    let mut thread_src = source::create_source(&source_config)?;
                    let mut sink = ExportSink::new(&export_for_sink)?;
                    thread_src.export(&chunk_query, None, None, &tuning, &mut sink)?;
                    if let Some(w) = sink.writer.take() {
                        w.finish()?;
                    }

                    agg_rows.fetch_add(sink.total_rows as i64, Ordering::Relaxed);

                    if sink.total_rows > 0 {
                        if validate {
                            validate_output(sink.tmp.path(), format_type, sink.total_rows)?;
                        }
                        let file_bytes = std::fs::metadata(sink.tmp.path())
                            .map(|m| m.len())
                            .unwrap_or(0);
                        agg_bytes.fetch_add(file_bytes, Ordering::Relaxed);
                        agg_files.fetch_add(1, Ordering::Relaxed);

                        let fmt = format::create_format(
                            format_type,
                            export_for_sink.compression,
                            export_for_sink.compression_level,
                        );
                        let file_name = format!(
                            "{}_{}_chunk{}.{}",
                            export_name,
                            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                            i,
                            fmt.file_extension()
                        );
                        let dest = destination::create_destination(dest_config)?;
                        dest.write(sink.tmp.path(), &file_name)?;
                        file_records
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push((file_name, sink.total_rows as i64, file_bytes as i64));
                    }

                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
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
    if validate {
        summary.validated = Some(true);
    }

    let fmt_name = format!("{:?}", export.format).to_lowercase();
    let comp_name = format!("{:?}", export.compression).to_lowercase();
    for (fname, rows, bytes) in file_records.into_inner().unwrap_or_else(|e| e.into_inner()) {
        let _ = state.record_file(
            &summary.run_id,
            &export.name,
            &fname,
            rows,
            bytes,
            &fmt_name,
            Some(&comp_name),
        );
    }

    let errs = errors.into_inner().unwrap_or_else(|e| e.into_inner());
    if !errs.is_empty() {
        anyhow::bail!(
            "export '{}': {} chunks failed:\n{}",
            export.name,
            errs.len(),
            errs.join("\n")
        );
    }

    log::info!(
        "export '{}': all {} chunks completed",
        export.name,
        total_chunks
    );
    Ok(())
}

// ─── Chunk checkpoint (SQLite plan + resume) ─────────────────

fn chunk_max_attempts_for_export(export: &ExportConfig, tuning: &SourceTuning) -> u32 {
    export
        .chunk_max_attempts
        .unwrap_or_else(|| tuning.max_retries.saturating_add(1).max(1))
}

#[allow(clippy::too_many_arguments)]
fn ensure_chunk_checkpoint_plan(
    state: &StateStore,
    export: &ExportConfig,
    summary: &mut RunSummary,
    base_query: &str,
    col: &str,
    chunks: &[(i64, i64)],
    resume: bool,
    tuning: &SourceTuning,
) -> Result<String> {
    let plan_hash = chunk_plan_fingerprint(base_query, col, export.chunk_size, export.chunk_dense);
    let max_att = chunk_max_attempts_for_export(export, tuning);

    if resume {
        let Some((rid, stored_hash)) = state.find_in_progress_chunk_run(&export.name)? else {
            anyhow::bail!(
                "export '{}': --resume but no in-progress chunk checkpoint; run without --resume first or `rivet state reset-chunks --export {}`",
                export.name,
                export.name
            );
        };
        if stored_hash != plan_hash {
            anyhow::bail!(
                "export '{}': chunk plan fingerprint mismatch (query, chunk_column, chunk_size, or chunk_dense changed); cannot resume",
                export.name
            );
        }
        summary.run_id = rid.clone();
        let n = state.reset_stale_running_chunk_tasks(&rid)?;
        if n > 0 {
            log::warn!(
                "export '{}': reset {} stale 'running' chunk task(s) after resume",
                export.name,
                n
            );
        }
        return Ok(rid);
    }

    if let Some((rid, _)) = state.find_in_progress_chunk_run(&export.name)? {
        anyhow::bail!(
            "export '{}': chunk checkpoint run '{}' still in progress; use `rivet run --resume` or `rivet state reset-chunks --export {}`",
            export.name,
            rid,
            export.name
        );
    }

    state.create_chunk_run(&summary.run_id, &export.name, &plan_hash, max_att)?;
    state.insert_chunk_tasks(&summary.run_id, chunks)?;
    log::info!(
        "export '{}': chunk checkpoint: {} tasks saved (run_id={})",
        export.name,
        chunks.len(),
        summary.run_id
    );
    Ok(summary.run_id.clone())
}

#[allow(clippy::too_many_arguments)]
fn export_one_chunk_range(
    src: &mut dyn Source,
    base_query: &str,
    col: &str,
    start: i64,
    end: i64,
    chunk_index: i64,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
) -> Result<(usize, Option<String>, u64)> {
    let chunk_query = build_chunk_query_sql(base_query, col, start, end, export.chunk_dense);

    let mut sink = ExportSink::new(export)?;
    src.export(&chunk_query, None, None, tuning, &mut sink)?;
    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    if sink.total_rows == 0 {
        return Ok((0, None, 0));
    }

    if validate {
        validate_output(sink.tmp.path(), export.format, sink.total_rows)?;
    }
    let file_bytes = std::fs::metadata(sink.tmp.path())
        .map(|m| m.len())
        .unwrap_or(0);

    let fmt = format::create_format(export.format, export.compression, export.compression_level);
    let file_name = format!(
        "{}_{}_chunk{}.{}",
        export.name,
        chrono::Utc::now().format("%Y%m%d_%H%M%S"),
        chunk_index,
        fmt.file_extension()
    );
    let dest = destination::create_destination(&export.destination)?;
    dest.write(sink.tmp.path(), &file_name)?;

    Ok((sink.total_rows, Some(file_name), file_bytes))
}

#[allow(clippy::too_many_arguments)]
fn run_chunk_with_source_retries(
    source_config: &SourceConfig,
    base_query: &str,
    col: &str,
    start: i64,
    end: i64,
    chunk_index: i64,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
) -> Result<(usize, Option<String>, u64)> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..=tuning.max_retries {
        if attempt > 0 {
            let (_, _needs_reconnect, extra_delay) = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or((false, false, 0));
            let backoff = tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + extra_delay;
            log::warn!(
                "export '{}': chunk {} retry {}/{} in {}ms",
                export.name,
                chunk_index,
                attempt,
                tuning.max_retries,
                backoff
            );
            std::thread::sleep(Duration::from_millis(backoff));
        }

        let mut src = match source::create_source(source_config) {
            Ok(s) => s,
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < tuning.max_retries && transient {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        };

        match export_one_chunk_range(
            &mut *src,
            base_query,
            col,
            start,
            end,
            chunk_index,
            export,
            tuning,
            validate,
        ) {
            Ok(v) => return Ok(v),
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < tuning.max_retries && transient {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk export failed after retries")))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_chunked_sequential_checkpoint(
    src: &mut dyn Source,
    source_config: &SourceConfig,
    state: &StateStore,
    base_query: &str,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
    summary: &mut RunSummary,
    st: Option<&StateStore>,
    resume: bool,
    config_path: &str,
) -> Result<()> {
    let _ = config_path;
    let col = export
        .chunk_column
        .as_deref()
        .expect("chunk_column required for chunked mode");

    let chunks = if resume {
        vec![]
    } else {
        detect_and_generate_chunks(
            src,
            base_query,
            col,
            export.chunk_size,
            &export.name,
            export.chunk_dense,
        )?
    };

    let run_id = ensure_chunk_checkpoint_plan(
        state, export, summary, base_query, col, &chunks, resume, tuning,
    )?;

    if !resume && !resource::check_memory(tuning.memory_threshold_mb) {
        log::warn!("memory threshold exceeded before chunk export; pausing 5s");
        std::thread::sleep(Duration::from_secs(5));
    }

    while let Some((chunk_index, sk, ek)) = state.claim_next_chunk_task(&run_id)? {
        if !resource::check_memory(tuning.memory_threshold_mb) {
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
            export.name,
            chunk_index,
            start,
            end
        );

        match run_chunk_with_source_retries(
            source_config,
            base_query,
            col,
            start,
            end,
            chunk_index,
            export,
            tuning,
            validate,
        ) {
            Ok((rows, fname, file_bytes)) => {
                summary.total_rows += rows as i64;
                if rows > 0 {
                    summary.bytes_written += file_bytes;
                    summary.files_produced += 1;
                    if let Some(name) = &fname
                        && let Some(store) = st
                    {
                        let _ = store.record_file(
                            &summary.run_id,
                            &export.name,
                            name,
                            rows as i64,
                            file_bytes as i64,
                            &format!("{:?}", export.format).to_lowercase(),
                            Some(&format!("{:?}", export.compression).to_lowercase()),
                        );
                    }
                }
                state.complete_chunk_task(&run_id, chunk_index, rows as i64, fname.as_deref())?;
            }
            Err(e) => {
                let msg = format!("{:#}", e);
                log::error!(
                    "export '{}': chunk {} failed: {}",
                    export.name,
                    chunk_index,
                    msg
                );
                state.fail_chunk_task(&run_id, chunk_index, &msg)?;
            }
        }
    }

    let pending = state.count_chunk_tasks_not_completed(&run_id)?;
    if pending > 0 {
        anyhow::bail!(
            "export '{}': chunk checkpoint incomplete ({} tasks not completed); fix errors and `rivet run --resume` or `rivet state reset-chunks`",
            export.name,
            pending
        );
    }

    state.finalize_chunk_run_completed(&run_id)?;
    log::info!(
        "export '{}': chunk checkpoint run completed (run_id={})",
        export.name,
        run_id
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_chunked_parallel_checkpoint(
    config_path: &str,
    source_config: &SourceConfig,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
    summary: &mut RunSummary,
    params: Option<&std::collections::HashMap<String, String>>,
    resume: bool,
) -> Result<()> {
    let base_query = export.resolve_query(config_dir, params)?;
    let col = export
        .chunk_column
        .as_deref()
        .expect("chunk_column required for chunked mode");

    let chunks = if resume {
        vec![]
    } else {
        let mut src = source::create_source(source_config)?;
        detect_and_generate_chunks(
            &mut *src,
            &base_query,
            col,
            export.chunk_size,
            &export.name,
            export.chunk_dense,
        )?
    };

    let run_id = ensure_chunk_checkpoint_plan(
        state,
        export,
        summary,
        &base_query,
        col,
        &chunks,
        resume,
        tuning,
    )?;

    let total_tasks = {
        let tasks = state.list_chunk_tasks_for_run(&run_id)?;
        tasks.len().max(1)
    };
    let parallel = export.parallel.min(total_tasks);
    log::info!(
        "export '{}': chunk checkpoint parallel: {} workers, run_id={}",
        export.name,
        parallel,
        run_id
    );

    let db_path = state.database_path().to_path_buf();
    let run_id_arc = std::sync::Arc::new(run_id.clone());
    let agg_rows = std::sync::atomic::AtomicI64::new(0);
    let agg_bytes = std::sync::atomic::AtomicU64::new(0);
    let agg_files = std::sync::atomic::AtomicUsize::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());

    let export_name = export.name.clone();
    let export_for_workers = export.clone();
    let format_type = export.format;
    let dest_config = export.destination.clone();
    let tuning_cl = tuning.clone();
    let source_config_cl = source_config.clone();
    let base_query_cl = base_query.clone();
    let col_owned = col.to_string();
    let config_path_owned = config_path.to_string();
    let fmt_label = format!("{:?}", export.format).to_lowercase();
    let comp_label = format!("{:?}", export.compression).to_lowercase();

    std::thread::scope(|s| {
        for _ in 0..parallel {
            let db_path = db_path.clone();
            let run_id_arc = std::sync::Arc::clone(&run_id_arc);
            let agg_rows = &agg_rows;
            let agg_bytes = &agg_bytes;
            let agg_files = &agg_files;
            let errors = &errors;
            let export_name = export_name.clone();
            let export_worker = export_for_workers.clone();
            let tuning_w = tuning_cl.clone();
            let source_config_w = source_config_cl.clone();
            let base_query_w = base_query_cl.clone();
            let col_w = col_owned.clone();
            let dest_config = dest_config.clone();
            let config_path_w = config_path_owned.clone();
            let fmt_label_w = fmt_label.clone();
            let comp_label_w = comp_label.clone();

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

                    if !resource::check_memory(tuning_w.memory_threshold_mb) {
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
                        &base_query_w,
                        &col_w,
                        start,
                        end,
                        export_worker.chunk_dense,
                    );

                    let result = (|| -> Result<(usize, Option<String>, u64)> {
                        let mut last_err: Option<anyhow::Error> = None;
                        for attempt in 0..=tuning_w.max_retries {
                            if attempt > 0 {
                                let (_, _, extra_delay) = last_err
                                    .as_ref()
                                    .map(classify_error)
                                    .unwrap_or((false, false, 0));
                                let backoff =
                                    tuning_w.retry_backoff_ms * 2u64.pow(attempt - 1) + extra_delay;
                                std::thread::sleep(Duration::from_millis(backoff));
                            }

                            let mut thread_src = match source::create_source(&source_config_w) {
                                Ok(s) => s,
                                Err(e) => {
                                    let (transient, _, _) = classify_error(&e);
                                    if attempt < tuning_w.max_retries && transient {
                                        last_err = Some(e);
                                        continue;
                                    }
                                    return Err(e);
                                }
                            };

                            let mut sink = ExportSink::new(&export_worker)?;

                            let export_attempt = (|| -> Result<(usize, Option<String>, u64)> {
                                thread_src.export(
                                    &chunk_query,
                                    None,
                                    None,
                                    &tuning_w,
                                    &mut sink,
                                )?;
                                if let Some(w) = sink.writer.take() {
                                    w.finish()?;
                                }
                                if sink.total_rows == 0 {
                                    return Ok((0, None, 0));
                                }
                                if validate {
                                    validate_output(sink.tmp.path(), format_type, sink.total_rows)?;
                                }
                                let file_bytes = std::fs::metadata(sink.tmp.path())
                                    .map(|m| m.len())
                                    .unwrap_or(0);
                                let fmt = format::create_format(
                                    format_type,
                                    export_worker.compression,
                                    export_worker.compression_level,
                                );
                                let file_name = format!(
                                    "{}_{}_chunk{}.{}",
                                    export_name,
                                    chrono::Utc::now().format("%Y%m%d_%H%M%S"),
                                    chunk_index,
                                    fmt.file_extension()
                                );
                                let dest = destination::create_destination(&dest_config)?;
                                dest.write(sink.tmp.path(), &file_name)?;
                                Ok((sink.total_rows, Some(file_name), file_bytes))
                            })();

                            match export_attempt {
                                Ok(v) => return Ok(v),
                                Err(e) => {
                                    let (transient, _, _) = classify_error(&e);
                                    if attempt < tuning_w.max_retries && transient {
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
                                if let (Some(name), Ok(store)) =
                                    (fname.as_ref(), StateStore::open(&config_path_w))
                                {
                                    let _ = store.record_file(
                                        run_id_arc.as_str(),
                                        &export_name,
                                        name,
                                        rows as i64,
                                        file_bytes as i64,
                                        &fmt_label_w,
                                        Some(comp_label_w.as_str()),
                                    );
                                }
                            }
                            let _ = StateStore::complete_chunk_task_at_path(
                                &db_path,
                                run_id_arc.as_str(),
                                chunk_index,
                                rows as i64,
                                fname.as_deref(),
                            );
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
    if validate {
        summary.validated = Some(true);
    }

    let errs = errors.into_inner().unwrap_or_else(|e| e.into_inner());
    if !errs.is_empty() {
        anyhow::bail!(
            "export '{}': parallel checkpoint worker errors:\n{}",
            export.name,
            errs.join("\n")
        );
    }

    let pending = state.count_chunk_tasks_not_completed(&run_id)?;
    if pending > 0 {
        anyhow::bail!(
            "export '{}': {} chunk task(s) not completed; `rivet run --resume` or inspect `rivet state chunks --export {}`",
            export.name,
            pending,
            export.name
        );
    }

    state.finalize_chunk_run_completed(&run_id)?;
    log::info!(
        "export '{}': chunk checkpoint parallel run completed",
        export.name
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_chunks() {
        let chunks = generate_chunks(1, 100, 30);
        assert_eq!(chunks, vec![(1, 30), (31, 60), (61, 90), (91, 100)]);
    }

    #[test]
    fn test_generate_chunks_exact() {
        let chunks = generate_chunks(0, 99, 50);
        assert_eq!(chunks, vec![(0, 49), (50, 99)]);
    }

    #[test]
    fn test_generate_chunks_single() {
        let chunks = generate_chunks(1, 10, 100);
        assert_eq!(chunks, vec![(1, 10)]);
    }

    #[test]
    fn test_generate_chunks_empty() {
        assert!(generate_chunks(10, 5, 100).is_empty());
    }

    #[test]
    fn test_build_chunk_query_range_mode() {
        let q = build_chunk_query_sql("SELECT id FROM t", "id", 1, 100, false);
        assert!(q.contains("WHERE id BETWEEN 1 AND 100"), "got: {}", q);
        assert!(!q.contains("ROW_NUMBER()"), "got: {}", q);
    }

    #[test]
    fn test_build_chunk_query_dense_mode() {
        let q = build_chunk_query_sql("SELECT id FROM t", "id", 1, 5000, true);
        assert!(q.contains("ROW_NUMBER()"), "got: {}", q);
        assert!(q.contains(RIVET_CHUNK_RN_COL), "got: {}", q);
        assert!(q.contains("BETWEEN 1 AND 5000"), "got: {}", q);
    }
}
