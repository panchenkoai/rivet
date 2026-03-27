use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::{Config, ExportConfig, ExportMode, FormatType, SourceConfig, TimeColumnType};
use crate::destination;
use crate::error::Result;
use crate::format::{self, FormatWriter};
use crate::resource;
use crate::source::{self, BatchSink, Source};
use crate::state::StateStore;
use crate::tuning::SourceTuning;

pub fn run(config_path: &str, export_name: Option<&str>, validate: bool) -> Result<()> {
    let config = Config::load(config_path)?;
    let tuning = SourceTuning::from_config(config.source.tuning.as_ref());
    log::info!("source tuning: {}", tuning);

    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let state = StateStore::open(config_path)?;

    let exports: Vec<&ExportConfig> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    for export in exports {
        log::info!("starting export '{}'", export.name);
        let start = std::time::Instant::now();
        let rss_before = crate::resource::get_rss_mb();

        let result = match export.mode {
            ExportMode::Chunked if export.parallel > 1 => {
                run_chunked_parallel(&config.source, &state, export, &tuning, &config_dir, validate)
            }
            _ => {
                run_with_reconnect(&config.source, &state, export, &tuning, &config_dir, validate)
            }
        };

        let duration_ms = start.elapsed().as_millis() as i64;
        let rss_after = crate::resource::get_rss_mb();
        let peak_rss = rss_after.max(rss_before) as i64;
        let mode_str = format!("{:?}", export.mode).to_lowercase();
        let fmt_str = format!("{:?}", export.format).to_lowercase();

        match &result {
            Ok(()) => {
                let _ = state.record_metric(
                    &export.name, duration_ms, 0, Some(peak_rss),
                    "success", None, Some(tuning.profile_name()), Some(&fmt_str), Some(&mode_str),
                );
            }
            Err(e) => {
                log::error!("export '{}' failed: {:#}", export.name, e);
                let _ = state.record_metric(
                    &export.name, duration_ms, 0, Some(peak_rss),
                    "failed", Some(&format!("{:#}", e)), Some(tuning.profile_name()), Some(&fmt_str), Some(&mode_str),
                );
            }
        }
    }

    Ok(())
}

fn run_with_reconnect(
    source_config: &SourceConfig,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 0..=tuning.max_retries {
        if attempt > 0 {
            let (_, needs_reconnect, extra_delay) = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or((false, false, 0));
            let backoff = tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + extra_delay;
            log::warn!(
                "export '{}': retry {}/{} in {}ms{}({})",
                export.name, attempt, tuning.max_retries, backoff,
                if needs_reconnect { " [reconnecting] " } else { " " },
                last_err.as_ref().map(|e: &anyhow::Error| format!("{:#}", e)).unwrap_or_default(),
            );
            std::thread::sleep(Duration::from_millis(backoff));
        }

        let mut src = match source::create_source(source_config) {
            Ok(s) => s,
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < tuning.max_retries && transient {
                    log::warn!("export '{}': connection failed, will retry: {:#}", export.name, e);
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        };

        match run_export(&mut *src, state, export, tuning, config_dir, validate) {
            Ok(()) => return Ok(()),
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

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("export failed after retries")))
}

/// Classifies transient errors into retry categories.
/// Returns (is_transient, needs_reconnect, extra_delay_ms)
pub fn classify_error(err: &anyhow::Error) -> (bool, bool, u64) {
    let msg = format!("{:#}", err).to_lowercase();

    // Network errors -- need reconnect
    if msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("connection refused")
        || msg.contains("no route to host")
        || msg.contains("network is unreachable")
        || msg.contains("name resolution")
        || msg.contains("dns")
        || msg.contains("ssl handshake")
        || msg.contains("i/o timeout")
        || msg.contains("unexpected eof")
        || msg.contains("closed the connection unexpectedly")
        || msg.contains("got an error reading communication packets")
    {
        return (true, true, 0);
    }

    // MySQL specific -- need reconnect
    if msg.contains("gone away")
        || msg.contains("lost connection")
        || msg.contains("the server closed the connection")
        || msg.contains("can't connect to mysql server")
    {
        return (true, true, 0);
    }

    // Timeout errors -- retry on same connection
    if msg.contains("timed out")
        || msg.contains("timeout")
        || msg.contains("canceling statement")
        || msg.contains("lock wait timeout")
        || msg.contains("execution time exceeded")
    {
        return (true, false, 0);
    }

    // Capacity errors -- retry with longer delay
    if msg.contains("too many connections")
        || msg.contains("the database system is starting up")
        || msg.contains("the database system is shutting down")
    {
        return (true, true, 15_000);
    }

    // Deadlock/serialization -- retry once, same connection
    if msg.contains("deadlock") || msg.contains("could not serialize access") {
        return (true, false, 1_000);
    }

    // Not transient
    (false, false, 0)
}

#[cfg(test)]
pub(crate) fn is_transient(err: &anyhow::Error) -> bool {
    classify_error(err).0
}

fn run_export(
    src: &mut dyn Source,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
) -> Result<()> {
    let base_query = export.resolve_query(config_dir)?;

    match export.mode {
        ExportMode::Full => {
            run_single_export(src, &base_query, None, None, export, tuning, validate, Some(state))?;
        }
        ExportMode::Incremental => {
            let cursor_state = state.get(&export.name)?;
            let cursor_col = export.cursor_column.as_deref();
            run_single_export(src, &base_query, cursor_col, Some(&cursor_state), export, tuning, validate, Some(state))?;
        }
        ExportMode::Chunked => {
            run_chunked_sequential(src, &base_query, export, tuning, validate)?;
        }
        ExportMode::TimeWindow => {
            let windowed_query = build_time_window_query(
                &base_query,
                export.time_column.as_deref().unwrap(),
                export.time_column_type,
                export.days_window.unwrap(),
            );
            run_single_export(src, &windowed_query, None, None, export, tuning, validate, Some(state))?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_single_export(
    src: &mut dyn Source,
    query: &str,
    cursor_column: Option<&str>,
    cursor: Option<&crate::types::CursorState>,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
    state: Option<&StateStore>,
) -> Result<()> {
    let mut sink = ExportSink::new(export.format)?;

    src.export(query, cursor_column, cursor, tuning, &mut sink)?;

    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    log::info!("export '{}': {} rows written", export.name, sink.total_rows);

    if sink.total_rows == 0 {
        log::info!("export '{}': no data to export", export.name);
        return Ok(());
    }

    if validate {
        validate_output(sink.tmp.path(), export.format, sink.total_rows)?;
    }

    let file_name = build_file_name(&export.name, format::create_format(export.format).file_extension());
    let dest = destination::create_destination(&export.destination)?;
    dest.write(sink.tmp.path(), &file_name)?;

    if export.mode == ExportMode::Incremental
        && let (Some(cursor_col), Some(batch), Some(schema), Some(st)) =
            (&export.cursor_column, &sink.last_batch, &sink.schema, state)
            && let Some(last_val) = extract_last_cursor_value(batch, cursor_col, schema) {
                st.update(&export.name, &last_val)?;
                log::info!("export '{}': cursor updated to '{}'", export.name, last_val);
            }

    // Schema tracking
    if let (Some(schema), Some(st)) = (&sink.schema, state) {
        let columns: Vec<crate::state::SchemaColumn> = schema.fields().iter().map(|f| {
            crate::state::SchemaColumn {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            }
        }).collect();

        match st.detect_schema_change(&export.name, &columns) {
            Ok(Some(change)) => {
                log::warn!("export '{}': schema changed!", export.name);
                if !change.added.is_empty() {
                    log::warn!("  added columns: {}", change.added.join(", "));
                }
                if !change.removed.is_empty() {
                    log::warn!("  removed columns: {}", change.removed.join(", "));
                }
                for (col, old, new) in &change.type_changed {
                    log::warn!("  type changed: {} ({} -> {})", col, old, new);
                }
            }
            Ok(None) => {}
            Err(e) => log::warn!("schema tracking error: {:#}", e),
        }
    }

    log::info!("export '{}' completed successfully", export.name);
    Ok(())
}

// ─── Chunked Mode (Sequential) ──────────────────────────────

fn run_chunked_sequential(
    src: &mut dyn Source,
    base_query: &str,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
) -> Result<()> {
    let col = export.chunk_column.as_deref().unwrap();
    let chunks = detect_and_generate_chunks(src, base_query, col, export.chunk_size)?;

    log::info!("export '{}': {} chunks to process sequentially", export.name, chunks.len());

    for (i, (start, end)) in chunks.iter().enumerate() {
        if !resource::check_memory(tuning.memory_threshold_mb) {
            log::warn!("memory threshold exceeded, pausing 5s before chunk {}", i);
            std::thread::sleep(Duration::from_secs(5));
        }

        let chunk_query = format!(
            "SELECT * FROM ({base}) AS _rivet WHERE {col} BETWEEN {start} AND {end}",
            base = base_query, col = col, start = start, end = end,
        );
        log::info!("export '{}': chunk {}/{} ({}..{})", export.name, i + 1, chunks.len(), start, end);

        let mut sink = ExportSink::new(export.format)?;
        src.export(&chunk_query, None, None, tuning, &mut sink)?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
        }

        log::info!("export '{}': chunk {} -- {} rows", export.name, i + 1, sink.total_rows);

        if sink.total_rows > 0 {
            if validate {
                validate_output(sink.tmp.path(), export.format, sink.total_rows)?;
            }
            let fmt = format::create_format(export.format);
            let file_name = format!("{}_{}_chunk{}.{}", export.name, chrono::Utc::now().format("%Y%m%d_%H%M%S"), i, fmt.file_extension());
            let dest = destination::create_destination(&export.destination)?;
            dest.write(sink.tmp.path(), &file_name)?;
        }
    }

    log::info!("export '{}': all chunks completed", export.name);
    Ok(())
}

// ─── Chunked Mode (Parallel) ────────────────────────────────

fn run_chunked_parallel(
    source_config: &SourceConfig,
    _state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
) -> Result<()> {
    let base_query = export.resolve_query(config_dir)?;
    let col = export.chunk_column.as_deref().unwrap();

    let mut src = source::create_source(source_config)?;
    let chunks = detect_and_generate_chunks(&mut *src, &base_query, col, export.chunk_size)?;
    drop(src);

    let total_chunks = chunks.len();
    let parallel = export.parallel.min(total_chunks);
    log::info!("export '{}': {} chunks, {} parallel threads", export.name, total_chunks, parallel);

    let completed = AtomicUsize::new(0);
    let errors = std::sync::Mutex::new(Vec::<String>::new());
    let semaphore = AtomicUsize::new(0);

    std::thread::scope(|s| {

        for (i, (start, end)) in chunks.iter().enumerate() {
            // Wait for a slot
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
            let export_name = &export.name;
            let format_type = export.format;
            let dest_config = &export.destination;
            let base_query = &base_query;
            let completed = &completed;
            let errors = &errors;
            let semaphore = &semaphore;
            let start = *start;
            let end = *end;

            s.spawn(move || {
                let result = (|| -> Result<()> {
                    let chunk_query = format!(
                        "SELECT * FROM ({base}) AS _rivet WHERE {col} BETWEEN {start} AND {end}",
                        base = base_query, col = col,
                    );

                    let mut thread_src = source::create_source(&source_config)?;
                    let mut sink = ExportSink::new(format_type)?;
                    thread_src.export(&chunk_query, None, None, &tuning, &mut sink)?;
                    if let Some(w) = sink.writer.take() {
                        w.finish()?;
                    }

                    if sink.total_rows > 0 {
                        if validate {
                            validate_output(sink.tmp.path(), format_type, sink.total_rows)?;
                        }
                        let fmt = format::create_format(format_type);
                        let file_name = format!(
                            "{}_{}_chunk{}.{}",
                            export_name, chrono::Utc::now().format("%Y%m%d_%H%M%S"), i, fmt.file_extension()
                        );
                        let dest = destination::create_destination(dest_config)?;
                        dest.write(sink.tmp.path(), &file_name)?;
                    }

                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    log::info!(
                        "export '{}': chunk {}/{} done ({} rows)",
                        export_name, done, total_chunks, sink.total_rows
                    );
                    Ok(())
                })();

                semaphore.fetch_sub(1, Ordering::Relaxed);

                if let Err(e) = result {
                    log::error!("export '{}': chunk {} failed: {:#}", export_name, i, e);
                    errors.lock().unwrap().push(format!("chunk {}: {:#}", i, e));
                }
            });
        }
    });

    let errs = errors.into_inner().unwrap();
    if !errs.is_empty() {
        anyhow::bail!("export '{}': {} chunks failed:\n{}", export.name, errs.len(), errs.join("\n"));
    }

    log::info!("export '{}': all {} chunks completed", export.name, total_chunks);
    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────

pub(crate) fn detect_and_generate_chunks(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
) -> Result<Vec<(i64, i64)>> {
    let min_sql = format!(
        "SELECT min({col}) FROM ({q}) AS _rivet",
        col = chunk_column, q = base_query,
    );
    let max_sql = format!(
        "SELECT max({col}) FROM ({q}) AS _rivet",
        col = chunk_column, q = base_query,
    );

    let min_val = src.query_scalar(&min_sql)?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let max_val = src.query_scalar(&max_sql)?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);

    log::info!("chunk range: {} .. {} (chunk_size={})", min_val, max_val, chunk_size);

    Ok(generate_chunks(min_val, max_val, chunk_size as i64))
}

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

pub fn build_time_window_query(
    base_query: &str,
    time_column: &str,
    time_type: TimeColumnType,
    days_window: u32,
) -> String {
    let now = chrono::Utc::now();
    let window_start = now - chrono::Duration::days(days_window as i64);
    let truncated = window_start
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let condition = match time_type {
        TimeColumnType::Timestamp => {
            format!(
                "{} >= '{}'",
                time_column,
                truncated.format("%Y-%m-%d %H:%M:%S")
            )
        }
        TimeColumnType::Unix => {
            format!(
                "{} >= {}",
                time_column,
                truncated.and_utc().timestamp()
            )
        }
    };

    format!(
        "SELECT * FROM ({base}) AS _rivet WHERE {cond}",
        base = base_query, cond = condition,
    )
}

pub fn validate_output(path: &Path, format: FormatType, expected_rows: usize) -> Result<()> {
    let actual = match format {
        FormatType::Parquet => {
            let file = std::fs::File::open(path)?;
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            let mut count = 0usize;
            for batch in reader {
                count += batch?.num_rows();
            }
            count
        }
        FormatType::Csv => {
            let content = std::fs::read_to_string(path)?;
            let lines = content.lines().count();
            lines.saturating_sub(1) // subtract header
        }
    };

    if actual != expected_rows {
        anyhow::bail!(
            "validation failed: expected {} rows, got {} in {}",
            expected_rows, actual, path.display()
        );
    }

    log::info!("validation passed: {} rows verified", actual);
    Ok(())
}

struct ExportSink {
    writer: Option<Box<dyn FormatWriter>>,
    format_type: FormatType,
    tmp: tempfile::NamedTempFile,
    total_rows: usize,
    last_batch: Option<RecordBatch>,
    schema: Option<SchemaRef>,
}

impl ExportSink {
    fn new(format_type: FormatType) -> Result<Self> {
        let tmp = tempfile::NamedTempFile::new()?;
        Ok(Self {
            writer: None,
            format_type,
            tmp,
            total_rows: 0,
            last_batch: None,
            schema: None,
        })
    }
}

impl BatchSink for ExportSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        let fmt = format::create_format(self.format_type);
        let file = self.tmp.as_file().try_clone()?;
        let buf_writer = BufWriter::new(file);
        self.writer = Some(fmt.create_writer(&schema, Box::new(buf_writer))?);
        self.schema = Some(schema);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.total_rows += batch.num_rows();
        if let Some(w) = self.writer.as_mut() {
            w.write_batch(batch)?;
        }
        self.last_batch = Some(batch.clone());
        Ok(())
    }
}

pub(crate) fn build_file_name(export_name: &str, extension: &str) -> String {
    let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    format!("{}_{}.{}", export_name, now, extension)
}

pub(crate) fn extract_last_cursor_value(
    batch: &RecordBatch,
    cursor_column: &str,
    schema: &SchemaRef,
) -> Option<String> {
    let col_idx = schema.index_of(cursor_column).ok()?;
    let array = batch.column(col_idx);
    let last_row = batch.num_rows().checked_sub(1)?;

    if array.is_null(last_row) {
        return None;
    }

    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    match array.data_type() {
        DataType::Int16 => Some(array.as_any().downcast_ref::<Int16Array>()?.value(last_row).to_string()),
        DataType::Int32 => Some(array.as_any().downcast_ref::<Int32Array>()?.value(last_row).to_string()),
        DataType::Int64 => Some(array.as_any().downcast_ref::<Int64Array>()?.value(last_row).to_string()),
        DataType::Float64 => Some(array.as_any().downcast_ref::<Float64Array>()?.value(last_row).to_string()),
        DataType::Utf8 => Some(array.as_any().downcast_ref::<StringArray>()?.value(last_row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = array.as_any().downcast_ref::<TimestampMicrosecondArray>()?.value(last_row);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        }
        DataType::Date32 => {
            let days = array.as_any().downcast_ref::<Date32Array>()?.value(last_row);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)? + chrono::Duration::days(days as i64);
            Some(date.to_string())
        }
        _ => { log::warn!("cannot extract cursor for type {:?}", array.data_type()); None }
    }
}

pub fn show_state(config_path: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let states = state.list_all()?;
    if states.is_empty() {
        println!("No export state recorded yet.");
        return Ok(());
    }
    println!("{:<30} {:<40} LAST RUN", "EXPORT", "LAST CURSOR");
    println!("{}", "-".repeat(90));
    for s in &states {
        println!("{:<30} {:<40} {}",
            s.export_name,
            s.last_cursor_value.as_deref().unwrap_or("-"),
            s.last_run_at.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

pub fn reset_state(config_path: &str, export_name: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    state.reset(export_name)?;
    println!("State reset for export '{}'", export_name);
    Ok(())
}

pub fn show_metrics(config_path: &str, export_name: Option<&str>, limit: usize) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let metrics = state.get_metrics(export_name, limit)?;
    if metrics.is_empty() {
        println!("No metrics recorded yet.");
        return Ok(());
    }
    println!(
        "{:<20} {:<10} {:>10} {:>10} {:>8} RUN AT",
        "EXPORT", "STATUS", "ROWS", "DURATION", "RSS"
    );
    println!("{}", "-".repeat(85));
    for m in &metrics {
        let duration = if m.duration_ms >= 1000 {
            format!("{:.1}s", m.duration_ms as f64 / 1000.0)
        } else {
            format!("{}ms", m.duration_ms)
        };
        let rss = m.peak_rss_mb.map(|r| format!("{}MB", r)).unwrap_or_else(|| "-".into());
        println!(
            "{:<20} {:<10} {:>10} {:>10} {:>8} {}",
            m.export_name, m.status, m.total_rows, duration, rss, m.run_at
        );
        if let Some(err) = &m.error_message {
            println!("  Error: {}", err);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};

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
    fn test_build_time_window_timestamp() {
        let q = build_time_window_query("SELECT * FROM events", "created_at", TimeColumnType::Timestamp, 7);
        assert!(q.contains("created_at >= '"), "got: {}", q);
        assert!(q.contains("_rivet WHERE"));
    }

    #[test]
    fn test_build_time_window_unix() {
        let q = build_time_window_query("SELECT * FROM events", "ts", TimeColumnType::Unix, 30);
        assert!(q.contains("ts >= "), "got: {}", q);
        assert!(!q.contains("'"), "unix should not have quotes, got: {}", q);
    }

    #[test]
    fn test_build_file_name() {
        let name = build_file_name("users", "csv");
        assert!(name.starts_with("users_"));
        assert!(name.ends_with(".csv"));
    }

    #[test]
    fn test_extract_cursor_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![10, 20, 30]))]).unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), Some("30".into()));
    }

    #[test]
    fn test_extract_cursor_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]).unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn test_is_transient_matches() {
        assert!(is_transient(&anyhow::anyhow!("statement timed out")));
        assert!(is_transient(&anyhow::anyhow!("connection reset")));
    }

    #[test]
    fn test_is_transient_rejects() {
        assert!(!is_transient(&anyhow::anyhow!("syntax error")));
        assert!(!is_transient(&anyhow::anyhow!("permission denied")));
        assert!(!is_transient(&anyhow::anyhow!("table not found")));
    }

    #[test]
    fn test_classify_network_errors_need_reconnect() {
        let cases = [
            "connection refused",
            "no route to host",
            "network is unreachable",
            "broken pipe",
            "unexpected eof",
            "MySQL server has gone away",
            "lost connection to server",
            "can't connect to mysql server",
            "the server closed the connection",
            "got an error reading communication packets",
            "ssl handshake failed",
        ];
        for msg in cases {
            let (transient, reconnect, _) = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(transient, "should be transient: {}", msg);
            assert!(reconnect, "should need reconnect: {}", msg);
        }
    }

    #[test]
    fn test_classify_timeout_no_reconnect() {
        let (t, r, _) = classify_error(&anyhow::anyhow!("statement timed out"));
        assert!(t);
        assert!(!r, "timeout should not require reconnect");

        let (t, r, _) = classify_error(&anyhow::anyhow!("lock wait timeout exceeded"));
        assert!(t);
        assert!(!r);
    }

    #[test]
    fn test_classify_capacity_errors_extra_delay() {
        let (t, r, delay) = classify_error(&anyhow::anyhow!("too many connections"));
        assert!(t);
        assert!(r);
        assert!(delay >= 10_000, "capacity errors should have extra delay, got: {}ms", delay);

        let (t, _, delay) = classify_error(&anyhow::anyhow!("the database system is starting up"));
        assert!(t);
        assert!(delay >= 10_000);
    }

    #[test]
    fn test_classify_deadlock_retryable() {
        let (t, r, delay) = classify_error(&anyhow::anyhow!("deadlock detected"));
        assert!(t);
        assert!(!r, "deadlock should not require reconnect");
        assert!(delay >= 1_000, "deadlock should have small extra delay");
    }

    #[test]
    fn test_classify_permanent_errors() {
        let cases = ["syntax error", "permission denied", "relation does not exist", "column not found"];
        for msg in cases {
            let (transient, _, _) = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(!transient, "should NOT be transient: {}", msg);
        }
    }
}
