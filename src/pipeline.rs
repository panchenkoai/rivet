use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::config::{CompressionType, Config, ExportConfig, ExportMode, FormatType, MetaColumns, SourceConfig, TimeColumnType};
use crate::preflight::chunk_sparsity_from_counts;
use crate::destination;
use crate::enrich;
use crate::error::Result;
use crate::format::{self, FormatWriter};
use crate::resource;
use crate::source::{self, BatchSink, Source};
use crate::state::StateStore;
use crate::tuning::{merge_tuning_config, SourceTuning, TuningProfile};

/// Collects operational data during an export for end-of-run summary and metrics.
#[derive(Debug, Clone)]
pub struct RunSummary {
    pub run_id: String,
    pub export_name: String,
    pub status: String,
    pub total_rows: i64,
    pub files_produced: usize,
    pub bytes_written: u64,
    pub duration_ms: i64,
    pub peak_rss_mb: i64,
    pub retries: u32,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
    pub quality_passed: Option<bool>,
    pub error_message: Option<String>,
    /// `profile` from YAML, or `balanced (default)` if omitted.
    pub tuning_profile: String,
    /// Configured `batch_size` from YAML/profile (FETCH cap before `batch_size_memory_mb` override).
    pub batch_size: usize,
    /// When set, actual FETCH size is derived from schema (see logs / `SourceTuning::effective_batch_size`).
    pub batch_size_memory_mb: Option<usize>,
    pub format: String,
    pub mode: String,
    pub compression: String,
}

impl RunSummary {
    fn new(export: &ExportConfig, tuning: &SourceTuning, yaml_profile_label: &str) -> Self {
        let run_id = format!(
            "{}_{}", export.name,
            chrono::Utc::now().format("%Y%m%dT%H%M%S%.3f"),
        );
        Self {
            run_id,
            export_name: export.name.clone(),
            status: "running".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            duration_ms: 0,
            peak_rss_mb: 0,
            retries: 0,
            validated: None,
            schema_changed: None,
            quality_passed: None,
            error_message: None,
            tuning_profile: yaml_profile_label.to_string(),
            batch_size: tuning.batch_size,
            batch_size_memory_mb: tuning.batch_size_memory_mb,
            format: format!("{:?}", export.format).to_lowercase(),
            mode: format!("{:?}", export.mode).to_lowercase(),
            compression: format!("{:?}", export.compression).to_lowercase(),
        }
    }

    fn print(&self) {
        eprintln!();
        eprintln!("── {} ──", self.export_name);
        eprintln!("  run_id:      {}", self.run_id);
        eprintln!("  status:      {}", self.status);
        if let Some(mem) = self.batch_size_memory_mb {
            eprintln!(
                "  tuning:      profile={}, batch_size={} (batch_size_memory_mb={}MiB → effective FETCH in logs)",
                self.tuning_profile, self.batch_size, mem
            );
        } else {
            eprintln!(
                "  tuning:      profile={}, batch_size={}",
                self.tuning_profile, self.batch_size
            );
        }
        eprintln!("  rows:        {}", self.total_rows);
        eprintln!("  files:       {}", self.files_produced);
        if self.bytes_written > 0 {
            eprintln!("  bytes:       {}", format_bytes(self.bytes_written));
        }
        let dur = if self.duration_ms >= 1000 {
            format!("{:.1}s", self.duration_ms as f64 / 1000.0)
        } else {
            format!("{}ms", self.duration_ms)
        };
        eprintln!("  duration:    {}", dur);
        if self.peak_rss_mb > 0 {
            eprintln!("  peak RSS:    {}MB (sampled during run)", self.peak_rss_mb);
        }
        if self.format == "parquet" && self.compression != "zstd" {
            eprintln!("  compression: {}", self.compression);
        }
        if self.retries > 0 {
            eprintln!("  retries:     {}", self.retries);
        }
        if let Some(v) = self.validated {
            eprintln!("  validated:   {}", if v { "pass" } else { "FAIL" });
        }
        if let Some(sc) = self.schema_changed {
            eprintln!("  schema:      {}", if sc { "CHANGED" } else { "unchanged" });
        }
        if let Some(q) = self.quality_passed {
            eprintln!("  quality:     {}", if q { "pass" } else { "FAIL" });
        }
        if let Some(err) = &self.error_message {
            eprintln!("  error:       {}", err);
        }
    }
}

fn format_bytes(b: u64) -> String {
    if b >= 1_073_741_824 {
        format!("{:.1} GB", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1} MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
    }
}

fn run_export_job(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    validate: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
) {
    let merged = merge_tuning_config(config.source.tuning.as_ref(), export.tuning.as_ref());
    let tuning = SourceTuning::from_config(merged.as_ref());
    let yaml_profile_label = match merged.as_ref().and_then(|t| t.profile) {
        Some(TuningProfile::Fast) => "fast",
        Some(TuningProfile::Balanced) => "balanced",
        Some(TuningProfile::Safe) => "safe",
        None => "balanced (default)",
    };
    log::info!("starting export '{}' (effective tuning: {})", export.name, tuning);

    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(export, &tuning, yaml_profile_label);

    let result = match export.mode {
        ExportMode::Chunked if export.parallel > 1 && export.chunk_checkpoint => {
            run_chunked_parallel_checkpoint(
                config_path,
                &config.source,
                state,
                export,
                &tuning,
                config_dir,
                validate,
                &mut summary,
                params,
                resume,
            )
        }
        ExportMode::Chunked if export.parallel > 1 => {
            run_chunked_parallel(&config.source, state, export, &tuning, config_dir, validate, &mut summary, params)
        }
        _ => {
            run_with_reconnect(
                &config.source,
                state,
                export,
                &tuning,
                config_dir,
                validate,
                &mut summary,
                params,
                resume,
                config_path,
            )
        }
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    let tuning_class = tuning.profile_name().to_string();
    match &result {
        Ok(()) => {
            if summary.status == "running" {
                summary.status = "success".into();
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            summary.error_message = Some(format!("{:#}", e));
            log::error!("export '{}' failed: {:#}", export.name, e);
        }
    }

    let _ = state.record_metric(
        &summary.export_name, &summary.run_id, summary.duration_ms, summary.total_rows,
        Some(summary.peak_rss_mb), &summary.status, summary.error_message.as_deref(),
        Some(&tuning_class), Some(&summary.format), Some(&summary.mode),
        summary.files_produced as i64, summary.bytes_written as i64,
        summary.retries as i64, summary.validated, summary.schema_changed,
    );

    summary.print();
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);
}

/// Re-invoke this binary once per export (`run --config … --export <name>` only). Children do not inherit
/// parallel flags, so there is no recursion. Each process has its own address space → meaningful per-job RSS.
fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<()> {
    use std::process::{Command, Stdio};

    let exe = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("failed to resolve rivet executable for child processes: {:#}", e))?;

    let config_arg = std::path::Path::new(config_path)
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from(config_path));

    log::info!(
        "running {} exports as separate rivet processes (each child: single `--export`; SQLite state WAL allows concurrent writers)",
        exports.len()
    );

    let mut children: Vec<(String, std::process::Child)> = Vec::with_capacity(exports.len());
    for export in exports {
        let mut cmd = Command::new(&exe);
        cmd.arg("run")
            .arg("--config")
            .arg(&config_arg)
            .arg("--export")
            .arg(export.name.as_str());
        if validate {
            cmd.arg("--validate");
        }
        if resume {
            cmd.arg("--resume");
        }
        if let Some(p) = params {
            for (k, v) in p {
                cmd.arg("--param").arg(format!("{k}={v}"));
            }
        }
        cmd.stdin(Stdio::null());
        log::debug!("spawning child for export '{}': {:?}", export.name, cmd);
        let child = cmd.spawn().map_err(|e| {
            anyhow::anyhow!("failed to spawn rivet child for export '{}': {:#}", export.name, e)
        })?;
        children.push((export.name.clone(), child));
    }

    let mut failures = Vec::new();
    for (name, mut child) in children {
        let status = match child.wait() {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("export '{name}': wait failed: {e:#}"));
                continue;
            }
        };
        if !status.success() {
            let code = status.code().map(|c| c.to_string()).unwrap_or_else(|| "signal".to_string());
            failures.push(format!("export '{name}' exited with status {code}"));
        }
    }

    if !failures.is_empty() {
        anyhow::bail!("{}", failures.join("; "));
    }
    Ok(())
}

pub fn run(
    config_path: &str,
    export_name: Option<&str>,
    validate: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
    parallel_exports_cli: bool,
    parallel_export_processes_cli: bool,
) -> Result<()> {
    let config = Config::load_with_params(config_path, params)?;

    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

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

    let run_parallel_processes = (parallel_export_processes_cli || config.parallel_export_processes)
        && export_name.is_none()
        && exports.len() > 1;

    if run_parallel_processes {
        return run_exports_as_child_processes(config_path, &exports, validate, resume, params);
    }

    let run_parallel =
        (parallel_exports_cli || config.parallel_exports) && export_name.is_none() && exports.len() > 1;

    if run_parallel {
        log::info!(
            "running {} exports in parallel (separate state DB connection per export)",
            exports.len()
        );
        let mut state_errors: Vec<anyhow::Error> = Vec::new();
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for &export in &exports {
                handles.push(s.spawn(|| {
                    let state = StateStore::open(config_path).map_err(|e| {
                        anyhow::anyhow!("export '{}': failed to open state database: {:#}", export.name, e)
                    })?;
                    run_export_job(
                        config_path,
                        &config,
                        export,
                        &state,
                        &config_dir,
                        validate,
                        resume,
                        params,
                    );
                    Ok::<(), anyhow::Error>(())
                }));
            }
            for h in handles {
                match h.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => state_errors.push(e),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
        });
        if !state_errors.is_empty() {
            let text = state_errors
                .into_iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(anyhow::anyhow!(text));
        }
    } else {
        let state = StateStore::open(config_path)?;
        for export in exports {
            run_export_job(
                config_path,
                &config,
                export,
                &state,
                &config_dir,
                validate,
                resume,
                params,
            );
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
    summary: &mut RunSummary,
    params: Option<&std::collections::HashMap<String, String>>,
    resume: bool,
    config_path: &str,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 0..=tuning.max_retries {
        if attempt > 0 {
            summary.retries = attempt;
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

        match run_export(
            &mut *src,
            source_config,
            state,
            export,
            tuning,
            config_dir,
            validate,
            summary,
            params,
            resume,
            config_path,
        ) {
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

    // Auth / credential errors are never transient — fix config, not retry
    if msg.contains("loading credential")
        || msg.contains("loadcredential")
        || msg.contains("metadata.google.internal")
        || msg.contains("permission denied")
        || msg.contains("access denied")
        || msg.contains("invalid_grant")
        || msg.contains("token has been expired or revoked")
    {
        return (false, false, 0);
    }

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
    source_config: &SourceConfig,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
    summary: &mut RunSummary,
    params: Option<&std::collections::HashMap<String, String>>,
    resume: bool,
    config_path: &str,
) -> Result<()> {
    let base_query = export.resolve_query(config_dir, params)?;

    match export.mode {
        ExportMode::Full => {
            run_single_export(src, &base_query, None, None, export, tuning, validate, Some(state), summary)?;
        }
        ExportMode::Incremental => {
            let cursor_state = state.get(&export.name)?;
            let cursor_col = export.cursor_column.as_deref();
            run_single_export(src, &base_query, cursor_col, Some(&cursor_state), export, tuning, validate, Some(state), summary)?;
        }
        ExportMode::Chunked => {
            if export.chunk_checkpoint {
                run_chunked_sequential_checkpoint(
                    src,
                    source_config,
                    state,
                    &base_query,
                    export,
                    tuning,
                    validate,
                    summary,
                    Some(state),
                    resume,
                    config_path,
                )?;
            } else {
                run_chunked_sequential(src, &base_query, export, tuning, validate, summary, Some(state))?;
            }
        }
        ExportMode::TimeWindow => {
            let windowed_query = build_time_window_query(
                &base_query,
                export.time_column.as_deref().unwrap(),
                export.time_column_type,
                export.days_window.unwrap(),
            );
            run_single_export(src, &windowed_query, None, None, export, tuning, validate, Some(state), summary)?;
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
    summary: &mut RunSummary,
) -> Result<()> {
    let mut sink = ExportSink::new(export)?;

    src.export(query, cursor_column, cursor, tuning, &mut sink)?;

    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    summary.total_rows += sink.total_rows as i64;
    log::info!("export '{}': {} rows written", export.name, sink.total_rows);

    if sink.total_rows == 0 {
        if export.skip_empty {
            summary.status = "skipped".into();
            log::info!("export '{}': skipped (0 rows, skip_empty=true)", export.name);
        } else {
            log::info!("export '{}': no data to export", export.name);
        }
        return Ok(());
    }

    let quality_issues = sink.run_quality_checks();
    if !quality_issues.is_empty() {
        for issue in &quality_issues {
            let level = match issue.severity {
                crate::quality::Severity::Fail => "FAIL",
                crate::quality::Severity::Warn => "WARN",
            };
            log::warn!("quality {}: {}", level, issue.message);
        }
        if quality_issues.iter().any(|i| i.severity == crate::quality::Severity::Fail) {
            summary.quality_passed = Some(false);
            anyhow::bail!("export '{}': quality checks failed", export.name);
        }
    }
    if export.quality.is_some() {
        summary.quality_passed = Some(true);
    }

    // Collect the final part (whatever is left in sink.tmp)
    if sink.part_rows > 0 {
        sink.completed_parts.push(CompletedPart {
            tmp: std::mem::replace(&mut sink.tmp, tempfile::NamedTempFile::new()?),
            rows: sink.part_rows,
        });
    }

    let fmt = format::create_format(export.format, export.compression, export.compression_level);
    let ext = fmt.file_extension();
    let dest = destination::create_destination(&export.destination)?;
    let has_parts = sink.completed_parts.len() > 1;
    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");

    for (part_idx, part) in sink.completed_parts.iter().enumerate() {
        if validate {
            validate_output(part.tmp.path(), export.format, part.rows)?;
            summary.validated = Some(true);
        }

        let file_bytes = std::fs::metadata(part.tmp.path()).map(|m| m.len()).unwrap_or(0);
        summary.bytes_written += file_bytes;
        summary.files_produced += 1;

        let file_name = if has_parts {
            format!("{}_{}_part{}.{}", export.name, ts, part_idx, ext)
        } else {
            format!("{}_{}.{}", export.name, ts, ext)
        };
        dest.write(part.tmp.path(), &file_name)?;

        if let Some(st) = state {
            let _ = st.record_file(
                &summary.run_id, &export.name, &file_name,
                part.rows as i64, file_bytes as i64,
                &format!("{:?}", export.format).to_lowercase(),
                Some(&format!("{:?}", export.compression).to_lowercase()),
            );
        }
    }

    if export.mode == ExportMode::Incremental
        && let (Some(cursor_col), Some(batch), Some(schema), Some(st)) =
            (&export.cursor_column, &sink.last_batch, &sink.schema, state)
            && let Some(last_val) = extract_last_cursor_value(batch, cursor_col, schema) {
                st.update(&export.name, &last_val)?;
                log::info!("export '{}': cursor updated to '{}'", export.name, last_val);
            }

    if let (Some(schema), Some(st)) = (&sink.schema, state) {
        let columns: Vec<crate::state::SchemaColumn> = schema.fields().iter().map(|f| {
            crate::state::SchemaColumn {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            }
        }).collect();

        match st.detect_schema_change(&export.name, &columns) {
            Ok(Some(change)) => {
                summary.schema_changed = Some(true);
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
            Ok(None) => {
                summary.schema_changed = Some(false);
            }
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
    summary: &mut RunSummary,
    state: Option<&StateStore>,
) -> Result<()> {
    let col = export.chunk_column.as_deref().unwrap();
    let chunks = detect_and_generate_chunks(
        src,
        base_query,
        col,
        export.chunk_size,
        &export.name,
        export.chunk_dense,
    )?;

    log::info!("export '{}': {} chunks to process sequentially", export.name, chunks.len());

    for (i, (start, end)) in chunks.iter().enumerate() {
        if !resource::check_memory(tuning.memory_threshold_mb) {
            log::warn!("memory threshold exceeded, pausing 5s before chunk {}", i);
            std::thread::sleep(Duration::from_secs(5));
        }

        let chunk_query = build_chunk_query_sql(base_query, col, *start, *end, export.chunk_dense);
        log::info!("export '{}': chunk {}/{} ({}..{})", export.name, i + 1, chunks.len(), start, end);

        let mut sink = ExportSink::new(export)?;
        src.export(&chunk_query, None, None, tuning, &mut sink)?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
        }

        summary.total_rows += sink.total_rows as i64;
        log::info!("export '{}': chunk {} -- {} rows", export.name, i + 1, sink.total_rows);

        if sink.total_rows > 0 {
            if validate {
                validate_output(sink.tmp.path(), export.format, sink.total_rows)?;
                summary.validated = Some(true);
            }
            let file_bytes = std::fs::metadata(sink.tmp.path()).map(|m| m.len()).unwrap_or(0);
            summary.bytes_written += file_bytes;
            summary.files_produced += 1;

            let fmt = format::create_format(export.format, export.compression, export.compression_level);
            let file_name = format!("{}_{}_chunk{}.{}", export.name, chrono::Utc::now().format("%Y%m%d_%H%M%S"), i, fmt.file_extension());
            let dest = destination::create_destination(&export.destination)?;
            dest.write(sink.tmp.path(), &file_name)?;

            if let Some(st) = state {
                let _ = st.record_file(
                    &summary.run_id, &export.name, &file_name,
                    sink.total_rows as i64, file_bytes as i64,
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

fn run_chunked_parallel(
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
    let col = export.chunk_column.as_deref().unwrap();

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
    log::info!("export '{}': {} chunks, {} parallel threads", export.name, total_chunks, parallel);

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
                        let file_bytes = std::fs::metadata(sink.tmp.path()).map(|m| m.len()).unwrap_or(0);
                        agg_bytes.fetch_add(file_bytes, Ordering::Relaxed);
                        agg_files.fetch_add(1, Ordering::Relaxed);

                        let fmt = format::create_format(
                            format_type,
                            export_for_sink.compression,
                            export_for_sink.compression_level,
                        );
                        let file_name = format!(
                            "{}_{}_chunk{}.{}",
                            export_name, chrono::Utc::now().format("%Y%m%d_%H%M%S"), i, fmt.file_extension()
                        );
                        let dest = destination::create_destination(dest_config)?;
                        dest.write(sink.tmp.path(), &file_name)?;
                        file_records.lock().unwrap().push((file_name, sink.total_rows as i64, file_bytes as i64));
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

    summary.total_rows = agg_rows.load(Ordering::Relaxed);
    summary.bytes_written = agg_bytes.load(Ordering::Relaxed);
    summary.files_produced = agg_files.load(Ordering::Relaxed);
    if validate {
        summary.validated = Some(true);
    }

    let fmt_name = format!("{:?}", export.format).to_lowercase();
    let comp_name = format!("{:?}", export.compression).to_lowercase();
    for (fname, rows, bytes) in file_records.into_inner().unwrap() {
        let _ = state.record_file(
            &summary.run_id, &export.name, &fname,
            rows, bytes, &fmt_name, Some(&comp_name),
        );
    }

    let errs = errors.into_inner().unwrap();
    if !errs.is_empty() {
        anyhow::bail!("export '{}': {} chunks failed:\n{}", export.name, errs.len(), errs.join("\n"));
    }

    log::info!("export '{}': all {} chunks completed", export.name, total_chunks);
    Ok(())
}

// ─── Chunk checkpoint (SQLite plan + resume) ─────────────────

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

fn chunk_max_attempts_for_export(export: &ExportConfig, tuning: &SourceTuning) -> u32 {
    export
        .chunk_max_attempts
        .unwrap_or_else(|| tuning.max_retries.saturating_add(1).max(1))
}

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
                export.name, export.name
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
            log::warn!("export '{}': reset {} stale 'running' chunk task(s) after resume", export.name, n);
        }
        return Ok(rid);
    }

    if let Some((rid, _)) = state.find_in_progress_chunk_run(&export.name)? {
        anyhow::bail!(
            "export '{}': chunk checkpoint run '{}' still in progress; use `rivet run --resume` or `rivet state reset-chunks --export {}`",
            export.name, rid, export.name
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
    let file_bytes = std::fs::metadata(sink.tmp.path()).map(|m| m.len()).unwrap_or(0);

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
                export.name, chunk_index, attempt, tuning.max_retries, backoff
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
fn run_chunked_sequential_checkpoint(
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
    let col = export.chunk_column.as_deref().unwrap();

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

    let run_id = ensure_chunk_checkpoint_plan(state, export, summary, base_query, col, &chunks, resume, tuning)?;

    if !resume && !resource::check_memory(tuning.memory_threshold_mb) {
        log::warn!("memory threshold exceeded before chunk export; pausing 5s");
        std::thread::sleep(Duration::from_secs(5));
    }

    while let Some((chunk_index, sk, ek)) = state.claim_next_chunk_task(&run_id)? {
        if !resource::check_memory(tuning.memory_threshold_mb) {
            log::warn!("memory threshold exceeded, pausing 5s before chunk {}", chunk_index);
            std::thread::sleep(Duration::from_secs(5));
        }

        let start: i64 = sk
            .parse()
            .map_err(|_| anyhow::anyhow!("chunk {}: invalid start_key {:?}", chunk_index, sk))?;
        let end: i64 = ek
            .parse()
            .map_err(|_| anyhow::anyhow!("chunk {}: invalid end_key {:?}", chunk_index, ek))?;

        log::info!("export '{}': checkpoint chunk {} ({}..{})", export.name, chunk_index, start, end);

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
                    if let Some(ref name) = fname {
                        if let Some(store) = st {
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
                }
                state.complete_chunk_task(&run_id, chunk_index, rows as i64, fname.as_deref())?;
            }
            Err(e) => {
                let msg = format!("{:#}", e);
                log::error!("export '{}': chunk {} failed: {}", export.name, chunk_index, msg);
                state.fail_chunk_task(&run_id, chunk_index, &msg)?;
            }
        }
    }

    let pending = state.count_chunk_tasks_not_completed(&run_id)?;
    if pending > 0 {
        anyhow::bail!(
            "export '{}': chunk checkpoint incomplete ({} tasks not completed); fix errors and `rivet run --resume` or `rivet state reset-chunks`",
            export.name, pending
        );
    }

    state.finalize_chunk_run_completed(&run_id)?;
    log::info!("export '{}': chunk checkpoint run completed (run_id={})", export.name, run_id);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_chunked_parallel_checkpoint(
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
    let col = export.chunk_column.as_deref().unwrap();

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

    let run_id = ensure_chunk_checkpoint_plan(state, export, summary, &base_query, col, &chunks, resume, tuning)?;

    let total_tasks = {
        let tasks = state.list_chunk_tasks_for_run(&run_id)?;
        tasks.len().max(1)
    };
    let parallel = export.parallel.min(total_tasks);
    log::info!(
        "export '{}': chunk checkpoint parallel: {} workers, run_id={}",
        export.name, parallel, run_id
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
                    let claimed = match StateStore::claim_next_chunk_task_at_path(&db_path, run_id_arc.as_str()) {
                        Ok(c) => c,
                        Err(e) => {
                            errors
                                .lock()
                                .unwrap()
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
                                thread_src.export(&chunk_query, None, None, &tuning_w, &mut sink)?;
                                if let Some(w) = sink.writer.take() {
                                    w.finish()?;
                                }
                                if sink.total_rows == 0 {
                                    return Ok((0, None, 0));
                                }
                                if validate {
                                    validate_output(sink.tmp.path(), format_type, sink.total_rows)?;
                                }
                                let file_bytes =
                                    std::fs::metadata(sink.tmp.path()).map(|m| m.len()).unwrap_or(0);
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
                        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk failed after retries")))
                    })();

                    match result {
                        Ok((rows, fname, file_bytes)) => {
                            agg_rows.fetch_add(rows as i64, Ordering::Relaxed);
                            if rows > 0 {
                                agg_bytes.fetch_add(file_bytes, Ordering::Relaxed);
                                agg_files.fetch_add(1, Ordering::Relaxed);
                                if let (Some(ref name), Ok(store)) = (
                                    fname.as_ref(),
                                    StateStore::open(&config_path_w),
                                ) {
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
                            errors.lock().unwrap().push(format!("chunk {}: {}", chunk_index, msg));
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

    let errs = errors.into_inner().unwrap();
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
            export.name, pending, export.name
        );
    }

    state.finalize_chunk_run_completed(&run_id)?;
    log::info!("export '{}': chunk checkpoint parallel run completed", export.name);
    Ok(())
}

pub fn reset_chunk_checkpoint(config_path: &str, export_name: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let n = state.reset_chunk_checkpoint(export_name)?;
    println!("Removed {} chunk run record(s) for export '{}'.", n, export_name);
    Ok(())
}

pub fn show_chunk_checkpoint(config_path: &str, export_name: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    println!("database:   {}", StateStore::state_db_path(config_path).display());
    let Some((run_id, plan_hash, status, updated_at)) = state.get_latest_chunk_run(export_name)? else {
        println!("No chunk checkpoint data for export '{}'.", export_name);
        return Ok(());
    };
    println!("export:     {}", export_name);
    println!("run_id:     {}", run_id);
    println!("plan_hash:  {}", plan_hash);
    println!("status:     {}", status);
    println!("updated_at: {}", updated_at);
    println!();
    println!(
        "{:<6} {:<12} {:<18} {:<18} {:>4} {:>8} {}",
        "IDX", "STATUS", "START", "END", "ATT", "ROWS", "FILE"
    );
    println!("{}", "-".repeat(90));
    for t in state.list_chunk_tasks_for_run(&run_id)? {
        let file = t.file_name.as_deref().unwrap_or("-");
        let rows = t.rows_written.map(|r| r.to_string()).unwrap_or_else(|| "-".into());
        println!(
            "{:<6} {:<12} {:<18} {:<18} {:>4} {:>8} {}",
            t.chunk_index, t.status, t.start_key, t.end_key, t.attempts, rows, file
        );
        if let Some(e) = &t.last_error {
            println!("       error: {}", e);
        }
    }
    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────

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
        log::info!("export '{}': no BETWEEN windows (empty key range)", export_name);
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
        let empty_hint = ((1.0 - info.density).min(1.0).max(0.0)) * 100.0;
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

    log::info!(
        "export '{}': chunk_column `{}` range {} .. {} (chunk_size={})",
        export_name, chunk_column, min_val, max_val, chunk_size
    );

    let chunks = generate_chunks(min_val, max_val, chunk_size as i64);

    match query_wrapped_row_count(src, base_query) {
        Ok(row_count) => {
            log_chunk_sparsity_at_run(export_name, chunk_column, chunk_size, min_val, max_val, &chunks, row_count);
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

struct CompletedPart {
    tmp: tempfile::NamedTempFile,
    rows: usize,
}

struct ExportSink {
    writer: Option<Box<dyn FormatWriter>>,
    format_type: FormatType,
    compression: CompressionType,
    compression_level: Option<u32>,
    tmp: tempfile::NamedTempFile,
    total_rows: usize,
    part_rows: usize,
    last_batch: Option<RecordBatch>,
    schema: Option<SchemaRef>,
    meta: MetaColumns,
    enriched_schema: Option<SchemaRef>,
    exported_at_us: i64,
    quality_null_counts: std::collections::HashMap<String, usize>,
    quality_unique_sets: std::collections::HashMap<String, std::collections::HashSet<String>>,
    quality_columns: Option<crate::config::QualityConfig>,
    max_file_size: Option<u64>,
    completed_parts: Vec<CompletedPart>,
    /// When set, this column is removed from Arrow batches before enrichment and write (see `chunk_dense`).
    strip_internal_column: Option<String>,
}

impl ExportSink {
    fn new(export: &ExportConfig) -> Result<Self> {
        let tmp = tempfile::NamedTempFile::new()?;
        let exported_at_us = chrono::Utc::now().timestamp_micros();
        Ok(Self {
            writer: None,
            format_type: export.format,
            compression: export.compression,
            compression_level: export.compression_level,
            tmp,
            total_rows: 0,
            part_rows: 0,
            last_batch: None,
            schema: None,
            meta: export.meta_columns.clone(),
            enriched_schema: None,
            exported_at_us,
            quality_null_counts: std::collections::HashMap::new(),
            quality_unique_sets: std::collections::HashMap::new(),
            quality_columns: export.quality.clone(),
            max_file_size: export.max_file_size_bytes(),
            completed_parts: Vec::new(),
            strip_internal_column: if export.chunk_dense {
                Some(RIVET_CHUNK_RN_COL.to_string())
            } else {
                None
            },
        })
    }

    fn schema_without_internal(schema: &Schema, name: &str) -> Result<SchemaRef> {
        let idx = schema.index_of(name)?;
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    }

    fn record_batch_without_internal(batch: &RecordBatch, name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let idx = schema.index_of(name)?;
        let indices: Vec<usize> = (0..schema.fields().len()).filter(|&i| i != idx).collect();
        batch
            .project(&indices)
            .map_err(|e| anyhow::anyhow!("project batch without {}: {}", name, e))
    }

    fn maybe_split(&mut self) -> Result<()> {
        let max = match self.max_file_size {
            Some(m) => m,
            None => return Ok(()),
        };
        let written = self.writer.as_ref().map(|w| w.bytes_written()).unwrap_or(0);
        if written < max || self.part_rows == 0 {
            return Ok(());
        }

        if let Some(w) = self.writer.take() {
            w.finish()?;
        }

        let old_tmp = std::mem::replace(&mut self.tmp, tempfile::NamedTempFile::new()?);
        self.completed_parts.push(CompletedPart {
            tmp: old_tmp,
            rows: self.part_rows,
        });
        self.part_rows = 0;

        if let Some(schema) = &self.enriched_schema {
            let fmt = format::create_format(self.format_type, self.compression, self.compression_level);
            let file = self.tmp.as_file().try_clone()?;
            let buf_writer = BufWriter::new(file);
            self.writer = Some(fmt.create_writer(schema, Box::new(buf_writer))?);
        }

        log::info!("file split: started part {}", self.completed_parts.len() + 1);
        Ok(())
    }

    fn track_quality(&mut self, batch: &RecordBatch) {
        let qc = match &self.quality_columns {
            Some(q) => q,
            None => return,
        };
        let schema = batch.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            if qc.null_ratio_max.contains_key(name.as_str()) {
                *self.quality_null_counts.entry(name.clone()).or_default() += batch.column(i).null_count();
            }
            if qc.unique_columns.contains(name) {
                let col = batch.column(i);
                let set = self.quality_unique_sets.entry(name.clone()).or_default();
                if let Ok(formatter) = arrow::util::display::ArrayFormatter::try_new(
                    col.as_ref(),
                    &arrow::util::display::FormatOptions::default(),
                ) {
                    for row in 0..col.len() {
                        set.insert(formatter.value(row).to_string());
                    }
                }
            }
        }
    }

    fn run_quality_checks(&self) -> Vec<crate::quality::QualityIssue> {
        let qc = match &self.quality_columns {
            Some(q) => q,
            None => return Vec::new(),
        };
        let mut issues = Vec::new();
        issues.extend(crate::quality::check_row_count(self.total_rows, qc));

        if self.total_rows > 0 {
            for (col, max_ratio) in &qc.null_ratio_max {
                let nulls = self.quality_null_counts.get(col).copied().unwrap_or(0);
                let ratio = nulls as f64 / self.total_rows as f64;
                if ratio > *max_ratio {
                    issues.push(crate::quality::QualityIssue {
                        severity: crate::quality::Severity::Fail,
                        message: format!(
                            "column '{}': null ratio {:.4} exceeds threshold {:.4}",
                            col, ratio, max_ratio
                        ),
                    });
                }
            }

            for col in &qc.unique_columns {
                if let Some(set) = self.quality_unique_sets.get(col) {
                    let dupes = self.total_rows.saturating_sub(set.len());
                    if dupes > 0 {
                        issues.push(crate::quality::QualityIssue {
                            severity: crate::quality::Severity::Fail,
                            message: format!(
                                "column '{}': {} duplicate values out of {} rows",
                                col, dupes, self.total_rows
                            ),
                        });
                    }
                }
            }
        }
        issues
    }
}

impl BatchSink for ExportSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        let data_schema = if let Some(ref strip) = self.strip_internal_column {
            Self::schema_without_internal(schema.as_ref(), strip)?
        } else {
            schema.clone()
        };
        let enriched = enrich::enrich_schema(&data_schema, &self.meta);
        let fmt = format::create_format(self.format_type, self.compression, self.compression_level);
        let file = self.tmp.as_file().try_clone()?;
        let buf_writer = BufWriter::new(file);
        self.writer = Some(fmt.create_writer(&enriched, Box::new(buf_writer))?);
        self.schema = Some(data_schema);
        self.enriched_schema = Some(enriched);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let batch = if let Some(ref strip) = self.strip_internal_column {
            Self::record_batch_without_internal(batch, strip)?
        } else {
            batch.clone()
        };

        self.total_rows += batch.num_rows();
        self.part_rows += batch.num_rows();
        self.track_quality(&batch);

        let output = if let Some(es) = &self.enriched_schema {
            enrich::enrich_batch(&batch, &self.meta, es, self.exported_at_us)?
        } else {
            batch.clone()
        };

        if let Some(w) = self.writer.as_mut() {
            w.write_batch(&output)?;
        }
        self.last_batch = Some(batch);
        self.maybe_split()?;
        Ok(())
    }
}

#[allow(dead_code)]
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

pub fn show_files(config_path: &str, export_name: Option<&str>, limit: usize) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let files = state.get_files(export_name, limit)?;
    if files.is_empty() {
        println!("No files recorded yet.");
        return Ok(());
    }
    println!(
        "{:<35} {:<40} {:>8} {:>10} CREATED",
        "RUN ID", "FILE", "ROWS", "BYTES"
    );
    println!("{}", "-".repeat(110));
    for f in &files {
        println!(
            "{:<35} {:<40} {:>8} {:>10} {}",
            f.run_id, f.file_name, f.row_count,
            format_bytes(f.bytes as u64), f.created_at,
        );
    }
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
        "{:<20} {:<10} {:>10} {:>10} {:>8} {:>6} {:>10} RUN ID",
        "EXPORT", "STATUS", "ROWS", "DURATION", "RSS", "FILES", "BYTES"
    );
    println!("{}", "-".repeat(110));
    for m in &metrics {
        let duration = if m.duration_ms >= 1000 {
            format!("{:.1}s", m.duration_ms as f64 / 1000.0)
        } else {
            format!("{}ms", m.duration_ms)
        };
        let rss = m.peak_rss_mb.map(|r| format!("{}MB", r)).unwrap_or_else(|| "-".into());
        let bytes = if m.bytes_written > 0 {
            format_bytes(m.bytes_written as u64)
        } else {
            "-".into()
        };
        let run_id = m.run_id.as_deref().unwrap_or(&m.run_at);
        println!(
            "{:<20} {:<10} {:>10} {:>10} {:>8} {:>6} {:>10} {}",
            m.export_name, m.status, m.total_rows, duration, rss, m.files_produced, bytes, run_id
        );
        if let Some(err) = &m.error_message {
            println!("  Error: {}", err);
        }
        let mut flags = Vec::new();
        if m.retries > 0 {
            flags.push(format!("retries={}", m.retries));
        }
        if let Some(v) = m.validated {
            flags.push(format!("validated={}", if v { "pass" } else { "FAIL" }));
        }
        if let Some(sc) = m.schema_changed {
            flags.push(format!("schema={}", if sc { "CHANGED" } else { "ok" }));
        }
        if !flags.is_empty() {
            println!("  {}", flags.join("  "));
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

    #[test]
    fn test_classify_credential_errors_not_transient() {
        let cases = [
            "loading credential to sign http request",
            "error sending request for url (http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token): dns error",
            "invalid_grant: Token has been expired or revoked",
            "Access Denied: no permission",
        ];
        for msg in cases {
            let (transient, _, _) = classify_error(&anyhow::anyhow!("{}", msg));
            assert!(!transient, "credential error should NOT be transient: {}", msg);
        }
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(2_684_354_560), "2.5 GB");
    }

    #[test]
    fn test_run_summary_fields() {
        let export = ExportConfig {
            name: "test_export".into(),
            query: Some("SELECT 1".into()),
            query_file: None,
            mode: ExportMode::Full,
            cursor_column: None,
            chunk_column: None,
            chunk_size: 100_000,
            parallel: 1,
            time_column: None,
            time_column_type: TimeColumnType::Timestamp,
            days_window: None,
            format: FormatType::Parquet,
            compression: CompressionType::default(),
            compression_level: None,
            skip_empty: false,
            destination: crate::config::DestinationConfig {
                destination_type: crate::config::DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("./out".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
            },
            meta_columns: MetaColumns::default(),
            quality: None,
            max_file_size: None,
            chunk_checkpoint: false,
            chunk_max_attempts: None,
            tuning: None,
            chunk_dense: false,
        };
        let tuning = SourceTuning::from_config(None);
        let summary = RunSummary::new(&export, &tuning, "balanced (default)");
        assert_eq!(summary.export_name, "test_export");
        assert_eq!(summary.status, "running");
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
        assert_eq!(summary.tuning_profile, "balanced (default)");
        assert_eq!(summary.batch_size, 10_000);
        assert_eq!(summary.format, "parquet");
        assert_eq!(summary.mode, "full");
        assert!(summary.run_id.starts_with("test_export_"), "run_id should start with export name, got: {}", summary.run_id);
    }
}
