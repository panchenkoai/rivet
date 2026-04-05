mod chunked;
mod cli;
mod retry;
mod single;
mod sink;
mod validate;

#[allow(unused_imports)]
pub use chunked::generate_chunks;
pub use cli::{
    reset_chunk_checkpoint, reset_state, show_chunk_checkpoint, show_files, show_metrics,
    show_state,
};
#[allow(unused_imports)]
pub use retry::classify_error;
#[allow(unused_imports)]
pub use single::build_time_window_query;
#[allow(unused_imports)]
pub use validate::validate_output;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use retry::is_transient;

use std::path::Path;

use crate::config::{Config, ExportConfig, ExportMode};
use crate::error::Result;
use crate::state::StateStore;
use crate::tuning::{SourceTuning, TuningProfile, merge_tuning_config};

use chunked::run_chunked_parallel_checkpoint;
use single::run_with_reconnect;

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
            "{}_{}",
            export.name,
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
            eprintln!(
                "  schema:      {}",
                if sc { "CHANGED" } else { "unchanged" }
            );
        }
        if let Some(q) = self.quality_passed {
            eprintln!("  quality:     {}", if q { "pass" } else { "FAIL" });
        }
        if let Some(err) = &self.error_message {
            eprintln!("  error:       {}", err);
        }
    }
}

pub(crate) fn format_bytes(b: u64) -> String {
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

#[allow(clippy::too_many_arguments)]
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
    log::info!(
        "starting export '{}' (effective tuning: {})",
        export.name,
        tuning
    );

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
        ExportMode::Chunked if export.parallel > 1 => chunked::run_chunked_parallel(
            &config.source,
            state,
            export,
            &tuning,
            config_dir,
            validate,
            &mut summary,
            params,
        ),
        _ => run_with_reconnect(
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
        ),
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
        &summary.export_name,
        &summary.run_id,
        summary.duration_ms,
        summary.total_rows,
        Some(summary.peak_rss_mb),
        &summary.status,
        summary.error_message.as_deref(),
        Some(&tuning_class),
        Some(&summary.format),
        Some(&summary.mode),
        summary.files_produced as i64,
        summary.bytes_written as i64,
        summary.retries as i64,
        summary.validated,
        summary.schema_changed,
    );

    summary.print();
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);
}

/// Re-invoke this binary once per export. Children do not inherit parallel flags, so there is no recursion.
fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<()> {
    use std::process::{Command, Stdio};

    let exe = std::env::current_exe().map_err(|e| {
        anyhow::anyhow!(
            "failed to resolve rivet executable for child processes: {:#}",
            e
        )
    })?;

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
            anyhow::anyhow!(
                "failed to spawn rivet child for export '{}': {:#}",
                export.name,
                e
            )
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
            let code = status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string());
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

    let run_parallel_processes = (parallel_export_processes_cli
        || config.parallel_export_processes)
        && export_name.is_none()
        && exports.len() > 1;

    if run_parallel_processes {
        return run_exports_as_child_processes(config_path, &exports, validate, resume, params);
    }

    let run_parallel = (parallel_exports_cli || config.parallel_exports)
        && export_name.is_none()
        && exports.len() > 1;

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
                        anyhow::anyhow!(
                            "export '{}': failed to open state database: {:#}",
                            export.name,
                            e
                        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompressionType, FormatType, MetaColumns, TimeColumnType};
    use crate::tuning::SourceTuning;

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
        assert!(
            summary.run_id.starts_with("test_export_"),
            "run_id should start with export name, got: {}",
            summary.run_id
        );
    }
}
