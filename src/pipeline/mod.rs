//! **Layer: Coordinator** (planning → execution → persistence/observability)
//!
//! `pipeline/mod.rs` is the only module allowed to bridge all three layers.
//! It reads a resolved plan (planning), dispatches to execution modules, then
//! records metrics and sends notifications (persistence/observability).
//!
//! See `docs/adr/0003-layer-classification.md` for the full module taxonomy.

mod apply_cmd;
mod chunked;
mod cli;
pub mod journal;
mod plan_cmd;
pub(crate) mod progress;
mod retry;
mod single;
mod sink;
mod summary;
mod validate;

pub use apply_cmd::run_apply_command;
#[allow(unused_imports)]
pub use chunked::generate_chunks;
pub use cli::{
    reset_chunk_checkpoint, reset_state, show_chunk_checkpoint, show_files, show_metrics,
    show_state,
};
pub use plan_cmd::{PlanOutputFormat, run_plan_command};
#[allow(unused_imports)]
pub use retry::classify_error;
// build_time_window_query moved to crate::plan; re-exported here for integration tests.
#[allow(unused_imports)]
pub use crate::plan::build_time_window_query;
#[allow(unused_imports)]
pub use validate::validate_output;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use retry::is_transient;

use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::plan::{
    DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, build_plan, validate_plan,
};
use crate::state::StateStore;

use chunked::run_chunked_parallel_checkpoint;
use journal::RunEvent;
use single::run_with_reconnect;
pub use summary::RunSummary;

/// Per-run configuration flags passed from the CLI to the pipeline.
///
/// Replaces the previous pattern of threading 4+ positional `bool` arguments
/// through `run`, `run_export_job`, and child-process invocations.  Named fields
/// prevent silent argument transposition (e.g., `validate` and `reconcile`
/// swapped).
#[derive(Debug, Clone, Copy)]
pub struct RunOptions<'a> {
    pub validate: bool,
    pub reconcile: bool,
    pub resume: bool,
    pub params: Option<&'a std::collections::HashMap<String, String>>,
}

/// For chunked mode: quality checks (row_count, null_ratio, uniqueness) cannot run per-batch
/// inside the chunk workers because each chunk has its own `ExportSink`. This gate runs
/// row_count bounds after all chunks complete. Null/unique checks are warned-and-skipped.
fn run_chunked_quality_gate(
    result: Result<()>,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<()> {
    result?;

    if !matches!(plan.strategy, ExtractionStrategy::Chunked(_)) {
        return Ok(());
    }
    let qc = match &plan.quality {
        Some(q) => q,
        None => return Ok(()),
    };

    let total = summary.total_rows as usize;
    let row_issues = crate::quality::check_row_count(total, qc);
    let has_unsupported = !qc.null_ratio_max.is_empty() || !qc.unique_columns.is_empty();

    if has_unsupported {
        log::warn!(
            "export '{}': quality checks null_ratio_max and unique_columns are not supported in chunked mode (each chunk processes independently); only row_count bounds are checked",
            plan.export_name
        );
    }

    if !row_issues.is_empty() {
        for issue in &row_issues {
            log::warn!("quality FAIL: {}", issue.message);
        }
        summary.quality_passed = Some(false);
        anyhow::bail!(
            "export '{}': quality checks failed (chunked aggregate)",
            plan.export_name
        );
    }

    summary.quality_passed = Some(true);
    Ok(())
}

/// Run `SELECT COUNT(*) FROM ({query})` against the source and compare with exported rows.
/// Skips reconciliation for incremental exports that used a cursor (moving target).
fn reconcile_source_count(plan: &ResolvedRunPlan, summary: &mut RunSummary) {
    if matches!(plan.strategy, ExtractionStrategy::Incremental { .. }) {
        log::info!(
            "reconcile: skipping for incremental export '{}' (cursor-based, count may differ)",
            plan.export_name
        );
        return;
    }

    let count_sql = format!(
        "SELECT COUNT(*) FROM ({}) AS _rivet_reconcile",
        plan.base_query
    );
    log::info!(
        "reconcile: running source count query for '{}'",
        plan.export_name
    );

    let mut src = match crate::source::create_source(&plan.source) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("reconcile: could not connect to source: {:#}", e);
            return;
        }
    };

    match src.query_scalar(&count_sql) {
        Ok(Some(val)) => {
            if let Ok(count) = val.parse::<i64>() {
                summary.source_count = Some(count);
                summary.reconciled = Some(summary.total_rows == count);
                if summary.total_rows != count {
                    log::warn!(
                        "reconcile MISMATCH for '{}': exported {} rows, source has {}",
                        plan.export_name,
                        summary.total_rows,
                        count
                    );
                } else {
                    log::info!(
                        "reconcile MATCH for '{}': {}/{}",
                        plan.export_name,
                        summary.total_rows,
                        count
                    );
                }
            } else {
                log::warn!(
                    "reconcile: could not parse count result '{}' as integer",
                    val
                );
            }
        }
        Ok(None) => {
            log::warn!(
                "reconcile: COUNT(*) returned NULL for '{}'",
                plan.export_name
            );
        }
        Err(e) => {
            log::warn!(
                "reconcile: count query failed for '{}': {:#}",
                plan.export_name,
                e
            );
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

fn run_export_job(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    opts: &RunOptions<'_>,
) -> Result<()> {
    let plan = build_plan(
        config,
        export,
        config_dir,
        opts.validate,
        opts.reconcile,
        opts.resume,
        opts.params,
    )?;

    let diags = validate_plan(&plan);
    let mut rejected: Vec<String> = Vec::new();
    for d in &diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                log::error!("[{}] plan validation rejected: {}", d.rule, d.message);
                rejected.push(d.message.clone());
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }
    if !rejected.is_empty() {
        anyhow::bail!(
            "export '{}': plan validation failed:\n  {}",
            plan.export_name,
            rejected.join("\n  ")
        );
    }

    log::info!(
        "starting export '{}' (effective tuning: {})",
        plan.export_name,
        plan.tuning
    );

    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(&plan);

    // Record plan diagnostics that were already logged above.
    for d in &diags {
        if matches!(
            d.level,
            DiagnosticLevel::Warning | DiagnosticLevel::Degraded
        ) {
            summary.journal.record(RunEvent::PlanWarning {
                rule: d.rule.to_string(),
                message: d.message.clone(),
            });
        }
    }

    let result = if plan.strategy.requires_parallel_execution() {
        if plan.strategy.is_resumable() {
            run_chunked_parallel_checkpoint(
                config_path,
                state,
                &plan,
                &mut summary,
                chunked::ChunkSource::Detect,
            )
        } else {
            chunked::run_chunked_parallel(state, &plan, &mut summary, chunked::ChunkSource::Detect)
        }
    } else {
        run_with_reconnect(state, &plan, &mut summary, config_path)
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    let tuning_class = plan.tuning.profile_name().to_string();
    let result = run_chunked_quality_gate(result, &plan, &mut summary);
    let failed = result.is_err();
    match &result {
        Ok(()) => {
            if summary.status == "running" {
                summary.status = "success".into();
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            summary.error_message = Some(format!("{:#}", e));
            log::error!("export '{}' failed: {:#}", plan.export_name, e);
        }
    }

    if plan.reconcile && !failed {
        reconcile_source_count(&plan, &mut summary);
        if let (Some(source_count), Some(matched)) = (summary.source_count, summary.reconciled) {
            summary.journal.record(RunEvent::ReconciliationResult {
                source_count,
                exported_rows: summary.total_rows,
                matched,
            });
        }
    }

    summary.journal.record(RunEvent::RunCompleted {
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        duration_ms: summary.duration_ms,
    });

    if let Err(e) = state.record_metric(
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
    ) {
        log::warn!(
            "export '{}': metrics write failed (run outcome not stored): {:#}",
            summary.export_name,
            e
        );
    }

    summary.print();
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);

    if failed { result } else { Ok(()) }
}

/// Execute a pre-resolved plan with a caller-supplied `ChunkSource`.
///
/// Used by `rivet apply`: the plan comes from a deserialized `PlanArtifact` so
/// `build_plan` is skipped.  Everything else — quality gate, metrics, state
/// persistence — is identical to `run_export_job`.
pub(crate) fn run_export_job_with_chunk_source(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    chunk_source: chunked::ChunkSource,
) -> Result<()> {
    // Re-validate the plan from the artifact (fast, no DB queries).
    let diags = validate_plan(plan);
    for d in &diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                anyhow::bail!(
                    "export '{}': plan validation rejected: {}",
                    plan.export_name,
                    d.message
                );
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }

    log::info!(
        "apply: starting export '{}' (tuning: {})",
        plan.export_name,
        plan.tuning
    );

    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(plan);

    let result = if plan.strategy.requires_parallel_execution() {
        if plan.strategy.is_resumable() {
            // apply does not support checkpoint-parallel resume; use Detect fallback
            run_chunked_parallel_checkpoint("", state, plan, &mut summary, chunk_source)
        } else {
            chunked::run_chunked_parallel(state, plan, &mut summary, chunk_source)
        }
    } else {
        run_with_reconnect(state, plan, &mut summary, "")
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    let tuning_class = plan.tuning.profile_name().to_string();
    let result = run_chunked_quality_gate(result, plan, &mut summary);
    let failed = result.is_err();

    match &result {
        Ok(()) => {
            if summary.status == "running" {
                summary.status = "success".into();
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            summary.error_message = Some(format!("{:#}", e));
            log::error!("apply '{}' failed: {:#}", plan.export_name, e);
        }
    }

    if let Err(e) = state.record_metric(
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
    ) {
        log::warn!(
            "apply '{}': metrics write failed: {:#}",
            summary.export_name,
            e
        );
    }

    summary.print();

    if failed { result } else { Ok(()) }
}

/// Re-invoke this binary once per export. Children do not inherit parallel flags, so there is no recursion.
fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    reconcile: bool,
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
        if reconcile {
            cmd.arg("--reconcile");
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

#[allow(clippy::too_many_arguments)]
pub fn run(
    config_path: &str,
    export_name: Option<&str>,
    validate: bool,
    reconcile: bool,
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

    let opts = RunOptions {
        validate,
        reconcile,
        resume,
        params,
    };

    let run_parallel_processes = (parallel_export_processes_cli
        || config.parallel_export_processes)
        && export_name.is_none()
        && exports.len() > 1;

    if run_parallel_processes {
        return run_exports_as_child_processes(
            config_path,
            &exports,
            validate,
            reconcile,
            resume,
            params,
        );
    }

    let run_parallel = (parallel_exports_cli || config.parallel_exports)
        && export_name.is_none()
        && exports.len() > 1;

    if run_parallel {
        log::info!(
            "running {} exports in parallel (separate state DB connection per export)",
            exports.len()
        );
        let mut export_errors: Vec<anyhow::Error> = Vec::new();
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
                    run_export_job(config_path, &config, export, &state, &config_dir, &opts)
                }));
            }
            for h in handles {
                match h.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => export_errors.push(e),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
        });
        if !export_errors.is_empty() {
            let text = export_errors
                .into_iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(anyhow::anyhow!(text));
        }
    } else {
        let state = StateStore::open(config_path)?;
        let mut failures = Vec::new();
        for export in &exports {
            if let Err(e) = run_export_job(config_path, &config, export, &state, &config_dir, &opts)
            {
                failures.push(format!("{:#}", e));
            }
        }
        if !failures.is_empty() {
            anyhow::bail!("{}", failures.join("; "));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SourceConfig, SourceType};
    use crate::plan::{
        CompressionType, DestinationConfig, DestinationType, DiagnosticLevel, ExtractionStrategy,
        FormatType, MetaColumns, ResolvedRunPlan, validate_plan,
    };
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
    fn format_bytes_boundary_values() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1025), "1.0 KB");
        assert_eq!(format_bytes(1_048_575), "1024.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_823), "1024.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    fn minimal_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "test_export".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::default(),
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
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
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced (default)".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://localhost/test".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                tuning: None,
            },
        }
    }

    #[test]
    fn test_run_summary_fields() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);
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

    // ─── RunSummary::new() journal invariants ────────────────────────────────

    /// `RunSummary::new()` must immediately record a `PlanResolved` event as the
    /// first journal entry.  This satisfies the "what was planned?" query from ADR-0001.
    #[test]
    fn run_summary_new_records_plan_resolved_as_first_event() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);

        assert!(
            !summary.journal.entries.is_empty(),
            "journal must have at least one entry after RunSummary::new()"
        );
        assert!(
            matches!(
                summary.journal.entries[0].event,
                journal::RunEvent::PlanResolved(_)
            ),
            "first journal event must be PlanResolved, got: {:?}",
            summary.journal.entries[0].event
        );
    }

    /// The `PlanSnapshot` recorded inside `PlanResolved` must faithfully capture
    /// key fields from the `ResolvedRunPlan`.
    #[test]
    fn run_summary_plan_snapshot_matches_plan_fields() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);

        let snap = summary
            .journal
            .plan_snapshot()
            .expect("plan_snapshot() must be Some after RunSummary::new()");

        assert_eq!(snap.export_name, plan.export_name);
        assert_eq!(snap.validate, plan.validate);
        assert_eq!(snap.reconcile, plan.reconcile);
        assert_eq!(snap.resume, plan.resume);
        assert_eq!(snap.batch_size, plan.tuning.batch_size);
    }

    /// The journal's `run_id` must match the `RunSummary`'s `run_id`.
    #[test]
    fn run_summary_journal_run_id_matches_summary_run_id() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);
        assert_eq!(
            summary.journal.run_id, summary.run_id,
            "journal run_id must match summary run_id"
        );
    }

    // ─── Rejected plan gate ──────────────────────────────────────────────────

    /// Gap 7 — `run_export_job` bails before execution when `validate_plan` returns
    /// a `Rejected` diagnostic.  The wiring is in `run_export_job` (this file,
    /// ~line 210): if `rejected` is non-empty, the function returns `anyhow::bail!`.
    ///
    /// This test verifies the *condition* that triggers the bail: that `validate_plan`
    /// does in fact produce a `Rejected` diagnostic for the stdout+split combination.
    /// The gate itself (`run_export_job`) cannot be called directly in tests because
    /// it requires a live database connection and config; we test its precondition here.
    #[test]
    fn rejected_plan_produces_rejected_diagnostic_blocking_run_export_job() {
        let mut plan = minimal_plan();
        // stdout + max_file_size triggers check_stdout_split → Rejected.
        plan.destination.destination_type = DestinationType::Stdout;
        plan.max_file_size_bytes = Some(10 * 1024 * 1024);

        let diags = validate_plan(&plan);
        let rejected_count = diags
            .iter()
            .filter(|d| d.level == DiagnosticLevel::Rejected)
            .count();

        assert!(
            rejected_count > 0,
            "stdout + max_file_size must produce a Rejected diagnostic so that \
             run_export_job bails before calling run_with_reconnect; got: {:?}",
            diags
                .iter()
                .map(|d| (&d.rule, &d.level))
                .collect::<Vec<_>>()
        );
    }

    /// stdout + chunked strategy also triggers a Rejected diagnostic (check_stdout_chunked).
    #[test]
    fn rejected_plan_stdout_chunked_blocks_run_export_job() {
        use crate::plan::ChunkedPlan;
        let mut plan = minimal_plan();
        plan.destination.destination_type = DestinationType::Stdout;
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 1000,
            parallel: 1,
            dense: false,
            by_days: None,
            max_attempts: 3,
            checkpoint: false,
        });

        let diags = validate_plan(&plan);
        assert!(
            diags.iter().any(|d| d.level == DiagnosticLevel::Rejected),
            "stdout + chunked must produce a Rejected diagnostic"
        );
    }
}
