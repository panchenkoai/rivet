use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::plan::{
    DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, build_plan, validate_plan,
};
use crate::state::StateStore;

use super::RunOptions;
use super::chunked::{self, run_chunked_parallel_checkpoint};
use super::journal::RunEvent;
use super::single::run_with_reconnect;
use super::summary::RunSummary;

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
    if let Some(col) = plan.strategy.cursor_column() {
        log::info!(
            "reconcile: skipping for incremental export '{}' (cursor column '{}', count may differ)",
            plan.export_name,
            col
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

/// Synthesize a stand-in `RunSummary` for failures that occur **before** a
/// real summary can be created (plan-build errors, plan-validation rejection).
/// Aggregation needs every export accounted for, even those that never reached
/// `RunSummary::new`.
pub(crate) fn synthetic_failed_summary(export_name: &str, err: &anyhow::Error) -> RunSummary {
    let run_id = format!(
        "{}_{}",
        export_name,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3f"),
    );
    let journal = super::journal::RunJournal::new(&run_id, export_name);
    RunSummary {
        run_id,
        export_name: export_name.to_string(),
        status: "failed".into(),
        total_rows: 0,
        files_produced: 0,
        bytes_written: 0,
        duration_ms: 0,
        peak_rss_mb: 0,
        retries: 0,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message: Some(format!("{:#}", err)),
        tuning_profile: "balanced (default)".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: String::new(),
        mode: String::new(),
        compression: String::new(),
        source_count: None,
        reconciled: None,
        journal,
    }
}

pub(super) fn run_export_job(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    opts: &RunOptions<'_>,
) -> (Result<()>, RunSummary) {
    let plan = match build_plan(
        config,
        export,
        config_dir,
        opts.validate,
        opts.reconcile,
        opts.resume,
        opts.params,
    ) {
        Ok(p) => p,
        Err(e) => {
            let summary = synthetic_failed_summary(&export.name, &e);
            return (Err(e), summary);
        }
    };

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
        let err = anyhow::anyhow!(
            "export '{}': plan validation failed:\n  {}",
            plan.export_name,
            rejected.join("\n  ")
        );
        let summary = synthetic_failed_summary(&export.name, &err);
        return (Err(err), summary);
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

    let final_result = if failed { result } else { Ok(()) };
    (final_result, summary)
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
