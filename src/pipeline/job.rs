use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::{DataIntegrityError, Result};
use crate::plan::{
    DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, build_plan, validate_plan,
};
use crate::state::StateStore;

use super::RunOptions;
use super::chunked::{self, run_chunked_parallel_checkpoint};
use super::single::run_with_reconnect;
use super::summary::RunSummary;
use crate::journal::RunEvent;

/// Assemble the full `export_metrics` row (v9) from the finished run summary +
/// plan. One builder so the run and apply paths persist an identical shape
/// rather than each inlining the metric fields. The richer signals
/// (`pg_temp_bytes_delta`, `reconciled`/`source_count`, effective `batch_size`,
/// config dims, `rivet_version`) are what `record_metric`'s old 15-arg shim
/// dropped on the floor — they exist on the summary/plan here, so persist them.
fn build_metric_row(
    summary: &RunSummary,
    plan: &ResolvedRunPlan,
    tuning_class: &str,
) -> crate::state::MetricRow {
    let (chunk_size, parallel) = match &plan.strategy {
        crate::plan::ExtractionStrategy::Chunked(cp) => {
            (Some(cp.chunk_size as i64), Some(cp.parallel as i64))
        }
        _ => (None, None),
    };
    crate::state::MetricRow {
        export_name: summary.export_name.clone(),
        run_id: summary.run_id.clone(),
        duration_ms: summary.duration_ms,
        total_rows: summary.total_rows,
        peak_rss_mb: Some(summary.peak_rss_mb),
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        tuning_profile: Some(tuning_class.to_string()),
        format: Some(summary.format.clone()),
        mode: Some(summary.mode.clone()),
        files_produced: summary.files_produced as i64,
        bytes_written: summary.bytes_written as i64,
        retries: summary.retries as i64,
        validated: summary.validated,
        schema_changed: summary.schema_changed,
        files_committed: summary.files_committed as i64,
        reconciled: summary.reconciled,
        source_count: summary.source_count,
        quality_passed: summary.quality_passed,
        pg_temp_bytes_delta: summary.pg_temp_bytes_delta,
        batch_size: summary.batch_size as i64,
        batch_size_memory_mb: summary.batch_size_memory_mb.map(|m| m as i64),
        skip_reason: summary.skip_reason.clone(),
        schema_fingerprint: summary.schema_fingerprint.clone(),
        chunk_size,
        parallel,
        source_type: Some(format!("{:?}", plan.source.source_type).to_lowercase()),
        destination_type: Some(plan.destination.destination_type.label().to_string()),
        rivet_version: Some(env!("CARGO_PKG_VERSION").to_string()),
    }
}

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
        // Surface *which* checks failed via the shared failure contract — see
        // `crate::quality::failure_message`. (Chunked mode only aggregates
        // row_count; null/unique are per-chunk and warn-logged above.) Tagged as
        // a data-integrity failure (exit 3) so a scheduler stops rather than
        // retries; the message text is unchanged.
        let fails: Vec<&str> = row_issues.iter().map(|i| i.message.as_str()).collect();
        return Err(DataIntegrityError::new(crate::quality::failure_message(
            &plan.export_name,
            Some("chunked aggregate"),
            &fails,
        ))
        .into());
    }

    summary.quality_passed = Some(true);
    Ok(())
}

/// Snapshot `pg_stat_database.temp_bytes` for the run's source DB.
///
/// `None` for non-Postgres sources (no equivalent counter), or when the
/// snapshot probe fails (URL unresolvable, connection refused, view filtered).
/// Failures are silent — this is an observability metric, not a correctness
/// signal, so a failed probe must never block the actual export.
fn pg_temp_bytes_snapshot(plan: &ResolvedRunPlan) -> Option<i64> {
    if !matches!(plan.source.source_type, crate::config::SourceType::Postgres) {
        return None;
    }
    let url = plan.source.resolve_url().ok()?;
    crate::source::postgres::sample_temp_bytes(&url, plan.source.tls.as_ref())
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
                // ADR-0012 manifest-aware reconcile: compare source COUNT(*)
                // against the manifest's *cumulative* row total (sum of
                // committed parts), not just this run's writes.  In a
                // resume scenario, `summary.total_rows` reflects only the
                // chunks that re-ran in this invocation (e.g. 500 for one
                // chunk), while the on-disk dataset is everything that
                // ever committed (e.g. 2500 across resume attempts).
                // Comparing total_rows would falsely report MISMATCH on
                // every resume.  The manifest_parts accumulator already
                // holds the cumulative count (Phase C-γ hydration); use
                // its sum for the comparison.
                let committed_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
                let exported_total = if committed_rows > 0 {
                    committed_rows
                } else {
                    summary.total_rows
                };
                summary.reconciled = Some(exported_total == count);
                if exported_total != count {
                    log::warn!(
                        "reconcile MISMATCH for '{}': committed {} rows, source has {}",
                        plan.export_name,
                        exported_total,
                        count
                    );
                } else {
                    log::info!(
                        "reconcile MATCH for '{}': {}/{}",
                        plan.export_name,
                        exported_total,
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
    let journal = crate::journal::RunJournal::new(&run_id, export_name);
    RunSummary {
        run_id,
        export_name: export_name.to_string(),
        status: "failed".into(),
        total_rows: 0,
        files_produced: 0,
        bytes_written: 0,
        files_committed: 0,
        duration_ms: 0,
        peak_rss_mb: 0,
        retries: 0,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message: Some(crate::redact::redact_error(err)),
        tuning_profile: "balanced (default)".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: String::new(),
        mode: String::new(),
        compression: String::new(),
        // Pre-plan failure: we don't know (and never wrote to) a destination.
        destination_uri: None,
        source_count: None,
        pg_temp_bytes_delta: None,
        skip_reason: None,
        reconciled: None,
        manifest_parts: Vec::new(),
        schema_fingerprint: None,
        manifest_verification: None,
        apply_context: None,
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

    // ADR-0012 M8 / ADR-0013: refuse `--resume` against a destination whose
    // `_SUCCESS` marker is already present unless the operator explicitly
    // overrode the gate with `--force`.  Re-exporting over a verified
    // dataset is almost never what the operator meant; the gate makes the
    // override an audited decision.
    if opts.resume
        && !opts.force
        && let Err(e) = check_success_gate_for_resume(&plan)
    {
        let summary = synthetic_failed_summary(&export.name, &e);
        return (Err(e), summary);
    }

    // rerun-accumulation footgun (audit findings #5/#19/#30): a *fresh* run
    // (no `--resume`) into a prefix that already carries a completed export
    // does NOT overwrite — it appends a new timestamp-/nonce-named part set
    // alongside the old one and rewrites manifest.json to describe only this
    // run, so a glob reader over the prefix double-counts / sees orphaned
    // parts.  Refusing or auto-deleting would destroy operator data, so this
    // is a loud, non-fatal WARN instead (the `--resume` path above keeps its
    // refuse-without-`--force` gate).  `--force` is the explicit opt-out.
    if !opts.resume && !opts.force {
        warn_if_prefix_has_completed_run(&plan);
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

    // PG cursor / sort spill probe — captured around the actual run window.
    // Cluster-level counter, so this is a noisy upper bound on a shared host
    // but accurate on the single-tenant test DBs pilots typically use.
    let pg_temp_bytes_before = pg_temp_bytes_snapshot(&plan);

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

    // Compute the temp_bytes delta only when both snapshots succeeded — partial
    // failures (e.g. dropped connection between runs) leave the field None so
    // the summary card omits the line entirely.
    if let Some(before) = pg_temp_bytes_before
        && let Some(after) = pg_temp_bytes_snapshot(&plan)
    {
        let delta = (after - before).max(0);
        summary.pg_temp_bytes_delta = Some(delta);
        if delta > 100 * 1024 * 1024 {
            log::warn!(
                "export '{}': PG temp_bytes spill +{:.1} MB during run — cursor / sort overflow. \
                 Consider lowering `tuning.batch_size` or setting `tuning.batch_size_memory_mb` \
                 below PG's `work_mem`.",
                plan.export_name,
                delta as f64 / (1024.0 * 1024.0),
            );
        }
    }

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
            let redacted = crate::redact::redact_error(e);
            summary.error_message = Some(redacted.clone());
            log::error!("export '{}' failed: {}", plan.export_name, redacted);
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

    if let Err(e) = state.store_journal(&summary.journal) {
        log::warn!(
            "export '{}': journal persist failed (run history not stored): {:#}",
            summary.export_name,
            e
        );
    }

    summary.print();
    // Order matters: write the manifest first, then run the manifest-aware
    // `--validate` pass against the destination, then persist the metrics
    // row, then write the run report.  The report sees the verification
    // verdict only because we run it before `finalize_run_report`; the
    // metrics row must also wait for `finalize_validate_manifest`, which can
    // downgrade `summary.validated` — recording earlier left `rivet metrics`
    // permanently saying validated=pass for a run whose report says it
    // failed.  The notification fires last so it carries the most complete
    // summary.
    finalize_manifest(&plan, state, &summary, "export");
    if plan.validate {
        finalize_validate_manifest(&plan, &mut summary, "export");
    }
    if let Err(e) = state.record_metric_full(&build_metric_row(&summary, &plan, &tuning_class)) {
        log::warn!(
            "export '{}': metrics write failed (run outcome not stored): {:#}",
            summary.export_name,
            e
        );
    }
    finalize_run_report(config_path, &summary, "export");
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);

    let final_result = if failed { result } else { Ok(()) };
    (final_result, summary)
}

// `finalize_*` and the M8 success-gate live in `pipeline::finalize` so this
// file stays focused on orchestration (build plan → dispatch → record
// metric → call finalize hooks).  Imports below give us local names.
use super::finalize::{
    check_success_gate_for_resume, finalize_manifest, finalize_run_report,
    finalize_validate_manifest, warn_if_prefix_has_completed_run,
};

/// Execute a pre-resolved plan with a caller-supplied `ChunkSource`.
///
/// Used by `rivet apply`: the plan comes from a deserialized `PlanArtifact` so
/// `build_plan` is skipped.  Everything else — quality gate, metrics, state
/// persistence — is identical to `run_export_job`.
pub(crate) fn run_export_job_with_chunk_source(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    chunk_source: chunked::ChunkSource,
    config_path: &str,
    apply_context: Option<crate::pipeline::summary::ApplyContext>,
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
    summary.apply_context = apply_context;

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
            let redacted = crate::redact::redact_error(e);
            summary.error_message = Some(redacted.clone());
            log::error!("apply '{}' failed: {}", plan.export_name, redacted);
        }
    }

    summary.print();
    finalize_manifest(plan, state, &summary, "apply");
    if plan.validate {
        finalize_validate_manifest(plan, &mut summary, "apply");
    }
    // After finalize_validate_manifest: it can downgrade summary.validated,
    // and the metrics row must carry the final verdict (same ordering as
    // run_export_job).
    if let Err(e) = state.record_metric_full(&build_metric_row(&summary, plan, &tuning_class)) {
        log::warn!(
            "apply '{}': metrics write failed: {:#}",
            summary.export_name,
            e
        );
    }
    finalize_run_report(config_path, &summary, "apply");

    if failed { result } else { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_failed_summary_fields() {
        let err = anyhow::anyhow!("connection refused");
        let summary = synthetic_failed_summary("my_export", &err);
        assert_eq!(summary.export_name, "my_export");
        assert_eq!(summary.status, "failed");
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
        assert_eq!(summary.bytes_written, 0);
        assert!(
            summary
                .error_message
                .as_ref()
                .unwrap()
                .contains("connection refused")
        );
    }

    #[test]
    fn synthetic_failed_summary_run_id_contains_export_name() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(
            summary.run_id.starts_with("orders_"),
            "run_id was: {}",
            summary.run_id
        );
    }

    #[test]
    fn synthetic_failed_summary_journal_is_empty() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(summary.journal.entries.is_empty());
    }

    #[test]
    fn synthetic_failed_summary_no_quality_or_reconcile_state() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(summary.quality_passed.is_none());
        assert!(summary.reconciled.is_none());
        assert!(summary.validated.is_none());
    }

    // ── run_chunked_quality_gate ────────────────────────────────────────────
    //
    // The chunked quality gate is the post-aggregation row-count check that
    // fires AFTER every chunk has been written and the totals are known.
    // It is the only quality validation chunked mode supports — null_ratio
    // and unique_columns are explicitly out of scope because each chunk
    // processes independently. Tests pin the gate logic so changes to chunked
    // quality semantics surface as visible diffs, not silent behaviour drift.

    use crate::config::QualityConfig;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
    use crate::tuning::SourceTuning;

    fn chunked_plan_with_quality(quality: Option<QualityConfig>) -> ResolvedRunPlan {
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
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
                ..Default::default()
            },
            quality,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/x".into()),
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

    fn fresh_summary(plan: &ResolvedRunPlan, total_rows: i64) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("r", plan.export_name.clone());
        s.total_rows = total_rows;
        s.batch_size = 10_000;
        s.mode = "chunked".into();
        s.compression = "none".into();
        s
    }

    #[test]
    fn chunked_quality_gate_passes_through_existing_error() {
        // If the chunked run already failed, the gate must NOT mask that with
        // a successful Ok — the original error wins.
        let plan = chunked_plan_with_quality(None);
        let mut summary = fresh_summary(&plan, 0);
        let result = run_chunked_quality_gate(
            Err(anyhow::anyhow!("chunk 3 failed to write")),
            &plan,
            &mut summary,
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("chunk 3 failed"),
            "must propagate original error: {err}"
        );
        // quality_passed must remain None — we never got to evaluate it.
        assert!(summary.quality_passed.is_none());
    }

    #[test]
    fn chunked_quality_gate_no_quality_config_marks_no_decision() {
        // Without quality config, gate is a no-op and quality_passed stays None.
        let plan = chunked_plan_with_quality(None);
        let mut summary = fresh_summary(&plan, 5_000);
        run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect("must pass");
        assert!(summary.quality_passed.is_none());
    }

    #[test]
    fn chunked_quality_gate_row_count_within_bounds_passes() {
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(100),
            row_count_max: Some(10_000),
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 5_000);
        run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect("in bounds must pass");
        assert_eq!(summary.quality_passed, Some(true));
    }

    #[test]
    fn chunked_quality_gate_row_count_below_min_fails() {
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(100),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 42);
        let err =
            run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect_err("below min must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("quality check(s) failed") && msg.contains("chunked aggregate"),
            "error must name the failed quality gate: {err}"
        );
        assert!(
            msg.contains("  - "),
            "error must surface the specific failing check(s), not just a generic message: {err}"
        );
        // The chunked quality bail carries the DataIntegrityError marker → exit
        // class 3 (STOP). The operator message is unchanged (asserted above).
        assert!(
            err.downcast_ref::<DataIntegrityError>().is_some(),
            "chunked quality-gate failure must be a typed data-integrity error"
        );
        assert_eq!(crate::error::classify_exit(&err), 3);
        assert_eq!(summary.quality_passed, Some(false));
    }

    #[test]
    fn chunked_quality_gate_row_count_above_max_fails() {
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: None,
            row_count_max: Some(1_000),
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 50_000);
        let err =
            run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect_err("above max must fail");
        assert!(err.to_string().contains("quality"), "error: {err}");
        assert_eq!(summary.quality_passed, Some(false));
    }

    #[test]
    fn chunked_quality_gate_skips_unsupported_checks_with_warning() {
        // null_ratio_max and unique_columns are explicitly out of scope for
        // chunked mode. They must NOT cause failure; row_count alone decides.
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(10),
            row_count_max: None,
            null_ratio_max: [("name".into(), 0.1)].into_iter().collect(),
            unique_columns: vec!["id".into()],
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 1_000);
        run_chunked_quality_gate(Ok(()), &plan, &mut summary)
            .expect("unsupported checks must not fail in chunked mode");
        assert_eq!(summary.quality_passed, Some(true));
    }

    #[test]
    fn chunked_quality_gate_inactive_on_non_chunked_strategy() {
        // The gate must early-return on Snapshot/Incremental etc. — those
        // strategies validate inline via the streaming sink.
        let mut plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(99_999), // would fail if evaluated
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        plan.strategy = ExtractionStrategy::Snapshot;
        let mut summary = fresh_summary(&plan, 10);
        // No-op for non-chunked: must not fail even though min would not be met.
        run_chunked_quality_gate(Ok(()), &plan, &mut summary)
            .expect("non-chunked strategy must skip the gate");
        assert!(summary.quality_passed.is_none());
    }
}
