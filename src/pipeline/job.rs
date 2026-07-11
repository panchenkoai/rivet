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
        longest_chunk_ms: summary.journal.longest_chunk_ms(),
        // v12 chunking diagnostics: which key was chunked and whether it is a known
        // unique/PK. (The resolved strategy is already `mode` = strategy.mode_label.)
        // A sparse-key post-mortem is now one SELECT: mode + chunk_key + is_pk.
        chunk_key: plan.strategy.chunk_key().map(str::to_string),
        chunk_key_is_unique_pk: plan.strategy.chunk_key_is_unique_pk(),
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

/// Snapshot the broader source-harm counters for the run's source engine, as
/// `(metric, cumulative_value)` pairs, dispatched by source type to the
/// per-engine probe. `None` for any connect/query failure or a source whose
/// probe is unavailable (e.g. MSSQL without `VIEW SERVER STATE`) — harm metrics
/// are observability, never a gate, so a missing snapshot just yields no
/// `export_harm` rows.
fn harm_snapshot(plan: &ResolvedRunPlan) -> Option<Vec<(String, i64)>> {
    let url = plan.source.resolve_url().ok()?;
    let tls = plan.source.tls.as_ref();
    match plan.source.source_type {
        crate::config::SourceType::Postgres => {
            crate::source::postgres::sample_harm_counters(&url, tls)
        }
        crate::config::SourceType::Mysql => crate::source::mysql::sample_harm_counters(&url, tls),
        crate::config::SourceType::Mssql => crate::source::mssql::sample_harm_counters(&url, tls),
        crate::config::SourceType::Mongo => crate::source::mongo::sample_harm_counters(&url, tls),
    }
}

/// Per-metric delta (`after - before`, floored at 0) for counters present in
/// both snapshots, matched by name. Floored because these are monotonic
/// cumulative counters within a run; a negative would only arise from a counter
/// reset (server restart mid-run) and is not meaningful harm.
fn harm_deltas(before: &[(String, i64)], after: &[(String, i64)]) -> Vec<(String, i64)> {
    let bmap: std::collections::HashMap<&str, i64> =
        before.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    after
        .iter()
        .filter_map(|(k, after_v)| {
            bmap.get(k.as_str())
                .map(|b| (k.clone(), (after_v - b).max(0)))
        })
        .collect()
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
        cursor_column: None,
        cursor_low: None,
        cursor_high: None,
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
        column_checksums: Vec::new(),
        checksum_key_column: None,
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
    // CDC exports read the transaction log, not a query — they bypass the batch
    // plan/strategy machinery entirely and run through the dedicated CDC runner,
    // which produces the same (Result, RunSummary) contract + metric row.
    if export.mode == crate::config::ExportMode::Cdc {
        // `initial: snapshot`: anchor first, then each pending table's full
        // snapshot (a recursive `mode: full` run into `…/snapshot/`, with its
        // own metric + journal), then the drain below. A failed snapshot fails
        // the export — the anchor stays, so the retry resumes gap-free.
        let pending = match super::cdc_job::initial_snapshot_pending(config, export) {
            Ok(p) => p,
            Err(e) => {
                let summary = synthetic_failed_summary(&export.name, &e);
                return (Err(e), summary);
            }
        };
        for synth in &pending {
            let (res, summary) =
                run_export_job(config_path, config, synth, state, config_dir, opts);
            if res.is_err() {
                return (res, summary);
            }
        }
        return super::cdc_job::run_cdc_export(config_path, config, export, state);
    }
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
    // Tier 2: broader source-harm counters (locks, rows read, buffer misses,
    // temp files) bracketed around the same run window; the per-counter delta is
    // stored in export_harm. Best-effort — see `harm_snapshot`.
    let harm_before = harm_snapshot(&plan);

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

    // Tier 2: record the per-counter source-harm delta. A failed or absent probe
    // (e.g. missing VIEW SERVER STATE on MSSQL) leaves no rows — never fatal.
    if let Some(before) = &harm_before
        && let Some(after) = harm_snapshot(&plan)
    {
        let deltas = harm_deltas(before, &after);
        if let Err(e) = state.record_harm(&summary.run_id, &summary.export_name, &deltas) {
            log::debug!(
                "export '{}': harm metrics write failed (informational): {:#}",
                summary.export_name,
                e
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
                mongo: None,
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

    // ── build_metric_row ────────────────────────────────────────────────────
    //
    // The builder is what actually decides *what* lands in every `export_metrics`
    // row, so a single field wired to the wrong summary/plan member silently
    // persists a wrong metric for the entire pilot. The metrics-store test
    // (`record_metric_full_persists_v9_columns_in_order`) only guards the
    // MetricRow→SQL column mapping; this pins the summary/plan→MetricRow mapping
    // upstream of it. Every look-alike pair (files_committed vs files_produced,
    // source_count vs total_rows, chunk_size vs parallel) gets a *distinct* value
    // so a field swap surfaces as a wrong-value read, not a passing tie.

    #[test]
    fn build_metric_row_maps_every_summary_and_plan_field() {
        let mut summary = RunSummary::stub_for_testing("run-bmr", "orders");
        summary.duration_ms = 1234;
        summary.total_rows = 50_000;
        summary.peak_rss_mb = 142;
        summary.status = "success".into();
        summary.error_message = Some("boom".into());
        summary.format = "parquet".into();
        summary.mode = "chunked".into();
        summary.files_produced = 7;
        summary.bytes_written = 4096;
        summary.retries = 2;
        summary.validated = Some(true);
        summary.schema_changed = Some(false);
        // v9 signals — each distinct from its look-alike sibling.
        summary.files_committed = 6; // ≠ files_produced (7)
        summary.reconciled = Some(true);
        summary.source_count = Some(49_999); // ≠ total_rows (50_000)
        summary.quality_passed = Some(true);
        summary.pg_temp_bytes_delta = Some(1_048_576);
        summary.batch_size = 32_000;
        summary.batch_size_memory_mb = Some(256);
        summary.skip_reason = Some("manual".into());
        summary.schema_fingerprint = Some("fp-abc".into());

        // v10: longest_chunk_ms is delegated to the journal. Inject one paired
        // 640ms chunk span (via the journal's own test helper, not a reach into
        // `entries`) so the field is a known Some — proving the builder reads
        // the journal rather than hardcoding None.
        summary.journal.push_test_chunk_span(0, 640);

        // Chunked plan with distinct chunk_size/parallel so a swap can't pass.
        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 100_000,
            chunk_count: None,
            parallel: 4,
            dense: false,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        });

        // Destructure WITHOUT `..` so a new `MetricRow` field is a COMPILE error
        // here until it's bound and asserted — "every field" becomes a
        // compiler-enforced invariant, not a hopeful test name. (The earlier
        // `row.field` form silently ignored any field added to the struct.)
        let crate::state::MetricRow {
            export_name,
            run_id,
            duration_ms,
            total_rows,
            peak_rss_mb,
            status,
            error_message,
            tuning_profile,
            format,
            mode,
            files_produced,
            bytes_written,
            retries,
            validated,
            schema_changed,
            files_committed,
            reconciled,
            source_count,
            quality_passed,
            pg_temp_bytes_delta,
            batch_size,
            batch_size_memory_mb,
            skip_reason,
            schema_fingerprint,
            chunk_size,
            parallel,
            source_type,
            destination_type,
            rivet_version,
            longest_chunk_ms,
            chunk_key,
            chunk_key_is_unique_pk,
        } = build_metric_row(&summary, &plan, "safe");

        // ── core (v1) ──
        assert_eq!(export_name, "orders");
        assert_eq!(run_id, "run-bmr");
        assert_eq!(duration_ms, 1234);
        assert_eq!(total_rows, 50_000);
        assert_eq!(peak_rss_mb, Some(142));
        assert_eq!(status, "success");
        assert_eq!(error_message.as_deref(), Some("boom"));
        assert_eq!(tuning_profile.as_deref(), Some("safe")); // builder arg
        assert_eq!(format.as_deref(), Some("parquet"));
        assert_eq!(mode.as_deref(), Some("chunked"));
        assert_eq!(files_produced, 7);
        assert_eq!(bytes_written, 4096);
        assert_eq!(retries, 2);
        assert_eq!(validated, Some(true));
        assert_eq!(schema_changed, Some(false));
        // ── v9 ──
        assert_eq!(files_committed, 6);
        assert_eq!(reconciled, Some(true));
        assert_eq!(source_count, Some(49_999));
        assert_eq!(quality_passed, Some(true));
        assert_eq!(pg_temp_bytes_delta, Some(1_048_576));
        assert_eq!(batch_size, 32_000);
        assert_eq!(batch_size_memory_mb, Some(256));
        assert_eq!(skip_reason.as_deref(), Some("manual"));
        assert_eq!(schema_fingerprint.as_deref(), Some("fp-abc"));
        // ── plan-derived ──
        assert_eq!(chunk_size, Some(100_000));
        assert_eq!(parallel, Some(4));
        assert_eq!(source_type.as_deref(), Some("postgres"));
        assert_eq!(destination_type.as_deref(), Some("local"));
        assert_eq!(rivet_version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        // ── v10: delegated to the journal (pinned to the injected span) ──
        assert_eq!(longest_chunk_ms, Some(640));
        assert_eq!(longest_chunk_ms, summary.journal.longest_chunk_ms());
        // ── v12: chunking diagnostics — chunk key + PK-ness. The plan is a range
        // Chunked on "id", so the key is "id" and PK-ness is None (range does not
        // probe). The resolved strategy is the `mode` column ("chunked"), above.
        assert_eq!(chunk_key.as_deref(), Some("id"));
        assert_eq!(chunk_key_is_unique_pk, None);
    }

    #[test]
    fn build_metric_row_non_chunked_has_no_chunk_dims() {
        // The chunk-config dimensions only exist for the Chunked strategy; every
        // other strategy must leave chunk_size/parallel NULL (the `_ => (None,
        // None)` arm) rather than persist a stale or zero value.
        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Snapshot;
        let summary = RunSummary::stub_for_testing("run-snap", "orders");

        let row = build_metric_row(&summary, &plan, "balanced");

        assert!(row.chunk_size.is_none(), "snapshot has no chunk_size");
        assert!(row.parallel.is_none(), "snapshot has no parallel");
        // The non-chunk dimensions are still populated.
        assert_eq!(row.source_type.as_deref(), Some("postgres"));
        assert_eq!(row.destination_type.as_deref(), Some("local"));
    }

    // ── harm_deltas ─────────────────────────────────────────────────────────
    //
    // The per-counter delta feeding `export_harm`. Three semantics to pin:
    // matched counters subtract, a counter reset floors at 0 (never a negative
    // "harm"), and the result is the *name intersection* of the two snapshots
    // (a counter present in only one snapshot is dropped, not treated as 0).

    #[test]
    fn harm_deltas_subtracts_matched_counters() {
        let before = vec![
            ("pg_tup_returned".to_string(), 100),
            ("pg_blks_read".to_string(), 5),
        ];
        let after = vec![
            ("pg_tup_returned".to_string(), 150),
            ("pg_blks_read".to_string(), 9),
        ];
        let mut got = harm_deltas(&before, &after);
        got.sort();
        assert_eq!(
            got,
            vec![
                ("pg_blks_read".to_string(), 4),
                ("pg_tup_returned".to_string(), 50)
            ]
        );
    }

    #[test]
    fn harm_deltas_floors_counter_reset_at_zero() {
        // A mid-run server restart resets the cumulative counter; after < before
        // must not surface as negative harm.
        let before = vec![("pg_tup_returned".to_string(), 1_000)];
        let after = vec![("pg_tup_returned".to_string(), 40)];
        assert_eq!(
            harm_deltas(&before, &after),
            vec![("pg_tup_returned".to_string(), 0)]
        );
    }

    #[test]
    fn harm_deltas_intersects_counter_names() {
        // Only counters present in BOTH snapshots are emitted: a metric in only
        // `before` (probe stopped exposing it) or only `after` (newly appeared)
        // has no honest delta and is dropped.
        let before = vec![("shared".to_string(), 10), ("only_before".to_string(), 1)];
        let after = vec![("shared".to_string(), 25), ("only_after".to_string(), 7)];
        assert_eq!(
            harm_deltas(&before, &after),
            vec![("shared".to_string(), 15)]
        );
    }
}
