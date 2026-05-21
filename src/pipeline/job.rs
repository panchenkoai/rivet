use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::plan::{
    DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, build_plan, validate_plan,
};
use crate::state::StateStore;

use super::RunOptions;
use super::chunked::{self, run_chunked_parallel_checkpoint};
use super::single::run_with_reconnect;
use super::summary::RunSummary;
use crate::journal::RunEvent;

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
        error_message: Some(format!("{:#}", err)),
        tuning_profile: "balanced (default)".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: String::new(),
        mode: String::new(),
        compression: String::new(),
        source_count: None,
        pg_temp_bytes_delta: None,
        reconciled: None,
        manifest_parts: Vec::new(),
        schema_fingerprint: None,
        manifest_verification: None,
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

    if let Err(e) = state.store_journal(&summary.journal) {
        log::warn!(
            "export '{}': journal persist failed (run history not stored): {:#}",
            summary.export_name,
            e
        );
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
            "export '{}': metrics write failed (run outcome not stored): {:#}",
            summary.export_name,
            e
        );
    }

    summary.print();
    // Order matters: write the manifest first, then run the manifest-aware
    // `--validate` pass against the destination, then write the run report.
    // The report sees the verification verdict only because we run it before
    // `finalize_run_report`.  The notification fires last so it carries the
    // most complete summary.
    finalize_manifest(&plan, state, &summary, "export");
    if plan.validate {
        finalize_validate_manifest(&plan, &mut summary, "export");
    }
    finalize_run_report(config_path, &summary, "export");
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);

    let final_result = if failed { result } else { Ok(()) };
    (final_result, summary)
}

/// Write `.rivet/runs/<run_id>/{summary.md,summary.json}` and surface a stderr
/// hint pointing at the report (plus a resume command, when applicable).
///
/// Failures to write are non-fatal: the run keeps its existing exit code, the
/// reason is logged, and the resume hint is still shown so the operator can
/// recover even if disk-full prevents the report itself from landing.
fn finalize_run_report(config_path: &str, summary: &RunSummary, kind: &str) {
    use std::io::Write;

    let dir = crate::pipeline::report::report_dir(config_path, &summary.run_id);
    let written = match crate::pipeline::report::write_run_report(config_path, summary) {
        Ok(_) => true,
        Err(e) => {
            log::warn!(
                "{} '{}': run report write failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            false
        }
    };

    if crate::pipeline::ipc::capturing_events() {
        // The parent UI owns the screen in capturing mode; an extra stderr
        // tail here would interleave with the rendered cards.  The JSON/MD
        // files are still on disk for whoever wants them.
        return;
    }

    let stderr = std::io::stderr();
    let mut h = stderr.lock();
    if written {
        let _ = writeln!(h, "report:    {}", dir.join("summary.md").display());
    }
    if summary.status == "failed" && summary.files_committed > 0 {
        let _ = writeln!(
            h,
            "resume:    rivet run --config {} --resume",
            crate::pipeline::report::shell_quote(config_path)
        );
    }
    let _ = h.flush();
}

/// Build the cloud-output manifest from the run's accumulated parts and write
/// it (plus `_SUCCESS` for clean runs) to the destination.
///
/// ADR-0012 M1 / M2 / M7: parts are already committed, manifest is written
/// next, then `_SUCCESS` only when status == Success.  Failures are non-fatal
/// — the run keeps its exit code and operators can investigate via the local
/// run report.
fn finalize_manifest(
    plan: &crate::plan::ResolvedRunPlan,
    state: &StateStore,
    summary: &RunSummary,
    kind: &str,
) {
    use crate::manifest::ManifestStatus;
    use crate::pipeline::manifest_writer::{ManifestBuilder, WriteOutcome, write_manifest};

    let snapshot = match summary.journal.plan_snapshot() {
        Some(s) => s,
        None => {
            // Synthetic-failure summaries never recorded a PlanResolved event.
            // There is no committed work to manifest; just log and return.
            log::debug!(
                "{} '{}': no plan snapshot, manifest skipped",
                kind,
                summary.export_name
            );
            return;
        }
    };

    let status = match summary.status.as_str() {
        "success" => ManifestStatus::Success,
        "failed" => ManifestStatus::Failed,
        _ => ManifestStatus::Interrupted,
    };

    // ADR-0012 M3: prefer the fingerprint captured at the sink (single +
    // chunked + checkpoint paths all populate it).  Fall back to the
    // state-store lookup only for resume scenarios where the live summary
    // never saw a schema.  The placeholder is a last-resort signal to the
    // reader that schema evidence was unavailable for this run.
    let schema_fingerprint = summary
        .schema_fingerprint
        .clone()
        .or_else(|| {
            state
                .get_stored_schema(&summary.export_name)
                .ok()
                .flatten()
                .map(|cols| crate::state::schema_fingerprint(&cols))
        })
        .unwrap_or_else(|| "xxh3:0000000000000000".to_string());

    let source_engine = match plan.source.source_type {
        crate::config::SourceType::Postgres => "postgres",
        crate::config::SourceType::Mysql => "mysql",
    };

    // `export_name` is often `schema.table`; split for the manifest fields
    // without fabricating values for free-form queries.
    let (source_schema, source_table) = match summary.export_name.split_once('.') {
        Some((s, t)) if !s.is_empty() && !t.is_empty() => {
            (Some(s.to_string()), Some(t.to_string()))
        }
        _ => (None, None),
    };

    let started_at = summary
        .journal
        .entries
        .first()
        .map(|e| e.recorded_at)
        .unwrap_or_else(chrono::Utc::now);

    let mut builder = ManifestBuilder::new(
        snapshot,
        &summary.run_id,
        started_at,
        schema_fingerprint,
        source_engine,
        source_schema,
        source_table,
        destination_uri_for_manifest(&plan.destination),
    );
    for part in &summary.manifest_parts {
        builder.record_part(
            part.part_id,
            part.path.clone(),
            part.rows,
            part.size_bytes,
            part.content_fingerprint.clone(),
        );
    }
    let manifest = builder.finalize(status);

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "{} '{}': could not create destination for manifest write (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            return;
        }
    };

    match write_manifest(&*dest, &manifest) {
        Ok(WriteOutcome::Written { success_marker }) => {
            log::info!(
                "{} '{}': manifest.json written ({} parts, {} rows){}",
                kind,
                summary.export_name,
                manifest.part_count,
                manifest.row_count,
                if success_marker { " + _SUCCESS" } else { "" },
            );
        }
        Ok(WriteOutcome::SkippedStreaming) => {
            log::info!(
                "{} '{}': manifest skipped (streaming destination)",
                kind,
                summary.export_name,
            );
        }
        Err(e) => {
            log::warn!(
                "{} '{}': manifest write failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
        }
    }
}

/// Run the manifest-aware `--validate` pass against the destination prefix
/// (ADR-0012 M5/M6, ADR-0013).  Populates `summary.manifest_verification`;
/// failures are logged and non-fatal — the existing per-file row check has
/// already set `summary.validated`, and the operator gets a richer report
/// regardless of whether destination I/O succeeded here.
///
/// Streaming destinations (stdout) have no prefix to verify; skipped silently
/// since `finalize_manifest` has already logged its own "skipped streaming"
/// note for that case.
fn finalize_validate_manifest(
    plan: &crate::plan::ResolvedRunPlan,
    summary: &mut RunSummary,
    kind: &str,
) {
    use crate::destination::WriteCommitProtocol;
    use crate::pipeline::validate_manifest::verify_at_destination;

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "{} '{}': could not create destination for --validate manifest pass (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            return;
        }
    };
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
        log::debug!(
            "{} '{}': streaming destination — skipping manifest-aware --validate",
            kind,
            summary.export_name
        );
        return;
    }

    match verify_at_destination(&*dest, "") {
        Ok(v) => {
            // Compose the file-row check (already on summary.validated) with
            // the manifest-aware verdict.  Legacy runs (M6) keep their existing
            // row-count verdict — manifest verification only DOWNgrades when
            // it has explicit failures.
            if v.has_failures()
                && let Some(false) | None = summary.validated
            {
                // already false — leave alone
            } else if v.has_failures() {
                summary.validated = Some(false);
            }
            log::info!(
                "{} '{}': --validate manifest pass: {} parts verified, {} failed{}{}",
                kind,
                summary.export_name,
                v.parts_verified,
                v.parts_failed,
                if v.success_marker_consistent {
                    " (_SUCCESS consistent)"
                } else if v.manifest_found {
                    ""
                } else {
                    " (legacy_run: no manifest)"
                },
                if v.has_failures() {
                    format!(" — {} issue(s)", v.failures.len())
                } else {
                    String::new()
                },
            );
            summary.manifest_verification = Some(v);
        }
        Err(e) => {
            log::warn!(
                "{} '{}': --validate manifest pass failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
        }
    }
}

/// ADR-0012 M8 — refuse to start a `--resume` run against a destination prefix
/// whose `_SUCCESS` marker is already present, unless the operator passed
/// `--force`.  The marker is the unambiguous signal that the prefix already
/// holds a verified dataset; quietly overwriting it is the kind of mistake
/// that costs a re-extraction window's worth of source pressure.
///
/// Streaming destinations (stdout) have no prefix to gate on; permitted.
/// I/O failures probing `_SUCCESS` (e.g. permission denied on the bucket
/// we're about to write to) bubble up as `Err` so the operator sees the
/// real problem before the run starts spending source query time.
fn check_success_gate_for_resume(plan: &crate::plan::ResolvedRunPlan) -> Result<()> {
    use crate::destination::WriteCommitProtocol;
    use crate::manifest::SUCCESS_FILENAME;

    let dest = crate::destination::create_destination(&plan.destination)?;
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
        return Ok(());
    }
    match dest.head(SUCCESS_FILENAME)? {
        Some(_) => anyhow::bail!(
            "export '{}': --resume refused — destination prefix already has _SUCCESS \
             from a prior completed run.  Re-running would overwrite a verified dataset. \
             Pass --force to override, or use a different destination prefix.",
            plan.export_name
        ),
        None => Ok(()),
    }
}

/// Best-effort textual URI for the manifest's `destination.uri` field.  The
/// manifest is a record of where data was written, so the URI must reflect
/// what an operator would type to find the prefix again.
fn destination_uri_for_manifest(cfg: &crate::config::DestinationConfig) -> String {
    use crate::config::DestinationType;
    match cfg.destination_type {
        DestinationType::Local => cfg
            .path
            .clone()
            .or_else(|| cfg.prefix.clone())
            .map(|p| format!("file://{p}"))
            .unwrap_or_else(|| "file://.".to_string()),
        DestinationType::S3 => {
            let bucket = cfg.bucket.as_deref().unwrap_or("");
            let prefix = cfg.prefix.as_deref().unwrap_or("");
            if prefix.is_empty() {
                format!("s3://{bucket}/")
            } else {
                format!("s3://{bucket}/{prefix}")
            }
        }
        DestinationType::Gcs => {
            let bucket = cfg.bucket.as_deref().unwrap_or("");
            let prefix = cfg.prefix.as_deref().unwrap_or("");
            if prefix.is_empty() {
                format!("gs://{bucket}/")
            } else {
                format!("gs://{bucket}/{prefix}")
            }
        }
        DestinationType::Stdout => "stdout".to_string(),
    }
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
    config_path: &str,
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
    finalize_manifest(plan, state, &summary, "apply");
    if plan.validate {
        finalize_validate_manifest(plan, &mut summary, "apply");
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
                bucket: None,
                prefix: None,
                path: Some("/tmp".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
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
        assert!(
            err.to_string().contains("quality checks failed"),
            "error must mention quality: {err}"
        );
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
