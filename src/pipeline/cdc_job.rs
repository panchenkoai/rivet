//! CDC export runner — the `mode: cdc` arm of `rivet run`.
//!
//! Runs a change stream configured from the YAML (not the `rivet cdc` CLI),
//! writes typed Parquet/CSV through the commit-seam sink, and records a
//! `RunSummary` + metric row so a CDC export appears in `rivet metrics` and the
//! run aggregate exactly like a batch export. Mirrors `run_export_job`'s contract
//! — `(Result<()>, RunSummary)`, metric recorded internally — so the orchestrator
//! treats a CDC export like any other.

use std::path::PathBuf;

use super::finalize::finalize_run_report;
use super::summary::RunSummary;
use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::source::cdc::sink::{SinkConfig, run_to_files};
use crate::source::cdc::{CdcConfig, create_change_stream, resolve_cdc_columns};
use crate::state::StateStore;

/// Run one `mode: cdc` export end to end, then record + report it like a batch
/// export. The metric row is written here (as `run_export_job` does); the
/// `RunSummary` is returned for the run aggregate.
pub(super) fn run_cdc_export(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
) -> (Result<()>, RunSummary) {
    let started = std::time::Instant::now();
    let run_id = format!(
        "{}_{}",
        export.name,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3f")
    );

    let result = run_cdc_inner(config, export, &run_id);
    let duration_ms = started.elapsed().as_millis() as i64;

    let summary = match &result {
        Ok((rows, files, bytes)) => cdc_summary(
            &run_id,
            export,
            "success",
            *rows,
            *files,
            *bytes,
            duration_ms,
            None,
        ),
        Err(e) => cdc_summary(
            &run_id,
            export,
            "failed",
            0,
            0,
            0,
            duration_ms,
            Some(crate::redact::redact_error(e)),
        ),
    };

    record_metric(state, config, export, &summary);
    finalize_run_report(config_path, &summary, "cdc");
    (result.map(|_| ()), summary)
}

/// The actual stream → typed files → manifest work. Returns
/// `(rows, files, bytes)` for the run record.
fn run_cdc_inner(
    config: &Config,
    export: &ExportConfig,
    run_id: &str,
) -> Result<(i64, usize, u64)> {
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.clone();
    let cdc = export.cdc.clone().unwrap_or_default();
    let table = export
        .table
        .clone()
        .ok_or_else(|| anyhow::anyhow!("export '{}': cdc mode requires `table:`", export.name))?;
    let checkpoint = cdc.checkpoint.as_ref().map(PathBuf::from);

    let cdc_cfg = CdcConfig {
        url: url.clone(),
        server_id: cdc.server_id.unwrap_or(4271),
        slot: cdc.slot.clone().unwrap_or_else(|| "rivet_slot".to_string()),
        capture_instance: cdc.capture_instance.clone(),
        checkpoint: checkpoint.clone(),
        until_current: cdc.until_current,
        tls: tls.clone(),
    };
    let mut stream = create_change_stream(&cdc_cfg)?;

    let columns = resolve_cdc_columns(&url, &table, tls.as_ref())?;
    let dest = crate::destination::create_destination(&export.destination)?;
    let dest_uri = export.destination.path.clone().unwrap_or_default();
    let engine = engine_label(&url)?;
    let now = chrono::Utc::now().to_rfc3339();

    let sink_cfg = SinkConfig {
        columns: &columns,
        dest: dest.as_ref(),
        dest_uri,
        engine,
        table: &table,
        format: export.format,
        tables: vec![table.clone()],
        checkpoint,
        max_events: cdc.max_events,
        rollover: cdc.rollover.unwrap_or(10_000),
        rollover_memory_bytes: cdc.rollover_memory_mb.map(|mb| mb * 1024 * 1024),
        started_at: now,
        run_id: run_id.to_string(),
    };
    let manifest = run_to_files(stream.as_mut(), sink_cfg)?;
    let bytes: u64 = manifest.parts.iter().map(|p| p.size_bytes).sum();
    Ok((
        manifest.row_count as i64,
        manifest.part_count as usize,
        bytes,
    ))
}

/// The source engine label for the metric row's `source_type`.
fn engine_label(url: &str) -> Result<&'static str> {
    if url.starts_with("mysql://") {
        Ok("mysql")
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok("postgres")
    } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
        Ok("mssql")
    } else {
        anyhow::bail!(
            "cdc: unsupported source url scheme (expected mysql:// / postgresql:// / sqlserver://)"
        )
    }
}

/// Build the per-run summary (mirrors `synthetic_failed_summary`'s shape, for a
/// CDC run). CDC has no plan/tuning/cursor, so those fields default.
#[allow(clippy::too_many_arguments)]
fn cdc_summary(
    run_id: &str,
    export: &ExportConfig,
    status: &str,
    total_rows: i64,
    files: usize,
    bytes: u64,
    duration_ms: i64,
    error_message: Option<String>,
) -> RunSummary {
    let journal = crate::journal::RunJournal::new(run_id, &export.name);
    RunSummary {
        run_id: run_id.to_string(),
        export_name: export.name.clone(),
        status: status.to_string(),
        total_rows,
        files_produced: files,
        bytes_written: bytes,
        files_committed: files,
        duration_ms,
        peak_rss_mb: 0,
        retries: 0,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message,
        tuning_profile: "cdc".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: export.format.label().to_string(),
        mode: "cdc".into(),
        compression: String::new(),
        destination_uri: export.destination.path.clone(),
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

/// Write the `export_metrics` row (built directly — no plan coupling, unlike the
/// batch `build_metric_row`) so a CDC run is queryable like a batch run.
fn record_metric(state: &StateStore, config: &Config, export: &ExportConfig, summary: &RunSummary) {
    let source_type = config
        .source
        .resolve_url()
        .ok()
        .and_then(|u| engine_label(&u).ok())
        .map(|s| s.to_string());
    // Only the fields a CDC run actually has; the batch-specific rest (chunk_size,
    // cursor, quality, …) stay at their MetricRow::default() (None / 0).
    let row = crate::state::MetricRow {
        export_name: summary.export_name.clone(),
        run_id: summary.run_id.clone(),
        duration_ms: summary.duration_ms,
        total_rows: summary.total_rows,
        peak_rss_mb: Some(summary.peak_rss_mb),
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        tuning_profile: Some("cdc".to_string()),
        format: Some(summary.format.clone()),
        mode: Some("cdc".to_string()),
        files_produced: summary.files_produced as i64,
        bytes_written: summary.bytes_written as i64,
        files_committed: summary.files_committed as i64,
        source_type,
        destination_type: Some(export.destination.destination_type.label().to_string()),
        rivet_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        longest_chunk_ms: summary.journal.longest_chunk_ms(),
        ..Default::default()
    };
    if let Err(e) = state.record_metric_full(&row) {
        log::warn!(
            "cdc: failed to record metric for export '{}': {:#}",
            export.name,
            e
        );
    }
}
