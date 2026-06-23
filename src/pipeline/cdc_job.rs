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
use crate::source::cdc::{CdcCapture, CdcConfig, engine_label, run_capture};
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

    let mut summary = match &result {
        Ok(manifest) => {
            let bytes: u64 = manifest.parts.iter().map(|p| p.size_bytes).sum();
            cdc_summary(
                &run_id,
                export,
                "success",
                manifest.row_count,
                manifest.part_count as usize,
                bytes,
                duration_ms,
                None,
            )
        }
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

    // Record the run in the journal (so `rivet journal` shows a CDC run like a
    // batch one): one FileWritten per committed part, then the RunCompleted outcome.
    if let Ok(manifest) = &result {
        for (i, part) in manifest.parts.iter().enumerate() {
            summary
                .journal
                .record(crate::journal::RunEvent::FileWritten {
                    file_name: part.path.clone(),
                    rows: part.rows,
                    bytes: part.size_bytes,
                    part_index: i,
                });
        }
    }
    summary
        .journal
        .record(crate::journal::RunEvent::RunCompleted {
            status: summary.status.clone(),
            error_message: summary.error_message.clone(),
            duration_ms,
        });
    if let Err(e) = state.store_journal(&summary.journal) {
        log::warn!(
            "cdc: journal persist failed for export '{}': {:#}",
            export.name,
            e
        );
    }

    record_metric(state, config, export, &summary);
    finalize_run_report(config_path, &summary, "cdc");
    (result.map(|_| ()), summary)
}

/// Build the capture from the config + export and drive it through the shared
/// [`crate::source::cdc::run_capture`] (the same assembler the `rivet cdc` CLI
/// uses). Returns the `RunManifest` so the caller records the metric + journal.
fn run_cdc_inner(
    config: &Config,
    export: &ExportConfig,
    run_id: &str,
) -> Result<crate::manifest::RunManifest> {
    let url = config.source.resolve_url()?;
    let cdc = export.cdc.clone().unwrap_or_default();
    let table = export
        .table
        .clone()
        .ok_or_else(|| anyhow::anyhow!("export '{}': cdc mode requires `table:`", export.name))?;
    let dest = crate::destination::create_destination(&export.destination)?;
    let now = chrono::Utc::now().to_rfc3339();

    run_capture(CdcCapture {
        cdc_cfg: CdcConfig {
            url,
            server_id: cdc.server_id.unwrap_or(4271),
            slot: cdc.slot.clone().unwrap_or_else(|| "rivet_slot".to_string()),
            capture_instance: cdc.capture_instance.clone(),
            checkpoint: cdc.checkpoint.as_ref().map(PathBuf::from),
            until_current: cdc.until_current,
            tls: config.source.tls.clone(),
        },
        table,
        dest: dest.as_ref(),
        dest_uri: export.destination.path.clone().unwrap_or_default(),
        format: export.format,
        max_events: cdc.max_events,
        rollover: cdc.rollover.unwrap_or(10_000),
        rollover_memory_bytes: cdc.rollover_memory_mb.map(|mb| mb * 1024 * 1024),
        run_id: run_id.to_string(),
        started_at: now,
    })
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
    // Only the fields a CDC run has; the batch-specific rest (cursor, quality,
    // chunk, reconcile, …) stay at RunSummary::default() (None / 0 / empty).
    RunSummary {
        run_id: run_id.to_string(),
        export_name: export.name.clone(),
        status: status.to_string(),
        total_rows,
        files_produced: files,
        bytes_written: bytes,
        files_committed: files,
        duration_ms,
        error_message,
        tuning_profile: "cdc".into(),
        format: export.format.label().to_string(),
        mode: "cdc".into(),
        destination_uri: export.destination.path.clone(),
        journal: crate::journal::RunJournal::new(run_id, &export.name),
        ..Default::default()
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
