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
        // One manifest per captured table (a multi-table `tables:` stream
        // produces several); the export-level summary carries the totals.
        Ok(manifests) => {
            let bytes: u64 = manifests
                .iter()
                .flat_map(|m| &m.parts)
                .map(|p| p.size_bytes)
                .sum();
            cdc_summary(
                &run_id,
                export,
                "success",
                manifests.iter().map(|m| m.row_count).sum(),
                manifests.iter().map(|m| m.part_count as usize).sum(),
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
    if let Ok(manifests) = &result {
        for (i, part) in manifests.iter().flat_map(|m| &m.parts).enumerate() {
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

/// A multi-table stream lands each table under its own sub-prefix of the
/// export's destination (`<base>/<table>/`), so every table's prefix is
/// self-describing (its own parts + `manifest.json` + `_SUCCESS`), exactly like
/// N single-table exports — minus the N−1 extra slots/connections.
fn dest_for_table(
    base: &crate::config::DestinationConfig,
    table: &str,
) -> crate::config::DestinationConfig {
    let mut d = base.clone();
    match d.destination_type {
        crate::config::DestinationType::Local => {
            let p = d.path.take().unwrap_or_else(|| ".".into());
            d.path = Some(format!("{}/{}", p.trim_end_matches('/'), table));
        }
        crate::config::DestinationType::Stdout => {}
        // Cloud destinations: extend the key prefix. Cloud prefixes are
        // LITERAL key prefixes — the destination concatenates `prefix + key`
        // with no separator (the docs' `prefix: exports/` convention) — so the
        // sub-prefix must supply both its slashes itself, or every object
        // lands as a mangled flat key (`<prefix>/<table>cdc-….parquet`).
        _ => {
            let pfx = d.prefix.take().unwrap_or_default();
            let base = pfx.trim_end_matches('/');
            d.prefix = Some(if base.is_empty() {
                format!("{table}/")
            } else {
                format!("{base}/{table}/")
            });
        }
    }
    d
}

/// Build the capture from the config + export and drive it through the shared
/// [`crate::source::cdc::run_capture`] (the same assembler the `rivet cdc` CLI
/// uses). Returns the manifests (one per captured table) so the caller records
/// the metric + journal.
fn run_cdc_inner(
    config: &Config,
    export: &ExportConfig,
    run_id: &str,
) -> Result<Vec<crate::manifest::RunManifest>> {
    let url = config.source.resolve_url()?;
    let cdc = export.cdc.clone().unwrap_or_default();
    // `tables:` (multi-table, one stream) or the single `table:` — validation
    // guarantees exactly one of them is set.
    let (tables, multi) = match (&export.tables, &export.table) {
        (Some(ts), _) => (ts.clone(), true),
        (None, Some(t)) => (vec![t.clone()], false),
        (None, None) => {
            anyhow::bail!("export '{}': cdc mode requires `table:`", export.name)
        }
    };
    // Per-table destinations must outlive the borrowed CaptureOutputs.
    let mut wired: Vec<(String, Box<dyn crate::destination::Destination>, String)> =
        Vec::with_capacity(tables.len());
    for t in &tables {
        let dcfg = if multi {
            dest_for_table(&export.destination, t)
        } else {
            export.destination.clone()
        };
        let dest = crate::destination::create_destination(&dcfg)?;
        let uri = dcfg
            .path
            .clone()
            .or_else(|| dcfg.prefix.clone())
            .unwrap_or_default();
        wired.push((t.clone(), dest, uri));
    }
    let outputs = wired
        .iter()
        .map(|(t, d, u)| crate::source::cdc::CaptureOutput {
            table: t.clone(),
            dest: d.as_ref(),
            dest_uri: u.clone(),
        })
        .collect();
    let now = chrono::Utc::now().to_rfc3339();

    run_capture(CdcCapture {
        cdc_cfg: CdcConfig {
            url,
            server_id: cdc
                .server_id
                .unwrap_or(crate::config::DEFAULT_MYSQL_SERVER_ID),
            slot: cdc
                .slot
                .clone()
                .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string()),
            capture_instance: cdc.capture_instance.clone(),
            checkpoint: cdc.checkpoint.as_ref().map(PathBuf::from),
            until_current: cdc.until_current,
            tls: config.source.tls.clone(),
        },
        outputs,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};

    // RED test for the finding: cloud prefixes are LITERAL key prefixes —
    // `cloud.rs` concatenates `prefix + key` with NO separator (hence the
    // docs' `prefix: exports/` convention). The multi-table sub-prefix must
    // therefore supply both slashes itself; without the trailing one, every
    // object of a `tables:` export lands as `<prefix>/<table>cdc-….parquet`
    // (mangled flat keys) — observed live on a real GCS bucket.
    #[test]
    fn cloud_table_sub_prefix_carries_its_own_trailing_slash() {
        let gcs = DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("b".into()),
            prefix: Some("exports".into()),
            ..Default::default()
        };
        let d = dest_for_table(&gcs, "orders");
        let prefix = d.prefix.unwrap();
        // The exact join the cloud destination performs:
        let key = format!("{prefix}{}", "cdc-r-000000.parquet");
        assert_eq!(
            key, "exports/orders/cdc-r-000000.parquet",
            "cloud keys are prefix ++ name — the sub-prefix must end with '/'"
        );

        // A trailing slash on the configured prefix must not double up.
        let gcs2 = DestinationConfig {
            prefix: Some("exports/".into()),
            ..gcs.clone()
        };
        assert_eq!(
            dest_for_table(&gcs2, "orders").prefix.unwrap(),
            "exports/orders/"
        );

        // No configured prefix: the table becomes the whole prefix.
        let gcs3 = DestinationConfig {
            prefix: None,
            ..gcs
        };
        assert_eq!(dest_for_table(&gcs3, "orders").prefix.unwrap(), "orders/");
    }

    #[test]
    fn local_table_sub_path_is_a_plain_directory_join() {
        let local = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/data/cdc/".into()),
            ..Default::default()
        };
        assert_eq!(
            dest_for_table(&local, "orders").path.unwrap(),
            "/data/cdc/orders",
            "local paths go through the filesystem join — no trailing slash needed"
        );
    }
}
