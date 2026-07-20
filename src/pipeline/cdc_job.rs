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
use crate::source::cdc::{CdcCapture, CdcConfig, CdcEngine, CdcEngineOpts, DrainMode, run_capture};
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

/// `cdc.initial: snapshot` — the anchor-then-snapshot half, run by the
/// orchestrator BEFORE the CDC drain. Returns the synthesized `mode: full`
/// exports still pending (their `snapshot/_SUCCESS` marker absent), after
/// ensuring the anchor exists. Anchor-BEFORE-snapshot is the whole point: a
/// change landing mid-snapshot is then also in the stream — an overlap the
/// PK+`__op` dedupe absorbs, never a gap.
pub(super) fn initial_snapshot_pending(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
) -> Result<Vec<ExportConfig>> {
    let cdc = export.cdc.clone().unwrap_or_default();
    if cdc.initial != Some(crate::config::CdcInitialMode::Snapshot) {
        return Ok(Vec::new());
    }
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();

    let slot = cdc
        .slot
        .clone()
        .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string());

    // Each table's snapshot destination + whether its snapshot is already done.
    // Scanned BEFORE the anchor step, because a completed snapshot is resume
    // EVIDENCE: a missing server-side anchor after one must fail loud, never
    // silently re-anchor at "current" (which would skip every change since the
    // drop while reporting success — finding #28).
    let (tables, multi) = match (&export.tables, &export.table) {
        (Some(ts), _) => (ts.clone(), true),
        (None, Some(t)) => (vec![t.clone()], false),
        (None, None) => anyhow::bail!("export '{}': cdc mode requires `table:`", export.name),
    };
    let mut table_dests = Vec::with_capacity(tables.len());
    let mut done_flags = Vec::with_capacity(tables.len());
    for t in &tables {
        let table_dcfg = if multi {
            dest_for_table(&export.destination, t)
        } else {
            export.destination.clone()
        };
        let snap_dcfg = dest_for_table(&table_dcfg, "snapshot");
        let dest = crate::destination::create_destination(&snap_dcfg)?;
        // The state DB is authoritative (survives `cleanup_source` wiping the
        // bucket); the GCS `snapshot/_SUCCESS` marker stays a legacy co-signal so
        // pre-v14 runs and setups without state still skip correctly.
        let done = state.snapshot_done(&export.name, t)? || dest.head("_SUCCESS")?.is_some();
        table_dests.push((t.clone(), snap_dcfg));
        done_flags.push(done);
    }

    // The pure decision: which tables still need a snapshot, and whether prior
    // evidence forces the fail-loud anchor guard.
    let ckpt_resume = cdc
        .checkpoint
        .as_deref()
        .map(std::path::Path::new)
        .and_then(|p| crate::source::cdc::Position::load(p).ok().flatten())
        .is_some();
    let (pending_idx, resume_expected) = snapshot_plan(&done_flags, ckpt_resume);

    // The anchor — one entry point; the engine's AnchorModel decides the
    // mechanism (idempotent: a present anchor is never moved).
    CdcEngine::from_url(&url)?.ensure_anchor(
        &url,
        &slot,
        cdc.checkpoint.as_deref().map(std::path::Path::new),
        tls,
        resume_expected,
    )?;

    let mut pending = Vec::new();
    for idx in pending_idx {
        let (t, snap_dcfg) = &table_dests[idx];
        let mut synth = export.clone();
        synth.name = format!("{}__snapshot_{t}", export.name);
        synth.mode = crate::config::ExportMode::Full;
        synth.table = Some(t.clone());
        synth.tables = None;
        synth.cdc = None;
        synth.destination = snap_dcfg.clone();
        // NEVER inherit skip_empty: an EMPTY table with skip_empty=true would
        // write no snapshot/_SUCCESS, so the marker check re-snapshots on
        // every run forever. An empty snapshot must still complete (manifest +
        // _SUCCESS with 0 rows) for the handoff to converge.
        synth.skip_empty = false;
        pending.push(synth);
    }
    Ok(pending)
}

/// The pure `initial: snapshot` decision, split out of the I/O in
/// [`initial_snapshot_pending`] so it can be unit-tested. Given, per table in
/// order, whether its snapshot is already `done` (state DB OR the legacy GCS
/// marker) and whether a checkpoint position survives (`ckpt_resume`), returns
/// the indices still PENDING a snapshot and whether the anchor step must treat a
/// missing server-side anchor as resume evidence.
///
/// A `done` snapshot is never re-run — the state DB remembers it even after
/// `cleanup_source` wiped the bucket marker. `resume_expected` is `true` when
/// ANY prior evidence exists — a live checkpoint OR any done snapshot — so a
/// lost server-side anchor fails LOUD instead of silently re-anchoring at
/// "current" (finding #28).
fn snapshot_plan(done_flags: &[bool], ckpt_resume: bool) -> (Vec<usize>, bool) {
    let pending = done_flags
        .iter()
        .enumerate()
        .filter_map(|(i, &done)| (!done).then_some(i))
        .collect();
    let resume_expected = ckpt_resume || done_flags.iter().any(|&d| d);
    (pending, resume_expected)
}

/// A multi-table stream lands each table under its own sub-prefix of the
/// export's destination (`<base>/<table>/`), so every table's prefix is
/// self-describing (its own parts + `manifest.json` + `_SUCCESS`), exactly like
/// N single-table exports — minus the N−1 extra slots/connections.
pub(crate) fn dest_for_table(
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
        // Finding #44, early check: refuse BEFORE the first part lands if the
        // prefix belongs to the other pipeline shape (config error, fail the
        // run cleanly — the write-seam guard stays as the backstop).
        crate::manifest::guard_manifest_mode(dest.as_ref(), "cdc")?;
        let uri = dcfg
            .path
            .clone()
            .or_else(|| dcfg.prefix.clone())
            .unwrap_or_default();
        wired.push((t.clone(), dest, uri));
    }
    // `columns:` type overrides, narrowed per table: bare keys apply to every
    // captured table; qualified keys ("table.column") only to theirs, winning
    // over bare — so one table's override can never bleed into a same-named
    // column elsewhere.
    let all_overrides =
        crate::plan::build::parse_column_overrides_pub(&export.columns, &export.name)?;
    let outputs = wired
        .iter()
        .map(|(t, d, u)| crate::source::cdc::CaptureOutput {
            table: t.clone(),
            dest: d.as_ref(),
            dest_uri: u.clone(),
            overrides: crate::types::overrides_for_table(
                &all_overrides,
                t.rsplit('.').next().unwrap_or(t),
            ),
        })
        .collect();
    let now = chrono::Utc::now().to_rfc3339();

    // `until_current` defaults to `true` (bounded, scheduler-friendly). An explicit
    // `false` opts into a long-lived continuous stream — surface it so it is a
    // deliberate choice, never a silent never-terminating run.
    if !cdc.until_current {
        log::warn!(
            "cdc: `until_current: false` runs a CONTINUOUS stream (a long-lived daemon that never \
             exits on its own). For the scheduler model, omit it or set `until_current: true` (the \
             default) to drain to the log end and exit."
        );
    }

    run_capture(CdcCapture {
        cdc_cfg: CdcConfig {
            url,
            checkpoint: cdc.checkpoint.as_ref().map(PathBuf::from),
            drain: DrainMode::from_until_current(cdc.until_current),
            tls: config.source.tls.clone(),
            engine: match config.source.source_type {
                crate::config::SourceType::Mysql => CdcEngineOpts::Mysql {
                    server_id: cdc
                        .server_id
                        .unwrap_or(crate::config::DEFAULT_MYSQL_SERVER_ID),
                },
                crate::config::SourceType::Postgres => CdcEngineOpts::Postgres {
                    slot: cdc
                        .slot
                        .clone()
                        .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string()),
                },
                crate::config::SourceType::Mssql => CdcEngineOpts::Mssql {
                    capture_instance: cdc.capture_instance.clone(),
                },
                crate::config::SourceType::Mongo => {
                    CdcEngineOpts::Mongo {
                        canonical: config.source.mongo.as_ref().is_some_and(|m| {
                            matches!(m.json, crate::config::MongoJsonMode::Canonical)
                        }),
                    }
                }
            },
        },
        outputs,
        format: export.format,
        max_events: cdc.max_events,
        rollover: cdc.rollover.unwrap_or(100_000),
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
        cursor_column: None,
        cursor_low: None,
        cursor_high: None,
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
        .and_then(|u| CdcEngine::from_url(&u).ok().map(CdcEngine::label))
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

    // ── snapshot_plan: the pure `initial: snapshot` decision ─────────────────

    #[test]
    fn snapshot_plan_first_run_snapshots_all_with_no_resume_evidence() {
        // Nothing done, no checkpoint → snapshot every table, and this is a
        // genuine first anchor (resume_expected=false).
        assert_eq!(snapshot_plan(&[false, false], false), (vec![0, 1], false));
    }

    #[test]
    fn snapshot_plan_all_done_snapshots_nothing_but_keeps_resume_evidence() {
        // Every snapshot already done — the state DB remembers even after
        // `cleanup_source` wiped the bucket marker → re-snapshot NOTHING; and
        // that prior evidence forces the fail-loud anchor guard (#28).
        assert_eq!(
            snapshot_plan(&[true, true], false),
            (Vec::<usize>::new(), true)
        );
    }

    #[test]
    fn snapshot_plan_partial_snapshots_only_the_undone() {
        // One table done, one not → snapshot only the undone; a done sibling is
        // still resume evidence.
        assert_eq!(snapshot_plan(&[true, false], false), (vec![1], true));
    }

    #[test]
    fn snapshot_plan_checkpoint_alone_is_resume_evidence() {
        // No snapshot done but a live checkpoint survives → still snapshot (the
        // marker is gone), yet the checkpoint alone makes a lost anchor fail loud.
        assert_eq!(snapshot_plan(&[false], true), (vec![0], true));
    }

    #[test]
    fn snapshot_plan_no_evidence_is_a_legitimate_first_anchor() {
        // Nothing done, no checkpoint → snapshot, and no evidence means the
        // anchor is a legitimate first anchor, not a loud failure.
        assert_eq!(snapshot_plan(&[false], false), (vec![0], false));
    }
}
