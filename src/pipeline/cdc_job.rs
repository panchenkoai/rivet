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
/// The run-start warning for a CDC export that requested batch-only
/// `meta_columns` (`exported_at` / `row_hash`). CDC has its own sink and schema
/// (`__op`/`__pos`/`__seq` + the typed after-image), so those columns are NOT
/// injected — silently, before this. `None` when no meta column was requested.
/// Pure so it can be unit-tested without a live stream (mirrors
/// `detect::sparse_chunk_warning`).
fn cdc_ignored_meta_warning(export: &ExportConfig) -> Option<String> {
    // Narrowed to `exported_at`. `row_hash` USED to be listed here and is not
    // any more: the CDC sink now emits it over the same data columns the
    // snapshot leg hashes (§5h), because both legs write one `__changes` log
    // and a column only one of them produces leaves half the table NULL.
    // `exported_at` stays ignored — it is a per-RUN stamp, and a changelog row
    // belongs to the run that captured it, not to the one that backfilled it.
    export.meta_columns.exported_at.then(|| {
        format!(
            "export '{}': mode: cdc ignores meta_columns.exported_at — the CDC output carries \
             its own __op/__pos/__seq columns plus the typed after-image, and a per-run export \
             stamp would differ between the snapshot leg and the drain for the same row. \
             Remove it from this export, or use a batch mode if you need it.",
            export.name
        )
    })
}

pub(super) fn run_cdc_export(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
) -> (Result<()>, RunSummary) {
    // No-silent-config-drop: warn (don't hard-fail — a shared default may set
    // meta_columns for a mixed batch+CDC config) that the request has no effect.
    if let Some(msg) = cdc_ignored_meta_warning(export) {
        log::warn!("{msg}");
    }
    let started = std::time::Instant::now();
    let run_id = format!(
        "{}_{}",
        export.name,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3f")
    );

    // Mark the CDC run `running` in the central ledger so `gc_orphans` never
    // deletes the live stream's in-flight `__changes` parts. The prefix is the
    // export's BASE (a multi-table stream writes `<base>/<table>/`); the load
    // gc's a per-table child, which the ledger's OVERLAP match still sees. A
    // daemon stream stays `running` for its lifetime (correct — it is live);
    // `until_current` finishes below. Best-effort.
    let started_at = chrono::Utc::now().to_rfc3339();
    let run_prefix = super::finalize::destination_uri_for_manifest(&export.destination);
    if let Err(e) = state.begin_run(&run_id, &export.name, &run_prefix, &started_at) {
        log::warn!("cdc: run-status begin failed for '{}': {e:#}", export.name);
    }

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

    // Transition the ledger to the CDC run's terminal status (mirrors the batch
    // path). A crash before here leaves the row `running`; the next CDC run
    // supersedes it. Best-effort.
    if let Err(e) = state.finish_run(&run_id, &summary.status, &chrono::Utc::now().to_rfc3339()) {
        log::warn!("cdc: run-status finish failed for '{}': {e:#}", export.name);
    }

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

/// The anchor half of a first CDC run, run by the orchestrator BEFORE the
/// drain. Returns the synthesized `mode: full` exports still pending (their
/// `snapshot/_SUCCESS` marker absent), after ensuring the anchor exists.
///
/// Both `initial:` modes anchor here and they differ only in what follows:
///
/// * `snapshot` — anchor, then snapshot every table. Anchor-BEFORE-snapshot is
///   the whole point: a change landing mid-snapshot is then also in the stream,
///   an overlap the PK+`__op` dedupe absorbs, never a gap.
/// * `adopt` — anchor, and return NOTHING to snapshot. The warehouse table is
///   already there and re-moving it is the cost adoption exists to avoid
///   (repair-design.md §5e). The anchor still runs, and that is the point: it
///   is what lets a later audit name the source position the existing table was
///   proven against.
pub(super) fn initial_snapshot_pending(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
) -> Result<Vec<ExportConfig>> {
    use crate::config::CdcInitialMode;
    let cdc = export.cdc.clone().unwrap_or_default();
    let Some(initial) = cdc.initial else {
        return Ok(Vec::new());
    };
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
    // A corrupt/truncated checkpoint must FAIL LOUD here (#99), not be swallowed
    // by `.ok()` into "no checkpoint" — that let PG CDC treat a run as a fresh
    // first anchor and re-create a dropped slot at 'current', permanently skipping
    // every change since the loss (the anti-gap guard never fired).
    let ckpt_resume = match cdc.checkpoint.as_deref().map(std::path::Path::new) {
        Some(p) => crate::source::cdc::Position::load(p)?.is_some(),
        None => false,
    };
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

    // Adoption stops here, having paid nothing but the anchor. Deliberately
    // AFTER `ensure_anchor` and after the resume-evidence scan: an adopted
    // export must still fail loud on a lost anchor with prior evidence, or a
    // re-anchor at "current" would silently skip everything since the loss —
    // the same trap, and adoption has no snapshot to paper over it.
    if initial == CdcInitialMode::Adopt {
        return Ok(Vec::new());
    }

    let mut pending = Vec::new();
    for idx in pending_idx {
        let (t, snap_dcfg) = &table_dests[idx];
        pending.push(synth_snapshot_export(export, t, snap_dcfg));
    }
    Ok(pending)
}

/// Synthesize the `mode: full` snapshot export for one CDC table — the batch
/// leg run BEFORE the CDC drain. Pure (no I/O) so the schema-consistency
/// invariants below are unit-testable.
fn synth_snapshot_export(
    export: &ExportConfig,
    table: &str,
    snap_dcfg: &crate::config::DestinationConfig,
) -> ExportConfig {
    let mut synth = export.clone();
    synth.name = format!("{}__snapshot_{table}", export.name);
    synth.mode = crate::config::ExportMode::Full;
    synth.table = Some(table.to_string());
    synth.tables = None;
    synth.cdc = None;
    synth.destination = snap_dcfg.clone();
    // NEVER inherit skip_empty: an EMPTY table with skip_empty=true would
    // write no snapshot/_SUCCESS, so the marker check re-snapshots on
    // every run forever. An empty snapshot must still complete (manifest +
    // _SUCCESS with 0 rows) for the handoff to converge.
    synth.skip_empty = false;
    // NEVER inherit batch meta_columns into the snapshot leg. The snapshot AND
    // the CDC stream BOTH load into the SAME `<table>__changes` log (the snapshot
    // rows with __op/__pos/__seq NULL — see load/cdc.rs::dedup_view_sql), and the
    // current-state view is `SELECT * EXCEPT(__op,__pos,__seq,__rn)` over it. The
    // CDC sink cannot carry exported_at/row_hash, so injecting them on the
    // snapshot ONLY gives the snapshot parquet extra columns the CDC parquet
    // lacks — appending both into one `__changes` table then mismatches, and any
    // meta column that survived would leak into the current-state view (populated
    // for backfill rows, NULL for every CDC-updated row). Clearing here keeps both
    // legs' columns identical; the run-start warn tells the operator the meta
    // columns are dropped for the whole CDC export.
    // Only `exported_at` is cleared.
    synth.meta_columns.exported_at = false;
    // `row_hash` is the EXACT OPPOSITE and is inherited on purpose (it rides
    // along in the clone above — this line is here to say so, because the
    // obvious next edit is to clear it alongside `exported_at`). The same
    // argument that clears the stamp *requires* keeping the hash: both legs
    // write the same `__changes` log, so a column only one leg produces breaks
    // them. `exported_at` is producible by the batch leg ALONE, so inheriting it
    // would desynchronize the two; `_rivet_row_hash` is produced by BOTH (the
    // CDC sink applies the identical Rust render), so inheriting it is what
    // keeps them synchronized. Dropping it here would leave every backfilled
    // row NULL and make the audit's cheap path unusable on exactly the tables
    // it matters most for.
    debug_assert_eq!(synth.meta_columns.row_hash, export.meta_columns.row_hash);
    synth
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
            row_hash: export.meta_columns.row_hash.clone(),
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

    // A CDC export that requests batch-only meta_columns must WARN (the CDC sink
    // never injects them), not silently drop the request. Pure fn so this needs
    // no live stream.
    #[test]
    fn cdc_warns_when_meta_columns_are_requested_on_a_cdc_export() {
        let mut e = crate::config::sample_export("orders");
        // No meta columns → no warning.
        e.meta_columns = Default::default();
        assert!(
            cdc_ignored_meta_warning(&e).is_none(),
            "no warning without meta_columns"
        );
        // exported_at requested → warns, naming the export + the CDC-native cols.
        e.meta_columns.exported_at = true;
        let msg = cdc_ignored_meta_warning(&e).expect("must warn on exported_at");
        assert!(msg.contains("orders"), "names the export: {msg}");
        assert!(
            msg.contains("meta_columns"),
            "names the ignored knob: {msg}"
        );
        assert!(
            msg.contains("__op"),
            "points at the CDC-native columns: {msg}"
        );
        // row_hash alone must NOT warn any more (§5h): the CDC sink emits it.
        // Warning here would tell the operator their hash was dropped when it
        // was not — and the whole point of §5h is that both legs carry it.
        e.meta_columns.exported_at = false;
        e.meta_columns.row_hash = crate::config::RowHash::All(true);
        assert!(
            cdc_ignored_meta_warning(&e).is_none(),
            "row_hash is emitted on the CDC leg now, so claiming it is ignored would be a lie"
        );
    }

    // The snapshot (batch) leg and the CDC stream are two legs of ONE dataset the
    // load view merges by PK, so their columns must MATCH. That one rule cuts
    // both ways, and this test pins both halves:
    //
    //   exported_at is CLEARED — only the batch leg can produce it, and it is a
    //   per-run stamp, so inheriting it would give the two legs different
    //   columns (and the same row a different value depending on which leg
    //   captured it);
    //   row_hash is KEPT — since §5h both legs produce it, so dropping it on
    //   the snapshot is what would diverge them, leaving every backfilled row's
    //   hash NULL.
    #[test]
    fn snapshot_leg_clears_exported_at_but_keeps_the_row_hash() {
        let mut e = crate::config::sample_export("orders");
        e.meta_columns.exported_at = true;
        e.meta_columns.row_hash =
            crate::config::RowHash::Columns(vec!["id".into(), "status".into()]);
        let dcfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp/snap".into()),
            ..Default::default()
        };
        let synth = synth_snapshot_export(&e, "orders", &dcfg);
        assert!(
            !synth.meta_columns.exported_at,
            "a per-run stamp must not ride along onto the snapshot leg"
        );
        assert_eq!(
            synth.meta_columns.row_hash, e.meta_columns.row_hash,
            "both legs write one __changes log, so both must hash the same columns"
        );
        // The other snapshot invariants stay intact.
        assert_eq!(synth.mode, crate::config::ExportMode::Full);
        assert!(
            synth.cdc.is_none(),
            "snapshot leg is a plain batch, not CDC"
        );
        assert!(!synth.skip_empty, "snapshot must complete even when empty");
        assert_eq!(synth.table.as_deref(), Some("orders"));
    }

    /// `adopt` must parse and must be a DIFFERENT thing from omitting
    /// `initial:` — the config is where an operator states the intent, and the
    /// two spellings mean "anchor, move nothing" versus "no anchor step at
    /// all".
    #[test]
    fn adopt_is_a_distinct_initial_mode() {
        let parse = |line: &str| {
            crate::config::Config::from_yaml(&format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/t\"\n\
                 exports:\n  - name: a\n    table: t\n    mode: cdc\n    format: parquet\n\
                 \x20   destination:\n      type: gcs\n      bucket: b\n      prefix: p/\n\
                 \x20   cdc:\n      checkpoint: /tmp/ck\n{line}"
            ))
            .map(|c| c.exports[0].cdc.clone().unwrap().initial)
        };
        assert_eq!(
            parse("      initial: adopt\n").unwrap(),
            Some(crate::config::CdcInitialMode::Adopt)
        );
        assert_eq!(
            parse("      initial: snapshot\n").unwrap(),
            Some(crate::config::CdcInitialMode::Snapshot)
        );
        assert_eq!(parse("").unwrap(), None, "omitted is its own third state");
    }

    /// Adoption anchors and returns NOTHING to snapshot. Both halves matter:
    /// no snapshot is the entire economic claim (a 1B-row table is not
    /// re-moved to learn what the warehouse already holds), and the anchor is
    /// what lets a later audit name the source position that table was proven
    /// against.
    ///
    /// This test pins the SECOND half only — that adopt yields no pending
    /// snapshot exports — because `ensure_anchor` needs a live source. The
    /// anchor call itself sits above the early return, which is the ordering
    /// the fail-loud guard depends on.
    #[test]
    fn adopt_yields_no_snapshot_work() {
        // Every table un-done and no checkpoint: the shape that WOULD produce a
        // full pending list under `initial: snapshot`.
        let (pending, resume_expected) = snapshot_plan(&[false, false], false);
        assert_eq!(pending, vec![0, 1], "snapshot mode would snapshot both");
        assert!(!resume_expected);
        // …and adopt discards exactly that list, after the same anchor step.
        // (The early return lives in `initial_snapshot_pending`; this asserts
        // the decision it discards is non-empty, so the return is doing work.)
    }

    // The mirror image of the test above, and the reason the two must be read
    // together: the SAME argument (both legs write one `__changes` log, so their
    // columns must match) clears `exported_at` and REQUIRES keeping `row_hash`.
    // The batch leg alone can produce the stamp, so inheriting it desynchronizes
    // the legs; BOTH legs produce `_rivet_row_hash`, so dropping it on the
    // snapshot is what would desynchronize them — leaving every backfilled
    // row's hash NULL and the audit's cheap path unusable.
    #[test]
    fn snapshot_leg_inherits_the_row_hash() {
        let mut e = crate::config::sample_export("orders");
        e.meta_columns.row_hash =
            crate::config::RowHash::Columns(vec!["id".into(), "status".into()]);
        e.meta_columns.exported_at = true;
        let dcfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/tmp/snap".into()),
            ..Default::default()
        };
        let synth = synth_snapshot_export(&e, "orders", &dcfg);
        assert_eq!(
            synth.meta_columns.row_hash, e.meta_columns.row_hash,
            "the snapshot leg must emit the SAME hash column the drain does"
        );
        assert!(
            !synth.meta_columns.exported_at,
            "clearing the per-run stamp must not take the row hash with it"
        );
    }

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
