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

    // Resolve `{date}`/`{export}`/… ONCE, here, before anything reads a prefix.
    //
    // The batch path gets this for free and that is exactly why the gap was
    // invisible: every batch runner writes to `plan.destination`, and
    // `plan::build` expands the templates while building it
    // (`expand_destination_templates`). CDC returns from `job.rs` BEFORE
    // `build_plan` ever runs, so it had no plan and no expansion — the drain
    // called `create_destination` on the RAW config and created a directory
    // named, literally, `{date}`. Meanwhile `rivet validate` resolves the same
    // template to today's date (`validate_cmd.rs`, whose comment asserts this
    // is "the same shape `rivet run` resolves at write time" — true for batch,
    // false here) and reported an empty destination over a stream that had
    // captured everything correctly.
    //
    // `for_today` is deliberately the SAME constructor the reader uses. The
    // load-bearing property is not which date lands in the path — it is that
    // the writer and the reader compute it the same way.
    let expanded_export = {
        let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&export.name);
        let mut e = export.clone();
        e.destination = crate::destination::placeholder::expand_destination(e.destination, &ctx);
        e
    };
    let export = &expanded_export;

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
    // NO bucket-side running marker here — deliberately, and the gap is REAL:
    // a cross-host `gc_orphans` (whose `StateStore::open` creates a fresh empty
    // DB beside the load config) still sees a live stream's unmanifested parts
    // as crash debris. A marker was written here and REVERTED: the sink names
    // its per-roll durability manifest with the SAME
    // `run_unique_manifest_name(run_id)` at the SAME prefix, so the first
    // commit-boundary roll physically overwrote the marker — the protection
    // lasted exactly one rollover (proven live: after a `rollover: 1` run the
    // prefix held only the sink's `success` manifest under the marker's own
    // name). Multi-table is worse: the marker sits at the base prefix while the
    // terminal manifests land in per-table sub-prefixes, so it is superseded by
    // its OWN run and swept, and the sub-prefixes never get a marker at all.
    //
    // Closing this needs a marker key the sink cannot collide with, a
    // supersession rule that exempts a run from retiring its own marker, and a
    // marker per destination prefix. That is a design change, not a call site —
    // tracked as an open finding rather than shipped half-working, because a
    // marker that self-destructs is worse than none: it retires the known issue
    // while leaving the hole open.

    let read_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let result = run_cdc_inner(config, export, &run_id, state, &read_bytes);
    let duration_ms = started.elapsed().as_millis() as i64;

    // The manifests describe what was made DURABLE; `outcome` says whether the
    // run finished. They are independent, and the totals come from the
    // manifests on BOTH paths.
    //
    // This used to hard-code 0 / 0 / 0 on the error path. The CDC drain rolls
    // (flush → per-roll manifest → checkpoint → ack) many times per run, so a
    // run that captured and ACKED N rows and then failed reported "failed,
    // 0 rows, 0 files" while the object store held the parts and the source was
    // advanced past them. Nothing was lost — but an operator reading that
    // concludes nothing was captured, and re-runs expecting to recapture
    // changes the log no longer has. Any tool summing metric rows under-counted
    // by the same amount.
    let (manifests, outcome) = result;
    let bytes_read = read_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let bytes: u64 = manifests
        .iter()
        .flat_map(|m| &m.parts)
        .map(|p| p.size_bytes)
        .sum();
    let mut summary = cdc_summary(
        &run_id,
        export,
        if outcome.is_ok() { "success" } else { "failed" },
        manifests.iter().map(|m| m.row_count).sum(),
        manifests.iter().map(|m| m.part_count as usize).sum(),
        bytes,
        bytes_read,
        duration_ms,
        outcome.as_ref().err().map(crate::redact::redact_error),
    );

    // Transition the ledger to the CDC run's terminal status (mirrors the batch
    // path). A crash before here leaves the row `running`; the next CDC run
    // supersedes it. Best-effort.
    if let Err(e) = state.finish_run(&run_id, &summary.status, &chrono::Utc::now().to_rfc3339()) {
        log::warn!("cdc: run-status finish failed for '{}': {e:#}", export.name);
    }

    // Record the run in the journal (so `rivet journal` shows a CDC run like a
    // batch one): one FileWritten per committed part, then the RunCompleted outcome.
    // Unconditionally: a part that is durable happened, whatever the run's
    // verdict. Gating this on `Ok` left a failed run with no FileWritten events
    // at all, so `rivet journal` showed nothing to recover from — the record of
    // the debris is exactly what makes it discoverable.
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
    (outcome, summary)
}

/// The anchor half of a first CDC run, run by the orchestrator BEFORE the
/// drain. Returns the synthesized `mode: full` exports still pending (their
/// `snapshot/_SUCCESS` marker absent), after ensuring the anchor exists.
///
/// `snapshot` anchors, then snapshots every table. Anchor-BEFORE-snapshot is the
/// whole point: a change landing mid-snapshot is then also in the change stream,
/// an overlap the PK+`__op` dedupe absorbs, never a gap.
pub(super) fn initial_snapshot_pending(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
) -> Result<Vec<ExportConfig>> {
    let cdc = export.cdc.clone().unwrap_or_default();
    // `snapshot` is the only OSS `initial:` mode; absence means "capture changes
    // only", which needs no anchor step and no snapshot legs.
    if cdc.initial.is_none() {
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
    // The relation the SNAPSHOT must read, when the engine can name it from a
    // catalog rather than from the configured string. On SQL Server the capture
    // instance settles it outright, and the two readings disagreed in silence: the
    // baseline was `SELECT … FROM <configured>` resolved in the connection's DEFAULT
    // schema, so a same-named decoy's rows were deposited in the captured table's
    // own prefix (MEASURED: `{"id":900,"amount":42,"dbo_only":7}` for a relation
    // holding neither that id nor those columns, `status: success`). The LABEL stays
    // the configured string — both legs share one prefix and the snapshot marker is
    // keyed by it — while the READ follows the catalog.
    let catalog_read: Option<String> = match (CdcEngine::from_url(&url)?, &cdc.capture_instance) {
        (CdcEngine::Mssql, Some(ci)) => {
            let Some((schema, table)) =
                crate::source::mssql::cdc::source_object_of_capture_instance(&url, ci, tls)?
            else {
                // The catalog has no such capture instance. `open` bails on this
                // too — but the SNAPSHOT leg runs, writes, and MARKS ITSELF DONE
                // before `open` is ever called, so deferring to it means a typo'd
                // instance deposits a baseline read from whatever the configured
                // name resolves to and then never re-reads (round-4, DEMONSTRATED:
                // correcting the typo left the fabricated row in place and the real
                // rows never backfilled). Refuse where it is still cheap.
                anyhow::bail!(
                    "sqlserver cdc: export '{}' names capture instance '{ci}', which \
                     `cdc.change_tables` does not know. Enable CDC on the table \
                     (`sys.sp_cdc_enable_table`) or fix the name — continuing would \
                     snapshot whatever `{}` resolves to in the connection's default \
                     schema and record that as this table's baseline.",
                    export.name,
                    tables.first().map(String::as_str).unwrap_or("the table"),
                );
            };
            // And the configured name must AGREE with the catalog, by the same
            // predicate the sink routes by. `open` checks this; the snapshot leg had
            // no routing check at all, so a `table:` contradicting its capture
            // instance backfilled one relation into the other's prefix and only then
            // failed the drain.
            for t in &tables {
                if !crate::source::cdc::sink::table_matches(CdcEngine::Mssql, t, &schema, &table) {
                    anyhow::bail!(
                        "sqlserver cdc: export '{}' configures `table: {t}` while capture \
                         instance '{ci}' emits changes for `{schema}.{table}` — the two \
                         name different relations, so the snapshot would back up one and \
                         the drain capture the other. Set `table:` to `{schema}.{table}`, \
                         or point `capture_instance:` at the table you meant.",
                        export.name,
                    );
                }
            }
            Some(format!("{schema}.{table}"))
        }
        _ => None,
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
        table_dests.push((
            t.clone(),
            catalog_read.clone().unwrap_or_else(|| t.clone()),
            snap_dcfg,
        ));
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

    let mut pending = Vec::new();
    for idx in pending_idx {
        let (label, read, snap_dcfg) = &table_dests[idx];
        pending.push(synth_snapshot_export(export, label, read, snap_dcfg));
    }
    Ok(pending)
}

/// Synthesize the `mode: full` snapshot export for one CDC table — the batch
/// leg run BEFORE the CDC drain. Pure (no I/O) so the schema-consistency
/// invariants below are unit-testable.
/// Test-only door onto [`synth_snapshot_export`], so the load guard's family
/// tests can derive the leg's identity from the code that BUILDS it rather than
/// hand-typing the expected family — the fixture bypass that made the earlier
/// version of that test green against a product which still mis-folded.
#[cfg(test)]
/// Test door onto [`synth_snapshot_export`], taking BOTH strings.
///
/// It took only the one until round-4, and passed it twice — so no unit test could
/// express LABEL ≠ READ, which is the entire distinction the SQL Server catalog
/// resolution introduced. That is how a broken snapshot-done key shipped: the
/// mechanism's activation threshold is two DIFFERENT strings, and every fixture had
/// one.
pub(crate) fn synth_snapshot_export_for_test(
    export: &ExportConfig,
    table: &str,
    read: &str,
) -> ExportConfig {
    synth_snapshot_export(export, table, read, &export.destination)
}

fn synth_snapshot_export(
    export: &ExportConfig,
    table: &str,
    read: &str,
    snap_dcfg: &crate::config::DestinationConfig,
) -> ExportConfig {
    let mut synth = export.clone();
    // The leg's family is the PARENT export, recorded here — the one place that
    // knows, because it is building the name from it.
    synth.snapshot_parent = Some(export.name.clone());
    // The label travels WITH the leg, so every consumer that must agree with the
    // configured string can ask for it instead of re-deriving it.
    synth.snapshot_label = Some(table.to_string());
    synth.name = format!(
        "{}{}{table}",
        export.name,
        crate::manifest::SNAPSHOT_LEG_INFIX
    );
    synth.mode = crate::config::ExportMode::Full;
    // The LABEL names the leg (and through it the prefix and the snapshot marker);
    // the READ names the relation. They differ only where a catalog knows better
    // than the configured string.
    synth.table = Some(read.to_string());
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
/// Returns what the drain made DURABLE paired with its outcome — never one
/// without the other. See `sink::run_to_files`.
fn run_cdc_inner(
    config: &Config,
    export: &ExportConfig,
    run_id: &str,
    state: &StateStore,
    read_bytes: &std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> (Vec<crate::manifest::RunManifest>, Result<()>) {
    let url = match config.source.resolve_url() {
        Ok(u) => u,
        Err(e) => return (Vec::new(), Err(e)),
    };
    let cdc = export.cdc.clone().unwrap_or_default();
    // `tables:` (multi-table, one stream) or the single `table:` — validation
    // guarantees exactly one of them is set.
    let (tables, multi) = match (&export.tables, &export.table) {
        (Some(ts), _) => (ts.clone(), true),
        (None, Some(t)) => (vec![t.clone()], false),
        (None, None) => {
            return (
                Vec::new(),
                Err(anyhow::anyhow!(
                    "export '{}': cdc mode requires `table:`",
                    export.name
                )),
            );
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
        let dest = match crate::destination::create_destination(&dcfg) {
            Ok(d) => d,
            Err(e) => return (Vec::new(), Err(e)),
        };
        // Finding #44, early check: refuse BEFORE the first part lands if the
        // prefix belongs to the other pipeline shape (config error, fail the
        // run cleanly — the write-seam guard stays as the backstop).
        if let Err(e) = crate::manifest::guard_manifest_mode(dest.as_ref(), "cdc") {
            return (Vec::new(), Err(e));
        }
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
        match crate::plan::build::parse_column_overrides_pub(&export.columns, &export.name) {
            Ok(v) => v,
            Err(e) => return (Vec::new(), Err(e)),
        };
    let outputs = wired
        .iter()
        .map(|(t, d, u)| crate::source::cdc::CaptureOutput {
            table: t.clone(),
            dest: d.as_ref(),
            dest_uri: u.clone(),
            overrides: crate::types::overrides_for_unit(&all_overrides, Some(t)),
            row_hash: export.meta_columns.row_hash.clone(),
        })
        .collect();
    let now = chrono::Utc::now().to_rfc3339();

    // `until_current` defaults to `true` (bounded, scheduler-friendly). An explicit
    // `false` opts into a long-lived continuous stream — surface it so it is a
    // deliberate choice, never a silent never-terminating run.
    if !cdc.until_current {
        // Engine-specific, because the promise is: MySQL blocks on the binlog and
        // genuinely stays up, while the POLL adapters (PostgreSQL, SQL Server)
        // only lose their open-time ceiling and still exit on catch-up — the
        // `DrainMode` doc's "an outer loop re-wraps them" describes a loop that
        // does not exist. Telling a PG operator the run "never exits on its own"
        // sent them away from the supervisor they actually need.
        let shape = match config.source.source_type {
            crate::config::SourceType::Mysql => "blocks on the binlog and stays up until stopped",
            crate::config::SourceType::Mongo => {
                "blocks on the change stream awaiting events and stays up until stopped \
                 (it ends only if the stream is invalidated or closed)"
            }
            _ => {
                concat!(
                    "removes the open-time ceiling but STILL EXITS ON CATCH-UP — on ",
                    "this engine it is one unbounded pass, not a daemon; run it under ",
                    "a supervisor that restarts it, or use the scheduler model instead",
                )
            }
        };
        log::warn!(
            concat!(
                "cdc: `until_current: false` opts into the continuous model, which on {:?} {}. ",
                "For the scheduler model, omit it or set `until_current: true` (the default) ",
                "to drain to the log end and exit.",
            ),
            config.source.source_type,
            shape
        );
    }

    run_capture(
        CdcCapture {
            export_name: export.name.clone(),
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
                        // The same strings the sink will route by — see the field's doc.
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                    crate::config::SourceType::Postgres => CdcEngineOpts::Postgres {
                        slot: cdc
                            .slot
                            .clone()
                            .unwrap_or_else(|| crate::config::DEFAULT_PG_SLOT.to_string()),
                        // The same strings the sink will route by — see the field's doc.
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                    crate::config::SourceType::Mssql => CdcEngineOpts::Mssql {
                        capture_instance: cdc.capture_instance.clone(),
                        // The same strings the sink will route by — see the field's doc.
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                    crate::config::SourceType::Mongo => CdcEngineOpts::Mongo {
                        canonical: config.source.mongo.as_ref().is_some_and(|m| {
                            matches!(m.json, crate::config::MongoJsonMode::Canonical)
                        }),
                        configured_tables: wired.iter().map(|(t, _, _)| t.clone()).collect(),
                    },
                },
            },
            outputs,
            format: export.format,
            max_events: cdc.max_events,
            rollover: cdc.rollover.unwrap_or(100_000),
            rollover_memory_bytes: cdc.rollover_memory_mb.map(|mb| mb * 1024 * 1024),
            run_id: run_id.to_string(),
            started_at: now,
            state: Some(state),
        },
        read_bytes,
    )
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
    bytes_read: u64,
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
        bytes_read,
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
/// The CDC `export_metrics` row, extracted PURE so its field mapping (notably
/// bytes_read — #196) is unit-tested without a live state store / Config.
fn cdc_metric_row(
    summary: &RunSummary,
    source_type: Option<String>,
    destination_type: Option<String>,
) -> crate::state::MetricRow {
    // Only the fields a CDC run actually has; the batch-specific rest (chunk_size,
    // cursor, quality, …) stay at their MetricRow::default() (None / 0).
    crate::state::MetricRow {
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
        bytes_read: summary.bytes_read as i64,
        files_committed: summary.files_committed as i64,
        source_type,
        destination_type,
        rivet_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        longest_chunk_ms: summary.journal.longest_chunk_ms(),
        ..Default::default()
    }
}

fn record_metric(state: &StateStore, config: &Config, export: &ExportConfig, summary: &RunSummary) {
    let source_type = config
        .source
        .resolve_url()
        .ok()
        .and_then(|u| CdcEngine::from_url(&u).ok().map(CdcEngine::label))
        .map(|s| s.to_string());
    let dest_type = Some(export.destination.destination_type.label().to_string());
    let row = cdc_metric_row(summary, source_type, dest_type);
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
    /// #196: cdc_summary must carry bytes_read into the RunSummary (the CDC read
    /// leg). RED against deleting the field (defaults to 0).
    #[test]
    fn cdc_summary_carries_bytes_read() {
        let export = crate::config::sample_export("t");
        let s = super::cdc_summary("r1", &export, "success", 100, 2, 5_000, 12_345, 50, None);
        assert_eq!(s.bytes_read, 12_345, "read leg must reach the summary");
        assert_eq!(s.bytes_written, 5_000);
    }

    /// #196: the CDC metrics row must carry bytes_read from the summary. RED
    /// against deleting the field in cdc_metric_row (defaults to 0).
    #[test]
    fn cdc_metric_row_carries_bytes_read() {
        let export = crate::config::sample_export("t");
        let summary = super::cdc_summary("r1", &export, "success", 100, 2, 5_000, 98_765, 50, None);
        let row = super::cdc_metric_row(&summary, Some("mysql".into()), Some("local".into()));
        assert_eq!(
            row.bytes_read, 98_765,
            "read leg must reach the export_metrics row"
        );
        assert_eq!(row.bytes_written, 5_000);
        // The engine labels are threaded straight through — pin them so the
        // metric row is attributable (source/dest engine per CDC run), not just
        // the byte counts. RED against deleting either field in cdc_metric_row.
        assert_eq!(
            row.source_type.as_deref(),
            Some("mysql"),
            "source engine must reach the export_metrics row"
        );
        assert_eq!(
            row.destination_type.as_deref(),
            Some("local"),
            "destination engine must reach the export_metrics row"
        );
    }

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
        let synth = synth_snapshot_export(&e, "orders", "orders", &dcfg);
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

    /// `adopt` (anchor an already-loaded table, move no rows) is a paid-tier
    /// mode (the one-way seam, ADR-0026 — docs may name the crate, this tree may
    /// not), so the OSS config must REJECT it rather than quietly accept a value
    /// this binary cannot honour. 0.24.0 shipped it in the OSS enum, which put it
    /// in the published JSON schema and the generated reference — irreversible
    /// once the crate is published, and indistinguishable to a user from a
    /// supported feature. Omitted `initial:` stays its own third state (capture
    /// changes only, no anchor step), which is what an OSS operator uses for an
    /// already-loaded table.
    #[test]
    fn oss_rejects_the_pro_only_initial_adopt() {
        let parse = |line: &str| {
            crate::config::Config::from_yaml(&format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/t\"\n\
                 exports:\n  - name: a\n    table: t\n    mode: cdc\n    format: parquet\n\
                 \x20   destination:\n      type: gcs\n      bucket: b\n      prefix: p/\n\
                 \x20   cdc:\n      checkpoint: /tmp/ck\n{line}"
            ))
        };
        let err = format!(
            "{:#}",
            parse("      initial: adopt\n").expect_err("`adopt` is not an OSS mode")
        );
        assert!(
            err.contains("adopt") || err.contains("initial"),
            "the refusal must point at the offending value; got: {err}"
        );
        assert_eq!(
            parse("      initial: snapshot\n").unwrap().exports[0]
                .cdc
                .clone()
                .unwrap()
                .initial,
            Some(crate::config::CdcInitialMode::Snapshot)
        );
        assert_eq!(
            parse("").unwrap().exports[0].cdc.clone().unwrap().initial,
            None,
            "omitted is its own third state — capture changes only, no anchor step"
        );
    }

    // The mirror image of the test above, and the reason the two must be read
    // together: the SAME argument (both legs write one `__changes` log, so their
    // columns must match) clears `exported_at` and REQUIRES keeping `row_hash`.
    // The batch leg alone can produce the stamp, so inheriting it desynchronizes
    // the legs; BOTH legs produce `_rivet_row_hash`, so dropping it on the
    // snapshot is what would desynchronize them — leaving every backfilled
    // row's hash NULL and the audit's cheap path unusable.
    /// LABEL and READ are different strings, and each consumer must get the right
    /// one.
    ///
    /// Round-4: once the snapshot leg learned to READ the relation SQL Server's
    /// capture instance names, `synth.table` stopped being the configured string —
    /// and `mark_snapshot_done` kept writing `synth.table` while `snapshot_plan`
    /// asked with the configured one. The key is byte-exact, so the row could never
    /// answer its own question and every scheduled cycle re-snapshotted the whole
    /// table under a green run.
    ///
    /// The fixture needs TWO DIFFERENT strings; the test door passed one twice
    /// until now, which is why nothing here could see it.
    #[test]
    fn the_snapshot_leg_carries_the_label_and_reads_the_catalog_relation() {
        let e = crate::config::sample_export("orders");
        let synth = synth_snapshot_export_for_test(&e, "orders", "sales.orders");
        assert_eq!(
            synth.table.as_deref(),
            Some("sales.orders"),
            "the relation READ follows the catalog — that is the whole point of the \
             resolution"
        );
        assert_eq!(
            synth.snapshot_label.as_deref(),
            Some("orders"),
            "and the LABEL travels with the leg, because everything a later run keys \
             on — the snapshot-done row above all — asks with the configured string"
        );
        assert!(
            synth.name.contains("orders") && !synth.name.contains("sales.orders"),
            "the leg's NAME is built from the label too, so the destination \
             sub-prefix and the manifest identity do not move when a catalog \
             resolution changes: {}",
            synth.name
        );
    }

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
        let synth = synth_snapshot_export(&e, "orders", "orders", &dcfg);
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
