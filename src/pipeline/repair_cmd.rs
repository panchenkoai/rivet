//! `rivet repair` — targeted re-export of reconcile mismatches (Epic H).
//!
//! ## Flow
//!
//! 1. Build a `ReconcileReport` (either freshly, or load a previous JSON via
//!    `--report`).
//! 2. Derive a [`RepairPlan`](crate::plan::RepairPlan) — the set of chunk
//!    ranges that need re-export.
//! 3. Without `--execute` (default): emit the plan and exit.
//! 4. With `--execute`: re-run just those chunks via
//!    [`chunked::run_chunked_sequential`] using a `Precomputed` chunk source.
//!    Output files are written with collision-proof naming — they are new
//!    files alongside the originals; Rivet does not delete or overwrite the
//!    old files.
//!
//! ## Closing the trust loop
//!
//! A re-export that lands a fresh file but leaves the recorded state stale
//! breaks the operator's trust loop: `reconcile` still recounts the source
//! against the *old* `chunk_task.rows_written` and reports the same mismatch
//! (the loop never converges), and `rivet validate` lists the prefix and flags
//! the un-recorded repair file as an `untracked_object`. So after a successful
//! per-chunk re-export this path:
//!
//! 1. updates that chunk's `chunk_task` row — `rows_written` is set to the
//!    freshly-exported count and the task is re-marked `completed` — so the
//!    next `reconcile` compares the live source count against the repaired
//!    count and converges to a match; and
//! 2. appends the repair-written part(s) to the destination `manifest.json`
//!    (read → append → rewrite) so the new file is tracked and `validate` no
//!    longer reports it as untracked. The originals are left in place and in
//!    the manifest (repair is additive, ADR-0009 RR5 / ADR-0012).
//!
//! Progression semantics (ADR-0008): repair does **not** advance
//! `last_committed_*` — the committed boundary already covers the chunk index.
//! The reconcile the operator runs (or that the repaired state now passes)
//! advances `last_verified_*`.

use std::collections::HashMap;
use std::path::Path;

use crate::config::Config;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, ManifestPart, ManifestStatus, PartStatus, RunManifest};
use crate::plan::{
    ExtractionStrategy, ReconcileReport, RepairAction, RepairOutcome, RepairPlan, RepairReport,
    ResolvedRunPlan, build_plan,
};
use crate::source;
use crate::state::StateStore;

use super::RunSummary;
use super::chunked::{ChunkSource, run_chunked_sequential};
use super::reconcile_cmd;

/// Output format for the repair plan / report.
pub enum RepairOutputFormat {
    /// Human-readable summary to stdout.
    Pretty,
    /// Pretty-printed JSON to the given path (or stdout if `None`).
    Json(Option<String>),
}

/// Source of the reconcile report used to derive the repair plan.
pub enum RepairReportSource {
    /// Read a reconcile report JSON from disk.
    File(String),
    /// Run reconcile in-process against the latest chunk run.
    Auto,
}

pub fn run_repair_command(
    config_path: &str,
    export_name: &str,
    params: Option<&HashMap<String, String>>,
    report_source: RepairReportSource,
    execute: bool,
    format: RepairOutputFormat,
) -> Result<()> {
    let config = Config::load_with_params(config_path, params)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let export = config
        .exports
        .iter()
        .find(|e| e.name == export_name)
        .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", export_name))?;

    let mut plan = build_plan(&config, export, config_dir, false, false, false, params)?;
    if !matches!(plan.strategy, ExtractionStrategy::Chunked(_)) {
        anyhow::bail!(
            "repair: '{}' mode — only chunked exports are supported in v1 (Epic H)",
            plan.strategy.mode_label()
        );
    }

    let state_path = config_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    let reconcile_report = load_or_build_reconcile(&plan, &state, report_source)?;
    let repair_plan = RepairPlan::from_reconcile(&reconcile_report);

    if !execute {
        emit_plan(&repair_plan, &format)?;
        return Ok(());
    }

    if repair_plan.is_empty() {
        println!(
            "repair: nothing to repair for '{}' (reconcile report is clean)",
            export_name
        );
        return Ok(());
    }

    let report = execute_repair(&mut plan, &state, repair_plan)?;
    emit_report(&report, &format)?;
    Ok(())
}

fn load_or_build_reconcile(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    source: RepairReportSource,
) -> Result<ReconcileReport> {
    match source {
        RepairReportSource::File(path) => {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("cannot read reconcile report '{}': {}", path, e))?;
            let r: ReconcileReport = serde_json::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("invalid reconcile report '{}': {}", path, e))?;
            if r.export_name != plan.export_name {
                anyhow::bail!(
                    "repair: reconcile report is for export '{}' but config targets '{}'",
                    r.export_name,
                    plan.export_name
                );
            }
            Ok(r)
        }
        RepairReportSource::Auto => reconcile_cmd::reconcile_chunked_fresh(plan, state),
    }
}

fn execute_repair(
    plan: &mut ResolvedRunPlan,
    state: &StateStore,
    repair_plan: RepairPlan,
) -> Result<RepairReport> {
    let mut results: Vec<(RepairAction, RepairOutcome)> =
        Vec::with_capacity(repair_plan.actions.len());

    // The chunk run whose `chunk_task` rows reconcile reads. Repair re-exports
    // against the latest run for this export — the same run reconcile counted.
    // Without it we can re-export the data but cannot point the recorded state
    // at the fresh count, so `reconcile → repair → reconcile` could never
    // converge (audit finding #7).
    let run_id = state
        .get_latest_chunk_run(&plan.export_name)?
        .map(|(rid, _, _, _)| rid);

    // One summary across the whole repair (matches the original single
    // `RunSummary::new`): `record_part` appends every freshly-written part to
    // `summary.manifest_parts`. We snapshot its length and `total_rows` around
    // each single-chunk re-export to attribute the exact rows and the exact
    // new file(s) to that chunk — no even-split lie.
    let mut src = source::create_source(&plan.source)?;
    let mut summary = RunSummary::new(plan);

    // One destination handle for the whole repair: used to rename each
    // repair-written part so its filename carries the ORIGINAL chunk index
    // (audit L15). The single-chunk `Precomputed` source restarts enumeration
    // at 0, so the writer always names the file `..._chunk0_...`; without this
    // the file repairing chunk 2 would land as `chunk0`, no longer reflecting
    // the logical chunk it repairs. Built once here (re-`create_destination`d
    // again only in the manifest-rewrite step, which runs at most once).
    let dest = crate::destination::create_destination(&plan.destination)?;

    // Repair-written parts to record in the destination manifest, in order.
    let mut new_parts: Vec<ManifestPart> = Vec::new();

    for a in &repair_plan.actions {
        let (start, end) = match (a.start_key.parse::<i64>(), a.end_key.parse::<i64>()) {
            (Ok(s), Ok(e)) => (s, e),
            _ => {
                results.push((
                    a.clone(),
                    RepairOutcome::Skipped {
                        reason: format!("unparseable chunk keys [{}..{}]", a.start_key, a.end_key),
                    },
                ));
                continue;
            }
        };

        let rows_before = summary.total_rows;
        let parts_before = summary.manifest_parts.len();
        let outcome = run_chunked_sequential(
            &mut *src,
            plan,
            &mut summary,
            Some(state),
            ChunkSource::Precomputed(vec![(start, end)]),
        );
        match outcome {
            Ok(()) => {
                let rows = summary.total_rows - rows_before;
                // Every part `record_part` appended for this single chunk — its
                // path, rows, bytes, fingerprint, md5. One chunk yields one part
                // unless max_file_size rotated it; either way these are exactly
                // the new files the manifest must learn about.
                let mut chunk_parts: Vec<ManifestPart> =
                    summary.manifest_parts[parts_before..].to_vec();

                // L15: the writer named each part `..._chunk0_...` (the
                // single-chunk `Precomputed` source enumerates from 0), but this
                // part repairs the logical chunk `a.chunk_index`. Rename the file
                // and rewrite the recorded `path` so the name carries the real
                // index — both `complete_chunk_task` (file_name) and the manifest
                // append below then reference the corrected name. Best-effort
                // (ADR-0012 M9 `move` semantics): the bytes are already durable,
                // so a failed rename keeps the original name and warns rather than
                // failing the repair. A no-op when the chunk index is already 0.
                for p in &mut chunk_parts {
                    if let Some(renamed) = relabel_repair_chunk_index(&p.path, a.chunk_index) {
                        match dest.r#move(&p.path, &renamed) {
                            Ok(()) => p.path = renamed,
                            Err(e) => log::warn!(
                                "repair: chunk {} re-exported but could not rename \
                                 '{}' → '{}' to carry the original chunk index \
                                 (the file is durable under its chunk0 name): {:#}",
                                a.chunk_index,
                                p.path,
                                renamed,
                                e
                            ),
                        }
                    }
                }

                // (1) Close finding #7: point `chunk_task.rows_written` at the
                //     freshly-exported count (and re-mark the task completed,
                //     clearing any stale error) so the next reconcile compares
                //     the live source count against the repaired count. The
                //     `file_name` records the newest part for this chunk; if the
                //     chunk rotated into several parts the latest is recorded
                //     (reconcile keys on rows_written, not file_name).
                if let Some(rid) = &run_id {
                    let file_name = chunk_parts.last().map(|p| p.path.as_str());
                    if let Err(e) = state.complete_chunk_task(rid, a.chunk_index, rows, file_name) {
                        // Non-fatal to the data (the file is durable) but fatal
                        // to trust — surface it loudly rather than report a
                        // false "executed" that leaves reconcile stuck.
                        log::warn!(
                            "repair: chunk {} re-exported but chunk_task update failed — \
                             reconcile will still report the old mismatch: {:#}",
                            a.chunk_index,
                            e
                        );
                    }
                } else {
                    log::warn!(
                        "repair: chunk {} re-exported but no chunk run is recorded for export \
                         '{}' — chunk_task could not be updated; reconcile will not converge",
                        a.chunk_index,
                        plan.export_name
                    );
                }

                new_parts.extend(chunk_parts);
                results.push((a.clone(), RepairOutcome::Executed { rows_written: rows }));
            }
            Err(e) => {
                let msg = crate::redact::redact_error(&e);
                results.push((a.clone(), RepairOutcome::Failed { error: msg }));
            }
        }
    }

    // (2) Close finding #8: record the repair-written parts in the destination
    //     manifest so `rivet validate` no longer flags them as untracked. Best
    //     effort and warn-on-fail (ADR-0001 I7 / ADR-0012): the parts are
    //     already durable at the destination, so a manifest-rewrite failure
    //     must not change the repair's exit code — but it is logged loudly so
    //     the operator knows validate may still flag the files.
    if !new_parts.is_empty()
        && let Err(e) = record_repair_parts_in_manifest(plan, &new_parts)
    {
        log::warn!(
            "repair: re-exported parts were written but the destination manifest could not be \
             updated (the files are durable; `rivet validate` may flag them as untracked): {:#}",
            e
        );
    }

    Ok(RepairReport::new(
        repair_plan,
        format!("repair-{}", chrono::Utc::now().format("%Y%m%dT%H%M%S")),
        results,
    ))
}

/// Read the destination `manifest.json`, append the repair-written parts as new
/// committed entries (fresh unique `part_id`s, recomputed `row_count` /
/// `part_count`), and rewrite it. The originals stay recorded — repair is
/// additive — so this closes the "untracked repair file" gap (finding #8)
/// without dropping the manifest's history of the prior parts.
///
/// Returns `Err` if no manifest exists at the prefix (a repair against a prefix
/// that was never finalized has nothing to amend) or if the read/write fails;
/// the caller logs and continues since the data itself is already durable.
fn record_repair_parts_in_manifest(
    plan: &ResolvedRunPlan,
    new_parts: &[ManifestPart],
) -> Result<()> {
    let dest = crate::destination::create_destination(&plan.destination)?;

    // Manifests live at the prefix root (manifest_dir == "" for the local/path
    // and bucket-prefix destinations repair supports); parts are recorded with
    // prefix-relative paths, which is exactly what `record_part` stored.
    let raw = match dest.head(MANIFEST_FILENAME)? {
        Some(_) => crate::pipeline::validate_manifest::read_capped(
            &*dest,
            MANIFEST_FILENAME,
            crate::pipeline::validate_manifest::MANIFEST_MAX_BYTES,
        )?,
        None => anyhow::bail!(
            "no manifest.json at the destination prefix — cannot record repair parts \
             (was the original export finalized?)"
        ),
    };
    let mut manifest: RunManifest = serde_json::from_slice(&raw)
        .map_err(|e| anyhow::anyhow!("destination manifest.json is unparseable: {e}"))?;

    // Unique, monotonic part_ids (ADR-0012 M4): max existing + 1, incrementing.
    let mut next_id = manifest.parts.iter().map(|p| p.part_id).max().unwrap_or(0) + 1;
    for p in new_parts {
        manifest.parts.push(ManifestPart {
            part_id: next_id,
            path: p.path.clone(),
            rows: p.rows,
            size_bytes: p.size_bytes,
            content_fingerprint: p.content_fingerprint.clone(),
            content_md5: p.content_md5.clone(),
            status: PartStatus::Committed,
        });
        next_id += 1;
    }

    // Keep the manifest self-consistent (validate's step 2 checks this): the
    // declared aggregates must match the committed parts after the append.
    manifest.row_count = manifest.committed_rows();
    manifest.part_count = manifest.committed_part_count() as u32;
    manifest.finished_at = chrono::Utc::now().to_rfc3339();

    // Reuse the standard writer so atomicity / streaming-skip rules stay in one
    // place. A repaired dataset is not a fresh clean run, so do NOT re-stamp
    // _SUCCESS here — preserve whatever terminal status the manifest carried
    // (the writer emits _SUCCESS only for `Success`, which the original clean
    // run already established).
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    let _ = ManifestStatus::Success; // (status unchanged; documented above)
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &bytes)?;
    dest.write(tmp.path(), MANIFEST_FILENAME)?;
    Ok(())
}

/// Rewrite a repair-written part filename so it carries the ORIGINAL chunk
/// index (L15). The single-chunk `Precomputed` source the repair runner uses
/// enumerates from 0, so the writer always emits `..._chunk0_<nonce>.<ext>`
/// (or a rotated `..._chunk0_<nonce>_p<n>.<ext>`); this replaces that `_chunk0_`
/// token with `_chunk{original_chunk_index}_` so the name reflects the logical
/// chunk it repairs.
///
/// Returns `None` when there is nothing to do: the chunk index is already 0
/// (the name is already correct), or the path carries no `_chunk0_` token
/// (defensive — an unexpected name shape is left untouched rather than mangled).
///
/// Targets the **rightmost** `_chunk0_`: everything after the chunk token is a
/// 16-hex nonce, an optional `_p<n>` rotation suffix, and the extension — none
/// of which can contain `_chunk0_`, so the rightmost match is the chunk token.
fn relabel_repair_chunk_index(path: &str, original_chunk_index: i64) -> Option<String> {
    if original_chunk_index == 0 {
        return None;
    }
    let token = "_chunk0_";
    let at = path.rfind(token)?;
    Some(format!(
        "{}_chunk{}_{}",
        &path[..at],
        original_chunk_index,
        &path[at + token.len()..],
    ))
}

fn emit_plan(plan: &RepairPlan, format: &RepairOutputFormat) -> Result<()> {
    match format {
        RepairOutputFormat::Pretty => print_plan_pretty(plan),
        RepairOutputFormat::Json(None) => println!("{}", plan.to_json_pretty()?),
        RepairOutputFormat::Json(Some(path)) => {
            std::fs::write(path, plan.to_json_pretty()?)
                .map_err(|e| anyhow::anyhow!("cannot write repair plan '{}': {}", path, e))?;
            println!("Repair plan written to: {}", path);
        }
    }
    Ok(())
}

fn emit_report(report: &RepairReport, format: &RepairOutputFormat) -> Result<()> {
    match format {
        RepairOutputFormat::Pretty => print_report_pretty(report),
        RepairOutputFormat::Json(None) => println!("{}", report.to_json_pretty()?),
        RepairOutputFormat::Json(Some(path)) => {
            std::fs::write(path, report.to_json_pretty()?)
                .map_err(|e| anyhow::anyhow!("cannot write repair report '{}': {}", path, e))?;
            println!("Repair report written to: {}", path);
        }
    }
    Ok(())
}

fn print_plan_pretty(plan: &RepairPlan) {
    println!();
    println!("  Export            : {}", plan.export_name);
    println!("  Reconcile run     : {}", plan.reconcile_run_id);
    println!("  Actions           : {}", plan.actions.len());
    for a in &plan.actions {
        println!(
            "    • chunk {} [{}..{}] — {}",
            a.chunk_index, a.start_key, a.end_key, a.reason
        );
    }
    if !plan.skipped.is_empty() {
        println!("  Skipped           :");
        for s in &plan.skipped {
            println!("    • {s}");
        }
    }
    if plan.is_empty() && plan.skipped.is_empty() {
        println!("  (nothing to repair)");
    }
    println!();
}

fn print_report_pretty(report: &RepairReport) {
    println!();
    println!("  Export       : {}", report.plan.export_name);
    println!("  Repair run   : {}", report.repair_run_id);
    println!(
        "  Summary      : planned {} · executed {} · skipped {} · failed {} · rows {}",
        report.summary.planned,
        report.summary.executed,
        report.summary.skipped,
        report.summary.failed,
        report.summary.rows_written,
    );
    for (a, out) in &report.results {
        let tag = match out {
            RepairOutcome::Executed { rows_written } => format!("executed ({rows_written} rows)"),
            RepairOutcome::Skipped { reason } => format!("skipped ({reason})"),
            RepairOutcome::Failed { error } => format!("failed ({error})"),
        };
        println!(
            "    • chunk {} [{}..{}] — {tag}",
            a.chunk_index, a.start_key, a.end_key
        );
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{PartitionKind, PartitionResult, ReconcileReport};

    #[test]
    fn plan_from_auto_would_derive_actions_from_reconcile() {
        // Smoke-test the public derivation path without hitting the DB.
        let partitions = vec![
            PartitionResult::classify(
                PartitionKind::Chunk,
                "chunk 0 [1..100]".into(),
                Some(100),
                Some(100),
            ),
            PartitionResult::classify(
                PartitionKind::Chunk,
                "chunk 1 [101..200]".into(),
                Some(100),
                Some(90),
            ),
        ];
        let r = ReconcileReport::new(
            "orders".into(),
            "rec-1".into(),
            "chunked".into(),
            partitions,
        );
        let plan = RepairPlan::from_reconcile(&r);
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(plan.actions[0].chunk_index, 1);
    }

    // ── L15: repair-written filename carries the ORIGINAL chunk index ─────────

    #[test]
    fn relabel_repair_chunk_index_rewrites_chunk0_to_original() {
        // The writer always emits `_chunk0_` for a single-chunk Precomputed
        // source; repairing logical chunk 2 must rename it to `_chunk2_`.
        let written = "orders_20260611_120000_chunk0_a1b2c3d4e5f6a7b8.parquet";
        let renamed = relabel_repair_chunk_index(written, 2)
            .expect("a non-zero chunk index must produce a renamed path");
        assert_eq!(
            renamed,
            "orders_20260611_120000_chunk2_a1b2c3d4e5f6a7b8.parquet"
        );
        assert!(!renamed.contains("_chunk0_"), "no chunk0 token survives");
    }

    #[test]
    fn relabel_repair_chunk_index_handles_rotated_part_suffix() {
        // A max_file_size rotation suffixes `_p<n>` after the nonce; the chunk
        // token still rewrites and the rotation suffix is preserved.
        let written = "orders_20260611_120000_chunk0_a1b2c3d4e5f6a7b8_p1.parquet";
        let renamed = relabel_repair_chunk_index(written, 3).unwrap();
        assert_eq!(
            renamed,
            "orders_20260611_120000_chunk3_a1b2c3d4e5f6a7b8_p1.parquet"
        );
    }

    #[test]
    fn relabel_repair_chunk_index_is_noop_for_chunk_zero() {
        // Chunk 0's name is already correct — nothing to rename, so no move.
        let written = "orders_20260611_120000_chunk0_a1b2c3d4e5f6a7b8.parquet";
        assert!(relabel_repair_chunk_index(written, 0).is_none());
    }

    #[test]
    fn relabel_repair_chunk_index_leaves_unexpected_shapes_untouched() {
        // A name without the chunk0 token (e.g. an unexpected writer shape) is
        // left alone rather than mangled.
        assert!(relabel_repair_chunk_index("orders_no_chunk_token.parquet", 5).is_none());
    }
}
