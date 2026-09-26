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
//!    longer reports it as untracked, and marks the chunk's original part(s)
//!    `superseded` so a manifest reader sees each row once. The original FILES
//!    stay on disk until opt-in `gc_orphans` (ADR-0009 RR5); when the mapping of
//!    an original to its chunk is ambiguous the chunk stays additive, with a warning.
//!
//! Progression semantics (ADR-0008): repair does **not** advance
//! `last_committed_*` — the committed boundary already covers the chunk index.
//! The reconcile the operator runs (or that the repaired state now passes)
//! advances `last_verified_*`.

use std::collections::HashMap;
use std::path::Path;

use crate::config::Config;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, ManifestPart, PartStatus, RunManifest};
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

    // Each executed chunk with the repair-written parts that replace it, in order.
    let mut repaired: Vec<(i64, Vec<ManifestPart>)> = Vec::new();

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

                repaired.push((a.chunk_index, chunk_parts));
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
    if !repaired.is_empty()
        && let Err(e) = record_repair_parts_in_manifest(
            &plan.destination,
            &repaired,
            run_id.as_deref(),
            &summary.ledger.integrity.column_checksums,
            summary.ledger.integrity.checksum_key_column.as_deref(),
        )
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

/// Read the destination `manifest.json`, mark each repaired chunk's original parts
/// `Superseded`, append the repair-written parts as committed, and rewrite it.
///
/// Returns `Err` if no manifest exists at the prefix (a repair against a prefix
/// that was never finalized has nothing to amend) or if the read/write fails;
/// the caller logs and continues since the data itself is already durable.
fn record_repair_parts_in_manifest(
    destination: &crate::config::DestinationConfig,
    repaired: &[(i64, Vec<ManifestPart>)],
    chunk_run_id: Option<&str>,
    repair_checksums: &std::collections::BTreeMap<String, u64>,
    repair_key: Option<&str>,
) -> Result<()> {
    let dest = crate::destination::create_destination(destination)?;

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

    let superseded = superseded_parts(&manifest, repaired, chunk_run_id);
    let removed = superseded_checksums(&*dest, &manifest, &superseded);
    for &i in &superseded {
        manifest.parts[i].status = PartStatus::Superseded;
    }

    // Unique, monotonic part_ids (ADR-0012 M4): max existing + 1, incrementing.
    let mut next_id = manifest.parts.iter().map(|p| p.part_id).max().unwrap_or(0) + 1;
    for p in repaired.iter().flat_map(|(_, parts)| parts) {
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
    fold_repair_checksums(
        &mut manifest,
        repair_checksums,
        repair_key,
        removed.as_ref(),
    );

    // Route through the shared writer so the canonical `manifest.json`, the
    // immutable run-unique `manifest-<run_id>.json` copy, and the `_SUCCESS`
    // fingerprint all update TOGETHER. The old hand-rolled write touched ONLY
    // the canonical file, leaving the run-unique sidecar stale — and `rivet load`
    // is manifest-authoritative and reads the run-unique copies preferentially
    // (`list_manifest_keys`), so it saw the PRE-repair part list and SILENTLY
    // dropped the repaired parts with every count/gate green. `write_manifest`
    // re-emits `_SUCCESS` only for a `Success` manifest, so a repaired clean run
    // keeps its marker (re-fingerprinted to the new bytes) and a repaired failed
    // run stays marker-less — the terminal status is preserved.
    crate::pipeline::manifest_writer::write_manifest(&*dest, &manifest)?;
    Ok(())
}

/// Indexes of the committed manifest parts the repaired chunks replace; an ambiguous mapping supersedes nothing for that chunk.
fn superseded_parts(
    manifest: &RunManifest,
    repaired: &[(i64, Vec<ManifestPart>)],
    chunk_run_id: Option<&str>,
) -> Vec<usize> {
    let additive = |why: &str| {
        log::warn!(
            "repair: {why} — keeping the original part(s) declared beside the repair part \
             (additive); a manifest reader will see the chunk's unchanged rows twice"
        )
    };
    if chunk_run_id != Some(manifest.run_id.as_str()) {
        additive(&format!(
            "the destination manifest is run '{}', not the repaired chunk run {chunk_run_id:?}",
            manifest.run_id
        ));
        return Vec::new();
    }
    let own_prefix = format!("{}_", manifest.export_name);
    let chunk_of = |path: &str| -> Option<i64> {
        let base = path.rsplit('/').next().unwrap_or(path);
        base.strip_prefix(&own_prefix)?;
        super::chunked::chunk_index_of(base)?.parse().ok()
    };
    let committed = || {
        manifest
            .parts
            .iter()
            .enumerate()
            .filter(|(_, p)| p.status == PartStatus::Committed)
    };
    if let Some((_, p)) = committed().find(|(_, p)| chunk_of(&p.path).is_none()) {
        additive(&format!(
            "committed part '{}' carries no chunk index of export '{}'",
            p.path, manifest.export_name
        ));
        return Vec::new();
    }
    let mut chunks = std::collections::BTreeSet::new();
    for (chunk, parts) in repaired {
        match parts.iter().find(|p| chunk_of(&p.path) != Some(*chunk)) {
            Some(p) => additive(&format!(
                "repair part '{}' is not named for chunk {chunk}",
                p.path
            )),
            None => {
                chunks.insert(*chunk);
            }
        }
    }
    committed()
        .filter(|(_, p)| chunk_of(&p.path).is_some_and(|c| chunks.contains(&c)))
        .map(|(i, _)| i)
        .collect()
}

/// The superseded parts' Form-B contribution, re-read with the manifest's own render and key; `None` when it cannot be recomputed.
fn superseded_checksums(
    dest: &dyn crate::destination::Destination,
    manifest: &RunManifest,
    superseded: &[usize],
) -> Option<std::collections::BTreeMap<String, u64>> {
    use std::io::Write;

    if superseded.is_empty() || manifest.column_checksums.is_none() {
        return Some(Default::default());
    }
    let mut tmps = Vec::with_capacity(superseded.len());
    for &i in superseded {
        let path = &manifest.parts[i].path;
        let written = dest.read(path).and_then(|body| {
            let mut tmp = tempfile::NamedTempFile::new()?;
            tmp.write_all(&body)?;
            tmp.flush()?;
            Ok(tmp)
        });
        match written {
            Ok(tmp) => tmps.push(tmp),
            Err(e) => {
                log::warn!("repair: cannot re-read superseded part '{path}': {e:#}");
                return None;
            }
        }
    }
    let paths: Vec<std::path::PathBuf> = tmps.iter().map(|t| t.path().to_path_buf()).collect();
    let fold =
        crate::source::value_checksum::Fold::from_render_id(manifest.checksum_render.as_deref());
    match crate::source::value_checksum::reread_column_checksums(
        &paths,
        manifest.checksum_key_column.as_deref(),
        fold,
    ) {
        Ok(Ok(sums)) => Some(sums),
        Ok(Err(detail)) => {
            log::warn!("repair: {detail}");
            None
        }
        Err(e) => {
            log::warn!("repair: cannot re-read the superseded parts: {e:#}");
            None
        }
    }
}

/// Fold the repair parts' Form-B checksums into the manifest, or drop the record when it cannot cover them truthfully.
fn fold_repair_checksums(
    manifest: &mut RunManifest,
    repair: &std::collections::BTreeMap<String, u64>,
    repair_key: Option<&str>,
    superseded: Option<&std::collections::BTreeMap<String, u64>>,
) {
    let Some(recorded) = manifest.column_checksums.as_mut() else {
        return;
    };
    let foldable = manifest.checksum_render.as_deref()
        == Some(crate::source::value_checksum::CHECKSUM_RENDER_ID)
        && manifest.checksum_key_column.as_deref() == repair_key
        && recorded.len() == repair.len()
        && recorded
            .iter()
            .all(|c| repair.contains_key(&c.name) && c.checksum.parse::<u64>().is_ok())
        && superseded
            .is_some_and(|s| s.is_empty() || recorded.iter().all(|c| s.contains_key(&c.name)));
    if !foldable {
        log::warn!(
            "repair: the manifest's value checksums cannot be extended to cover the repaired \
             parts (different fold, key column or column set, or a superseded part could not \
             be re-read) — dropping them; `validate --depth full` will skip the value re-read \
             for this prefix"
        );
        manifest.column_checksums = None;
        manifest.checksum_render = None;
        manifest.checksum_key_column = None;
        return;
    }
    for c in recorded.iter_mut() {
        let sum = c
            .checksum
            .parse::<u64>()
            .unwrap_or(0)
            .wrapping_add(repair[&c.name])
            .wrapping_sub(
                superseded
                    .and_then(|s| s.get(&c.name))
                    .copied()
                    .unwrap_or(0),
            );
        c.checksum = sum.to_string();
    }
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

    // ── repair must update the run-unique manifest copy the loader reads ──────
    //
    // RED before the fix: repair wrote ONLY the canonical manifest.json, leaving
    // the immutable `manifest-<run_id>.json` sidecar stale. `rivet load` is
    // manifest-authoritative and prefers the run-unique copy, so the repaired
    // parts were silently dropped at load while every count/gate passed. Assert
    // on the manifest COPY (not a data re-read — a re-read can't see a sidecar
    // clobber; the process rules sidecar rule).
    fn formb_manifest(render: Option<&str>, key: Option<&str>) -> RunManifest {
        serde_json::from_value(serde_json::json!({
            "manifest_version": crate::manifest::MANIFEST_VERSION,
            "run_id": "r", "export_name": "e", "mode": "chunked",
            "started_at": "t", "finished_at": "t", "status": "success",
            "source": {"engine": "postgres"},
            "destination": {"kind": "local", "uri": "/x"},
            "format": "parquet", "compression": "zstd", "schema_fingerprint": "f",
            "row_count": 0, "part_count": 0, "parts": [],
            "column_checksums": [{"name": "id", "checksum": u64::MAX.to_string()},
                                 {"name": "v", "checksum": "10"}],
            "checksum_render": render, "checksum_key_column": key,
        }))
        .unwrap()
    }

    #[test]
    fn repair_checksums_fold_by_wrapping_sum_into_a_v2_manifest() {
        use crate::source::value_checksum::CHECKSUM_RENDER_ID;
        let repair = [("id".to_string(), 2u64), ("v".to_string(), 5u64)].into();
        let mut m = formb_manifest(Some(CHECKSUM_RENDER_ID), Some("id"));
        fold_repair_checksums(&mut m, &repair, Some("id"), Some(&Default::default()));
        let got: Vec<(String, String)> = m
            .column_checksums
            .unwrap()
            .into_iter()
            .map(|c| (c.name, c.checksum))
            .collect();
        assert_eq!(
            got,
            vec![("id".into(), "1".into()), ("v".into(), "15".into())]
        );
        assert_eq!(m.checksum_render.as_deref(), Some(CHECKSUM_RENDER_ID));
    }

    #[test]
    fn repair_checksums_are_dropped_when_they_cannot_be_folded_truthfully() {
        use crate::source::value_checksum::CHECKSUM_RENDER_ID;
        let full = [("id".to_string(), 2u64), ("v".to_string(), 5u64)].into();
        let partial = [("id".to_string(), 2u64)].into();
        for (render, key, repair) in [
            (None, Some("id"), &full),
            (Some(CHECKSUM_RENDER_ID), None, &full),
            (Some(CHECKSUM_RENDER_ID), Some("id"), &partial),
        ] {
            let mut m = formb_manifest(render, key);
            fold_repair_checksums(&mut m, repair, Some("id"), Some(&Default::default()));
            assert!(
                m.column_checksums.is_none(),
                "{render:?} {key:?} {repair:?}"
            );
        }
    }

    #[test]
    fn repair_updates_the_run_unique_manifest_copy_not_just_the_canonical() {
        use crate::config::{DestinationConfig, DestinationType};
        use crate::manifest::{
            MANIFEST_VERSION, ManifestDestination, ManifestSource, ManifestStatus,
            run_unique_manifest_name,
        };

        let dir = tempfile::tempdir().unwrap();
        let dpath = dir.path().to_str().unwrap().to_string();
        let run_id = "orders_20260722T120000.000";
        let part = |id: u32, rows: i64| ManifestPart {
            part_id: id,
            path: format!("orders_{id}.parquet"),
            rows,
            size_bytes: 100,
            content_fingerprint: "xxh3:0000000000000000".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        };
        // A finalized run: 2 committed parts, 20 rows.
        let manifest = RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            manifest_version: MANIFEST_VERSION,
            run_id: run_id.into(),
            export_name: "public.orders".into(),
            export_family: String::new(),
            mode: "chunked".into(),
            started_at: "2026-07-22T12:00:00Z".into(),
            finished_at: "2026-07-22T12:00:10Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: dpath.clone(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count: 20,
            part_count: 2,
            parts: vec![part(1, 10), part(2, 10)],
            column_checksums: None,
            checksum_key_column: None,
        };
        let bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        // Both files as a real finalize would leave them; the run-unique copy is
        // the one the loader reads and the one that went stale after repair.
        std::fs::write(dir.path().join(MANIFEST_FILENAME), &bytes).unwrap();
        std::fs::write(dir.path().join(run_unique_manifest_name(run_id)), &bytes).unwrap();

        let dest_cfg = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dpath),
            ..Default::default()
        };
        // Repair recovers one more part: id 3, 7 rows.
        record_repair_parts_in_manifest(
            &dest_cfg,
            &[(3, vec![part(3, 7)])],
            Some(run_id),
            &Default::default(),
            None,
        )
        .unwrap();

        let read = |name: String| -> RunManifest {
            serde_json::from_slice(&std::fs::read(dir.path().join(name)).unwrap()).unwrap()
        };
        let run_unique = read(run_unique_manifest_name(run_id));
        assert_eq!(
            run_unique.parts.len(),
            3,
            "the loader-authoritative run-unique copy must list the repair part"
        );
        assert_eq!(
            run_unique.row_count, 27,
            "run-unique row_count must reflect the recovered rows (20 + 7)"
        );
        // The canonical stays consistent with it.
        assert_eq!(read(MANIFEST_FILENAME.to_string()).parts.len(), 3);
    }

    // ── option B: the manifest declares the replacement, the files stay ──────

    use crate::pipeline::chunked::chunk_part_filename;

    /// Write an `(id BIGINT, v TEXT)` parquet part at `dir/name` and return its write-side keyed checksums.
    fn write_part(
        dir: &std::path::Path,
        name: &str,
        ids: std::ops::RangeInclusive<i64>,
    ) -> Vec<u64> {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = ids.collect();
        let vs: Vec<String> = ids.iter().map(|i| format!("v{i}")).collect();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(Int64Array::from(ids)),
                std::sync::Arc::new(StringArray::from(vs)),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join(name)).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        crate::source::value_checksum::arrow_batch_checksums_keyed(&batch, 0)
    }

    fn sum(parts: &[&Vec<u64>]) -> Vec<String> {
        (0..2)
            .map(|c| {
                parts
                    .iter()
                    .fold(0u64, |a, p| a.wrapping_add(p[c]))
                    .to_string()
            })
            .collect()
    }

    fn mpart(id: u32, path: &str, rows: i64) -> ManifestPart {
        ManifestPart {
            part_id: id,
            path: path.into(),
            rows,
            size_bytes: 1,
            content_fingerprint: "xxh3:0000000000000000".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }
    }

    /// A finalized chunked run of `orders` (run `r1`): chunk 0 = ids 1..=3, chunk 1 = ids 4..=5 (id 6 missing).
    struct Fixture {
        dir: tempfile::TempDir,
        chunk0: String,
        chunk1: String,
        sums0: Vec<u64>,
    }

    fn fixture() -> Fixture {
        use crate::source::value_checksum::CHECKSUM_RENDER_ID;
        let dir = tempfile::tempdir().unwrap();
        let exp = dir.path().join("exp");
        std::fs::create_dir_all(&exp).unwrap();
        let chunk0 = chunk_part_filename("orders", 0, "parquet");
        let chunk1 = chunk_part_filename("orders", 1, "parquet");
        let sums0 = write_part(&exp, &chunk0, 1..=3);
        let sums1 = write_part(&exp, &chunk1, 4..=5);
        let m = RunManifest {
            export_name: "orders".into(),
            export_family: "orders".into(),
            mode: "chunked".into(),
            column_checksums: Some(
                ["id", "v"]
                    .iter()
                    .zip(sum(&[&sums0, &sums1]))
                    .map(|(n, c)| crate::manifest::ColumnChecksum {
                        name: (*n).into(),
                        checksum: c,
                    })
                    .collect(),
            ),
            checksum_render: Some(CHECKSUM_RENDER_ID.into()),
            checksum_key_column: Some("id".into()),
            ..RunManifest::for_test("r1", &[(&chunk0, 3), (&chunk1, 2)])
        };
        crate::pipeline::manifest_writer::write_manifest(
            &*crate::destination::create_destination(&dest_cfg(&exp)).unwrap(),
            &m,
        )
        .unwrap();
        Fixture {
            dir,
            chunk0,
            chunk1,
            sums0,
        }
    }

    fn dest_cfg(exp: &std::path::Path) -> crate::config::DestinationConfig {
        crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(exp.to_str().unwrap().to_string()),
            ..Default::default()
        }
    }

    impl Fixture {
        fn exp(&self) -> std::path::PathBuf {
            self.dir.path().join("exp")
        }
        /// Write the repair part of chunk 1 (ids 4..=6) and record it; returns (its name, its checksums).
        fn repair(&self, run: &str, name: Option<String>) -> (String, Vec<u64>) {
            let name = name.unwrap_or_else(|| chunk_part_filename("orders", 1, "parquet"));
            let sums = write_part(&self.exp(), &name, 4..=6);
            let repair = [("id".to_string(), sums[0]), ("v".to_string(), sums[1])].into();
            record_repair_parts_in_manifest(
                &dest_cfg(&self.exp()),
                &[(1, vec![mpart(0, &name, 3)])],
                Some(run),
                &repair,
                Some("id"),
            )
            .unwrap();
            (name, sums)
        }
        fn manifest(&self) -> RunManifest {
            serde_json::from_slice(&std::fs::read(self.exp().join(MANIFEST_FILENAME)).unwrap())
                .unwrap()
        }
        fn status_of(&self, path: &str) -> PartStatus {
            self.manifest()
                .parts
                .iter()
                .find(|p| p.path == path)
                .unwrap()
                .status
        }
    }

    #[test]
    fn repair_supersedes_the_chunks_original_part_and_keeps_its_file() {
        let f = fixture();
        let (repair, repair_sums) = f.repair("r1", None);
        let m = f.manifest();
        assert_eq!(f.status_of(&f.chunk1), PartStatus::Superseded);
        assert_eq!(f.status_of(&f.chunk0), PartStatus::Committed);
        assert_eq!(f.status_of(&repair), PartStatus::Committed);
        assert_eq!(
            (m.row_count, m.part_count),
            (6, 2),
            "3 + 3, the replaced 2 gone"
        );
        assert!(
            f.exp().join(&f.chunk1).exists(),
            "RR5: the superseded file stays"
        );
        let got: Vec<String> = m
            .column_checksums
            .expect("the checksums can be recomputed truthfully, so they must survive")
            .into_iter()
            .map(|c| c.checksum)
            .collect();
        assert_eq!(
            got,
            sum(&[&f.sums0, &repair_sums]),
            "the manifest must attest exactly the committed parts, written-side"
        );
        // Form B over the committed parts agrees with it.
        let dest = crate::destination::create_destination(&dest_cfg(&f.exp())).unwrap();
        assert!(
            crate::source::value_checksum::validate_manifest_checksums(&*dest, "")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn repair_drops_the_checksums_when_a_superseded_part_cannot_be_reread() {
        let f = fixture();
        std::fs::write(f.exp().join(&f.chunk1), b"not parquet").unwrap();
        f.repair("r1", None);
        assert_eq!(f.status_of(&f.chunk1), PartStatus::Superseded);
        assert!(f.manifest().column_checksums.is_none(), "never left stale");
    }

    #[test]
    fn repair_stays_additive_when_the_mapping_is_ambiguous() {
        // A manifest from another run.
        let f = fixture();
        let (repair, _) = f.repair("r-other", None);
        assert_eq!(f.status_of(&f.chunk1), PartStatus::Committed);
        assert_eq!(f.status_of(&repair), PartStatus::Committed);
        assert_eq!(f.manifest().row_count, 8);

        // A repair part the rename could not relabel (still `_chunk0_`).
        let f = fixture();
        f.repair("r1", Some(chunk_part_filename("orders", 0, "parquet")));
        assert_eq!(f.status_of(&f.chunk1), PartStatus::Committed);
        assert_eq!(f.status_of(&f.chunk0), PartStatus::Committed);

        // A committed part whose name carries no chunk index.
        let m = RunManifest {
            export_name: "orders".into(),
            ..RunManifest::for_test(
                "r1",
                &[
                    (&chunk_part_filename("orders", 1, "parquet"), 2),
                    ("orders_stray.parquet", 1),
                ],
            )
        };
        let repaired = [(
            1,
            vec![mpart(0, &chunk_part_filename("orders", 1, "parquet"), 3)],
        )];
        assert!(superseded_parts(&m, &repaired, Some("r1")).is_empty());

        // Another export's part named like a chunk part.
        let m = RunManifest {
            export_name: "orders".into(),
            ..RunManifest::for_test("r1", &[(&chunk_part_filename("items", 1, "parquet"), 2)])
        };
        assert!(superseded_parts(&m, &repaired, Some("r1")).is_empty());
    }

    #[test]
    fn a_superseded_part_is_neither_loaded_nor_kept_by_gc() {
        use crate::destination::gcs::GcsStore;
        use crate::load::reconcile::{fetch_manifests_keyed, gc_orphans, select_load_keys};

        let f = fixture();
        let (repair, _) = f.repair("r1", None);
        let store = GcsStore::open_fs(f.dir.path().to_str().unwrap()).unwrap();
        let keyed = fetch_manifests_keyed(&store, "gs://bucket/exp").unwrap();
        let all: Vec<String> = [&f.chunk0, &f.chunk1, &repair]
            .iter()
            .map(|n| format!("exp/{n}"))
            .collect();
        let mut want = vec![format!("exp/{}", f.chunk0), format!("exp/{repair}")];
        want.sort();
        assert_eq!(
            select_load_keys(&keyed, &all),
            want,
            "the load reads each row once"
        );

        // A run is active on the prefix: gc spares the superseded file.
        gc_orphans(&store, "gs://bucket/exp", &keyed, true, &Default::default()).unwrap();
        assert!(f.exp().join(&f.chunk1).exists());
        // No run is active: gc collects it, and only it.
        let (removed, _) = gc_orphans(
            &store,
            "gs://bucket/exp",
            &keyed,
            false,
            &Default::default(),
        )
        .unwrap();
        assert_eq!(removed, 1);
        assert!(!f.exp().join(&f.chunk1).exists());
        assert!(f.exp().join(&f.chunk0).exists() && f.exp().join(&repair).exists());
    }

    #[test]
    fn validate_claims_a_superseded_part_instead_of_calling_it_untracked() {
        let f = fixture();
        f.repair("r1", None);
        let dest = crate::destination::create_destination(&dest_cfg(&f.exp())).unwrap();
        let rec = crate::pipeline::manifest_reconcile::reconcile_manifest_against_listing(
            &f.manifest(),
            &dest.list_prefix("").unwrap(),
            "",
        );
        assert!(rec.untracked.is_empty(), "{:?}", rec.untracked);
        assert_eq!(
            rec.per_part.len(),
            2,
            "no presence verdict for the superseded part"
        );
    }
}
