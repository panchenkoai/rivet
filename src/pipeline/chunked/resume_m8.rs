//! **Layer: Coordinator**
//!
//! ADR-0012 M8 — manifest-aware resume preamble for chunked exports.
//!
//! Runs once at the start of `--resume` for chunked-checkpoint exports,
//! BEFORE any worker claims a `chunk_task`.  Reads the prior run's
//! `manifest.json` at the destination, lists the prefix, and applies
//! [`crate::pipeline::resume_decisions::build_resume_plan`] to produce
//! per-part Skip / Rewrite / Quarantine decisions.
//!
//! For each `Rewrite` or `Quarantine` decision, the corresponding
//! `chunk_task` is reset from `completed` back to `pending` so the
//! existing claim loop re-exports it.  `Skip` decisions are left alone —
//! that is exactly what the existing chunk_checkpoint resume already
//! does (skip completed tasks).
//!
//! **M9 quarantine (wired):** for each `Quarantine` decision — a manifest
//! part whose destination object diverged on size/fingerprint, or an
//! untracked surplus object under the prefix — this layer resets the
//! `chunk_task` to `pending` AND moves the offending object to
//! `_quarantine/<run_id>/<original-key>` via `quarantine_move` →
//! `Destination::r#move` (copy+delete on S3/GCS, `rename` on local).  The
//! move is **best-effort** (ADR-0012 M9): a failure is counted in
//! `M8ResumeStats.quarantine_move_failures`, escalated to a WARN, and never
//! aborts the resume.  Successful moves land in `quarantined_moved`.
//!
//! **What this layer does not do:**
//! - It never *deletes* an unknown object outright (M9 §"never deletes
//!   unknown objects").  Quarantine is a move; a failed move leaves the
//!   object in place to re-trip M9 on the next resume.
//! - For a `Rewrite` decision (manifest part lost at the destination) it
//!   does NOT pre-clean the slot: the next worker run overwrites it via
//!   `dest.write` — `Atomic` and `FinalizeOnClose` backends both support
//!   clean replacement (ADR-0004).
//!
//! Behaviour with no manifest at destination (fresh prefix or pre-0.7.0
//! legacy run): no-op.  The runner falls back to the existing
//! state-only resume logic.

use std::collections::HashMap;

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, QUARANTINE_PREFIX, RunManifest};
use crate::pipeline::RunSummary;
use crate::pipeline::resume_decisions::{ResumeDecision, build_resume_plan};
// V21 (CWE-400): cap the manifest body so a planted multi-GB manifest.json
// cannot OOM the resume preamble.  The shared enforcement point lives with the
// other two control-artifact readers in `validate_manifest`.
use crate::pipeline::validate_manifest::{MANIFEST_MAX_BYTES, read_capped};
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// Aggregate counters from one M8 reconciliation pass.
///
/// Surfaced into the run report (`summary.json` / `summary.md`) so an
/// operator can see, at a glance, how much work the manifest saved (or
/// invalidated).  Shipped opaquely as a struct so adding fields
/// (`legacy_run`, `manifest_run_id_mismatch`, …) is forward-compatible.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct M8Stats {
    /// Manifest parts whose state row stays `completed` — work the
    /// resume run skips entirely.
    pub skipped: usize,
    /// Manifest parts whose state row was reset to `pending` — chunks
    /// the resume run will re-export.
    pub reset_for_rewrite: usize,
    /// Manifest parts the matrix flagged for quarantine (size mismatch,
    /// fingerprint drift).  After Phase C-δ, the divergent destination
    /// object is also moved to `_quarantine/<run_id>/` (best-effort —
    /// see `quarantined_moved` for the count of successful moves).
    pub reset_for_quarantine: usize,
    /// Quarantine-move successes — count of objects relocated to
    /// `_quarantine/<run_id>/` so the next write doesn't share the
    /// prefix with stale data.  ADR-0012 M9: best-effort, never
    /// fatal.  `quarantined_moved <= reset_for_quarantine + untracked
    /// surplus` — failures stay in `quarantine_move_failures`.
    pub quarantined_moved: usize,
    /// Quarantine-move failures.  Logged but the run proceeds — the
    /// next write may overwrite the original key (atomic backends),
    /// or the operator can clean up by hand.  Each failure is logged
    /// at WARN with source/destination/reason.
    pub quarantine_move_failures: usize,
    /// Manifest parts whose `path` did not match any chunk_task's
    /// `file_name`.  Means the manifest references a file we don't
    /// know about (foreign manifest, or chunk_task table missing the
    /// file_name due to a prior crash).  Logged but not acted on —
    /// destination side will be re-checked on next M5 run.
    pub orphan_parts: usize,
}

/// Apply ADR-0012 M8's decision matrix to the destination state.
///
/// Run order: AFTER `ensure_chunk_checkpoint_plan` (which establishes
/// the in-progress run_id and resets stale 'running' tasks) and BEFORE
/// the worker claim loop starts.  The state lock window between this
/// preamble and the workers is short and SQLite/Postgres handle
/// concurrent reads fine; once a worker holds a 'running' status, it
/// is invisible to a second M8 invocation regardless.
///
/// Streaming destinations skip silently (no prefix to verify, mirrors
/// `finalize_validate_manifest`).  I/O failures reading the manifest
/// or listing the prefix are logged and the function returns
/// `M8Stats::default()` so resume falls back to the pre-0.7.0
/// state-only path — never abort a resume because a destination probe
/// failed.
///
/// `run_id` must be the chunk_checkpoint run id (the one
/// `ensure_chunk_checkpoint_plan` returned for the resume path).
/// Round-4/5 fix (durability-ordering-matrix `manifest_driven_recovery` chunked): a
/// chunked-checkpoint crash BEFORE the terminal manifest write leaves the pre-crash
/// parts durably committed (parquet on the destination + a `file_log` row per part)
/// but with NO destination manifest. On `--resume`, `claim_next_chunk_task` skips
/// completed chunks and the M8 preamble finds no manifest to hydrate, so
/// `finalize_manifest` (built solely from `summary.manifest_parts`) writes a manifest
/// that OMITS them — the manifest-authoritative `rivet load` then silently drops
/// their rows. Reconstruct those parts from the state DB's `file_log` (which records
/// EVERY committed part — including every `max_file_size` rotation sibling — with its
/// real byte size), so the finalize manifest is COMPLETE. The state DB is the export
/// machine's own resume record; the reconstructed destination manifest stays the
/// loader's source of truth (ADR-0001). fingerprint/md5 aren't in file_log, so the
/// parts are DECLARED + size-verified (an empty md5 degrades validate to a size-only
/// check) though not content-re-verified — strictly better than a silent orphan.
/// Returns the number of parts reconstructed.
pub(crate) fn rehydrate_manifest_parts_from_file_log(
    state: &StateStore,
    run_id: &str,
    summary: &mut RunSummary,
) -> Result<usize> {
    // Reconstruct from file_log, NOT chunk_task: file_log records EVERY committed
    // part — including every max_file_size rotation sibling — with its real byte
    // size, whereas chunk_task stores only the FIRST sibling's file_name and the
    // full-chunk row count. Rehydrating from chunk_task orphaned the other siblings
    // (silent loss / blocked load) and left size_bytes=0 (a lying PartSizeMismatch
    // in `rivet validate`) — both closed by using file_log (round-5).
    let files = state.list_files_for_run(run_id)?;
    let mut next_id = summary
        .manifest_parts
        .iter()
        .map(|p| p.part_id)
        .max()
        .unwrap_or(0);
    let mut rehydrated = 0usize;
    let mut rehydrated_rows = 0i64;
    let mut rehydrated_bytes = 0u64;
    for f in files {
        // Don't duplicate a part a fresh record_part already added this run.
        if summary.manifest_parts.iter().any(|p| p.path == f.file_name) {
            continue;
        }
        next_id += 1;
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: next_id,
            path: f.file_name,
            rows: f.row_count,
            size_bytes: f.bytes.max(0) as u64,
            // fingerprint/md5 aren't in file_log; an EMPTY md5 degrades `rivet
            // validate` to a size-only check (the real bytes now match), so the
            // part is DECLARED + size-verified, never a lying mismatch.
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        rehydrated += 1;
        rehydrated_rows += f.row_count;
        rehydrated_bytes += f.bytes.max(0) as u64;
    }
    if rehydrated > 0 {
        // Keep the summary aggregates consistent with the reconstructed manifest so
        // the run card, the reconcile gate, and the coherence invariant all agree.
        summary.files_committed += rehydrated;
        summary.files_produced += rehydrated;
        summary.total_rows += rehydrated_rows;
        summary.bytes_written += rehydrated_bytes;
        log::info!(
            "resume: reconstructed {rehydrated} committed part(s) ({rehydrated_rows} rows, \
             {rehydrated_bytes} bytes) into the manifest from the state DB file_log (no \
             destination manifest to hydrate from) — the finalize manifest now covers every \
             committed part, rotation siblings included"
        );
    }
    Ok(rehydrated)
}

pub(crate) fn apply_m8_resume_decisions(
    state: &StateStore,
    run_id: &str,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<M8Stats> {
    use crate::destination::WriteCommitProtocol;

    // ── 1. Open destination + early-exit on streaming ──────────────────
    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "M8 resume preamble: cannot open destination for export '{}' (not fatal — \
                 falling back to state-only resume): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
        log::debug!(
            "M8 resume preamble: streaming destination for export '{}'; skipped",
            plan.export_name
        );
        return Ok(M8Stats::default());
    }

    // ── 2. Read manifest body (absent → fresh / legacy prefix, no-op) ──
    let manifest_bytes = match dest.head(MANIFEST_FILENAME) {
        Ok(Some(_)) => match read_capped(&*dest, MANIFEST_FILENAME, MANIFEST_MAX_BYTES) {
            Ok(b) => b,
            Err(e) => {
                log::warn!(
                    "M8 resume preamble: manifest.json present but unreadable on destination \
                     for export '{}' (not fatal): {:#}",
                    plan.export_name,
                    e
                );
                return Ok(M8Stats::default());
            }
        },
        Ok(None) => {
            log::info!(
                "M8 resume preamble: no manifest at destination for export '{}'; \
                 falling back to state-only resume (legacy / fresh prefix)",
                plan.export_name
            );
            // Round-4 fix: with NO destination manifest to hydrate from (a first-run
            // crash before the terminal write), reconstruct the already-committed
            // parts from the state DB's COMPLETED chunk_tasks — otherwise finalize
            // writes a manifest containing ONLY the resume-processed chunks, orphaning
            // the pre-crash chunks' durable parts from the manifest-authoritative
            // loader (silent row loss). A "fresh prefix" (no completed chunks) rehydr-
            // ates nothing, so this is safe for the legacy/fresh case too.
            rehydrate_manifest_parts_from_file_log(state, run_id, summary)?;
            return Ok(M8Stats::default());
        }
        Err(e) => {
            log::warn!(
                "M8 resume preamble: manifest.json head failed for export '{}' (not fatal): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };

    let manifest: RunManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "M8 resume preamble: manifest.json at destination did not parse for export '{}' \
                 (not fatal — manifest may be from a future schema version): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };

    // ── 3. Refuse to act on a foreign manifest (different run_id) ──────
    //
    // chunk_checkpoint reuses the same run_id across resumes, so a
    // mismatch here means: someone restored a backup, copy-pasted a
    // prefix, or two exports collided at the same destination.  We do
    // NOT reset chunk_tasks based on a manifest that wasn't produced
    // by *this* run — the file_name match could be coincidental.
    if manifest.run_id != run_id {
        log::info!(
            "M8 resume preamble: destination manifest run_id '{}' differs from current resume \
             run_id '{}' for export '{}'; skipping reconciliation (foreign manifest)",
            manifest.run_id,
            run_id,
            plan.export_name
        );
        return Ok(M8Stats::default());
    }

    // ── 4. List the prefix ─────────────────────────────────────────────
    let listing = match dest.list_prefix("") {
        Ok(l) => l,
        Err(e) => {
            log::warn!(
                "M8 resume preamble: list_prefix failed for export '{}' (not fatal): {:#}",
                plan.export_name,
                e
            );
            return Ok(M8Stats::default());
        }
    };

    // ── 5. Apply the matrix + reset misaligned chunk_tasks ─────────────
    let resume_plan = build_resume_plan(&manifest, &listing);
    let tasks = state.list_chunk_tasks_for_run(run_id)?;

    // Index by file_name so we can find the chunk_task for a manifest
    // part in O(1).  Missing file_name (chunk that crashed before
    // recording it) is filtered out — those are claimed by the
    // existing `--resume` mechanism via their `pending`/`failed` status.
    let by_file: HashMap<&str, &crate::state::ChunkTaskInfo> = tasks
        .iter()
        .filter_map(|t| t.file_name.as_deref().map(|n| (n, t)))
        .collect();

    let mut stats = M8Stats::default();

    // Index manifest parts by path so we can hydrate `summary.manifest_parts`
    // for `Skip`-decided rows.  Without this hydration the manifest written
    // at end-of-resume would carry only THIS run's freshly-committed parts,
    // erasing the prior manifest's record of the parts we successfully
    // skipped.  The next `rivet validate` would then flag every skipped
    // part as `UntrackedObject`.  ADR-0012 M4 requires the manifest to be
    // the cumulative record of the run, including parts inherited from
    // prior resume attempts.
    let manifest_part_by_path: HashMap<&str, &crate::manifest::ManifestPart> = manifest
        .parts
        .iter()
        .filter(|p| p.status == crate::manifest::PartStatus::Committed)
        .map(|p| (p.path.as_str(), p))
        .collect();

    // Hydrate the run-wide schema fingerprint from the prior manifest.
    // If THIS resume happens to do no write at all (e.g. just confirms an
    // unchanged destination), the sink in `chunked/exec.rs` never resolves
    // a schema and `summary.schema_fingerprint` stays None.  Without this
    // hydration `finalize_manifest` would then emit the placeholder
    // `xxh3:0000000000000000` — losing the M3 evidence the prior run
    // already produced.  Since this resume reuses the same run_id we just
    // verified, the prior fingerprint is the authoritative one.
    if summary.schema_fingerprint.is_none()
        && manifest.schema_fingerprint != "xxh3:0000000000000000"
    {
        summary.schema_fingerprint = Some(manifest.schema_fingerprint.clone());
    }

    for (path, decision) in &resume_plan.per_part {
        match decision.decision {
            ResumeDecision::Skip => {
                stats.skipped += 1;
                // Hydrate the summary so the end-of-resume manifest carries
                // this committed part too.  Clone the prior manifest's part
                // verbatim — the part_id, content_fingerprint, rows, and
                // size_bytes are all from the previous successful write and
                // remain valid for the destination object that's still there.
                //
                // A Skip needs NO chunk_task: it copies a durable, matching
                // manifest part. Requiring one here silently dropped every
                // max_file_size ROTATION SIBLING — chunk_task records only the
                // FIRST sibling's file_name, so `chunk-N-p1…` missed the lookup,
                // was counted an orphan, and never hydrated. The finalize
                // manifest (built solely from `manifest_parts`) then omitted it
                // and the manifest-authoritative `rivet load` lost its rows.
                if let Some(p) = manifest_part_by_path.get(path.as_str()) {
                    summary.manifest_parts.push((*p).clone());
                }
            }
            // Rewrite / Quarantine RE-EXPORT the owning chunk, so they DO need
            // its chunk_task. A part with no task is a genuine orphan (a foreign
            // or stale manifest entry) — logged, not acted on, as before.
            ResumeDecision::Rewrite => {
                let Some(task) = by_file.get(path.as_str()) else {
                    stats.orphan_parts += 1;
                    continue;
                };
                let n = state.reset_chunk_task_for_re_export(
                    run_id,
                    task.chunk_index,
                    "M8 reset: manifest part missing at destination",
                )?;
                if n > 0 {
                    stats.reset_for_rewrite += 1;
                }
            }
            ResumeDecision::Quarantine { reason } => {
                let Some(task) = by_file.get(path.as_str()) else {
                    stats.orphan_parts += 1;
                    continue;
                };
                let n = state.reset_chunk_task_for_re_export(
                    run_id,
                    task.chunk_index,
                    &format!("M8 reset: destination part diverged ({:?})", reason),
                )?;
                if n > 0 {
                    stats.reset_for_quarantine += 1;
                }
                // M9: move the divergent destination object out of the way.
                // Best-effort — see `quarantine_move` below.
                quarantine_move(&*dest, path, run_id, &plan.export_name, &mut stats);
            }
        }
    }

    // ── 6. M9: move untracked surplus objects ──────────────────────────
    //
    // Surplus = objects under the destination prefix that don't belong
    // to any committed part in the manifest.  Most likely leftovers from
    // a prior resume that wrote with a different timestamp.  Best-effort
    // move to `_quarantine/<run_id>/<original-name>` — never fatal,
    // never deletes (M9 §"never deletes unknown objects").
    //
    // BUT the destination manifest can LAG the state DB: a resume that
    // committed parts (a `file_log` row + a `completed` chunk_task each) then
    // crashed before re-finalizing leaves those parts ABSENT from the stale
    // manifest yet DURABLY committed. The manifest-vs-listing reconcile flags
    // them as untracked surplus — quarantining (MOVING) them would drop the exact
    // rows finalize is about to rehydrate from file_log (silent loss AND destroyed
    // at-least-once recoverability, since a move is not a delete but the part is
    // gone from its recorded path). The state DB is the export's source of truth
    // (ADR-0001), so exclude any object file_log records as a committed part for
    // this run before quarantining. This only ever PREVENTS an erroneous move; a
    // truly-surplus object has no file_log row and is still quarantined.
    let committed_parts: std::collections::HashSet<String> = state
        .list_files_for_run(run_id)?
        .into_iter()
        .map(|f| f.file_name)
        .collect();
    let mut untracked_surplus = 0usize;
    for key in resume_plan.untracked.keys() {
        if committed_parts.contains(key.as_str()) {
            continue; // durably committed per file_log — not surplus, do not move
        }
        untracked_surplus += 1;
        quarantine_move(&*dest, key, run_id, &plan.export_name, &mut stats);
    }

    log::info!(
        "M8 resume preamble: export '{}' run_id '{}' — {} skipped, {} reset for rewrite, \
         {} reset for quarantine, {} quarantined ({} move failure(s)), {} orphan part(s), \
         {} untracked surplus object(s)",
        plan.export_name,
        run_id,
        stats.skipped,
        stats.reset_for_rewrite,
        stats.reset_for_quarantine,
        stats.quarantined_moved,
        stats.quarantine_move_failures,
        stats.orphan_parts,
        untracked_surplus,
    );

    Ok(stats)
}

/// ADR-0012 M9 — best-effort move of a destination object to the
/// quarantine prefix.  Layout: `_quarantine/<run_id>/<original-key>`.
/// On failure: log at WARN, increment counter, continue.  Never fatal,
/// never deletes the source on a partial failure (the underlying
/// `Destination::move` is allowed to be non-atomic for object stores —
/// see ADR-0012 M9 §"copy + delete").
fn quarantine_move(
    dest: &dyn Destination,
    src_key: &str,
    run_id: &str,
    export_name: &str,
    stats: &mut M8Stats,
) {
    let quarantine_key = format!("{}/{}/{}", QUARANTINE_PREFIX, run_id, src_key);
    match dest.r#move(src_key, &quarantine_key) {
        Ok(()) => {
            stats.quarantined_moved += 1;
            log::info!(
                "M9 quarantine: export '{}' moved '{}' → '{}'",
                export_name,
                src_key,
                quarantine_key,
            );
        }
        Err(e) => {
            stats.quarantine_move_failures += 1;
            log::warn!(
                "M9 quarantine: export '{}' could not move '{}' → '{}' (not fatal — \
                 next resume will re-trip M9 on this object): {:#}",
                export_name,
                src_key,
                quarantine_key,
                e,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ChunkTaskInfo;

    // ── mutation-tier2 gap closure ───────────────────────────────────────────
    // `apply_m8_resume_decisions` — the resume-decision CORE (skip / rewrite /
    // quarantine per part) — had 38 missed mutants: even stubbing the whole
    // function to `Ok(Default::default())` survived, because no test drove the
    // full flow (real local destination + state + manifest). A wrong skip is a
    // silently missing part; a wrong rewrite is duplicates.

    fn m8_manifest(run_id: &str, parts: Vec<crate::manifest::ManifestPart>) -> RunManifest {
        use crate::manifest::{ManifestDestination, ManifestSource, ManifestStatus};
        let row_count = parts.iter().map(|p| p.rows).sum();
        let part_count = parts.len() as u32;
        RunManifest {
            mode: "batch".to_string(),
            manifest_version: crate::manifest::MANIFEST_VERSION,
            run_id: run_id.into(),
            export_name: "orders".into(),
            started_at: "2026-07-14T00:00:00Z".into(),
            finished_at: "2026-07-14T00:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: None,
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out/".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:00000000deadbeef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    fn m8_part(path: &str, rows: i64, size: u64) -> crate::manifest::ManifestPart {
        crate::manifest::ManifestPart {
            part_id: 0, // ids are irrelevant to the M8 path match; keep distinct below
            path: path.into(),
            rows,
            size_bytes: size,
            content_fingerprint: "xxh3:1".into(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        }
    }

    fn m8_plan(dest: &std::path::Path) -> crate::plan::ResolvedRunPlan {
        use crate::config::{DestinationConfig, DestinationType, SourceConfig, SourceType};
        use crate::tuning::SourceTuning;
        crate::plan::ResolvedRunPlan {
            export_name: "orders".into(),
            base_query: "SELECT 1".into(),
            strategy: crate::plan::ExtractionStrategy::Snapshot,
            format: crate::config::FormatType::Parquet,
            compression: crate::config::CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some(dest.to_string_lossy().into_owned()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: true,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
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

    #[test]
    fn rehydration_recovers_all_rotation_siblings_from_file_log() {
        // Round-5: chunk_task stores ONE file_name per chunk (only the FIRST
        // max_file_size rotation sibling), so rehydrating from it orphaned the other
        // siblings (silent loss) and left size_bytes=0 (a lying validate mismatch).
        // file_log records EVERY committed part with its real bytes. RED against the
        // chunk_task-based rehydration (which recovered 1 of 2 siblings, size 0).
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        let run_id = "r_rot";
        // Two rotation siblings of ONE chunk, as record_part logs them per-part.
        state
            .record_file(
                run_id,
                "orders",
                "orders_chunk0_p0.parquet",
                30,
                4096,
                "parquet",
                None,
            )
            .unwrap();
        state
            .record_file(
                run_id,
                "orders",
                "orders_chunk0_p1.parquet",
                20,
                2048,
                "parquet",
                None,
            )
            .unwrap();
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let rows_before = summary.total_rows;

        let n = rehydrate_manifest_parts_from_file_log(&state, run_id, &mut summary).unwrap();

        assert_eq!(
            n, 2,
            "BOTH rotation siblings must be reconstructed, not just the first"
        );
        let paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        assert!(
            paths.contains(&"orders_chunk0_p0.parquet")
                && paths.contains(&"orders_chunk0_p1.parquet"),
            "both sibling files must be declared: {paths:?}"
        );
        assert_eq!(
            summary.manifest_parts.iter().map(|p| p.rows).sum::<i64>(),
            50,
            "all 50 rows across both siblings declared"
        );
        assert!(
            summary.manifest_parts.iter().all(|p| p.size_bytes > 0),
            "parts carry their REAL byte size (not 0) so validate's size check can't lie"
        );
        assert_eq!(
            summary.total_rows - rows_before,
            50,
            "row aggregate bumped by the union"
        );
    }

    /// Full-matrix flow: part A intact → Skip (+ summary hydration), part B
    /// missing → Rewrite (task reset), part C size-diverged → Quarantine
    /// (task reset + object moved), part D with no task → orphan. Also
    /// hydrates the schema fingerprint from the prior manifest.
    #[test]
    fn apply_m8_full_matrix_skip_rewrite_quarantine_orphan() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();

        // Destination objects: A matches its manifest size, C diverges, B absent.
        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap(); // 5
        std::fs::write(dir.path().join("part-c.parquet"), b"CCC").unwrap(); // 3 != 9
        let mut parts = vec![
            m8_part("part-a.parquet", 10, 5),
            m8_part("part-b.parquet", 10, 7),
            m8_part("part-c.parquet", 10, 9),
            m8_part("part-d.parquet", 10, 1), // no chunk_task → orphan
        ];
        for (i, p) in parts.iter_mut().enumerate() {
            p.part_id = (i + 1) as u32;
        }
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        // State: tasks 0..2 completed with file names a/b/c.
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state
            .insert_chunk_tasks(run_id, &[(0, 10), (10, 20), (20, 30)])
            .unwrap();
        for (idx, name) in [
            (0, "part-a.parquet"),
            (1, "part-b.parquet"),
            (2, "part-c.parquet"),
        ] {
            // claim → completed so file_name is recorded like a real run.
            state.claim_next_chunk_task(run_id).unwrap();
            state
                .complete_chunk_task(run_id, idx, 10, Some(name))
                .unwrap();
        }

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        assert!(summary.schema_fingerprint.is_none(), "fixture precondition");

        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert_eq!(stats.skipped, 1, "part A (intact) is skipped");
        assert_eq!(
            stats.reset_for_rewrite, 1,
            "part B (missing) resets its task"
        );
        assert_eq!(
            stats.reset_for_quarantine, 1,
            "part C (size-diverged) resets its task"
        );
        assert_eq!(
            stats.quarantined_moved, 1,
            "part C's divergent object is moved to _quarantine/<run_id>/"
        );
        assert_eq!(stats.quarantine_move_failures, 0);
        assert_eq!(stats.orphan_parts, 1, "part D has no chunk_task");
        assert!(
            dir.path()
                .join(format!("{QUARANTINE_PREFIX}/{run_id}/part-c.parquet"))
                .exists(),
            "quarantined object relocated"
        );

        // Skip hydration: the summary carries the prior manifest's part A.
        assert_eq!(
            summary
                .manifest_parts
                .iter()
                .map(|p| p.path.as_str())
                .collect::<Vec<_>>(),
            vec!["part-a.parquet"],
            "skipped parts hydrate into the resume summary (M4 cumulative manifest)"
        );
        assert_eq!(
            summary.schema_fingerprint.as_deref(),
            Some("xxh3:00000000deadbeef"),
            "schema fingerprint hydrates from the prior manifest"
        );

        // State-side effects: A stays completed; B and C are pending again.
        let tasks = state.list_chunk_tasks_for_run(run_id).unwrap();
        let status_of = |i: i64| {
            tasks
                .iter()
                .find(|t| t.chunk_index == i)
                .unwrap()
                .status
                .clone()
        };
        assert_eq!(status_of(0), "completed");
        assert_eq!(status_of(1), "pending", "rewrite resets the task");
        assert_eq!(status_of(2), "pending", "quarantine resets the task");
    }

    // RED for the Path-B rotation-sibling silent loss (graph-surfaced). When a
    // prior-run manifest EXISTS (resume with manifest, not the Path-A "no
    // manifest" reconstruct), the Skip hydration looked up a chunk_task by the
    // part's file_name FIRST and `continue`d on a miss. A max_file_size chunk
    // rotates into siblings but chunk_task records only the FIRST sibling's name,
    // so every OTHER sibling missed the lookup, was counted an orphan, and was
    // NOT hydrated into `summary.manifest_parts` — the finalize manifest (built
    // solely from that vec) then omitted it and the manifest-authoritative
    // `rivet load` silently dropped its rows. A Skip needs NO chunk_task (it
    // clones the prior manifest's part verbatim), so it must hydrate regardless.
    #[test]
    fn apply_m8_skip_hydrates_rotation_siblings_without_a_chunk_task() {
        let run_id = "m8rot";
        let dir = tempfile::tempdir().unwrap();

        // One chunk rotated into two siblings; BOTH still present at the dest
        // (matching their manifest size) → both are Skip decisions.
        std::fs::write(dir.path().join("chunk0-p0.parquet"), b"AAAAA").unwrap(); // 5
        std::fs::write(dir.path().join("chunk0-p1.parquet"), b"BBBBBBB").unwrap(); // 7
        let mut parts = vec![
            m8_part("chunk0-p0.parquet", 30, 5),
            m8_part("chunk0-p1.parquet", 20, 7),
        ];
        for (i, p) in parts.iter_mut().enumerate() {
            p.part_id = (i + 1) as u32;
        }
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        // ONE chunk_task for chunk 0, recording only the FIRST sibling's name —
        // exactly how a real rotated chunk is logged.
        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 50)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 50, Some("chunk0-p0.parquet"))
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));

        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert_eq!(stats.skipped, 2, "BOTH siblings are intact → both skipped");
        assert_eq!(
            stats.orphan_parts, 0,
            "a rotation sibling is NOT an orphan just because chunk_task holds one name"
        );
        let mut paths: Vec<&str> = summary
            .manifest_parts
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec!["chunk0-p0.parquet", "chunk0-p1.parquet"],
            "both siblings must hydrate into the resume manifest — else load drops the second's rows"
        );
        assert_eq!(
            summary.manifest_parts.iter().map(|p| p.rows).sum::<i64>(),
            50,
            "all 50 rows across both siblings are carried into the finalize manifest"
        );
    }

    // RED before the file_log guard in step 6: a part durably committed by a
    // prior resume (a file_log row) but ABSENT from the stale destination
    // manifest was classified untracked surplus and quarantine-MOVED — dropping
    // the exact rows finalize rehydrates from file_log (silent loss). The guard
    // must skip any untracked object file_log records for the run.
    #[test]
    fn m8_does_not_quarantine_a_part_recorded_in_the_state_file_log() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();

        // Manifest tracks only part-a. part-x is on disk + in file_log but NOT in
        // the manifest — a resume committed it, then crashed before re-finalize.
        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap(); // 5
        std::fs::write(dir.path().join("part-x.parquet"), b"XXXXXXX").unwrap(); // 7
        let mut parts = vec![m8_part("part-a.parquet", 10, 5)];
        parts[0].part_id = 1;
        let manifest = m8_manifest(run_id, parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some("part-a.parquet"))
            .unwrap();
        // file_log records BOTH committed parts — including part-x, which the
        // stale manifest omits (the crash-before-finalize case).
        state
            .record_file(
                run_id,
                "orders",
                "part-a.parquet",
                10,
                5,
                "parquet",
                Some("zstd"),
            )
            .unwrap();
        state
            .record_file(
                run_id,
                "orders",
                "part-x.parquet",
                10,
                7,
                "parquet",
                Some("zstd"),
            )
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        // part-x is committed per file_log → left in place, NOT quarantined.
        assert!(
            dir.path().join("part-x.parquet").exists(),
            "a file_log-committed part must stay at its path"
        );
        assert!(
            !dir.path()
                .join(format!("{QUARANTINE_PREFIX}/{run_id}/part-x.parquet"))
                .exists(),
            "a file_log-committed part must NOT be moved to _quarantine (that would be silent loss)"
        );
    }

    /// The fingerprint hydration guard is `summary is None AND manifest is not
    /// the placeholder` — an `&& -> ||` mutant overwrites a LIVE summary's
    /// fingerprint with the prior manifest's, silently rewriting this run's
    /// schema evidence.
    #[test]
    fn apply_m8_never_overwrites_a_live_schema_fingerprint() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap();
        let mut parts = vec![m8_part("part-a.parquet", 10, 5)];
        parts[0].part_id = 1;
        let manifest = m8_manifest(run_id, parts); // fingerprint xxh3:00000000deadbeef
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some("part-a.parquet"))
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        summary.schema_fingerprint = Some("xxh3:1111111111111111".into()); // LIVE evidence
        apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();
        assert_eq!(
            summary.schema_fingerprint.as_deref(),
            Some("xxh3:1111111111111111"),
            "a live fingerprint must never be overwritten by the prior manifest's"
        );
    }

    /// A manifest from a DIFFERENT run_id must be ignored wholesale — resetting
    /// chunk_tasks off a foreign manifest could destroy a healthy resume.
    #[test]
    fn apply_m8_ignores_foreign_manifest() {
        let run_id = "m8run";
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part-a.parquet"), b"AAAAA").unwrap();
        let mut parts = vec![m8_part("part-a.parquet", 10, 5)];
        parts[0].part_id = 1;
        let manifest = m8_manifest("SOMEONE_ELSES_RUN", parts);
        std::fs::write(
            dir.path().join(MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let state_dir = tempfile::tempdir().unwrap();
        let state =
            crate::state::StateStore::open_at_path(&state_dir.path().join("state.db")).unwrap();
        state.insert_chunk_tasks(run_id, &[(0, 10)]).unwrap();
        state.claim_next_chunk_task(run_id).unwrap();
        state
            .complete_chunk_task(run_id, 0, 10, Some("part-a.parquet"))
            .unwrap();

        let plan = m8_plan(dir.path());
        let mut summary =
            crate::pipeline::summary::RunSummary::stub_for_testing(run_id, String::from("orders"));
        let stats = apply_m8_resume_decisions(&state, run_id, &plan, &mut summary).unwrap();

        assert_eq!(
            stats,
            M8Stats::default(),
            "foreign manifest: no action at all"
        );
        assert!(
            summary.manifest_parts.is_empty(),
            "no hydration from a foreign manifest"
        );
        assert_eq!(
            state.list_chunk_tasks_for_run(run_id).unwrap()[0].status,
            "completed",
            "no task reset off a foreign manifest"
        );
    }

    #[test]
    fn m8stats_aggregates_default_to_zero() {
        // Pinning the wire shape — adding fields must not change the
        // existing zero-valued meaning of each counter.
        let s = M8Stats::default();
        assert_eq!(s.skipped, 0);
        assert_eq!(s.reset_for_rewrite, 0);
        assert_eq!(s.reset_for_quarantine, 0);
        assert_eq!(s.orphan_parts, 0);
    }

    /// ADR-0012 M9 — `quarantine_move` is best-effort at the executor
    /// site: a successful `Destination::move` lands the object under
    /// `_quarantine/<run_id>/` and bumps `quarantined_moved`; a failing
    /// move is swallowed into `quarantine_move_failures` and never panics
    /// or bails ("never bail on a quarantine failure").  The decision
    /// matrix that *produces* the Quarantine verdict is covered in
    /// `trust_artifacts_integration` §27; this pins the move half.
    #[test]
    fn quarantine_move_is_best_effort_on_success_and_failure() {
        use crate::destination::{
            Destination, DestinationCapabilities, WriteCommitProtocol, WriteOutcome,
        };
        use std::path::Path;
        use std::sync::Mutex;

        struct MoveMock {
            fail: bool,
            calls: Mutex<Vec<(String, String)>>,
        }
        impl Destination for MoveMock {
            fn write(&self, _p: &Path, _k: &str) -> Result<WriteOutcome> {
                unreachable!("quarantine_move must never call write")
            }
            fn capabilities(&self) -> DestinationCapabilities {
                DestinationCapabilities {
                    commit_protocol: WriteCommitProtocol::Atomic,
                    idempotent_overwrite: true,
                    retry_safe: false,
                    partial_write_risk: true,
                }
            }
            fn r#move(&self, from: &str, to: &str) -> Result<()> {
                self.calls
                    .lock()
                    .unwrap()
                    .push((from.to_string(), to.to_string()));
                if self.fail {
                    anyhow::bail!("simulated move failure")
                }
                Ok(())
            }
        }

        // Success: moved under `_quarantine/<run_id>/`, credited as moved.
        let ok = MoveMock {
            fail: false,
            calls: Mutex::new(Vec::new()),
        };
        let mut stats = M8Stats::default();
        quarantine_move(
            &ok,
            "part-000003.parquet",
            "run_xyz",
            "public.orders",
            &mut stats,
        );
        assert_eq!(stats.quarantined_moved, 1);
        assert_eq!(stats.quarantine_move_failures, 0);
        assert_eq!(
            ok.calls.lock().unwrap().as_slice(),
            &[(
                "part-000003.parquet".to_string(),
                format!("{QUARANTINE_PREFIX}/run_xyz/part-000003.parquet"),
            )],
            "successful move targets _quarantine/<run_id>/<original-key>"
        );

        // Failure: swallowed into the failure counter — no panic, no credit.
        let bad = MoveMock {
            fail: true,
            calls: Mutex::new(Vec::new()),
        };
        let mut stats = M8Stats::default();
        quarantine_move(
            &bad,
            "orphan.parquet",
            "run_xyz",
            "public.orders",
            &mut stats,
        );
        assert_eq!(stats.quarantined_moved, 0);
        assert_eq!(stats.quarantine_move_failures, 1);
    }

    // ── SEC-RED V21: uncapped manifest read can OOM resume/validate ──────
    //
    // resume_m8.rs:136-137 does `head()` then `read()` for `manifest.json`,
    // discarding the `size_bytes` the `head()` already returned, and reads
    // the whole body into a `Vec<u8>` with no upper bound.  An attacker who
    // can write the destination prefix (a shared bucket prefix, a
    // world-writable export dir) can plant a multi-GB `manifest.json`; the
    // next `rivet --resume` / `rivet validate` / `rivet repair` slurps it
    // into memory and OOMs the process.  The same uncapped pattern appears
    // in `repair_cmd.rs:323-324` and `validate_manifest.rs:355-357` — three
    // readers, one missing bound.
    //
    // SECURE behaviour: the capped reader consults the size `head()` reports
    // and BAILS (Err) once the object exceeds the manifest cap, instead of
    // materialising an unbounded body.  Enforced once in
    // `validate_manifest::read_capped` (cap `MANIFEST_MAX_BYTES`) and called by
    // all three readers: resume_m8 here, `repair_cmd`, and `validate_manifest`.
    #[test]
    fn sec_manifest_read_rejects_oversized() {
        // SEC-RED V21: a destination manifest larger than the cap must be
        // refused by the read path, not loaded whole into memory.
        use crate::config::{DestinationConfig, DestinationType};
        use crate::destination::local::LocalDestination;

        // A manifest cap well under the planted body.  64 MiB is the figure
        // the audit cites; the planted object is comfortably above it.
        const MANIFEST_MAX_BYTES: u64 = 64 * 1024 * 1024;

        let dir = tempfile::tempdir().unwrap();
        let dest = LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();

        // Plant a > cap `manifest.json` at the prefix root.  Use a sparse
        // write (seek + single byte) so the test stays fast and doesn't
        // itself allocate the multi-MB body it is guarding against.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::File::create(dir.path().join(MANIFEST_FILENAME)).unwrap();
            f.seek(SeekFrom::Start(MANIFEST_MAX_BYTES + 4096)).unwrap();
            f.write_all(b"}").unwrap();
            f.flush().unwrap();
        }

        // head() reports the oversized length — proving the size was knowable
        // before the read (the discarded `size_bytes` at line 136).
        let meta = dest.head(MANIFEST_FILENAME).unwrap().unwrap();
        assert!(
            meta.size_bytes > MANIFEST_MAX_BYTES,
            "precondition: planted manifest must exceed the cap"
        );

        // SECURE: the capped reader must BAIL rather than read the whole body.
        let result = read_capped(&dest, MANIFEST_FILENAME, MANIFEST_MAX_BYTES);
        assert!(
            result.is_err(),
            "oversized manifest.json must be rejected by the capped read path, \
             not materialised into memory (V21 DoS)"
        );
    }

    /// Cross-check: the in-memory `by_file` index this module builds is
    /// what makes the matrix → chunk_task lookup O(1).  Smoke-test the
    /// two corner cases (missing file_name; multiple chunks).
    #[test]
    fn by_file_index_skips_chunks_without_file_name() {
        let tasks = [
            ChunkTaskInfo {
                chunk_index: 0,
                start_key: "0".into(),
                end_key: "100".into(),
                status: "completed".into(),
                attempts: 1,
                last_error: None,
                rows_written: Some(100),
                file_name: Some("part-000001.parquet".into()),
            },
            ChunkTaskInfo {
                chunk_index: 1,
                start_key: "100".into(),
                end_key: "200".into(),
                status: "pending".into(),
                attempts: 0,
                last_error: None,
                rows_written: None,
                file_name: None, // never wrote a file
            },
            ChunkTaskInfo {
                chunk_index: 2,
                start_key: "200".into(),
                end_key: "300".into(),
                status: "completed".into(),
                attempts: 1,
                last_error: None,
                rows_written: Some(50),
                file_name: Some("part-000003.parquet".into()),
            },
        ];
        let by_file: HashMap<&str, &ChunkTaskInfo> = tasks
            .iter()
            .filter_map(|t| t.file_name.as_deref().map(|n| (n, t)))
            .collect();
        assert_eq!(by_file.len(), 2);
        assert_eq!(by_file["part-000001.parquet"].chunk_index, 0);
        assert_eq!(by_file["part-000003.parquet"].chunk_index, 2);
        assert!(!by_file.contains_key("part-000002.parquet"));
    }
}
