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
        Ok(Some(_)) => match dest.read(MANIFEST_FILENAME) {
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
        let task = match by_file.get(path.as_str()) {
            Some(t) => *t,
            None => {
                stats.orphan_parts += 1;
                continue;
            }
        };
        match decision.decision {
            ResumeDecision::Skip => {
                stats.skipped += 1;
                // Hydrate the summary so the end-of-resume manifest carries
                // this committed part too.  Clone the prior manifest's part
                // verbatim — the part_id, content_fingerprint, rows, and
                // size_bytes are all from the previous successful write
                // and remain valid for the destination object that's still
                // there.
                if let Some(p) = manifest_part_by_path.get(path.as_str()) {
                    summary.manifest_parts.push((*p).clone());
                }
            }
            ResumeDecision::Rewrite => {
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
    for key in resume_plan.untracked.keys() {
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
        resume_plan.untracked.len(),
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
