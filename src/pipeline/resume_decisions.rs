//! **Layer: Planning** (decision-only, no I/O)
//!
//! Pure decision logic for ADR-0012 M8 — "given the prior manifest and the
//! current destination listing, what should resume do with each part?".
//!
//! No I/O, no state reads, no `Destination` calls.  The caller (in the
//! Execution layer) gathers the inputs (manifest body via `dest.read`,
//! prefix listing via `dest.list_prefix`), passes them in, and consumes
//! the resulting `ResumePlan`.  This separation lets every row of the M8
//! decision matrix be unit-tested without spinning up a destination.
//!
//! This module is intentionally side-effect-free: it consumes a `RunManifest`
//! plus a `Vec<ObjectMeta>` listing of the destination prefix and returns a
//! decision per part name (plus a separate decision per untracked surplus).
//! The wiring that actually executes the decisions — skipping a chunk task,
//! moving an object to `_quarantine/`, recording a "rewrite" — lives in
//! [`pipeline::chunked`] and [`pipeline::manifest_writer`] respectively.
//!
//! Keeping this layer pure means:
//! - Every row of the M8 decision matrix is unit-testable without I/O.
//! - The chunked executor can mock the listing for tests of its own logic.
//! - Future backends (resume against a state snapshot from the journal,
//!   say) can drive the same matrix without re-implementing it.

// Phase C-β ships the pure decision matrix; the chunked-resume wiring that
// consumes a `ResumePlan` to actually skip / quarantine / rewrite parts is
// the next step (C-γ / C-δ).  Until then, the binary doesn't reach these
// items at runtime — only the lib-side integration tests do.  Allow the
// dead-code warnings on the bin-target compilation rather than tagging
// every individual item.
#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::destination::ObjectMeta;
use crate::manifest::RunManifest;
use crate::pipeline::manifest_reconcile::{PartPresence, reconcile_manifest_against_listing};

/// What `--resume` should do with a single part on the next run.
///
/// The variants mirror ADR-0012 M8's decision table 1:1.  The names match
/// the table column "Decision" so a reviewer can grep both ways.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeDecision {
    /// Manifest entry exists, object exists, size matches, fingerprint
    /// matches → already committed; resume must NOT re-export this part.
    Skip,
    /// Manifest entry exists but the object is missing at the destination
    /// (e.g. someone deleted it; partial bucket lifecycle policy ate it;
    /// the prior run never finished its M1 part-write).  Resume re-runs
    /// the corresponding chunk so the part is rewritten.
    Rewrite,
    /// Manifest entry exists, object exists, but `size_bytes` differs
    /// from the recorded value (or the listing surfaced a fingerprint
    /// drift the caller decoded into the args).  M9 — move the object
    /// to the quarantine prefix and re-export the chunk.
    Quarantine { reason: QuarantineReason },
}

/// What to do with an object that exists at the destination but has no
/// manifest entry pointing at it.  Distinct from [`ResumeDecision`] because
/// the carrier is "object key" rather than "part_id".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UntrackedDecision {
    /// Move to `<prefix>/_quarantine/<run_id>/<original-name>` (best-effort).
    Quarantine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineReason {
    /// `size_bytes` on the destination differs from the manifest entry.
    SizeMismatch,
    /// The object's content MD5 (from the listing metadata) diverged from
    /// the manifest's — corruption at the same size, caught with no download.
    /// Produced when the reconciliation surfaces a `ChecksumMismatch`.
    FingerprintMismatch,
}

/// Aggregate output of one matrix evaluation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResumePlan {
    /// Per-part decisions, keyed by `manifest.parts[].path`.  Sorted so
    /// callers iterating in deterministic order don't need to sort
    /// themselves.
    pub per_part: BTreeMap<String, PartDecision>,
    /// Surplus objects under the prefix that are not referenced by any
    /// committed manifest entry (M9 untracked).  Keyed by relative key.
    pub untracked: BTreeMap<String, UntrackedDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartDecision {
    pub part_id: u32,
    pub decision: ResumeDecision,
}

impl ResumePlan {
    /// Number of parts whose decision is `Skip` — the resume work-saving
    /// metric an operator wants to see in the run report.
    pub fn skipped(&self) -> usize {
        self.per_part
            .values()
            .filter(|d| matches!(d.decision, ResumeDecision::Skip))
            .count()
    }

    /// Number of parts the executor must re-export.
    pub fn rewrites(&self) -> usize {
        self.per_part
            .values()
            .filter(|d| matches!(d.decision, ResumeDecision::Rewrite))
            .count()
    }

    /// Number of parts marked for quarantine (size or fingerprint drift).
    pub fn quarantines(&self) -> usize {
        self.per_part
            .values()
            .filter(|d| matches!(d.decision, ResumeDecision::Quarantine { .. }))
            .count()
    }
}

/// Apply ADR-0012 M8's decision matrix.
///
/// `manifest` is the prior run's manifest read from the destination (post-
/// `_SUCCESS` or pre-`_SUCCESS` — both are valid inputs; the gate logic
/// above this module already covered the "_SUCCESS without --force" case).
/// `listing` is the recursive listing of the destination prefix; entries
/// whose key starts with [`crate::manifest::QUARANTINE_PREFIX`] are
/// silently filtered (they are evidence of prior quarantine moves).
///
/// The function does **not** mutate either input.  A chunked executor may
/// run this once at the start of a resume to populate its skip-set.
pub fn build_resume_plan(manifest: &RunManifest, listing: &[ObjectMeta]) -> ResumePlan {
    // Resume reads the destination at the prefix root (`list_prefix("")`), so
    // part keys are the bare `part.path` — `manifest_dir = ""`.
    let rec = reconcile_manifest_against_listing(manifest, listing, "");

    let mut plan = ResumePlan::default();
    for check in rec.per_part {
        // Resume time trusts a size match (ADR-0012 M3 — the fingerprint
        // compare is `--validate --deep`, deliberately not paid on the hot
        // resume path).  Missing → re-export; size drift → quarantine + rewrite.
        let decision = match check.presence {
            PartPresence::Present { .. } => ResumeDecision::Skip,
            PartPresence::Missing => ResumeDecision::Rewrite,
            PartPresence::SizeMismatch { .. } => ResumeDecision::Quarantine {
                reason: QuarantineReason::SizeMismatch,
            },
            // Content drift at the same size — quarantine the corrupt object
            // and re-export, same as a size mismatch.
            PartPresence::ChecksumMismatch { .. } => ResumeDecision::Quarantine {
                reason: QuarantineReason::FingerprintMismatch,
            },
        };
        plan.per_part.insert(
            check.path,
            PartDecision {
                part_id: check.part_id,
                decision,
            },
        );
    }
    for obj in rec.untracked {
        plan.untracked
            .insert(obj.key, UntrackedDecision::Quarantine);
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        MANIFEST_FILENAME, MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource,
        ManifestStatus, PartStatus, RunManifest, SUCCESS_FILENAME,
    };

    fn part(part_id: u32, path: &str, size: u64) -> ManifestPart {
        ManifestPart {
            part_id,
            path: path.into(),
            rows: 100,
            size_bytes: size,
            content_fingerprint: format!("xxh3:{:016x}", part_id as u64),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }
    }

    fn manifest(parts: Vec<ManifestPart>) -> RunManifest {
        let row_count = parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .map(|p| p.rows)
            .sum();
        let part_count = parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .count() as u32;
        RunManifest {
            mode: "batch".to_string(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r1".into(),
            export_name: "public.orders".into(),
            started_at: "2026-05-21T12:00:00Z".into(),
            finished_at: "2026-05-21T12:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0000000000000000".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    fn obj(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            key: key.into(),
            size_bytes: size,
            content_md5: None,
        }
    }

    // ── M8 decision matrix rows ────────────────────────────────────────

    /// Row: manifest yes + obj yes + size match → skip (committed).
    #[test]
    fn part_present_with_matching_size_is_skipped() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![obj("part-000001.parquet", 4096)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 1);
        assert_eq!(plan.rewrites(), 0);
        assert_eq!(plan.quarantines(), 0);
        assert_eq!(
            plan.per_part["part-000001.parquet"].decision,
            ResumeDecision::Skip
        );
    }

    /// Row: manifest yes + obj yes + size mismatch → quarantine (M9).
    #[test]
    fn part_present_with_size_drift_is_quarantined() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![obj("part-000001.parquet", 9999)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.quarantines(), 1);
        assert_eq!(
            plan.per_part["part-000001.parquet"].decision,
            ResumeDecision::Quarantine {
                reason: QuarantineReason::SizeMismatch
            }
        );
    }

    /// Row: manifest yes + obj missing → rewrite (lost).
    #[test]
    fn part_missing_from_destination_is_rewritten() {
        let m = manifest(vec![
            part(1, "part-000001.parquet", 4096),
            part(2, "part-000002.parquet", 8192),
        ]);
        let listing = vec![obj("part-000001.parquet", 4096)]; // 2 missing
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 1);
        assert_eq!(plan.rewrites(), 1);
        assert_eq!(plan.quarantines(), 0);
        assert_eq!(
            plan.per_part["part-000002.parquet"].decision,
            ResumeDecision::Rewrite
        );
    }

    /// Row: manifest no + obj yes → quarantine (untracked).
    #[test]
    fn untracked_object_is_quarantined() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![obj("part-000001.parquet", 4096), obj("rogue.parquet", 99)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 1);
        assert_eq!(plan.untracked.len(), 1);
        assert_eq!(
            plan.untracked["rogue.parquet"],
            UntrackedDecision::Quarantine
        );
    }

    /// Row: manifest no + obj no → new (write).  Modeled by absence —
    /// `build_resume_plan` returns no per_part entry for a part the
    /// caller hasn't yet committed; the caller's chunked executor knows
    /// to write it because no decision says skip.
    #[test]
    fn new_part_not_yet_committed_has_no_decision_entry() {
        let m = manifest(vec![]); // no parts committed yet
        let listing: Vec<ObjectMeta> = vec![]; // nothing on destination
        let plan = build_resume_plan(&m, &listing);
        assert!(plan.per_part.is_empty());
        assert!(plan.untracked.is_empty());
    }

    // ── trust-artifact filtering ───────────────────────────────────────

    #[test]
    fn manifest_and_success_themselves_are_not_flagged_as_untracked() {
        // The decision matrix runs over data parts, not over the trust
        // contract files themselves.  Otherwise we'd loop trying to
        // quarantine our own manifest.
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![
            obj("part-000001.parquet", 4096),
            obj(MANIFEST_FILENAME, 600),
            obj(SUCCESS_FILENAME, 22),
        ];
        let plan = build_resume_plan(&m, &listing);
        assert!(
            plan.untracked.is_empty(),
            "trust-contract files must not be 'untracked'"
        );
    }

    #[test]
    fn objects_under_quarantine_prefix_are_silently_ignored() {
        // Already-quarantined artifacts must not be re-quarantined or
        // confused with new untracked drift.  The prefix is reserved.
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![
            obj("part-000001.parquet", 4096),
            obj("_quarantine/r0/old.parquet", 1234),
        ];
        let plan = build_resume_plan(&m, &listing);
        assert!(plan.untracked.is_empty());
    }

    // ── status: only Committed parts get decisions ─────────────────────

    #[test]
    fn quarantined_manifest_entries_do_not_get_resume_decisions() {
        // `manifest.parts` may carry old quarantined entries for audit
        // (M9 leaves them in the manifest as evidence).  Resume should
        // not act on them — they belong to the prior run's history.
        let mut quarantined_part = part(2, "old-corrupt.parquet", 999);
        quarantined_part.status = PartStatus::Quarantined;
        let m = manifest(vec![part(1, "part-000001.parquet", 4096), quarantined_part]);
        let listing = vec![obj("part-000001.parquet", 4096)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(
            plan.per_part.len(),
            1,
            "only the committed entry gets a decision"
        );
        assert!(!plan.per_part.contains_key("old-corrupt.parquet"));
    }

    // ── multiple parts, mixed outcomes ─────────────────────────────────

    #[test]
    fn mixed_run_produces_per_decision_counts() {
        // Realistic resume: some parts already committed, some lost,
        // some drifted, plus surplus.  Verifies the aggregate counters.
        let m = manifest(vec![
            part(1, "p1.parquet", 100), // present + matching → skip
            part(2, "p2.parquet", 200), // missing → rewrite
            part(3, "p3.parquet", 300), // size drift → quarantine
            part(4, "p4.parquet", 400), // present + matching → skip
        ]);
        let listing = vec![
            obj("p1.parquet", 100),
            obj("p3.parquet", 9999), // drift
            obj("p4.parquet", 400),
            obj("rogue.parquet", 50), // untracked
        ];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 2);
        assert_eq!(plan.rewrites(), 1);
        assert_eq!(plan.quarantines(), 1);
        assert_eq!(plan.untracked.len(), 1);
        assert_eq!(plan.per_part["p1.parquet"].decision, ResumeDecision::Skip);
        assert_eq!(
            plan.per_part["p2.parquet"].decision,
            ResumeDecision::Rewrite
        );
        assert!(matches!(
            plan.per_part["p3.parquet"].decision,
            ResumeDecision::Quarantine { .. }
        ));
        assert_eq!(plan.per_part["p4.parquet"].decision, ResumeDecision::Skip);
    }

    // ── determinism ────────────────────────────────────────────────────

    #[test]
    fn build_resume_plan_is_deterministic_for_same_inputs() {
        // ADR-0012 M8: "Given the same destination prefix and the same
        // source/cursor state, --resume makes identical decisions on
        // every run."  The pure layer makes that easy to test.
        let m = manifest(vec![part(1, "p1.parquet", 100), part(2, "p2.parquet", 200)]);
        let listing = vec![obj("p1.parquet", 100), obj("p2.parquet", 9999)];
        let plan1 = build_resume_plan(&m, &listing);
        let plan2 = build_resume_plan(&m, &listing);
        assert_eq!(plan1, plan2);
    }

    #[test]
    fn build_resume_plan_is_listing_order_insensitive() {
        // The destination listing has unspecified order; the plan must
        // not depend on which order opendal/local-walk surfaces objects.
        let m = manifest(vec![part(1, "p1.parquet", 100), part(2, "p2.parquet", 200)]);
        let listing_a = vec![obj("p1.parquet", 100), obj("p2.parquet", 200)];
        let listing_b = vec![obj("p2.parquet", 200), obj("p1.parquet", 100)];
        let plan_a = build_resume_plan(&m, &listing_a);
        let plan_b = build_resume_plan(&m, &listing_b);
        assert_eq!(plan_a, plan_b);
    }
}
