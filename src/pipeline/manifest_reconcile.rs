//! **Layer: Planning** (pure, no I/O)
//!
//! The one place that compares a [`RunManifest`] against what a destination
//! prefix actually holds.  Both consumers — destination verification
//! (`--validate`, [`crate::pipeline::validate_manifest`]) and chunked resume
//! ([`crate::pipeline::resume_decisions::build_resume_plan`]) — used to carry
//! their own near-identical "for each committed part: present? size match?"
//! walk plus their own untracked-surplus filter.  This module owns that walk
//! once; the consumers are thin mappers from [`PartPresence`] to their own
//! verdict vocabulary (`Failure` for verify, `ResumeDecision` for resume).
//!
//! **Why size only.** A destination listing yields [`ObjectMeta`] = `{key,
//! size_bytes}` — no etag, no content hash.  So `size_bytes` is the strongest
//! signal derivable *without fetching the object*.  Row- and fingerprint-aware
//! checks (the manifest carries both `rows` and `content_fingerprint`) require
//! downloading each part and therefore live in a separate, opt-in I/O step
//! (`--validate --deep`), not here.  Keeping this function pure is what lets
//! every row of the matrix be unit-tested without a destination.

use std::collections::BTreeMap;

use crate::destination::ObjectMeta;
use crate::manifest::{
    DOCTOR_PROBE_FILENAME, MANIFEST_FILENAME, PartStatus, QUARANTINE_PREFIX, RunManifest,
    SUCCESS_FILENAME, join_key,
};

/// Presence verdict for one committed manifest part against the listing.
///
/// Size-only by construction (see module docs).  `Present` means the object
/// exists at the recorded `size_bytes`; it does **not** assert row count or
/// content — that is the `--deep` tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartPresence {
    /// Object exists and its size matches the manifest.
    Present,
    /// Manifest declares the part but no object exists at its key.
    Missing,
    /// Object exists but its size differs from the manifest's record.
    SizeMismatch { expected: u64, actual: u64 },
}

/// One committed part's reconciliation outcome.  `path` is manifest-relative
/// (the manifest's `part.path`), independent of `manifest_dir`, so consumers
/// render the same identifier the manifest does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartCheck {
    pub part_id: u32,
    pub path: String,
    pub presence: PartPresence,
}

/// The full outcome of comparing a manifest to a destination listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reconciliation {
    /// Per committed part, in manifest order.
    pub per_part: Vec<PartCheck>,
    /// Surplus objects under the prefix that no committed part claims and
    /// that are not Rivet-internal sidecars (manifest / `_SUCCESS` / doctor
    /// probe / quarantine).  Carries `size_bytes` because verify reports it.
    pub untracked: Vec<ObjectMeta>,
}

/// Compare `manifest` against the destination `listing` (keys joined under
/// `manifest_dir`).  Pure: no I/O, no mutation of either input.
///
/// - Committed parts are classified [`PartPresence::Present`] / `Missing` /
///   `SizeMismatch`.  Quarantined manifest entries are audit-only and skipped.
/// - Untracked surplus is every listed key that is not a committed part, the
///   manifest, the `_SUCCESS` marker, the doctor probe, or under the
///   `_quarantine/` prefix.
pub fn reconcile_manifest_against_listing(
    manifest: &RunManifest,
    listing: &[ObjectMeta],
    manifest_dir: &str,
) -> Reconciliation {
    // Index the listing by key, dropping the quarantine prefix outright — its
    // contents are audit artifacts and must never match a part or be flagged
    // as untracked.
    let listed: BTreeMap<&str, &ObjectMeta> = listing
        .iter()
        .filter(|m| !m.key.contains(QUARANTINE_PREFIX))
        .map(|m| (m.key.as_str(), m))
        .collect();

    let mut out = Reconciliation::default();
    let mut claimed: Vec<String> = Vec::new();

    for part in &manifest.parts {
        if part.status != PartStatus::Committed {
            continue; // quarantined entries carry no presence verdict
        }
        let key = join_key(manifest_dir, &part.path);
        let presence = match listed.get(key.as_str()) {
            None => PartPresence::Missing,
            Some(meta) if meta.size_bytes == part.size_bytes => PartPresence::Present,
            Some(meta) => PartPresence::SizeMismatch {
                expected: part.size_bytes,
                actual: meta.size_bytes,
            },
        };
        claimed.push(key);
        out.per_part.push(PartCheck {
            part_id: part.part_id,
            path: part.path.clone(),
            presence,
        });
    }

    // Untracked surplus: listed keys no committed part claimed, minus the
    // Rivet-internal sidecars.
    let manifest_key = join_key(manifest_dir, MANIFEST_FILENAME);
    let success_key = join_key(manifest_dir, SUCCESS_FILENAME);
    for (key, meta) in &listed {
        if claimed.iter().any(|c| c == key) {
            continue;
        }
        if *key == manifest_key || *key == success_key {
            continue;
        }
        if key.rsplit('/').next() == Some(DOCTOR_PROBE_FILENAME) {
            continue;
        }
        out.untracked.push((*meta).clone());
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        ManifestDestination, ManifestPart, ManifestSource, ManifestStatus, RunManifest,
        MANIFEST_VERSION,
    };

    fn part(id: u32, size: u64) -> ManifestPart {
        ManifestPart {
            part_id: id,
            path: format!("part-{id:06}.parquet"),
            rows: 10,
            size_bytes: size,
            content_fingerprint: "xxh3:0".into(),
            status: PartStatus::Committed,
        }
    }

    fn manifest(parts: Vec<ManifestPart>) -> RunManifest {
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: "r".into(),
            export_name: "e".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource { engine: "pg".into(), schema: None, table: None },
            destination: ManifestDestination { kind: "local".into(), uri: "file:///x".into() },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: parts.iter().map(|p| p.rows).sum(),
            part_count: parts.len() as u32,
            parts,
        }
    }

    fn obj(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta { key: key.into(), size_bytes: size }
    }

    #[test]
    fn present_missing_and_size_mismatch_are_classified() {
        let m = manifest(vec![part(0, 100), part(1, 200), part(2, 300)]);
        let listing = vec![
            obj("part-000000.parquet", 100), // present
            obj("part-000002.parquet", 999), // size drift
            // part-000001 absent → missing
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert_eq!(rec.per_part[0].presence, PartPresence::Present);
        assert_eq!(rec.per_part[1].presence, PartPresence::Missing);
        assert_eq!(
            rec.per_part[2].presence,
            PartPresence::SizeMismatch { expected: 300, actual: 999 }
        );
        assert!(rec.untracked.is_empty());
    }

    #[test]
    fn surplus_objects_are_untracked_but_sidecars_and_quarantine_are_not() {
        let m = manifest(vec![part(0, 100)]);
        let listing = vec![
            obj("part-000000.parquet", 100),
            obj(MANIFEST_FILENAME, 50),
            obj(SUCCESS_FILENAME, 20),
            obj(DOCTOR_PROBE_FILENAME, 1),
            obj("_quarantine/r/old.parquet", 100),
            obj("stray.parquet", 7), // the only real surplus
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert_eq!(rec.untracked.len(), 1);
        assert_eq!(rec.untracked[0].key, "stray.parquet");
    }

    #[test]
    fn manifest_dir_namespaces_part_and_sidecar_keys() {
        let m = manifest(vec![part(0, 100)]);
        let listing = vec![
            obj("sub/run/part-000000.parquet", 100),
            obj("sub/run/manifest.json", 50),
            obj("sub/run/foreign.parquet", 9),
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "sub/run");
        assert_eq!(rec.per_part[0].presence, PartPresence::Present);
        assert_eq!(rec.untracked.len(), 1);
        assert_eq!(rec.untracked[0].key, "sub/run/foreign.parquet");
    }

    #[test]
    fn quarantined_manifest_entries_get_no_presence_verdict() {
        let mut p = part(0, 100);
        p.status = PartStatus::Quarantined;
        let m = manifest(vec![p, part(1, 200)]);
        let listing = vec![obj("part-000001.parquet", 200)];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert_eq!(rec.per_part.len(), 1, "only the committed part is checked");
        assert_eq!(rec.per_part[0].part_id, 1);
    }
}
