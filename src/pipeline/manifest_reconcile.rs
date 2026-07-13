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
    SUCCESS_FILENAME, is_run_unique_manifest_name, join_key,
};

/// Normalise a stored MD5 to its 16 raw digest bytes so encodings from
/// different backends compare equal.  Handles GCS's base64 `md5Hash` and S3's
/// hex single-part ETag.  Returns `None` for anything that is not a plain
/// 16-byte MD5 — an S3 multipart composite ETag (`<hash>-<N>`), an empty or
/// legacy value — signalling "not comparable, fall back to size-only".
pub(crate) fn md5_digest_bytes(s: &str) -> Option<[u8; 16]> {
    // Hex (S3 single-part ETag): exactly 32 hex chars.
    if s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        let mut out = [0u8; 16];
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
        }
        return Some(out);
    }
    // Base64 (GCS md5Hash): must decode to exactly 16 bytes.
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD.decode(s).ok()?;
    decoded.try_into().ok()
}

/// Presence verdict for one committed manifest part against the listing.
///
/// Size-only by construction (see module docs).  `Present` means the object
/// exists at the recorded `size_bytes`; it does **not** assert row count or
/// content — that is the `--deep` tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartPresence {
    /// Object exists and its size matches the manifest.  `md5_verified` is
    /// `true` only when both the manifest and the listing carried a comparable
    /// MD5 and it matched — i.e. the content (not just the size) was confirmed.
    /// `false` means the size matched but no MD5 was comparable (a backend that
    /// doesn't surface one, a streamed/large part, a legacy manifest) — so the
    /// part is size-only verified.
    Present { md5_verified: bool },
    /// Manifest declares the part but no object exists at its key.
    Missing,
    /// Object exists but its size differs from the manifest's record.
    SizeMismatch { expected: u64, actual: u64 },
    /// Object exists at the recorded size, but its content MD5 (from the
    /// listing metadata — GCS `md5Hash`, etc.) differs from the manifest's.
    /// Catches transit / at-rest corruption with **no download**.  Only
    /// produced when both sides carry an MD5; absent either, the part is
    /// `Present` (size-only).
    ChecksumMismatch { expected: String, actual: String },
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
///   manifest-rooted `_quarantine/` directory.
pub fn reconcile_manifest_against_listing(
    manifest: &RunManifest,
    listing: &[ObjectMeta],
    manifest_dir: &str,
) -> Reconciliation {
    // Index the listing by key, dropping the manifest-rooted `_quarantine/`
    // directory outright — its contents are audit artifacts and must never
    // match a part or be flagged as untracked.  Anchored to the directory
    // (the dir's own key, or anything under `<dir>/`), never a substring
    // match: a part legitimately named `*_quarantine*` is an ordinary key.
    let quarantine_dir = join_key(manifest_dir, QUARANTINE_PREFIX);
    let quarantine_subtree = format!("{quarantine_dir}/");
    let listed: BTreeMap<&str, &ObjectMeta> = listing
        .iter()
        .filter(|m| m.key != quarantine_dir && !m.key.starts_with(&quarantine_subtree))
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
            Some(meta) if meta.size_bytes == part.size_bytes => {
                // Size matches.  When both the manifest and the listing carry
                // a comparable MD5, the content must match too — a free
                // no-download integrity check.  Stores encode it differently
                // (GCS `md5Hash` base64; S3 single-part ETag hex), so both
                // sides normalise to raw digest bytes before comparing.
                // Anything that isn't a plain 16-byte MD5 — an S3 multipart
                // composite ETag (`<hash>-<N>`), a streamed Azure block blob
                // with no Content-MD5 (Azure auto-MD5s only a single `Put
                // Blob`; rivet one-shots small parts to get it, streams large
                // ones), a local FS `None`, a legacy value — is not comparable,
                // so the part degrades to size-only.
                // Compare digests only when BOTH sides carry a parsable MD5;
                // any absent/unparsable side → size-only.
                let want = md5_digest_bytes(&part.content_md5);
                let got = meta.content_md5.as_deref().and_then(md5_digest_bytes);
                match (want, got) {
                    (Some(a), Some(b)) if a == b => PartPresence::Present { md5_verified: true },
                    (Some(_), Some(_)) => PartPresence::ChecksumMismatch {
                        expected: part.content_md5.clone(),
                        actual: meta.content_md5.clone().unwrap_or_default(),
                    },
                    _ => PartPresence::Present {
                        md5_verified: false,
                    },
                }
            }
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
        // Run-unique manifest copies (`manifest-<run_id>.json`) are Rivet
        // sidecars written beside the canonical manifest so a cross-run consumer
        // can sum across runs — not foreign data.
        let base = key.rsplit('/').next().unwrap_or("");
        if base == DOCTOR_PROBE_FILENAME || is_run_unique_manifest_name(base) {
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
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        RunManifest,
    };

    fn part(id: u32, size: u64) -> ManifestPart {
        part_md5(id, size, "")
    }

    fn part_md5(id: u32, size: u64, md5: &str) -> ManifestPart {
        ManifestPart {
            part_id: id,
            path: format!("part-{id:06}.parquet"),
            rows: 10,
            size_bytes: size,
            content_fingerprint: "xxh3:0".into(),
            content_md5: md5.into(),
            status: PartStatus::Committed,
        }
    }

    fn manifest(parts: Vec<ManifestPart>) -> RunManifest {
        RunManifest {
            mode: "batch".to_string(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r".into(),
            export_name: "e".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "pg".into(),
                schema: None,
                table: None,
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///x".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: parts.iter().map(|p| p.rows).sum(),
            part_count: parts.len() as u32,
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

    fn obj_md5(key: &str, size: u64, md5: &str) -> ObjectMeta {
        ObjectMeta {
            key: key.into(),
            size_bytes: size,
            content_md5: Some(md5.into()),
        }
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
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
        assert_eq!(rec.per_part[1].presence, PartPresence::Missing);
        assert_eq!(
            rec.per_part[2].presence,
            PartPresence::SizeMismatch {
                expected: 300,
                actual: 999
            }
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
            // Run-unique manifest copies (this run's + a prior run's, sharing the
            // prefix) are sidecars, not surplus.
            obj("manifest-run_001.json", 50),
            obj("manifest-run_000.json", 50),
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
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
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

    // A real MD5 digest in both encodings (verified live: rivet export →
    // GCS md5Hash base64, S3 ETag hex — same 16 bytes).
    const MD5_B64: &str = "9jgqdWB0dO+/XMZGVIiAfA==";
    const MD5_HEX: &str = "f6382a75607474efbf5cc6465488807c";
    const ZEROS_B64: &str = "AAAAAAAAAAAAAAAAAAAAAA=="; // 16 zero bytes, a valid but different digest

    #[test]
    fn md5_mismatch_at_matching_size_is_caught_without_download() {
        // Both sides carry an MD5; the size matches but the digest differs —
        // corruption a size check alone would miss.
        let m = manifest(vec![part_md5(0, 100, MD5_B64), part_md5(1, 100, MD5_B64)]);
        let listing = vec![
            obj_md5("part-000000.parquet", 100, MD5_B64), // match → Present
            obj_md5("part-000001.parquet", 100, ZEROS_B64), // drift → ChecksumMismatch
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        // md5 present on both sides and matching → content confirmed.
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present { md5_verified: true }
        );
        assert!(matches!(
            rec.per_part[1].presence,
            PartPresence::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn md5_compares_across_encodings_gcs_base64_vs_s3_hex() {
        // The S3 bug: manifest stores base64, an S3 listing returns hex ETag —
        // same digest must NOT be a mismatch (regression for the live finding).
        let m = manifest(vec![part_md5(0, 100, MD5_B64)]);
        let rec = reconcile_manifest_against_listing(
            &m,
            &[obj_md5("part-000000.parquet", 100, MD5_HEX)],
            "",
        );
        // base64 manifest vs hex listing of the same digest → still md5-verified.
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present { md5_verified: true }
        );
    }

    #[test]
    fn md5_check_degrades_to_size_only_when_not_comparable() {
        // Manifest has MD5, listing does not (local FS) → Present (size-only).
        let m = manifest(vec![part_md5(0, 100, MD5_B64)]);
        let rec = reconcile_manifest_against_listing(&m, &[obj("part-000000.parquet", 100)], "");
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
        // Listing carries an S3 multipart composite ETag (`<hash>-<N>`), which
        // is not a plain MD5 → not comparable → Present (size-only).
        let composite = format!("{MD5_HEX}-3");
        let rec2 = reconcile_manifest_against_listing(
            &m,
            &[obj_md5("part-000000.parquet", 100, &composite)],
            "",
        );
        assert_eq!(
            rec2.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
    }

    // ROAST-RED quarantine-anchor: the listing filter drops any key CONTAINING
    // the "_quarantine" substring anywhere, instead of only keys under the
    // manifest-rooted `_quarantine/` directory, so a committed part whose file
    // name merely contains the substring is reported Missing (fatal in verify,
    // duplicate re-export in resume).
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_part_named_like_quarantine_is_matched_not_missing() {
        // Contract (resume_decisions.rs): only entries whose key STARTS WITH
        // QUARANTINE_PREFIX — i.e. the `_quarantine/` audit directory — are
        // skipped.  An export literally named `orders_quarantine_audit`
        // produces ordinary committed parts that must match the listing.
        let mut p = part(0, 100);
        p.path = "orders_quarantine_audit-000000.parquet".into();
        let m = manifest(vec![p]);
        let listing = vec![obj("orders_quarantine_audit-000000.parquet", 100)];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            },
            "committed part `orders_quarantine_audit-000000.parquet` was \
             classified {:?} instead of Present: the quarantine filter \
             matched the `_quarantine` substring in the file name, not the \
             `_quarantine/` directory",
            rec.per_part[0].presence
        );
    }

    #[test]
    fn key_exactly_equal_to_quarantine_dir_is_filtered() {
        // Some object stores list a zero-byte placeholder object for the
        // directory itself (key `_quarantine`, no slash).  It is a Rivet
        // audit artifact, not untracked surplus.
        let m = manifest(vec![part(0, 100)]);
        let listing = vec![obj("part-000000.parquet", 100), obj(QUARANTINE_PREFIX, 0)];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
        assert!(rec.untracked.is_empty());
    }

    #[test]
    fn nested_quarantine_files_are_filtered_at_any_depth() {
        let m = manifest(vec![part(0, 100)]);
        let listing = vec![
            obj("part-000000.parquet", 100),
            obj("_quarantine/r/deep/nested/part-000099.parquet", 7),
            // Substring in the file name AND under the directory → the
            // directory anchor, not the name, decides: filtered.
            obj("_quarantine/r/_quarantine.parquet", 7),
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert!(rec.untracked.is_empty());
    }

    #[test]
    fn root_file_named_quarantine_dot_parquet_is_an_ordinary_key() {
        // `_quarantine.parquet` is NOT under `_quarantine/` — it behaves like
        // any other key: matched when a committed part claims it, reported
        // untracked when nothing does (the old substring filter silently
        // hid such surplus).
        let mut p = part(0, 100);
        p.path = "_quarantine.parquet".into();
        let m = manifest(vec![p]);
        let listing = vec![
            obj("_quarantine.parquet", 100),
            obj("stray_quarantine_export.parquet", 7),
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "");
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
        assert_eq!(rec.untracked.len(), 1);
        assert_eq!(rec.untracked[0].key, "stray_quarantine_export.parquet");
    }

    #[test]
    fn quarantine_anchor_respects_manifest_dir_prefix() {
        // Prefixed destination: the quarantine directory lives under the
        // manifest dir (`sub/run/_quarantine/…`), and a part whose name
        // contains the substring still matches its prefixed key.
        let mut p = part(0, 100);
        p.path = "orders_quarantine-000000.parquet".into();
        let m = manifest(vec![p]);
        let listing = vec![
            obj("sub/run/orders_quarantine-000000.parquet", 100),
            obj("sub/run/_quarantine/r/old.parquet", 9),
        ];
        let rec = reconcile_manifest_against_listing(&m, &listing, "sub/run");
        assert_eq!(
            rec.per_part[0].presence,
            PartPresence::Present {
                md5_verified: false
            }
        );
        assert!(rec.untracked.is_empty());
    }
}
