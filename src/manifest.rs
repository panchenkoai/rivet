//! **Layer: Trust contract**
//!
//! Public JSON manifest written next to every cloud-or-local-file run's
//! output.  Defines the wire schema and the in-memory builder; the actual
//! writer (atomic rename / atomic PUT) lives next to the destination
//! implementations.
//!
//! Invariants are documented in [`docs/adr/0012-cloud-manifest-contract.md`].
//! This module owns *only* the data types and a tiny set of pure helpers —
//! ordering, atomicity, and `_SUCCESS` semantics belong to the writer.
//!
//! The manifest is read by:
//! - `--resume` (decision matrix M8: skip / rewrite / quarantine)
//! - `--validate` (M5: every listed part exists at recorded size)
//! - `--reconcile` (manifest row counts vs source `COUNT(*)`)
//! - the run report (informational; not a verdict source)
//!
//! Forward compatibility: callers MUST ignore unknown fields when reading.
//! Field additions are non-breaking; field removals or type changes require
//! a [`MANIFEST_VERSION`] bump.

// The wire types (RunManifest, ManifestPart, ...) and the writer-side
// helpers (success_marker_body) are already wired into the pipeline; the
// reader-side helpers (validate_self_consistency, committed_rows,
// committed_part_count, parse_success_marker, ManifestInconsistency) ship
// next when `--validate` / `--reconcile` learn to inspect the manifest.
// Mark the whole module as dead-code-tolerant until then so the bin crate
// (which doesn't compile tests) stays clean.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// Current manifest schema version.  See ADR-0012 §Manifest schema.
pub const MANIFEST_VERSION: u32 = 1;

/// File name of the manifest at the destination prefix.
pub const MANIFEST_FILENAME: &str = "manifest.json";

/// File name of the success marker.  Written *after* the manifest per M2;
/// its presence implies M5 (every listed part exists at recorded size).
pub const SUCCESS_FILENAME: &str = "_SUCCESS";

/// Prefix under which untracked / corrupt parts are moved on resume (M9).
/// Layout: `<prefix>/_quarantine/<run_id>/<original-name>`.
pub const QUARANTINE_PREFIX: &str = "_quarantine";

/// Writability probe `rivet doctor` drops at the destination prefix.  It is a
/// Rivet-internal sidecar (like [`MANIFEST_FILENAME`] / [`SUCCESS_FILENAME`]),
/// so the manifest-aware `--validate` pass must not flag it as an untracked
/// foreign object when a run follows a `doctor` against the same prefix.
pub const DOCTOR_PROBE_FILENAME: &str = ".rivet_doctor_probe";

/// Join a manifest-relative key (e.g. a part `path`, [`MANIFEST_FILENAME`])
/// onto a destination sub-directory.  An empty `dir` returns `key` unchanged
/// — the common case, since production callers pass `""` (the manifest lives
/// at the prefix root).  Shared by the destination-verification and
/// resume-reconciliation paths so both speak the same key namespace.
pub fn join_key(dir: &str, key: &str) -> String {
    let dir = dir.trim_end_matches('/');
    if dir.is_empty() {
        key.to_string()
    } else {
        format!("{dir}/{key}")
    }
}

/// Compute the body of the `_SUCCESS` marker for a given serialized manifest.
///
/// Format: a single line `"xxh3:<16-hex>\n"`.  ADR-0012 M2 — `_SUCCESS`
/// carries the manifest fingerprint so an orchestrator can detect manifest
/// changes (rerun, resume that completed, repair) with a cheap `GET _SUCCESS`
/// instead of refetching the full manifest body.
///
/// `manifest_bytes` must be the exact bytes that were written to `manifest.json`
/// — usually the result of `serde_json::to_vec_pretty(&RunManifest)`.  The
/// caller is responsible for using the same bytes for both writes; computing
/// the fingerprint from a re-serialized struct would risk encoding drift
/// (key ordering, whitespace) producing a different hash.
pub fn success_marker_body(manifest_bytes: &[u8]) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    format!("xxh3:{:016x}\n", xxh3_64(manifest_bytes))
}

/// Parse the fingerprint out of a `_SUCCESS` marker body.
///
/// Returns `Some("xxh3:<hex>")` on a well-formed marker, `None` on anything
/// else (empty file, missing prefix, wrong length, non-hex body).  Trailing
/// whitespace and newlines are tolerated to match the on-wire shape produced
/// by [`success_marker_body`].
///
/// Used by `--validate` and by external polling consumers (Airflow sensors,
/// CI checks) to decide whether a cached manifest is still current.
pub fn parse_success_marker(body: &str) -> Option<&str> {
    let trimmed = body.trim_end_matches(|c: char| c.is_ascii_whitespace());
    if trimmed.len() != "xxh3:".len() + 16 {
        return None;
    }
    let (prefix, hex) = trimmed.split_at("xxh3:".len());
    if prefix != "xxh3:" {
        return None;
    }
    if !hex
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return None;
    }
    Some(trimmed)
}

/// Public, stable JSON shape for the run manifest.
///
/// One manifest is written per `run_id` per export.  See ADR-0012 M4
/// (Append-Only Per Run) for the resume-across-interruption story.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunManifest {
    pub manifest_version: u32,
    pub run_id: String,
    pub export_name: String,
    pub started_at: String,
    pub finished_at: String,
    pub status: ManifestStatus,
    pub source: ManifestSource,
    pub destination: ManifestDestination,
    pub format: String,
    pub compression: String,
    /// xxh3 fingerprint of the column schema; see [`crate::state::schema_fingerprint`].
    pub schema_fingerprint: String,
    pub row_count: i64,
    pub part_count: u32,
    pub parts: Vec<ManifestPart>,
    /// Per-column additive value checksum over the whole export (Form B), the
    /// i128 sums as decimal strings (JSON-stable). Present only when
    /// `RIVET_VALUE_CHECKSUM` was on during the run. `rivet validate` re-reads the
    /// parts and recomputes this to catch an `Arrow→Parquet` encode fault or
    /// post-write corruption — the step the in-process Form A check cannot see.
    /// Optional for back-compat: older manifests omit it (no `MANIFEST_VERSION`
    /// bump), newer readers tolerate its absence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_checksums: Option<Vec<String>>,
}

/// Terminal status of the run *as recorded by the writer*.
///
/// `success` is only written when M2 (Manifest Before SUCCESS) is satisfied
/// — i.e. when the writer is about to drop the `_SUCCESS` marker.
/// `failed` and `interrupted` manifests serve as audit trails and as input
/// to resume; they do NOT trigger `_SUCCESS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestStatus {
    Success,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestSource {
    pub engine: String,
    pub schema: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestDestination {
    pub kind: String,
    pub uri: String,
}

/// One committed (or quarantined) output part.
///
/// `path` is **relative to the destination prefix** (ADR-0012 §Manifest
/// schema) so the manifest is portable across copies of the dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestPart {
    pub part_id: u32,
    pub path: String,
    pub rows: i64,
    pub size_bytes: u64,
    /// xxh3 fingerprint of the part body.  Format mirrors [`crate::state::schema_fingerprint`]:
    /// `"xxh3:<16-hex>"`.  Algorithm prefix MUST be checked before interpreting
    /// the hex body (sha256/blake3 reserved for future hashers).
    pub content_fingerprint: String,
    /// Base64 MD5 of the part body, in GCS's `md5Hash` encoding — lets
    /// destination verification compare against the object's listing metadata
    /// with **no download** (GCS/S3/Azure surface this; the comparison rides
    /// the listing `--validate` already does).  Empty for legacy manifests and
    /// for parts whose MD5 could not be computed; the check then degrades to
    /// size-only.  `#[serde(default)]` keeps pre-0.7.x manifests parseable.
    #[serde(default)]
    pub content_md5: String,
    pub status: PartStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartStatus {
    /// Listed in the active manifest at the destination.
    Committed,
    /// Found in a prior manifest but rejected on resume (M9); retained for audit.
    Quarantined,
}

impl RunManifest {
    /// Sum of `rows` across `Committed` parts.  Used by M5 sanity checks and
    /// by `--reconcile` to compare against source `COUNT(*)`.
    pub fn committed_rows(&self) -> i64 {
        self.parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .map(|p| p.rows)
            .sum()
    }

    /// Number of `Committed` parts.
    pub fn committed_part_count(&self) -> usize {
        self.parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .count()
    }

    /// Verify that the recorded aggregates (`row_count`, `part_count`) match
    /// the actual `Committed` parts in `parts`.  A mismatch is a writer bug;
    /// callers should refuse to act on the manifest until investigated.
    pub fn validate_self_consistency(&self) -> std::result::Result<(), ManifestInconsistency> {
        if self.manifest_version != MANIFEST_VERSION {
            return Err(ManifestInconsistency::UnsupportedVersion {
                found: self.manifest_version,
                supported: MANIFEST_VERSION,
            });
        }
        let actual_parts = self.committed_part_count();
        if actual_parts != self.part_count as usize {
            return Err(ManifestInconsistency::PartCountMismatch {
                declared: self.part_count,
                actual: actual_parts,
            });
        }
        let actual_rows = self.committed_rows();
        if actual_rows != self.row_count {
            return Err(ManifestInconsistency::RowCountMismatch {
                declared: self.row_count,
                actual: actual_rows,
            });
        }
        // Part IDs must be unique within a manifest.
        let mut ids: Vec<u32> = self.parts.iter().map(|p| p.part_id).collect();
        ids.sort_unstable();
        for w in ids.windows(2) {
            if w[0] == w[1] {
                return Err(ManifestInconsistency::DuplicatePartId(w[0]));
            }
        }
        Ok(())
    }
}

/// Self-consistency failures detected by [`RunManifest::validate_self_consistency`].
///
/// These represent writer bugs, not destination drift; M5 destination-state
/// checks live in the validate command path.
#[derive(Debug, PartialEq)]
pub enum ManifestInconsistency {
    UnsupportedVersion { found: u32, supported: u32 },
    PartCountMismatch { declared: u32, actual: usize },
    RowCountMismatch { declared: i64, actual: i64 },
    DuplicatePartId(u32),
}

impl std::fmt::Display for ManifestInconsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion { found, supported } => write!(
                f,
                "manifest_version {found} is not supported by this build (expected {supported})"
            ),
            Self::PartCountMismatch { declared, actual } => write!(
                f,
                "part_count declares {declared} parts but {actual} committed parts found"
            ),
            Self::RowCountMismatch { declared, actual } => write!(
                f,
                "row_count declares {declared} rows but committed parts sum to {actual}"
            ),
            Self::DuplicatePartId(id) => {
                write!(f, "duplicate part_id {id} in manifest.parts")
            }
        }
    }
}

impl std::error::Error for ManifestInconsistency {}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(id: u32, rows: i64, size: u64) -> ManifestPart {
        ManifestPart {
            part_id: id,
            path: format!("part-{id:06}.parquet"),
            rows,
            size_bytes: size,
            content_fingerprint: format!("xxh3:{:016x}", id as u64),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }
    }

    fn manifest_with_parts(parts: Vec<ManifestPart>) -> RunManifest {
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
            manifest_version: MANIFEST_VERSION,
            run_id: "orders_20260521T120000.000".into(),
            export_name: "public.orders".into(),
            started_at: "2026-05-21T12:00:00Z".into(),
            finished_at: "2026-05-21T12:14:33Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
            },
            destination: ManifestDestination {
                kind: "gcs".into(),
                uri: "gs://rivet-exports/public.orders/run/".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
        }
    }

    // ── constants ───────────────────────────────────────────────────────────

    #[test]
    fn manifest_version_is_one() {
        assert_eq!(MANIFEST_VERSION, 1);
    }

    #[test]
    fn filenames_are_stable() {
        assert_eq!(MANIFEST_FILENAME, "manifest.json");
        assert_eq!(SUCCESS_FILENAME, "_SUCCESS");
        assert_eq!(QUARANTINE_PREFIX, "_quarantine");
    }

    // ── self-consistency ────────────────────────────────────────────────────

    #[test]
    fn self_consistent_manifest_validates() {
        let m = manifest_with_parts(vec![part(1, 100, 4096), part(2, 200, 8192)]);
        assert_eq!(m.validate_self_consistency(), Ok(()));
    }

    #[test]
    fn rejects_part_count_mismatch() {
        let mut m = manifest_with_parts(vec![part(1, 100, 4096)]);
        m.part_count = 5;
        assert!(matches!(
            m.validate_self_consistency(),
            Err(ManifestInconsistency::PartCountMismatch {
                declared: 5,
                actual: 1
            })
        ));
    }

    #[test]
    fn rejects_row_count_mismatch() {
        let mut m = manifest_with_parts(vec![part(1, 100, 4096)]);
        m.row_count = 999;
        assert!(matches!(
            m.validate_self_consistency(),
            Err(ManifestInconsistency::RowCountMismatch {
                declared: 999,
                actual: 100
            })
        ));
    }

    #[test]
    fn rejects_duplicate_part_id() {
        let m = manifest_with_parts(vec![part(1, 100, 4096), part(1, 200, 8192)]);
        let err = m.validate_self_consistency().unwrap_err();
        assert_eq!(err, ManifestInconsistency::DuplicatePartId(1));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut m = manifest_with_parts(vec![]);
        m.manifest_version = 999;
        m.part_count = 0;
        m.row_count = 0;
        assert!(matches!(
            m.validate_self_consistency(),
            Err(ManifestInconsistency::UnsupportedVersion {
                found: 999,
                supported: 1
            })
        ));
    }

    // ── quarantined parts ──────────────────────────────────────────────────

    #[test]
    fn quarantined_parts_do_not_count_toward_row_or_part_totals() {
        let mut p_q = part(2, 999, 8192);
        p_q.status = PartStatus::Quarantined;
        let m = manifest_with_parts(vec![part(1, 100, 4096), p_q]);

        // The factory only counts committed; manifest must validate.
        assert_eq!(m.validate_self_consistency(), Ok(()));
        assert_eq!(m.committed_rows(), 100);
        assert_eq!(m.committed_part_count(), 1);
    }

    // ── serde roundtrip ────────────────────────────────────────────────────

    #[test]
    fn json_roundtrip_preserves_fields() {
        let m = manifest_with_parts(vec![part(1, 100, 4096), part(2, 200, 8192)]);
        let json = serde_json::to_string_pretty(&m).unwrap();
        let parsed: RunManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, parsed);
    }

    #[test]
    fn status_serializes_as_snake_case() {
        let m = manifest_with_parts(vec![]);
        // Force part_count=0 so the empty-parts manifest still validates self-consistency,
        // then check the wire form.  (This test cares about the enum encoding, not totals.)
        let mut m = m;
        m.part_count = 0;
        m.row_count = 0;
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"status\":\"success\""));

        m.status = ManifestStatus::Interrupted;
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"status\":\"interrupted\""));
    }

    // ── success marker ─────────────────────────────────────────────────────

    #[test]
    fn success_marker_body_is_xxh3_prefix_plus_16_hex_plus_newline() {
        let body = success_marker_body(b"some manifest bytes");
        assert!(body.starts_with("xxh3:"), "body = {body:?}");
        assert!(body.ends_with('\n'), "body = {body:?}");
        let trimmed = body.trim_end();
        let hex = &trimmed["xxh3:".len()..];
        assert_eq!(hex.len(), 16, "body = {body:?}");
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn success_marker_body_is_deterministic_for_same_input() {
        let a = success_marker_body(b"hello");
        let b = success_marker_body(b"hello");
        assert_eq!(a, b);
    }

    #[test]
    fn success_marker_body_differs_for_different_manifest_bytes() {
        let a = success_marker_body(b"manifest one");
        let b = success_marker_body(b"manifest two");
        assert_ne!(a, b);
    }

    #[test]
    fn parse_success_marker_roundtrips_with_writer() {
        let body = success_marker_body(b"some manifest bytes");
        let fp = parse_success_marker(&body).expect("must parse");
        assert!(fp.starts_with("xxh3:"));
        assert_eq!(fp.len(), "xxh3:".len() + 16);
    }

    #[test]
    fn parse_success_marker_rejects_malformed_bodies() {
        assert_eq!(parse_success_marker(""), None);
        assert_eq!(parse_success_marker("\n"), None);
        assert_eq!(parse_success_marker("sha256:0123456789abcdef"), None);
        // Wrong hex length:
        assert_eq!(parse_success_marker("xxh3:0123\n"), None);
        // Uppercase hex (we emit lowercase; reject to keep the format strict):
        assert_eq!(parse_success_marker("xxh3:0123456789ABCDEF\n"), None);
        // Non-hex body:
        assert_eq!(parse_success_marker("xxh3:zzzzzzzzzzzzzzzz\n"), None);
        // Missing prefix:
        assert_eq!(parse_success_marker("0123456789abcdef\n"), None);
    }

    #[test]
    fn parse_success_marker_tolerates_trailing_whitespace() {
        let body = "xxh3:0123456789abcdef\n";
        assert_eq!(parse_success_marker(body), Some("xxh3:0123456789abcdef"));
        // CRLF on Windows, double newline, trailing spaces — all fine.
        let body = "xxh3:0123456789abcdef\r\n";
        assert_eq!(parse_success_marker(body), Some("xxh3:0123456789abcdef"));
    }

    #[test]
    fn unknown_fields_are_ignored_by_reader() {
        // ADR-0012 forward-compatibility contract: a reader compiled against
        // v1 must tolerate v2-style fields that it doesn't recognise.
        let json = r#"{
            "manifest_version": 1,
            "run_id": "r1",
            "export_name": "t",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:01:00Z",
            "status": "success",
            "source": {"engine": "postgres"},
            "destination": {"kind": "local", "uri": "file:///tmp/out/"},
            "format": "parquet",
            "compression": "zstd",
            "schema_fingerprint": "xxh3:0000000000000000",
            "row_count": 0,
            "part_count": 0,
            "parts": [],
            "future_field_added_in_v2": {"nested": true}
        }"#;
        let parsed: RunManifest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.run_id, "r1");
        assert_eq!(parsed.validate_self_consistency(), Ok(()));
    }
}
