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

/// `manifest-<sanitized run_id>.json` — the immutable per-run COPY written
/// beside the canonical [`MANIFEST_FILENAME`]. The canonical name is
/// last-writer-wins (a pointer to the latest run); consecutive runs into one
/// prefix would clobber it, so this copy preserves EACH run's manifest for a
/// consumer that sums row counts across runs. Same run-token sanitizer the
/// parts use: an RFC3339 run id carries `:`/`+` (illegal on Windows), so map
/// anything outside `[A-Za-z0-9._-]` to `-`.
pub fn run_unique_manifest_name(run_id: &str) -> String {
    let token: String = run_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("manifest-{token}.json")
}

/// True if `name` (a final path segment) is a [`run_unique_manifest_name`] copy
/// — a Rivet-internal sidecar the validate/reconcile paths must NOT flag as an
/// untracked foreign object. Excludes the canonical `manifest.json` (`manifest.`
/// prefix, not `manifest-`).
pub fn is_run_unique_manifest_name(name: &str) -> bool {
    name.starts_with("manifest-") && name.ends_with(".json")
}

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
    // `strip_prefix` + a BYTE-length check, NOT `split_at`: split_at panics on a
    // non-char-boundary index, so a crafted 21-byte valid-UTF-8 `_SUCCESS` body with
    // a multibyte codepoint straddling byte 5 (`"aaa" + U+0800 + …`) passed the old
    // length gate and aborted the whole `validate`/`--resume`/repair process under
    // the release `panic=abort` profile — a DoS of the trust oracle via a
    // destination-writable planted file (the same threat surface the manifest.json
    // byte-cap already defends). strip_prefix + byte-len never touch a char boundary.
    let hex = trimmed.strip_prefix("xxh3:")?;
    if hex.len() != 16 {
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

/// One per-column Form B checksum, keyed by column **name** (not position) so a
/// column reorder between export and validate can never silently misalign the
/// comparison (the positional `Vec<String>` it replaced could). `checksum` is the
/// per-column xxh3, XOR-combined over the whole export, as a decimal string
/// (JSON-stable).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnChecksum {
    pub name: String,
    pub checksum: String,
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
    /// Which pipeline shape wrote this manifest — `batch` or `cdc`. Guards a
    /// prefix against silent cross-shape clobbering (finding #44: a CDC run
    /// overwrote a batch export's manifest at a shared prefix, orphaning its
    /// parts from `rivet validate`). Defaults to `batch` for manifests
    /// written before this field existed.
    #[serde(default = "default_manifest_mode")]
    pub mode: String,
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
    /// Per-column value checksum over the whole export (Form B), keyed by column
    /// name — the per-column xxh3 XOR-combined over the run. `rivet validate`
    /// re-reads the parts and recomputes this to catch an `Arrow→Parquet` encode
    /// fault or post-write corruption — the step the in-process Form A check
    /// cannot see. Optional for back-compat: older manifests omit it (no
    /// `MANIFEST_VERSION` bump), newer readers tolerate its absence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_checksums: Option<Vec<ColumnChecksum>>,
    /// The column the Form B checksum is keyed to (`xxh3(key ‖ value)`, the
    /// export's cursor/key column) so `validate` re-keys identically. `None` ⇒
    /// un-keyed (a full export with no cursor). See [`ColumnChecksum`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_key_column: Option<String>,
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
    /// The extraction contract this run fulfilled — strategy, cursor identity,
    /// and the cursor RANGE this extract covered. Ported from the
    /// pip_db_replicator meta-catalog idea: cursor metadata travels WITH the
    /// extract so a downstream warehouse can reconcile continuity
    /// (`run N+1.cursor_low` must follow `run N.cursor_high` — a gap is a
    /// silently-skipped range) without querying rivet's private state.
    /// `None` on manifests written before this field existed, and on paths
    /// that carry no cursor (a full snapshot records only the strategy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction: Option<ExtractionMetadata>,
}

/// Cursor/strategy metadata shipped in the manifest for warehouse-side
/// reconciliation. Cursor bounds are STRING-encoded and type-tagged: a lexical
/// order lies on numbers (the same trap as a binlog `__pos` string), so the
/// consumer compares by `cursor_type`, not by raw string order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtractionMetadata {
    /// `full` / `incremental` / `chunked` / `keyset` / `timewindow`.
    pub strategy: String,
    /// Resolved cursor/key column, when the strategy has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_column: Option<String>,
    /// Source type of the cursor column (for typed range comparison).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_type: Option<String>,
    /// Lowest cursor value covered by THIS extract (the prior run's high, or
    /// the min seen this run). `None` for a full snapshot / first run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_low: Option<String>,
    /// Highest cursor value covered — the value the NEXT run must resume from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_high: Option<String>,
    /// Source-side row count at extraction time, when cheaply known — distinct
    /// from the manifest's `row_count` (rows EXTRACTED). A divergence is the
    /// reconciliation signal. `None` when not probed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_row_count: Option<i64>,
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
            mode: "batch".to_string(),
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
                extraction: None,
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
            checksum_key_column: None,
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

    #[test]
    fn run_unique_manifest_name_sanitizes_and_is_recognised() {
        // A real CLI run id is RFC3339 — `:` and `+` are not filename-safe (`:`
        // is illegal on Windows), so they map to `-`.
        let n = run_unique_manifest_name("orders_2026-07-13T12:00:00+00:00");
        assert_eq!(n, "manifest-orders_2026-07-13T12-00-00-00-00.json");
        assert!(is_run_unique_manifest_name(&n));
        // The canonical pointer is NOT a per-run copy (its stem is `manifest.`,
        // not `manifest-`), so validate/reconcile still treat it distinctly.
        assert!(!is_run_unique_manifest_name(MANIFEST_FILENAME));
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
        // A crafted 21-byte (== the valid length) body with a MULTIBYTE codepoint
        // straddling byte 5: passes the byte-length gate, and the old `split_at(5)`
        // panicked on the non-char-boundary → a DoS of the trust oracle under
        // panic=abort (a destination-writable planted `_SUCCESS`). Must be `None`,
        // never a panic. ("aaa" + U+0800 [bytes 3..6] + 15×'a' = 21 bytes.)
        assert_eq!(parse_success_marker("aaa\u{0800}aaaaaaaaaaaaaaa"), None);
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

fn default_manifest_mode() -> String {
    "batch".to_string()
}

/// Finding #44 guard: refuse to overwrite a manifest written by the OTHER
/// pipeline shape. Same-shape overwrites stay allowed (a batch full re-run
/// replaces its own manifest by design; CDC extends its own). A missing or
/// unreadable manifest is not this guard's business — corruption surfaces in
/// `rivet validate`, and first runs must not require a read round-trip.
pub fn guard_manifest_mode(
    dest: &dyn crate::destination::Destination,
    new_mode: &str,
) -> anyhow::Result<()> {
    let Ok(bytes) = dest.read("manifest.json") else {
        return Ok(());
    };
    let Ok(existing) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Ok(());
    };
    // A manifest WITHOUT the field predates 0.16.6 — every live CDC
    // deployment's prefix looks like that after an upgrade, and refusing it
    // would brick their own resume. Absent mode ⇒ unknown ⇒ allow; only an
    // EXPLICIT cross-shape mismatch refuses.
    let Some(existing_mode) = existing.get("mode").and_then(|m| m.as_str()) else {
        return Ok(());
    };
    if existing_mode != new_mode {
        anyhow::bail!(
            "destination already holds a '{existing_mode}' manifest (run_id {run}); refusing to \
             overwrite it with a '{new_mode}' manifest — a batch export and a CDC export sharing \
             one prefix silently destroy each other's audit trail. Give this export its own \
             prefix (the scaffold now uses exports/<table>/cdc/ for CDC).",
            run = existing
                .get("run_id")
                .and_then(|r| r.as_str())
                .unwrap_or("unknown"),
        );
    }
    Ok(())
}

#[cfg(test)]
mod mode_guard_tests {
    use super::*;

    #[test]
    fn pre_extraction_manifests_parse_with_none() {
        // Manifests written before the extraction section (the real v0.16
        // fixture) must parse — the field is opt-in.
        let old = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/compat/v0.16/cdc_manifest.json"),
        )
        .expect("compat fixture");
        let m: RunManifest = serde_json::from_str(&old).expect("parses");
        assert!(m.source.extraction.is_none());
    }

    #[test]
    fn extraction_roundtrips_and_omits_none_fields() {
        let ex = ExtractionMetadata {
            strategy: "incremental".into(),
            cursor_column: Some("id".into()),
            cursor_type: None,
            cursor_low: Some("1000".into()),
            cursor_high: Some("2000".into()),
            source_row_count: None,
        };
        let j = serde_json::to_string(&ex).unwrap();
        // skip_serializing_if omits the None fields entirely.
        assert!(!j.contains("cursor_type"), "None fields omitted: {j}");
        assert!(!j.contains("source_row_count"), "{j}");
        assert!(j.contains("\"cursor_low\":\"1000\""), "{j}");
        let back: ExtractionMetadata = serde_json::from_str(&j).unwrap();
        assert_eq!(back, ex);
    }

    #[test]
    fn pre_mode_manifests_default_to_batch() {
        // The REAL committed v0.16 manifest (compat fixture) predates the
        // `mode` field — it must parse and read as batch.
        let old = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/compat/v0.16/cdc_manifest.json"),
        )
        .expect("compat fixture");
        let m: RunManifest = serde_json::from_str(&old).expect("old manifests parse");
        assert_eq!(m.mode, "batch");
    }

    #[test]
    fn cross_shape_overwrite_is_refused_same_shape_allowed() {
        let d = tempfile::tempdir().unwrap();
        let dest = crate::destination::create_destination(&crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(d.path().to_str().unwrap().to_string()),
            ..Default::default()
        })
        .unwrap();
        // empty prefix: any mode fine
        guard_manifest_mode(dest.as_ref(), "batch").unwrap();
        std::fs::write(
            d.path().join("manifest.json"),
            r#"{"mode":"batch","run_id":"r1"}"#,
        )
        .unwrap();
        guard_manifest_mode(dest.as_ref(), "batch").expect("same shape re-run allowed");
        let err = guard_manifest_mode(dest.as_ref(), "cdc")
            .unwrap_err()
            .to_string();
        assert!(err.contains("refusing to"), "loud refusal: {err}");
        assert!(
            err.contains("exports/<table>/cdc/"),
            "carries the recovery: {err}"
        );
        // A pre-0.16.6 manifest (no mode field) must be ALLOWED — every live
        // CDC deployment's prefix looks like that right after an upgrade, and
        // refusing would brick their own resume. Unknown ⇒ pass.
        std::fs::write(d.path().join("manifest.json"), r#"{"run_id":"legacy"}"#).unwrap();
        guard_manifest_mode(dest.as_ref(), "cdc").expect("legacy prefixes must keep resuming");
        guard_manifest_mode(dest.as_ref(), "batch").expect("in either direction");
    }
}
