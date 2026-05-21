//! **Layer: Observability**
//!
//! Manifest-aware verification for `--validate` (ADR-0012 §M5 / §M6,
//! constrained by the ADR-0013 trust-flag contract that says: no new flags;
//! manifest-aware checks live under the existing `--validate`).
//!
//! Although this module reads from a `Destination` (technically L2 surface),
//! it makes **no execution decisions**: it does not write data, advance
//! cursors, mutate state, or change the pipeline path.  Its only output is
//! a structured `ManifestVerification` verdict the run report renders.
//! Per ADR-0003, that places it firmly in L4 Observability — the
//! destination read surface is just the carrier.
//!
//! Inputs (read-only):
//! - the destination's `manifest.json` body
//! - the destination's `_SUCCESS` body, if present
//! - the listing of every object under the destination prefix
//!
//! Outputs:
//! - [`ManifestVerification`] — a structured verdict the run report renders
//!   into the operator-facing "Verdicts" section.
//!
//! Out of scope here:
//! - per-file row-count check (that runs *during* the export, against the
//!   local temp file before upload — see `pipeline::validate::validate_output`).
//! - source-side reconciliation (lives in [`pipeline::reconcile_cmd`] and
//!   is what `--reconcile` adds on top of this).
//! - re-fingerprinting parts (`--validate --deep`, future).
//!
//! Failure modes are explicit: each check produces a `Failure` enum variant
//! that is rendered verbatim in `summary.json` so an Airflow / CI consumer
//! can branch on the kind, not parse strings.

use serde::{Deserialize, Serialize};

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{
    MANIFEST_FILENAME, RunManifest, SUCCESS_FILENAME, parse_success_marker, success_marker_body,
};

/// Outcome of a single `--validate` pass over a destination prefix.
///
/// Stable enough to be embedded in `summary.json` directly (see
/// `pipeline::report::ValidationOutcome`).  Forward-compat: consumers MUST
/// ignore unknown fields (no `deny_unknown_fields`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestVerification {
    /// True iff a `manifest.json` was found at the destination and parsed.
    /// `false` triggers ADR-0012 M6 fallback (legacy run); higher-level
    /// check results below are then "skipped" rather than "passed".
    pub manifest_found: bool,
    /// Mirrors ADR-0012 M6's required `legacy_run` operator-facing label.
    pub legacy_run: bool,
    /// Manifest parts whose presence and recorded `size_bytes` were
    /// confirmed at the destination.  0 when no manifest was found.
    pub parts_verified: usize,
    /// Manifest parts that were declared `committed` but not actually
    /// present, present at a different size, or otherwise mismatched.
    pub parts_failed: usize,
    /// True iff `_SUCCESS` exists at the destination AND its body matches
    /// the fingerprint of the bytes we read for `manifest.json`.  An
    /// existing `_SUCCESS` whose body diverges from the manifest is itself
    /// an integrity failure — surfaced via `failures`.
    pub success_marker_consistent: bool,
    /// Self-consistency of the manifest (`row_count`, `part_count`,
    /// duplicate `part_id`s).  Skipped when `manifest_found = false`.
    pub manifest_self_consistent: bool,
    /// Final verdict.  `true` only when every applicable sub-check passed.
    /// Legacy-run prefixes do **not** produce `passed = true` from this
    /// module — `pipeline::validate::validate_output` is the row-count
    /// fallback for those, and the caller composes the two.
    pub passed: bool,
    /// Per-failure detail.  Empty when `passed = true` (modulo legacy_run,
    /// which has its own bool).  Stable variant set; new variants land
    /// under a new manifest version per ADR-0012.
    pub failures: Vec<Failure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Failure {
    /// Manifest declared a part that does not exist at the destination.
    PartMissing { part_id: u32, path: String },
    /// Manifest declared a part whose actual size differs from `size_bytes`.
    PartSizeMismatch {
        part_id: u32,
        path: String,
        expected: u64,
        actual: u64,
    },
    /// `_SUCCESS` exists but its body is malformed (not `xxh3:<16-hex>` after
    /// trim).  ADR-0012 M2 — orchestrators rely on this format being strict.
    SuccessMarkerMalformed { body_preview: String },
    /// `_SUCCESS` body parsed but does not match `xxh3(manifest.json bytes)`.
    /// Two legitimate sources: (a) someone overwrote `_SUCCESS` after the
    /// manifest was rewritten — orchestrator bug; (b) the manifest was
    /// edited in place after the run — operator bug.  Either way the
    /// manifest is no longer trustworthy.
    SuccessMarkerStale {
        marker_fingerprint: String,
        manifest_fingerprint: String,
    },
    /// `RunManifest::validate_self_consistency` rejected the manifest.
    /// Usually a writer bug (declared row_count != sum of committed parts'
    /// rows); blocks the rest of the verification because the manifest
    /// itself is unreliable.
    ManifestSelfInconsistent { detail: String },
    /// Reading `manifest.json` returned an I/O error other than "absent".
    ManifestReadError { detail: String },
    /// Reading `_SUCCESS` returned an I/O error other than "absent".
    SuccessMarkerReadError { detail: String },
    /// Listing the destination prefix returned an I/O error.  Reduces the
    /// untracked-parts check (M5 surplus) to a no-op for this run.
    ListPrefixError { detail: String },
    /// A file is present at the destination prefix but no manifest entry
    /// references it.  M9-adjacent: `--validate` only flags it; quarantine
    /// belongs to `--resume`.
    UntrackedObject { key: String, size_bytes: u64 },
}

impl std::fmt::Display for Failure {
    /// One operator-facing line per failure variant.  Used by:
    /// - `pipeline::report::render_markdown` (summary.md "failure:" lines)
    /// - `pipeline::validate_cmd::render_pretty` (`rivet validate` stdout)
    /// - any future consumer that wants a human-readable failure label
    ///
    /// The wire format (`failures[].kind` + per-variant fields) lives in
    /// the `Serialize` derive above and is the contract Airflow / CI
    /// consumers branch on.  This `Display` impl is for humans only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::PartMissing { part_id, path } => {
                write!(f, "part {} missing at {}", part_id, path)
            }
            Failure::PartSizeMismatch {
                part_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "part {} size mismatch at {}: manifest {}, dest {}",
                part_id, path, expected, actual
            ),
            Failure::SuccessMarkerMalformed { body_preview } => {
                write!(f, "_SUCCESS body malformed: {body_preview:?}")
            }
            Failure::SuccessMarkerStale {
                marker_fingerprint,
                manifest_fingerprint,
            } => write!(
                f,
                "_SUCCESS body {} != manifest fingerprint {} (stale marker)",
                marker_fingerprint, manifest_fingerprint
            ),
            Failure::ManifestSelfInconsistent { detail } => {
                write!(f, "manifest self-consistency: {detail}")
            }
            Failure::ManifestReadError { detail } => {
                write!(f, "manifest read error: {detail}")
            }
            Failure::SuccessMarkerReadError { detail } => {
                write!(f, "_SUCCESS read error: {detail}")
            }
            Failure::ListPrefixError { detail } => {
                write!(f, "destination listing error: {detail}")
            }
            Failure::UntrackedObject { key, size_bytes } => {
                write!(f, "untracked object: {} ({} bytes)", key, size_bytes)
            }
        }
    }
}

impl ManifestVerification {
    /// Construct the M6 (legacy run) verdict for a destination that has no
    /// manifest at all.  Caller composes this with the existing per-file
    /// row-count check; together they form the legacy `--validate` result.
    pub fn legacy() -> Self {
        Self {
            manifest_found: false,
            legacy_run: true,
            parts_verified: 0,
            parts_failed: 0,
            success_marker_consistent: false,
            manifest_self_consistent: false,
            // `passed = false` here is intentional — it does NOT mean
            // "validation failed", it means "this verifier cannot certify".
            // The caller layers per-file row counts on top and composes
            // a final verdict for the report.
            passed: false,
            failures: Vec::new(),
        }
    }

    /// True iff this verification surfaced any explicit failure (i.e. a
    /// reason an orchestrator should refuse the run).  Distinct from
    /// `!passed`, which can also mean "legacy / not applicable".
    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }
}

/// Run the manifest-aware verification at `manifest_dir` (the destination-
/// relative directory containing `manifest.json` and `_SUCCESS`).
///
/// `manifest_dir` is the same key shape `Destination::write` was called with
/// for the manifest itself — typically empty (`""`) for prefix-rooted runs,
/// or the per-export sub-directory.  Trailing `/` is optional.
///
/// This function does not panic on any expected I/O outcome — every read
/// failure becomes a `Failure::*ReadError` so the caller can render a
/// useful message instead of bailing.
pub fn verify_at_destination(
    dest: &dyn Destination,
    manifest_dir: &str,
) -> Result<ManifestVerification> {
    let manifest_key = join_key(manifest_dir, MANIFEST_FILENAME);
    let success_key = join_key(manifest_dir, SUCCESS_FILENAME);

    // ── 1. Manifest read ───────────────────────────────────────────────
    let manifest_bytes = match dest.head(&manifest_key)? {
        None => return Ok(ManifestVerification::legacy()),
        Some(_) => match dest.read(&manifest_key) {
            Ok(b) => b,
            Err(e) => {
                let mut v = ManifestVerification::legacy();
                v.legacy_run = false;
                v.failures.push(Failure::ManifestReadError {
                    detail: format!("{e:#}"),
                });
                v.passed = false;
                return Ok(v);
            }
        },
    };

    let manifest: RunManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(e) => {
            // A malformed manifest is treated as a self-inconsistency —
            // semantically equivalent for the operator (the manifest can't
            // be trusted) but kept distinct in `failures` so the kind is
            // explicit on the wire.
            return Ok(ManifestVerification {
                manifest_found: true,
                legacy_run: false,
                parts_verified: 0,
                parts_failed: 0,
                success_marker_consistent: false,
                manifest_self_consistent: false,
                passed: false,
                failures: vec![Failure::ManifestSelfInconsistent {
                    detail: format!("manifest.json parse failed: {e}"),
                }],
            });
        }
    };

    let mut out = ManifestVerification {
        manifest_found: true,
        legacy_run: false,
        parts_verified: 0,
        parts_failed: 0,
        success_marker_consistent: false,
        manifest_self_consistent: true,
        passed: true,
        failures: Vec::new(),
    };

    // ── 2. Self-consistency ─────────────────────────────────────────────
    if let Err(e) = manifest.validate_self_consistency() {
        out.manifest_self_consistent = false;
        out.passed = false;
        out.failures.push(Failure::ManifestSelfInconsistent {
            detail: format!("{e}"),
        });
        // Don't short-circuit — we still want to surface part-presence
        // failures because the operator may want to know both classes at
        // once rather than fix-then-rerun.
    }

    // ── 3. Per-part presence + size ────────────────────────────────────
    for part in &manifest.parts {
        if part.status != crate::manifest::PartStatus::Committed {
            continue; // quarantined / other audit-only entries are M9, not M5
        }
        let part_key = join_key(manifest_dir, &part.path);
        match dest.head(&part_key) {
            Ok(Some(meta)) if meta.size_bytes == part.size_bytes => {
                out.parts_verified += 1;
            }
            Ok(Some(meta)) => {
                out.parts_failed += 1;
                out.passed = false;
                out.failures.push(Failure::PartSizeMismatch {
                    part_id: part.part_id,
                    path: part.path.clone(),
                    expected: part.size_bytes,
                    actual: meta.size_bytes,
                });
            }
            Ok(None) => {
                out.parts_failed += 1;
                out.passed = false;
                out.failures.push(Failure::PartMissing {
                    part_id: part.part_id,
                    path: part.path.clone(),
                });
            }
            Err(e) => {
                out.parts_failed += 1;
                out.passed = false;
                out.failures.push(Failure::PartMissing {
                    part_id: part.part_id,
                    path: format!("{} (head failed: {e})", part.path),
                });
            }
        }
    }

    // ── 4. _SUCCESS marker consistency ─────────────────────────────────
    match dest.head(&success_key)? {
        None => {
            // Absent _SUCCESS is informational, not a failure: per ADR-0012
            // M2, only successful runs land it.  A failed-then-rewritten
            // manifest legitimately lacks _SUCCESS.  Leave
            // `success_marker_consistent = false` (this is a "no signal"
            // bool, not a "broken" bool) and let the caller decide.
        }
        Some(_) => match dest.read(&success_key) {
            Err(e) => {
                out.passed = false;
                out.failures.push(Failure::SuccessMarkerReadError {
                    detail: format!("{e:#}"),
                });
            }
            Ok(body) => {
                let body_str = match std::str::from_utf8(&body) {
                    Ok(s) => s,
                    Err(_) => {
                        out.passed = false;
                        out.failures.push(Failure::SuccessMarkerMalformed {
                            body_preview: format!("(non-utf8, {} bytes)", body.len()),
                        });
                        return Ok(out);
                    }
                };
                match parse_success_marker(body_str) {
                    None => {
                        out.passed = false;
                        out.failures.push(Failure::SuccessMarkerMalformed {
                            body_preview: preview(body_str),
                        });
                    }
                    Some(marker_fp) => {
                        let manifest_fp = success_marker_body(&manifest_bytes);
                        // success_marker_body returns the trailing `\n`
                        // form; trim before comparing to the parsed marker
                        // (which already trims).
                        let manifest_fp_trimmed = manifest_fp.trim_end_matches('\n');
                        if marker_fp == manifest_fp_trimmed {
                            out.success_marker_consistent = true;
                        } else {
                            out.passed = false;
                            out.failures.push(Failure::SuccessMarkerStale {
                                marker_fingerprint: marker_fp.to_string(),
                                manifest_fingerprint: manifest_fp_trimmed.to_string(),
                            });
                        }
                    }
                }
            }
        },
    }

    // ── 5. Untracked objects under prefix ──────────────────────────────
    //
    // Best-effort: list the prefix and surface anything that isn't the
    // manifest, the success marker, or a manifest-listed part.  A list
    // failure is logged as a Failure::ListPrefixError but does not
    // invalidate the rest of the run — `passed` already reflects part
    // and marker outcomes.
    match dest.list_prefix(manifest_dir) {
        Err(e) => {
            out.failures.push(Failure::ListPrefixError {
                detail: format!("{e:#}"),
            });
            // We don't flip `passed` — the parts we DID verify are still
            // verified; we just couldn't enumerate the surplus.
        }
        Ok(listing) => {
            let known: std::collections::HashSet<String> = std::iter::once(manifest_key.clone())
                .chain(std::iter::once(success_key.clone()))
                .chain(
                    manifest
                        .parts
                        .iter()
                        .filter(|p| p.status == crate::manifest::PartStatus::Committed)
                        .map(|p| join_key(manifest_dir, &p.path)),
                )
                .collect();
            for obj in listing {
                if known.contains(&obj.key) {
                    continue;
                }
                // Skip the quarantine prefix — it's the right place for
                // legitimate untracked artifacts (resume's M9 destination).
                if obj.key.contains(crate::manifest::QUARANTINE_PREFIX) {
                    continue;
                }
                out.failures.push(Failure::UntrackedObject {
                    key: obj.key,
                    size_bytes: obj.size_bytes,
                });
            }
        }
    }

    Ok(out)
}

/// Join a destination-relative dir prefix with a child key, normalising the
/// separator.  Empty `dir` is the root (no leading `/`); a trailing `/` on
/// `dir` is collapsed.
fn join_key(dir: &str, key: &str) -> String {
    let dir = dir.trim_end_matches('/');
    if dir.is_empty() {
        key.to_string()
    } else {
        format!("{}/{}", dir, key)
    }
}

/// Truncate `s` to a small printable preview for error messages.
fn preview(s: &str) -> String {
    let trimmed: String = s.chars().take(40).collect();
    if s.chars().count() > 40 {
        format!("{trimmed}…")
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::local::LocalDestination;
    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        PartStatus, RunManifest,
    };
    use std::path::Path;

    fn local_dest(base: &Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            bucket: None,
            prefix: None,
            path: Some(base.to_string_lossy().into_owned()),
            region: None,
            endpoint: None,
            credentials_file: None,
            access_key_env: None,
            secret_key_env: None,
            aws_profile: None,
            allow_anonymous: false,
        })
        .unwrap()
    }

    fn part(part_id: u32, rows: i64, size: u64, fp: &str) -> ManifestPart {
        ManifestPart {
            part_id,
            path: format!("part-{part_id:06}.parquet"),
            rows,
            size_bytes: size,
            content_fingerprint: fp.into(),
            status: PartStatus::Committed,
        }
    }

    fn build_manifest(parts: Vec<ManifestPart>, status: ManifestStatus) -> RunManifest {
        let row_count: i64 = parts
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
            run_id: "r".into(),
            export_name: "public.orders".into(),
            started_at: "2026-05-21T12:00:00Z".into(),
            finished_at: "2026-05-21T12:01:00Z".into(),
            status,
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
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count,
            part_count,
            parts,
        }
    }

    /// Lay out a clean dataset with manifest + _SUCCESS at the root.
    fn write_dataset(dir: &Path, m: &RunManifest, parts_with_bytes: &[(&str, &[u8])]) {
        for (name, bytes) in parts_with_bytes {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        let body = serde_json::to_vec_pretty(m).unwrap();
        std::fs::write(dir.join(MANIFEST_FILENAME), &body).unwrap();
        if matches!(m.status, ManifestStatus::Success) {
            std::fs::write(dir.join(SUCCESS_FILENAME), success_marker_body(&body)).unwrap();
        }
    }

    // ── happy path ───────────────────────────────────────────────────────

    #[test]
    fn happy_path_verifies_all_parts_and_success_marker() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(
            dir.path(),
            &m,
            &[
                ("part-000001.parquet", b"AAAA"),
                ("part-000002.parquet", b"BBBBB"),
            ],
        );
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(v.manifest_found);
        assert!(!v.legacy_run);
        assert_eq!(v.parts_verified, 2);
        assert_eq!(v.parts_failed, 0);
        assert!(v.success_marker_consistent);
        assert!(v.manifest_self_consistent);
        assert!(v.passed);
        assert!(v.failures.is_empty());
    }

    // ── M6 legacy run ───────────────────────────────────────────────────

    #[test]
    fn no_manifest_returns_legacy_run_label() {
        // Empty prefix — no manifest, no parts.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let v = verify_at_destination(&dest, "").unwrap();
        assert!(!v.manifest_found);
        assert!(v.legacy_run);
        assert_eq!(v.parts_verified, 0);
        assert!(!v.passed);
        assert!(v.failures.is_empty(), "no failures, just a legacy label");
    }

    // ── M5 part-presence failures ───────────────────────────────────────

    #[test]
    fn missing_part_is_flagged_with_part_id_and_path() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(
            dir.path(),
            &m,
            &[("part-000001.parquet", b"AAAA")], // part 2 missing
        );
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert_eq!(v.parts_verified, 1);
        assert_eq!(v.parts_failed, 1);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. }))
        );
    }

    #[test]
    fn part_size_mismatch_is_flagged_with_expected_and_actual() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        // Manifest claims 4 bytes; we write 6.
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"OOPSIE")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(!v.passed);
        let mismatch = v
            .failures
            .iter()
            .find_map(|f| match f {
                Failure::PartSizeMismatch {
                    part_id,
                    expected,
                    actual,
                    ..
                } => Some((*part_id, *expected, *actual)),
                _ => None,
            })
            .expect("must surface the size mismatch");
        assert_eq!(mismatch, (1, 4, 6));
    }

    // ── _SUCCESS marker integrity ───────────────────────────────────────

    #[test]
    fn stale_success_marker_is_flagged_as_inconsistent() {
        // Write a manifest, then overwrite _SUCCESS with the marker for a
        // *different* manifest body — simulating an orchestrator that
        // mishandled a re-run.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(b"different manifest body"),
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(!v.success_marker_consistent);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::SuccessMarkerStale { .. }))
        );
    }

    #[test]
    fn malformed_success_marker_body_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(dir.path().join(SUCCESS_FILENAME), b"not even xxh3 shaped").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::SuccessMarkerMalformed { .. }))
        );
    }

    #[test]
    fn absent_success_marker_does_not_fail_validation_alone() {
        // ADR-0012 M2: only successful runs land _SUCCESS.  A failed-then-
        // rewritten manifest legitimately lacks one — verification must
        // not flip `passed` just for that.
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Failed,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        // Note: write_dataset only writes _SUCCESS for status == Success,
        // so no marker exists here.
        assert!(!dir.path().join(SUCCESS_FILENAME).exists());
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(v.manifest_found);
        assert!(
            !v.success_marker_consistent,
            "no marker => false (no signal)"
        );
        // The parts still verified, so passed = true.
        assert!(v.passed);
        assert!(v.failures.is_empty());
    }

    // ── self-consistency ────────────────────────────────────────────────

    #[test]
    fn self_inconsistent_manifest_is_flagged_but_part_check_still_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        m.row_count = 9999; // lie

        let body = serde_json::to_vec_pretty(&m).unwrap();
        std::fs::write(dir.path().join("part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), &body).unwrap();
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(&body),
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(v.manifest_found);
        assert!(!v.manifest_self_consistent);
        assert!(!v.passed);
        // Parts that are physically present still get their `parts_verified`
        // counter bumped — both signals are independently useful.
        assert_eq!(v.parts_verified, 1);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ManifestSelfInconsistent { .. }))
        );
    }

    // ── untracked objects ───────────────────────────────────────────────

    #[test]
    fn untracked_object_under_prefix_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(dir.path().join("rogue.parquet"), b"XX").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(
            v.failures.iter().any(
                |f| matches!(f, Failure::UntrackedObject { key, .. } if key == "rogue.parquet")
            )
        );
        // Untracked objects are surfaced but do NOT flip `passed` — that
        // is the resume-side decision (M9).  Parts and marker are fine,
        // so passed remains true.
        assert!(v.passed);
    }

    #[test]
    fn quarantine_prefix_objects_are_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::create_dir_all(dir.path().join(crate::manifest::QUARANTINE_PREFIX)).unwrap();
        std::fs::write(
            dir.path()
                .join(crate::manifest::QUARANTINE_PREFIX)
                .join("old.parquet"),
            b"OO",
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "").unwrap();
        assert!(v.passed);
        assert!(
            !v.failures
                .iter()
                .any(|f| matches!(f, Failure::UntrackedObject { .. })),
            "quarantine_prefix is the legitimate home for these — must not flag"
        );
    }

    // ── manifest_dir join semantics ─────────────────────────────────────

    #[test]
    fn verifies_in_subdirectory_when_manifest_dir_is_non_empty() {
        let outer = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outer.path().join("sub/run")).unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        let body = serde_json::to_vec_pretty(&m).unwrap();
        std::fs::write(outer.path().join("sub/run/part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(outer.path().join("sub/run").join(MANIFEST_FILENAME), &body).unwrap();
        std::fs::write(
            outer.path().join("sub/run").join(SUCCESS_FILENAME),
            success_marker_body(&body),
        )
        .unwrap();
        let dest = local_dest(outer.path());

        let v = verify_at_destination(&dest, "sub/run").unwrap();
        assert!(v.passed);
        assert_eq!(v.parts_verified, 1);

        // Trailing slash is normalised — same outcome.
        let v2 = verify_at_destination(&dest, "sub/run/").unwrap();
        assert!(v2.passed);
    }
}
