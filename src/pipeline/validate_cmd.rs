//! `rivet validate` — re-run the manifest-aware `--validate` pass against
//! an existing destination, without performing an extraction.
//!
//! ADR-0013 amendment: this is **not** a new trust noun.  It is a standalone
//! driver for the same M5/M6 verification surface that `rivet run --validate`
//! already performs at end-of-run (see [`crate::pipeline::validate_manifest`]).
//! The verdict shape is identical; the only difference is no source query,
//! no extraction, no state writes.
//!
//! Use cases:
//! - "Is the output at this prefix still complete?" — Airflow / CI poller
//!   between runs.
//! - "Did someone delete a part by mistake?" — operator triage on a
//!   suspected-broken dataset.
//! - "Does this legacy prefix have a manifest yet?" — fast check for M6.
//!
//! Out of scope:
//! - Source-side reconciliation (`COUNT(*)`).  That's `--reconcile` /
//!   `rivet reconcile`, which already exists.
//! - Per-byte re-fingerprint of every part (`--validate --deep`, future).
//!
//! Exit code: `0` if `passed` (or the legacy-run case where the verifier
//! cannot certify but no failures were seen); non-zero on any explicit
//! failure (`PartMissing`, `PartSizeMismatch`, `SuccessMarkerStale`, …).

use std::path::Path;

use crate::config::Config;
use crate::error::Result;
use crate::pipeline::ManifestVerification;
use crate::pipeline::validate_manifest::verify_at_destination;

/// Output format mirroring the `rivet reconcile` / `rivet repair` pattern.
pub enum ValidateOutputFormat {
    /// Human-readable summary printed to stdout.
    Pretty,
    /// JSON to the given path or stdout if `None`.
    Json(Option<String>),
}

/// Driver for `rivet validate <export>` (or every export when
/// `export_name` is `None`).
///
/// Returns `Err` on the first explicit verification failure across the
/// requested exports so an Airflow / CI step can branch on the exit code.
/// Per-export verdicts are still printed to stdout / written to JSON for
/// every export, including subsequent ones — the bail at the end is the
/// last action.
pub fn run_validate_command(
    config_path: &str,
    export_name: Option<&str>,
    format: ValidateOutputFormat,
) -> Result<()> {
    let config = Config::load_with_params(config_path, None)?;

    let exports: Vec<&crate::config::ExportConfig> = match export_name {
        Some(name) => match config.exports.iter().find(|e| e.name == name) {
            Some(e) => vec![e],
            None => anyhow::bail!("export '{}' not found in config", name),
        },
        None => config.exports.iter().collect(),
    };

    if exports.is_empty() {
        anyhow::bail!("no exports defined in config — nothing to validate");
    }

    let mut all_results: Vec<(String, ManifestVerification)> = Vec::with_capacity(exports.len());
    let mut hard_failures: Vec<String> = Vec::new();

    for export in &exports {
        let dest = match crate::destination::create_destination(&export.destination) {
            Ok(d) => d,
            Err(e) => {
                let msg = format!(
                    "export '{}': could not open destination: {:#}",
                    export.name, e
                );
                hard_failures.push(msg);
                continue;
            }
        };
        // Streaming destinations have no prefix to verify — note and skip.
        if dest.capabilities().commit_protocol == crate::destination::WriteCommitProtocol::Streaming
        {
            log::info!(
                "export '{}': streaming destination, skipping (nothing to verify)",
                export.name
            );
            continue;
        }
        match verify_at_destination(&*dest, "") {
            Ok(v) => {
                all_results.push((export.name.clone(), v));
            }
            Err(e) => {
                hard_failures.push(format!(
                    "export '{}': verify_at_destination failed: {:#}",
                    export.name, e
                ));
            }
        }
    }

    match format {
        ValidateOutputFormat::Pretty => render_pretty(&all_results, &hard_failures),
        ValidateOutputFormat::Json(out_path) => {
            render_json(&all_results, &hard_failures, out_path)?
        }
    }

    // Exit-code policy: the standalone driver fails when an export's
    // composite verdict is `passed: false` — i.e. M5 saw an explicit
    // verification failure (missing part, size mismatch, stale _SUCCESS,
    // self-inconsistent manifest).  Surplus untracked objects (`UntrackedObject`)
    // are surfaced in `failures` for operator audit but do NOT flip `passed`,
    // because their cleanup is M9's job (resume), not validate's.  An
    // operator who wants strict "no surplus allowed" can grep the JSON
    // report for `kind: untracked_object` themselves; a future
    // `rivet validate --strict` flag may surface that exit-code mode if
    // demand appears (out of scope for this PR).
    //
    // Legacy runs (M6) keep exit 0: `passed: false` there means "verifier
    // cannot certify", not "verifier found a problem".
    let any_failed = all_results
        .iter()
        .any(|(_, v)| v.manifest_found && !v.passed);
    if !hard_failures.is_empty() || any_failed {
        anyhow::bail!(
            "rivet validate: {} export(s) failed verification",
            hard_failures.len()
                + all_results
                    .iter()
                    .filter(|(_, v)| v.manifest_found && !v.passed)
                    .count()
        );
    }
    Ok(())
}

fn render_pretty(results: &[(String, ManifestVerification)], hard_failures: &[String]) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut h = stdout.lock();

    for (name, v) in results {
        let _ = writeln!(h, "── {} ──", name);
        if v.legacy_run {
            let _ = writeln!(
                h,
                "  status:    legacy_run (no manifest at destination — pre-0.7.0 prefix)"
            );
            continue;
        }
        if !v.manifest_found {
            let _ = writeln!(h, "  status:    NO MANIFEST");
            continue;
        }
        let _ = writeln!(
            h,
            "  status:    {}",
            if v.passed { "PASSED" } else { "FAILED" }
        );
        let _ = writeln!(
            h,
            "  parts:     {} verified, {} failed",
            v.parts_verified, v.parts_failed
        );
        let _ = writeln!(
            h,
            "  _SUCCESS:  {}",
            if v.success_marker_consistent {
                "consistent"
            } else if v.failures.iter().any(|f| matches!(
                f,
                crate::pipeline::ManifestVerificationFailure::SuccessMarkerStale { .. }
                    | crate::pipeline::ManifestVerificationFailure::SuccessMarkerMalformed { .. }
                    | crate::pipeline::ManifestVerificationFailure::SuccessMarkerReadError { .. }
            )) {
                "INCONSISTENT (see failures)"
            } else {
                "absent (no signal)"
            }
        );
        let _ = writeln!(
            h,
            "  manifest:  {}",
            if v.manifest_self_consistent {
                "self-consistent"
            } else {
                "INCONSISTENT (see failures)"
            }
        );
        for f in &v.failures {
            let _ = writeln!(h, "  failure:   {}", render_failure(f));
        }
    }

    if !hard_failures.is_empty() {
        let _ = writeln!(h);
        let _ = writeln!(h, "── errors ──");
        for e in hard_failures {
            let _ = writeln!(h, "  {}", e);
        }
    }
    let _ = h.flush();
}

fn render_json(
    results: &[(String, ManifestVerification)],
    hard_failures: &[String],
    out_path: Option<String>,
) -> Result<()> {
    let payload = serde_json::json!({
        "exports": results
            .iter()
            .map(|(name, v)| {
                serde_json::json!({
                    "export_name": name,
                    "verification": v,
                })
            })
            .collect::<Vec<_>>(),
        "errors": hard_failures,
    });
    let serialized = serde_json::to_string_pretty(&payload)?;
    match out_path {
        Some(p) => {
            std::fs::write(Path::new(&p), &serialized)?;
            log::info!("rivet validate: wrote JSON report to {}", p);
        }
        None => {
            println!("{}", serialized);
        }
    }
    Ok(())
}

fn render_failure(f: &crate::pipeline::ManifestVerificationFailure) -> String {
    use crate::pipeline::ManifestVerificationFailure as F;
    match f {
        F::PartMissing { part_id, path } => format!("part {} missing at {}", part_id, path),
        F::PartSizeMismatch {
            part_id,
            path,
            expected,
            actual,
        } => format!(
            "part {} size mismatch at {} (manifest {}, dest {})",
            part_id, path, expected, actual
        ),
        F::SuccessMarkerMalformed { body_preview } => {
            format!("_SUCCESS body malformed: {body_preview:?}")
        }
        F::SuccessMarkerStale {
            marker_fingerprint,
            manifest_fingerprint,
        } => format!(
            "_SUCCESS body {} != manifest fingerprint {} (stale)",
            marker_fingerprint, manifest_fingerprint
        ),
        F::ManifestSelfInconsistent { detail } => format!("manifest self-consistency: {detail}"),
        F::ManifestReadError { detail } => format!("manifest read error: {detail}"),
        F::SuccessMarkerReadError { detail } => format!("_SUCCESS read error: {detail}"),
        F::ListPrefixError { detail } => format!("destination listing error: {detail}"),
        F::UntrackedObject { key, size_bytes } => {
            format!("untracked object: {} ({} bytes)", key, size_bytes)
        }
    }
}
