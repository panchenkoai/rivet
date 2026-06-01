//! **Layer: Coordinator** (config → destination → verification → render)
//!
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
//! - "Was yesterday's run complete?" — `--date YYYY-MM-DD` or `--run-id`
//!   re-targets a prior day's prefix without re-running the export
//!   (v0.7.2 historical-validation flags).
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

use chrono::NaiveDate;

use crate::config::Config;
use crate::destination::placeholder::PlaceholderContext;
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

/// Re-targeting overrides for `rivet validate`.
///
/// Default (`ValidateTarget::default()`) reproduces the v0.7.1 behaviour:
/// resolve `{date}` against today's UTC date, with no `{run_id}`
/// substitution, and use the config's destination prefix/path unchanged.
#[derive(Debug, Default, Clone)]
pub struct ValidateTarget {
    /// `--date YYYY-MM-DD` — override the date used for `{date}`.
    pub date: Option<NaiveDate>,
    /// `--run-id RID` — substitute `{run_id}` in the destination template.
    pub run_id: Option<String>,
    /// `--prefix STRING` — bypass placeholder resolution entirely and
    /// verify exactly this prefix.  Replaces both `prefix` and `path`.
    pub prefix_override: Option<String>,
}

impl ValidateTarget {
    fn placeholder_context(&self, export_name: &str) -> PlaceholderContext {
        let mut ctx = match self.date {
            Some(d) => PlaceholderContext::for_date(d, export_name),
            None => PlaceholderContext::for_today(export_name),
        };
        if let Some(rid) = &self.run_id {
            ctx = ctx.with_run_id(rid.clone());
        }
        ctx
    }
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
    target: ValidateTarget,
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

    // `--prefix` only makes sense for a single export; with multiple
    // exports it would silently re-point all of them at the same physical
    // bytes.  Catch this at the boundary so we never head-check the wrong
    // dataset under operator triage pressure.
    if target.prefix_override.is_some() && exports.len() > 1 {
        anyhow::bail!(
            "--prefix requires --export <name>: cannot apply one override to {} exports",
            exports.len()
        );
    }

    let mut all_results: Vec<ExportVerdict> = Vec::with_capacity(exports.len());
    let mut hard_failures: Vec<String> = Vec::new();

    for export in &exports {
        // Apply the operator-supplied re-targeting if any, else fall back
        // to today's UTC date (same shape `rivet run` resolves at write
        // time).
        let ctx = target.placeholder_context(&export.name);
        let mut expanded_dest =
            crate::destination::placeholder::expand_destination(export.destination.clone(), &ctx);
        if let Some(p) = &target.prefix_override {
            // Bypass placeholder resolution: trust the operator's literal
            // prefix.  Replace both `path` (local) and `prefix` (cloud)
            // so whichever the backend reads picks up the override.
            expanded_dest.path = Some(p.clone());
            expanded_dest.prefix = Some(p.clone());
        }
        let resolved_prefix = resolved_prefix_for_display(&expanded_dest);
        let dest = match crate::destination::create_destination(&expanded_dest) {
            Ok(d) => d,
            Err(e) => {
                let msg = format!(
                    "export '{}' (prefix: {}): could not open destination: {:#}",
                    export.name, resolved_prefix, e
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
            Ok(mut v) => {
                // Apply this export's `verify` policy: `content` fails the
                // verdict when any part is only size-verified (review D).
                v.enforce_content_policy(export.verify.requires_content());
                all_results.push(ExportVerdict {
                    name: export.name.clone(),
                    resolved_prefix,
                    verification: v,
                });
            }
            Err(e) => {
                hard_failures.push(format!(
                    "export '{}' (prefix: {}): verify_at_destination failed: {:#}",
                    export.name, resolved_prefix, e
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
        .any(|r| r.verification.manifest_found && !r.verification.passed);
    if !hard_failures.is_empty() || any_failed {
        anyhow::bail!(
            "rivet validate: {} export(s) failed verification",
            hard_failures.len()
                + all_results
                    .iter()
                    .filter(|r| r.verification.manifest_found && !r.verification.passed)
                    .count()
        );
    }
    Ok(())
}

/// Per-export verdict plus the resolved physical prefix the verifier
/// looked at — surfaced in both pretty and JSON output so an operator can
/// confirm at a glance which bytes were checked.
struct ExportVerdict {
    name: String,
    resolved_prefix: String,
    verification: ManifestVerification,
}

/// Render the destination's resolved prefix for human/JSON output.
///
/// Cloud backends carry the data location in `prefix`; the local backend
/// uses `path`.  Falling back to `<unresolved>` should never fire under
/// normal config (clap + Config::load enforce one of the two) but keeps
/// `validate` from panicking if a future config shape lands here.
fn resolved_prefix_for_display(dest: &crate::config::DestinationConfig) -> String {
    dest.prefix
        .clone()
        .or_else(|| dest.path.clone())
        .unwrap_or_else(|| "<unresolved>".into())
}

fn render_pretty(results: &[ExportVerdict], hard_failures: &[String]) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut h = stdout.lock();

    for r in results {
        let _ = writeln!(h, "── {} ──", r.name);
        let _ = writeln!(h, "  prefix:    {}", r.resolved_prefix);
        let v = &r.verification;
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
            "  parts:     {} verified ({} md5, {} size-only), {} failed",
            v.parts_verified,
            v.parts_md5_verified,
            v.parts_verified.saturating_sub(v.parts_md5_verified),
            v.parts_failed
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
        for failure in &v.failures {
            // `Failure: Display` is the single source of truth for these
            // lines; same string the run report's "failure:" entry uses.
            let _ = writeln!(h, "  failure:   {}", failure);
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
    results: &[ExportVerdict],
    hard_failures: &[String],
    out_path: Option<String>,
) -> Result<()> {
    let payload = serde_json::json!({
        "exports": results
            .iter()
            .map(|r| {
                serde_json::json!({
                    "export_name": r.name,
                    "resolved_prefix": r.resolved_prefix,
                    "verification": r.verification,
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── ValidateTarget::placeholder_context ────────────────────────────────

    #[test]
    fn target_default_uses_today() {
        let target = ValidateTarget::default();
        let ctx = target.placeholder_context("orders");
        assert_eq!(ctx.date, chrono::Utc::now().date_naive());
        assert_eq!(ctx.export_name, "orders");
        assert!(ctx.run_id.is_none());
    }

    #[test]
    fn target_with_date_overrides_today() {
        let target = ValidateTarget {
            date: Some(NaiveDate::from_ymd_opt(2026, 5, 21).unwrap()),
            ..Default::default()
        };
        let ctx = target.placeholder_context("orders");
        assert_eq!(ctx.date, NaiveDate::from_ymd_opt(2026, 5, 21).unwrap());
        assert!(ctx.run_id.is_none());
    }

    #[test]
    fn target_composes_date_and_run_id() {
        // Regression for the "run yesterday, validate today" scenario:
        // operator passes both --date and --run-id; the resolver must see
        // both.
        let target = ValidateTarget {
            date: Some(NaiveDate::from_ymd_opt(2026, 5, 21).unwrap()),
            run_id: Some("r-abc123".into()),
            prefix_override: None,
        };
        let ctx = target.placeholder_context("orders");
        assert_eq!(ctx.date, NaiveDate::from_ymd_opt(2026, 5, 21).unwrap());
        assert_eq!(ctx.run_id.as_deref(), Some("r-abc123"));
    }

    // ── resolved_prefix_for_display ────────────────────────────────────────

    #[test]
    fn resolved_prefix_prefers_cloud_prefix_over_path() {
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::S3,
            prefix: Some("exports/2026-05-21/orders/".into()),
            path: Some("/scratch".into()),
            ..Default::default()
        };
        assert_eq!(
            resolved_prefix_for_display(&dest),
            "exports/2026-05-21/orders/",
        );
    }

    #[test]
    fn resolved_prefix_falls_back_to_path_when_prefix_missing() {
        let dest = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            prefix: None,
            path: Some("/data/out".into()),
            ..Default::default()
        };
        assert_eq!(resolved_prefix_for_display(&dest), "/data/out");
    }
}
