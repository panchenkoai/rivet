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
use crate::pipeline::validate_manifest::{ValidateDepth, verify_at_destination};

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
    /// `--depth light|sample|full` — the graded verify layer (see
    /// [`ValidateDepth`]).  Defaults (`ValidateDepth::default()` →
    /// [`ValidateDepth::Full`]) to the pre-graded behaviour: all five
    /// sections **plus** the Form B value re-read, so existing callers
    /// constructing `ValidateTarget::default()` are unchanged.
    pub depth: ValidateDepth,
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
        match verify_at_destination(&*dest, "", target.depth) {
            Ok(mut v) => {
                // Apply this export's `verify` policy: `content` fails the
                // verdict when any part is only size-verified (review D).
                v.enforce_content_policy(export.verify.requires_content());
                // Finding #20: when the operator pinned a literal `--prefix`,
                // they asserted a real dataset lives here. An absent manifest is
                // then NOT the benign M6 legacy-run case (exit 0) — it almost
                // always means the prefix was never written (a misconfigured CI
                // gate `rivet validate && deploy` sailing past nothing). Escalate
                // that exact shape (no manifest, no other failure) to a fatal
                // `ManifestRequiredButAbsent` so the exit gate refuses it loudly
                // instead of silently passing. No-op for every other shape (a
                // real manifest, or an absent one already carrying a read error).
                if target.prefix_override.is_some() {
                    v.require_manifest_present(&resolved_prefix);
                }
                // Capture the verdict before `v` is moved into the result: the
                // deeper Form B checksum re-read below must run *only* on a
                // manifest that was found and passed the standard checks.
                let manifest_verified = v.manifest_found && v.passed;
                all_results.push(ExportVerdict {
                    name: export.name.clone(),
                    resolved_prefix,
                    verification: v,
                });
                // CDC-specific: re-read the parts and confirm `__pos` stayed in
                // source-log order (no reorder / no part-boundary overlap). The
                // manifest check above already covered per-part MD5 / size / _SUCCESS.
                if export.mode == crate::config::ExportMode::Cdc
                    && export.format == crate::config::FormatType::Parquet
                {
                    match crate::source::cdc::validate::check_positions(&*dest, "") {
                        Ok(pc) if pc.is_ok() => log::info!(
                            "export '{}': cdc __pos continuity OK — {} changes across {} parts, range {:?}..{:?}",
                            export.name,
                            pc.rows,
                            pc.parts,
                            pc.first,
                            pc.last
                        ),
                        Ok(pc) => {
                            for v in &pc.violations {
                                hard_failures
                                    .push(format!("export '{}': cdc __pos: {}", export.name, v));
                            }
                        }
                        Err(e) => hard_failures.push(format!(
                            "export '{}': cdc __pos check failed: {:#}",
                            export.name, e
                        )),
                    }
                }
                // Form B: re-read the parts and verify the per-column value
                // checksums recorded in the manifest (catches an Arrow→Parquet
                // encode / post-write fault the in-process Form A cannot see).
                // Gated on a found+passed manifest: an absent (legacy pass) or
                // unreadable manifest is already accounted for above, so re-reading
                // it here would either break the legacy pass or double-count.
                //
                // Graded depth: Form B is the **only** part-download step, so it
                // runs at `--depth full` alone.  `light` and `sample` deliberately
                // skip it — `sample` is "all structural checks, no part bodies".
                if target.depth == ValidateDepth::Full
                    && manifest_verified
                    && export.format == crate::config::FormatType::Parquet
                    && let Err(e) =
                        crate::source::value_checksum::validate_manifest_checksums(&*dest, "")
                {
                    hard_failures
                        .push(format!("export '{}': value checksum: {:#}", export.name, e));
                }
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
    // verdict surfaced an explicit failure it could not pass over
    // (`verdict_fails_exit`) — an M5 verification failure on a found
    // manifest (missing part, size mismatch, stale _SUCCESS,
    // self-inconsistent manifest) or a manifest that could not even be
    // read (`ManifestReadError`: `manifest_found` is false, but the
    // verifier has a concrete reason to refuse, not a legacy prefix).
    // Surplus untracked objects (`UntrackedObject`) are surfaced in
    // `failures` for operator audit but do NOT flip `passed`, because
    // their cleanup is M9's job (resume), not validate's.  An operator
    // who wants strict "no surplus allowed" can grep the JSON report for
    // `kind: untracked_object` themselves; a future
    // `rivet validate --strict` flag may surface that exit-code mode if
    // demand appears (out of scope for this PR).
    //
    // Legacy runs (M6) keep exit 0: `passed: false` with no failures
    // means "verifier cannot certify", not "verifier found a problem".
    let failed_verdicts = all_results
        .iter()
        .filter(|r| verdict_fails_exit(&r.verification))
        .count();
    if failed_verdicts > 0 {
        // A verified-and-wrong verdict (missing part, size mismatch, stale
        // _SUCCESS, self-inconsistent manifest) is the data-integrity class
        // (exit 3) — typed so a scheduler stops rather than blindly retries.
        // `hard_failures` (couldn't open / read the destination) are operational
        // "could not verify", not "verified wrong", so they fold into the count
        // but the class is driven by the real verdict failure.
        return Err(crate::error::DataIntegrityError::new(format!(
            "rivet validate: {} export(s) failed verification",
            hard_failures.len() + failed_verdicts
        ))
        .into());
    }
    if !hard_failures.is_empty() {
        // Could-not-verify only (no verified-wrong verdict): operational, generic.
        anyhow::bail!(
            "rivet validate: {} export(s) failed verification",
            hard_failures.len()
        );
    }
    Ok(())
}

/// Exit-code predicate for one export's verdict: non-zero iff the verifier
/// surfaced an explicit failure (`has_failures` — "a reason an orchestrator
/// should refuse the run") on a verdict that did not pass.  Both documented
/// exit-0 cases survive: legacy runs (M6 — `passed: false` with no failures
/// is "cannot certify", not "found a problem") and advisory-only verdicts
/// (`UntrackedObject` never flips `passed`).
fn verdict_fails_exit(v: &ManifestVerification) -> bool {
    !v.passed && v.has_failures()
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
        // Graded verify layer: surface how deep this pass went so a reader
        // knows whether a PASSED verdict reconciled parts (sample/full) or
        // only the manifest + _SUCCESS (light).
        let _ = writeln!(h, "  depth:     {}", v.depth_level);
        if v.legacy_run {
            let _ = writeln!(
                h,
                "  status:    legacy_run (no manifest at destination — pre-0.7.0 prefix)"
            );
            continue;
        }
        if !v.manifest_found {
            let _ = writeln!(h, "  status:    NO MANIFEST");
            // A read-error verdict lands here (manifest present but
            // unreadable, or head failed): its `failures` are the
            // operator's only signal, so print them before bailing out
            // of this export's section.  Each line carries its stable
            // `RIVET_VERIFY_*` code in brackets so CI can grep it.
            for failure in &v.failures {
                let _ = writeln!(h, "  failure:   [{}] {}", failure.error_code(), failure);
            }
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
            // `Failure: Display` is the single source of truth for the message;
            // same string the run report uses.  L14: advisory (non-fatal)
            // entries — `UntrackedObject` surplus — are labelled "warning:" not
            // "failure:".  They never flip `passed` and never change the exit
            // code (cleanup is `--resume`'s job, M9), so rendering them as
            // "failure:" beside exit 0 was contradictory.  Fatal failures keep
            // the "failure:" label.
            let label = if failure.is_fatal() {
                "failure:"
            } else {
                "warning:"
            };
            // Stable `RIVET_VERIFY_*` code in brackets ahead of the human
            // message so an orchestrator can branch on the code without
            // parsing the prose.
            let _ = writeln!(h, "  {}   [{}] {}", label, failure.error_code(), failure);
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

/// Serialize one [`ManifestVerificationFailure`] to JSON with its stable
/// `RIVET_VERIFY_*` code injected next to `kind`.
///
/// The derive emits `{ "kind": "...", <variant fields> }`; this adds `"code"`
/// so a consumer can branch on the code without re-deriving it from `kind`.
/// Returns the enriched object (or the unmodified serde value if, impossibly,
/// the failure didn't serialize as a JSON object).
fn failure_json(f: &crate::pipeline::ManifestVerificationFailure) -> serde_json::Value {
    let mut value = serde_json::json!(f);
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "code".to_string(),
            serde_json::Value::String(f.error_code().to_string()),
        );
    }
    value
}

/// Serialize a [`ManifestVerification`] to JSON, replacing the derive's plain
/// `failures` array with one whose entries each carry their `RIVET_VERIFY_*`
/// `code`.  All other fields (`depth_level`, `passed`, counts, …) ride the
/// derive unchanged, so the stable wire contract is preserved and only widened.
fn verification_json(v: &ManifestVerification) -> serde_json::Value {
    let mut value = serde_json::json!(v);
    if let Some(obj) = value.as_object_mut() {
        let failures: Vec<serde_json::Value> = v.failures.iter().map(failure_json).collect();
        obj.insert("failures".to_string(), serde_json::Value::Array(failures));
    }
    value
}

fn render_json(
    results: &[ExportVerdict],
    hard_failures: &[String],
    out_path: Option<String>,
) -> Result<()> {
    // L14: surface advisory (non-fatal) entries — `UntrackedObject` surplus —
    // in a dedicated top-level `warnings` array so a consumer can tell at a
    // glance that "failures means failures".  The per-export
    // `verification.failures` array is the stable wire contract (consumers
    // branch on `failures[].kind`), so advisory entries stay there too — this
    // is an additive lens over the same data, not a relocation.  Each entry
    // also carries its stable `RIVET_VERIFY_*` `code` next to `kind`.
    let warnings: Vec<serde_json::Value> = results
        .iter()
        .flat_map(|r| {
            r.verification
                .failures
                .iter()
                .filter(|f| !f.is_fatal())
                .map(move |f| {
                    serde_json::json!({
                        "export_name": r.name,
                        "warning": failure_json(f),
                    })
                })
        })
        .collect();

    let payload = serde_json::json!({
        "exports": results
            .iter()
            .map(|r| {
                serde_json::json!({
                    "export_name": r.name,
                    "resolved_prefix": r.resolved_prefix,
                    "verification": verification_json(&r.verification),
                })
            })
            .collect::<Vec<_>>(),
        "warnings": warnings,
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
            ..Default::default()
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

    // ── verdict_fails_exit (exit-code policy) ──────────────────────────────

    use crate::pipeline::ManifestVerificationFailure as VFailure;

    /// Verdict shape `verify_at_destination` returns when `manifest.json`
    /// exists but cannot be read: not legacy, not passed, one explicit
    /// `ManifestReadError`.
    fn read_error_verdict() -> ManifestVerification {
        ManifestVerification {
            legacy_run: false,
            failures: vec![VFailure::ManifestReadError {
                detail: "permission denied".into(),
            }],
            ..ManifestVerification::legacy()
        }
    }

    #[test]
    fn exit_gate_counts_manifest_read_error_as_failure() {
        assert!(verdict_fails_exit(&read_error_verdict()));
    }

    #[test]
    fn exit_gate_keeps_legacy_run_at_zero() {
        // M6: no manifest, no failures — "cannot certify" is not "found a
        // problem".
        assert!(!verdict_fails_exit(&ManifestVerification::legacy()));
    }

    #[test]
    fn exit_gate_keeps_advisory_untracked_at_zero() {
        let v = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            passed: true,
            parts_verified: 1,
            failures: vec![VFailure::UntrackedObject {
                key: "stray.parquet".into(),
                size_bytes: 9,
            }],
            ..ManifestVerification::legacy()
        };
        assert!(!verdict_fails_exit(&v));
    }

    #[test]
    fn exit_gate_counts_fatal_failure_on_found_manifest() {
        let v = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            failures: vec![VFailure::PartMissing {
                part_id: 1,
                path: "part-000001.parquet".into(),
            }],
            ..ManifestVerification::legacy()
        };
        assert!(verdict_fails_exit(&v));
    }

    // ── run_validate_command end-to-end (local destination; the source URL
    //     is never dialed — see tests/validate_historical.rs) ──────────────

    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        PartStatus, RunManifest,
    };

    fn success_manifest(parts: Vec<ManifestPart>) -> RunManifest {
        let row_count: i64 = parts.iter().map(|p| p.rows).sum();
        let part_count = parts.len() as u32;
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: "r-validate-cmd".into(),
            export_name: "orders".into(),
            started_at: "2026-06-09T12:00:00Z".into(),
            finished_at: "2026-06-09T12:01:00Z".into(),
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
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    /// Land `manifest.json` + `_SUCCESS` at `prefix` via the public writer
    /// surface — same path the `rivet run` end-of-run writer takes.
    fn stage_dataset(prefix: &Path, m: &RunManifest) {
        std::fs::create_dir_all(prefix).unwrap();
        let dest = crate::destination::create_destination(&crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(prefix.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();
        crate::pipeline::write_manifest(&*dest, m).unwrap();
    }

    /// Config with a single export pointing at `prefix`.  Written next to —
    /// never inside — the prefix, so it can't surface as untracked surplus.
    fn write_cfg(dir: &Path, prefix: &Path) -> std::path::PathBuf {
        let cfg = dir.join("rivet.yaml");
        let yaml = format!(
            "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{}\"\n",
            prefix.to_string_lossy()
        );
        std::fs::write(&cfg, yaml).unwrap();
        cfg
    }

    /// In-process twin of the live roast test (tests/roast_validate_exit.rs):
    /// `manifest.json` present but unreadable must exit non-zero.  head()
    /// (fs::metadata) succeeds, read() (fs::read) hits EACCES — exactly the
    /// `ManifestReadError` verdict.
    #[cfg(unix)]
    #[test]
    fn unreadable_manifest_fails_the_command() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        let cfg = write_cfg(dir.path(), &prefix);

        let manifest_path = prefix.join(crate::manifest::MANIFEST_FILENAME);
        std::fs::set_permissions(&manifest_path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&manifest_path).is_ok() {
            // euid 0 ignores file modes — the degraded state can't be staged.
            eprintln!("skipping unreadable_manifest_fails_the_command: running as root");
            return;
        }

        let report = dir.path().join("report.json");
        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect_err("an unreadable manifest is an explicit failure, not exit 0");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );

        // The JSON report (written before the bail) still carries the
        // verdict so the operator sees why.
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["manifest_found"], false);
        assert_eq!(verification["legacy_run"], false);
        assert_eq!(verification["failures"][0]["kind"], "manifest_read_error");
    }

    #[test]
    fn untracked_surplus_alone_keeps_exit_zero() {
        // The advisory neighbor of the read-error fix: gating on
        // `has_failures()` alone would flip this verdict to non-zero, but
        // surplus cleanup is `--resume`'s job (M9), not validate's.
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        std::fs::write(prefix.join("rogue.parquet"), b"XX").unwrap();
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect("advisory untracked surplus must not flip the exit code");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["passed"], true);
        // The stable wire contract is preserved: untracked entries still ride
        // `verification.failures` (consumers branch on `failures[].kind`).
        assert_eq!(verification["failures"][0]["kind"], "untracked_object");

        // L14: …and the same advisory entry is also surfaced in the top-level
        // `warnings` array so "failures means failures" — an exit-0 verdict no
        // longer hides a surplus object under a "failure" label.
        let warnings = json["warnings"].as_array().expect("warnings array present");
        assert_eq!(warnings.len(), 1, "the untracked surplus is one warning");
        assert_eq!(warnings[0]["export_name"], "orders");
        assert_eq!(warnings[0]["warning"]["kind"], "untracked_object");
        assert_eq!(warnings[0]["warning"]["key"], "rogue.parquet");
    }

    #[test]
    fn json_warnings_array_is_empty_when_no_advisory_failures() {
        // A clean dataset with no surplus → no warnings.  Guards against the
        // `warnings` lens accidentally picking up fatal failures.
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect("a clean dataset must pass");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        assert_eq!(
            json["warnings"]
                .as_array()
                .expect("warnings array present")
                .len(),
            0,
            "no surplus → no warnings"
        );
    }

    #[test]
    fn missing_part_fails_the_command() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        let m = success_manifest(vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 10,
            size_bytes: 4,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }]);
        stage_dataset(&prefix, &m); // the part itself is never written
        let cfg = write_cfg(dir.path(), &prefix);

        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget::default(),
        )
        .expect_err("a missing committed part must fail verification");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );
    }

    // ── finding #20: operator-pinned --prefix requires a manifest ────────────

    /// `--prefix` at a real, complete dataset still passes — the normal
    /// "validate exactly this prefix" case must not regress.
    #[test]
    fn prefix_override_with_real_manifest_passes() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        let cfg = write_cfg(dir.path(), &prefix);

        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget {
                prefix_override: Some(prefix.to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .expect("a real dataset under a pinned --prefix must pass");
    }

    /// `--prefix` at a never-written directory FAILS (exit non-zero): the
    /// operator asserted a dataset lives here, so an absent manifest is a
    /// refusal reason, not the benign legacy-run pass. This is the in-process
    /// twin of the live `audit_validate_absent_prefix_can_fail` roast.
    #[test]
    fn prefix_override_at_absent_manifest_fails() {
        let dir = tempfile::tempdir().unwrap();
        // The export's config destination is irrelevant — `--prefix` overrides
        // it. Point the override at a dir that exists but was never written.
        let cfg_prefix = dir.path().join("cfg_dest");
        std::fs::create_dir_all(&cfg_prefix).unwrap();
        let cfg = write_cfg(dir.path(), &cfg_prefix);
        let empty_prefix = dir.path().join("never_written");
        std::fs::create_dir_all(&empty_prefix).unwrap();

        let report = dir.path().join("report.json");
        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                prefix_override: Some(empty_prefix.to_string_lossy().into_owned()),
                ..Default::default()
            },
        )
        .expect_err("a never-written prefix pinned via --prefix must fail, not legacy-pass");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );

        // The verdict (written before the bail) carries the explicit reason so
        // the operator sees why the gate refused, not a bare exit code.
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["manifest_found"], false);
        assert_eq!(verification["legacy_run"], false);
        assert_eq!(
            verification["failures"][0]["kind"],
            "manifest_required_but_absent"
        );
    }

    /// Without `--prefix`, an absent manifest stays the benign M6 legacy-run
    /// pass (exit 0) — today's behaviour is preserved for config-resolved
    /// destinations that may legitimately be pre-0.7.0 prefixes.
    #[test]
    fn absent_manifest_without_prefix_override_stays_legacy_pass() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        std::fs::create_dir_all(&prefix).unwrap(); // exists, but no manifest
        let cfg = write_cfg(dir.path(), &prefix);

        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget::default(), // no --prefix
        )
        .expect("an absent manifest with no pinned --prefix is a legacy pass (exit 0)");
    }

    // ── graded verify layer (--depth) end-to-end ─────────────────────────

    /// Stage a dataset that passes sections 1-5 (manifest reads + is
    /// self-consistent, the single part is present at the recorded size,
    /// `_SUCCESS` matches) **but** records a non-empty `column_checksums`, so
    /// the Form B re-read is *reachable*. The part body is deliberately NOT
    /// valid Parquet, so if Form B runs it errors on the Parquet open — making
    /// "did Form B run?" observable as a pass/fail of the command.
    fn stage_dataset_form_b_would_fail(prefix: &Path) {
        std::fs::create_dir_all(prefix).unwrap();
        // 4-byte non-Parquet body; the manifest records size 4 so the part
        // reconcile (size-only, empty content_md5) passes.
        let part_body: &[u8] = b"AAAA";
        std::fs::write(prefix.join("part-000001.parquet"), part_body).unwrap();

        let mut m = success_manifest(vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 1,
            size_bytes: part_body.len() as u64,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }]);
        // Non-empty → Form B does NOT early-return; it proceeds to read the
        // (garbage) part as Parquet and fail.
        m.column_checksums = Some(vec![crate::manifest::ColumnChecksum {
            name: "id".into(),
            checksum: "0".into(),
        }]);
        stage_dataset(prefix, &m);
    }

    #[test]
    fn sample_depth_does_not_run_form_b() {
        // At `--depth sample` the structural checks (parts present, _SUCCESS,
        // self-consistency) all pass and the Form B value re-read is skipped —
        // so the command succeeds even though the part body is not real Parquet.
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset_form_b_would_fail(&prefix);
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                depth: ValidateDepth::Sample,
                ..Default::default()
            },
        )
        .expect("sample depth skips Form B, so a non-Parquet part still passes");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        assert_eq!(verification["passed"], true);
        assert_eq!(verification["parts_verified"], 1, "sample reconciles parts");
        assert_eq!(verification["depth_level"], "sample");
    }

    #[test]
    fn full_depth_runs_form_b() {
        // The contrast: identical dataset, `--depth full`. Sections 1-5 still
        // pass (so `manifest_verified` is true and Form B is gated open), Form B
        // re-reads the part, fails to parse it as Parquet, and the command exits
        // non-zero. Proves Form B runs at full depth and only at full depth.
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset_form_b_would_fail(&prefix);
        let cfg = write_cfg(dir.path(), &prefix);

        let err = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(None),
            ValidateTarget {
                depth: ValidateDepth::Full,
                ..Default::default()
            },
        )
        .expect_err("full depth runs Form B, which fails on a non-Parquet part");
        assert!(
            format!("{err:#}").contains("1 export(s) failed verification"),
            "got: {err:#}"
        );
    }

    #[test]
    fn json_report_carries_failure_code_and_depth_level() {
        // render_json injects the stable `RIVET_VERIFY_*` code next to each
        // failure's `kind`, and the verdict carries the `depth_level` it ran at.
        // A missing committed part gives us a fatal failure to inspect.
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        let m = success_manifest(vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 10,
            size_bytes: 4,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }]);
        stage_dataset(&prefix, &m); // the part itself is never written → PartMissing
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        let _ = run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget {
                depth: ValidateDepth::Sample,
                ..Default::default()
            },
        )
        .expect_err("a missing part fails the command");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let verification = &json["exports"][0]["verification"];
        // depth_level surfaces the level the pass ran at.
        assert_eq!(verification["depth_level"], "sample");
        // The PartMissing failure carries BOTH its stable kind and its code.
        let failure = &verification["failures"][0];
        assert_eq!(failure["kind"], "part_missing");
        assert_eq!(failure["code"], "RIVET_VERIFY_PART_MISSING");
        // The per-variant fields ride alongside (the derive output is widened,
        // not replaced).
        assert_eq!(failure["part_id"], 1);
    }

    #[test]
    fn json_warning_entry_also_carries_its_code() {
        // The advisory `warnings` lens carries the code too — an untracked
        // surplus surfaces `RIVET_VERIFY_UNTRACKED_OBJECT` without flipping exit.
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("out");
        stage_dataset(&prefix, &success_manifest(Vec::new()));
        std::fs::write(prefix.join("rogue.parquet"), b"XX").unwrap();
        let cfg = write_cfg(dir.path(), &prefix);

        let report = dir.path().join("report.json");
        run_validate_command(
            cfg.to_str().unwrap(),
            Some("orders"),
            ValidateOutputFormat::Json(Some(report.to_string_lossy().into_owned())),
            ValidateTarget::default(),
        )
        .expect("advisory untracked surplus must not flip the exit code");

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
        let warning = &json["warnings"][0]["warning"];
        assert_eq!(warning["kind"], "untracked_object");
        assert_eq!(warning["code"], "RIVET_VERIFY_UNTRACKED_OBJECT");
        // And the default depth (full) is recorded on the verdict.
        assert_eq!(json["exports"][0]["verification"]["depth_level"], "full");
    }
}
