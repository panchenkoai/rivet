//! **`rivet plan`** — build and display a `PlanArtifact` without executing.
//!
//! This module is the coordinator for the plan command. It:
//!
//! 1. Loads config and resolves the `ResolvedRunPlan` (same as `rivet run`).
//! 2. Runs preflight diagnostics via `preflight::get_export_diagnostic`.
//! 3. For `Chunked` exports: opens a source connection and pre-computes chunk
//!    boundaries using `detect_and_generate_chunks` — no data is exported.
//! 4. For `Incremental` exports: reads the current cursor from `StateStore`.
//! 5. Packages everything into a `PlanArtifact` and either writes it to a file
//!    or prints a human-readable summary to stdout.

use std::collections::HashMap;
use std::path::Path;

use crate::config::Config;
use crate::error::Result;
use crate::plan::{
    ComputedPlanData, ExportRecommendation, ExtractionStrategy, PlanArtifact, PlanDiagnostics,
    PlanPrioritizationSnapshot, PrioritizationInputs, build_plan,
    campaign::recommend_campaign,
    inputs::{PrioritizationHints, build_prioritization_inputs},
    recommend::recommend_export,
    validate::{DiagnosticLevel, validate_plan},
};
use crate::state::StateStore;
use crate::{preflight, source};

use super::chunked::{chunk_plan_fingerprint, detect_and_generate_chunks};

/// Output format for `rivet plan`.
pub enum PlanOutputFormat {
    /// Human-readable summary printed to stdout; no file written.
    Pretty,
    /// Pretty-printed JSON written to the given path (or stdout if `None`).
    Json(Option<String>),
}

/// Entry point for `rivet plan`.
///
/// Iterates over all matching exports, builds a `PlanArtifact` for each, and
/// either prints or saves it according to `format`.
pub fn run_plan_command(
    config_path: &str,
    export_name: Option<&str>,
    params: Option<&HashMap<String, String>>,
    format: PlanOutputFormat,
) -> Result<()> {
    let config = Config::load_with_params(config_path, params)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let exports: Vec<_> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    let state_path = config_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    let mut built: Vec<(PlanArtifact, PrioritizationInputs, ExportRecommendation)> = Vec::new();

    for export in exports {
        built.push(build_plan_artifact(
            &config,
            export,
            config_path,
            config_dir,
            params,
            &state,
        )?);
    }

    let campaign_opt = if built.len() > 1 {
        let pairs: Vec<_> = built
            .iter()
            .map(|(_, i, r)| (i.clone(), r.clone()))
            .collect();
        Some(recommend_campaign(pairs))
    } else {
        None
    };

    // When `--output FILE` is given but the config yields more than one export,
    // a single path cannot hold every artifact: `rivet apply` reads exactly one
    // `PlanArtifact` per file (`PlanArtifact::from_file`), and a JSON array is
    // not a valid artifact. Writing all of them to the same path silently kept
    // only the last (audit #4). Instead, derive a distinct per-export path so no
    // export is dropped and each file remains directly consumable by `apply`.
    let multi_export = built.len() > 1;

    let mut artifacts: Vec<PlanArtifact> = Vec::with_capacity(built.len());
    for (mut artifact, _inputs, standalone_rec) in built {
        let snap = if let Some(ref camp) = campaign_opt {
            let rec = camp
                .ordered_exports
                .iter()
                .find(|e| e.export_name == artifact.export_name)
                .cloned()
                .unwrap_or_else(|| standalone_rec.clone());
            PlanPrioritizationSnapshot {
                export_recommendation: rec,
                campaign: Some(camp.clone()),
            }
        } else {
            PlanPrioritizationSnapshot {
                export_recommendation: standalone_rec,
                campaign: None,
            }
        };
        artifact.prioritization = Some(snap);
        artifacts.push(artifact);
    }

    emit_artifacts(&artifacts, &format, multi_export)?;

    Ok(())
}

/// Derive a distinct per-export output path from the `--output` value when more
/// than one export is being planned.
///
/// Inserts the export name into the file stem (`plan.json` → `plan.orders.json`)
/// so every artifact lands on its own path and `rivet apply <file>` consumes a
/// single artifact unchanged. The export name is sanitised to a safe filename
/// fragment (any non-alphanumeric/`-`/`_` byte becomes `_`) so an export named
/// with a slash or dot cannot escape the intended directory or mangle the
/// extension. The original extension is preserved so per-export files keep the
/// same suffix (`.json`) the operator asked for.
fn per_export_output_path(base: &str, export_name: &str) -> String {
    let sanitized: String = export_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let path = Path::new(base);
    let parent = path.parent();
    let stem = path.file_stem().and_then(|s| s.to_str());
    let ext = path.extension().and_then(|s| s.to_str());

    let file_name = match (stem, ext) {
        (Some(stem), Some(ext)) => format!("{stem}.{sanitized}.{ext}"),
        (Some(stem), None) => format!("{stem}.{sanitized}"),
        // No usable stem (e.g. path was empty or just an extension): fall back to
        // appending the sanitized name so the artifact still lands on a distinct
        // path rather than colliding.
        (None, _) => format!("{base}.{sanitized}"),
    };

    match parent {
        Some(dir) if !dir.as_os_str().is_empty() => {
            dir.join(file_name).to_string_lossy().into_owned()
        }
        _ => file_name,
    }
}

fn build_plan_artifact(
    config: &Config,
    export: &crate::config::ExportConfig,
    config_path: &str,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    state: &StateStore,
) -> Result<(PlanArtifact, PrioritizationInputs, ExportRecommendation)> {
    let plan = build_plan(config, export, config_dir, false, false, false, params)?;

    // Collect plan-level compatibility diagnostics and emit Rejected ones as errors.
    let validate_diags = validate_plan(&plan);
    let mut validate_warnings: Vec<String> = Vec::new();
    for d in &validate_diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                anyhow::bail!("[{}] {}", d.rule, d.message);
            }
            DiagnosticLevel::Warning | DiagnosticLevel::Degraded => {
                validate_warnings.push(format!("[{}] {}", d.rule, d.message));
            }
        }
    }

    let (computed, plan_diagnostics, hints) = match preflight::get_export_diagnostic(config, export)
    {
        Ok(diag) => {
            let mut warnings = diag.warnings.clone();
            warnings.extend(validate_warnings);
            // F3 (0.7.5 audit): the JSON artifact's `diagnostics`
            // exposed a non-Efficient verdict with `warnings: []`, so a
            // machine consumer could not see *why* the plan was flagged.
            // If preflight emitted no specific warning, fall back to the
            // build_suggestion text (the same line shown in `rivet check`)
            // so the JSON always carries at least one human-readable
            // reason matching the verdict.
            if warnings.is_empty() && !matches!(diag.verdict, preflight::HealthVerdict::Efficient) {
                if let Some(s) = diag.suggestion.clone() {
                    warnings.push(s);
                } else {
                    warnings.push(format!(
                        "verdict {} but preflight collected no specific warnings — review `rivet check` output for context",
                        diag.verdict
                    ));
                }
            }
            let plan_diagnostics = PlanDiagnostics {
                verdict: diag.verdict.to_string(),
                warnings,
                recommended_profile: diag.recommended_profile.to_string(),
            };
            let computed = compute_plan_data(&plan, diag.row_estimate, state)?;
            let hints = PrioritizationHints {
                incremental_uses_index: diag.uses_index,
                cursor_range_observed: diag.cursor_min.is_some() && diag.cursor_max.is_some(),
            };
            (computed, plan_diagnostics, hints)
        }
        Err(e) => {
            log::warn!(
                "plan '{}': preflight diagnostics failed (continuing without them): {:#}",
                export.name,
                e
            );
            let computed = compute_plan_data(&plan, None, state)?;
            let mut warnings = vec!["preflight diagnostics unavailable".into()];
            warnings.extend(validate_warnings);
            let plan_diagnostics = PlanDiagnostics {
                verdict: "unknown (preflight failed)".into(),
                warnings,
                recommended_profile: "balanced".into(),
            };
            (computed, plan_diagnostics, PrioritizationHints::default())
        }
    };

    let fingerprint = match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.chunk_count,
            cp.dense,
            cp.by_days,
        ),
        _ => String::new(),
    };

    // Epic I: fold recent-run history into prioritization (bounded contribution).
    let history = match state.get_metrics(Some(&export.name), 20) {
        Ok(metrics) => Some(crate::plan::HistorySnapshot::summarize(&metrics)),
        Err(e) => {
            log::warn!(
                "plan '{}': history lookup failed; proceeding without historical refinement: {:#}",
                export.name,
                e
            );
            None
        }
    };

    let inputs =
        build_prioritization_inputs(export, &plan, &computed, &plan_diagnostics, hints, history);
    let recommendation = recommend_export(&inputs, &plan_diagnostics);

    let mut artifact = PlanArtifact::new(
        plan.export_name.clone(),
        plan.strategy.mode_label().to_string(),
        fingerprint,
        plan,
        computed,
        plan_diagnostics,
    );

    // F13 (0.7.5 audit): record the absolute config path so `rivet
    // apply` can locate the matching `.rivet_state.db` (cursors,
    // manifest history) even when the plan JSON is stored in a
    // different directory.  `canonicalize` resolves symlinks and
    // produces an absolute path; we fall back to the original string
    // if the path no longer resolves (rare, e.g. config deleted
    // between plan and apply).
    artifact.config_path = Some(
        Path::new(config_path)
            .canonicalize()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| config_path.to_string()),
    );

    Ok((artifact, inputs, recommendation))
}

/// Compute the `ComputedPlanData` portion of the artifact.
///
/// For `Chunked` exports this opens a source connection and calls
/// `detect_and_generate_chunks` to pre-compute chunk boundaries.  No rows are
/// exported — we only run the `SELECT min(col) / max(col)` boundary queries.
///
/// For `Incremental` exports we read the last cursor value from `StateStore`.
fn compute_plan_data(
    plan: &crate::plan::ResolvedRunPlan,
    row_estimate: Option<i64>,
    state: &StateStore,
) -> Result<ComputedPlanData> {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => {
            let mut src = source::create_source(&plan.source)?;
            let chunk_ranges = detect_and_generate_chunks(
                &mut *src,
                &plan.base_query,
                &cp.column,
                cp.chunk_size,
                cp.chunk_count,
                &plan.export_name,
                cp.dense,
                cp.by_days,
                plan.source.source_type,
            )?;
            let chunk_count = chunk_ranges.len();
            // F4 (0.7.5 audit): for chunked exports the artifact already
            // contains the exact key span (`chunk_ranges[0].0` ..
            // `chunk_ranges[-1].1`).  Using that as `row_estimate`
            // beats `pg_class.reltuples` (which is `1130` on a fresh
            // 30-row PG table because `ANALYZE` hasn't run).  This
            // makes PG and MySQL agree on the same artifact.
            let chunked_estimate = chunk_ranges
                .first()
                .zip(chunk_ranges.last())
                .map(|(first, last)| (last.1 - first.0 + 1).max(0));
            Ok(ComputedPlanData {
                chunk_ranges,
                chunk_count,
                cursor_snapshot: None,
                row_estimate: chunked_estimate.or(row_estimate),
            })
        }

        ExtractionStrategy::Incremental(_) => {
            let cursor_snapshot = state.get(&plan.export_name)?.last_cursor_value;
            Ok(ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot,
                row_estimate,
            })
        }

        // Keyset pages are computed dynamically at run time (seek pagination),
        // so there are no precomputed ranges to describe here — like a snapshot.
        ExtractionStrategy::Snapshot
        | ExtractionStrategy::TimeWindow { .. }
        | ExtractionStrategy::Keyset(_) => Ok(ComputedPlanData {
            chunk_ranges: vec![],
            chunk_count: 0,
            cursor_snapshot: None,
            row_estimate,
        }),
    }
}

fn emit_artifacts(
    artifacts: &[PlanArtifact],
    format: &PlanOutputFormat,
    multi_export: bool,
) -> Result<()> {
    match format {
        PlanOutputFormat::Pretty => {
            for artifact in artifacts {
                artifact.print_summary();
            }
        }
        // Multi-export JSON to stdout: a single export prints its object
        // unchanged, but printing N objects back-to-back is invalid JSON (audit
        // #L10: `jq` fails with "Extra data"). Wrap them in a JSON array so the
        // stream parses as one document.
        PlanOutputFormat::Json(None) => {
            if artifacts.len() > 1 {
                // `&[PlanArtifact]` serializes as a JSON array — one parseable
                // document rather than N concatenated objects.
                println!("{}", serde_json::to_string_pretty(artifacts)?);
            } else if let Some(artifact) = artifacts.first() {
                println!("{}", artifact.to_json_pretty()?);
            }
        }
        PlanOutputFormat::Json(Some(path)) => {
            for artifact in artifacts {
                let json = artifact.to_json_pretty()?;
                // For a single export keep the operator's exact path so `rivet
                // apply <path>` works verbatim. For multiple exports a single
                // path would overwrite (audit #4): give each export its own file.
                let out_path = if multi_export {
                    per_export_output_path(path, &artifact.export_name)
                } else {
                    path.clone()
                };
                std::fs::write(&out_path, &json)
                    .map_err(|e| anyhow::anyhow!("cannot write plan file '{}': {}", out_path, e))?;
                println!("Plan written to: {}", out_path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::per_export_output_path;
    use std::path::Path;

    /// Regression (audit #4): two distinct exports under one `--output FILE`
    /// must derive two distinct paths, so neither is silently overwritten.
    #[test]
    fn per_export_paths_are_distinct() {
        let a = per_export_output_path("/tmp/plan.json", "orders");
        let b = per_export_output_path("/tmp/plan.json", "users");
        assert_ne!(a, b, "distinct exports must not collide on one path");
        assert_eq!(a, "/tmp/plan.orders.json");
        assert_eq!(b, "/tmp/plan.users.json");
    }

    /// The original extension is preserved so `rivet apply` (and any `*.json`
    /// glob) still find the per-export files.
    #[test]
    fn per_export_path_keeps_json_extension() {
        let p = per_export_output_path("/tmp/plan.json", "orders");
        assert_eq!(
            Path::new(&p).extension().and_then(|e| e.to_str()),
            Some("json"),
            "per-export file must keep the .json extension"
        );
    }

    /// An export name with no extension on the base still gets a distinct,
    /// non-colliding suffix rather than overwriting the base path.
    #[test]
    fn per_export_path_without_extension_appends_name() {
        let p = per_export_output_path("/tmp/plan", "orders");
        assert_eq!(p, "/tmp/plan.orders");
    }

    /// A hostile export name (path separators, dots) must not escape the
    /// directory or mangle the extension — every unsafe byte is replaced.
    #[test]
    fn per_export_path_sanitizes_unsafe_export_name() {
        let p = per_export_output_path("/tmp/plan.json", "../../etc/passwd");
        // No remaining path separators or `..` traversal in the derived name.
        assert!(
            !p.contains(".."),
            "sanitized path must not contain a `..` traversal: {p}"
        );
        let file = Path::new(&p)
            .file_name()
            .and_then(|f| f.to_str())
            .expect("derived path has a file name");
        assert!(
            !file.contains('/') && !file.contains('\\'),
            "sanitized file name must not contain a path separator: {file}"
        );
        assert_eq!(
            Path::new(&p).parent().and_then(|d| d.to_str()),
            Some("/tmp"),
            "derived path must stay in the requested directory"
        );
        assert_eq!(
            Path::new(&p).extension().and_then(|e| e.to_str()),
            Some("json"),
            "extension must survive sanitization"
        );
    }
}
