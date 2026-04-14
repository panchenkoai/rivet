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
    ComputedPlanData, ExtractionStrategy, PlanArtifact, PlanDiagnostics, build_plan,
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

    for export in exports {
        let artifact = build_plan_artifact(&config, export, config_dir, params, &state)?;
        emit_artifact(&artifact, &format)?;
    }

    Ok(())
}

fn build_plan_artifact(
    config: &Config,
    export: &crate::config::ExportConfig,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    state: &StateStore,
) -> Result<PlanArtifact> {
    // 1. Resolve execution plan (same path as `rivet run`)
    let plan = build_plan(config, export, config_dir, false, false, false, params)?;

    // 2. Preflight diagnostics (DB queries — row estimate, index check, etc.)
    let diag = match preflight::get_export_diagnostic(config, export) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "plan '{}': preflight diagnostics failed (continuing without them): {:#}",
                export.name,
                e
            );
            // Return a minimal stub so plan generation continues
            return build_artifact_without_diagnostics(plan, state);
        }
    };

    let plan_diagnostics = PlanDiagnostics {
        verdict: diag.verdict.to_string(),
        warnings: diag.warnings.clone(),
        recommended_profile: diag.recommended_profile.to_string(),
    };

    // 3. Compute strategy-specific data
    let computed = compute_plan_data(&plan, diag.row_estimate, state)?;

    // 4. Build fingerprint (non-empty for Chunked only)
    let fingerprint = match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.dense,
            cp.by_days,
        ),
        _ => String::new(),
    };

    Ok(PlanArtifact::new(
        plan.export_name.clone(),
        plan.strategy.mode_label().to_string(),
        fingerprint,
        plan,
        computed,
        plan_diagnostics,
    ))
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
                &plan.export_name,
                cp.dense,
                cp.by_days,
                plan.source.source_type,
            )?;
            let chunk_count = chunk_ranges.len();
            Ok(ComputedPlanData {
                chunk_ranges,
                chunk_count,
                cursor_snapshot: None,
                row_estimate,
            })
        }

        ExtractionStrategy::Incremental { .. } => {
            let cursor_snapshot = state.get(&plan.export_name)?.last_cursor_value;
            Ok(ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot,
                row_estimate,
            })
        }

        ExtractionStrategy::Snapshot | ExtractionStrategy::TimeWindow { .. } => {
            Ok(ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate,
            })
        }
    }
}

/// Build a minimal artifact when preflight diagnostics fail (e.g., DB unreachable).
fn build_artifact_without_diagnostics(
    plan: crate::plan::ResolvedRunPlan,
    state: &StateStore,
) -> Result<PlanArtifact> {
    let computed = compute_plan_data(&plan, None, state)?;
    let fingerprint = match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.dense,
            cp.by_days,
        ),
        _ => String::new(),
    };
    let label = plan.strategy.mode_label().to_string();
    let name = plan.export_name.clone();
    Ok(PlanArtifact::new(
        name,
        label,
        fingerprint,
        plan,
        computed,
        PlanDiagnostics {
            verdict: "unknown (preflight failed)".into(),
            warnings: vec!["preflight diagnostics unavailable".into()],
            recommended_profile: "balanced".into(),
        },
    ))
}

fn emit_artifact(artifact: &PlanArtifact, format: &PlanOutputFormat) -> Result<()> {
    match format {
        PlanOutputFormat::Pretty => {
            artifact.print_summary();
        }
        PlanOutputFormat::Json(None) => {
            println!("{}", artifact.to_json_pretty()?);
        }
        PlanOutputFormat::Json(Some(path)) => {
            let json = artifact.to_json_pretty()?;
            std::fs::write(path, &json)
                .map_err(|e| anyhow::anyhow!("cannot write plan file '{}': {}", path, e))?;
            println!("Plan written to: {}", path);
        }
    }
    Ok(())
}
