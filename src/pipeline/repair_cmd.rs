//! `rivet repair` — targeted re-export of reconcile mismatches (Epic H).
//!
//! ## Flow
//!
//! 1. Build a `ReconcileReport` (either freshly, or load a previous JSON via
//!    `--report`).
//! 2. Derive a [`RepairPlan`](crate::plan::RepairPlan) — the set of chunk
//!    ranges that need re-export.
//! 3. Without `--execute` (default): emit the plan and exit.
//! 4. With `--execute`: re-run just those chunks via
//!    [`chunked::run_chunked_sequential`] using a `Precomputed` chunk source.
//!    Output files are written with the standard `<export>_<ts>_chunk<idx>.<ext>`
//!    naming — they are new files alongside the originals; Rivet does not
//!    delete or overwrite the old files.
//!
//! Progression semantics (ADR-0008): repair does **not** advance
//! `last_committed_*` — the committed boundary already covers the chunk index.
//! Operator runs `rivet reconcile` afterwards to advance `last_verified_*`.

use std::collections::HashMap;
use std::path::Path;

use crate::config::Config;
use crate::error::Result;
use crate::plan::{
    ExtractionStrategy, ReconcileReport, RepairAction, RepairOutcome, RepairPlan, RepairReport,
    ResolvedRunPlan, build_plan,
};
use crate::source;
use crate::state::StateStore;

use super::RunSummary;
use super::chunked::{ChunkSource, run_chunked_sequential};
use super::reconcile_cmd;

/// Output format for the repair plan / report.
pub enum RepairOutputFormat {
    /// Human-readable summary to stdout.
    Pretty,
    /// Pretty-printed JSON to the given path (or stdout if `None`).
    Json(Option<String>),
}

/// Source of the reconcile report used to derive the repair plan.
pub enum RepairReportSource {
    /// Read a reconcile report JSON from disk.
    File(String),
    /// Run reconcile in-process against the latest chunk run.
    Auto,
}

pub fn run_repair_command(
    config_path: &str,
    export_name: &str,
    params: Option<&HashMap<String, String>>,
    report_source: RepairReportSource,
    execute: bool,
    format: RepairOutputFormat,
) -> Result<()> {
    let config = Config::load_with_params(config_path, params)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let export = config
        .exports
        .iter()
        .find(|e| e.name == export_name)
        .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", export_name))?;

    let mut plan = build_plan(&config, export, config_dir, false, false, false, params)?;
    if !matches!(plan.strategy, ExtractionStrategy::Chunked(_)) {
        anyhow::bail!(
            "repair: '{}' mode — only chunked exports are supported in v1 (Epic H)",
            plan.strategy.mode_label()
        );
    }

    let state_path = config_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    let reconcile_report = load_or_build_reconcile(&plan, &state, report_source)?;
    let repair_plan = RepairPlan::from_reconcile(&reconcile_report);

    if !execute {
        emit_plan(&repair_plan, &format)?;
        return Ok(());
    }

    if repair_plan.is_empty() {
        println!(
            "repair: nothing to repair for '{}' (reconcile report is clean)",
            export_name
        );
        return Ok(());
    }

    let report = execute_repair(&mut plan, &state, repair_plan)?;
    emit_report(&report, &format)?;
    Ok(())
}

fn load_or_build_reconcile(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    source: RepairReportSource,
) -> Result<ReconcileReport> {
    match source {
        RepairReportSource::File(path) => {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("cannot read reconcile report '{}': {}", path, e))?;
            let r: ReconcileReport = serde_json::from_str(&raw)
                .map_err(|e| anyhow::anyhow!("invalid reconcile report '{}': {}", path, e))?;
            if r.export_name != plan.export_name {
                anyhow::bail!(
                    "repair: reconcile report is for export '{}' but config targets '{}'",
                    r.export_name,
                    plan.export_name
                );
            }
            Ok(r)
        }
        RepairReportSource::Auto => reconcile_cmd::reconcile_chunked_fresh(plan, state),
    }
}

fn execute_repair(
    plan: &mut ResolvedRunPlan,
    state: &StateStore,
    repair_plan: RepairPlan,
) -> Result<RepairReport> {
    // Map start/end strings → i64. Skip ranges that don't parse (recorded as skipped outcomes).
    let mut ranges: Vec<(i64, i64)> = Vec::with_capacity(repair_plan.actions.len());
    let mut prebuilt_outcomes: Vec<(RepairAction, RepairOutcome)> = Vec::new();
    for a in &repair_plan.actions {
        match (a.start_key.parse::<i64>(), a.end_key.parse::<i64>()) {
            (Ok(s), Ok(e)) => ranges.push((s, e)),
            _ => prebuilt_outcomes.push((
                a.clone(),
                RepairOutcome::Skipped {
                    reason: format!("unparseable chunk keys [{}..{}]", a.start_key, a.end_key),
                },
            )),
        }
    }

    let mut results: Vec<(RepairAction, RepairOutcome)> =
        Vec::with_capacity(repair_plan.actions.len());
    results.extend(prebuilt_outcomes);

    if !ranges.is_empty() {
        let mut src = source::create_source(&plan.source)?;
        let mut summary = RunSummary::new(plan);
        let before = summary.total_rows;
        let outcome = run_chunked_sequential(
            &mut *src,
            plan,
            &mut summary,
            Some(state),
            ChunkSource::Precomputed(ranges.clone()),
        );
        let delta = summary.total_rows - before;
        match outcome {
            Ok(()) => {
                // Sequential runs chunks in order; we do not track per-chunk row
                // counts separately here, so attribute the delta to the set.
                // If the set is a single chunk, the attribution is exact.
                let executed_actions: Vec<_> = repair_plan
                    .actions
                    .iter()
                    .filter(|a| {
                        a.start_key.parse::<i64>().is_ok() && a.end_key.parse::<i64>().is_ok()
                    })
                    .cloned()
                    .collect();
                if executed_actions.len() == 1 {
                    results.push((
                        executed_actions[0].clone(),
                        RepairOutcome::Executed {
                            rows_written: delta,
                        },
                    ));
                } else {
                    // Even split is a lie — mark each as executed, attribute total to the first.
                    let mut first = true;
                    for a in executed_actions {
                        let rows = if first { delta } else { 0 };
                        first = false;
                        results.push((a, RepairOutcome::Executed { rows_written: rows }));
                    }
                }
            }
            Err(e) => {
                let msg = format!("{:#}", e);
                for a in repair_plan.actions.iter().filter(|a| {
                    a.start_key.parse::<i64>().is_ok() && a.end_key.parse::<i64>().is_ok()
                }) {
                    results.push((a.clone(), RepairOutcome::Failed { error: msg.clone() }));
                }
            }
        }
    }

    Ok(RepairReport::new(
        repair_plan,
        format!("repair-{}", chrono::Utc::now().format("%Y%m%dT%H%M%S")),
        results,
    ))
}

fn emit_plan(plan: &RepairPlan, format: &RepairOutputFormat) -> Result<()> {
    match format {
        RepairOutputFormat::Pretty => print_plan_pretty(plan),
        RepairOutputFormat::Json(None) => println!("{}", plan.to_json_pretty()?),
        RepairOutputFormat::Json(Some(path)) => {
            std::fs::write(path, plan.to_json_pretty()?)
                .map_err(|e| anyhow::anyhow!("cannot write repair plan '{}': {}", path, e))?;
            println!("Repair plan written to: {}", path);
        }
    }
    Ok(())
}

fn emit_report(report: &RepairReport, format: &RepairOutputFormat) -> Result<()> {
    match format {
        RepairOutputFormat::Pretty => print_report_pretty(report),
        RepairOutputFormat::Json(None) => println!("{}", report.to_json_pretty()?),
        RepairOutputFormat::Json(Some(path)) => {
            std::fs::write(path, report.to_json_pretty()?)
                .map_err(|e| anyhow::anyhow!("cannot write repair report '{}': {}", path, e))?;
            println!("Repair report written to: {}", path);
        }
    }
    Ok(())
}

fn print_plan_pretty(plan: &RepairPlan) {
    println!();
    println!("  Export            : {}", plan.export_name);
    println!("  Reconcile run     : {}", plan.reconcile_run_id);
    println!("  Actions           : {}", plan.actions.len());
    for a in &plan.actions {
        println!(
            "    • chunk {} [{}..{}] — {}",
            a.chunk_index, a.start_key, a.end_key, a.reason
        );
    }
    if !plan.skipped.is_empty() {
        println!("  Skipped           :");
        for s in &plan.skipped {
            println!("    • {s}");
        }
    }
    if plan.is_empty() && plan.skipped.is_empty() {
        println!("  (nothing to repair)");
    }
    println!();
}

fn print_report_pretty(report: &RepairReport) {
    println!();
    println!("  Export       : {}", report.plan.export_name);
    println!("  Repair run   : {}", report.repair_run_id);
    println!(
        "  Summary      : planned {} · executed {} · skipped {} · failed {} · rows {}",
        report.summary.planned,
        report.summary.executed,
        report.summary.skipped,
        report.summary.failed,
        report.summary.rows_written,
    );
    for (a, out) in &report.results {
        let tag = match out {
            RepairOutcome::Executed { rows_written } => format!("executed ({rows_written} rows)"),
            RepairOutcome::Skipped { reason } => format!("skipped ({reason})"),
            RepairOutcome::Failed { error } => format!("failed ({error})"),
        };
        println!(
            "    • chunk {} [{}..{}] — {tag}",
            a.chunk_index, a.start_key, a.end_key
        );
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{PartitionKind, PartitionResult, ReconcileReport};

    #[test]
    fn plan_from_auto_would_derive_actions_from_reconcile() {
        // Smoke-test the public derivation path without hitting the DB.
        let partitions = vec![
            PartitionResult::classify(
                PartitionKind::Chunk,
                "chunk 0 [1..100]".into(),
                Some(100),
                Some(100),
            ),
            PartitionResult::classify(
                PartitionKind::Chunk,
                "chunk 1 [101..200]".into(),
                Some(100),
                Some(90),
            ),
        ];
        let r = ReconcileReport::new(
            "orders".into(),
            "rec-1".into(),
            "chunked".into(),
            partitions,
        );
        let plan = RepairPlan::from_reconcile(&r);
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(plan.actions[0].chunk_index, 1);
    }
}
