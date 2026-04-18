//! `rivet reconcile` — partition/window reconciliation (Epic F).
//!
//! Re-runs per-partition `COUNT(*)` against the source for a chunked export and
//! compares the result with the stored per-chunk row counts. Produces a
//! [`ReconcileReport`] — matches, mismatches, and repair candidates.
//!
//! v1 supports **chunked exports with `chunk_checkpoint: true`** (so per-chunk
//! row counts and ranges are persisted). Other modes return a clear
//! "not supported in v1" error — reconcile semantics for snapshot / incremental
//! / time-window differ (see roadmap: Epic F & Epic G).

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::plan::{
    ExtractionStrategy, PartitionKind, PartitionResult, ReconcileReport, ResolvedRunPlan,
    build_plan,
};
use crate::source;
use crate::state::{ChunkTaskInfo, StateStore};

use super::chunked::build_chunk_query_sql;

/// Output format for the reconcile report.
pub enum ReconcileOutputFormat {
    /// Human-readable summary printed to stdout.
    Pretty,
    /// Pretty-printed JSON written to the given path (or stdout if `None`).
    Json(Option<String>),
}

pub fn run_reconcile_command(
    config_path: &str,
    export_name: &str,
    params: Option<&HashMap<String, String>>,
    format: ReconcileOutputFormat,
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

    let plan = build_plan(&config, export, config_dir, false, false, false, params)?;

    let state_path = config_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    let report = match &plan.strategy {
        ExtractionStrategy::Chunked(_) => reconcile_chunked(&plan, &state, export)?,
        ExtractionStrategy::TimeWindow { .. } => {
            anyhow::bail!(
                "reconcile: time-window mode is not supported in v1 (Epic F). \
                 Convert to chunked mode with `chunk_by_days` for partition-level reconcile."
            );
        }
        ExtractionStrategy::Snapshot | ExtractionStrategy::Incremental(_) => {
            anyhow::bail!(
                "reconcile: '{}' mode has no natural partitions — use `rivet run --reconcile` for a whole-export count check",
                plan.strategy.mode_label()
            );
        }
    };

    emit_report(&report, &format)?;
    Ok(())
}

/// Run a reconcile pass against the latest chunk run and return the report.
/// Exposed so `rivet repair --auto` can build a repair plan from a fresh reconcile
/// without duplicating the logic.
pub(crate) fn reconcile_chunked_fresh(
    plan: &ResolvedRunPlan,
    state: &StateStore,
) -> Result<ReconcileReport> {
    reconcile_chunked_inner(plan, state)
}

fn reconcile_chunked(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    _export: &ExportConfig,
) -> Result<ReconcileReport> {
    reconcile_chunked_inner(plan, state)
}

fn reconcile_chunked_inner(plan: &ResolvedRunPlan, state: &StateStore) -> Result<ReconcileReport> {
    let (run_id, _plan_hash, _status, _updated) = state
        .get_latest_chunk_run(&plan.export_name)?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "reconcile: no chunk run recorded for export '{}'. \
                 Enable `chunk_checkpoint: true` and run the export first.",
                plan.export_name
            )
        })?;

    let tasks = state.list_chunk_tasks_for_run(&run_id)?;
    if tasks.is_empty() {
        anyhow::bail!(
            "reconcile: chunk run '{}' for export '{}' has no tasks",
            run_id,
            plan.export_name
        );
    }

    let mut src = source::create_source(&plan.source)?;
    let partitions = reconcile_chunked_tasks(plan, &tasks, |chunk_query| {
        let count_sql = format!("SELECT COUNT(*) FROM ({chunk_query}) AS _rc");
        let raw = src.query_scalar(&count_sql)?;
        Ok(raw.and_then(|s| s.trim().parse::<i64>().ok()))
    })?;

    let report = ReconcileReport::new(
        plan.export_name.clone(),
        run_id.clone(),
        plan.strategy.mode_label().to_string(),
        partitions,
    );

    // Epic G: a reconcile with no mismatches and no unknowns is a fresh verified boundary.
    if report.summary.mismatches == 0 && report.summary.unknown == 0 {
        let highest = tasks
            .iter()
            .filter(|t| t.status == "completed")
            .map(|t| t.chunk_index)
            .max();
        if let Some(idx) = highest
            && let Err(e) = state.record_verified_chunked(&plan.export_name, idx, &run_id)
        {
            log::warn!(
                "export '{}': verified boundary update failed: {:#}",
                plan.export_name,
                e
            );
        }
    }

    Ok(report)
}

/// Pure classification of `ChunkTaskInfo`s into `PartitionResult`s.
///
/// Abstracts the source round-trip via `count_source` so the logic can be
/// exercised in unit tests without a live database.
pub(crate) fn reconcile_chunked_tasks<F>(
    plan: &ResolvedRunPlan,
    tasks: &[ChunkTaskInfo],
    mut count_source: F,
) -> Result<Vec<PartitionResult>>
where
    F: FnMut(&str) -> Result<Option<i64>>,
{
    let cp = match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => cp,
        _ => anyhow::bail!("reconcile_chunked_tasks requires Chunked strategy"),
    };

    let mut out: Vec<PartitionResult> = Vec::with_capacity(tasks.len());
    for t in tasks {
        let exported = if t.status == "completed" {
            t.rows_written
        } else {
            None
        };

        let (start, end) = match (t.start_key.parse::<i64>(), t.end_key.parse::<i64>()) {
            (Ok(s), Ok(e)) => (s, e),
            _ => {
                // Unparseable chunk keys: keep exported count but cannot re-count source.
                out.push(PartitionResult::classify(
                    PartitionKind::Chunk,
                    format!("chunk {} [{}..{}]", t.chunk_index, t.start_key, t.end_key),
                    None,
                    exported,
                ));
                continue;
            }
        };

        let chunk_query = build_chunk_query_sql(
            &plan.base_query,
            &cp.column,
            start,
            end,
            cp.dense,
            cp.by_days.is_some(),
            plan.source.source_type,
        );
        let source_count = count_source(&chunk_query)?;

        out.push(PartitionResult::classify(
            PartitionKind::Chunk,
            format!("chunk {} [{}..{}]", t.chunk_index, start, end),
            source_count,
            exported,
        ));
    }
    Ok(out)
}

fn emit_report(report: &ReconcileReport, format: &ReconcileOutputFormat) -> Result<()> {
    match format {
        ReconcileOutputFormat::Pretty => {
            print_report_pretty(report);
        }
        ReconcileOutputFormat::Json(None) => {
            println!("{}", report.to_json_pretty()?);
        }
        ReconcileOutputFormat::Json(Some(path)) => {
            let json = report.to_json_pretty()?;
            std::fs::write(path, &json)
                .map_err(|e| anyhow::anyhow!("cannot write reconcile report '{}': {}", path, e))?;
            println!("Reconcile report written to: {}", path);
        }
    }
    Ok(())
}

fn print_report_pretty(report: &ReconcileReport) {
    println!();
    println!("  Export    : {}", report.export_name);
    println!("  Run       : {}", report.run_id);
    println!("  Strategy  : {}", report.strategy);
    println!(
        "  Partitions: {} ({} match, {} mismatch, {} unknown)",
        report.summary.total_partitions,
        report.summary.matches,
        report.summary.mismatches,
        report.summary.unknown,
    );
    println!(
        "  Rows      : source {} / exported {}",
        report.summary.total_source_rows, report.summary.total_exported_rows,
    );

    let repair = report.repair_candidates();
    if repair.is_empty() {
        println!("  Status    : all partitions match");
    } else {
        println!("  Repair candidates:");
        for p in repair {
            println!("    • {} — {}", p.identifier, format_status_note(p));
        }
    }
    println!();
}

fn format_status_note(p: &PartitionResult) -> String {
    let s = match (p.source_count, p.exported_count) {
        (Some(s), Some(e)) => format!("source={s}, exported={e}"),
        (Some(s), None) => format!("source={s}, exported=n/a"),
        (None, Some(e)) => format!("source=n/a, exported={e}"),
        (None, None) => "no counts".to_string(),
    };
    if p.note.is_empty() {
        s
    } else {
        format!("{s} ({})", p.note)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy};
    use crate::state::ChunkTaskInfo;
    use crate::tuning::SourceTuning;

    fn chunked_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "orders".into(),
            base_query: "SELECT * FROM orders".into(),
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                parallel: 1,
                dense: false,
                by_days: None,
                checkpoint: true,
                max_attempts: 3,
            }),
            format: FormatType::Parquet,
            compression: CompressionType::Zstd,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("./out".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced (default)".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://localhost/test".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                tuning: None,
                tls: None,
            },
        }
    }

    fn task(idx: i64, start: &str, end: &str, status: &str, rows: Option<i64>) -> ChunkTaskInfo {
        ChunkTaskInfo {
            chunk_index: idx,
            start_key: start.into(),
            end_key: end.into(),
            status: status.into(),
            attempts: 1,
            last_error: None,
            rows_written: rows,
            file_name: None,
        }
    }

    #[test]
    fn matches_and_mismatches_are_classified() {
        let plan = chunked_plan();
        let tasks = vec![
            task(0, "1", "100", "completed", Some(42)),
            task(1, "101", "200", "completed", Some(30)),
        ];
        // Stub source: chunk 0 matches, chunk 1 undercounts on export side.
        let mut n = 0;
        let parts = reconcile_chunked_tasks(&plan, &tasks, |_q| {
            n += 1;
            Ok(Some(if n == 1 { 42 } else { 33 }))
        })
        .unwrap();

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].status, crate::plan::PartitionStatus::Match);
        assert_eq!(parts[1].status, crate::plan::PartitionStatus::Mismatch);
        assert_eq!(parts[1].source_count, Some(33));
        assert_eq!(parts[1].exported_count, Some(30));
    }

    #[test]
    fn unfinished_task_is_unknown_and_does_not_hide_source_count() {
        let plan = chunked_plan();
        let tasks = vec![task(0, "1", "100", "failed", None)];
        let parts = reconcile_chunked_tasks(&plan, &tasks, |_q| Ok(Some(42))).unwrap();
        assert_eq!(parts[0].status, crate::plan::PartitionStatus::Unknown);
        assert_eq!(parts[0].source_count, Some(42));
        assert_eq!(parts[0].exported_count, None);
    }

    #[test]
    fn unparseable_chunk_keys_are_unknown_without_source_lookup() {
        let plan = chunked_plan();
        let tasks = vec![task(0, "alpha", "omega", "completed", Some(5))];
        let mut called = false;
        let parts = reconcile_chunked_tasks(&plan, &tasks, |_q| {
            called = true;
            Ok(Some(99))
        })
        .unwrap();
        assert!(
            !called,
            "reconcile must skip source count for unparseable chunk keys"
        );
        assert_eq!(parts[0].status, crate::plan::PartitionStatus::Unknown);
        assert_eq!(parts[0].exported_count, Some(5));
    }

    #[test]
    fn chunk_query_passes_through_chunked_math() {
        let plan = chunked_plan();
        let tasks = vec![task(0, "10", "20", "completed", Some(5))];
        let mut captured = String::new();
        reconcile_chunked_tasks(&plan, &tasks, |q| {
            captured = q.to_string();
            Ok(Some(5))
        })
        .unwrap();
        // Must reuse the same WHERE predicate used during extraction (ADR-0001 shape).
        assert!(captured.contains("BETWEEN 10 AND 20"), "got: {captured}");
        assert!(
            captured.contains("\"id\""),
            "identifier must be quoted: {captured}"
        );
    }
}
