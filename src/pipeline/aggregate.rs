//! Aggregate summary for a `rivet run` invocation.
//!
//! A "run aggregate" is the rollup of every per-export `RunSummary` produced in
//! a single CLI invocation.  It answers "what did the last cron run do as a
//! whole?" without forcing the operator to scroll through 15 per-export blocks.
//!
//! The aggregate is persisted to `.rivet_state.db` (`run_aggregate` table) so
//! that downstream tooling can query past runs without re-parsing logs.
//! Optionally it is also written to a JSON file via `--summary-output`.
//!
//! Invariant: aggregation is purely observational.  It is built **after** every
//! per-export `record_metric` call and never on its own affects the exit code
//! or which exports run — failures still propagate through the existing
//! `Result` chain.

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::state::{RunAggregate, RunAggregateEntry, StateStore};

use super::summary::RunSummary;
use super::{format_bytes, strip_chunked_recovery_hint};

/// Convert a per-export summary into an aggregate row.
pub(super) fn entry_from_summary(s: &RunSummary) -> RunAggregateEntry {
    RunAggregateEntry {
        export_name: s.export_name.clone(),
        status: s.status.clone(),
        run_id: s.run_id.clone(),
        rows: s.total_rows,
        files: s.files_produced as i64,
        bytes: s.bytes_written,
        duration_ms: s.duration_ms,
        mode: s.mode.clone(),
        error_message: s.error_message.clone(),
    }
}

/// Build the aggregate from per-export entries plus run-level metadata.
///
/// `started_at` / `finished_at` are wall-clock timestamps captured by the
/// caller — duration is derived from them rather than from the sum of
/// per-export durations (parallel runs would otherwise overcount).
pub(super) fn build(
    entries: Vec<RunAggregateEntry>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    config_path: Option<&str>,
    parallel_mode: &str,
) -> RunAggregate {
    let total_exports = entries.len();
    let success_count = entries.iter().filter(|e| e.status == "success").count();
    let failed_count = entries.iter().filter(|e| e.status == "failed").count();
    let skipped_count = total_exports
        .saturating_sub(success_count)
        .saturating_sub(failed_count);
    let total_rows = entries.iter().map(|e| e.rows).sum();
    let total_files = entries.iter().map(|e| e.files).sum();
    let total_bytes = entries.iter().map(|e| e.bytes).sum();

    let id = format!("agg_{}", started_at.format("%Y%m%dT%H%M%S%3f"));

    RunAggregate {
        run_aggregate_id: id,
        started_at: started_at.to_rfc3339(),
        finished_at: finished_at.to_rfc3339(),
        duration_ms: (finished_at - started_at).num_milliseconds(),
        config_path: config_path.map(|s| s.to_string()),
        parallel_mode: parallel_mode.to_string(),
        total_exports,
        success_count,
        failed_count,
        skipped_count,
        total_rows,
        total_files,
        total_bytes,
        per_export: entries,
    }
}

/// Pretty-print the aggregate after all per-export blocks.
pub(super) fn print(agg: &RunAggregate) {
    eprintln!();
    eprintln!("════════════════════════════════════════════════════════");
    eprintln!("  Run summary ({} exports)", agg.total_exports);
    eprintln!("════════════════════════════════════════════════════════");
    eprintln!("  id:          {}", agg.run_aggregate_id);
    let mut status_line = format!(
        "{} success · {} failed",
        agg.success_count, agg.failed_count
    );
    if agg.skipped_count > 0 {
        status_line.push_str(&format!(" · {} skipped", agg.skipped_count));
    }
    eprintln!("  status:      {}", status_line);
    eprintln!("  rows:        {}", agg.total_rows);
    eprintln!("  files:       {}", agg.total_files);
    if agg.total_bytes > 0 {
        eprintln!("  bytes:       {}", format_bytes(agg.total_bytes));
    }
    eprintln!(
        "  duration:    {} (wall clock)",
        format_duration(agg.duration_ms)
    );
    if agg.duration_ms > 0 && agg.total_rows > 0 {
        let rps = agg.total_rows as f64 * 1000.0 / agg.duration_ms as f64;
        eprintln!("  throughput:  {} rows/s", format_rate(rps));
    }
    eprintln!("  mode:        {}", agg.parallel_mode);
    if let Some(cp) = &agg.config_path {
        eprintln!("  config:      {}", cp);
    }
    if agg.failed_count > 0 {
        eprintln!();
        eprintln!("  failed exports:");
        let mut chunked_recovery: Vec<&str> = Vec::new();
        for e in agg.per_export.iter().filter(|e| e.status == "failed") {
            let msg = e
                .error_message
                .as_deref()
                .unwrap_or("(no error message recorded)");
            let (cause, has_chunked_hint) = strip_chunked_recovery_hint(msg);
            if has_chunked_hint {
                chunked_recovery.push(e.export_name.as_str());
            }
            eprintln!("    - {}: {}", e.export_name, truncate(cause, 200));
        }
        if !chunked_recovery.is_empty() {
            print_chunked_recovery(&chunked_recovery, agg.config_path.as_deref());
        }
    }
}

/// Render one consolidated recovery block instead of repeating the same
/// `rivet run --resume` / `rivet state reset-chunks` commands per failed
/// export.  `config_path` is taken from the aggregate so the printed
/// commands are copy-paste runnable.
fn print_chunked_recovery(exports: &[&str], config_path: Option<&str>) {
    let cfg = match config_path {
        Some(p) if !p.is_empty() => format!("--config {}", p),
        _ => "--config <CONFIG>".to_string(),
    };
    eprintln!();
    eprintln!("  recovery ({} chunked export(s)):", exports.len());
    eprintln!("    resume in-progress checkpoint runs:");
    eprintln!("      rivet run {} --resume", cfg);
    eprintln!("    or reset chunk state and rerun a specific export:");
    eprintln!("      rivet state reset-chunks {} --export <NAME>", cfg);
    eprintln!("      NAME ∈ {{{}}}", exports.join(", "));
}

fn format_duration(ms: i64) -> String {
    if ms < 1000 {
        return format!("{}ms", ms);
    }
    let total_secs = ms / 1000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if h > 0 {
        format!("{}h {}m {}s", h, m, s)
    } else if m > 0 {
        format!("{}m {}s", m, s)
    } else {
        format!("{:.1}s", ms as f64 / 1000.0)
    }
}

fn format_rate(r: f64) -> String {
    if r >= 1_000_000.0 {
        format!("{:.1}M", r / 1_000_000.0)
    } else if r >= 1_000.0 {
        format!("{:.1}K", r / 1_000.0)
    } else {
        format!("{:.0}", r)
    }
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars).collect();
    out.push('…');
    out
}

/// Persist to state DB and optionally write JSON.  Failures are logged but
/// **never propagated** — aggregation is observational and must not turn a
/// successful run into a failed one.
pub(super) fn persist(state: &StateStore, agg: &RunAggregate, summary_output: Option<&Path>) {
    if let Err(e) = state.record_run_aggregate(agg) {
        log::warn!(
            "aggregate: failed to record run_aggregate (observational, ignored): {:#}",
            e
        );
    } else {
        log::info!(
            "aggregate: recorded {} ({} exports, {} success, {} failed)",
            agg.run_aggregate_id,
            agg.total_exports,
            agg.success_count,
            agg.failed_count,
        );
    }

    if let Some(path) = summary_output {
        match write_json(path, agg) {
            Ok(()) => eprintln!("  written:     {}", path.display()),
            Err(e) => log::warn!(
                "aggregate: failed to write summary JSON to {}: {:#}",
                path.display(),
                e
            ),
        }
    }
}

fn write_json(path: &Path, agg: &RunAggregate) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("create_dir_all({}): {:#}", parent.display(), e))?;
    }
    let json =
        serde_json::to_string_pretty(agg).map_err(|e| anyhow::anyhow!("serde_json: {:#}", e))?;
    std::fs::write(path, json)
        .map_err(|e| anyhow::anyhow!("write({}): {:#}", path.display(), e))?;
    Ok(())
}

/// Reconstruct per-export entries for `--parallel-export-processes`, where each
/// child wrote its own `record_metric` row and the parent had no in-memory
/// `RunSummary`.  Strategy:
///
/// - Look up the most recent `export_metrics` row for each export.
/// - Accept it only if its `run_at` is at-or-after the parent's `started_at`
///   (otherwise it is from a previous run).
/// - Otherwise synthesize a `failed` entry, preferring the child's exit-code
///   error message if the parent recorded one.
pub(super) fn collect_child_entries(
    state: &StateStore,
    exports: &[&crate::config::ExportConfig],
    started_at: DateTime<Utc>,
    child_failures: &HashMap<String, String>,
) -> Vec<RunAggregateEntry> {
    let mut out = Vec::with_capacity(exports.len());
    for export in exports {
        let mut entry: Option<RunAggregateEntry> = None;
        match state.get_metrics(Some(&export.name), 1) {
            Ok(rows) => {
                if let Some(m) = rows.into_iter().next()
                    && let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&m.run_at)
                    && parsed.with_timezone(&Utc) >= started_at
                {
                    entry = Some(RunAggregateEntry {
                        export_name: m.export_name,
                        status: m.status,
                        run_id: m.run_id.unwrap_or_default(),
                        rows: m.total_rows,
                        files: m.files_produced,
                        bytes: m.bytes_written.max(0) as u64,
                        duration_ms: m.duration_ms,
                        mode: m.mode.unwrap_or_default(),
                        error_message: m.error_message,
                    });
                }
            }
            Err(e) => {
                log::warn!(
                    "aggregate: metric query failed for '{}': {:#} (treating as failed)",
                    export.name,
                    e
                );
            }
        }

        out.push(entry.unwrap_or_else(|| {
            RunAggregateEntry {
                export_name: export.name.clone(),
                status: "failed".into(),
                run_id: String::new(),
                rows: 0,
                files: 0,
                bytes: 0,
                duration_ms: 0,
                mode: String::new(),
                error_message: Some(
                    child_failures
                        .get(export.name.as_str())
                        .cloned()
                        .unwrap_or_else(|| "no metric recorded for this run".into()),
                ),
            }
        }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn entry(name: &str, status: &str, rows: i64, files: i64, bytes: u64) -> RunAggregateEntry {
        RunAggregateEntry {
            export_name: name.into(),
            status: status.into(),
            run_id: format!("{name}_run"),
            rows,
            files,
            bytes,
            duration_ms: 1000,
            mode: "full".into(),
            error_message: if status == "failed" {
                Some("boom".into())
            } else {
                None
            },
        }
    }

    #[test]
    fn build_aggregates_counts_and_totals() {
        let started = Utc::now();
        let finished = started + Duration::seconds(120);
        let agg = build(
            vec![
                entry("a", "success", 100, 1, 1024),
                entry("b", "failed", 0, 0, 0),
                entry("c", "success", 50, 2, 2048),
            ],
            started,
            finished,
            Some("conf.yaml"),
            "sequential",
        );

        assert_eq!(agg.total_exports, 3);
        assert_eq!(agg.success_count, 2);
        assert_eq!(agg.failed_count, 1);
        assert_eq!(agg.skipped_count, 0);
        assert_eq!(agg.total_rows, 150);
        assert_eq!(agg.total_files, 3);
        assert_eq!(agg.total_bytes, 3072);
        assert_eq!(agg.duration_ms, 120_000);
        assert_eq!(agg.parallel_mode, "sequential");
        assert_eq!(agg.config_path.as_deref(), Some("conf.yaml"));
        assert!(
            agg.run_aggregate_id.starts_with("agg_"),
            "id should start with `agg_`, got {}",
            agg.run_aggregate_id
        );
    }

    #[test]
    fn build_handles_unknown_status_as_skipped() {
        let started = Utc::now();
        let finished = started + Duration::seconds(1);
        let agg = build(
            vec![
                entry("a", "success", 1, 0, 0),
                entry("b", "running", 0, 0, 0), // never reached terminal verdict
            ],
            started,
            finished,
            None,
            "sequential",
        );
        assert_eq!(agg.success_count, 1);
        assert_eq!(agg.failed_count, 0);
        assert_eq!(agg.skipped_count, 1);
    }

    #[test]
    fn build_with_zero_exports_is_well_formed() {
        let now = Utc::now();
        let agg = build(vec![], now, now, None, "sequential");
        assert_eq!(agg.total_exports, 0);
        assert_eq!(agg.total_rows, 0);
        assert_eq!(agg.success_count, 0);
        assert_eq!(agg.failed_count, 0);
        assert_eq!(agg.skipped_count, 0);
    }

    #[test]
    fn format_duration_picks_unit() {
        assert_eq!(format_duration(500), "500ms");
        assert_eq!(format_duration(1500), "1.5s");
        assert_eq!(format_duration(65_000), "1m 5s");
        assert_eq!(format_duration(3_725_000), "1h 2m 5s");
    }

    #[test]
    fn format_rate_scales() {
        assert_eq!(format_rate(42.0), "42");
        assert_eq!(format_rate(1500.0), "1.5K");
        assert_eq!(format_rate(2_500_000.0), "2.5M");
    }

    #[test]
    fn truncate_respects_char_boundary_with_unicode() {
        let s = "тест".repeat(100); // cyrillic, 400 chars
        let t = truncate(&s, 10);
        assert_eq!(t.chars().count(), 11); // 10 + ellipsis
    }

    #[test]
    fn persist_records_to_state_and_writes_file() {
        use crate::state::StateStore;
        let s = StateStore::open_in_memory().unwrap();
        let now = Utc::now();
        let agg = build(
            vec![entry("a", "success", 10, 1, 100)],
            now - Duration::seconds(5),
            now,
            Some("test.yaml"),
            "sequential",
        );

        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("nested").join("summary.json");

        persist(&s, &agg, Some(&out));

        // Recorded in DB.
        let rows = s.get_recent_run_aggregates(1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].run_aggregate_id, agg.run_aggregate_id);
        assert_eq!(rows[0].total_rows, 10);

        // Wrote JSON to nested path (parent created).
        let body = std::fs::read_to_string(&out).unwrap();
        let round: RunAggregate = serde_json::from_str(&body).unwrap();
        assert_eq!(round.run_aggregate_id, agg.run_aggregate_id);
        assert_eq!(round.per_export.len(), 1);
        assert_eq!(round.per_export[0].export_name, "a");
    }
}
