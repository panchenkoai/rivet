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
use serde::Serialize;

use crate::error::Result;
use crate::state::{ExportMetric, RunAggregate, RunAggregateEntry, StateStore};

use super::summary::RunSummary;
use super::{format_bytes, strip_chunked_recovery_hint};

/// Machine-readable view of one `export_metrics` row for `rivet metrics --json`.
///
/// `ExportMetric` (the internal `state` row) derives only `Debug`, so this DTO
/// is the stable on-the-wire contract — decoupled from the storage struct the
/// same way [`super::report::RunReport`] is decoupled from `RunSummary`, so the
/// JSON schema can evolve independently of column layout.  Field names mirror
/// the `export_metrics` columns; `Option` fields are emitted as JSON `null`
/// (kept, not skipped) so consumers see a fixed-shape object every row.
///
/// `dead_code`-allowed because the only production caller — the `rivet metrics
/// --json` flag dispatch — is added in a later wave (the CLI arg lives in
/// `cli.rs`/`args.rs`); this serialization layer lands first so wiring the flag
/// is a one-liner.  Exercised today by the module's unit tests.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
pub(super) struct MetricRowJson {
    pub export_name: String,
    pub run_id: Option<String>,
    pub run_at: String,
    pub duration_ms: i64,
    pub total_rows: i64,
    pub peak_rss_mb: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub tuning_profile: Option<String>,
    pub format: Option<String>,
    pub mode: Option<String>,
    pub files_produced: i64,
    pub bytes_written: i64,
    pub retries: i64,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
}

impl From<&ExportMetric> for MetricRowJson {
    fn from(m: &ExportMetric) -> Self {
        Self {
            export_name: m.export_name.clone(),
            run_id: m.run_id.clone(),
            run_at: m.run_at.clone(),
            duration_ms: m.duration_ms,
            total_rows: m.total_rows,
            peak_rss_mb: m.peak_rss_mb,
            status: m.status.clone(),
            error_message: m.error_message.clone(),
            tuning_profile: m.tuning_profile.clone(),
            format: m.format.clone(),
            mode: m.mode.clone(),
            files_produced: m.files_produced,
            bytes_written: m.bytes_written,
            retries: m.retries,
            validated: m.validated,
            schema_changed: m.schema_changed,
        }
    }
}

/// Render metric rows as a pretty-printed JSON array for `rivet metrics --json`.
///
/// An empty slice renders as `[]` (not an error, not the human-table's
/// "No metrics recorded yet." text) so machine consumers always parse a valid
/// JSON array.  The caller is responsible for the `--config` existence check
/// (finding #9) before reaching here — this function is pure formatting.
///
/// `dead_code`-allowed until the `rivet metrics --json` flag dispatch (a later
/// wave, in the off-limits `cli.rs`/`args.rs`) calls it.
#[allow(dead_code)]
pub(super) fn metrics_to_json(metrics: &[ExportMetric]) -> Result<String> {
    let rows: Vec<MetricRowJson> = metrics.iter().map(MetricRowJson::from).collect();
    serde_json::to_string_pretty(&rows).map_err(|e| anyhow::anyhow!("serde_json: {:#}", e))
}

/// Convert a per-export summary into an aggregate row.
pub(super) fn entry_from_summary(s: &RunSummary) -> RunAggregateEntry {
    RunAggregateEntry {
        export_name: s.export_name.clone(),
        status: s.status.clone(),
        run_id: s.run_id.clone(),
        rows: s.total_rows,
        files: s.files_produced as i64,
        bytes: s.bytes_written,
        bytes_read: s.bytes_read,
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
    let total_bytes_read = entries.iter().map(|e| e.bytes_read).sum();

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
        total_bytes_read,
        per_export: entries,
    }
}

/// One export's run-over-run throughput comparison input: current and
/// previous-success `(rows, duration_ms)`, plus the export MODE each side ran
/// in (`full` / `incremental` / …) — a mode switch changes what the work IS,
/// so the two rows are not two measurements of one thing.
pub(super) struct ThroughputPair {
    pub export_name: String,
    pub cur_rows: i64,
    pub cur_ms: i64,
    pub prev_rows: i64,
    pub prev_ms: i64,
    pub cur_mode: String,
    /// `None` when the baseline row predates the `mode` column — unknown mode
    /// is not evidence of a change, so it does not block the comparison.
    pub prev_mode: Option<String>,
}

/// Floor below which a run is too small/short for a throughput comparison to
/// mean anything (startup overhead dominates; noise reads as regression).
/// Applied to BOTH sides: the fixed per-run cost (connect, schema detect,
/// boundary probe, destination init, manifest/validate) is charged to the
/// CURRENT run too, so a short current run understates its own rows/s.
const REGRESSION_MIN_ROWS: i64 = 10_000;
const REGRESSION_MIN_MS: i64 = 5_000;
/// A run counts as regressed when its rows/s drop to ≤ 2/3 of the previous
/// success (≥1.5× slower per row).
const REGRESSION_RATIO: f64 = 1.5;
/// The two runs must have moved comparable amounts of data: past this factor
/// the fixed per-run cost is amortized over wildly different row counts and the
/// ratio measures SHAPE, not speed (a weekend backfill vs a daily delta).
const REGRESSION_MAX_SCALE: i64 = 2;

/// Why two runs of one export are NOT comparable on rows/s.
///
/// Made explicit (and named) because the skip condition is the load-bearing
/// half of this check: a false "5.0× slower — check governor sheds" on 154
/// healthy exports drowns the one real regression, the same
/// diagnostic-bypass harm a false UNSAFE does in preflight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Incomparable {
    /// Either side moved too few rows for rows/s to mean anything.
    TooFewRows,
    /// Either side ran too briefly — fixed per-run cost dominates.
    TooShort,
    /// The two runs moved very different amounts of data.
    ScaleMismatch,
    /// The export ran in a different mode (a `full` backfill vs an
    /// `incremental` delta reads as a regression while both are healthy).
    ModeChanged,
}

/// The comparability rule, pure and total: `None` means the pair may be
/// compared, `Some(reason)` names the refusal.
pub(super) fn incomparable(p: &ThroughputPair) -> Option<Incomparable> {
    if p.cur_rows < REGRESSION_MIN_ROWS || p.prev_rows < REGRESSION_MIN_ROWS {
        return Some(Incomparable::TooFewRows);
    }
    if p.cur_ms < REGRESSION_MIN_MS || p.prev_ms < REGRESSION_MIN_MS {
        return Some(Incomparable::TooShort);
    }
    let scale = REGRESSION_MAX_SCALE;
    if p.cur_rows.saturating_mul(scale) < p.prev_rows
        || p.prev_rows.saturating_mul(scale) < p.cur_rows
    {
        return Some(Incomparable::ScaleMismatch);
    }
    if let Some(prev_mode) = &p.prev_mode
        && !prev_mode.is_empty()
        && !p.cur_mode.is_empty()
        && prev_mode != &p.cur_mode
    {
        return Some(Incomparable::ModeChanged);
    }
    None
}

/// Does this run's concurrency mode make a per-export rows/s DROP expected?
///
/// Exports sharing one source/network/CPU legitimately each run slower while
/// the run's makespan improves — the very trade `--pool` is for. Unknown modes
/// are treated as concurrent: hedged text on a serial run is harmless, a
/// confident "check governor sheds" on a pool run is a false accusation.
fn mode_shares_the_source(run_mode: &str) -> bool {
    !matches!(run_mode, "sequential" | "wave-sequential" | "single")
}

/// Pure run-over-run self-check: compare each export's throughput (rows/s)
/// against its previous SUCCESS and name the material regressions.
///
/// This is the run proving itself: the 2026-08-13 field regression (a
/// governor reading its own exhaust ran every keyset export 2–2.7× slower,
/// +1h48m) was invisible in the moment — counts matched, statuses were green,
/// and only a hand-written SQL join over two state DBs surfaced it days
/// later. Rows/s (not wall time) so organic data growth does not read as a
/// slowdown; pairs of incomparable SHAPE are refused by [`incomparable`].
///
/// `run_mode` is the current run's concurrency mode (`sequential`, `pool`,
/// `parallel-threads`, …). It never suppresses the line — the field regression
/// happened on a concurrent run, so suppressing there would delete the signal
/// where it is needed most — but on a source-sharing mode the text says so
/// instead of blaming a shed the operator's own `--pool` explains. The
/// BASELINE run's mode is not recorded on `export_metrics`, so this scopes the
/// attribution, not the comparison.
pub(super) fn throughput_regressions(pairs: &[ThroughputPair], run_mode: &str) -> Vec<String> {
    let mut out = Vec::new();
    for p in pairs {
        if let Some(reason) = incomparable(p) {
            log::debug!(
                "throughput self-check: skipping '{}' — {:?}",
                p.export_name,
                reason
            );
            continue;
        }
        let cur_tp = p.cur_rows as f64 * 1000.0 / p.cur_ms as f64;
        let prev_tp = p.prev_rows as f64 * 1000.0 / p.prev_ms as f64;
        if prev_tp > 0.0 && prev_tp / cur_tp >= REGRESSION_RATIO {
            let tail = if mode_shares_the_source(run_mode) {
                format!(
                    "this run ran {run_mode}, where exports share the source and per-export \
                     rows/s falls BY DESIGN — compare the run's makespan before blaming a \
                     governor shed / adaptive batch shrink / source load"
                )
            } else {
                "check governor sheds / adaptive batch shrinks / source load".to_string()
            };
            out.push(format!(
                "export '{}': throughput {} → {} rows/s ({:.1}× slower than its last success) — \
                 {tail}",
                p.export_name,
                format_rate(prev_tp),
                format_rate(cur_tp),
                prev_tp / cur_tp,
            ));
        }
    }
    out
}

/// Read each successful export's previous success from the state DB and WARN
/// about material throughput regressions. Best-effort: a state read failing
/// never affects the run. Called beside [`print`] at every aggregate site so
/// EVERY run self-reports degradation at the default log level — the answer
/// to "prove the next run is not strangling itself" is that the run says so.
///
/// `run_mode` is the same string [`build`] records on the aggregate — passed
/// here so the warning can say which concurrency the run used (see
/// [`throughput_regressions`]); the single-export path has no aggregate and
/// passes `sequential` / `concurrent-siblings` for itself.
pub(super) fn warn_throughput_regressions(
    state: &StateStore,
    entries: &[RunAggregateEntry],
    run_mode: &str,
) {
    let mut pairs = Vec::new();
    for e in entries.iter().filter(|e| e.status == "success") {
        // Direct success query, excluding this run's own row — a fixed
        // recent-window scan went blind after ~9 consecutive failures,
        // exactly during the degraded period the baseline exists for
        // (bughunt 2026-08-13).
        let Ok(Some(prev)) = state.get_last_success_metric_excluding(&e.export_name, &e.run_id)
        else {
            continue;
        };
        pairs.push(ThroughputPair {
            export_name: e.export_name.clone(),
            cur_rows: e.rows,
            cur_ms: e.duration_ms,
            prev_rows: prev.total_rows,
            prev_ms: prev.duration_ms,
            cur_mode: e.mode.clone(),
            prev_mode: prev.mode.clone(),
        });
    }
    for line in throughput_regressions(&pairs, run_mode) {
        log::warn!("{line}");
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
    if agg.total_bytes_read > 0 {
        eprintln!("  bytes read:  {}", format_bytes(agg.total_bytes_read));
    }
    if agg.total_bytes > 0 {
        eprintln!("  bytes writ:  {}", format_bytes(agg.total_bytes));
    }
    eprintln!(
        "  duration:    {} (wall clock)",
        format_duration(agg.duration_ms)
    );
    // Transient destination retry ATTEMPTS the RetryLayer scheduled. Per-attempt
    // lines log at DEBUG after the first, so this aggregate is where the
    // "destination was flaky" signal lives — a nonzero count on a green run is
    // early warning of throttling/network degradation, not a data problem. The
    // wording is owned by `transient_retries_summary`, which is also what the
    // single-export summary card renders: the interceptor counts on the way INTO
    // a retry, so neither line may claim the retries recovered.
    if let Some(v) =
        crate::destination::transient_retries_summary(crate::destination::transient_retries_total())
    {
        eprintln!("  dest retries: {v}");
    }
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
    let names_spaced = exports.join(" ");
    eprintln!();
    eprintln!("  recovery ({} chunked export(s)):", exports.len());
    eprintln!("    resume in-progress checkpoint runs:");
    eprintln!("      rivet run {} --resume", cfg);
    eprintln!(
        "    or reset stuck checkpoints for every export in this config (chunk_run.status = in_progress), then resume:"
    );
    eprintln!(
        "      rivet state reset-chunks {} --stuck-checkpoints && rivet run {} --resume",
        cfg, cfg
    );
    eprintln!("    or reset only the exports listed above, then resume:");
    eprintln!(
        "      for e in {}; do rivet state reset-chunks {} --export \"$e\"; done && rivet run {} --resume",
        names_spaced, cfg, cfg
    );
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
    match s.char_indices().nth(max_chars) {
        None => s.to_owned(),
        Some((byte_pos, _)) => {
            let mut out = s[..byte_pos].to_owned();
            out.push('…');
            out
        }
    }
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
                // A `running` row is an IN-FLIGHT marker, not an outcome. Since
                // this branch began projecting a run's aggregate as each part
                // lands, a hard-crashed child (OOM-kill, panic — there is no
                // catch_unwind) leaves exactly such a row: partial `total_rows`,
                // status `running`, timestamped by its last durable part. Taking
                // it as the child's result reported those partial rows as the
                // export's outcome AND discarded the real cause, which the parent
                // holds in `child_failures` and which only the fallback below
                // surfaces. Before the projection existed a crashed child left no
                // row at all and fell through correctly.
                if let Some(m) = rows.into_iter().next()
                    && m.status != "running"
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
                        bytes_read: m.bytes_read.max(0) as u64,
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
                bytes_read: 0,
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

    /// A child that CRASHED must be reported as failed with its own cause — not
    /// as its half-finished in-flight aggregate.
    ///
    /// Since a run's aggregate is projected as each part lands, a hard-killed
    /// child leaves a `running` row carrying whatever it had committed. Reading
    /// the latest row without checking that it is TERMINAL turned that into the
    /// export's reported outcome: partial rows presented as the result, and the
    /// real failure — which the parent holds and only the fallback surfaces —
    /// thrown away.
    #[test]
    fn a_crashed_childs_in_flight_row_is_not_mistaken_for_its_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_at_path(&dir.path().join("state.db")).unwrap();

        // Exactly what a killed child leaves behind: parts on record, so the
        // projection wrote a `running` aggregate with PARTIAL rows.
        for i in 0..3 {
            state
                .record_file(crate::state::FilePart {
                    run_id: "r_crashed",
                    export_name: "orders",
                    file_name: &format!("part{i}.parquet"),
                    rows: 100,
                    bytes: 1000,
                    format: "parquet",
                    compression: None,
                    cursor_high: None,
                })
                .unwrap();
        }
        state
            .record_durable_part(crate::state::DurablePart {
                run_id: "r_crashed",
                export_name: "orders",
                file_name: "part0.parquet",
                rows: 100,
                bytes: 1000,
                format: "parquet",
                compression: None,
                mode: "chunked",
                cursor_high: None,
            })
            .unwrap();

        // Not inert: the row really is there, really is `running`, and really
        // carries rows — otherwise there is nothing to mistake for an outcome.
        let latest = state.get_metrics(Some("orders"), 1).unwrap();
        assert_eq!(latest.len(), 1, "the projection must have written a row");
        assert_eq!(latest[0].status, "running");
        assert!(latest[0].total_rows > 0, "partial rows are the trap");

        let export: crate::config::ExportConfig = serde_yaml_ng::from_str(
            "name: orders\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n",
        )
        .expect("parse test ExportConfig");
        let mut failures = std::collections::HashMap::new();
        failures.insert("orders".to_string(), "child killed by signal 9".to_string());
        let entries = collect_child_entries(
            &state,
            &[&export],
            chrono::Utc::now() - chrono::Duration::hours(1),
            &failures,
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].status, "failed",
            "a child that never finished must not be reported by its in-flight row"
        );
        assert_eq!(
            entries[0].rows, 0,
            "the rows it had written when it died are not the rows it delivered"
        );
        assert_eq!(
            entries[0].error_message.as_deref(),
            Some("child killed by signal 9"),
            "the parent's own cause must survive — it is the only place it exists"
        );
    }
    use super::*;
    use chrono::Duration;

    /// A comparable pair by default — same export mode both sides, so each
    /// test names only the dimension it is about.
    fn pair(
        name: &str,
        cur_rows: i64,
        cur_ms: i64,
        prev_rows: i64,
        prev_ms: i64,
    ) -> ThroughputPair {
        ThroughputPair {
            export_name: name.into(),
            cur_rows,
            cur_ms,
            prev_rows,
            prev_ms,
            cur_mode: "full".into(),
            prev_mode: Some("full".into()),
        }
    }

    fn entry(name: &str, status: &str, rows: i64, files: i64, bytes: u64) -> RunAggregateEntry {
        RunAggregateEntry {
            bytes_read: 0,
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

    /// The run-over-run self-check that answers "prove the next run is not
    /// strangling itself": a material rows/s drop vs the last success must be
    /// named, organic growth and noise must not. RED-proven against a mutant
    /// inverting the ratio comparison.
    #[test]
    fn throughput_regressions_flag_real_slowdowns_only() {
        // 2.4× slower per row (the field regression's shape) → flagged.
        let out = throughput_regressions(
            &[pair("big", 1_000_000, 24_000, 1_000_000, 10_000)],
            "sequential",
        );
        assert_eq!(out.len(), 1, "a 2.4× slowdown must be flagged: {out:?}");
        assert!(
            out[0].contains("big") && out[0].contains("slower"),
            "{out:?}"
        );
        // 1.2× — within noise/growth → silent.
        assert!(
            throughput_regressions(
                &[pair("ok", 1_000_000, 12_000, 1_000_000, 10_000)],
                "sequential"
            )
            .is_empty()
        );
        // Faster run → silent.
        assert!(
            throughput_regressions(
                &[pair("fast", 1_000_000, 8_000, 1_000_000, 10_000)],
                "sequential"
            )
            .is_empty()
        );
        // Rows GREW 1.8× while wall grew 1.8× — throughput flat → silent
        // (wall-time comparison would have false-flagged this; rows/s is the
        // honest unit). Kept inside the comparability band on purpose, so it
        // still exercises the RATIO rather than being short-circuited by the
        // scale rule below.
        assert!(
            throughput_regressions(
                &[pair("grew", 1_800_000, 18_000, 1_000_000, 10_000)],
                "sequential"
            )
            .is_empty()
        );
        // Tiny/short runs are noise → silent even at 10× slower.
        assert!(
            throughput_regressions(&[pair("tiny", 500, 5_000, 500, 500)], "sequential").is_empty()
        );
        assert!(
            throughput_regressions(&[pair("short", 20_000, 4_000, 20_000, 400)], "sequential")
                .is_empty(),
            "prev under the min-duration floor must not baseline"
        );
    }

    /// The comparability rule is the load-bearing half of the self-check: a
    /// confident "5.0× slower — check governor sheds" on a healthy run is the
    /// diagnostic-bypass harm (154 false alarms drown the one real one). Each
    /// case below is a pair the check must REFUSE to compare, and each was a
    /// false WARN before this rule existed.
    ///
    /// RED-proven, one mutant per rule, each reverting to the pre-fix code
    /// (`left` is the mutant's verdict, `right` the required one):
    ///
    ///  * `p.cur_ms < REGRESSION_MIN_MS` → `p.cur_ms <= 0` — the floor applied
    ///    to the PREVIOUS side only: `left: None, right: Some(TooShort)`.
    ///  * drop the `ScaleMismatch` arm: `left: None, right: Some(ScaleMismatch)`.
    ///  * drop the `ModeChanged` arm: `left: None, right: Some(ModeChanged)`.
    ///
    /// The fixture carries FOUR pairs, one of them a genuine regression, so a
    /// mutant that refuses everything (`incomparable` → always `Some`) is also
    /// RED — the filter must SELECT, not merely return empty.
    #[test]
    fn throughput_regressions_refuse_pairs_of_incomparable_shape() {
        // (a) The weekend backfill baselining a daily delta: both sides clear
        // every noise floor, the ratio is 5.0×, and nothing is wrong.
        let backfill = pair("orders", 40_000, 6_000, 2_000_000, 60_000);
        assert_eq!(incomparable(&backfill), Some(Incomparable::ScaleMismatch));
        // (b) A 4 s current run: fixed per-run cost (connect, schema detect,
        // boundary probe, manifest/validate) is most of its wall, so its
        // rows/s understates itself — the same reason `prev_ms` has a floor.
        let short_cur = pair("short-cur", 40_000, 4_000, 80_000, 5_000);
        assert_eq!(incomparable(&short_cur), Some(Incomparable::TooShort));
        // (c) A mode switch changes what the work IS.
        let mut mode_flip = pair("switched", 100_000, 40_000, 100_000, 10_000);
        mode_flip.cur_mode = "incremental".into();
        mode_flip.prev_mode = Some("full".into());
        assert_eq!(incomparable(&mode_flip), Some(Incomparable::ModeChanged));
        // (d) …and one genuine regression, same shape both runs.
        let real = pair("real", 1_000_000, 24_000, 1_000_000, 10_000);
        assert_eq!(incomparable(&real), None);

        let out = throughput_regressions(&[backfill, short_cur, mode_flip, real], "sequential");
        assert_eq!(
            out.len(),
            1,
            "only the comparable pair may be reported: {out:?}"
        );
        assert!(out[0].contains("real"), "{out:?}");
    }

    /// A concurrency mode is not a regression: under `--pool` every export
    /// shares the source, so per-export rows/s falls while the makespan
    /// improves — the run must not blame the governor for the operator's own
    /// `--pool`. The line still PRINTS (the field regression happened on a
    /// concurrent run; suppressing there deletes the signal where it is needed
    /// most) but says which mode it ran.
    ///
    /// RED-proven against `mode_shares_the_source` → `false` for every mode
    /// (the pre-fix behaviour: one text for all modes): the `contains("pool")`
    /// assert fails.
    #[test]
    fn throughput_regression_text_names_a_source_sharing_mode() {
        let p = || pair("orders", 1_000_000, 24_000, 1_000_000, 10_000);
        let pooled = throughput_regressions(&[p()], "pool");
        assert_eq!(pooled.len(), 1, "the line must still print under a pool");
        assert!(
            pooled[0].contains("pool") && pooled[0].contains("makespan"),
            "a source-sharing run must name its mode instead of blaming a shed: {pooled:?}"
        );
        let serial = throughput_regressions(&[p()], "sequential");
        assert!(
            serial[0].contains("check governor sheds"),
            "a serial run keeps the direct attribution: {serial:?}"
        );
        // Every mode the aggregate records, classified.
        for m in [
            "parallel-threads",
            "parallel-processes",
            "wave-parallel-processes",
            "pool",
            "concurrent-siblings",
        ] {
            assert!(mode_shares_the_source(m), "{m} shares the source");
        }
        for m in ["sequential", "wave-sequential", "single"] {
            assert!(!mode_shares_the_source(m), "{m} is serial");
        }
    }

    #[test]
    fn format_rate_scales() {
        assert_eq!(format_rate(42.0), "42");
        assert_eq!(format_rate(1500.0), "1.5K");
        assert_eq!(format_rate(2_500_000.0), "2.5M");
    }

    #[test]
    fn truncate_respects_char_boundary_with_unicode() {
        let s = "αβγδ".repeat(100); // multibyte unicode, 400 chars
        let t = truncate(&s, 10);
        assert_eq!(t.chars().count(), 11); // 10 + ellipsis
    }

    fn metric(name: &str, status: &str) -> ExportMetric {
        ExportMetric {
            bytes_read: 0,
            export_name: name.into(),
            run_id: Some(format!("{name}_run")),
            run_at: "2026-06-09T12:00:00+00:00".into(),
            duration_ms: 1500,
            total_rows: 42,
            peak_rss_mb: Some(64),
            status: status.into(),
            error_message: if status == "failed" {
                Some("boom".into())
            } else {
                None
            },
            tuning_profile: Some("balanced".into()),
            format: Some("parquet".into()),
            mode: Some("full".into()),
            files_produced: 2,
            bytes_written: 4096,
            retries: 1,
            validated: Some(true),
            schema_changed: Some(false),
        }
    }

    #[test]
    fn metrics_to_json_empty_is_valid_array() {
        let json = metrics_to_json(&[]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array(), "empty metrics must serialize as []");
        assert_eq!(parsed.as_array().unwrap().len(), 0);
        // Must NOT leak the human-table sentinel into the machine contract.
        assert!(!json.contains("No metrics recorded yet"));
    }

    #[test]
    fn metrics_to_json_carries_all_fields() {
        let json =
            metrics_to_json(&[metric("orders", "success"), metric("users", "failed")]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let rows = parsed.as_array().unwrap();
        assert_eq!(rows.len(), 2);

        let first = &rows[0];
        // Every column of `export_metrics` is present under its column name.
        for key in [
            "export_name",
            "run_id",
            "run_at",
            "duration_ms",
            "total_rows",
            "peak_rss_mb",
            "status",
            "error_message",
            "tuning_profile",
            "format",
            "mode",
            "files_produced",
            "bytes_written",
            "retries",
            "validated",
            "schema_changed",
        ] {
            assert!(
                first.get(key).is_some(),
                "metrics JSON row must carry `{key}`; got {first}"
            );
        }
        assert_eq!(first["export_name"], "orders");
        assert_eq!(first["status"], "success");
        assert_eq!(first["total_rows"], 42);
        assert_eq!(first["files_produced"], 2);
        assert_eq!(first["validated"], true);
        // `None` fields are emitted as JSON null (fixed shape, not skipped).
        assert!(first["error_message"].is_null());
        // The failed row carries its error message.
        assert_eq!(rows[1]["status"], "failed");
        assert_eq!(rows[1]["error_message"], "boom");
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
