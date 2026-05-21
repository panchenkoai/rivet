//! **Layer: Coordinator** (planning → execution → persistence/observability)
//!
//! `pipeline/mod.rs` is the only module allowed to bridge all three layers.
//! It reads a resolved plan (planning), dispatches to execution modules, then
//! records metrics and sends notifications (persistence/observability).
//!
//! See `docs/adr/0003-layer-classification.md` for the full module taxonomy.

mod aggregate;
mod apply_cmd;
pub(crate) mod chunked;
mod cli;
pub(crate) mod ipc;
mod job;
mod parallel_children;
pub(crate) mod parent_ui;
mod plan_cmd;
pub(crate) mod progress;
mod reconcile_cmd;
mod repair_cmd;
pub(crate) mod report;
mod retry;
mod single;
mod sink;
mod summary;
mod validate;

pub use apply_cmd::run_apply_command;
#[allow(unused_imports)]
pub use chunked::generate_chunks;
pub use cli::{
    reset_chunk_checkpoint, reset_chunk_checkpoints_stuck, reset_state, show_chunk_checkpoint,
    show_files, show_journal, show_metrics, show_progression, show_state,
};
pub(crate) use job::run_export_job_with_chunk_source;
pub use plan_cmd::{PlanOutputFormat, run_plan_command};
pub use reconcile_cmd::{ReconcileOutputFormat, run_reconcile_command};
pub use repair_cmd::{RepairOutputFormat, RepairReportSource, run_repair_command};
#[allow(unused_imports)]
pub use retry::{RetryClass, classify_error};
// build_time_window_query moved to crate::plan; re-exported here for integration tests.
#[allow(unused_imports)]
pub use crate::plan::build_time_window_query;
#[allow(unused_imports)]
pub use validate::validate_output;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use retry::is_transient;

use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::state::StateStore;

pub use summary::RunSummary;

/// Per-run configuration flags passed from the CLI to the pipeline.
///
/// Replaces the previous pattern of threading 4+ positional `bool` arguments
/// through `run`, `run_export_job`, and child-process invocations.  Named fields
/// prevent silent argument transposition (e.g., `validate` and `reconcile`
/// swapped).
#[derive(Debug, Clone, Copy)]
pub struct RunOptions<'a> {
    pub validate: bool,
    pub reconcile: bool,
    pub resume: bool,
    pub params: Option<&'a std::collections::HashMap<String, String>>,
}

use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

/// True when the current process is running more than one export in this
/// `rivet run` invocation (sequential or `--parallel-exports`).  Per-export
/// renderers (`RunSummary::print`, `ChunkProgress`) read this to switch to
/// the compact one-line format and to suppress the indicatif chunk bar
/// respectively, so 15 exports take 15 lines instead of 100+ and threads
/// don't stack progress bars on top of each other.
///
/// Children of `--parallel-export-processes` always have `exports.len() == 1`
/// in their own process so this flag stays `false` for them; the parent
/// renders cards itself via `parent_ui`.
pub(crate) static MULTI_EXPORT_MODE: AtomicBool = AtomicBool::new(false);

/// True only when multiple exports run **concurrently** in the current
/// process (i.e. `--parallel-exports`, threads).  Used to suppress
/// per-export `indicatif` chunk progress bars whose terminal writes
/// otherwise interleave across threads and corrupt each other.
pub(crate) static MULTI_EXPORT_CONCURRENT: AtomicBool = AtomicBool::new(false);

pub(crate) fn multi_export_mode() -> bool {
    MULTI_EXPORT_MODE.load(AtomicOrdering::Relaxed)
}

#[allow(dead_code)] // kept for future renderers; flag is still set in pipeline::run.
pub(crate) fn multi_export_concurrent() -> bool {
    MULTI_EXPORT_CONCURRENT.load(AtomicOrdering::Relaxed)
}

pub(crate) fn format_bytes(b: u64) -> String {
    if b >= 1_073_741_824 {
        format!("{:.1} GB", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1} MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
    }
}

/// Strip the trailing recovery-hint portion of a chunked-pipeline error
/// message produced by `pipeline::chunked`.  Returns the cause prefix and
/// whether a chunked-checkpoint hint was detected.
///
/// Hints emitted by `pipeline::chunked` always follow the pattern
/// `<cause>; <connector> \`rivet …\` …`, so we cut at the first `; ` whose
/// remainder contains a backtick-quoted `rivet` invocation.
///
/// Used by both the per-export card renderer (`parent_ui`) and the run
/// aggregator (`aggregate`) so the long inline command doesn't wrap, distort
/// the in-place card layout, and doesn't repeat the consolidated recovery
/// block printed by the aggregator.
pub(crate) fn strip_chunked_recovery_hint(msg: &str) -> (&str, bool) {
    let mut pos = 0;
    while let Some(off) = msg[pos..].find("; ") {
        let abs = pos + off;
        let tail = &msg[abs + 2..];
        if tail.contains("`rivet ") {
            return (&msg[..abs], true);
        }
        pos = abs + 2;
    }
    (msg, false)
}

/// Truncate `s` to at most `max_chars` Unicode characters, appending `…`
/// when truncated.  Returns `s` unchanged if already short enough.  Used by
/// the in-place card renderer to keep every line within the chosen
/// terminal width — line wrapping breaks the cursor-up redraw math and
/// causes cards to drift down the screen.
pub(crate) fn clamp_line(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}

fn print_json_summary(agg: &crate::state::RunAggregate) {
    match serde_json::to_string_pretty(agg) {
        Ok(json) => println!("{json}"),
        Err(e) => eprintln!(
            "rivet: error: failed to serialize run summary as JSON: {:#}",
            e
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    config_path: &str,
    export_name: Option<&str>,
    validate: bool,
    reconcile: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
    parallel_exports_cli: bool,
    parallel_export_processes_cli: bool,
    summary_output: Option<&Path>,
    json_output: bool,
) -> Result<()> {
    let config = Config::load_with_params(config_path, params)?;

    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let exports: Vec<&ExportConfig> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    let opts = RunOptions {
        validate,
        reconcile,
        resume,
        params,
    };

    let run_parallel_processes = (parallel_export_processes_cli
        || config.parallel_export_processes)
        && export_name.is_none()
        && exports.len() > 1;

    let started_at = chrono::Utc::now();

    if run_parallel_processes {
        // Run schema migrations once in the parent BEFORE forking children.
        // Otherwise N children race for the exclusive write lock on a
        // brand-new `.rivet_state.db` and `busy_timeout` is not enough to
        // serialise them — most fail with `migration v1 failed: database is
        // locked`.  After this open succeeds the schema is at the latest
        // version and children's `StateStore::open` calls become idempotent
        // (the `MIGRATIONS` loop is a no-op when `ver <= current`).
        if let Err(e) = StateStore::open(config_path) {
            return Err(anyhow::anyhow!(
                "state: failed to initialize state DB before spawning children: {:#}",
                e
            ));
        }

        let (result, child_failures, stderr_dump) =
            parallel_children::run_exports_as_child_processes(
                config_path,
                &exports,
                validate,
                reconcile,
                resume,
                params,
            );
        let finished_at = chrono::Utc::now();
        // Best-effort aggregate: open the state DB read-only-ish and reconstruct
        // entries from the per-child `record_metric` rows.  Failure to open the
        // DB here only suppresses the aggregate, not the run itself.
        match StateStore::open(config_path) {
            Ok(state) => {
                let entries =
                    aggregate::collect_child_entries(&state, &exports, started_at, &child_failures);
                let agg = aggregate::build(
                    entries,
                    started_at,
                    finished_at,
                    Some(config_path),
                    "parallel-processes",
                );
                aggregate::print(&agg);
                aggregate::persist(&state, &agg, summary_output);
                if json_output {
                    print_json_summary(&agg);
                }
            }
            Err(e) => log::warn!(
                "aggregate: cannot open state DB to record run aggregate: {:#}",
                e
            ),
        }
        // Captured child stderr is printed AFTER the aggregate so the run
        // summary stays immediately under the card stack — verbose log
        // output sits below for triage when needed.
        if !stderr_dump.is_empty() {
            use std::io::Write;
            let mut h = std::io::stderr().lock();
            let _ = h.write_all(stderr_dump.as_bytes());
            let _ = h.flush();
        }
        return result;
    }

    let run_parallel = (parallel_exports_cli || config.parallel_exports)
        && export_name.is_none()
        && exports.len() > 1;

    // Compact-rendering hints for the per-export renderers.  Set once here so
    // every code path below — sequential, `--parallel-exports`, the apply
    // path, etc. — sees a consistent mode.  Restored at the end of the run
    // so subsequent invocations within the same process (tests, library
    // callers) start with a clean slate.
    let multi_export = export_name.is_none() && exports.len() > 1;
    let prev_multi = MULTI_EXPORT_MODE.swap(multi_export, AtomicOrdering::Relaxed);
    let prev_concurrent = MULTI_EXPORT_CONCURRENT.swap(run_parallel, AtomicOrdering::Relaxed);
    struct ResetMultiExport(bool, bool);
    impl Drop for ResetMultiExport {
        fn drop(&mut self) {
            MULTI_EXPORT_MODE.store(self.0, AtomicOrdering::Relaxed);
            MULTI_EXPORT_CONCURRENT.store(self.1, AtomicOrdering::Relaxed);
        }
    }
    let _reset_multi = ResetMultiExport(prev_multi, prev_concurrent);

    let mut summaries: Vec<RunSummary> = Vec::with_capacity(exports.len());
    let mut failures: Vec<String> = Vec::new();

    if run_parallel {
        log::info!(
            "running {} exports in parallel (separate state DB connection per export)",
            exports.len()
        );

        // In threads mode every export emits the same `ChildEvent` stream
        // that `--parallel-export-processes` children emit, but routed
        // through an in-process `mpsc` channel.  A single UI thread (the
        // same `parent_ui::run_ui` used for the process-mode parent) owns
        // stderr and renders one card line per export — no indicatif, no
        // multi-bar coordination headache, no scrollback artefacts from
        // concurrent redraws.  Ensure stderr is also pre-migrated so child
        // threads opening their own `StateStore` don't race on schema DDL.
        if let Err(e) = StateStore::open(config_path) {
            return Err(anyhow::anyhow!(
                "state: failed to initialize state DB before spawning export threads: {:#}",
                e
            ));
        }
        let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
        ipc::install_in_process_tx(tx);
        let ui_thread = std::thread::Builder::new()
            .name("rivet-ui".to_string())
            .spawn(move || parent_ui::run_ui(rx))
            .ok();

        let collected: std::sync::Mutex<Vec<(Result<()>, RunSummary)>> =
            std::sync::Mutex::new(Vec::with_capacity(exports.len()));
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for &export in &exports {
                handles.push(s.spawn(|| {
                    let state = match StateStore::open(config_path) {
                        Ok(s) => s,
                        Err(e) => {
                            let err = anyhow::anyhow!(
                                "export '{}': failed to open state database: {:#}",
                                export.name,
                                e
                            );
                            let summary = job::synthetic_failed_summary(&export.name, &err);
                            return (Err(err), summary);
                        }
                    };
                    job::run_export_job(config_path, &config, export, &state, &config_dir, &opts)
                }));
            }
            for h in handles {
                match h.join() {
                    Ok(pair) => collected.lock().unwrap().push(pair),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
        });

        // All exports are done → drop the sender so `parent_ui::run_ui`
        // sees the channel close and exits cleanly (committing the final
        // card stack to scrollback).  Joining is best-effort: even if the
        // UI thread is wedged we still want to print the run aggregate
        // below.
        ipc::clear_in_process_tx();
        if let Some(t) = ui_thread {
            let _ = t.join();
        }

        for (res, summary) in collected.into_inner().unwrap() {
            if let Err(e) = res {
                failures.push(format!("{e:#}"));
            }
            summaries.push(summary);
        }
    } else {
        let state = StateStore::open(config_path)?;
        for export in &exports {
            let (res, summary) =
                job::run_export_job(config_path, &config, export, &state, &config_dir, &opts);
            if let Err(e) = res {
                failures.push(format!("{e:#}"));
            }
            summaries.push(summary);
        }
    }

    let finished_at = chrono::Utc::now();
    // Skip the aggregate for single-export runs.  Two cases this catches:
    //   1) `rivet run --export X` (manual one-off): the per-export block
    //      already says everything, an aggregate of one row is just noise.
    //   2) Children spawned by `--parallel-export-processes`: each child
    //      enters this code path with exports.len() == 1.  The parent
    //      (parallel_processes branch above) builds the run-wide aggregate
    //      from every child's `export_metrics` row, so a child-level
    //      aggregate would just write a duplicate into `run_aggregate`.
    // Force-write the JSON file even when skipping, so `--summary-output`
    // remains useful for one-off runs.
    if exports.len() > 1 {
        let parallel_mode = if run_parallel {
            "parallel-threads"
        } else {
            "sequential"
        };
        let entries: Vec<_> = summaries
            .iter()
            .map(aggregate::entry_from_summary)
            .collect();
        let agg = aggregate::build(
            entries,
            started_at,
            finished_at,
            Some(config_path),
            parallel_mode,
        );
        aggregate::print(&agg);
        // Open a fresh state handle for persisting the aggregate so we don't
        // assume which thread owned the per-export `StateStore` above.
        match StateStore::open(config_path) {
            Ok(state) => aggregate::persist(&state, &agg, summary_output),
            Err(e) => log::warn!(
                "aggregate: cannot open state DB to record run aggregate: {:#}",
                e
            ),
        }
        if json_output {
            print_json_summary(&agg);
        }
    } else if summary_output.is_some() || json_output {
        // One export, but the user asked for a summary file and/or JSON stdout —
        // honour both without polluting the DB or stderr.
        let entries: Vec<_> = summaries
            .iter()
            .map(aggregate::entry_from_summary)
            .collect();
        let agg = aggregate::build(
            entries,
            started_at,
            finished_at,
            Some(config_path),
            "sequential",
        );
        if let Some(out) = summary_output
            && let Err(e) =
                std::fs::write(out, serde_json::to_string_pretty(&agg).unwrap_or_default())
        {
            log::warn!(
                "aggregate: failed to write summary JSON to {}: {:#}",
                out.display(),
                e
            );
        }
        if json_output {
            print_json_summary(&agg);
        }
    }

    if !failures.is_empty() {
        anyhow::bail!("{}", failures.join("; "));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SourceConfig, SourceType};
    use crate::plan::{
        CompressionType, DestinationConfig, DestinationType, DiagnosticLevel, ExtractionStrategy,
        FormatType, MetaColumns, ResolvedRunPlan, validate_plan,
    };
    use crate::tuning::SourceTuning;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(2_684_354_560), "2.5 GB");
    }

    #[test]
    fn strip_chunked_recovery_hint_strips_use_form() {
        let m = "export 'users': chunk checkpoint run 'users_x' still in progress; \
                 use `rivet run --config foo.yaml --export users --resume` or \
                 `rivet state reset-chunks --config foo.yaml --export users`";
        let (cause, hinted) = strip_chunked_recovery_hint(m);
        assert!(hinted);
        assert_eq!(
            cause,
            "export 'users': chunk checkpoint run 'users_x' still in progress"
        );
    }

    #[test]
    fn strip_chunked_recovery_hint_strips_fix_errors_form() {
        let m = "export 'a': chunk checkpoint incomplete (3 tasks not completed); \
                 fix errors and `rivet run --config c.yaml --export a --resume` or \
                 `rivet state reset-chunks --config c.yaml --export a`";
        let (cause, hinted) = strip_chunked_recovery_hint(m);
        assert!(hinted);
        assert_eq!(
            cause,
            "export 'a': chunk checkpoint incomplete (3 tasks not completed)"
        );
    }

    #[test]
    fn strip_chunked_recovery_hint_passthrough_when_no_hint() {
        let m = "export 'q': source connection refused; retry exhausted";
        let (cause, hinted) = strip_chunked_recovery_hint(m);
        assert!(!hinted);
        assert_eq!(cause, m);
    }

    #[test]
    fn clamp_line_truncates_with_ellipsis() {
        assert_eq!(clamp_line("short", 80), "short");
        assert_eq!(clamp_line("hello world", 8), "hello w…");
        let s = "αβγδ".repeat(50);
        let out = clamp_line(&s, 10);
        assert_eq!(out.chars().count(), 10);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn format_bytes_boundary_values() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1025), "1.0 KB");
        assert_eq!(format_bytes(1_048_575), "1024.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_823), "1024.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    fn minimal_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "test_export".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::default(),
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
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        }
    }

    #[test]
    fn test_run_summary_fields() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);
        assert_eq!(summary.export_name, "test_export");
        assert_eq!(summary.status, "running");
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
        assert_eq!(summary.tuning_profile, "balanced (default)");
        assert_eq!(summary.batch_size, 10_000);
        assert_eq!(summary.format, "parquet");
        assert_eq!(summary.mode, "full");
        assert!(
            summary.run_id.starts_with("test_export_"),
            "run_id should start with export name, got: {}",
            summary.run_id
        );
    }

    // ─── RunSummary::new() journal invariants ────────────────────────────────

    /// `RunSummary::new()` must immediately record a `PlanResolved` event as the
    /// first journal entry.  This satisfies the "what was planned?" query from ADR-0001.
    #[test]
    fn run_summary_new_records_plan_resolved_as_first_event() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);

        assert!(
            !summary.journal.entries.is_empty(),
            "journal must have at least one entry after RunSummary::new()"
        );
        assert!(
            matches!(
                summary.journal.entries[0].event,
                crate::journal::RunEvent::PlanResolved(_)
            ),
            "first journal event must be PlanResolved, got: {:?}",
            summary.journal.entries[0].event
        );
    }

    /// The `PlanSnapshot` recorded inside `PlanResolved` must faithfully capture
    /// key fields from the `ResolvedRunPlan`.
    #[test]
    fn run_summary_plan_snapshot_matches_plan_fields() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);

        let snap = summary
            .journal
            .plan_snapshot()
            .expect("plan_snapshot() must be Some after RunSummary::new()");

        assert_eq!(snap.export_name, plan.export_name);
        assert_eq!(snap.validate, plan.validate);
        assert_eq!(snap.reconcile, plan.reconcile);
        assert_eq!(snap.resume, plan.resume);
        assert_eq!(snap.batch_size, plan.tuning.batch_size);
    }

    /// The journal's `run_id` must match the `RunSummary`'s `run_id`.
    #[test]
    fn run_summary_journal_run_id_matches_summary_run_id() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);
        assert_eq!(
            summary.journal.run_id, summary.run_id,
            "journal run_id must match summary run_id"
        );
    }

    // ─── Rejected plan gate ──────────────────────────────────────────────────

    /// Gap 7 — `run_export_job` bails before execution when `validate_plan` returns
    /// a `Rejected` diagnostic.  The wiring is in `run_export_job` (this file,
    /// ~line 210): if `rejected` is non-empty, the function returns `anyhow::bail!`.
    ///
    /// This test verifies the *condition* that triggers the bail: that `validate_plan`
    /// does in fact produce a `Rejected` diagnostic for the stdout+split combination.
    /// The gate itself (`run_export_job`) cannot be called directly in tests because
    /// it requires a live database connection and config; we test its precondition here.
    #[test]
    fn rejected_plan_produces_rejected_diagnostic_blocking_run_export_job() {
        let mut plan = minimal_plan();
        // stdout + max_file_size triggers check_stdout_split → Rejected.
        plan.destination.destination_type = DestinationType::Stdout;
        plan.max_file_size_bytes = Some(10 * 1024 * 1024);

        let diags = validate_plan(&plan);
        let rejected_count = diags
            .iter()
            .filter(|d| d.level == DiagnosticLevel::Rejected)
            .count();

        assert!(
            rejected_count > 0,
            "stdout + max_file_size must produce a Rejected diagnostic so that \
             run_export_job bails before calling run_with_reconnect; got: {:?}",
            diags
                .iter()
                .map(|d| (&d.rule, &d.level))
                .collect::<Vec<_>>()
        );
    }

    /// stdout + chunked strategy also triggers a Rejected diagnostic (check_stdout_chunked).
    #[test]
    fn rejected_plan_stdout_chunked_blocks_run_export_job() {
        use crate::plan::ChunkedPlan;
        let mut plan = minimal_plan();
        plan.destination.destination_type = DestinationType::Stdout;
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 1000,
            chunk_count: None,
            parallel: 1,
            dense: false,
            by_days: None,
            max_attempts: 3,
            checkpoint: false,
        });

        let diags = validate_plan(&plan);
        assert!(
            diags.iter().any(|d| d.level == DiagnosticLevel::Rejected),
            "stdout + chunked must produce a Rejected diagnostic"
        );
    }

    // ─── synthetic_failed_summary ────────────────────────────────────────────

    /// Pre-`RunSummary::new` failures (plan-build error, plan-validation
    /// rejection) still need to be aggregated.  `synthetic_failed_summary`
    /// produces a minimally-populated summary that aggregation can consume
    /// without panicking.
    #[test]
    fn synthetic_failed_summary_carries_error_and_status() {
        let err = anyhow::anyhow!("could not connect to source: timeout");
        let s = job::synthetic_failed_summary("orders", &err);
        assert_eq!(s.export_name, "orders");
        assert_eq!(s.status, "failed");
        assert_eq!(
            s.error_message.as_deref(),
            Some("could not connect to source: timeout")
        );
        assert!(
            s.run_id.starts_with("orders_"),
            "run_id must be derived from export name, got {}",
            s.run_id
        );
        // Aggregation reads these fields directly — they must default to zero.
        assert_eq!(s.total_rows, 0);
        assert_eq!(s.files_produced, 0);
        assert_eq!(s.bytes_written, 0);
        assert_eq!(s.duration_ms, 0);
    }

    /// `entry_from_summary` must faithfully copy fields the aggregate cares
    /// about.  This guards against silent drift if `RunAggregateEntry` or
    /// `RunSummary` gain new fields.
    #[test]
    fn aggregate_entry_from_summary_copies_observable_fields() {
        let plan = minimal_plan();
        let mut summary = RunSummary::new(&plan);
        summary.status = "success".into();
        summary.total_rows = 12_345;
        summary.files_produced = 3;
        summary.bytes_written = 9_876_543;
        summary.duration_ms = 5_000;

        let entry = aggregate::entry_from_summary(&summary);
        assert_eq!(entry.export_name, summary.export_name);
        assert_eq!(entry.status, "success");
        assert_eq!(entry.run_id, summary.run_id);
        assert_eq!(entry.rows, 12_345);
        assert_eq!(entry.files, 3);
        assert_eq!(entry.bytes, 9_876_543);
        assert_eq!(entry.duration_ms, 5_000);
        assert_eq!(entry.mode, summary.mode);
        assert_eq!(entry.error_message, None);
    }
}
