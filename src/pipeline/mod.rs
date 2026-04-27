//! **Layer: Coordinator** (planning → execution → persistence/observability)
//!
//! `pipeline/mod.rs` is the only module allowed to bridge all three layers.
//! It reads a resolved plan (planning), dispatches to execution modules, then
//! records metrics and sends notifications (persistence/observability).
//!
//! See `docs/adr/0003-layer-classification.md` for the full module taxonomy.

mod aggregate;
mod apply_cmd;
mod chunked;
mod cli;
pub(crate) mod ipc;
pub mod journal;
pub(crate) mod parent_ui;
mod plan_cmd;
pub(crate) mod progress;
mod reconcile_cmd;
mod repair_cmd;
mod retry;
mod single;
mod sink;
mod summary;
mod validate;

pub use apply_cmd::run_apply_command;
#[allow(unused_imports)]
pub use chunked::generate_chunks;
pub use cli::{
    reset_chunk_checkpoint, reset_state, show_chunk_checkpoint, show_files, show_metrics,
    show_progression, show_state,
};
pub use plan_cmd::{PlanOutputFormat, run_plan_command};
pub use reconcile_cmd::{ReconcileOutputFormat, run_reconcile_command};
pub use repair_cmd::{RepairOutputFormat, RepairReportSource, run_repair_command};
#[allow(unused_imports)]
pub use retry::classify_error;
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
use crate::plan::{
    DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, build_plan, validate_plan,
};
use crate::state::StateStore;

use chunked::run_chunked_parallel_checkpoint;
use journal::RunEvent;
use single::run_with_reconnect;
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

/// For chunked mode: quality checks (row_count, null_ratio, uniqueness) cannot run per-batch
/// inside the chunk workers because each chunk has its own `ExportSink`. This gate runs
/// row_count bounds after all chunks complete. Null/unique checks are warned-and-skipped.
fn run_chunked_quality_gate(
    result: Result<()>,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<()> {
    result?;

    if !matches!(plan.strategy, ExtractionStrategy::Chunked(_)) {
        return Ok(());
    }
    let qc = match &plan.quality {
        Some(q) => q,
        None => return Ok(()),
    };

    let total = summary.total_rows as usize;
    let row_issues = crate::quality::check_row_count(total, qc);
    let has_unsupported = !qc.null_ratio_max.is_empty() || !qc.unique_columns.is_empty();

    if has_unsupported {
        log::warn!(
            "export '{}': quality checks null_ratio_max and unique_columns are not supported in chunked mode (each chunk processes independently); only row_count bounds are checked",
            plan.export_name
        );
    }

    if !row_issues.is_empty() {
        for issue in &row_issues {
            log::warn!("quality FAIL: {}", issue.message);
        }
        summary.quality_passed = Some(false);
        anyhow::bail!(
            "export '{}': quality checks failed (chunked aggregate)",
            plan.export_name
        );
    }

    summary.quality_passed = Some(true);
    Ok(())
}

/// Run `SELECT COUNT(*) FROM ({query})` against the source and compare with exported rows.
/// Skips reconciliation for incremental exports that used a cursor (moving target).
fn reconcile_source_count(plan: &ResolvedRunPlan, summary: &mut RunSummary) {
    if let Some(col) = plan.strategy.cursor_column() {
        log::info!(
            "reconcile: skipping for incremental export '{}' (cursor column '{}', count may differ)",
            plan.export_name,
            col
        );
        return;
    }

    let count_sql = format!(
        "SELECT COUNT(*) FROM ({}) AS _rivet_reconcile",
        plan.base_query
    );
    log::info!(
        "reconcile: running source count query for '{}'",
        plan.export_name
    );

    let mut src = match crate::source::create_source(&plan.source) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("reconcile: could not connect to source: {:#}", e);
            return;
        }
    };

    match src.query_scalar(&count_sql) {
        Ok(Some(val)) => {
            if let Ok(count) = val.parse::<i64>() {
                summary.source_count = Some(count);
                summary.reconciled = Some(summary.total_rows == count);
                if summary.total_rows != count {
                    log::warn!(
                        "reconcile MISMATCH for '{}': exported {} rows, source has {}",
                        plan.export_name,
                        summary.total_rows,
                        count
                    );
                } else {
                    log::info!(
                        "reconcile MATCH for '{}': {}/{}",
                        plan.export_name,
                        summary.total_rows,
                        count
                    );
                }
            } else {
                log::warn!(
                    "reconcile: could not parse count result '{}' as integer",
                    val
                );
            }
        }
        Ok(None) => {
            log::warn!(
                "reconcile: COUNT(*) returned NULL for '{}'",
                plan.export_name
            );
        }
        Err(e) => {
            log::warn!(
                "reconcile: count query failed for '{}': {:#}",
                plan.export_name,
                e
            );
        }
    }
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

/// Synthesize a stand-in `RunSummary` for failures that occur **before** a
/// real summary can be created (plan-build errors, plan-validation rejection).
/// Aggregation needs every export accounted for, even those that never reached
/// `RunSummary::new`.
fn synthetic_failed_summary(export_name: &str, err: &anyhow::Error) -> RunSummary {
    let run_id = format!(
        "{}_{}",
        export_name,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3f"),
    );
    let journal = journal::RunJournal::new(&run_id, export_name);
    RunSummary {
        run_id,
        export_name: export_name.to_string(),
        status: "failed".into(),
        total_rows: 0,
        files_produced: 0,
        bytes_written: 0,
        duration_ms: 0,
        peak_rss_mb: 0,
        retries: 0,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message: Some(format!("{:#}", err)),
        tuning_profile: "balanced (default)".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: String::new(),
        mode: String::new(),
        compression: String::new(),
        source_count: None,
        reconciled: None,
        journal,
    }
}

fn run_export_job(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    opts: &RunOptions<'_>,
) -> (Result<()>, RunSummary) {
    let plan = match build_plan(
        config,
        export,
        config_dir,
        opts.validate,
        opts.reconcile,
        opts.resume,
        opts.params,
    ) {
        Ok(p) => p,
        Err(e) => {
            let summary = synthetic_failed_summary(&export.name, &e);
            return (Err(e), summary);
        }
    };

    let diags = validate_plan(&plan);
    let mut rejected: Vec<String> = Vec::new();
    for d in &diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                log::error!("[{}] plan validation rejected: {}", d.rule, d.message);
                rejected.push(d.message.clone());
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }
    if !rejected.is_empty() {
        let err = anyhow::anyhow!(
            "export '{}': plan validation failed:\n  {}",
            plan.export_name,
            rejected.join("\n  ")
        );
        let summary = synthetic_failed_summary(&export.name, &err);
        return (Err(err), summary);
    }

    log::info!(
        "starting export '{}' (effective tuning: {})",
        plan.export_name,
        plan.tuning
    );

    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(&plan);

    // Record plan diagnostics that were already logged above.
    for d in &diags {
        if matches!(
            d.level,
            DiagnosticLevel::Warning | DiagnosticLevel::Degraded
        ) {
            summary.journal.record(RunEvent::PlanWarning {
                rule: d.rule.to_string(),
                message: d.message.clone(),
            });
        }
    }

    let result = if plan.strategy.requires_parallel_execution() {
        if plan.strategy.is_resumable() {
            run_chunked_parallel_checkpoint(
                config_path,
                state,
                &plan,
                &mut summary,
                chunked::ChunkSource::Detect,
            )
        } else {
            chunked::run_chunked_parallel(state, &plan, &mut summary, chunked::ChunkSource::Detect)
        }
    } else {
        run_with_reconnect(state, &plan, &mut summary, config_path)
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    let tuning_class = plan.tuning.profile_name().to_string();
    let result = run_chunked_quality_gate(result, &plan, &mut summary);
    let failed = result.is_err();
    match &result {
        Ok(()) => {
            if summary.status == "running" {
                summary.status = "success".into();
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            summary.error_message = Some(format!("{:#}", e));
            log::error!("export '{}' failed: {:#}", plan.export_name, e);
        }
    }

    if plan.reconcile && !failed {
        reconcile_source_count(&plan, &mut summary);
        if let (Some(source_count), Some(matched)) = (summary.source_count, summary.reconciled) {
            summary.journal.record(RunEvent::ReconciliationResult {
                source_count,
                exported_rows: summary.total_rows,
                matched,
            });
        }
    }

    summary.journal.record(RunEvent::RunCompleted {
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        duration_ms: summary.duration_ms,
    });

    if let Err(e) = state.record_metric(
        &summary.export_name,
        &summary.run_id,
        summary.duration_ms,
        summary.total_rows,
        Some(summary.peak_rss_mb),
        &summary.status,
        summary.error_message.as_deref(),
        Some(&tuning_class),
        Some(&summary.format),
        Some(&summary.mode),
        summary.files_produced as i64,
        summary.bytes_written as i64,
        summary.retries as i64,
        summary.validated,
        summary.schema_changed,
    ) {
        log::warn!(
            "export '{}': metrics write failed (run outcome not stored): {:#}",
            summary.export_name,
            e
        );
    }

    summary.print();
    crate::notify::maybe_send(config.notifications.as_ref(), &summary);

    let final_result = if failed { result } else { Ok(()) };
    (final_result, summary)
}

/// Execute a pre-resolved plan with a caller-supplied `ChunkSource`.
///
/// Used by `rivet apply`: the plan comes from a deserialized `PlanArtifact` so
/// `build_plan` is skipped.  Everything else — quality gate, metrics, state
/// persistence — is identical to `run_export_job`.
pub(crate) fn run_export_job_with_chunk_source(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    chunk_source: chunked::ChunkSource,
) -> Result<()> {
    // Re-validate the plan from the artifact (fast, no DB queries).
    let diags = validate_plan(plan);
    for d in &diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                anyhow::bail!(
                    "export '{}': plan validation rejected: {}",
                    plan.export_name,
                    d.message
                );
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }

    log::info!(
        "apply: starting export '{}' (tuning: {})",
        plan.export_name,
        plan.tuning
    );

    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(plan);

    let result = if plan.strategy.requires_parallel_execution() {
        if plan.strategy.is_resumable() {
            // apply does not support checkpoint-parallel resume; use Detect fallback
            run_chunked_parallel_checkpoint("", state, plan, &mut summary, chunk_source)
        } else {
            chunked::run_chunked_parallel(state, plan, &mut summary, chunk_source)
        }
    } else {
        run_with_reconnect(state, plan, &mut summary, "")
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    let tuning_class = plan.tuning.profile_name().to_string();
    let result = run_chunked_quality_gate(result, plan, &mut summary);
    let failed = result.is_err();

    match &result {
        Ok(()) => {
            if summary.status == "running" {
                summary.status = "success".into();
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            summary.error_message = Some(format!("{:#}", e));
            log::error!("apply '{}' failed: {:#}", plan.export_name, e);
        }
    }

    if let Err(e) = state.record_metric(
        &summary.export_name,
        &summary.run_id,
        summary.duration_ms,
        summary.total_rows,
        Some(summary.peak_rss_mb),
        &summary.status,
        summary.error_message.as_deref(),
        Some(&tuning_class),
        Some(&summary.format),
        Some(&summary.mode),
        summary.files_produced as i64,
        summary.bytes_written as i64,
        summary.retries as i64,
        summary.validated,
        summary.schema_changed,
    ) {
        log::warn!(
            "apply '{}': metrics write failed: {:#}",
            summary.export_name,
            e
        );
    }

    summary.print();

    if failed { result } else { Ok(()) }
}

/// Re-invoke this binary once per export. Children do not inherit parallel flags, so there is no recursion.
///
/// Each child has `RIVET_IPC_EVENTS=1` set in its environment and runs with
/// stdout piped: it emits one JSON line per significant event
/// ([`ipc::ChildEvent`]) on stdout instead of drawing its own progress bar
/// or per-export summary.  The parent reads those events in dedicated reader
/// threads and forwards them through an `mpsc` channel to a single UI thread
/// that owns an `indicatif::MultiProgress`, drawing one card per export.
///
/// Returns `(Result, child_failures)` so the caller can build an aggregate.
/// `child_failures` maps export name → error message for children that did not
/// exit cleanly; on success this map is empty.
fn run_exports_as_child_processes(
    config_path: &str,
    exports: &[&ExportConfig],
    validate: bool,
    reconcile: bool,
    resume: bool,
    params: Option<&std::collections::HashMap<String, String>>,
) -> (Result<()>, std::collections::HashMap<String, String>) {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;

    use super::pipeline::ipc::{ChildEvent, ENV_IPC_EVENTS};
    use super::pipeline::parent_ui::{ChildWaitStatus, UiMessage};

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            return (
                Err(anyhow::anyhow!(
                    "failed to resolve rivet executable for child processes: {:#}",
                    e
                )),
                std::collections::HashMap::new(),
            );
        }
    };

    let config_arg = std::path::Path::new(config_path)
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from(config_path));

    log::info!(
        "running {} exports as separate rivet processes (each child: single `--export`; SQLite state WAL allows concurrent writers; IPC card UI on)",
        exports.len()
    );

    // Single channel feeds the UI thread.  Each child gets a stdout reader
    // thread that parses NDJSON and forwards events with the export name
    // resolved from the event itself (not from the child handle), so out-of-
    // order events still route correctly.
    let (tx, rx) = mpsc::channel::<UiMessage>();

    let ui_handle = std::thread::Builder::new()
        .name("rivet-ipc-ui".into())
        .spawn(move || parent_ui::run_ui(rx))
        .ok();

    let mut children: Vec<(String, std::process::Child)> = Vec::with_capacity(exports.len());
    let mut reader_handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(exports.len());
    let mut spawn_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for export in exports {
        let mut cmd = Command::new(&exe);
        cmd.arg("run")
            .arg("--config")
            .arg(&config_arg)
            .arg("--export")
            .arg(export.name.as_str());
        if validate {
            cmd.arg("--validate");
        }
        if reconcile {
            cmd.arg("--reconcile");
        }
        if resume {
            cmd.arg("--resume");
        }
        if let Some(p) = params {
            for (k, v) in p {
                cmd.arg("--param").arg(format!("{k}={v}"));
            }
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            // stderr stays inherited so env_logger output still reaches the
            // user's terminal.  indicatif draws above the log lines via the
            // standard MultiProgress mechanism.
            .env(ENV_IPC_EVENTS, "1");
        log::debug!("spawning child for export '{}': {:?}", export.name, cmd);
        match cmd.spawn() {
            Ok(mut child) => {
                if let Some(stdout) = child.stdout.take() {
                    let tx = tx.clone();
                    let export_name = export.name.clone();
                    let h = std::thread::Builder::new()
                        .name(format!("rivet-ipc-rx-{}", export.name))
                        .spawn(move || {
                            let reader = BufReader::new(stdout);
                            for line in reader.lines() {
                                let line = match line {
                                    Ok(l) => l,
                                    Err(e) => {
                                        log::debug!(
                                            "ipc: child '{}' stdout read error: {:#}",
                                            export_name,
                                            e
                                        );
                                        break;
                                    }
                                };
                                let trimmed = line.trim();
                                if trimmed.is_empty() {
                                    continue;
                                }
                                match serde_json::from_str::<ChildEvent>(trimmed) {
                                    Ok(ev) => {
                                        let _ = tx.send(UiMessage::Event(ev));
                                    }
                                    Err(e) => {
                                        log::debug!(
                                            "ipc: child '{}' emitted unparsable line: {} ({:#})",
                                            export_name,
                                            trimmed,
                                            e
                                        );
                                    }
                                }
                            }
                        })
                        .ok();
                    if let Some(h) = h {
                        reader_handles.push(h);
                    }
                }
                children.push((export.name.clone(), child));
            }
            Err(e) => {
                spawn_failures.insert(export.name.clone(), format!("spawn failed: {e:#}"));
            }
        }
    }

    let mut failures = Vec::new();
    let mut wait_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for (name, mut child) in children {
        let status = match child.wait() {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("wait failed: {e:#}");
                failures.push(format!("export '{name}': {msg}"));
                wait_failures.insert(name.clone(), msg.clone());
                let _ = tx.send(UiMessage::ChildClosed {
                    export_name: name,
                    wait_status: ChildWaitStatus::Failed(msg),
                });
                continue;
            }
        };
        if !status.success() {
            let code = status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string());
            let msg = format!("exited with status {code}");
            failures.push(format!("export '{name}' {msg}"));
            wait_failures.insert(name.clone(), msg.clone());
            let _ = tx.send(UiMessage::ChildClosed {
                export_name: name,
                wait_status: ChildWaitStatus::Failed(msg),
            });
        } else {
            let _ = tx.send(UiMessage::ChildClosed {
                export_name: name,
                wait_status: ChildWaitStatus::Success,
            });
        }
    }

    // Drop our own sender so the UI thread can observe the channel closing
    // once every reader thread has also exited.
    drop(tx);
    for h in reader_handles {
        let _ = h.join();
    }
    if let Some(h) = ui_handle {
        let _ = h.join();
    }

    let mut all_failures = spawn_failures;
    all_failures.extend(wait_failures);
    for (name, msg) in &all_failures {
        if !failures.iter().any(|f| f.contains(name)) {
            failures.push(format!("export '{name}': {msg}"));
        }
    }

    let result = if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("{}", failures.join("; ")))
    };
    (result, all_failures)
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
        let (result, child_failures) = run_exports_as_child_processes(
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
            }
            Err(e) => log::warn!(
                "aggregate: cannot open state DB to record run aggregate: {:#}",
                e
            ),
        }
        return result;
    }

    let run_parallel = (parallel_exports_cli || config.parallel_exports)
        && export_name.is_none()
        && exports.len() > 1;

    let mut summaries: Vec<RunSummary> = Vec::with_capacity(exports.len());
    let mut failures: Vec<String> = Vec::new();

    if run_parallel {
        log::info!(
            "running {} exports in parallel (separate state DB connection per export)",
            exports.len()
        );
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
                            let summary = synthetic_failed_summary(&export.name, &err);
                            return (Err(err), summary);
                        }
                    };
                    run_export_job(config_path, &config, export, &state, &config_dir, &opts)
                }));
            }
            for h in handles {
                match h.join() {
                    Ok(pair) => collected.lock().unwrap().push(pair),
                    Err(payload) => std::panic::resume_unwind(payload),
                }
            }
        });
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
                run_export_job(config_path, &config, export, &state, &config_dir, &opts);
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
    } else if let Some(out) = summary_output {
        // One export, but the user explicitly asked for a summary file —
        // honour it without polluting the DB or stderr.
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
        if let Err(e) = std::fs::write(out, serde_json::to_string_pretty(&agg).unwrap_or_default())
        {
            log::warn!(
                "aggregate: failed to write summary JSON to {}: {:#}",
                out.display(),
                e
            );
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
                tuning: None,
                tls: None,
            },
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
                journal::RunEvent::PlanResolved(_)
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
        let s = synthetic_failed_summary("orders", &err);
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
