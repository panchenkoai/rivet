//! **Layer: Coordinator entrypoint** — the `rivet run` orchestrator.
//!
//! Single bridge between planning, execution, and persistence/observability.
//! Owns the multi-export render-mode flags, decides between sequential vs
//! thread-parallel vs process-parallel, and produces the run aggregate at
//! the end.
//!
//! Lives in its own file so [`crate::pipeline`] (which is read as a facade
//! by every other module) stays a thin re-export layer rather than a
//! ~300-LOC orchestrator wrapped in mod-level declarations.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::state::StateStore;

use super::summary::RunSummary;
use super::{aggregate, ipc, job, parallel_children, parent_ui, partition_expand};

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
    /// Override safety gates that would otherwise refuse to start the run.
    ///
    /// Currently used by ADR-0012 M8 — `--resume` against a prefix whose
    /// `_SUCCESS` marker is present is refused unless `--force` is given,
    /// so an operator cannot accidentally re-export over a verified
    /// dataset.  Other gates may share the same flag in the future
    /// (per ADR-0013: one `--force`, scoped to whichever gate it overrides).
    pub force: bool,
    pub params: Option<&'a std::collections::HashMap<String, String>>,
}

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

#[allow(dead_code)] // kept for future renderers; flag is still set in `run` below.
pub(crate) fn multi_export_concurrent() -> bool {
    MULTI_EXPORT_CONCURRENT.load(AtomicOrdering::Relaxed)
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

#[allow(clippy::too_many_arguments)] // CLI fan-in; surface stays stable per ADR-0013
pub fn run(
    config_path: &str,
    export_name: Option<&str>,
    validate: bool,
    reconcile: bool,
    resume: bool,
    force: bool,
    params: Option<&std::collections::HashMap<String, String>>,
    parallel_exports_cli: bool,
    parallel_export_processes_cli: bool,
    summary_output: Option<&Path>,
    json_output: bool,
) -> Result<()> {
    // F-NEW-B (0.7.5 audit): `--force` is scoped to whichever gate it
    // overrides (today: the `_SUCCESS`-already-present refusal on
    // resume).  When the operator passes `--force` without `--resume`,
    // the flag is a no-op — surface that explicitly so a typo or
    // copy-paste mistake does not pass silently.
    if force && !resume {
        log::warn!(
            "--force without --resume is a no-op today (force only overrides the resume safety \
             gate against a destination prefix whose _SUCCESS is already present)"
        );
    }
    let config = Config::load_with_params(config_path, params)?;

    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let selected: Vec<&ExportConfig> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    // Value-based partitioning: rewrite any `partition_by` export into one
    // concrete child export per bucket *before* the run loop. Non-partitioned
    // exports pass through. The owned vec must outlive the borrowed `exports`
    // view rebuilt over it, so it is declared in the enclosing scope.
    let partitioned = partition_expand::any_partitioned(&selected);
    let expanded_owned: Vec<ExportConfig>;
    let exports: Vec<&ExportConfig> = if partitioned {
        expanded_owned = partition_expand::expand_partitioned_exports(
            &selected,
            &config.source,
            &config_dir,
            params,
        )?;
        expanded_owned.iter().collect()
    } else {
        selected
    };

    let opts = RunOptions {
        validate,
        reconcile,
        resume,
        force,
        params,
    };

    let process_mode_requested =
        parallel_export_processes_cli || config.parallel_export_processes;
    // Process-mode children re-exec `rivet run --export <name>` and re-load the
    // config from disk, so they cannot see the synthesised partition child
    // names. Force in-process execution when partitioning is active.
    if partitioned && process_mode_requested {
        log::warn!(
            "partition_by: --parallel-export-processes is disabled with partitioned exports \
             (child processes re-load the config and can't see synthesised partitions); \
             running in-process"
        );
    }
    let run_parallel_processes =
        process_mode_requested && export_name.is_none() && exports.len() > 1 && !partitioned;

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
                force,
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

        // Always route through `parent_ui` — same as `--parallel-exports`.
        // Gating on `is_attended()` left VHS/ttyd on indicatif when the
        // attended bit is unset; `run_ui` already falls back to linear
        // mode for piped stderr.
        let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
        ipc::install_in_process_tx(tx);
        let ui_thread = std::thread::Builder::new()
            .name("rivet-ui".to_string())
            .spawn(move || parent_ui::run_ui(rx))
            .ok();

        for export in &exports {
            let (res, summary) =
                job::run_export_job(config_path, &config, export, &state, &config_dir, &opts);
            if let Err(e) = res {
                failures.push(format!("{e:#}"));
            }
            summaries.push(summary);
        }

        ipc::clear_in_process_tx();
        if let Some(t) = ui_thread {
            let _ = t.join();
        }
        // Single-export sequential runs still emit the detailed block after
        // the card commits to scrollback.
        if exports.len() == 1
            && let Some(summary) = summaries.last()
        {
            summary.print_stderr_block();
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
