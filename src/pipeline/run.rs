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
use super::{aggregate, finalize, ipc, job, parallel_children, parent_ui, partition_expand};

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

/// Env marker a parent sets on re-exec'd children (--parallel-export-processes,
/// apply's wave-parallel batches) so a single-export child still knows sibling
/// processes share its server-counter windows. In-process concurrency uses the
/// atomic; the env covers the process boundary the atomic cannot cross.
pub(crate) const ENV_CONCURRENT_SIBLINGS: &str = "RIVET_CONCURRENT_SIBLINGS";

pub(crate) fn multi_export_concurrent() -> bool {
    MULTI_EXPORT_CONCURRENT.load(AtomicOrdering::Relaxed)
        || std::env::var_os(ENV_CONCURRENT_SIBLINGS).is_some()
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

/// Emit captured child stderr from a parallel run. It's verbose — every child's
/// full run card — so write it to a timestamped log beside the config and print
/// a one-line pointer, instead of flooding the console with all N exports'
/// stderr. Falls back to the inline console dump if the file can't be written.
fn emit_child_stderr(dump: &str, dir: &Path) {
    if dump.is_empty() {
        return;
    }
    let name = format!(
        "rivet-child-stderr-{}.log",
        chrono::Utc::now().format("%Y%m%dT%H%M%S")
    );
    let path = dir.join(name);
    match std::fs::write(&path, dump) {
        // stderr, not stdout — stdout may carry the machine-readable `--json`
        // run summary, which this pointer would otherwise corrupt.
        Ok(()) => eprintln!(
            "\n  child stderr (full per-export logs) → {}",
            path.display()
        ),
        Err(e) => {
            log::warn!(
                "could not write child stderr to {} ({e}); printing inline",
                path.display()
            );
            use std::io::Write;
            let mut h = std::io::stderr().lock();
            let _ = h.write_all(dump.as_bytes());
            let _ = h.flush();
        }
    }
}

/// Whether THIS process emits the run-over-run throughput self-check for the
/// exports it just ran, or defers to the parent that spawned it.
///
/// The contract is EXACTLY ONCE per export per run, and there are two ways to
/// break it. The first shipped: the check was wired into `run()` only, so
/// `apply` and `apply --pool` skipped it whenever one export was in scope. The
/// second is its mirror: `--parallel-export-processes` and `apply --parallel`
/// re-exec each export as `rivet run --export X`, so every child ALSO reaches a
/// tail — and it does not self-cancel, because the parent rebuilds each child's
/// entry from the state DB carrying the CHILD's own `run_id`, so the parent's
/// baseline query excludes the same row the child excluded and reproduces the
/// child's line verbatim.
///
/// The parent wins the tie, for a reason stronger than symmetry: a child's
/// stderr is CAPTURED (`emit_child_stderr` diverts it to a
/// `rivet-child-stderr-*.log` beside the config), so a WARN the child emits is
/// not on the operator's console at all — while the parent prints beside the run
/// aggregate, and knows the run-wide concurrency mode the hedge text needs.
/// `RIVET_IPC_EVENTS` is the marker: `run_exports_as_child_processes` sets it on
/// EVERY child (not just concurrent batches, unlike `ENV_CONCURRENT_SIBLINGS`),
/// which is exactly the "my parent is aggregating me" declaration.
fn owns_throughput_self_check(reexec_child: bool) -> bool {
    !reexec_child
}

/// The ONE call site of [`aggregate::warn_throughput_regressions`]: every
/// orchestrator tail (`run`'s two branches, `run_waves`, `run_pool`) routes its
/// entries through here, unconditionally — no `len() > 1` gate, because the
/// field regression this check exists for hit a single-export config.
///
/// Structural, not conventional: the call is welded to the aggregate the tail
/// already builds (build → print? → self-check → persist?), and
/// `every_orchestrator_tail_routes_the_self_check_through_one_seam` fails the
/// build if a tail grows a second path or calls the aggregate helper directly.
fn self_check_throughput(
    state: &StateStore,
    entries: &[crate::state::RunAggregateEntry],
    run_mode: &str,
) {
    if !owns_throughput_self_check(ipc::ipc_events_enabled()) {
        log::debug!(
            "throughput self-check: deferred to the parent process (this is a re-exec'd child; \
             its stderr is captured, the parent prints beside the run aggregate)"
        );
        return;
    }
    aggregate::warn_throughput_regressions(state, entries, run_mode);
}

/// What [`run`]'s tail does with the aggregate it just built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailPlan {
    /// Print the run-summary card and record the `run_aggregate` row.
    aggregate: bool,
    /// Write `--summary-output` from the tail itself (the aggregate path writes
    /// it through [`aggregate::persist`], so exactly one of the two does).
    machine_output: bool,
}

/// The tail's routing rule, pure because the value-level oracle needs a live
/// source (a real `run()` needs a database, a destination and a state DB).
///
/// `n_exports` is the count AFTER `partition_by` expansion, which is why the
/// zero case is not hypothetical: `expand_one` logs "found no rows — nothing to
/// export" and pushes no children, so an empty table leaves the run with zero
/// exports and a machine consumer still holding a `--json` pipe. `<= 1` (not
/// `== 1`) is the whole fix — an empty-but-valid `total_exports: 0` document
/// beats silence, which parses as neither JSON nor "nothing happened".
fn tail_plan(n_exports: usize, machine_output_requested: bool) -> TailPlan {
    TailPlan {
        aggregate: n_exports > 1,
        machine_output: machine_output_requested && n_exports <= 1,
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

    // Seeds the card-table name column so it aligns from the first redraw
    // (the renderer can't see a long name until its export emits `Started`).
    let name_floor = exports
        .iter()
        .map(|e| e.name.chars().count())
        .max()
        .unwrap_or(0);
    let process_mode_requested = parallel_export_processes_cli || config.parallel_export_processes;
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

    // Stamped here for the paths that open no harm bracket; the bracketed paths
    // below RE-stamp from `RunHarmBracket::open`, which hands back an instant
    // taken AFTER its source probe so the window never charges the run for
    // rivet's own instrumentation (see `snapshot_then_stamp`).
    let mut started_at = chrono::Utc::now();

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

        // Every child sets ENV_CONCURRENT_SIBLINGS, so every child's per-export
        // DIAGNOSIS hedges and points at "the run-level harm line" — which only
        // the PARENT can emit (each child sees one export's window). Same
        // bracket the pool uses, so the pointer is not a dangling reference.
        let (run_harm, window_start) = RunHarmBracket::open(&config.source);
        started_at = window_start;
        let (result, child_failures, stderr_dump) =
            parallel_children::run_exports_as_child_processes(
                config_path,
                &exports,
                validate,
                reconcile,
                resume,
                force,
                params,
                name_floor,
            );
        // Stamp the window BEFORE closing the bracket: the close opens a source
        // connection and queries the counters, so stamping after it folds the
        // instrumentation's own round-trip into the aggregate's duration and
        // throughput (same ordering the pool now keeps).
        let finished_at = chrono::Utc::now();
        run_harm.close_and_warn(HarmWindow::Parallel {
            exports: exports.len(),
        });
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
                self_check_throughput(&state, &agg.per_export, &agg.parallel_mode);
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
        // Captured child stderr (verbose per-export cards) goes to a file
        // artifact beside the config, with a one-line console pointer — the run
        // summary stays clean instead of flooding with every child's stderr.
        emit_child_stderr(&stderr_dump, &config_dir);
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
    // Keep the typed `anyhow::Error`s (not flattened strings) so the final bail
    // can carry a representative one — its DataIntegrityError / SchemaDriftError /
    // transient marker downcasts through anyhow's context chain in
    // `error::classify_exit`, giving the right process exit code without grepping
    // the message.
    let mut failures: Vec<anyhow::Error> = Vec::new();
    // Set by the concurrent path that closes a run-harm bracket: the export
    // window ends when the exports do, not when the counter probe returns.
    // `None` on the sequential path, which opens no bracket.
    let mut window_end: Option<chrono::DateTime<chrono::Utc>> = None;

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
        let n_cards = exports.len();
        let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
        ipc::install_in_process_tx(tx);
        let ui_thread = std::thread::Builder::new()
            .name("rivet-ui".to_string())
            .spawn(move || parent_ui::run_ui(rx, name_floor, n_cards))
            .ok();

        // In-process concurrency sets MULTI_EXPORT_CONCURRENT, so every export's
        // DIAGNOSIS hedges and points at the run-level harm line — emit it here
        // (same bracket as the pool and the process-parallel parent).
        let (run_harm, window_start) = RunHarmBracket::open(&config.source);
        started_at = window_start;
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
        // Stamp the window BEFORE the bracket close queries the source, so the
        // aggregate's duration excludes the instrumentation round-trip.
        window_end = Some(chrono::Utc::now());
        run_harm.close_and_warn(HarmWindow::Parallel {
            exports: exports.len(),
        });

        for (res, summary) in collected.into_inner().unwrap() {
            if let Err(e) = res {
                failures.push(e);
            }
            summaries.push(summary);
        }
    } else {
        let state = StateStore::open(config_path)?;

        // Always route through `parent_ui` — same as `--parallel-exports`.
        // Gating on `is_attended()` left VHS/ttyd on indicatif when the
        // attended bit is unset; `run_ui` already falls back to linear
        // mode for piped stderr.
        let n_cards = exports.len();
        let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
        ipc::install_in_process_tx(tx);
        let ui_thread = std::thread::Builder::new()
            .name("rivet-ui".to_string())
            .spawn(move || parent_ui::run_ui(rx, name_floor, n_cards))
            .ok();

        for export in &exports {
            let (res, summary) =
                job::run_export_job(config_path, &config, export, &state, &config_dir, &opts);
            if let Err(e) = res {
                failures.push(e);
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

    let finished_at = window_end.unwrap_or_else(chrono::Utc::now);
    // ONE aggregate for every shape of this run, then a routing decision over
    // it (see [`tail_plan`]). Building it unconditionally is what closes the
    // zero-export hole: a `partition_by` export whose table is currently empty
    // expands to NO children, and the previous `> 1` / `== 1` pair matched
    // neither, so `--json` printed nothing and `--summary-output` was never
    // created — on a run that exited 0 (bughunt 2026-08-14).
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
    let plan = tail_plan(exports.len(), summary_output.is_some() || json_output);
    if plan.aggregate {
        aggregate::print(&agg);
    }
    // Open a fresh state handle so we don't assume which thread owned the
    // per-export `StateStore` above. The self-check runs for EVERY shape —
    // including the lone export, which is exactly the shape the 2026-08-13
    // field regression had — while the aggregate CARD and the `run_aggregate`
    // row stay multi-export-only (an aggregate of one row is noise, and a
    // child of --parallel-export-processes would write a duplicate of the
    // parent's).
    match StateStore::open(config_path) {
        Ok(state) => {
            self_check_throughput(&state, &agg.per_export, &agg.parallel_mode);
            if plan.aggregate {
                aggregate::persist(&state, &agg, summary_output);
            }
        }
        Err(e) => log::warn!(
            "aggregate: cannot open state DB to record run aggregate: {:#}",
            e
        ),
    }
    if plan.machine_output {
        // 0 or 1 exports, and the user asked for a summary file and/or JSON
        // stdout — honour both without polluting the DB or stderr (the
        // multi-export path writes the file through `persist` above).
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
    }
    if json_output {
        print_json_summary(&agg);
    }

    if !failures.is_empty() {
        // Carry a representative typed failure as the returned error so
        // `error::classify_exit` downcasts the marker (DataIntegrityError=3,
        // SchemaDriftError=4, transient=2) through anyhow's context chain. Pick
        // the most "stop-worthy" class — data-integrity (possibly-wrong data)
        // outranks schema-drift, which outranks retryable, which outranks
        // generic — so a mixed batch exits on the scariest reason.
        let primary_idx = representative_failure_idx(&failures).unwrap();
        let primary = failures.remove(primary_idx);
        if failures.is_empty() {
            // Single failure — return it verbatim (its own message + marker).
            return Err(primary);
        }
        // Multiple failures: list the others as higher-level context; `primary`
        // (with its typed marker) rides underneath so the downcast still finds it.
        let others = failures
            .iter()
            .map(|e| format!("{e:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(primary.context(format!(
            "{} export(s) failed; representative error follows (also: {others})",
            failures.len() + 1
        )));
    }

    Ok(())
}

/// `rivet apply -c config.yaml` (plan→apply cycle): run every export of the
/// config **wave by wave** in ascending `wave:` order — exports with no `wave:`
/// run last — reusing the same per-export job + run aggregate as [`run`]. This
/// first cut runs each wave's exports SEQUENTIALLY (deterministic); safety-aware
/// within-wave parallelism is a follow-up, and `partition_by` exports are not
/// expanded here yet (use `rivet run` for those).
pub(crate) fn run_waves(
    config_path: &str,
    force: bool,
    parallel_cli: bool,
    resume: bool,
) -> Result<()> {
    let config = Config::load_with_params(config_path, None)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let opts = RunOptions {
        validate: false,
        reconcile: false,
        resume,
        force,
        params: None,
    };

    // Group exports by wave (ascending; an export with no `wave:` runs last).
    // The ordering is the contract apply depends on, so it lives in a pure
    // tested helper rather than hiding inline here.
    let by_wave = group_exports_by_wave(&config.exports);
    let total: usize = by_wave.iter().map(|(_, v)| v.len()).sum();
    if total == 0 {
        log::warn!("apply: config '{config_path}' defines no exports");
        return Ok(());
    }

    // `--parallel` (or `parallel_export_processes: true` in the config) opts into
    // within-wave parallelism: each wave's exports run as concurrent child
    // processes (per-child governor keeps each one source-safe), the call blocks
    // until all exit = the wave barrier. Default stays sequential.
    let parallel = parallel_cli || config.parallel_export_processes;

    // Compact per-export rendering for the SEQUENTIAL path only. The parallel
    // (subprocess) path renders the parent card stack itself and each child sees
    // `exports.len() == 1`, so the flag must stay clear there — matching `run`'s
    // parallel-processes branch.
    let prev_multi = MULTI_EXPORT_MODE.swap(total > 1 && !parallel, AtomicOrdering::Relaxed);
    struct ResetMulti(bool);
    impl Drop for ResetMulti {
        fn drop(&mut self) {
            MULTI_EXPORT_MODE.store(self.0, AtomicOrdering::Relaxed);
        }
    }
    let _reset = ResetMulti(prev_multi);

    let state = StateStore::open(config_path)?;
    // `apply --parallel` re-execs children with ENV_CONCURRENT_SIBLINGS, so each
    // child's per-export DIAGNOSIS hedges and points at "the run-level harm
    // line". Only this parent spans the whole concurrent window, so it owns the
    // bracket — one per RUN, not per wave/batch, since the counters are
    // server-global and the operator's lever (concurrency) is run-wide.
    // Opened BEFORE the window is stamped: `open` probes the source and hands
    // back the start instant, so the probe cannot land inside the window the
    // aggregate's duration and rows/s are computed over.
    let (run_harm, started_at) = match parallel.then(|| RunHarmBracket::open(&config.source)) {
        Some((bracket, window_start)) => (Some(bracket), window_start),
        None => (None, chrono::Utc::now()),
    };
    let mut summaries: Vec<RunSummary> = Vec::with_capacity(total);
    let mut failures: Vec<anyhow::Error> = Vec::new();
    // Parallel-path accumulators: per-child metrics live in the state DB, so the
    // parent reconstructs one aggregate from them after every wave has joined.
    let mut all_exports: Vec<&ExportConfig> = Vec::with_capacity(total);
    let mut child_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut combined_stderr = String::new();
    // The widest batch that ever ran at once, across every wave — the run's REAL
    // concurrency, which the cost gate (not the flag) decides. Frames the
    // run-level harm line; see [`wave_harm_window`].
    let mut peak_concurrency = 0usize;

    for (wave, exports) in &by_wave {
        let label = if *wave == u32::MAX {
            "unscheduled".to_string()
        } else {
            wave.to_string()
        };
        // Skip-completed under --resume: an export whose destination already has
        // `_SUCCESS` is done — re-running must not redo it (and would hit the
        // resume gate). The rest run with `resume`, so an incomplete chunked
        // export continues from its checkpoint. Reuses `finalize`'s prior-run
        // probe rather than re-implementing the marker check.
        let pending: Vec<&ExportConfig> = exports
            .iter()
            .copied()
            .filter(|e| {
                // Probe the EXPANDED destination, not the raw template. A
                // templated prefix (`{export}`/`{table}`/`{date}`) never matches a
                // literal `_SUCCESS` path, so a completed templated export was
                // never skipped and instead re-ran into the resume gate. Resolve
                // the same way `rivet run` does at write time (today's UTC date, no
                // `{run_id}` — a run-unique prefix is fresh every run, so there is
                // nothing to skip and the literal token correctly never matches).
                let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&e.name);
                let expanded = crate::destination::placeholder::expand_destination(
                    e.destination.clone(),
                    &ctx,
                );
                let done = resume && finalize::destination_has_success(&expanded);
                if done {
                    log::info!(
                        "apply: skipping '{}' — destination already complete (_SUCCESS)",
                        e.name
                    );
                }
                !done
            })
            .collect();
        if pending.is_empty() {
            continue;
        }
        if total > 1 {
            println!("\n  ── wave {label} · {} export(s) ──", pending.len());
        }
        // The wave barrier is the loop itself: each strategy below fully drains
        // the wave (the sequential loop, or the blocking child-process join)
        // before the next iteration starts the next wave.
        if parallel {
            // Cost safety-gate: within the wave, the cheap (`parallel_safe`)
            // exports run together in ONE concurrent batch; every heavier export
            // runs ALONE in its own single-child batch, since a big table already
            // chunk-parallelizes internally and two at once would overload the
            // source. The per-child governor still bounds each one; this gate also
            // bounds the concurrent connection count.
            let (safe, lone): (Vec<&ExportConfig>, Vec<&ExportConfig>) =
                pending.iter().copied().partition(|e| is_parallel_safe(e));
            log::info!(
                "apply: wave {} — {} parallel-safe export(s) in parallel, {} run alone",
                label,
                safe.len(),
                lone.len()
            );
            // One single-child batch per lone export (run sequentially), then
            // one concurrent batch for all parallel-safe exports.
            let mut batches: Vec<Vec<&ExportConfig>> = lone.iter().map(|e| vec![*e]).collect();
            if !safe.is_empty() {
                batches.push(safe);
            }
            // Batches run one after another (the loop below blocks per batch),
            // so the run's concurrency is the WIDEST batch, never the number of
            // exports it covered.
            peak_concurrency =
                peak_concurrency.max(batches.iter().map(Vec::len).max().unwrap_or(0));
            // Wave-wide name floor so cards align across the safe/lone batches
            // (the cost gate splits a wave into one safe batch + N lone batches,
            // each its own renderer — without a shared floor they'd each pad to
            // their own widest name and the table would step).
            let wave_name_floor = pending
                .iter()
                .map(|e| e.name.chars().count())
                .max()
                .unwrap_or(0);
            for batch in &batches {
                let (result, cf, stderr_dump) = parallel_children::run_exports_as_child_processes(
                    config_path,
                    batch,
                    false,
                    false,
                    resume,
                    force,
                    None,
                    wave_name_floor,
                );
                child_failures.extend(cf);
                combined_stderr.push_str(&stderr_dump);
                if let Err(e) = result {
                    failures.push(e);
                }
            }
            all_exports.extend_from_slice(&pending);
        } else {
            log::info!(
                "apply: wave {} — {} export(s), sequential",
                label,
                pending.len()
            );
            for export in &pending {
                let (res, summary) =
                    job::run_export_job(config_path, &config, export, &state, &config_dir, &opts);
                if let Err(e) = res {
                    failures.push(e);
                }
                summaries.push(summary);
            }
        }
    }

    // Stamp the window BEFORE the bracket close queries the source, so the
    // aggregate's duration excludes the instrumentation round-trip.
    let finished_at = chrono::Utc::now();
    if let Some(bracket) = run_harm {
        // Frame the window by what the run ACTUALLY did — the cost gate can put
        // every export in its own single-child batch (the default, since
        // `parallel_safe` is `None` unless `rivet plan` set it), in which case
        // nothing overlapped and "N concurrent exports · lower the export
        // concurrency" would tell the operator to shrink a 1 (bughunt
        // 2026-08-14).
        if let Some(window) = wave_harm_window(peak_concurrency, all_exports.len()) {
            bracket.close_and_warn(window);
        }
    }

    // ONE aggregate for both paths; the CARD and the `run_aggregate` row stay
    // multi-export-only, but the run-over-run self-check fires whatever the
    // count — `apply` has no `--export` flag, so a one-export config IS the
    // whole invocation and skipping it there was the runner-bypass half of the
    // fix that only reached `run()` (bughunt 2026-08-14).
    let entries = if parallel {
        aggregate::collect_child_entries(&state, &all_exports, started_at, &child_failures)
    } else {
        summaries
            .iter()
            .map(aggregate::entry_from_summary)
            .collect()
    };
    let agg = aggregate::build(
        entries,
        started_at,
        finished_at,
        Some(config_path),
        if parallel {
            "wave-parallel-processes"
        } else {
            "wave-sequential"
        },
    );
    if total > 1 {
        aggregate::print(&agg);
    }
    self_check_throughput(&state, &agg.per_export, &agg.parallel_mode);
    if total > 1 {
        aggregate::persist(&state, &agg, None);
    }
    // Captured child stderr (verbose per-export cards, parallel path only) goes
    // to a file artifact beside the config, with a one-line console pointer.
    emit_child_stderr(&combined_stderr, &config_dir);

    if !failures.is_empty() {
        let primary_idx = representative_failure_idx(&failures).unwrap();
        let primary = failures.remove(primary_idx);
        if failures.is_empty() {
            return Err(primary);
        }
        let others = failures
            .iter()
            .map(|e| format!("{e:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(primary.context(format!(
            "{} export(s) failed across waves; representative error follows (also: {others})",
            failures.len() + 1
        )));
    }
    Ok(())
}

/// How the run shaped its concurrency, for the run-level harm line's frame —
/// the operator needs to know WHICH window the total covers before they can act
/// on it (shed slots vs shed export concurrency).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HarmWindow {
    /// `--pool m`: `exports` exports drained through `slots` slots.
    Pool { exports: usize, slots: usize },
    /// `--parallel-exports` / `--parallel-export-processes` / `apply --parallel`:
    /// `exports` exports concurrent with no slot cap. `exports` is the PEAK
    /// number that actually overlapped, not how many the run covered — the
    /// frame is a concurrency claim, so it must count concurrency.
    Parallel { exports: usize },
    /// A run that ASKED for parallelism and got none: `apply --parallel` whose
    /// cost gate put every (non-`parallel_safe`) export in its own single-child
    /// batch, which the wave loop then runs strictly one after another. Its own
    /// arm because the parallel arm's frame ("N concurrent exports") and lever
    /// ("lower the export concurrency") are both FALSE here — there was no
    /// concurrency to lower, and naming it buries the levers that do exist
    /// (bughunt 2026-08-14).
    Serial { exports: usize },
}

/// The window a wave run actually presented to the source.
///
/// `peak` is the widest batch that ever ran at once across every wave;
/// `exports` is how many ran in total. `apply --parallel` opens its bracket on
/// the FLAG, before any batching is known — the cost gate may then serialize the
/// whole run (the default: `parallel_safe` is `None` → not safe → one
/// single-child batch per export), and the children agree, since
/// `run_exports_as_child_processes` sets `ENV_CONCURRENT_SIBLINGS` only for a
/// batch of >1. So the honest frame is decided HERE, at close, from what
/// happened — never from the flag or from the export count.
///
/// `None` when the run executed nothing (every export resume-skipped): rivet's
/// window then contains none of rivet's work, so any spill in it is foreign and
/// blaming the run would be the same false accusation one step further out.
fn wave_harm_window(peak: usize, exports: usize) -> Option<HarmWindow> {
    if exports == 0 {
        return None;
    }
    Some(if peak > 1 {
        HarmWindow::Parallel { exports: peak }
    } else {
        HarmWindow::Serial { exports }
    })
}

/// The run-window harm verdict, pure so the threshold and wording are
/// unit-tested (its per-export sibling in `job::run_diagnosis` always was;
/// this copy had zero cover behind a live-only seam — walk find, 2026-08-13).
/// Shares [`job::SPILL_FLAG_MIN`] AND the [`job::spill_total`] fold with that
/// sibling so the two rules cannot drift — they already had, on the fold: both
/// matched `tmp_disk` only, so PG's `pg_temp_files` tripped neither.
fn run_harm_verdict(deltas: &[(String, i64)], window: HarmWindow) -> Option<String> {
    let job::Spill {
        total: spills,
        unit,
    } = job::spill_total(deltas);
    if spills < job::SPILL_FLAG_MIN {
        return None;
    }
    let (scope, frame, lever) = match window {
        HarmWindow::Pool { exports, slots } => (
            "pool run",
            format!("the pool window ({exports} exports, {slots} slots)"),
            Some("`--pool` slots"),
        ),
        HarmWindow::Parallel { exports } => (
            "parallel run",
            format!("the run window ({exports} concurrent exports)"),
            Some("the export concurrency (`--pool N` bounds it)"),
        ),
        // No concurrency lever: this window HAD no concurrency. Naming one
        // would send the operator to shrink a 1, and would push the two levers
        // that do work behind a lie.
        HarmWindow::Serial { exports } => (
            "serialized run",
            format!("the run window ({exports} exports, run one at a time)"),
            None,
        ),
    };
    let levers = match lever {
        Some(l) => format!("Lower {l}, `chunk_size` pages, or `tuning.batch_size`"),
        None => "Lower `chunk_size` pages or `tuning.batch_size`".to_string(),
    };
    Some(format!(
        "{scope}: source harm — {spills} {unit} server-wide across {frame}: the source \
         spilled to disk while rivet ran. {levers} to shed pressure (foreign clients can \
         contribute, but rivet's window is the frame)."
    ))
}

/// The run-level source-harm bracket: one `before` snapshot, one `after`
/// snapshot, one WARN verdict — shared by EVERY parent that runs exports
/// concurrently (the pool, `--parallel-exports`, `--parallel-export-processes`,
/// `apply --parallel`).
///
/// It is shared because the per-export DIAGNOSIS hedge POINTS at this line
/// ("the run-level harm line carries the whole-window total"): the hedge fires
/// wherever [`multi_export_concurrent`] is true, which since the
/// `ENV_CONCURRENT_SIBLINGS` marker includes the process-parallel and
/// `apply --parallel` children — paths that emitted no such line, so the
/// pointer dangled at a line the operator could never find. One bracket type
/// used by every parent keeps the pointer TRUE by construction rather than by
/// four copies staying in sync.
///
/// Best-effort throughout, like the per-export snapshots: a failed probe yields
/// no line, never an error.
pub(crate) struct RunHarmBracket<'a> {
    source: &'a crate::config::SourceConfig,
    before: Option<Vec<(String, i64)>>,
}

/// Probe FIRST, stamp SECOND — the open-side mirror of the `finished_at` rule.
///
/// `job::harm_snapshot` is a real source CONNECT plus a catalog query (TLS
/// handshake, MySQL pool build, an MSSQL runtime + login): on a tunnelled or
/// cold link it costs seconds. A window stamped BEFORE it charges the run for
/// rivet's own measurement — the pool then prints "actual makespan X vs
/// predicted Y" grading its model against the cost of grading it, and the
/// aggregate's rows/s (which `warn_throughput_regressions` compares run over
/// run) is deflated by the same amount. The close side was fixed by hand at
/// four sites; this side is fixed by CONSTRUCTION — [`RunHarmBracket::open`]
/// hands the caller the stamp, so no caller can order the probe inside the
/// window it grades.
///
/// Split out of `open` so the ordering is provable without a live source: the
/// probe is injected, and the test asserts the stamp is not older than the
/// instant the probe finished.
fn snapshot_then_stamp<T>(probe: impl FnOnce() -> T) -> (T, chrono::DateTime<chrono::Utc>) {
    let before = probe();
    (before, chrono::Utc::now())
}

impl<'a> RunHarmBracket<'a> {
    /// Take the `before` snapshot and return it with the window-START stamp,
    /// taken after the probe (see [`snapshot_then_stamp`]). Call immediately
    /// before the concurrent work and use the returned instant as the run's
    /// `started_at`.
    fn open(source: &'a crate::config::SourceConfig) -> (Self, chrono::DateTime<chrono::Utc>) {
        let (before, window_start) = snapshot_then_stamp(|| job::harm_snapshot(source));
        (Self { source, before }, window_start)
    }

    /// Take the `after` snapshot and WARN the verdict if the window crossed the
    /// shared threshold. WARN (not info) so it is visible at the default log
    /// level — an invisible "your source is spilling" line is no line at all.
    fn close_and_warn(self, window: HarmWindow) {
        if let (Some(before), Some(after)) = (&self.before, job::harm_snapshot(self.source))
            && let Some(line) = run_harm_verdict(&job::harm_deltas(before, &after), window)
        {
            log::warn!("{line}");
        }
    }
}

/// The pool's pick rule, pure for the mutation gate (#166): the first queued
/// export a freeing slot may start. A `parallel_safe` export is always
/// eligible; a non-safe (heavy) one only when NO other heavy export is
/// currently running — heavies serialize among THEMSELVES (a big table already
/// chunk-parallelizes internally; two at once overload the source) while cheap
/// exports backfill the remaining slots. That is exactly the field ask: the
/// giant runs, the small and medium ride alongside — but never two giants.
fn next_eligible(safe_flags: &[bool], heavy_running: bool) -> Option<usize> {
    safe_flags.iter().position(|&safe| safe || !heavy_running)
}

/// `rivet apply <config.yaml> --pool N` (#166): run the WHOLE config as one
/// bounded work-stealing pool of `m` slots. Exports start longest-first (LPT by
/// last measured duration; 5 s placeholder for history-less ones, which only
/// orders their first run) and every freeing slot pulls the next eligible —
/// no wave barriers, so the wall approaches `max(longest, total/m)`.
///
/// Deliberate semantics, stated once (see `pipeline::pool`):
/// - PRIORITY `wave:` tiers are NOT honored — the pool is makespan mode for a
///   full refresh with no inter-table ordering; ordered pipelines keep waves.
/// - non-`parallel_safe` exports never co-run with EACH OTHER (see
///   [`next_eligible`]); `--resume` skips `_SUCCESS`-complete exports and
///   resumes checkpoints, exactly like waves.
/// - a failed export is collected and the pool keeps draining (wave
///   semantics); the run exits non-zero with the representative error.
pub(crate) fn run_pool(
    config_path: &str,
    force: bool,
    resume: bool,
    m: usize,
    split: bool,
) -> Result<()> {
    let m = m.max(1);
    let config = Config::load_with_params(config_path, None)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let opts = RunOptions {
        validate: false,
        reconcile: false,
        resume,
        force,
        params: None,
    };
    // Pre-migrate the state DB once before worker threads race on DDL, and use
    // this handle for the duration reads below.
    let state = StateStore::open(config_path)?;

    // Skip-completed contract (probe the EXPANDED destination for _SUCCESS under
    // --resume). OWNED (`.cloned()`) so a `--split` can splice synthesized range
    // sub-exports into the set — they are fresh `ExportConfig`s with no
    // config.exports slot to borrow, and everything downstream (`by_name`,
    // `queue`) borrows from THIS vec.
    //
    // #167 per-unit resume: with `--split` the prefix-level _SUCCESS is AMBIGUOUS
    // (a split prefix's _SUCCESS is written per-unit, so it is present the moment
    // ONE unit finishes) — pre-skipping the giant on it would drop a partially-
    // complete split. So when `--split` is set the pre-skip is deferred to a
    // PER-UNIT skip AFTER the split (below); without `--split` the normal
    // prefix-level skip applies here unchanged.
    let mut effective: Vec<ExportConfig> = config
        .exports
        .iter()
        .filter(|e| {
            if split {
                return true; // per-unit skip happens after the split
            }
            let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&e.name);
            let expanded =
                crate::destination::placeholder::expand_destination(e.destination.clone(), &ctx);
            let done = resume && finalize::destination_has_success(&expanded);
            if done {
                log::info!(
                    "apply --pool: skipping '{}' — destination already complete (_SUCCESS)",
                    e.name
                );
            }
            !done
        })
        .cloned()
        .collect();
    if effective.is_empty() {
        log::warn!("apply --pool: nothing to run (no exports, or all complete)");
        return Ok(());
    }

    // The pre-split sweep keeps the (item, classification) PAIRS, not just the
    // items: a split unit inherits the giant's PROVENANCE as well as its
    // seconds (see `pool::split_unit_from`). Dropping the classification here
    // is what let `--split` re-label a giant that has never succeeded as
    // "measured" and delete the LOWER BOUND hedge (bughunt 2026-08-14).
    let predicted_pre: Vec<(super::pool::PoolItem, super::pool::PredictedFrom)> =
        super::pool::predict_items(
            &state,
            effective
                .iter()
                .map(|e| (e.name.as_str(), is_parallel_safe(e))),
            &std::collections::HashMap::new(),
        );
    let mut items: Vec<super::pool::PoolItem> =
        predicted_pre.iter().map(|(i, _)| i.clone()).collect();

    // #167: a single dominating export IS the pool floor — extra slots buy
    // nothing past it until the giant is itself divisible. The pure planner
    // ([`pool::advise_split`], shape-driven / M-free) decides WHETHER + into how
    // many; what happens next depends on the opt-in `--split`:
    //  * `--split` set → REALIZE it: probe the giant's key span and splice N range
    //    sub-exports into the set, so the pool places them concurrently and the
    //    floor breaks. An explicit opt-in, so a probe failure is LOUD (returned),
    //    never a silent fall-back to the un-split giant.
    //  * `--split` NOT set → the pre-existing ADVISORY warn (tells the operator
    //    the concrete floor-breaker; they act on it).
    // #167 per-unit resume: (destination, family) of the giant that was split, so
    // the post-split skip can read which of its units already completed.
    let mut split_info: Option<(crate::config::DestinationConfig, String)> = None;
    // #167 + bughunt 2026-08-13: a synthesized `{giant}#N` unit has no metrics
    // history, so predicted_from would give it the 5 s placeholder and LPT
    // would schedule the giant's slices LAST — smalls first, nothing
    // backfilling behind the units, defeating the split's makespan purpose.
    // Each unit inherits giant_predicted / realized (the same arithmetic
    // pool::split_dominating models) AND the giant's classification, recorded
    // here and consulted by the final classification sweep.
    let mut split_seeds: std::collections::HashMap<String, super::pool::PredictedFrom> =
        std::collections::HashMap::new();
    let advise = super::pool::advise_split(&items, m, 3.0, m.max(2));
    if split {
        match &advise {
            Some((giant, n, broken)) => {
                let base = effective
                    .iter()
                    .find(|e| &e.name == giant)
                    .expect("advise_split names an export in the set")
                    .clone();
                // On --resume, RECONSTRUCT the exact partition the prior run used from its
                // units' persisted windows — never re-sample (finding 2: sample_key_boundaries
                // is offset/percentile-based, so a source that grew between crash and resume
                // yields different boundaries, and the name-based skip below then covers a
                // different key range than was exported → silent gap). Re-probe only when there
                // is no prior split in the prefix (a genuine first run).
                let units_opt = match resume
                    .then(|| {
                        super::split::reconstruct_units_from_prefix(
                            &base.destination,
                            &base.family(),
                            &base,
                        )
                    })
                    .flatten()
                {
                    Some(u) => Some(u),
                    None => super::split::probe_and_synthesize(&config, &base, &config_dir, *n)?,
                };
                match units_opt {
                    Some(units) => {
                        let realized = units.len();
                        // Seed each unit with its share of the giant's
                        // prediction so LPT places the slices where the giant
                        // stood (front of the queue), not at the 5 s
                        // placeholder tail — and with the giant's CLASSIFICATION,
                        // so a giant that has never succeeded does not turn into
                        // N "measured" units and silently delete the LOWER BOUND
                        // hedge below (bughunt 2026-08-14).
                        let (giant_secs, giant_from) = predicted_pre
                            .iter()
                            .find(|(i, _)| &i.name == giant)
                            .map(|(i, f)| (i.predicted_secs, Some(f.clone())))
                            .unwrap_or((0.0, None));
                        let share = giant_secs / realized.max(1) as f64;
                        let unit_from = match &giant_from {
                            Some(f) => super::pool::split_unit_from(f, share),
                            None => super::pool::PredictedFrom::SeededSplit(share),
                        };
                        for u in &units {
                            split_seeds.insert(u.name.clone(), unit_from.clone());
                        }
                        split_info = Some((base.destination.clone(), base.family()));
                        effective.retain(|e| &e.name != giant);
                        // A synthesized unit is named `{giant}#i`; if a user export already carries
                        // that exact name, the `by_name` HashMap below collapses the two and
                        // silently DROPS the pre-existing export's whole table (convergence round-2
                        // LOW — `#` is not reserved in export-name validation). Refuse loudly.
                        if let Some(clash) = first_name_collision(&units, &effective) {
                            anyhow::bail!(
                                "apply --pool --split: the synthesized split unit '{clash}' collides \
                                 with an existing export of the same name. Rename that export — a \
                                 name of the form '{giant}#<n>' is reserved for split unit names."
                            );
                        }
                        effective.extend(units);
                        // `items` is rebuilt by the single post-split
                        // classification sweep below.
                        // The wall is only as good as the giant's prediction:
                        // an unmeasured giant makes it a LOWER BOUND, and this
                        // line is the one an operator reads while the run
                        // starts (the accounting print below repeats it).
                        let wall_hedge = match &unit_from {
                            super::pool::PredictedFrom::SeededSplit(_) => "",
                            _ => {
                                " This wall is a LOWER BOUND: the giant has no successful run to \
                                  measure from, so each unit is seeded from a failed attempt / \
                                  placeholder."
                            }
                        };
                        log::warn!(
                            "apply --pool --split: split '{giant}' into {realized} range \
                             sub-export(s) over its key — predicted wall ~{:.1} min (was the \
                             single-export floor). The units share one prefix and fold to family \
                             '{giant}', so the load view reads them as one table.{wall_hedge}",
                            broken / 60.0,
                        );
                    }
                    None => log::warn!(
                        "apply --pool --split: '{giant}' dominates the floor but is not splittable \
                         (needs a `chunk_by_key:`/`chunk_column:`, and not incremental/CDC) — \
                         running it whole."
                    ),
                }
            }
            None => {
                // advise_split returns None for a heavy dominator too, not only a balanced set.
                // The most actionable case is a giant that DOMINATES the floor but is HEAVY (not
                // parallel_safe): its range sub-units inherit the heavy flag and still serialize
                // under the C3 floor, so a split cannot lower the wall — the fix is to mark it
                // parallel_safe (independent rows), not to add a chunk key. Say so at WARN
                // (visible), instead of the misattributed "nothing dominates" at INFO. A
                // genuinely balanced set stays informational.
                let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
                match items
                    .iter()
                    .max_by(|a, b| a.predicted_secs.total_cmp(&b.predicted_secs))
                    .filter(|l| l.predicted_secs > total / (m.max(1) as f64) && !l.parallel_safe)
                {
                    Some(l) => log::warn!(
                        "apply --pool --split: '{}' dominates the pool floor but is HEAVY (not \
                         parallel_safe) — its split units would inherit the heavy flag and still \
                         serialize (the C3 floor), so splitting cannot lower the wall. Mark it \
                         `parallel_safe: true` if its rows are independent; running it whole.",
                        l.name
                    ),
                    None => log::info!(
                        "apply --pool --split: no export dominates the pool floor — nothing to \
                         split."
                    ),
                }
            }
        }
    } else if let Some((giant, n, broken)) = &advise {
        let giant_secs = items
            .iter()
            .find(|i| &i.name == giant)
            .map(|i| i.predicted_secs)
            .unwrap_or(0.0);
        log::warn!(
            "apply --pool: export '{}' (~{:.1} min) dominates the pool floor — extra slots cannot \
             beat it. Re-run with `--split` to split it into {} range sub-exports over its key \
             (separate scheduler units, one shared prefix) and drop the wall to ~{:.1} min.",
            giant,
            giant_secs / 60.0,
            n,
            broken / 60.0,
        );
    }

    // #167 per-unit resume: with `--split --resume`, skip PER UNIT (the pre-skip
    // was deferred above because a split prefix's _SUCCESS is ambiguous). A split
    // unit whose Success manifest copy is already in the shared prefix is done —
    // drop it; the rest re-run (a crashed unit resumes its own checkpoint, a
    // never-started one runs fresh — the per-unit resume flag is set at the call
    // site below). Non-split exports still skip on their own prefix _SUCCESS.
    if split && resume {
        let completed_units = split_info
            .as_ref()
            .map(|(dest, family)| super::split::completed_units_in_prefix(dest, family))
            .unwrap_or_default();
        effective.retain(|e| match &e.split {
            Some(_) => {
                let done = completed_units.contains(&e.name);
                if done {
                    log::info!(
                        "apply --pool --split: skipping unit '{}' — already complete (its \
                         manifest copy is present)",
                        e.name
                    );
                }
                !done
            }
            None => {
                let ctx = crate::destination::placeholder::PlaceholderContext::for_today(&e.name);
                let expanded = crate::destination::placeholder::expand_destination(
                    e.destination.clone(),
                    &ctx,
                );
                let done = finalize::destination_has_success(&expanded);
                if done {
                    log::info!(
                        "apply --pool: skipping '{}' — destination already complete (_SUCCESS)",
                        e.name
                    );
                }
                !done
            }
        });
        if effective.is_empty() {
            log::warn!("apply --pool: nothing to run (no exports, or all complete)");
            return Ok(());
        }
    }

    // LPT order + the makespan the model PREDICTS (POST-split) — printed up front
    // and graded against the actual wall at the end, so every run improves trust
    // in (or honestly indicts) the model.
    //
    // ONE classification sweep feeds BOTH the schedule (items) and the
    // measured/estimated accounting below — a second predicted_from sweep
    // would re-query the state store per export and could describe a
    // different schedule than the one that runs (walk find, 2026-08-13).
    let predicted = super::pool::predict_items(
        &state,
        effective
            .iter()
            .map(|e| (e.name.as_str(), is_parallel_safe(e))),
        &split_seeds,
    );
    let classified: Vec<super::pool::PredictedFrom> =
        predicted.iter().map(|(_, f)| f.clone()).collect();
    items = predicted.into_iter().map(|(i, _)| i).collect();
    let order = super::pool::pool_order(&items);
    let predicted_secs = super::pool::predicted_makespan_secs(&items, m);
    let floor_secs = super::pool::makespan_floor_secs(&items, m);
    let (measured_n, attempt_n, placeholder_n) = super::pool::classification_counts(&classified);
    println!(
        "  Pool: {} export(s) × {} slot(s) — predicted makespan ~{:.1} min (floor {:.1}; {} measured, {} estimated)",
        effective.len(),
        m,
        predicted_secs / 60.0,
        floor_secs / 60.0,
        measured_n,
        attempt_n + placeholder_n,
    );
    // An unmeasured export is scheduled at a placeholder (or a failed
    // attempt's floor) — the prediction cannot be better than its inputs, so
    // say so up front instead of letting "~12 min" stand for a run that a
    // single unmeasured giant can stretch to hours.
    if attempt_n + placeholder_n > 0 {
        println!(
            "        prediction is a LOWER BOUND: {} export(s) have no successful run to \
             measure from ({} scheduled at a failed attempt's duration, {} at a {}s \
             placeholder) — it tightens as runs complete",
            attempt_n + placeholder_n,
            attempt_n,
            placeholder_n,
            super::pool::POOL_PLACEHOLDER_SECS as i64,
        );
    }

    let pending: Vec<&ExportConfig> = effective.iter().collect();
    let by_name: std::collections::HashMap<&str, &ExportConfig> =
        pending.iter().map(|e| (e.name.as_str(), *e)).collect();
    let queue: std::sync::Mutex<std::collections::VecDeque<&ExportConfig>> = std::sync::Mutex::new(
        order
            .iter()
            .filter_map(|n| by_name.get(n.as_str()).copied())
            .collect(),
    );
    // Mutated ONLY while holding `queue` (claim) or after a heavy export
    // finishes (release) — claim-side check+set is serialized by the queue
    // lock, so two slots can never claim two heavies together.
    let heavy_running = std::sync::atomic::AtomicBool::new(false);

    // The same in-process ChildEvent UI the threads path uses: one card per
    // export, a single UI thread owning stderr.
    let name_floor = pending
        .iter()
        .map(|e| e.name.chars().count())
        .max()
        .unwrap_or(0);
    let prev_multi = MULTI_EXPORT_MODE.swap(false, AtomicOrdering::Relaxed);
    // The pool runs up to `m` exports on concurrent in-process threads:
    // declare that, so (a) per-export indicatif chunk bars stay suppressed
    // (concurrent threads corrupt each other's terminal writes — same reason
    // as `--parallel-exports`) and (b) `run_diagnosis` knows the server-global
    // harm counters overlapped sibling exports' windows and hedges its
    // attribution instead of blaming the one export it prints beside
    // (field find, 2026-08-13). Restored via a Drop guard — the `run()` path's
    // discipline — so a panic escaping `thread::scope` cannot leak
    // CONCURRENT=true into the rest of the process.
    // Only claim concurrency when the pool can actually overlap work — a
    // `--pool 1` (or a single pending export) runs strictly serially, and the
    // hedge text run_diagnosis emits for concurrent siblings would be a false
    // claim there (bughunt 2026-08-13).
    let really_concurrent = m.min(pending.len()) > 1;
    let prev_concurrent = MULTI_EXPORT_CONCURRENT.swap(really_concurrent, AtomicOrdering::Relaxed);
    struct ResetPoolStatics(bool, bool);
    impl Drop for ResetPoolStatics {
        fn drop(&mut self) {
            MULTI_EXPORT_MODE.store(self.0, AtomicOrdering::Relaxed);
            MULTI_EXPORT_CONCURRENT.store(self.1, AtomicOrdering::Relaxed);
        }
    }
    let _reset_pool_statics = ResetPoolStatics(prev_multi, prev_concurrent);
    // The ipc sender gets the same panic-safety: a worker panic re-raised by
    // `thread::scope` would otherwise skip the straight-line clear below and
    // leak a stale global Sender for the rest of the process (bughunt
    // 2026-08-13). clear is idempotent, so the guard + the normal-path clear
    // coexist harmlessly.
    struct ClearIpcTx;
    impl Drop for ClearIpcTx {
        fn drop(&mut self) {
            ipc::clear_in_process_tx();
        }
    }
    let _clear_ipc = ClearIpcTx;
    let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
    ipc::install_in_process_tx(tx);
    let n_cards = pending.len();
    let ui_thread = std::thread::Builder::new()
        .name("rivet-ui".to_string())
        .spawn(move || parent_ui::run_ui(rx, name_floor, n_cards))
        .ok();

    // Run-level source-harm bracket: the per-export deltas overlap in time
    // under pool concurrency (each reads the same server-global counters over
    // its own window), so summing them double-counts and blaming one export
    // mis-attributes. The POOL window is the honest scope: rivet's slots are
    // the dominant load in it, so `after - before` here is "what this run did
    // to the source" (modulo foreign clients). Best-effort, like the
    // per-export snapshots.
    //
    // The OPEN comes first and hands back `started_at`: this probe is a source
    // connect + catalog query, and the very next lines grade the schedule with
    // "actual makespan X vs predicted Y". Stamping before it charged the model
    // for the cost of measuring it — the unfixed mirror of the `finished_at`
    // move below (bughunt 2026-08-14).
    let (run_harm, started_at) = RunHarmBracket::open(&config.source);
    let collected: std::sync::Mutex<Vec<(Result<()>, RunSummary)>> =
        std::sync::Mutex::new(Vec::with_capacity(pending.len()));
    std::thread::scope(|s| {
        for _ in 0..m.min(pending.len()) {
            s.spawn(|| {
                loop {
                    // Claim under the queue lock (heavy check+set serialized).
                    let export = {
                        let mut q = queue.lock().unwrap();
                        if q.is_empty() {
                            break;
                        }
                        let flags: Vec<bool> = q.iter().map(|e| is_parallel_safe(e)).collect();
                        match next_eligible(&flags, heavy_running.load(AtomicOrdering::SeqCst)) {
                            Some(i) => {
                                let e = q.remove(i).unwrap();
                                if !is_parallel_safe(e) {
                                    heavy_running.store(true, AtomicOrdering::SeqCst);
                                }
                                e
                            }
                            None => {
                                // Only heavies left while one runs: wait for it.
                                drop(q);
                                std::thread::sleep(std::time::Duration::from_millis(200));
                                continue;
                            }
                        }
                    };
                    // Release the heavy slot on EVERY exit path — including a
                    // PANIC inside run_export_job. Without the guard a panicking
                    // heavy leaked heavy_running=true and every other worker
                    // looped forever on next_eligible (only heavies left, one
                    // "running") — the pool hung instead of failing (roast
                    // 2026-08-09). The guard drops on normal end AND on unwind.
                    struct HeavyGuard<'a>(&'a std::sync::atomic::AtomicBool, bool);
                    impl Drop for HeavyGuard<'_> {
                        fn drop(&mut self) {
                            if self.1 {
                                self.0.store(false, AtomicOrdering::SeqCst);
                            }
                        }
                    }
                    let _heavy = HeavyGuard(&heavy_running, !is_parallel_safe(export));
                    let pair = match StateStore::open(config_path) {
                        Ok(st) => {
                            // #167 per-unit resume: a split unit resumes ONLY if it
                            // has a checkpoint to resume (a crashed unit) — else it
                            // runs fresh, so `--resume` never bails on a
                            // never-started unit's "no in-progress checkpoint".
                            // Non-split exports keep the run-wide resume flag.
                            let mut unit_opts = opts;
                            if export.split.is_some() {
                                unit_opts.resume = resume
                                    && st.has_resumable_checkpoint(&export.name).unwrap_or(false);
                            }
                            job::run_export_job(
                                config_path,
                                &config,
                                export,
                                &st,
                                &config_dir,
                                &unit_opts,
                            )
                        }
                        Err(e) => {
                            let err = anyhow::anyhow!(
                                "export '{}': failed to open state database: {:#}",
                                export.name,
                                e
                            );
                            let summary = job::synthetic_failed_summary(&export.name, &err);
                            (Err(err), summary)
                        }
                    };
                    collected.lock().unwrap().push(pair);
                }
            });
        }
    });
    // The measured makespan must close at the END OF THE WORK, not at the end
    // of the instrumentation that grades it. Closing the harm bracket below
    // OPENS a source connection and queries the server's counters; stamping
    // `finished_at` after it folded that round-trip into the number printed as
    // "actual makespan X vs predicted Y" and into the aggregate's window — the
    // model would grade itself against its own measurement cost (bughunt
    // 2026-08-13). Taken here, right after the export loop drains.
    let finished_at = chrono::Utc::now();
    ipc::clear_in_process_tx();
    if let Some(h) = ui_thread {
        let _ = h.join();
    }
    // The run-level harm verdict the per-export DIAGNOSIS lines point at:
    // spills during the pool window are REAL harm to the source (disk-spilling
    // tmp tables, PG temp files) whoever triggered them — WARN so it is visible
    // at the default log level, with the levers that shrink the pressure.
    run_harm.close_and_warn(HarmWindow::Pool {
        exports: effective.len(),
        slots: m,
    });

    let actual_secs = (finished_at - started_at).num_milliseconds() as f64 / 1000.0;
    println!(
        "  Pool: actual makespan {:.1} min vs predicted {:.1} min ({:+.0}%) — the model grades itself every run",
        actual_secs / 60.0,
        predicted_secs / 60.0,
        if predicted_secs > 0.0 {
            (actual_secs - predicted_secs) / predicted_secs * 100.0
        } else {
            0.0
        },
    );

    let mut summaries: Vec<RunSummary> = Vec::new();
    let mut failures: Vec<anyhow::Error> = Vec::new();
    // #167: track whether every `--split` UNIT of the giant succeeded this run —
    // the pool is the single writer of the prefix `_SUCCESS` (units suppress it),
    // so the marker goes down only once the whole giant is complete.
    let unit_prefix = split_info.as_ref().map(|(_, family)| format!("{family}#"));
    let mut split_units_all_ok = true;
    for (res, summary) in collected.into_inner().unwrap() {
        if let Some(pfx) = &unit_prefix
            && summary.export_name.starts_with(pfx.as_str())
            && (res.is_err() || summary.status != "success")
        {
            split_units_all_ok = false;
        }
        if let Err(e) = res {
            failures.push(e);
        }
        summaries.push(summary);
    }
    // Every split unit succeeded → write the ONE prefix-level `_SUCCESS` the units
    // deliberately suppressed (finalize wrote each unit's manifest + run-unique
    // copy; this is the marker that says the whole giant is done). Best-effort: a
    // missing marker only affects the resume-skip fast path, never data integrity.
    if let Some((dest_config, family)) = &split_info
        && split_units_all_ok
    {
        let ctx = crate::destination::placeholder::PlaceholderContext::for_today(family);
        let expanded =
            crate::destination::placeholder::expand_destination(dest_config.clone(), &ctx);
        if let Err(e) = finalize::write_split_success_marker(&expanded) {
            log::warn!(
                "apply --pool --split: could not write the prefix _SUCCESS for '{family}': {e:#}"
            );
        }
    }
    // ONE aggregate, then the same routing every other orchestrator uses: the
    // card and the `run_aggregate` row are multi-export-only, the run-over-run
    // self-check is not. A `--pool` run whose `--resume` skip leaves ONE pending
    // export is the shape that silently dropped the check before (bughunt
    // 2026-08-14) — and it is the shape a degraded export produces, since the
    // others completed.
    let entries = summaries
        .iter()
        .map(aggregate::entry_from_summary)
        .collect();
    let agg = aggregate::build(entries, started_at, finished_at, Some(config_path), "pool");
    if pending.len() > 1 {
        aggregate::print(&agg);
    }
    self_check_throughput(&state, &agg.per_export, &agg.parallel_mode);
    if pending.len() > 1 {
        aggregate::persist(&state, &agg, None);
    }
    if !failures.is_empty() {
        let primary_idx = representative_failure_idx(&failures).unwrap();
        let primary = failures.remove(primary_idx);
        if failures.is_empty() {
            return Err(primary);
        }
        let others = failures
            .iter()
            .map(|e| format!("{e:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(primary.context(format!(
            "{} export(s) failed in the pool; representative error follows (also: {others})",
            failures.len() + 1
        )));
    }
    Ok(())
}

/// Group exports by `wave:` in ascending order; an export with no `wave:` runs
/// last (sorted as `u32::MAX`). Pure + unit-tested — the ordering is the
/// contract `apply` depends on, so it does not hide inside [`run_waves`].
fn group_exports_by_wave(exports: &[ExportConfig]) -> Vec<(u32, Vec<&ExportConfig>)> {
    let mut by_wave: std::collections::BTreeMap<u32, Vec<&ExportConfig>> =
        std::collections::BTreeMap::new();
    for e in exports {
        by_wave
            .entry(e.wave.unwrap_or(u32::MAX))
            .or_default()
            .push(e);
    }
    by_wave.into_iter().collect()
}

/// Whether an export may run concurrently with its wave-mates: the
/// `parallel_safe` flag that `rivet plan` records from the source-aware cost
/// class (true only for cheap, `Low`-cost tables — see
/// [`ExportConfig::parallel_safe`]). A heavy table already chunk-parallelizes
/// internally, so it runs ALONE within its wave; only the cheap exports share a
/// concurrent batch. `None` (un-planned / hand-written) is treated as not-safe.
fn is_parallel_safe(export: &ExportConfig) -> bool {
    export.parallel_safe.unwrap_or(false)
}

/// The first synthesized split-unit name (`{giant}#i`) that collides with an EXISTING export's
/// name. `--pool --split` splices the units into the export set, and the downstream `by_name`
/// HashMap collapses same-named entries — so a user export literally named `{giant}#0` would be
/// silently dropped (its whole table lost). `Some(name)` here → the caller refuses loudly.
fn first_name_collision<'a>(
    units: &'a [ExportConfig],
    existing: &[ExportConfig],
) -> Option<&'a str> {
    units
        .iter()
        .find(|u| existing.iter().any(|e| e.name == u.name))
        .map(|u| u.name.as_str())
}

#[cfg(test)]
mod pool_harm_tests {
    use super::{HarmWindow, run_harm_verdict};

    fn pool() -> HarmWindow {
        HarmWindow::Pool {
            exports: 154,
            slots: 5,
        }
    }

    /// The pool-window verdict was the untested twin of run_diagnosis's spill
    /// flag (same threshold, same filter, zero cover). Pinned here: fires at
    /// the shared threshold on tmp_disk counters only, silent below it.
    /// RED against a `<`→`<=`/threshold mutant or a dropped filter.
    #[test]
    fn pool_harm_verdict_fires_at_threshold_on_tmp_disk_only() {
        let at = vec![("mysql_created_tmp_disk_tables".to_string(), 100_i64)];
        let line = run_harm_verdict(&at, pool()).expect("threshold reached");
        assert!(line.contains("100 tmp-disk spills") && line.contains("154 exports"));
        let below = vec![("mysql_created_tmp_disk_tables".to_string(), 99_i64)];
        assert!(run_harm_verdict(&below, pool()).is_none());
        // Non-spill counters never trip it, however large.
        let other = vec![("mysql_innodb_rows_read".to_string(), 1_000_000_i64)];
        assert!(run_harm_verdict(&other, pool()).is_none());
    }

    /// The run-level verdict must see EVERY engine's spill counter, not just
    /// MySQL's. Both this fold and `run_diagnosis`'s used to match `tmp_disk`
    /// only, so on PostgreSQL — whose harm set carries `pg_temp_files`, the
    /// direct spill analogue — the run-level line was silently never emitted.
    /// The other PG harm counters (scan/cache/deadlock) must stay silent, and
    /// the wording must name what was COUNTED: temp FILES, not tmp-disk tables.
    /// RED against restoring the `tmp_disk`-only filter (`is_none()` on the PG
    /// case) or against reusing MySQL's noun for it.
    #[test]
    fn run_harm_verdict_sees_postgres_temp_files_with_its_own_unit() {
        // >= 2 counters so the fold is a real fold, not a passthrough.
        let pg = vec![
            ("pg_temp_files".to_string(), 60_i64),
            ("pg_temp_files_replica".to_string(), 60_i64),
        ];
        let line = run_harm_verdict(&pg, pool()).expect("120 temp files must reach the threshold");
        assert!(
            line.contains("120 temp-file spills"),
            "PG's count must be summed and named as temp FILES: {line}"
        );
        assert!(
            !line.contains("tmp-disk"),
            "must not print MySQL's unit for a temp_files count: {line}"
        );
        // PG's non-spill counters never trip it, however large.
        let noise = vec![
            ("pg_tup_returned".to_string(), 9_000_000_i64),
            ("pg_blks_read".to_string(), 5_000_000_i64),
            ("pg_deadlocks".to_string(), 7_i64),
        ];
        assert!(run_harm_verdict(&noise, pool()).is_none());
    }

    /// The line the per-export DIAGNOSIS hedge points at is now emitted by the
    /// PARALLEL parents too, so its frame must describe THAT window (concurrent
    /// exports, no slot cap) and name a lever that path actually has — printing
    /// "pool window / lower --pool slots" to a `--parallel-export-processes` run
    /// is the same dangling pointer one layer down. RED against collapsing the
    /// two windows into one wording.
    #[test]
    fn run_harm_verdict_frames_the_parallel_window_in_its_own_terms() {
        let spills = vec![("mysql_created_tmp_disk_tables".to_string(), 300_i64)];
        let par =
            run_harm_verdict(&spills, HarmWindow::Parallel { exports: 12 }).expect("300 >= 100");
        assert!(
            par.contains("12 concurrent exports") && !par.contains("slots"),
            "parallel frame must not claim pool slots: {par}"
        );
        assert!(
            par.contains("export concurrency"),
            "parallel lever must be the concurrency, not `--pool` slots alone: {par}"
        );
        let pooled = run_harm_verdict(&spills, pool()).expect("300 >= 100");
        assert!(
            pooled.contains("5 slots") && pooled.contains("`--pool` slots"),
            "pool frame keeps its slot lever: {pooled}"
        );
    }

    /// The per-export DIAGNOSIS hedge points at "the run-level harm line", and
    /// that line exists only where a parent BRACKETS the window. Before this
    /// fix only `run_pool` did, while the hedge fires on every concurrent path
    /// (the `ENV_CONCURRENT_SIBLINGS` marker covers the process-parallel and
    /// `apply --parallel` children) — an operator-facing pointer at a line that
    /// was never printed.
    ///
    /// HONESTY: this pins the WIRING, not the emission. The emission needs a
    /// live source (both snapshots come from a real server), so no unit test can
    /// observe the log line; the live proof is a concurrent run against a
    /// spilling source. What this DOES catch, and goes RED on, is a parent
    /// losing its bracket — deleting any one `close_and_warn` call site fails
    /// it, which is exactly how the pointer dangled in the first place.
    #[test]
    fn every_concurrent_export_parent_brackets_the_run_harm_window() {
        let whole = include_str!("run.rs");
        // Analyse the PRODUCT half only — the test modules below would otherwise
        // satisfy the last slice's check with this test's own text.
        let src = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        // Spelled in pieces so this needle does not match the assertion that
        // uses it (the same self-match that made the first draft read 6 of 4).
        let open = concat!("RunHarmBracket::", "open(&config.source)");
        // The needle stops at the paren: `run_waves` COMPUTES its window
        // (`wave_harm_window`, since only the close knows what actually
        // overlapped) rather than spelling a `HarmWindow::` literal inline, and
        // requiring the literal would push the next parent back to a hard-coded
        // frame — which is the bug this file just fixed. It keeps the leading
        // DOT, so the method's own definition (which sits between two anchors
        // and would otherwise answer for the parent whose slice it lands in)
        // does not satisfy the check.
        let close = concat!(".close_and", "_warn(");
        // Slice each concurrent parent's body by its signature anchor; the last
        // one runs to the end of the product half.
        let anchors = [
            ("run", "\npub fn run("),
            ("run_waves", "\npub(crate) fn run_waves("),
            ("run_pool", "\npub(crate) fn run_pool("),
        ];
        let mut starts: Vec<(&str, usize)> = anchors
            .iter()
            .map(|(name, sig)| {
                (
                    *name,
                    src.find(sig)
                        .unwrap_or_else(|| panic!("{name}'s signature moved — update the anchor")),
                )
            })
            .collect();
        starts.sort_by_key(|(_, at)| *at);
        for (i, (name, at)) in starts.iter().enumerate() {
            let end = starts.get(i + 1).map(|(_, n)| *n).unwrap_or(src.len());
            let body = &src[*at..end];
            assert!(
                body.contains(open) && body.contains(close),
                "{name} runs exports concurrently, so it must bracket the run-level \
                 harm window — the per-export DIAGNOSIS hedge points at the line it emits"
            );
        }
        // `run` brackets BOTH of its concurrent paths (child processes AND
        // in-process threads); a single site would leave one path pointing at a
        // line it never prints. Fold ≥ 2 by construction, so count the sites.
        assert_eq!(
            src.matches(open).count(),
            4,
            "expected one bracket per concurrent path: run/processes, run/threads, \
             run_waves/parallel, run_pool"
        );
    }

    /// A wave run's harm frame must describe what HAPPENED, not what was asked
    /// for. `apply --parallel` opens its bracket on the flag, but the cost gate
    /// then puts every non-`parallel_safe` export (the default, absent a `rivet
    /// plan` annotation) in its own single-child batch, and the wave loop runs
    /// batches strictly one after another — so a 12-export run can have a peak
    /// concurrency of 1 while the shipped line said "12 concurrent exports" and
    /// told the operator to lower a concurrency that never existed.
    ///
    /// RED against the shipped `HarmWindow::Parallel { exports: all_exports.len() }`:
    /// the peak-1 case then reads `Parallel { exports: 12 }`.
    #[test]
    fn wave_harm_window_is_framed_by_the_peak_that_actually_overlapped() {
        use super::wave_harm_window;
        // Every export ran alone: 12 of them, none concurrent.
        assert_eq!(
            wave_harm_window(1, 12),
            Some(HarmWindow::Serial { exports: 12 }),
            "a serialized run must not be framed as a concurrent one"
        );
        // A mixed wave: the widest batch was 3 (the parallel_safe group), even
        // though 12 exports ran across the run — the frame counts CONCURRENCY.
        assert_eq!(
            wave_harm_window(3, 12),
            Some(HarmWindow::Parallel { exports: 3 }),
            "the frame must report the peak width, not the export count"
        );
        // Nothing ran (every export resume-skipped): rivet's window holds none
        // of rivet's work, so there is no run to blame for a spill in it.
        assert_eq!(wave_harm_window(0, 0), None);
    }

    /// The serialized frame must also drop the LEVER — a remedy list that opens
    /// with "lower the export concurrency" on a run that had none buries the two
    /// levers that do work behind a false one (the same diagnostic-bypass harm a
    /// false UNSAFE does in preflight).
    ///
    /// RED against reusing the `Parallel` arm for a serialized run, and against
    /// a shared lever sentence that keeps the concurrency clause.
    #[test]
    fn run_harm_verdict_for_a_serialized_run_names_no_concurrency_lever() {
        // ≥2 counters so the fold is a real fold.
        let spills = vec![
            ("Created_tmp_disk_tables".to_string(), 40_i64),
            ("mysql_created_tmp_disk_tables".to_string(), 60_i64),
        ];
        let line = run_harm_verdict(&spills, HarmWindow::Serial { exports: 12 })
            .expect("100 reaches the shared threshold");
        assert!(
            line.contains("12 exports, run one at a time"),
            "the frame must say the exports did not overlap: {line}"
        );
        assert!(
            !line.contains("concurrent"),
            "a serialized run must claim no concurrency: {line}"
        );
        assert!(
            !line.contains("--pool") && !line.contains("export concurrency"),
            "there is no concurrency to lower on this path — naming the lever \
             sends the operator to shrink a 1: {line}"
        );
        assert!(
            line.contains("`chunk_size`") && line.contains("`tuning.batch_size`"),
            "the levers this window DOES have must still be named: {line}"
        );
    }
}

#[cfg(test)]
mod run_tail_tests {
    use super::{owns_throughput_self_check, snapshot_then_stamp, tail_plan};

    /// `--json` / `--summary-output` must emit a document on EVERY run, including
    /// one that exports nothing: a `partition_by` export over a currently-empty
    /// table expands to zero children (`partition_expand::expand_one` logs
    /// "found no rows — nothing to export" and pushes none), and the shipped
    /// `exports.len() == 1` predicate matched neither that nor the `> 1`
    /// aggregate branch — so stdout was EMPTY and the summary file was never
    /// created, on a run that exited 0. A scheduler's `json.load(stdout)` raises
    /// a parse error; an empty-but-valid `total_exports: 0` document does not.
    ///
    /// Pure because the value-level oracle needs a live source (a real `run()`
    /// wants a database, a destination and a state DB); the aggregate this feeds
    /// is proven serializable at zero exports by
    /// `aggregate::a_zero_export_run_still_builds_a_valid_document`.
    ///
    /// RED against restoring `n_exports == 1`: the zero case reads `false`.
    #[test]
    fn tail_plan_emits_a_machine_document_for_a_zero_export_run() {
        // Zero exports (partition_by over an empty table) — the regression.
        assert!(
            tail_plan(0, true).machine_output,
            "a zero-export run must still write --summary-output / print --json"
        );
        assert!(
            !tail_plan(0, true).aggregate,
            "no card, no run_aggregate row"
        );
        // One export: unchanged — machine output from the tail, no card.
        assert!(tail_plan(1, true).machine_output);
        assert!(!tail_plan(1, true).aggregate);
        // Two or more: the aggregate path owns both the card and the file
        // (through `persist`), so the tail must NOT write it a second time.
        assert!(tail_plan(2, true).aggregate);
        assert!(
            !tail_plan(2, true).machine_output,
            "the aggregate path writes --summary-output via persist; a second \
             write here would race it"
        );
        // Nobody asked for machine output → none, at every count.
        assert!(!tail_plan(0, false).machine_output);
        assert!(!tail_plan(1, false).machine_output);
    }

    /// Exactly once per export per run — the half that is easy to get backwards.
    /// `--parallel-export-processes` and `apply --parallel` re-exec each export
    /// as `rivet run --export X`, so parent AND child both reach a tail over the
    /// same export, and it does not self-cancel: the parent rebuilds the child's
    /// entry carrying the CHILD's `run_id`, so its baseline query excludes the
    /// same row the child excluded and reproduces the child's line verbatim.
    ///
    /// The parent wins because a child's stderr is CAPTURED to
    /// `rivet-child-stderr-*.log` — a WARN emitted there never reaches the
    /// operator's console at all.
    ///
    /// RED against dropping the deferral (both cases read `true`) or inverting
    /// it (the parent then goes silent and NOBODY reports).
    #[test]
    fn a_reexecd_child_defers_the_throughput_self_check_to_its_parent() {
        assert!(
            !owns_throughput_self_check(true),
            "a re-exec'd child must defer — its parent aggregates the same row, \
             and the child's stderr is captured to a file"
        );
        assert!(
            owns_throughput_self_check(false),
            "a top-level run owns its own self-check — deferring it to a parent \
             that does not exist is how the check goes silent everywhere"
        );
    }

    /// The window is stamped AFTER the source probe, never before.
    ///
    /// `RunHarmBracket::open` runs `job::harm_snapshot` — a real connect (TLS
    /// handshake / pool build / MSSQL login) plus a catalog query, seconds over a
    /// tunnel. A `started_at` taken before it lands the instrumentation INSIDE
    /// the window that grades the run: the pool prints "actual makespan X vs
    /// predicted Y" and the aggregate's rows/s (the input to the run-over-run
    /// self-check) is deflated by the probe's own cost. The close side of the
    /// same rule was fixed at four sites by hand; this side is structural —
    /// `open` returns the stamp, so a caller cannot order it wrong.
    ///
    /// RED against the reversed body (`let at = Utc::now(); (probe(), at)`): the
    /// stamp then predates the probe's completion by the sleep below.
    #[test]
    fn the_harm_window_is_stamped_after_the_probe_that_opens_it() {
        let probe_finished = std::cell::Cell::new(None);
        let (before, stamp) = snapshot_then_stamp(|| {
            std::thread::sleep(std::time::Duration::from_millis(25));
            probe_finished.set(Some(chrono::Utc::now()));
            // ≥2 counters: the probe's value must pass through untouched, and a
            // one-element fixture cannot show a container was preserved.
            vec![
                ("mysql_created_tmp_disk_tables".to_string(), 7_i64),
                ("mysql_innodb_rows_read".to_string(), 11_i64),
            ]
        });
        assert_eq!(
            before.len(),
            2,
            "the probe's value passes through unchanged"
        );
        let probe_finished = probe_finished.get().expect("the probe ran");
        assert!(
            stamp >= probe_finished,
            "the window start ({stamp}) predates the end of the probe that \
             measures it ({probe_finished}) — the run is being charged for its \
             own instrumentation"
        );
    }

    /// The self-check reaches every orchestrator through ONE seam.
    ///
    /// The shipped bug was a runner bypass: the check was wired into `run()`'s
    /// tail only, so `apply` (`run_waves`) and `apply --pool` (`run_pool`) fell
    /// through their `total > 1` / `pending.len() > 1` gates and said nothing —
    /// and `apply` has no `--export` flag, so a one-export config IS the whole
    /// invocation. This makes the next bypass a diff-time failure: the aggregate
    /// helper has exactly one caller, and every orchestrator calls it.
    ///
    /// HONESTY: this pins the WIRING, not the emission — the warning needs two
    /// state-DB rows for one export across two runs, which no unit test in this
    /// module can stage. What it goes RED on is a tail losing the call, or
    /// growing a second path around the seam (verified by deleting `run_pool`'s
    /// call, and by pointing one tail straight at `aggregate::`).
    #[test]
    fn every_orchestrator_tail_routes_the_self_check_through_one_seam() {
        let whole = include_str!("run.rs");
        // Product half only — the test modules below would otherwise answer for
        // the code they are supposed to grade.
        let src = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        // CODE occurrences only: a doc comment naming the function is not a call.
        let code: String = src
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");

        // Spelled in pieces so this test's own text cannot satisfy the count if
        // the module boundary ever moves.
        let helper = concat!("aggregate::warn_throughput", "_regressions(");
        assert_eq!(
            code.matches(helper).count(),
            1,
            "the aggregate helper must have exactly ONE caller — the \
             `self_check_throughput` seam. A tail calling it directly skips the \
             re-exec'd-child deferral and reports the same export twice."
        );

        let seam = concat!("self_check", "_throughput(");
        let anchors = [
            ("run", "\npub fn run("),
            ("run_waves", "\npub(crate) fn run_waves("),
            ("run_pool", "\npub(crate) fn run_pool("),
        ];
        let mut starts: Vec<(&str, usize)> = anchors
            .iter()
            .map(|(name, sig)| {
                (
                    *name,
                    code.find(sig)
                        .unwrap_or_else(|| panic!("{name}'s signature moved — update the anchor")),
                )
            })
            .collect();
        starts.sort_by_key(|(_, at)| *at);
        for (i, (name, at)) in starts.iter().enumerate() {
            let end = starts.get(i + 1).map(|(_, n)| *n).unwrap_or(code.len());
            assert!(
                code[*at..end].contains(seam),
                "{name} ends a run, so it must self-check its exports' throughput \
                 — 'EVERY run self-reports degradation' is the contract, and it \
                 was false on `apply` and `apply --pool` for a whole release"
            );
        }
        // `run` has TWO tails (the process-parallel parent returns early, the
        // in-process one falls through); a single call would leave one silent.
        // Fold ≥2 by construction, so count the sites. The seam's own `fn` is a
        // definition, not a call.
        let calls = code
            .match_indices(seam)
            .filter(|(i, _)| !code[..*i].ends_with("fn "))
            .count();
        assert_eq!(
            calls, 4,
            "expected one self-check per tail: run/processes, run/in-process, \
             run_waves, run_pool"
        );
    }
}

#[cfg(test)]
mod wave_grouping_tests {
    use super::{first_name_collision, group_exports_by_wave, is_parallel_safe, next_eligible};

    #[test]
    fn a_synthesized_split_unit_colliding_with_an_existing_export_is_detected() {
        // Convergence round-2 LOW: a user export named exactly `orders#0` coexisting with the
        // split of `orders` would be collapsed by the pool's by_name map and silently dropped.
        // The guard must catch the collision so the caller can refuse. RED against no-guard.
        use crate::config::sample_export;
        let units = vec![sample_export("orders#0"), sample_export("orders#1")];
        let existing = vec![sample_export("users"), sample_export("orders#0")]; // pre-existing table
        assert_eq!(first_name_collision(&units, &existing), Some("orders#0"));
        // No collision when names are disjoint.
        let clean = vec![sample_export("users"), sample_export("events")];
        assert_eq!(first_name_collision(&units, &clean), None);
    }

    /// The pool's pick rule (#166), both directions — RED against `||`→`&&`
    /// (which would starve every heavy export the moment slots are free) and
    /// against dropping the heavy-serialization guard (two giants at once —
    /// the exact overload the wave cost-gate exists to prevent).
    #[test]
    fn pool_next_eligible_serializes_heavies_and_backfills_safe() {
        // No heavy running: the FIRST item is eligible whatever it is (LPT
        // order must not be reshuffled when unconstrained).
        assert_eq!(next_eligible(&[false, true], false), Some(0));
        assert_eq!(next_eligible(&[true, false], false), Some(0));
        // A heavy is running: the next heavy must WAIT; the first SAFE one
        // backfills (the giant + small-and-medium field ask).
        assert_eq!(next_eligible(&[false, true, false], true), Some(1));
        assert_eq!(next_eligible(&[true, false], true), Some(0));
        // Only heavies left while one runs: nothing eligible — the slot waits.
        assert_eq!(next_eligible(&[false, false], true), None);
        // Empty queue: nothing.
        assert_eq!(next_eligible(&[], false), None);
    }

    #[test]
    fn groups_ascending_with_unscheduled_last() {
        let mut a = crate::config::sample_export("a");
        a.wave = Some(3);
        let mut b = crate::config::sample_export("b");
        b.wave = None; // unscheduled → must sort last
        let mut c = crate::config::sample_export("c");
        c.wave = Some(1);
        let mut d = crate::config::sample_export("d");
        d.wave = Some(1); // shares wave 1 with c, preserves input order

        let exports = vec![a, b, c, d];
        let grouped = group_exports_by_wave(&exports);

        let waves: Vec<u32> = grouped.iter().map(|(w, _)| *w).collect();
        assert_eq!(waves, vec![1, 3, u32::MAX], "ascending, unscheduled last");
        let wave1: Vec<&str> = grouped[0].1.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(wave1, vec!["c", "d"], "same-wave keeps input order");
        assert_eq!(grouped[2].1.len(), 1);
        assert_eq!(
            grouped[2].1[0].name, "b",
            "the no-wave export lands in the last group"
        );
    }

    #[test]
    fn parallel_safe_reads_the_plan_flag() {
        // default sample_export leaves `parallel_safe` None → not safe
        let unset = crate::config::sample_export("unset");
        assert!(!is_parallel_safe(&unset), "None is treated as not-safe");

        let mut safe = crate::config::sample_export("safe");
        safe.parallel_safe = Some(true);
        assert!(is_parallel_safe(&safe), "parallel_safe: true → concurrent");

        let mut not_safe = crate::config::sample_export("heavy");
        not_safe.parallel_safe = Some(false);
        assert!(!is_parallel_safe(&not_safe), "parallel_safe: false → alone");
    }
}

/// Index of the most "stop-worthy" failure in a batch: data-integrity (exit 3)
/// outranks schema-drift (4), which outranks retryable (2), which outranks
/// generic (1). The chosen error's typed marker then rides up so `classify_exit`
/// exits the process on the scariest reason rather than whichever export happened
/// to fail first. Returns `None` for an empty slice.
pub(crate) fn representative_failure_idx(failures: &[anyhow::Error]) -> Option<usize> {
    let rank = |e: &anyhow::Error| match crate::error::classify_exit(e) {
        c if c == crate::error::ExitClass::DataIntegrity.code() => 3,
        c if c == crate::error::ExitClass::SchemaDrift.code() => 2,
        c if c == crate::error::ExitClass::Retryable.code() => 1,
        _ => 0,
    };
    (0..failures.len()).max_by_key(|&i| rank(&failures[i]))
}

#[cfg(test)]
mod representative_failure_tests {
    use super::representative_failure_idx;
    use crate::error::{DataIntegrityError, ExitClass, SchemaDriftError, classify_exit};

    #[test]
    fn empty_batch_has_no_representative() {
        assert_eq!(representative_failure_idx(&[]), None);
    }

    #[test]
    fn data_integrity_outranks_everything_regardless_of_position() {
        // Data-integrity sits LAST so a naive "first failure" or a flipped
        // min/max selector would pick the generic error instead.
        let failures = vec![
            anyhow::anyhow!("generic boom"),
            SchemaDriftError::new("shape changed").into(),
            anyhow::anyhow!("another generic"),
            DataIntegrityError::new("reconcile mismatch").into(),
        ];
        let idx = representative_failure_idx(&failures).unwrap();
        assert_eq!(
            classify_exit(&failures[idx]),
            ExitClass::DataIntegrity.code(),
            "a mixed batch must surface the data-integrity (exit 3) failure"
        );
    }

    #[test]
    fn schema_drift_outranks_retryable_and_generic() {
        // No data-integrity present → schema-drift (exit 4) is the scariest.
        let failures = vec![
            anyhow::anyhow!("generic"),
            SchemaDriftError::new("drift").into(),
        ];
        let idx = representative_failure_idx(&failures).unwrap();
        assert_eq!(classify_exit(&failures[idx]), ExitClass::SchemaDrift.code());
    }
}
