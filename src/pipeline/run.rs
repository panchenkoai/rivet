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
                name_floor,
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
    let started_at = chrono::Utc::now();
    let mut summaries: Vec<RunSummary> = Vec::with_capacity(total);
    let mut failures: Vec<anyhow::Error> = Vec::new();
    // Parallel-path accumulators: per-child metrics live in the state DB, so the
    // parent reconstructs one aggregate from them after every wave has joined.
    let mut all_exports: Vec<&ExportConfig> = Vec::with_capacity(total);
    let mut child_failures: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut combined_stderr = String::new();

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

    let finished_at = chrono::Utc::now();
    if total > 1 {
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
        aggregate::print(&agg);
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

/// Last successful run's duration for `name`, in seconds — the pool's
/// predictor (the same source the wave packer reads). `None` = no history.
fn last_success_secs(state: &StateStore, name: &str) -> Option<f64> {
    state
        .get_metrics(Some(name), 25)
        .ok()?
        .into_iter()
        .find(|m| m.status == "success")
        .map(|m| (m.duration_ms as f64 / 1000.0).max(0.001))
}

/// Longest recorded duration across `name`'s recent runs of ANY terminal
/// status — the fallback predictor when no run ever SUCCEEDED. A failed or
/// interrupted attempt died early, so this is a LOWER bound on the real
/// duration; it is still far more honest than the flat placeholder (field
/// find, 2026-08-13 pool dogfood: a nine-figure-row export with no success
/// history was scheduled as 5 s, and the printed makespan promised minutes
/// for a run that takes hours).
fn last_attempt_secs(state: &StateStore, name: &str) -> Option<f64> {
    state
        .get_metrics(Some(name), 25)
        .ok()?
        .into_iter()
        .map(|m| (m.duration_ms as f64 / 1000.0).max(0.001))
        .fold(None, |acc: Option<f64>, s| {
            Some(acc.map_or(s, |a| a.max(s)))
        })
}

/// A pool item's predicted duration plus which signal produced it, so the
/// makespan print can say how much of the prediction is guesswork.
enum PredictedFrom {
    Measured(f64),
    FailedAttemptFloor(f64),
    Placeholder(f64),
}

impl PredictedFrom {
    fn secs(&self) -> f64 {
        match self {
            PredictedFrom::Measured(s)
            | PredictedFrom::FailedAttemptFloor(s)
            | PredictedFrom::Placeholder(s) => *s,
        }
    }
}

/// Placeholder for an export with no run history at all. Deliberately small so
/// unknown exports never displace measured heavies in LPT order; the makespan
/// print flags how many predictions rest on it.
const POOL_PLACEHOLDER_SECS: f64 = 5.0;

fn predicted_from(state: &StateStore, name: &str) -> PredictedFrom {
    if let Some(s) = last_success_secs(state, name) {
        return PredictedFrom::Measured(s);
    }
    match last_attempt_secs(state, name) {
        // A crashed/failed attempt's duration is a floor on the real one —
        // taking `max` with the placeholder keeps sub-5s failures at 5 s.
        Some(s) => PredictedFrom::FailedAttemptFloor(s.max(POOL_PLACEHOLDER_SECS)),
        None => PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS),
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

    let build_items = |exps: &[ExportConfig]| -> Vec<super::pool::PoolItem> {
        exps.iter()
            .map(|e| super::pool::PoolItem {
                name: e.name.clone(),
                predicted_secs: predicted_from(&state, &e.name).secs(),
                parallel_safe: is_parallel_safe(e),
            })
            .collect()
    };
    let mut items = build_items(&effective);

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
                        items = build_items(&effective);
                        log::warn!(
                            "apply --pool --split: split '{giant}' into {realized} range \
                             sub-export(s) over its key — predicted wall ~{:.1} min (was the \
                             single-export floor). The units share one prefix and fold to family \
                             '{giant}', so the load view reads them as one table.",
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
        items = build_items(&effective);
    }

    // LPT order + the makespan the model PREDICTS (POST-split) — printed up front
    // and graded against the actual wall at the end, so every run improves trust
    // in (or honestly indicts) the model.
    let order = super::pool::pool_order(&items);
    let predicted_secs = super::pool::predicted_makespan_secs(&items, m);
    let floor_secs = super::pool::makespan_floor_secs(&items, m);
    let (mut measured_n, mut attempt_n, mut placeholder_n) = (0usize, 0usize, 0usize);
    for e in &effective {
        match predicted_from(&state, &e.name) {
            PredictedFrom::Measured(_) => measured_n += 1,
            PredictedFrom::FailedAttemptFloor(_) => attempt_n += 1,
            PredictedFrom::Placeholder(_) => placeholder_n += 1,
        }
    }
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
            POOL_PLACEHOLDER_SECS as i64,
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
    let (tx, rx) = std::sync::mpsc::channel::<parent_ui::UiMessage>();
    ipc::install_in_process_tx(tx);
    let n_cards = pending.len();
    let ui_thread = std::thread::Builder::new()
        .name("rivet-ui".to_string())
        .spawn(move || parent_ui::run_ui(rx, name_floor, n_cards))
        .ok();

    let started_at = chrono::Utc::now();
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
    ipc::clear_in_process_tx();
    if let Some(h) = ui_thread {
        let _ = h.join();
    }
    MULTI_EXPORT_MODE.store(prev_multi, AtomicOrdering::Relaxed);

    let finished_at = chrono::Utc::now();
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
    if pending.len() > 1 {
        let entries = summaries
            .iter()
            .map(aggregate::entry_from_summary)
            .collect();
        let agg = aggregate::build(entries, started_at, finished_at, Some(config_path), "pool");
        aggregate::print(&agg);
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
mod pool_prediction_tests {
    use super::{POOL_PLACEHOLDER_SECS, PredictedFrom, predicted_from};
    use crate::state::{MetricRow, StateStore};

    fn store_with(rows: &[(&str, &str, i64, &str)]) -> StateStore {
        let s = StateStore::open_in_memory().expect("in-memory store");
        for (export, run_id, duration_ms, status) in rows {
            s.record_metric_full(&MetricRow {
                export_name: (*export).into(),
                run_id: (*run_id).into(),
                duration_ms: *duration_ms,
                status: (*status).into(),
                ..Default::default()
            })
            .expect("record metric");
        }
        s
    }

    /// Field find (2026-08-13 pool dogfood): an export with FAILED history but
    /// no success was scheduled at the flat 5 s placeholder, so a giant that
    /// had already demonstrated an hours-long attempt was predicted as noise
    /// and the printed makespan promised minutes for an hours-long run. A
    /// failed attempt's duration is a floor on the real one — use it.
    #[test]
    fn unmeasured_export_with_failed_history_predicts_at_least_that_attempt() {
        let s = store_with(&[("big", "r1", 3_600_000, "failed")]);
        match predicted_from(&s, "big") {
            PredictedFrom::FailedAttemptFloor(secs) => {
                assert!(
                    (secs - 3600.0).abs() < 1.0,
                    "the failed attempt's hour must survive as the floor, got {secs}"
                );
            }
            _ => panic!("failed-only history must be a FailedAttemptFloor, not a placeholder"),
        }
    }

    #[test]
    fn measured_success_beats_failed_attempts_and_no_history_is_a_placeholder() {
        // A success among failures → measured, at the SUCCESS duration.
        let s = store_with(&[
            ("t", "r1", 3_600_000, "failed"),
            ("t", "r2", 120_000, "success"),
        ]);
        match predicted_from(&s, "t") {
            PredictedFrom::Measured(secs) => assert!((secs - 120.0).abs() < 1.0, "got {secs}"),
            _ => panic!("a successful run must classify as Measured"),
        }
        // No history at all → the placeholder, honestly labeled as such.
        let empty = store_with(&[]);
        match predicted_from(&empty, "unknown") {
            PredictedFrom::Placeholder(secs) => assert_eq!(secs, POOL_PLACEHOLDER_SECS),
            _ => panic!("no history must classify as Placeholder"),
        }
        // A sub-placeholder failed attempt is floored at the placeholder, so a
        // 1 s crash does not schedule BELOW the unknown baseline.
        let quick = store_with(&[("q", "r1", 1_000, "failed")]);
        assert!(predicted_from(&quick, "q").secs() >= POOL_PLACEHOLDER_SECS);
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
