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
pub(crate) fn run_waves(config_path: &str, force: bool) -> Result<()> {
    let config = Config::load_with_params(config_path, None)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let opts = RunOptions {
        validate: false,
        reconcile: false,
        resume: false,
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

    // `parallel_export_processes: true` opts into within-wave parallelism: each
    // wave's exports run as concurrent child processes (per-child governor keeps
    // each one source-safe), the call blocks until all exit = the wave barrier.
    // Default stays sequential.
    let parallel = config.parallel_export_processes;

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
        // The wave barrier is the loop itself: each strategy below fully drains
        // the wave (the sequential loop, or the blocking child-process join)
        // before the next iteration starts the next wave.
        if parallel {
            // Keyset safety-gate: within the wave, exports that paginate by a
            // keyset chunk column run together in ONE concurrent batch; every
            // other export (full scan / incremental / time-window / CDC) runs
            // ALONE in its own single-child batch so it never stacks source
            // pressure against a wave-mate. The per-child governor still bounds
            // each one; this gate also bounds the concurrent connection count.
            let (keyset, lone): (Vec<&ExportConfig>, Vec<&ExportConfig>) =
                exports.iter().copied().partition(|e| is_parallel_safe(e));
            log::info!(
                "apply: wave {} — {} keyset export(s) in parallel, {} run alone",
                label,
                keyset.len(),
                lone.len()
            );
            // One single-child batch per lone export (run sequentially), then
            // one concurrent batch for all keyset exports.
            let mut batches: Vec<Vec<&ExportConfig>> = lone.iter().map(|e| vec![*e]).collect();
            if !keyset.is_empty() {
                batches.push(keyset);
            }
            for batch in &batches {
                let (result, cf, stderr_dump) = parallel_children::run_exports_as_child_processes(
                    config_path,
                    batch,
                    false,
                    false,
                    false,
                    force,
                    None,
                );
                child_failures.extend(cf);
                combined_stderr.push_str(&stderr_dump);
                if let Err(e) = result {
                    failures.push(e);
                }
            }
            all_exports.extend_from_slice(exports);
        } else {
            log::info!(
                "apply: wave {} — {} export(s), sequential",
                label,
                exports.len()
            );
            for export in exports {
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
    // Captured child stderr prints AFTER the aggregate (parallel path only) so
    // the run summary stays under the card stack, logs below — matching `run`.
    if !combined_stderr.is_empty() {
        use std::io::Write;
        let mut h = std::io::stderr().lock();
        let _ = h.write_all(combined_stderr.as_bytes());
        let _ = h.flush();
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
            "{} export(s) failed across waves; representative error follows (also: {others})",
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

/// Config-derivable predicate: an export is safe to run concurrently with its
/// wave-mates when it paginates the source by a keyset chunk column (bounded,
/// index-backed scans). Full / incremental / time-window / CDC exports scan or
/// stream the table, so they run ALONE within the wave to avoid stacking source
/// pressure. Conservative on purpose — widen only with evidence.
fn is_parallel_safe(export: &ExportConfig) -> bool {
    export.mode == crate::config::ExportMode::Chunked && export.chunk_column.is_some()
}

#[cfg(test)]
mod wave_grouping_tests {
    use super::{group_exports_by_wave, is_parallel_safe};

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
    fn parallel_safe_only_for_keyset_chunked() {
        // default sample_export is mode=Full, chunk_column=None
        let full = crate::config::sample_export("full");
        assert!(!is_parallel_safe(&full), "full scan is not parallel-safe");

        let mut chunked = crate::config::sample_export("chunked");
        chunked.mode = crate::config::ExportMode::Chunked;
        chunked.chunk_column = Some("id".into());
        assert!(
            is_parallel_safe(&chunked),
            "keyset chunked is parallel-safe"
        );

        // chunked but no chunk_column → not keyset → not safe
        let mut chunked_no_col = crate::config::sample_export("nocol");
        chunked_no_col.mode = crate::config::ExportMode::Chunked;
        chunked_no_col.chunk_column = None;
        assert!(
            !is_parallel_safe(&chunked_no_col),
            "chunked without a chunk column is not keyset"
        );
    }
}

/// Index of the most "stop-worthy" failure in a batch: data-integrity (exit 3)
/// outranks schema-drift (4), which outranks retryable (2), which outranks
/// generic (1). The chosen error's typed marker then rides up so `classify_exit`
/// exits the process on the scariest reason rather than whichever export happened
/// to fail first. Returns `None` for an empty slice.
fn representative_failure_idx(failures: &[anyhow::Error]) -> Option<usize> {
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
