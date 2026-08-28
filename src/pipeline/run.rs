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

/// Env marker a parent sets on re-exec'd children to claim their run-over-run
/// throughput self-check: "I am aggregating you, and I will report your exports"
/// (see [`owns_throughput_self_check`]).
///
/// A SECOND variable, set alongside `RIVET_IPC_EVENTS` by
/// `parallel_children::run_exports_as_child_processes`, because the deferral
/// must key on something only a real parent can supply. `RIVET_IPC_EVENTS` is
/// not that: it is a presence flag documented to users in
/// `docs/reference/cli.md` as the way to get the NDJSON progress stream, so an
/// operator who sets it in an Airflow/K8s pod spec turned every plain `rivet
/// run` into a process that defers its self-check to a parent that does not
/// exist — the "check nowhere" failure the seam exists to prevent, made
/// environment-dependent (round-7 bughunt). This one is internal, undocumented,
/// and set only at the spawn site.
pub(crate) const ENV_PARENT_SELF_CHECK: &str = "RIVET_PARENT_OWNS_SELF_CHECK";

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
fn owns_throughput_self_check(reexec_child: bool) -> bool {
    !reexec_child
}

/// Is this process a re-exec'd child whose PARENT owns its self-check?
///
/// BOTH markers, and the second one is the fix. The deferral used to key on
/// `RIVET_IPC_EVENTS` alone — but that variable is a documented USER-FACING
/// switch (`docs/reference/cli.md`: it turns on the NDJSON progress stream) and
/// a presence flag, so `RIVET_IPC_EVENTS=0` in an Airflow/K8s pod spec, or a
/// wrapper that wants machine-readable progress, made every plain `rivet run` in
/// that environment defer its self-check to a parent that does not exist. The
/// check then ran NOWHERE — the exact failure the seam was built to prevent,
/// now environment-dependent (round-7 bughunt).
///
/// [`ENV_PARENT_SELF_CHECK`] is the signal only a real parent can supply:
/// internal, undocumented, and set at the one `current_exe` spawn site
/// (`parallel_children::run_exports_as_child_processes`) alongside
/// `RIVET_IPC_EVENTS`. Requiring BOTH also fails in the recoverable direction —
/// an operator who somehow sets one of them gets the check RUN (a duplicate line
/// at worst), never the silence.
fn is_reexecd_child(ipc_events: bool, parent_owns_self_check: bool) -> bool {
    ipc_events && parent_owns_self_check
}

/// The ONE call site of [`aggregate::warn_throughput_regressions`]: every
/// orchestrator tail (`run`'s two branches, `run_waves`, `run_pool` and
/// `apply_cmd::run_apply_command`'s plan-artifact replay) routes its entries
/// through here, unconditionally — no `len() > 1` gate, because the field
/// regression this check exists for hit a single-export config, and a sealed
/// plan artifact IS a single-export run.
///
/// Structural, not conventional: the call is welded to the aggregate the tail
/// already builds (build → print? → self-check → persist?), and
/// `every_orchestrator_tail_routes_the_self_check_through_one_seam` — which
/// DERIVES the tail set from `src/pipeline/**` rather than naming it — fails the
/// build if a tail grows a second path or calls the aggregate helper directly.
pub(super) fn self_check_throughput(
    state: &StateStore,
    entries: &[crate::state::RunAggregateEntry],
    modes: &RunModes<'_>,
) {
    self_check_throughput_as(
        state,
        entries,
        modes,
        // The only untestable half: two process-global env reads. Every other
        // decision here is a parameter, so the polarity below is graded at
        // value level (see [`self_check_throughput_as`]), and the RULE joining
        // the two markers is graded in [`is_reexecd_child`].
        is_reexecd_child(
            ipc::ipc_events_enabled(),
            std::env::var_os(ENV_PARENT_SELF_CHECK).is_some(),
        ),
    );
}

/// [`self_check_throughput`] with the "am I a re-exec'd child" answer INJECTED
/// rather than read from the process environment.
///
/// The split exists because the polarity of the guard below is exactly what
/// shipped broken twice (the check nowhere, then the check twice), and the env
/// read cannot be flipped by a unit test — `RIVET_IPC_EVENTS` is process-global
/// and the suite runs its tests in parallel threads. With the answer as an
/// argument, `a_reexecd_child_stays_silent_while_a_parent_emits_the_regression`
/// observes the EMISSION for both roles against a real `StateStore` baseline,
/// so a dropped `!` here (parent silent, captured child talking to nobody) is a
/// RED test rather than a survivor.
fn self_check_throughput_as(
    state: &StateStore,
    entries: &[crate::state::RunAggregateEntry],
    modes: &RunModes<'_>,
    reexec_child: bool,
) {
    if !owns_throughput_self_check(reexec_child) {
        log::debug!(
            "throughput self-check: deferred to the parent process (this is a re-exec'd child; \
             its stderr is captured, the parent prints beside the run aggregate)"
        );
        return;
    }
    // One GROUP per distinct label, each entry in exactly one group — so the
    // EXACTLY-ONCE-per-export contract survives the split, and the aggregate
    // helper keeps its single call site (the seam test counts it).
    let mut by_mode: std::collections::BTreeMap<&str, Vec<crate::state::RunAggregateEntry>> =
        std::collections::BTreeMap::new();
    for e in entries {
        by_mode
            .entry(modes.for_export(&e.export_name))
            .or_default()
            .push(e.clone());
    }
    for (mode, group) in by_mode {
        aggregate::warn_throughput_regressions(state, &group, mode);
    }
}

/// Which concurrency label the throughput self-check applies to each export.
///
/// `run`'s two branches have ONE honest answer for the whole run: both spawn a
/// child process / a thread PER EXPORT with no cap and join them all, so every
/// export really did overlap every other one — [`RunModes::uniform`] is the
/// truth there.
///
/// The WAVE and POOL runners do not. The wave's cost gate emits one single-child
/// batch per heavy export plus one concurrent batch for the `parallel_safe`
/// ones, and those batches run strictly ONE AFTER ANOTHER — so an export in a
/// lone batch shared the source with nothing, whatever the widest batch of the
/// run was. The pool's queue has the same trailing shape for a different reason:
/// [`next_eligible`] never co-runs two heavies, so ONE `parallel_safe` export is
/// enough to make [`pool_is_concurrent`] true for the RUN while each heavy left
/// after the safe work drains runs strictly ALONE (`--pool 5` over 1 safe + 7
/// heavy: the safe export rides alongside the first heavy, the other six do not
/// overlap anything).
///
/// Labelling those exports with the run's peak hands
/// `aggregate::mode_shares_the_source` the source-sharing string for an export
/// that ran alone: the self-check then excuses a real regression as "BY DESIGN —
/// compare the run's makespan" and DELETES the actionable pointer ("check
/// governor sheds / adaptive batch shrinks / source load"), which is the exact
/// excuse round 4 removed from the flag-derived run-level label, round 6 found
/// again one layer in on the wave, and round 7 found a third time on the pool
/// (bughunt 2026-08-16).
///
/// An export the map never saw falls back to the run-wide label — the same
/// fail-safe direction `mode_shares_the_source` uses for an unknown mode: hedged
/// text on a serial export is harmless, a confident "check governor sheds" on a
/// concurrent one is a false accusation.
pub(super) struct RunModes<'a> {
    run: &'a str,
    per_export: std::collections::HashMap<String, &'static str>,
}

impl<'a> RunModes<'a> {
    /// Every export ran under the run's one mode.
    pub(super) fn uniform(run: &'a str) -> Self {
        Self {
            run,
            per_export: std::collections::HashMap::new(),
        }
    }

    /// Each named export ran under its OWN mode; anything unnamed under `run`.
    pub(super) fn per_export(
        run: &'a str,
        per_export: std::collections::HashMap<String, &'static str>,
    ) -> Self {
        Self { run, per_export }
    }

    /// The label to attribute ONE export's throughput to.
    fn for_export(&self, export: &str) -> &str {
        self.per_export.get(export).copied().unwrap_or(self.run)
    }
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
/// The subject is the AGGREGATE the tail just built — see
/// [`reports_run_aggregate`] for why the count may not be re-derived at the call
/// site. Its `total_exports` is the count AFTER `partition_by` expansion, which
/// is why the zero case is not hypothetical: `expand_one` logs "found no rows —
/// nothing to export" and pushes no children, so an empty table leaves the run
/// with zero exports and a machine consumer still holding a `--json` pipe. `<=
/// 1` (not `== 1`) is the whole fix — an empty-but-valid `total_exports: 0`
/// document beats silence, which parses as neither JSON nor "nothing happened".
fn tail_plan(agg: &crate::state::RunAggregate, machine_output_requested: bool) -> TailPlan {
    TailPlan {
        aggregate: reports_run_aggregate(agg),
        machine_output: machine_output_requested && agg.total_exports <= 1,
    }
}

/// Does this run report a run AGGREGATE — the summary card on stderr and the
/// `run_aggregate` row in the state DB?
///
/// ONE rule, called by every orchestrator tail that has a choice (`run` via
/// [`tail_plan`], `run_waves`, `run_pool`), because it was three inline `> 1`
/// comparisons in three live-only functions before — the shape the
/// runner-bypass class keeps arriving in, and one no unit test could reach.
/// An aggregate over a single row is noise: the per-export card already printed
/// every number it would contain (and a child of `--parallel-export-processes`
/// would write a duplicate of its parent's row).
///
/// The argument is the AGGREGATE, not a count, because unifying the PREDICATE
/// left the three tails disagreeing about its SUBJECT: `run` passed the
/// post-expansion export count and `run_pool` the post-skip pending count (both
/// equal to what lands in the aggregate), while `run_waves` passed
/// `config.exports.len()`, computed BEFORE the per-wave `--resume` `_SUCCESS`
/// skip. A five-export config with every destination already complete then
/// printed "Run summary (0 exports) … rows: 0" and `aggregate::persist` wrote a
/// `run_aggregate` row for a run that did no work — polluting the table an
/// operator queries for history, while the same config under `--pool --resume`
/// returned early and wrote nothing. One shared rule, opposite behaviour
/// (round-7 bughunt). Taking the aggregate makes the subject the same quantity
/// by construction: a caller cannot hand it a number it re-derived.
///
/// Deliberately NOT the gate on the run-over-run self-check, which fires at
/// every count — a one-export config IS the whole invocation of `apply`, and
/// that is the shape the 2026-08-13 field regression had.
fn reports_run_aggregate(agg: &crate::state::RunAggregate) -> bool {
    agg.total_exports > 1
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
                    // From the count that ran at once, never the flag: one
                    // child per export, all spawned before any is joined (see
                    // [`run_mode_label`]).
                    run_mode_label(exports.len(), true),
                );
                aggregate::print(&agg);
                self_check_throughput(
                    &state,
                    &agg.per_export,
                    &RunModes::uniform(&agg.parallel_mode),
                );
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
    // How many exports ran AT ONCE — the measurement the run's mode label is
    // derived from (see [`run_mode_label`]). 1 until a concurrent path raises
    // it, so the sequential loop below cannot claim a concurrency it never had.
    let mut peak_concurrency = 1usize;

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
            // Every thread is spawned before any is joined, so the number of
            // live handles IS the overlap this run reached — counted, not
            // assumed from `run_parallel`.
            peak_concurrency = peak_concurrency.max(handles.len());
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
    // From the overlap this run REACHED, not from `run_parallel` — the label
    // decides whether the throughput self-check keeps its actionable tail or
    // excuses a regression as source sharing, so it is a measurement (see
    // [`run_mode_label`]).
    let parallel_mode = run_mode_label(peak_concurrency, false);
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
    // EITHER machine channel asks for the document — `--summary-output` alone is
    // the common scheduler shape, so this is `||`, never `&&`. Live-only by
    // construction (a real `run()` needs a source, a destination and a state
    // DB); the oracle that goes RED on `&&` is
    // `run_summary_output_writes_json_to_file` (tests/live/live_cli_flags.rs) —
    // one export, `--summary-output`, no `--json`, asserting the file exists.
    // `run_json_flag_prints_aggregate_summary_to_stdout` does NOT grade this
    // operator: `--json` prints from `json_output` directly a few lines below.
    let plan = tail_plan(&agg, summary_output.is_some() || json_output);
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
            self_check_throughput(
                &state,
                &agg.per_export,
                &RunModes::uniform(&agg.parallel_mode),
            );
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

    fold_failures(failures, "")
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
    // …and the concurrency each EXPORT ran at, which on this runner is a
    // per-export fact, not a run-level one: the cost gate's batches execute one
    // after another, so a lone batch's export shared the source with nothing
    // however wide the run's widest batch was. Read by the throughput
    // self-check's attribution only — see [`RunModes`].
    let mut export_modes: std::collections::HashMap<String, &'static str> =
        std::collections::HashMap::new();
    // The "`--parallel` bought you nothing" warning is a run-level fact; emit it
    // at the first wave that shows it and not once per wave.
    let mut degenerate_parallel_warned = false;

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
            // Cost safety-gate — see [`cost_gate_batches`] for the rule.
            let safe_n = pending.iter().filter(|e| is_parallel_safe(e)).count();
            log::info!(
                "apply: wave {} — {} parallel-safe export(s) in parallel, {} run alone",
                label,
                safe_n,
                pending.len() - safe_n
            );
            let batches = cost_gate_batches(&pending);
            // …and the one fact that `info` line cannot deliver: `--parallel`
            // that bought NO concurrency, at WARN, BEFORE this wave's batches
            // execute, naming the lever (see [`degenerate_parallel_warning`]).
            // Once per run — a second degenerate wave adds no information the
            // first line did not already give, and repeating it buries it.
            if !degenerate_parallel_warned
                && let Some(line) = degenerate_parallel_warning(
                    &label,
                    &batches.iter().map(Vec::len).collect::<Vec<_>>(),
                )
            {
                log::warn!("{line}");
                degenerate_parallel_warned = true;
            }
            // Batches run one after another (the loop below blocks per batch),
            // so the run's concurrency is the WIDEST batch, never the number of
            // exports it covered.
            peak_concurrency =
                peak_concurrency.max(batches.iter().map(Vec::len).max().unwrap_or(0));
            // …and the same fact PER EXPORT, for the throughput self-check: an
            // export in a lone batch overlapped nothing even when another batch
            // of this run was wide (see [`RunModes`]).
            export_modes.extend(wave_batch_modes(&batches));
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
    // The mode label is decided from the concurrency the run ACHIEVED (the same
    // `peak_concurrency` the harm frame above reads), never from the flag — see
    // [`wave_mode_label`].
    // The mode label + RunModes are the ONLY wave-specific inputs: waves is the
    // one tail whose exports did NOT all run under the run's label
    // ([`RunModes::per_export`]). Everything after is the shared tail.
    let mode_label = wave_mode_label(peak_concurrency);
    let modes = RunModes::per_export(mode_label, export_modes);
    finish_run_tail(
        &state,
        entries,
        started_at,
        finished_at,
        config_path,
        mode_label,
        &modes,
        Some((&combined_stderr, &config_dir)),
        failures,
        " across waves",
    )
}

/// Arch-roast follow-up (2026-08-21): the representative-failure fold, written
/// once. Three orchestrator tails carried the identical block with only the
/// context word differing — the exact per-copy drift the run-level tail keeps
/// growing. Pure over its inputs.
fn fold_failures(mut failures: Vec<anyhow::Error>, context: &str) -> crate::error::Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    // Carry a representative typed failure as the returned error so
    // `error::classify_exit` downcasts the marker (DataIntegrityError=3,
    // SchemaDriftError=4, transient=2) through anyhow's context chain. Pick
    // the most "stop-worthy" class — data-integrity (possibly-wrong data)
    // outranks schema-drift, which outranks retryable, which outranks
    // generic — so a mixed batch exits on the scariest reason.
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
    Err(primary.context(format!(
        "{} export(s) failed{}; representative error follows (also: {others})",
        failures.len() + 1,
        context,
    )))
}

/// The waves/pool run tail, written once: ONE aggregate, then the shared
/// routing — the card and the `run_aggregate` row are multi-export-only
/// ([`reports_run_aggregate`]), the run-over-run self-check is not gated at
/// all, and the failure fold picks the scariest class. The two machine-channel
/// tails (`run()`'s process branch and in-process tail, which honour
/// `--summary-output`/`--json`) are the tracked next slice — this seam unifies
/// the two copies that were already byte-alike so a routing fix lands once.
#[allow(clippy::too_many_arguments)]
fn finish_run_tail(
    state: &StateStore,
    entries: Vec<crate::state::RunAggregateEntry>,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: chrono::DateTime<chrono::Utc>,
    config_path: &str,
    mode_label: &str,
    modes: &RunModes<'_>,
    stderr_artifact: Option<(&str, &Path)>,
    failures: Vec<anyhow::Error>,
    failure_context: &str,
) -> crate::error::Result<()> {
    let agg = aggregate::build(
        entries,
        started_at,
        finished_at,
        Some(config_path),
        mode_label,
    );
    if reports_run_aggregate(&agg) {
        aggregate::print(&agg);
    }
    self_check_throughput(state, &agg.per_export, modes);
    if reports_run_aggregate(&agg) {
        aggregate::persist(state, &agg, None);
    }
    if let Some((dump, dir)) = stderr_artifact {
        emit_child_stderr(dump, dir);
    }
    fold_failures(failures, failure_context)
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

/// The run-mode label a wave run records on its aggregate — from the same
/// measured `peak` [`wave_harm_window`] reads, never from the `--parallel` flag.
///
/// The label is not decoration: `aggregate::throughput_regressions` classifies
/// it (`mode_shares_the_source`) to decide whether a per-export rows/s DROP is
/// EXPECTED ("exports share the source … compare the run's makespan") or
/// ACTIONABLE ("check governor sheds / adaptive batch shrinks / source load").
/// `apply --parallel` over a hand-written config (`parallel_safe` unset → not
/// safe) gives every export its own single-child batch and runs them one after
/// another; passing the flag-derived string there excused a genuine 2.7×
/// per-export regression as by-design and deleted the only actionable pointer —
/// the exact text the 2026-08-13 field regression needed (bughunt 2026-08-14).
pub(super) fn wave_mode_label(peak: usize) -> &'static str {
    if peak > 1 {
        "wave-parallel-processes"
    } else {
        "wave-sequential"
    }
}

/// What `apply --parallel` owes the operator when the cost gate bought it NO
/// concurrency — `None` when the wave really did overlap something.
///
/// `parallel_safe` is `Option<bool>` and [`is_parallel_safe`] reads
/// `unwrap_or(false)`, so in a hand-written config (one `rivet plan` never
/// annotated) EVERY export is heavy and [`cost_gate_batches`] gives each its own
/// single-child batch — which the wave loop then runs strictly one after
/// another. `--parallel` serialised the whole run, and the only thing rivet said
/// about it was `log::info!("… {} parallel-safe export(s) in parallel, {} run
/// alone")`, invisible at the default `warn` level (`main.rs`:
/// `default_filter_or("warn")`). The operator waits out a sequential run
/// believing they asked for a parallel one; the sibling `--pool` path prints its
/// schedule with `println!`, so two runners disagreed about the visibility of
/// the same fact (round-7 bughunt).
///
/// WARN, per the project rule that a run-start diagnostic telling the operator
/// their config will be slow must be visible at the default level, and it names
/// the lever rather than just the symptom. `None` at one export (nothing could
/// have overlapped — that is not a config problem) and `None` the moment any
/// batch is wider than 1, so the ordinary informative case stays at `info` and
/// this does not fire on every wave of a healthy run.
fn degenerate_parallel_warning(wave: &str, batch_widths: &[usize]) -> Option<String> {
    let exports: usize = batch_widths.iter().sum();
    if exports <= 1 || batch_widths.iter().copied().max().unwrap_or(0) > 1 {
        return None;
    }
    Some(format!(
        "apply --parallel: wave {wave} — all {exports} export(s) run ALONE, one after another, \
         so `--parallel` bought no concurrency here. The cost gate only batches exports marked \
         `parallel_safe: true`, and none of these are (an export with no `parallel_safe:` counts \
         as heavy). Set `parallel_safe: true` on the cheap ones — or regenerate the config with \
         `rivet plan`, which annotates it from the source-aware cost class — to actually overlap \
         them."
    ))
}

/// The wave cost-gate's batching, pure so the SHAPE the run executes is
/// observable without spawning child processes.
///
/// Within a wave the cheap (`parallel_safe`) exports run together in ONE
/// concurrent batch; every heavier export runs ALONE in its own single-child
/// batch, since a big table already chunk-parallelizes internally and two at
/// once would overload the source. The per-child governor still bounds each one;
/// this gate also bounds the concurrent connection count.
///
/// The caller runs the returned batches STRICTLY one after another, which is
/// what makes both derived facts true: the run's peak concurrency is the widest
/// batch ([`wave_harm_window`]), and each export's own concurrency is ITS
/// batch's width ([`wave_batch_modes`]).
fn cost_gate_batches<'a>(pending: &[&'a ExportConfig]) -> Vec<Vec<&'a ExportConfig>> {
    let (safe, lone): (Vec<&ExportConfig>, Vec<&ExportConfig>) =
        pending.iter().copied().partition(|e| is_parallel_safe(e));
    // One single-child batch per lone export (run sequentially), then one
    // concurrent batch for all parallel-safe exports.
    let mut batches: Vec<Vec<&ExportConfig>> = lone.iter().map(|e| vec![*e]).collect();
    if !safe.is_empty() {
        batches.push(safe);
    }
    batches
}

/// Each export's OWN concurrency label, derived from the batch it ran in.
///
/// The label comes from [`wave_mode_label`] rather than being spelled here, so
/// the per-export attribution and the run-level one cannot drift apart (and a
/// third label added there flows straight through). The width is the batch's
/// own `len()`: batches execute one after another, so an export in a
/// single-child batch overlapped nothing — even in a run whose other batch was
/// eight wide.
fn wave_batch_modes(batches: &[Vec<&ExportConfig>]) -> Vec<(String, &'static str)> {
    batches
        .iter()
        .flat_map(|batch| {
            let label = wave_mode_label(batch.len());
            batch.iter().map(move |e| (e.name.clone(), label))
        })
        .collect()
}

/// The widest overlap a pool of `slots` can reach over `safe` parallel-safe and
/// `heavy` non-`parallel_safe` exports — the pool's sibling of the wave's
/// "widest batch" peak, and derived the same way: from the SHAPE of the work,
/// not from the invocation.
///
/// [`next_eligible`] never lets two heavies co-run, so heavies contribute at
/// most ONE to the overlap however many are queued. The default config is
/// exactly this case: `parallel_safe` is `Option<bool>` and
/// [`is_parallel_safe`] reads `unwrap_or(false)`, so a hand-written config with
/// no `rivet plan` cost classes makes EVERY export heavy — `--pool 5` over
/// eight of them peaks at 1 and each worker but one sleeps in the
/// `next_eligible` retry loop. `slots.min(pending)` called that a pool run
/// (round-5 bughunt).
///
/// Structural, not sampled, and deliberately so: the same value must be known
/// BEFORE the workers start (`MULTI_EXPORT_CONCURRENT` gates every per-export
/// DIAGNOSIS hedge) and read again after they join (the harm frame, the mode
/// label). A runtime `fetch_max` peak could only feed the latter two, and the
/// three surfaces disagreeing about one run is the defect [`pool_is_concurrent`]
/// exists to prevent.
fn pool_peak_concurrency(slots: usize, safe: usize, heavy: usize) -> usize {
    slots.min(safe + heavy.min(1))
}

/// Did the pool actually OVERLAP any work?
///
/// `--pool 1`, or a single pending export left after the `--resume` skip, runs
/// strictly serially however many slots were asked for — and so does a pool
/// whose queue is all heavy ([`pool_peak_concurrency`]). ONE predicate because
/// THREE surfaces answer to it and must not disagree about the same run: the
/// per-export DIAGNOSIS hedge (`MULTI_EXPORT_CONCURRENT`), the run-level harm
/// frame ([`pool_harm_window`]) and the aggregate's mode label
/// ([`pool_mode_label`]). Only the first read it before 2026-08-14, so a solo
/// export got a correct un-hedged per-export attribution AND a second,
/// flag-framed run-level line about the same spill.
fn pool_is_concurrent(slots: usize, safe: usize, heavy: usize) -> bool {
    pool_peak_concurrency(slots, safe, heavy) > 1
}

/// The window a POOL run actually presented to the source — the pool's sibling
/// of [`wave_harm_window`], and for the same reason: the frame is a concurrency
/// claim, so it must come from the concurrency, not from `--pool N`.
///
/// `--pool 5 --resume` with one export left, `--pool 1` over eight, or a queue
/// of eight heavies that serialize against each other, never overlapped
/// anything; framing those as a pool window names "`--pool` slots" as the FIRST
/// lever, which tells the operator to shrink a 1 (or to shrink five slots of
/// which one was ever used) and pushes the two levers that DO work —
/// `chunk_size`, `tuning.batch_size` — behind a lie. That is verbatim the harm
/// [`HarmWindow::Serial`] was introduced for one runner over.
///
/// `None` when the pool drained nothing, matching the wave rule: rivet's window
/// then contains none of rivet's work, so a spill in it is foreign.
fn pool_harm_window(slots: usize, safe: usize, heavy: usize) -> Option<HarmWindow> {
    let pending = safe + heavy;
    if pending == 0 {
        return None;
    }
    Some(if pool_is_concurrent(slots, safe, heavy) {
        HarmWindow::Pool {
            exports: pending,
            // The lever the operator can actually turn is the flag they typed;
            // the peak may be narrower than it (heavies cap their own share),
            // and lowering `--pool` still sheds the safe exports riding along.
            slots,
        }
    } else {
        HarmWindow::Serial { exports: pending }
    })
}

/// The run-mode label a pool run records — the [`wave_mode_label`] rule on the
/// pool: a pool that never overlapped anything did not share the source, so the
/// throughput self-check must keep its actionable pointer instead of excusing a
/// regression with a concurrency the run did not have.
///
/// `"pool-serial"` is its own string (not `"sequential"`) so the aggregate card
/// and the `run_aggregate` row still say WHICH runner produced it;
/// `aggregate::mode_shares_the_source` classifies it with the serial modes.
pub(super) fn pool_mode_label(concurrent: bool) -> &'static str {
    if concurrent { "pool" } else { "pool-serial" }
}

/// One pool worker's outcome: the export's result, its `RunSummary`, and the
/// `(start_ms, end_ms)` window it occupied relative to the run's start — the
/// measurement [`pool_export_modes`] reads to attribute concurrency per export.
type PoolOutcome = (Result<()>, RunSummary, (i64, i64));

/// Each pool export's OWN concurrency label — the pool's sibling of
/// [`wave_batch_modes`], and the answer to the same question one runner over.
///
/// [`pool_is_concurrent`] answers for the RUN, and its own counter-example is in
/// its doc: `next_eligible` never co-runs two heavies, so a single
/// `parallel_safe` export riding alongside the first heavy makes the whole run
/// "pool" while every heavy left after the safe work drains runs strictly ALONE.
/// `--pool 5` over 1 safe + 7 heavy is that shape: six trailing heavies would
/// each be told "this run ran pool, where exports share the source and
/// per-export rows/s falls BY DESIGN" — six real regressions excused, and the
/// actionable pointer deleted, verbatim the harm the wave half fixed.
///
/// So the label is MEASURED, not modelled: `windows` is one `(export, start_ms,
/// end_ms)` per export that ran, stamped by the worker thread around
/// `run_export_job` and relative to the run's own start. An export is labelled
/// concurrent iff its window really intersects another export's. Deriving it
/// from the schedule instead (LPT order + the safe/heavy split) would be a
/// second model of what the workers do, free to disagree with them; the clock
/// cannot.
///
/// Half-open intervals (`start < other_end && other_start < end`), so two
/// exports that merely touch — one ending exactly when the next is claimed — do
/// not count as overlapping. A zero-length window (a state-open failure that
/// returns a synthetic summary immediately) overlaps only an export already
/// running at that instant, which is the honest answer for a run that did no
/// work.
fn pool_export_modes(windows: &[(String, i64, i64)]) -> Vec<(String, &'static str)> {
    windows
        .iter()
        .enumerate()
        .map(|(i, (name, start, end))| {
            let overlapped = windows
                .iter()
                .enumerate()
                .any(|(j, (_, other_start, other_end))| {
                    j != i && start < other_end && other_start < end
                });
            // The label comes from the run-level decider, never spelled here, so
            // the two attributions cannot drift (same rule as the wave's).
            (name.clone(), pool_mode_label(overlapped))
        })
        .collect()
}

/// The run-mode label the TOP-LEVEL runner ([`run`]) records — the third and
/// fourth producers of the string `aggregate::mode_shares_the_source`
/// classifies, and the two the round-4 fix left spelling their claim inline
/// (`"parallel-processes"` at the child-process aggregate, `if run_parallel {
/// "parallel-threads" } else { "sequential" }` at the tail).
///
/// `peak` is how many exports ran AT ONCE: `exports.len()` on both concurrent
/// paths, which spawn one child process / one thread per export with no cap and
/// join them all, and 1 on the sequential loop. Nothing serializes those paths
/// today — the fix is that the CLAIM is now derived from a count rather than
/// from `run_parallel`, so the first cost gate to split them (which is exactly
/// how `apply --parallel`'s wave bug was born: a flag that meant "concurrent"
/// until the gate started emitting single-child batches) cannot leave a
/// concurrent label on a serial run.
///
/// `"sequential"` for either shape at peak ≤ 1 — the string the sequential loop
/// already emitted, and one `aggregate::mode_shares_the_source` classifies
/// explicitly, so a serialized run keeps the self-check's ACTIONABLE tail
/// instead of excusing a regression as source sharing.
pub(super) fn run_mode_label(peak: usize, processes: bool) -> &'static str {
    match (peak > 1, processes) {
        (false, _) => "sequential",
        (true, true) => "parallel-processes",
        (true, false) => "parallel-threads",
    }
}

/// The "prediction is a LOWER BOUND" line — or `None` when every prediction in
/// the schedule rests on a real success.
///
/// ONE source for that claim, and it reads the RECONCILED classification
/// (`pool::classification_counts` over the post-split `predict_items` sweep).
/// The `--split` block cannot answer the question, because the only input it
/// has is the SEED: unit names are stable across runs, so from run 2 onward
/// each `{giant}#i` has history of its OWN that supersedes the seed
/// (`pool::reconcile_split_seed`), while the giant is retained out of the run
/// set and its rows stay frozen at the failure that motivated the split. A
/// hedge derived there kept saying "the giant has no successful run to measure
/// from" in the same run whose accounting printed "N measured, 0 estimated" —
/// one run, two contradictory honesty claims about the same exports (bughunt
/// 2026-08-14).
/// What `apply --pool` says when the `--resume` skip leaves nothing to run.
///
/// `split_noticed` is whether this run already emitted the `--split` notice,
/// whose closing clause forward-references the predicted-makespan line. That
/// line is printed BELOW the early return this message accompanies, so on a
/// fully-complete `--split --resume` re-run the promise could never be kept:
/// the operator was left looking for a line that does not exist — the dangling
/// forward reference round 3 fixed for the harm line, on a different message
/// (bughunt 2026-08-16).
///
/// So the split case CANCELS the pointer explicitly instead of going quiet. The
/// plain case keeps the one-line message it always had — there is nothing
/// pointing at the schedule to retract.
fn nothing_to_run_message(split_noticed: bool) -> String {
    let base = "apply --pool: nothing to run (no exports, or all complete)";
    if split_noticed {
        format!(
            "{base} — every split unit's work is already in the shared prefix, so this run \
             schedules nothing: the predicted-makespan line the `--split` notice above points \
             at does not print. Re-run without `--resume` (or into a fresh prefix) to schedule \
             the units again."
        )
    } else {
        base.to_string()
    }
}

fn lower_bound_hedge(attempt_n: usize, placeholder_n: usize) -> Option<String> {
    let unmeasured = attempt_n + placeholder_n;
    (unmeasured > 0).then(|| {
        format!(
            "        prediction is a LOWER BOUND: {unmeasured} export(s) have no successful run \
             to measure from ({attempt_n} scheduled at a failed attempt's duration, \
             {placeholder_n} at a {}s placeholder) — it tightens as runs complete",
            super::pool::POOL_PLACEHOLDER_SECS as i64,
        )
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
/// aggregate's own window (`RunAggregate.duration_ms`, and the run-level rows/s
/// `aggregate::print` derives from it) is inflated by the same amount. NOT the
/// run-over-run throughput self-check: that one compares each export's own
/// `rows`/`duration_ms` from its `RunSummary` / `export_metrics` row, so the run
/// window cannot move it (bughunt 2026-08-14). The close side was fixed by hand at
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
    // #167 per-unit resume: with `--split` the prefix-level _SUCCESS says the
    // WHOLE giant completed (units suppress their own marker; the pool writes
    // the one marker after every unit succeeds) — but a crash can leave a
    // partially-complete split with NO marker, and pre-skipping the giant on
    // the marker's absence alone cannot tell "never ran" from "half done". So
    // when `--split` is set the pre-skip is deferred to a
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
                    Some(u) => {
                        // The reconstruction may have SHRUNK the partition (a
                        // trailing-adjacent crash: the open tail absorbed the
                        // crashed units). Stamp the ceased ordinals' ledger rows
                        // + bucket markers terminal NOW — this is the only
                        // moment that knows they ceased, and unstamped they
                        // wedge gc/cleanup on the shared prefix forever
                        // (round-4; born with the reconstruction in #217).
                        super::split::stamp_ceased_units(
                            &base.destination,
                            &base.family(),
                            &base.name,
                            u.len(),
                            &state,
                        );
                        Some(u)
                    }
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
                        //
                        // This line speaks for the SPLIT, not for the run's
                        // prediction: `broken` is `advise_split`'s projection
                        // over the PRE-split items (the giant at whatever its
                        // frozen prediction was), and the seed it is derived
                        // from is only a first-run bootstrap — from run 2 on,
                        // each `{giant}#i` has history of its own that
                        // supersedes it (`pool::reconcile_split_seed`). So the
                        // honesty claim about the wall is NOT made here; it is
                        // made once, from the reconciled classification, by
                        // [`lower_bound_hedge`] beside the makespan print
                        // below. Hedging from `unit_from` printed "the giant
                        // has no successful run to measure from" in the same
                        // run whose accounting said "N measured, 0 estimated"
                        // (bughunt 2026-08-14).
                        log::warn!(
                            "apply --pool --split: split '{giant}' into {realized} range \
                             sub-export(s) over its key — projected wall ~{:.1} min from the \
                             pre-split predictions (was the single-export floor). The units share \
                             one prefix and fold to family '{giant}', so the load view reads them \
                             as one table. The run's own predicted makespan — reconciled against \
                             each unit's own history, and hedged when any of it rests on an \
                             unmeasured export — prints with the pool schedule on stdout, \
                             unless the `--resume` skip leaves nothing to schedule (which says \
                             so).",
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
            // Every split unit is complete — but this return sits ABOVE the
            // pool's prefix-`_SUCCESS` writer, so a crash in the
            // [last unit's Success → marker] window would leave the marker
            // missing FOREVER (this is the only path that ever looks again).
            // Repair it before declaring "nothing to run" (round-4).
            if let Some((dest_config, family)) = &split_info {
                finalize::repair_missing_split_marker(dest_config, family);
            }
            // This return sits ABOVE the schedule + makespan block, so a run
            // that got here prints neither — and the `--split` notice above
            // points forward at exactly that makespan line. Cancel the pointer
            // here rather than leaving the operator hunting for a line that
            // cannot print (bughunt 2026-08-16).
            log::warn!("{}", nothing_to_run_message(split_info.is_some()));
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
    // The ONE honesty claim about the wall, from the RECONCILED classification.
    if let Some(hedge) = lower_bound_hedge(attempt_n, placeholder_n) {
        println!("{hedge}");
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
    // claim there (bughunt 2026-08-13). The SAFE/HEAVY split is part of that
    // question, not decoration: `next_eligible` never co-runs two heavies, so a
    // queue with no `parallel_safe` export — the default for a hand-written
    // config — is strictly serial however many slots were asked for (round-5
    // bughunt). Counted once, here, and read by all three surfaces.
    let (safe_pending, heavy_pending) = pool_safe_heavy_split(&pending);
    let really_concurrent = pool_is_concurrent(m, safe_pending, heavy_pending);
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
    // Third element: the export's own `(start_ms, end_ms)` window, relative to
    // `started_at` — the MEASUREMENT the per-export concurrency label is derived
    // from (see [`pool_export_modes`]). Stamped by the worker around the job, so
    // it reports what ran rather than what the schedule intended.
    let collected: std::sync::Mutex<Vec<PoolOutcome>> =
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
                    // The window this export actually occupied. Opened here —
                    // AFTER the queue claim, so a worker that slept in the
                    // `next_eligible` retry loop does not charge that wait to an
                    // overlap it never had.
                    let claimed_at = chrono::Utc::now();
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
                    let window = (
                        (claimed_at - started_at).num_milliseconds(),
                        (chrono::Utc::now() - started_at).num_milliseconds(),
                    );
                    let (res, summary) = pair;
                    collected.lock().unwrap().push((res, summary, window));
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
    //
    // Framed by what OVERLAPPED, not by `--pool N`: a pool that ran everything
    // serially (`--pool 1`, one export left after the `--resume` skip, or an
    // all-heavy queue) must not name a slot lever it does not have — see
    // [`pool_harm_window`].
    if let Some(window) = pool_harm_window(m, safe_pending, heavy_pending) {
        run_harm.close_and_warn(window);
    }

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
    // One `(export, start_ms, end_ms)` per export that ran — the input the
    // per-export concurrency label is MEASURED from (see [`pool_export_modes`]).
    let mut pool_windows: Vec<(String, i64, i64)> = Vec::with_capacity(summaries.capacity());
    for (res, summary, (start_ms, end_ms)) in collected.into_inner().unwrap() {
        if let Some(pfx) = &unit_prefix
            && summary.export_name.starts_with(pfx.as_str())
            && (res.is_err() || summary.status != "success")
        {
            split_units_all_ok = false;
        }
        if let Err(e) = res {
            failures.push(e);
        }
        pool_windows.push((summary.export_name.clone(), start_ms, end_ms));
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
    // Pool-specific inputs only: label from what OVERLAPPED (a serialized pool
    // must keep the self-check's actionable pointer, [`pool_mode_label`]) and
    // per-export MEASURED attribution ([`pool_export_modes`] — one
    // `parallel_safe` export makes the RUN a pool while every heavy runs
    // alone). Everything after is the shared tail.
    let mode_label = pool_mode_label(really_concurrent);
    let modes = RunModes::per_export(
        mode_label,
        pool_export_modes(&pool_windows).into_iter().collect(),
    );
    finish_run_tail(
        &state,
        entries,
        started_at,
        finished_at,
        config_path,
        mode_label,
        &modes,
        None,
        failures,
        " in the pool",
    )
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

/// The pool queue's `(safe, heavy)` census — the INPUT every pool concurrency
/// claim is computed from ([`pool_is_concurrent`], [`pool_peak_concurrency`],
/// [`pool_harm_window`], [`pool_mode_label`]).
///
/// Pure and unit-tested because those four consumers are, and a correct
/// decision fed a wrong count is the defect this repo keeps paying for: the
/// counts used to be derived inline inside `run_pool` (`len() - heavy`), which
/// needs a live source, so the arithmetic that DECIDES "did this pool overlap
/// anything" was the one link in the chain nothing graded. Getting it wrong is
/// not cosmetic — an all-heavy queue that reports `safe > 0` claims a
/// concurrent window it never had, and `run_diagnosis` then hedges every harm
/// counter away as "shared with siblings" on a run that was strictly serial.
fn pool_safe_heavy_split(pending: &[&ExportConfig]) -> (usize, usize) {
    let heavy = pending.iter().filter(|e| !is_parallel_safe(e)).count();
    (pending.len() - heavy, heavy)
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

    /// The POOL's frame is a concurrency claim too, so it must come from the
    /// concurrency — `--pool 5 --resume` with one export left, and `--pool 1`
    /// over eight, both overlapped NOTHING. The shipped close read
    /// `HarmWindow::Pool { exports: effective.len(), slots: m }` straight off
    /// the invocation, so the operator was told to lower a 1 (or to lower five
    /// slots of which one was ever used) — the very harm `HarmWindow::Serial`
    /// was added for, one runner over, and worse here: with
    /// `really_concurrent == false` the per-export DIAGNOSIS correctly does NOT
    /// hedge, so the two lines about the same spill contradicted each other.
    ///
    /// RED against the shipped unconditional construction: the two serial cases
    /// then read `Pool { .., slots: 5 }` / `Pool { .., slots: 1 }`. And RED
    /// against `pool_peak_concurrency`'s round-4 shape (`slots.min(pending)`,
    /// blind to `parallel_safe`): the all-heavy case then reads
    /// `Pool { exports: 8, slots: 5 }` for a run that never overlapped a thing.
    #[test]
    fn pool_harm_window_is_framed_by_the_concurrency_the_pool_had() {
        use super::pool_harm_window;
        // Five slots, one export left after the --resume skip: nothing overlapped.
        assert_eq!(
            pool_harm_window(5, 1, 0),
            Some(HarmWindow::Serial { exports: 1 }),
            "a pool with one export to drain never overlapped anything"
        );
        // One slot, eight exports: strictly serial — naming `--pool` slots here
        // tells the operator to shrink a 1.
        assert_eq!(
            pool_harm_window(1, 8, 0),
            Some(HarmWindow::Serial { exports: 8 }),
            "`--pool 1` is a serial run whatever the export count"
        );
        // The default hand-written config: no export carries `parallel_safe`, so
        // every one is heavy and `next_eligible` runs them ONE at a time — five
        // slots of which four sleep. Structurally serial, and the frame must say
        // so (round-5 bughunt).
        assert_eq!(
            pool_harm_window(5, 0, 8),
            Some(HarmWindow::Serial { exports: 8 }),
            "heavies never co-run, so an all-heavy pool overlapped nothing"
        );
        // One cheap export riding alongside the heavies IS an overlap of 2.
        assert_eq!(
            pool_harm_window(5, 1, 7),
            Some(HarmWindow::Pool {
                exports: 8,
                slots: 5
            }),
            "one safe export can co-run with the single running heavy"
        );
        // A real pool keeps its own frame AND its slot lever.
        assert_eq!(
            pool_harm_window(5, 12, 0),
            Some(HarmWindow::Pool {
                exports: 12,
                slots: 5
            })
        );
        // The narrowest window that still overlaps.
        assert_eq!(
            pool_harm_window(2, 2, 0),
            Some(HarmWindow::Pool {
                exports: 2,
                slots: 2
            })
        );
        // Nothing to drain: rivet's window holds none of rivet's work, so a
        // spill in it is foreign — same rule as the wave sibling.
        assert_eq!(pool_harm_window(5, 0, 0), None);
    }

    /// The run-mode label decides whether the throughput self-check keeps its
    /// ACTIONABLE tail ("check governor sheds / adaptive batch shrinks / source
    /// load") or excuses the drop as source sharing. Both runners derived it
    /// from the FLAG, so `apply --parallel` over a hand-written config (every
    /// export in its own single-child batch → peak 1) and `apply --pool 1`
    /// printed the by-design excuse for a real 2.7× regression — the exact
    /// signal the 2026-08-13 field regression needed (bughunt 2026-08-14).
    ///
    /// The classification half is cross-checked from `aggregate`'s side by
    /// `every_run_mode_label_a_runner_can_emit_is_classified`, which reads these
    /// functions instead of re-typing the label list — and PARSES this file for
    /// every `*_mode_label` definition, so a fourth producer, or a fourth arm on
    /// one of these three, is RED there until it is classified. (It asked two of
    /// the three until round 6: this guard learned about `run_mode_label` in
    /// round 5 and its sibling did not, which is why that side is now derived
    /// rather than named.)
    ///
    /// RED against the flag-derived strings: peak 1 then reads
    /// `wave-parallel-processes`, a serial pool reads `pool`, and a serialized
    /// top-level run reads `parallel-threads` / `parallel-processes`.
    #[test]
    fn run_mode_labels_report_the_concurrency_that_happened() {
        use super::{pool_is_concurrent, pool_mode_label, run_mode_label, wave_mode_label};
        assert_eq!(wave_mode_label(3), "wave-parallel-processes");
        assert_eq!(
            wave_mode_label(1),
            "wave-sequential",
            "a `--parallel` run the cost gate serialized did not share the source"
        );
        assert_eq!(
            wave_mode_label(0),
            "wave-sequential",
            "the non-parallel wave path never raises the peak"
        );
        assert_eq!(pool_mode_label(true), "pool");
        assert_eq!(
            pool_mode_label(false),
            "pool-serial",
            "a pool that never overlapped must not be labelled a pool run"
        );
        // The ONE predicate all three pool surfaces answer to.
        assert!(pool_is_concurrent(5, 12, 0) && pool_is_concurrent(2, 2, 0));
        assert!(!pool_is_concurrent(5, 1, 0), "one export cannot overlap");
        assert!(!pool_is_concurrent(1, 8, 0), "one slot cannot overlap");
        assert!(!pool_is_concurrent(4, 0, 0));
        assert!(
            !pool_is_concurrent(5, 0, 8),
            "heavies serialize against each other, so an all-heavy queue never \
             overlaps however many slots were asked for"
        );
        assert!(
            pool_is_concurrent(5, 1, 7),
            "one parallel_safe export rides alongside the running heavy"
        );
        // The top-level runner's two paths, same rule.
        assert_eq!(run_mode_label(4, true), "parallel-processes");
        assert_eq!(run_mode_label(4, false), "parallel-threads");
        for processes in [true, false] {
            assert_eq!(
                run_mode_label(1, processes),
                "sequential",
                "a top-level run that overlapped nothing must keep the actionable \
                 attribution, whichever path asked for concurrency"
            );
            assert_eq!(run_mode_label(0, processes), "sequential");
        }
    }

    /// The wave runner's label is a PER-EXPORT fact, not a run-level one: its
    /// cost-gate batches run strictly one after another, so an export in a
    /// single-child batch overlapped nothing however wide the run's widest
    /// batch was.
    ///
    /// Staged from the REAL producers — `cost_gate_batches` is the function
    /// `run_waves` calls to shape the run, and `wave_batch_modes` the one whose
    /// output the tail hands the self-check. The test supplies exports, not
    /// batches, and does not re-implement the batching rule (the
    /// correct-logic-on-a-fabricated-input class).
    ///
    /// RED against the shipped run-wide label (`RunModes::for_export` returning
    /// `self.run`, or `wave_batch_modes` labelling from the peak): `orders`,
    /// which ran alone, then reads `wave-parallel-processes` and
    /// `aggregate::mode_shares_the_source` deletes its actionable pointer.
    ///
    /// HONESTY: this pins the PRODUCERS and the resolver, not `run_waves`'
    /// call to them — that tail spawns child processes against a real source,
    /// so no unit test can enter it. What no test here can catch is the tail
    /// dropping the `export_modes` accumulation and passing
    /// `RunModes::uniform` again.
    #[test]
    fn a_wave_export_is_labelled_by_the_batch_it_ran_in_not_the_runs_widest() {
        use super::{RunModes, cost_gate_batches, wave_batch_modes, wave_mode_label};
        use crate::config::{ExportConfig, sample_export};
        let safe = |n: &str| {
            let mut e = sample_export(n);
            e.parallel_safe = Some(true);
            e
        };
        // Two heavies (the default shape of a hand-written config) and a
        // three-wide safe batch — ≥2 lone exports and ≥2 safe ones so neither
        // side is a one-element fold.
        let exports = [
            sample_export("orders"),
            sample_export("events"),
            safe("dim_a"),
            safe("dim_b"),
            safe("dim_c"),
        ];
        let pending: Vec<&ExportConfig> = exports.iter().collect();
        let batches = cost_gate_batches(&pending);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 1, 3],
            "the cost gate emits one single-child batch per heavy export plus \
             one concurrent batch for the parallel_safe ones"
        );
        // The run really did reach a 3-wide batch, so the run-level label is
        // the concurrent one — the case where the bug is invisible.
        let peak = batches.iter().map(Vec::len).max().unwrap();
        assert_eq!(wave_mode_label(peak), "wave-parallel-processes");

        let modes: std::collections::HashMap<String, &'static str> =
            wave_batch_modes(&batches).into_iter().collect();
        assert_eq!(modes.len(), exports.len(), "every export gets a label");
        for lone in ["orders", "events"] {
            assert_eq!(
                modes[lone], "wave-sequential",
                "'{lone}' ran in a single-child batch, and batches run one after \
                 another — it shared the source with nothing"
            );
        }
        for batched in ["dim_a", "dim_b", "dim_c"] {
            assert_eq!(
                modes[batched], "wave-parallel-processes",
                "'{batched}' really did run alongside its two siblings"
            );
        }

        // …and the resolver the tail actually passes to the self-check.
        let resolver = RunModes::per_export(wave_mode_label(peak), modes);
        assert_eq!(resolver.for_export("orders"), "wave-sequential");
        assert_eq!(resolver.for_export("dim_b"), "wave-parallel-processes");
        assert_eq!(
            resolver.for_export("an-export-no-batch-covered"),
            "wave-parallel-processes",
            "an export the map never saw falls back to the RUN label — hedged \
             text on a serial export is harmless, a false 'check governor \
             sheds' on a concurrent one is not"
        );
    }

    /// The pool's label is a PER-EXPORT fact too, and for a reason its own
    /// run-level predicate already documents: `next_eligible` never co-runs two
    /// heavies, so ONE `parallel_safe` export riding alongside the first heavy
    /// makes `pool_is_concurrent(5, 1, 7)` true for the RUN while the six
    /// heavies left after the safe work drains each run strictly ALONE.
    ///
    /// The run-wide label then tells those six "this run ran pool, where exports
    /// share the source and per-export rows/s falls BY DESIGN — compare the
    /// run's makespan", deleting the actionable pointer on six real regressions.
    /// Verbatim the harm the wave half of this fix removed one runner over.
    ///
    /// Staged from the shape `run_pool` really produces: the windows below are
    /// the `(export, start_ms, end_ms)` triples its workers stamp around
    /// `run_export_job`, and the label comes back from `pool_export_modes`, the
    /// function the tail hands the self-check. ≥2 overlapping and ≥2 solo so
    /// neither side is a one-element fold.
    ///
    /// RED against the shipped `RunModes::uniform(&agg.parallel_mode)`: every
    /// export then reads `pool`, so the six trailing heavies lose the pointer.
    /// RED too against a `pool_export_modes` that labels from the RUN's
    /// concurrency (`pool_is_concurrent`) rather than from the windows.
    ///
    /// HONESTY: this pins the PRODUCER and the resolver plus the tail's SOURCE
    /// wiring (the last assertions) — not the values `run_pool` stamps, which
    /// need a live source, a state DB and a destination. The emission half is
    /// `a_lone_pool_export_keeps_the_actionable_attribution_in_a_concurrent_run`.
    #[test]
    fn a_pool_export_is_labelled_by_the_overlap_it_measured_not_the_runs_widest() {
        use super::{RunModes, pool_export_modes, pool_is_concurrent, pool_mode_label};
        // The run-level truth first: one safe export IS enough to make the pool
        // concurrent, which is exactly why the run-wide label hides the bug.
        assert!(
            pool_is_concurrent(5, 1, 7),
            "one parallel_safe export rides alongside the running heavy"
        );
        // 1 safe + 7 heavy through 5 slots: the safe export backfills the slot
        // beside the first heavy; the remaining heavies serialize behind it.
        let mut windows: Vec<(String, i64, i64)> =
            vec![("dim_small".into(), 0, 50), ("orders".into(), 0, 100)];
        for (i, start) in (100..700).step_by(100).enumerate() {
            windows.push((format!("heavy_{i}"), start, start + 100));
        }
        let modes: std::collections::HashMap<String, &'static str> =
            pool_export_modes(&windows).into_iter().collect();
        assert_eq!(modes.len(), windows.len(), "every export gets a label");
        for overlapped in ["dim_small", "orders"] {
            assert_eq!(
                modes[overlapped],
                pool_mode_label(true),
                "'{overlapped}' really did share the window with a sibling"
            );
        }
        for solo in (0..6).map(|i| format!("heavy_{i}")) {
            assert_eq!(
                modes[&solo], "pool-serial",
                "'{solo}' ran after the safe work drained, alone — it must keep \
                 the actionable pointer however concurrent the RUN was"
            );
        }
        // Touching windows are not overlapping ones: `heavy_0` ends at 600 and
        // `heavy_5` starts at 600 — a half-open interval, or every serialized
        // pool reads as concurrent.
        assert_eq!(
            pool_export_modes(&[("a".into(), 0, 100), ("b".into(), 100, 200)])
                .into_iter()
                .map(|(_, m)| m)
                .collect::<Vec<_>>(),
            vec!["pool-serial", "pool-serial"],
            "back-to-back exports did not overlap"
        );

        // …and the resolver the tail passes to the self-check.
        let resolver = RunModes::per_export(pool_mode_label(true), modes);
        assert_eq!(resolver.for_export("heavy_3"), "pool-serial");
        assert_eq!(resolver.for_export("orders"), "pool");
        assert_eq!(
            resolver.for_export("an-export-no-window-covered"),
            "pool",
            "an export the map never saw falls back to the RUN label — the same \
             fail-safe direction the wave twin uses"
        );

        // The tail's WIRING, since its values are live-only: `run_pool` must
        // hand the self-check the measured per-export map, not a uniform label.
        let whole = include_str!("run.rs");
        let code: String = whole[..whole.find("\n#[cfg(test)]").expect("test modules")]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let pool_body = &code[code
            .find("\npub(crate) fn run_pool(")
            .expect("run_pool's signature moved — update the anchor")..];
        assert!(
            pool_body.contains("pool_export_modes(&pool_windows)")
                && pool_body.contains("RunModes::per_export("),
            "the pool tail must attribute per export from the windows it \
             measured; `RunModes::uniform` here is the defect"
        );
    }

    /// `apply --parallel` that serialises the whole run must SAY so, at a level
    /// the operator sees.
    ///
    /// `parallel_safe` is unset in any hand-written config, so `is_parallel_safe`
    /// reads false for every export, the cost gate emits one single-child batch
    /// each, and the wave loop runs them strictly one after another. The only
    /// thing rivet said about it was an `info` line — and `main.rs` builds its
    /// logger with `default_filter_or("warn")`, so it reached nobody. Meanwhile
    /// the sibling `--pool` path `println!`s its schedule: two runners, one
    /// fact, opposite visibility.
    ///
    /// Staged from the REAL producer: the widths come from `cost_gate_batches`
    /// over exports built through the config deserializer, not from a hand-typed
    /// batch shape.
    ///
    /// RED against returning `None` unconditionally (the shipped silence) and
    /// against firing when the gate DID batch something (the noise failure — a
    /// warning on every healthy wave is how a real one gets ignored).
    #[test]
    fn a_degenerate_parallel_apply_warns_instead_of_serialising_in_silence() {
        use super::{cost_gate_batches, degenerate_parallel_warning};
        use crate::config::{ExportConfig, sample_export};
        let widths = |exports: &[ExportConfig]| -> Vec<usize> {
            let refs: Vec<&ExportConfig> = exports.iter().collect();
            cost_gate_batches(&refs).iter().map(Vec::len).collect()
        };
        let safe = |n: &str| {
            let mut e = sample_export(n);
            e.parallel_safe = Some(true);
            e
        };

        // The hand-written config: ≥2 exports, none annotated → all lone.
        let hand_written = [
            sample_export("orders"),
            sample_export("events"),
            sample_export("users"),
        ];
        let w = widths(&hand_written);
        assert_eq!(w, vec![1, 1, 1], "the cost gate serialised the whole wave");
        let line = degenerate_parallel_warning("1", &w)
            .expect("a `--parallel` run with no concurrency must say so");
        assert!(
            line.contains("no concurrency") && line.contains("3 export(s)"),
            "the warning must name the symptom: {line}"
        );
        assert!(
            line.contains("parallel_safe: true") && line.contains("rivet plan"),
            "…and the lever, or it is a complaint rather than a diagnostic: {line}"
        );

        // A wave the gate really did batch: no warning (this is the ordinary
        // case, and it keeps its `info` line).
        let mixed = [sample_export("orders"), safe("dim_a"), safe("dim_b")];
        assert_eq!(widths(&mixed), vec![1, 2]);
        assert_eq!(degenerate_parallel_warning("1", &widths(&mixed)), None);
        // One export cannot overlap anything — not a config problem.
        assert_eq!(
            degenerate_parallel_warning("1", &widths(&[safe("only")])),
            None
        );
        assert_eq!(degenerate_parallel_warning("1", &[]), None);

        // The EMISSION, since `run_waves` needs a live source: at WARN (an
        // `info` here is functionally silent), and BEFORE the batches execute.
        let whole = include_str!("run.rs");
        let code: String = whole[..whole.find("\n#[cfg(test)]").expect("test modules")]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        // Inside `run_waves`, not at the definition (which is a later `fn`).
        let waves = &code[code
            .find("\npub(crate) fn run_waves(")
            .expect("run_waves' signature moved — update the anchor")..];
        let at = waves
            .find("degenerate_parallel_warning(")
            .expect("run_waves must call the diagnostic");
        let statement = &waves[at.saturating_sub(200)..waves.len().min(at + 400)];
        assert!(
            statement.contains("log::warn!"),
            "the diagnostic must be emitted at WARN — the default log filter is \
             `warn`, so an `info` line about a run being slow reaches nobody:\n\
             {statement}"
        );
        let run_batches = waves
            .find("for batch in &batches {")
            .expect("run_waves runs its batches in a loop");
        assert!(
            at < run_batches,
            "the warning must print BEFORE the wave's batches execute — a \
             diagnostic that lands after the slow run has already happened is a \
             post-mortem, not a lever"
        );
    }

    /// The peak a pool can reach is capped by BOTH the slots and the shape of
    /// the queue: [`next_eligible`] never co-runs two heavies, so heavies
    /// contribute at most one to the overlap.
    ///
    /// `slots.min(pending)` — the round-4 shape — reads `--pool 5` over eight
    /// heavies as a 5-wide pool and hands `mode_shares_the_source` the string
    /// that DELETES the self-check's actionable tail, on a run that overlapped
    /// nothing. RED against it on the all-heavy rows below.
    #[test]
    fn pool_peak_counts_the_heavy_serialization_not_just_the_slots() {
        use super::{next_eligible, pool_peak_concurrency};
        // Slot-bound and queue-bound, no heavies: the old rule's cases.
        assert_eq!(pool_peak_concurrency(5, 12, 0), 5);
        assert_eq!(pool_peak_concurrency(5, 3, 0), 3);
        assert_eq!(pool_peak_concurrency(1, 8, 0), 1);
        assert_eq!(pool_peak_concurrency(5, 0, 0), 0);
        // Heavy-bound: N heavies buy exactly ONE slot of overlap between them.
        assert_eq!(
            pool_peak_concurrency(5, 0, 8),
            1,
            "eight heavies through five slots still run one at a time"
        );
        assert_eq!(pool_peak_concurrency(5, 2, 6), 3);
        assert_eq!(
            pool_peak_concurrency(2, 9, 4),
            2,
            "the slot cap still binds when the queue could go wider"
        );
        // …and that "exactly one" is [`next_eligible`]'s rule, not a guess:
        // with a heavy running, an all-heavy queue yields NO pick, so no second
        // worker can start (the sleep-and-retry arm), while a queue holding one
        // safe export yields it.
        assert_eq!(next_eligible(&[false, false, false], true), None);
        assert_eq!(next_eligible(&[false, true, false], true), Some(1));
        assert_eq!(next_eligible(&[false, false], false), Some(0));
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

    /// A runner may not spell its own concurrency claim as a LITERAL: the harm
    /// frame and the mode label are decided by the pure functions above, from
    /// what OVERLAPPED, and each claim-string lives in exactly ONE of them.
    ///
    /// HONESTY: this pins the SEAM, not the values — `run_waves` and `run_pool`
    /// both need a live source, a state DB and a destination, so no unit test in
    /// this module can observe what they hand their aggregate or their bracket.
    /// That is precisely where the defect lived: `wave_mode_label`'s rule (tested
    /// above) was already right in `wave_harm_window`'s shape, and the CALLER
    /// passed the flag instead of the measurement. The live oracles are
    /// `apply --parallel` / `apply --pool` runs against a real source.
    ///
    /// FOUR producers, not two. Round 4 extracted the wave's and the pool's
    /// deciders and this guard grew a needle per label — but `run` itself has
    /// two more paths (child processes, in-process threads + the sequential
    /// loop) and both still spelled their claim inline: `"parallel-processes"`
    /// straight into `aggregate::build`, and `if run_parallel {
    /// "parallel-threads" } else { "sequential" }` at the tail. A guard named
    /// "a runner never spells its concurrency claim as a literal" that
    /// enumerates half the runners grades only what its author already knew
    /// (round-5 bughunt) — so the list below covers every producer, and
    /// [`run_mode_label`] owns the three top-level strings.
    ///
    /// RED against the pre-fix code, each of which puts a SECOND occurrence of
    /// its needle in the product half: `if parallel { "wave-parallel-processes" }
    /// else { "wave-sequential" }` at the wave aggregate, `aggregate::build(..,
    /// "pool")` at the pool's, the unconditional `HarmWindow::Pool { exports:
    /// effective.len(), slots: m }` at the pool's bracket close, and the two
    /// top-level literals above.
    #[test]
    fn a_runner_never_spells_its_concurrency_claim_as_a_literal() {
        let whole = include_str!("run.rs");
        let src = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        // CODE only: a doc comment naming a label is not a claim.
        let code: String = src
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        for (needle, owner) in [
            ("\"wave-parallel-processes\"", "wave_mode_label"),
            ("\"wave-sequential\"", "wave_mode_label"),
            ("\"pool\"", "pool_mode_label"),
            ("\"pool-serial\"", "pool_mode_label"),
            // The top-level runner's three. The leading quote is what keeps
            // `"sequential"` from matching `"wave-sequential"`.
            ("\"parallel-processes\"", "run_mode_label"),
            ("\"parallel-threads\"", "run_mode_label"),
            ("\"sequential\"", "run_mode_label"),
        ] {
            assert_eq!(
                code.matches(needle).count(),
                1,
                "{needle} must appear ONCE, inside {owner} — a second occurrence is a \
                 runner claiming a concurrency it did not measure"
            );
        }
        // …and each runner hands the decider the value it MEASURED. The rule
        // being right is not the fix; the input being the measurement is.
        for call in [
            "wave_mode_label(peak_concurrency)",
            "pool_mode_label(really_concurrent)",
            // The pool's peak is capped by the heavy serialization as well as
            // by the slots, so both surfaces are fed the safe/heavy split — not
            // `pending.len()`, which called an all-heavy queue a 5-wide pool.
            "pool_harm_window(m, safe_pending, heavy_pending)",
            "pool_is_concurrent(m, safe_pending, heavy_pending)",
            // `run`'s two: the child-process aggregate counts the children it
            // spawned, the tail counts the threads that overlapped (1 on the
            // sequential loop).
            "run_mode_label(exports.len(), true)",
            "run_mode_label(peak_concurrency, false)",
        ] {
            assert!(
                code.contains(call),
                "expected the runner to decide from what happened: `{call}`"
            );
        }
        // `run_pool` may not name a window VARIANT at all — the shipped close
        // constructed `HarmWindow::Pool` inline from `--pool m` and the export
        // count, which is the finding. Everything after its signature in the
        // product half is `run_pool` plus small pure helpers that carry no
        // window, so the absence is checkable directly (the wave side cannot be
        // sliced this way: the deciders themselves live between the two
        // signatures, which is why its needle above is the label literal).
        let pool_body = &code[code
            .find("\npub(crate) fn run_pool(")
            .expect("run_pool's signature moved — update the anchor")..];
        assert!(
            !pool_body.contains("HarmWindow::"),
            "the pool must route its harm frame through `pool_harm_window`, not \
             construct a window from the invocation"
        );
    }

    /// The run's "this wall is a LOWER BOUND" claim has exactly ONE source, and
    /// it is the pure function fed the RECONCILED classification.
    ///
    /// The split block used to make the same claim from the pre-reconcile SEED,
    /// so a steady-state split (giant frozen at a failed attempt; every
    /// `{giant}#i` measured from run 1) printed the LOWER BOUND warn at start
    /// and "N measured, 0 estimated" — with the hedge suppressed — 130 lines
    /// later. One run, two contradictory honesty claims about the same exports.
    ///
    /// RED against restoring the `unit_from`-derived `wall_hedge` (the needle
    /// count reads 2), and against a hedge that fires on a fully measured
    /// schedule (`lower_bound_hedge(0, 0)` then returns `Some`).
    #[test]
    fn the_lower_bound_claim_has_one_source_and_reads_the_reconciled_counts() {
        use super::lower_bound_hedge;
        assert!(
            lower_bound_hedge(0, 0).is_none(),
            "a schedule resting entirely on successes is not a lower bound"
        );
        // ≥2 of each so the fold is a real fold and the two counts cannot be
        // swapped without the assert noticing.
        let hedge = lower_bound_hedge(2, 3).expect("5 unmeasured exports must hedge");
        assert!(
            hedge.contains("5 export(s)")
                && hedge.contains("2 scheduled at a failed attempt")
                && hedge.contains("3 at a"),
            "the hedge must count both flavours of unmeasured: {hedge}"
        );
        // One claim in the product half, and it lives in the pure function —
        // not in the split block, whose only input is the first-run seed.
        let whole = include_str!("run.rs");
        let src = &whole[..whole
            .find("\n#[cfg(test)]")
            .expect("run.rs has test modules")];
        let code: String = src
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let needle = concat!("LOWER ", "BOUND");
        assert_eq!(
            code.matches(needle).count(),
            1,
            "the run must publish ONE honesty claim about its wall"
        );
        let at = code
            .find(concat!("fn lower_bound", "_hedge"))
            .expect("the pure hedge exists");
        let until = code
            .find("\npub(crate) fn run_pool(")
            .expect("run_pool's signature moved — update the anchor");
        assert!(
            code[at..until].contains(needle),
            "the claim must be made by the function fed the reconciled counts"
        );
    }
}

#[cfg(test)]
mod run_tail_tests {
    use super::{
        RunModes, fold_failures, owns_throughput_self_check, pool_safe_heavy_split,
        reports_run_aggregate, self_check_throughput_as, snapshot_then_stamp, tail_plan,
    };
    use crate::config::ExportConfig;

    /// `fold_failures` is the shared representative-failure fold the three
    /// orchestrator tails route through (arch-roast 2026-08-21). Its whole-body
    /// stub `Ok(())` survived the in-diff mutation gate — a run that FAILED
    /// would then exit 0 — because every caller is a live-only tail. It is
    /// pure, so it is graded here directly.
    ///
    /// Three cases, and the typed-marker downcast is the load-bearing one:
    /// `error::classify_exit` reads the marker THROUGH anyhow's context chain,
    /// so a multi-failure fold must keep the representative error underneath
    /// its summary context or a data-integrity batch would exit 1 instead of 3.
    #[test]
    fn fold_failures_returns_ok_only_when_empty_and_keeps_the_typed_marker() {
        use crate::error::DataIntegrityError;

        // No failures → Ok. (The stub mutant makes EVERY case look like this.)
        assert!(fold_failures(Vec::new(), " in the pool").is_ok());

        // One failure → returned verbatim, marker intact.
        let one = fold_failures(
            vec![DataIntegrityError::new("rows differ").into()],
            " across waves",
        )
        .expect_err("a failure must not fold to Ok");
        assert_eq!(
            crate::error::classify_exit(&one),
            3,
            "a single data-integrity failure must still exit 3"
        );

        // Several → the SCARIEST class is representative and its marker must
        // survive the added context; the others are named in the message.
        let many = fold_failures(
            vec![
                anyhow::anyhow!("plain boom"),
                DataIntegrityError::new("rows differ").into(),
            ],
            " in the pool",
        )
        .expect_err("failures must not fold to Ok");
        assert_eq!(
            crate::error::classify_exit(&many),
            3,
            "the data-integrity marker must survive under the summary context, \
             or a mixed batch exits on the wrong reason"
        );
        let msg = format!("{many:#}");
        assert!(
            msg.contains("2 export(s) failed in the pool"),
            "the fold must count every failure and carry the caller's context; got: {msg}"
        );
        assert!(
            msg.contains("plain boom"),
            "the non-representative failures must still be listed; got: {msg}"
        );
    }

    /// Captures WARN records, because the run-over-run self-check has no other
    /// observable: it reads the state DB and LOGS. Installed once per test
    /// binary and shared by every test in it, so a reader must filter by its own
    /// unique export name rather than trusting the buffer to be its own.
    struct WarnCapture;
    static WARN_LINES: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    static WARN_CAPTURE: WarnCapture = WarnCapture;

    impl log::Log for WarnCapture {
        fn enabled(&self, m: &log::Metadata) -> bool {
            m.level() <= log::Level::Warn
        }
        fn log(&self, r: &log::Record) {
            if r.level() <= log::Level::Warn {
                WARN_LINES
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(r.args().to_string());
            }
        }
        fn flush(&self) {}
    }

    fn install_warn_capture() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = log::set_logger(&WARN_CAPTURE);
            log::set_max_level(log::LevelFilter::Warn);
        });
    }

    /// The aggregate a run of `n` exports leaves its tail, built by the REAL
    /// producer (`aggregate::build`) over the per-export entries — so the gate
    /// below is graded against the object the product passes it, not against a
    /// number a test invented.
    fn aggregate_of(n: usize) -> crate::state::RunAggregate {
        let entries: Vec<crate::state::RunAggregateEntry> = (0..n)
            .map(|i| crate::state::RunAggregateEntry {
                export_name: format!("export_{i}"),
                status: "success".to_string(),
                run_id: "this-run".to_string(),
                rows: 10,
                files: 1,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 5,
                mode: "full".to_string(),
                error_message: None,
            })
            .collect();
        let now = chrono::Utc::now();
        super::aggregate::build(
            entries,
            now,
            now,
            Some("cfg.yaml"),
            super::run_mode_label(n, false),
        )
    }

    fn captured_warnings_mentioning(needle: &str) -> Vec<String> {
        WARN_LINES
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|l| l.contains(needle))
            .cloned()
            .collect()
    }

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
            tail_plan(&aggregate_of(0), true).machine_output,
            "a zero-export run must still write --summary-output / print --json"
        );
        assert!(
            !tail_plan(&aggregate_of(0), true).aggregate,
            "no card, no run_aggregate row"
        );
        // One export: unchanged — machine output from the tail, no card.
        assert!(tail_plan(&aggregate_of(1), true).machine_output);
        assert!(!tail_plan(&aggregate_of(1), true).aggregate);
        // Two or more: the aggregate path owns both the card and the file
        // (through `persist`), so the tail must NOT write it a second time.
        assert!(tail_plan(&aggregate_of(2), true).aggregate);
        assert!(
            !tail_plan(&aggregate_of(2), true).machine_output,
            "the aggregate path writes --summary-output via persist; a second \
             write here would race it"
        );
        // Nobody asked for machine output → none, at every count.
        assert!(!tail_plan(&aggregate_of(0), false).machine_output);
        assert!(!tail_plan(&aggregate_of(1), false).machine_output);
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

    /// …and the MARKER the deferral keys on must be one only a real parent can
    /// set — `RIVET_IPC_EVENTS` alone must never silence the check.
    ///
    /// That variable is documented to users (`docs/reference/cli.md`) as the way
    /// to get the NDJSON progress stream, and it is a PRESENCE flag (`=0` is
    /// still on — pinned by `ipc_events_are_off_unless_the_variable_is_set_and_
    /// non_empty`). So an operator who exports it in an Airflow/K8s pod spec, or
    /// a wrapper that consumes child events, turned every plain `rivet run` into
    /// a process deferring its self-check to a parent that does not exist: the
    /// check ran NOWHERE, environment-dependent and silent (round-7 bughunt).
    ///
    /// The second marker (`ENV_PARENT_SELF_CHECK`) is internal and set only at
    /// the one `current_exe` spawn site — asserted here on the SPAWN SITE's own
    /// source, since a unit test cannot fork the binary and read the child's env.
    ///
    /// RED against `is_reexecd_child` reading either marker alone (`||`, or
    /// ignoring the parent flag): the first assertion fails.
    #[test]
    fn the_documented_ipc_variable_alone_does_not_silence_the_self_check() {
        use super::is_reexecd_child;
        assert!(
            !is_reexecd_child(true, false),
            "`RIVET_IPC_EVENTS` is a documented user-facing switch — a process \
             that merely emits NDJSON progress has no parent to defer to, and \
             deferring there is how the check goes silent everywhere"
        );
        assert!(
            !is_reexecd_child(false, true),
            "the internal marker without the IPC stream is not a child either"
        );
        assert!(
            is_reexecd_child(true, true),
            "a real re-exec'd child carries BOTH markers and must defer"
        );
        assert!(
            !is_reexecd_child(false, false),
            "a plain run owns its check"
        );
        // …and only a real parent sets the internal one: the single
        // `current_exe` spawn site sets it on the same command as the IPC flag.
        let spawner = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/pipeline/parallel_children.rs"),
        )
        .expect("read the spawn site");
        let code = pipeline_product_code(&spawner);
        assert_eq!(
            code.matches(".env(ENV_IPC_EVENTS").count(),
            code.matches("ENV_PARENT_SELF_CHECK").count(),
            "every child that gets the IPC flag must also get the parent's \
             self-check claim, or the child reports a regression into a captured \
             stderr file while the parent reports it too"
        );
        assert!(
            code.contains("ENV_PARENT_SELF_CHECK"),
            "the spawn site must claim the child's self-check"
        );
    }

    /// The same contract one layer up, at the EMISSION — because the test above
    /// grades a correct rule and the seam that consumes it is where the polarity
    /// actually lives (`if !owns_throughput_self_check(..)`). A dropped `!`
    /// there leaves both halves of this module's evidence green: the predicate
    /// still answers correctly, every tail still calls the seam, and the run
    /// self-reports nothing while a captured child talks to a log file nobody
    /// reads.
    ///
    /// Staged from the REAL producer: the baseline comes back through
    /// `StateStore::get_last_success_metric_excluding` over a row this test
    /// recorded with the state store's own recorder, not from a hand-built
    /// `ThroughputPair` fed to the pure comparator.
    ///
    /// RED against `delete !` at the seam — both assertions invert (the child
    /// emits the line, the parent emits none).
    #[test]
    fn a_reexecd_child_stays_silent_while_a_parent_emits_the_regression() {
        install_warn_capture();
        // Unique to this test: the capture buffer is shared by the whole binary.
        let export = "run_tail_selfcheck_probe_export";
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        // 20 000 rows in 5 s = 4 000 rows/s, the previous SUCCESS.
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: export.to_string(),
                run_id: "baseline-run".to_string(),
                duration_ms: 5_000,
                total_rows: 20_000,
                status: "success".to_string(),
                mode: Some("full".to_string()),
                ..Default::default()
            })
            .expect("record the baseline success");
        // This run: the same rows in 20 s = 1 000 rows/s — 4× slower, past the
        // 1.5× ratio, and comparable in shape (same mode, same scale, both
        // sides past the min-rows / min-duration floors).
        let entries = vec![crate::state::RunAggregateEntry {
            export_name: export.to_string(),
            status: "success".to_string(),
            run_id: "this-run".to_string(),
            rows: 20_000,
            files: 1,
            bytes: 0,
            bytes_read: 0,
            duration_ms: 20_000,
            mode: "full".to_string(),
            error_message: None,
        }];

        // A re-exec'd child (`--parallel-export-processes`): its parent rebuilds
        // this very row and would print the identical line, so it must not.
        self_check_throughput_as(&state, &entries, &RunModes::uniform("sequential"), true);
        assert!(
            captured_warnings_mentioning(export).is_empty(),
            "a re-exec'd child must emit nothing — its stderr is captured to a \
             file and its parent reports the same export: {:?}",
            captured_warnings_mentioning(export)
        );

        // The top-level run: it owns the report.
        self_check_throughput_as(&state, &entries, &RunModes::uniform("sequential"), false);
        let lines = captured_warnings_mentioning(export);
        assert_eq!(
            lines.len(),
            1,
            "a top-level run must report its own regression exactly once (0 \
             lines also means the WARN capture never installed): {lines:?}"
        );
        assert!(
            lines[0].contains("slower than its last success"),
            "the line must be the run-over-run self-check: {}",
            lines[0]
        );

        // …and the WRAPPER every orchestrator tail actually calls must delegate
        // to it. A stubbed seam is the 2026-08-14 bug itself (the check nowhere
        // at all) and every other assertion in this module survives it.
        // The markers are process-global, so this READS the ambient env rather
        // than setting one. The condition is the REQUIREMENT, not the product's
        // predicate restated: with no parent claiming this process
        // (`ENV_PARENT_SELF_CHECK` unset) the seam MUST report, whatever
        // `RIVET_IPC_EVENTS` says — asking `is_reexecd_child` here instead would
        // let the mutant that ignores the parent marker silence BOTH the seam
        // and the assertion, and the test would stay green under an operator's
        // documented `RIVET_IPC_EVENTS=1` (self-oracle).
        if std::env::var_os(super::ENV_PARENT_SELF_CHECK).is_none() {
            super::self_check_throughput(&state, &entries, &RunModes::uniform("sequential"));
            assert_eq!(
                captured_warnings_mentioning(export).len(),
                2,
                "the seam every tail calls must reach the same report"
            );
        }
    }

    /// The wave runner's per-export label, at the EMISSION: two exports in one
    /// `apply --parallel` run, one of which ran strictly alone (its own
    /// single-child batch) while the other rode a 3-wide batch. The lone one
    /// must keep the ACTIONABLE tail; the batched one gets the by-design
    /// framing it has earned.
    ///
    /// This is the harm the round-6 bughunt measured: the run-wide label put
    /// "exports share the source and per-export rows/s falls BY DESIGN" on an
    /// export nothing overlapped, and deleted the only pointer at the governor
    /// shed / adaptive batch shrink / source load — on the exact signal the
    /// 2026-08-13 field regression needed.
    ///
    /// RED against `RunModes::for_export` ignoring its map (the shipped
    /// run-wide label): the first assertion fails with the lone export's line
    /// carrying "BY DESIGN".
    #[test]
    fn a_lone_wave_export_keeps_the_actionable_attribution_in_a_wide_run() {
        install_warn_capture();
        let lone = "run_tail_wave_lone_probe_export";
        let batched = "run_tail_wave_batched_probe_export";
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut entries = Vec::new();
        for name in [lone, batched] {
            // 20 000 rows in 5 s = 4 000 rows/s, the previous SUCCESS.
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: "baseline-run".to_string(),
                    duration_ms: 5_000,
                    total_rows: 20_000,
                    status: "success".to_string(),
                    mode: Some("full".to_string()),
                    ..Default::default()
                })
                .expect("record the baseline success");
            // This run: the same rows in 20 s = 4× slower, past the ratio.
            entries.push(crate::state::RunAggregateEntry {
                export_name: name.to_string(),
                status: "success".to_string(),
                run_id: "this-run".to_string(),
                rows: 20_000,
                files: 1,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 20_000,
                mode: "full".to_string(),
                error_message: None,
            });
        }
        // The run reached a 3-wide batch, so its RUN-level label is the
        // concurrent one — the shape that hid the bug.
        let mut modes: std::collections::HashMap<String, &'static str> =
            std::collections::HashMap::new();
        modes.insert(lone.to_string(), super::wave_mode_label(1));
        modes.insert(batched.to_string(), super::wave_mode_label(3));
        self_check_throughput_as(
            &state,
            &entries,
            &RunModes::per_export(super::wave_mode_label(3), modes),
            false,
        );

        let lone_lines = captured_warnings_mentioning(lone);
        assert_eq!(lone_lines.len(), 1, "one line per export: {lone_lines:?}");
        assert!(
            lone_lines[0].contains("check governor sheds"),
            "an export that ran in its own single-child batch overlapped \
             nothing — it must keep the actionable pointer: {}",
            lone_lines[0]
        );
        let batched_lines = captured_warnings_mentioning(batched);
        assert_eq!(
            batched_lines.len(),
            1,
            "one line per export: {batched_lines:?}"
        );
        assert!(
            batched_lines[0].contains("share the source")
                && batched_lines[0].contains("wave-parallel-processes"),
            "an export that really did run alongside siblings names its mode: {}",
            batched_lines[0]
        );
    }

    /// The POOL's per-export label, at the EMISSION — the twin of the wave test
    /// above, on the runner whose run-level label is TRUE and still wrong for
    /// most of its exports.
    ///
    /// `--pool 5` over one `parallel_safe` export and seven heavies: the safe
    /// one rides alongside the first heavy (so `pool_is_concurrent` is true and
    /// the run records `pool`), and the six heavies after it each run strictly
    /// alone. The shipped `RunModes::uniform` told all seven "exports share the
    /// source and per-export rows/s falls BY DESIGN — compare the run's
    /// makespan", excusing six real regressions.
    ///
    /// Staged from the real producer: the modes come back from
    /// `pool_export_modes` over the windows `run_pool`'s workers stamp, and the
    /// baseline from `StateStore::get_last_success_metric_excluding` over a row
    /// this test recorded with the store's own recorder.
    ///
    /// RED against `RunModes::uniform(pool_mode_label(true))` (the shipped tail):
    /// the solo export's line carries "BY DESIGN" and the assertion fails.
    #[test]
    fn a_lone_pool_export_keeps_the_actionable_attribution_in_a_concurrent_run() {
        install_warn_capture();
        let solo = "run_tail_pool_solo_probe_export";
        let overlapped = "run_tail_pool_overlapped_probe_export";
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut entries = Vec::new();
        for name in [solo, overlapped] {
            // 20 000 rows in 5 s = 4 000 rows/s, the previous SUCCESS.
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: "baseline-run".to_string(),
                    duration_ms: 5_000,
                    total_rows: 20_000,
                    status: "success".to_string(),
                    mode: Some("full".to_string()),
                    ..Default::default()
                })
                .expect("record the baseline success");
            // This run: the same rows in 20 s = 4× slower, past the ratio.
            entries.push(crate::state::RunAggregateEntry {
                export_name: name.to_string(),
                status: "success".to_string(),
                run_id: "this-run".to_string(),
                rows: 20_000,
                files: 1,
                bytes: 0,
                bytes_read: 0,
                duration_ms: 20_000,
                mode: "full".to_string(),
                error_message: None,
            });
        }
        // The measured windows: a `parallel_safe` export overlapping the first
        // heavy (not asserted on here — it is what makes the RUN concurrent),
        // then the two exports under test, one solo and one overlapped.
        let windows = vec![
            ("a_parallel_safe_export".to_string(), 0, 5_000),
            (overlapped.to_string(), 0, 20_000),
            (solo.to_string(), 20_000, 40_000),
        ];
        let modes: std::collections::HashMap<String, &'static str> =
            super::pool_export_modes(&windows).into_iter().collect();
        self_check_throughput_as(
            &state,
            &entries,
            // The RUN label is the concurrent one — the shape that hid the bug.
            &RunModes::per_export(super::pool_mode_label(true), modes),
            false,
        );

        let solo_lines = captured_warnings_mentioning(solo);
        assert_eq!(solo_lines.len(), 1, "one line per export: {solo_lines:?}");
        assert!(
            solo_lines[0].contains("check governor sheds"),
            "a pool export whose window overlapped nothing must keep the \
             actionable pointer: {}",
            solo_lines[0]
        );
        let overlapped_lines = captured_warnings_mentioning(overlapped);
        assert_eq!(
            overlapped_lines.len(),
            1,
            "one line per export: {overlapped_lines:?}"
        );
        assert!(
            overlapped_lines[0].contains("share the source")
                && overlapped_lines[0].contains("pool"),
            "an export that really did share its window names its mode: {}",
            overlapped_lines[0]
        );
    }

    /// `apply --pool --split`'s notice promises the run's own predicted makespan
    /// "prints with the pool schedule"; the all-units-complete `--resume` path
    /// returns ABOVE that block, so the promise cannot be kept there. The early
    /// return has to retract it rather than leave the operator hunting for a
    /// line that never prints (the dangling forward reference round 3 fixed for
    /// the harm line).
    ///
    /// RED against one message for both cases (the shipped behaviour): the
    /// split assertion fails, since the plain line says nothing about the
    /// makespan.
    ///
    /// HONESTY: this pins the MESSAGE only. `run_pool`'s early return — and
    /// with it the `split_info.is_some()` argument that selects between these
    /// two strings — needs a live pool run over a real source, so a unit test
    /// cannot observe which one that call site asks for.
    #[test]
    fn the_all_complete_split_resume_return_retracts_the_makespan_promise() {
        let plain = super::nothing_to_run_message(false);
        let after_split = super::nothing_to_run_message(true);
        assert!(
            plain.starts_with("apply --pool: nothing to run"),
            "the plain case keeps its one-line message: {plain}"
        );
        assert!(
            !plain.contains("makespan"),
            "with no `--split` notice emitted there is no forward reference to \
             retract: {plain}"
        );
        assert!(
            after_split.contains("makespan") && after_split.contains("does not print"),
            "the `--split` notice pointed at the predicted-makespan line, and \
             this return is above it — say so: {after_split}"
        );
        assert!(
            after_split.contains("--resume"),
            "and name the way out, since the units are complete rather than \
             broken: {after_split}"
        );
    }

    /// The card + `run_aggregate` row are multi-export-only, and it is ONE rule
    /// for the three tails that gate it — `run` (through [`tail_plan`]),
    /// `run_waves` and `run_pool` each spelled it inline as `> 1` before, inside
    /// functions no unit test can enter.
    ///
    /// The boundary is the whole content: at 1 the per-export card already
    /// printed every number the aggregate would carry and the `run_aggregate`
    /// row would duplicate the export's own `export_metrics` row; at 2 the
    /// aggregate is the only thing that reports the run.
    ///
    /// RED against `>=` (a single-export `apply` grows a summary card and a
    /// duplicate row), `<` and `==` (a real multi-export run reports nothing).
    #[test]
    fn only_a_multi_export_run_reports_an_aggregate() {
        assert!(
            !reports_run_aggregate(&aggregate_of(0)),
            "a zero-export run has nothing to aggregate"
        );
        assert!(
            !reports_run_aggregate(&aggregate_of(1)),
            "one export: the per-export card already said it"
        );
        assert!(
            reports_run_aggregate(&aggregate_of(2)),
            "two exports: the aggregate reports"
        );
        assert!(reports_run_aggregate(&aggregate_of(9)));
        // The three tails ask the same question of the same rule.
        assert_eq!(
            tail_plan(&aggregate_of(1), false).aggregate,
            reports_run_aggregate(&aggregate_of(1))
        );
        assert_eq!(
            tail_plan(&aggregate_of(2), false).aggregate,
            reports_run_aggregate(&aggregate_of(2))
        );
    }

    /// The gate's SUBJECT, not just its predicate: the exports the run actually
    /// RAN — which is exactly what lands in the aggregate.
    ///
    /// Round 6 unified the rule and left the three tails disagreeing about what
    /// they asked it ABOUT: `run` passed the post-expansion export count,
    /// `run_pool` the post-skip pending count — both equal to the aggregate's own
    /// — while `run_waves` passed `config.exports.len()`, read BEFORE the
    /// per-wave `--resume` `_SUCCESS` skip. So a five-export config whose
    /// destinations were all already complete printed "Run summary (0 exports) …
    /// rows: 0" and PERSISTED a `run_aggregate` row for a run that did no work,
    /// while the same config under `--pool --resume` returned early and wrote
    /// nothing: one shared rule, opposite behaviour, and a history table an
    /// operator queries polluted with empty runs (round-7 bughunt).
    ///
    /// Half one is the value: the fully-skipped resume run's aggregate is the
    /// zero-entry one `aggregate::build` really produces there. Half two is the
    /// CALL SITE, derived — every call in the product half must pass the
    /// aggregate the tail just built, never a count it re-derived. The type now
    /// enforces that (a `usize` no longer compiles), and this keeps it true if
    /// someone reaches for `RunAggregate { total_exports: total, .. }`.
    ///
    /// RED against `run_waves`' pre-skip subject (restore `total`: the source
    /// half reads `reports_run_aggregate(total)`) and against `>= 1`.
    #[test]
    fn the_aggregate_gate_reads_the_exports_the_run_actually_ran() {
        // A `--resume` run of a five-export config with every destination
        // already `_SUCCESS`: every wave `continue`s, no summary is collected,
        // and the tail builds an aggregate over ZERO entries.
        let fully_skipped = aggregate_of(0);
        assert_eq!(fully_skipped.total_exports, 0);
        assert!(
            !reports_run_aggregate(&fully_skipped),
            "a run that ran nothing must print no card and write no \
             `run_aggregate` row — the pre-skip config count (5) is what said \
             otherwise"
        );
        // …and the same config when it really ran all five.
        assert!(reports_run_aggregate(&aggregate_of(5)));

        // The call sites, derived: no tail may re-derive the count.
        let whole = include_str!("run.rs");
        let code: String = whole[..whole.find("\n#[cfg(test)]").expect("test modules")]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let mut sites = 0;
        for (at, _) in code.match_indices(concat!("reports_run", "_aggregate(")) {
            if code[..at].trim_end().ends_with("fn") {
                continue; // the definition, not a call
            }
            let arg = code[at..]
                .split_once('(')
                .and_then(|(_, rest)| rest.split_once(')'))
                .map(|(a, _)| a.trim().to_string())
                .expect("a call has arguments");
            assert!(
                arg == "&agg" || arg == "agg",
                "the aggregate gate must be asked about the aggregate the tail \
                 built, not about a count re-derived at the call site (that is \
                 how `run_waves` came to ask about exports it had already \
                 skipped) — got `{arg}`"
            );
            sites += 1;
        }
        // Down from 5: the waves/pool tails now route through ONE
        // `finish_run_tail` (arch-roast 2026-08-21), so the gate's call sites
        // are that seam's two (print + persist) plus `tail_plan`'s one. Fewer
        // sites is the point — the invariant this test protects (ask about the
        // BUILT aggregate, never a re-derived count) now holds by construction
        // for every tail the seam serves.
        assert!(
            sites >= 3,
            "expected the gate at `finish_run_tail`'s two sites plus \
             `tail_plan` — found {sites}"
        );
        // The floor dropped 5→3 because waves/pool no longer gate inline — so
        // PIN that they still route through the shared tail, or an ungated
        // revert (rebuilding an aggregate inline, printing unconditionally)
        // would pass every remaining count (godsplit bughunt 2026-08-21, MED).
        for tail_fn in ["fn run_waves", "fn run_pool"] {
            let at = code.find(tail_fn).expect("orchestrator tail exists");
            let body = &code[at..code[at..]
                .find("\npub")
                .or_else(|| code[at..].find("\nfn "))
                .map(|o| at + o)
                .unwrap_or(code.len())];
            assert!(
                body.contains("finish_run_tail("),
                "{tail_fn} no longer routes through finish_run_tail — the \
                 shared-tail invariant (one aggregate, gated card, ungated \
                 self-check) is unenforced for it"
            );
        }
        // …and no tail may OVERWRITE the subject on its way to the gate, which
        // is the one way back to a re-derived count that still type-checks
        // (`agg.total_exports = total;` before the call). Reading it is the
        // gate's own job and happens in the two pure functions above; every
        // other mention in the product half is a write.
        for (at, _) in code.match_indices("total_exports") {
            let after = code[at + "total_exports".len()..].trim_start();
            assert!(
                !after.starts_with('=') || after.starts_with("=="),
                "run.rs ASSIGNS `total_exports` — a tail that overwrites the \
                 aggregate's own count re-derives the subject the type signature \
                 exists to fix, and it still type-checks: {}",
                code[at..].lines().next().unwrap_or("").trim()
            );
        }
    }

    /// The pool's `(safe, heavy)` census, which every concurrency claim is
    /// computed FROM — `pool_is_concurrent`, `pool_peak_concurrency`,
    /// `pool_harm_window`, `pool_mode_label`. All four are pure and tested; the
    /// counts they consume were derived inline inside `run_pool` (live-only),
    /// which is the fabricated-input class: correct decisions, ungraded input.
    ///
    /// Fixture past the threshold on purpose — ≥2 of each kind, plus the two
    /// homogeneous queues, because a 1+1 split cannot tell `len - heavy` from
    /// `len / heavy`, and an all-safe queue is where `/` divides by zero.
    ///
    /// RED against `-` → `+` (the all-heavy queue reports 2 safe exports and the
    /// pool claims a concurrent window it never had) and `-` → `/` (the all-safe
    /// queue panics on divide-by-zero).
    #[test]
    fn the_pool_census_counts_safe_and_heavy_exports_apart() {
        // Through the real deserializer, so `parallel_safe:` reaches the field
        // the way a config does — including the ABSENT case.
        let mk = |name: &str, safe: Option<bool>| -> ExportConfig {
            let line = safe
                .map(|s| format!("parallel_safe: {s}\n"))
                .unwrap_or_default();
            serde_yaml_ng::from_str(&format!(
                "name: {name}\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  \
                 type: local\n  path: /tmp\n{line}"
            ))
            .expect("parse test ExportConfig")
        };
        let mixed = [
            mk("s1", Some(true)),
            mk("h1", Some(false)),
            mk("s2", Some(true)),
            // No `parallel_safe:` at all — the hand-written-config default, and
            // heavy by that default.
            mk("h2", None),
            mk("h3", Some(false)),
        ];
        let refs: Vec<&ExportConfig> = mixed.iter().collect();
        assert_eq!(
            pool_safe_heavy_split(&refs),
            (2, 3),
            "two parallel_safe, three heavy (one of them by default)"
        );

        let all_safe: Vec<&ExportConfig> =
            mixed.iter().filter(|e| e.name.starts_with('s')).collect();
        assert_eq!(
            pool_safe_heavy_split(&all_safe),
            (2, 0),
            "no heavy export at all — heavy must be 0, not a divisor"
        );

        let all_heavy: Vec<&ExportConfig> =
            mixed.iter().filter(|e| e.name.starts_with('h')).collect();
        assert_eq!(
            pool_safe_heavy_split(&all_heavy),
            (0, 3),
            "a queue of heavies has ZERO safe exports — the count that decides \
             whether the pool ever overlapped anything"
        );
    }

    /// The window is stamped AFTER the source probe, never before.
    ///
    /// `RunHarmBracket::open` runs `job::harm_snapshot` — a real connect (TLS
    /// handshake / pool build / MSSQL login) plus a catalog query, seconds over a
    /// tunnel. A `started_at` taken before it lands the instrumentation INSIDE
    /// the window that grades the run: the pool prints "actual makespan X vs
    /// predicted Y" and the aggregate's own window (`duration_ms`, and the
    /// run-level rows/s printed from it) carries the probe's cost. The
    /// run-over-run self-check is NOT downstream of this — it reads each
    /// export's own duration, not the run window. The close side of the
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

    /// The product half of one `src/pipeline` source file: every top-level
    /// `#[cfg(test)]` block REMOVED, `//` comments dropped OUTSIDE string
    /// literals (a naive `split("//")` eats the rest of any line holding a
    /// `://` URL, which is how a scanner silently loses a call it was written to
    /// find).
    ///
    /// Blocks are removed rather than the file truncated at the first one:
    /// `run.rs`, `cdc_job.rs`, `chunked/mod.rs` and three more all define
    /// product functions BELOW a test module (24 of them), so truncating hides
    /// real code from the scan — the blind spot that lets the next tail land
    /// unseen. A top-level block runs from a column-0 `#[cfg(test)]` to the next
    /// column-0 `}`, which rustfmt guarantees is its own close.
    fn pipeline_product_code(text: &str) -> String {
        let mut product = String::with_capacity(text.len());
        let mut skipping = false;
        for line in text.lines() {
            if !skipping && line.starts_with("#[cfg(test)]") {
                skipping = true;
                continue;
            }
            if skipping {
                if line == "}" {
                    skipping = false;
                }
                continue;
            }
            product.push_str(line);
            product.push('\n');
        }
        product
            .lines()
            .map(|line| {
                let bytes = line.as_bytes();
                let (mut in_str, mut escaped, mut cut, mut i) = (false, false, line.len(), 0);
                while i < bytes.len() {
                    match bytes[i] {
                        _ if escaped => escaped = false,
                        b'\\' if in_str => escaped = true,
                        b'"' => in_str = !in_str,
                        b'/' if !in_str && bytes.get(i + 1) == Some(&b'/') => {
                            cut = i;
                            break;
                        }
                        _ => {}
                    }
                    i += 1;
                }
                &line[..cut]
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Every top-level `fn` in `code`, as `(name, body)`.
    ///
    /// Top-level only — a `fn` inside an `impl`, a closure or a test module is
    /// indented, and rustfmt keeps it so; the body therefore ends at the first
    /// `\n}`, which is that function's own close. The body starts AFTER the
    /// signature's `{`, or every function whose name contains a needle would
    /// match itself.
    fn top_level_fns(code: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (at, line) in std::iter::once((0usize, code.lines().next().unwrap_or(""))).chain(
            code.match_indices('\n')
                .map(|(i, _)| (i + 1, code[i + 1..].lines().next().unwrap_or(""))),
        ) {
            let decl = line
                .strip_prefix("pub ")
                .or_else(|| {
                    line.starts_with("pub(")
                        .then(|| line.split_once(") ").map(|(_, r)| r))
                        .flatten()
                })
                .unwrap_or(line);
            let decl = decl.strip_prefix("async ").unwrap_or(decl);
            if !decl.starts_with("fn ") {
                continue;
            }
            let name = decl["fn ".len()..]
                .split(['(', '<'])
                .next()
                .unwrap_or_default()
                .to_string();
            let body_start = at
                + code[at..]
                    .find('{')
                    .unwrap_or_else(|| panic!("no body found for `{name}`"));
            let end = code[body_start..]
                .find("\n}")
                .map(|i| body_start + i)
                .unwrap_or_else(|| panic!("no top-level close found for `{name}`"));
            out.push((name, code[body_start..end].to_string()));
        }
        out
    }

    /// The self-check reaches every orchestrator through ONE seam — and the set
    /// of orchestrators is DERIVED, not typed.
    ///
    /// The shipped bug was a runner bypass: the check was wired into `run()`'s
    /// tail only, so `apply` (`run_waves`) and `apply --pool` (`run_pool`) fell
    /// through their `total > 1` / `pending.len() > 1` gates and said nothing —
    /// and `apply` has no `--export` flag, so a one-export config IS the whole
    /// invocation.
    ///
    /// The guard written for THAT fix then repeated the defect one level up: it
    /// read `include_str!("run.rs")` and a hand-typed list of three anchors, so
    /// the FIFTH tail — `apply_cmd::run_apply_command`'s plan-artifact replay,
    /// which opens its own state store, runs an export and writes its metrics
    /// row in another file — was invisible to it. A coverage ledger that names
    /// its own dimension grades only what its author already knew (round-7
    /// bughunt). So the dimension is now read off the tree:
    ///
    /// - SCOPE: every `.rs` file under `src/pipeline/**`, product half only.
    /// - CANDIDATE (a run tail): a top-level `fn` that drives an export to
    ///   completion — it calls a per-export job entry point (`run_export_job` /
    ///   `run_export_job_with_chunk_source`, the two functions that write an
    ///   `export_metrics` row) or builds a run aggregate (`aggregate::build`).
    /// - RULE: it must call the seam at least once per aggregate it builds
    ///   (`run` has TWO tails — the process-parallel parent returns early, the
    ///   in-process one falls through — and one call would leave one silent).
    /// - EXEMPTION: a candidate that legitimately must NOT self-check is listed
    ///   below WITH its reason, and the list is checked for staleness rather
    ///   than trusted.
    ///
    /// LIMITS, stated because a source lint cannot be sound: it sees
    /// `src/pipeline/**` only; it recognises a tail by those three call
    /// signatures, so an orchestrator that inlines the job logic or reaches the
    /// state store some other way (`cdc_job`'s streaming tail, whose rows/s is
    /// not comparable run-over-run anyway) is out of scope; and it grades the
    /// PRESENCE of a call, not its reachability — a seam call behind a `false`
    /// branch would satisfy it. The emission itself needs two state-DB rows for
    /// one export across two runs, which the sibling tests in this module stage
    /// through `self_check_throughput_as`.
    ///
    /// RED-proven by deleting `run_pool`'s call, by deleting
    /// `run_apply_command`'s (the defect itself), by pointing a tail straight at
    /// `aggregate::warn_throughput_regressions`, and by removing an exemption's
    /// reason.
    #[test]
    fn every_orchestrator_tail_routes_the_self_check_through_one_seam() {
        // (file, fn) -> why this candidate is not a run tail.
        let exempt: &[(&str, &str, &str)] = &[(
            "job.rs",
            "run_export_job",
            "a PER-EXPORT job, not a run tail: it calls itself only to dispatch a \
             CDC initial-snapshot child. The orchestrator that drives it (run / \
             run_waves / run_pool) owns the run's self-check, and emitting one \
             here too would report every export twice.",
        )];

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline");
        let mut files: Vec<(String, String)> = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read src/pipeline") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let rel = path
                    .strip_prefix(&root)
                    .expect("under pipeline")
                    .to_string_lossy()
                    .replace('\\', "/");
                let text = std::fs::read_to_string(&path).expect("read source");
                files.push((rel, pipeline_product_code(&text)));
            }
        }
        assert!(
            files.len() > 10,
            "the pipeline walk found only {} files — the scanner is blind",
            files.len()
        );

        // Spelled in pieces so this test's own text cannot answer for the code
        // it grades, whatever the module boundary does.
        let helper = concat!("aggregate::warn_throughput", "_regressions(");
        let seam = concat!("self_check", "_throughput(");
        let helper_callers: Vec<String> = files
            .iter()
            .flat_map(|(rel, code)| {
                code.match_indices(helper)
                    .map(move |(i, _)| format!("{rel}:{}", code[..i].lines().count() + 1))
            })
            .collect();
        assert_eq!(
            helper_callers.len(),
            1,
            "the aggregate helper must have exactly ONE caller — the \
             `self_check_throughput` seam. A tail calling it directly skips the \
             re-exec'd-child deferral and reports the same export twice. Callers: \
             {helper_callers:?}"
        );

        let job_entry = concat!("run_export", "_job(");
        let apply_entry = concat!("run_export_job", "_with_chunk_source(");
        let aggregate_build = concat!("aggregate::", "build(");
        let mut candidates: Vec<String> = Vec::new();
        for (rel, code) in &files {
            for (name, body) in top_level_fns(code) {
                let builds = body.matches(aggregate_build).count();
                let drives_an_export =
                    body.contains(job_entry) || body.contains(apply_entry) || builds > 0;
                if !drives_an_export {
                    continue;
                }
                candidates.push(format!("{rel}::{name}"));
                if let Some((_, _, why)) = exempt
                    .iter()
                    .find(|(f, n, _)| *f == rel.as_str() && *n == name)
                {
                    assert!(
                        why.len() > 40,
                        "{rel}::{name} is exempt from the run-over-run self-check \
                         with no real reason given"
                    );
                    continue;
                }
                // The seam's own definition is not a call. Since the shared
                // waves/pool tail landed (arch-roast 2026-08-21), routing
                // through `finish_run_tail(` IS routing through the seam — it
                // owns the aggregate + self-check + persist sequence, and its
                // own body is graded by this same loop (it builds an aggregate
                // and must call the check directly).
                let direct = body
                    .match_indices(seam)
                    .filter(|(i, _)| !body[..*i].ends_with("fn "))
                    .count();
                let routed = body
                    .match_indices("finish_run_tail(")
                    .filter(|(i, _)| !body[..*i].ends_with("fn "))
                    .count();
                // Routed credit is NOT fungible (godsplit bughunt 2026-08-21):
                // a tail that builds its OWN aggregate must call the check on
                // it directly — forwarding a different entries vec through
                // finish_run_tail must not excuse the self-built one.
                let calls = if builds == 0 { direct + routed } else { direct };
                assert!(
                    calls >= builds.max(1),
                    "{rel}::{name} ends a run (it drives an export to completion \
                     and/or builds {builds} run aggregate(s)) but calls the \
                     throughput self-check {calls} time(s). 'EVERY run \
                     self-reports degradation' is the contract, and it was false \
                     on `apply`, on `apply --pool`, and then on `apply \
                     <plan.json>` for a release each. Route the tail through \
                     `self_check_throughput`, or add it to this test's exemption \
                     list WITH the reason it must not."
                );
            }
        }
        // Anti-inertness floor, NOT the dimension: a parser that matched nothing
        // would otherwise pass by grading an empty set. Today's candidates are
        // `run`, `run_waves`, `run_pool`, `run_apply_command` and the exempt
        // per-export job wrapper.
        assert!(
            candidates.len() >= 5,
            "expected at least the four orchestrator tails plus the exempt \
             per-export job; parsed {candidates:?}"
        );
        for (file, name, _) in exempt {
            assert!(
                candidates.contains(&format!("{file}::{name}")),
                "the exemption for {file}::{name} is stale — nothing by that name \
                 drives an export any more. Delete it rather than carrying a \
                 reason for code that no longer exists."
            );
        }

        // …and a tail OUTSIDE run.rs may no more spell its concurrency claim
        // than one inside it (`a_runner_never_spells_its_concurrency_claim_as_a_
        // literal` grades run.rs). The label the self-check classifies must come
        // from a `*_mode_label` decider, or a new tail can hand
        // `mode_shares_the_source` "pool" for a run that overlapped nothing and
        // delete the actionable pointer it exists to keep.
        for (rel, code) in files.iter().filter(|(rel, _)| rel != "run.rs") {
            for (at, _) in code.match_indices("RunModes::") {
                let stmt = &code[at..code.len().min(at + 300)];
                assert!(
                    stmt.contains("_mode_label("),
                    "{rel} builds a `RunModes` without asking a `*_mode_label` \
                     decider for the label — the claim must be derived from what \
                     the run did, never spelled at the call site:\n{}",
                    stmt.lines().take(6).collect::<Vec<_>>().join("\n")
                );
            }
        }
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
