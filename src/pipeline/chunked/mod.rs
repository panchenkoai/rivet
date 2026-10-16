//! **Layer: Execution** (with bounded persistence writes)
//!
//! Implements chunked extraction in three variants: sequential (no
//! checkpoint), parallel-simple (no checkpoint), and the two checkpoint
//! runners that drive the resumable `chunk_checkpoint: true` flow. The
//! module is split into focused siblings:
//!
//! - `math` — pure functions: date parsing, range generation, SQL building, fingerprinting.
//! - `detect` — chunk-boundary detection via source queries.
//! - `exec` — stateless sequential and parallel execution (no checkpoint).
//! - `sequential_checkpoint` — single-threaded chunk-checkpoint runner.
//! - `parallel_checkpoint` — multi-worker chunk-checkpoint runner.
//! - `mod` (this file) — shared orchestration helpers (`chunked_plan`,
//!   `config_hint`, `ensure_chunk_checkpoint_plan`, `record_chunked_commit`),
//!   the [`ChunkSource`] enum, public re-exports for callers in
//!   `pipeline::*`, and unit tests for the checkpoint state-machine helpers.
//!
//! The split keeps each checkpoint runner in a focused ~150–350 LoC file
//! and concentrates the helpers they share in one place rather than
//! duplicating them.

mod attempt;
mod detect;
mod exec;
pub(crate) mod math;
mod parallel_checkpoint;
mod part_keys;
mod resume_m8;
#[cfg(test)]
pub(crate) use resume_m8::tests::m8_plan;
mod sequential_checkpoint;

/// One checkpointed chunk's result: rows, part records, the chunk's Form-B
/// checksums and its shape bytes.
type ChunkOutcome = (
    usize,
    Vec<super::commit::PartRecord>,
    super::commit::UnitChecksums,
    super::commit::Observations,
);

// ─── Re-exports for callers in pipeline:: ────────────────────────────────────

pub(crate) use detect::detect_and_generate_chunks;
pub(in crate::pipeline) use exec::run_chunked_parallel;
pub(crate) use exec::run_chunked_sequential;
pub use math::generate_chunks;
pub(crate) use math::{build_chunk_query_sql, chunk_plan_fingerprint, strip_select_star_from};
pub(in crate::pipeline) use parallel_checkpoint::run_chunked_parallel_checkpoint;
pub(crate) use resume_m8::Listed;
pub(crate) use resume_m8::apply_m8_resume_decisions;
use resume_m8::prune_superseded_attempts;
pub(crate) use resume_m8::rehydrate_manifest_parts_probed;
// `M8Stats` is intentionally not re-exported yet — Phase C-γ keeps it
// internal until Phase C-δ surfaces it via summary.json.  Listed here
// in a `#[cfg(test)]` re-export so unit tests in `tests/*` can pin
// the wire shape without growing the public API prematurely.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use resume_m8::M8Stats;
pub(crate) use sequential_checkpoint::run_chunked_sequential_checkpoint;

// ─── Chunk part file naming ──────────────────────────────────────────────────

/// Build a chunk output filename that is **collision-proof**.
///
/// A 64-bit random nonce guarantees that two writes of the same
/// `(export, chunk_index)` never produce the same name — e.g. a `repair
/// --execute` re-export landing in the same wall-clock second as the original
/// run (the second-granularity timestamp alone would collide and silently
/// overwrite), or two runs of one export within the same second to a flat
/// prefix. The new file therefore always lands *alongside* the original and can
/// never overwrite it — the hard guarantee behind ADR-0009 RR5 ("additive") and
/// ADR-0012 additive parts. The UTC timestamp is kept purely for human-readable
/// recency; uniqueness comes from the nonce, not the clock.
pub(crate) fn chunk_part_filename(
    export_name: &str,
    chunk_index: impl std::fmt::Display,
    ext: &str,
) -> String {
    use rand::RngExt;
    format!(
        "{}_{}_chunk{}_{:016x}.{}",
        export_name,
        chrono::Utc::now().format("%Y%m%d_%H%M%S"),
        chunk_index,
        rand::rng().random::<u64>(),
        ext,
    )
}

/// The chunk index encoded in a part produced by [`chunk_part_filename`].
///
/// Lives BESIDE the formatter on purpose: the two are one contract, and a reader
/// that parses what a distant writer formats is how a name silently stops meaning
/// what it used to. Returns `None` for anything not shaped like a chunk part
/// (a single-mode part, a CDC part, an operator's stray file), so a caller can
/// treat "not a chunk part" as its own case rather than guessing.
pub(crate) fn chunk_index_of(file_name: &str) -> Option<&str> {
    // Parse from the END. The writer appends `_chunk{idx}_{nonce}` last, so the
    // FINAL `_chunk` is always its own — while the FIRST one might belong to the
    // export's NAME. An export called `orders_chunk1_daily` made this read the
    // name's digit instead of the writer's index, and the caller that filters
    // rehydrated parts by completed-chunk status
    // (`resume_m8::rehydrate_manifest_parts_from_file_log`) then dropped parts it
    // could not account for. On a KEYSET export that is total: keyset writes no
    // `chunk_task` rows at all, so the completed set is empty and every part whose
    // name yielded an index was discarded from the reconstructed manifest —
    // durable pages, silently absent, on the recovery path.
    // Match the writer's SHAPE at the tail, not the first occurrence of a
    // substring: the stem's last two fields must be `chunk{digits}` and the
    // 16-hex nonce. Splitting on the first `_chunk` let an export NAMED
    // `orders_chunk1_daily` answer for the writer — and a KEYSET page of that
    // export, which carries no chunk token at all, was classified as chunk 1.
    // `rehydrate_manifest_parts_from_file_log` then dropped it against a
    // completed-chunk set that keyset never populates: durable pages silently
    // missing from the manifest a resumed run declares.
    let stem = file_name.rsplit_once('.').map_or(file_name, |(s, _)| s);
    // Strip the ROTATION suffix first. `part_indexed_name` appends `_p{n}` AFTER
    // the nonce whenever a chunk rotates past `max_file_size`, so the last field
    // of a rotated part is `p0`, not the 16-hex nonce. Requiring the nonce last
    // made every rotated part unclassified — and the caller
    // (`resume_m8::rehydrate_manifest_parts_from_file_log`) treats "not a chunk
    // part" as KEEP, so an interrupted chunk's abandoned siblings were resurrected
    // into the manifest a resumed run declares, beside the replacements. The
    // same normalization `attempt_key` performs, applied before the shape check
    // rather than after it.
    let stem = match stem.rsplit_once("_p") {
        Some((h, n)) if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => h,
        _ => stem,
    };
    let (head, nonce) = stem.rsplit_once('_')?;
    if nonce.len() != 16 || !nonce.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let idx = head
        .rsplit_once('_')
        .map_or(head, |(_, f)| f)
        .strip_prefix("chunk")?;
    (!idx.is_empty() && idx.bytes().all(|b| b.is_ascii_digit())).then_some(idx)
}

// ─── Chunk source selection ───────────────────────────────────────────────────

/// Determines how chunk ranges are obtained at execution time.
///
/// `rivet run` always uses `Detect` — runs `SELECT min/max` against the source
/// to compute boundaries live.  `rivet apply` passes `Precomputed` ranges from
/// a previously-generated `PlanArtifact`, skipping the detection queries
/// entirely and replaying the same boundaries that were fixed at plan time.
// `Clone` so a retrying caller can hand a fresh value to each attempt:
// `run_with_reconnect` re-enters `run_export` per retry, and the sequential
// chunked runners take the source BY VALUE.
#[derive(Clone)]
pub(crate) enum ChunkSource {
    /// Query the source for min/max and compute boundaries at execution time.
    Detect,
    /// Replay pre-computed boundaries from a `PlanArtifact`.
    Precomputed(Vec<(i64, i64)>),
}

// ─── Shared checkpoint-orchestration helpers ─────────────────────────────────

use super::RunSummary;
use crate::error::Result;
use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
use crate::state::StateStore;

/// The shared `Detect`-path preamble for all four chunked runners: compute the
/// chunk ranges, then run the pre-chunk schema-drift check (ADR-0021) once,
/// before any chunk executes — so `on_schema_drift: fail` aborts before a single
/// chunk is written. The runners' durability-divergent *execution*
/// (ADR-0010/0017) stays per-runner; only this identical preamble is shared, the
/// way `commit::record_part` shares the per-part tail (ADR-0018).
///
/// `Precomputed`/resume chunk sources never call this — drift was evaluated on
/// the original planning run that produced the ranges — a claim that was FALSE
/// (see [`check_drift_only`], which the precomputed arms now call instead).
/// Drift runs only when the runner is stateful (`state.is_some()`).
pub(super) fn prepare_chunk_plan(
    src: &mut dyn crate::source::Source,
    plan: &ResolvedRunPlan,
    state: Option<&StateStore>,
    summary: &mut RunSummary,
) -> Result<Vec<(i64, i64)>> {
    let cp = chunked_plan(plan);
    let ranges = detect_and_generate_chunks(
        src,
        &plan.base_query,
        &cp.column,
        cp.chunk_size,
        cp.chunk_count,
        &plan.export_name,
        cp.by_days,
        plan.source.source_type,
    )?;
    if let Some(st) = state {
        super::schema_drift::check_from_type_mappings(src, st, plan, summary)?;
    }
    Ok(ranges)
}

/// Run the pre-chunk drift gate WITHOUT computing ranges.
///
/// The gate compares the source's current column types against the stored ones;
/// it has nothing to do with chunk boundaries. That is why a run whose ranges
/// come from a plan artifact can — and must — still take it.
///
/// It used to be reachable only through [`prepare_chunk_plan`], which only the
/// `ChunkSource::Detect` arm calls, so `rivet apply` of any chunked plan skipped
/// `on_schema_drift` entirely. Four runners recorded that as
/// `chunks_precomputed = true; // drift gate skipped by contract`, citing a
/// contract that said drift "was evaluated on the original planning run that
/// produced the ranges". It was not: `src/plan/` performs no drift evaluation —
/// `plan/build.rs` only COPIES `on_schema_drift` into the artifact. And a
/// planning-time check could not have helped anyway, because the drift this
/// guards against happens BETWEEN plan and apply, which is the whole reason the
/// plan → review → apply workflow exists.
///
/// Measured: one config, `on_schema_drift: fail`, a column dropped after the
/// plan was sealed — `rivet run` aborted ("schema drift detected — removed:
/// dropme") while `rivet apply` of the same policy exported 500 rows and exited
/// 0. The non-chunked half of `apply` applied the gate throughout
/// (`single.rs`), so the split was never "apply skips it" but "apply skips it
/// for chunked exports" — the ones moving the most data.
pub(super) fn check_drift_only(
    src: &mut dyn crate::source::Source,
    plan: &ResolvedRunPlan,
    state: Option<&StateStore>,
    summary: &mut RunSummary,
) -> Result<()> {
    let Some(st) = state else { return Ok(()) };
    super::schema_drift::check_from_type_mappings(src, st, plan, summary)
}

/// The ranges a resumed run plans again: the source's key span as it is now under `Detect`, none for a sealed plan; the drift gate runs either way.
pub(super) fn resumed_chunk_plan(
    src: &mut dyn crate::source::Source,
    plan: &ResolvedRunPlan,
    state: &StateStore,
    summary: &mut RunSummary,
    chunk_source: &ChunkSource,
) -> Result<Vec<(i64, i64)>> {
    match chunk_source {
        ChunkSource::Detect => prepare_chunk_plan(src, plan, Some(state), summary),
        ChunkSource::Precomputed(_) => {
            check_drift_only(src, plan, Some(state), summary)?;
            Ok(vec![])
        }
    }
}

/// What the stored tasks of a resumed run need to cover `ranges`, the source's key span now: whether to drop them, and the tasks to plan. A run that committed no part is planned again whole; any other gains the pieces of `ranges` outside its stored span.
fn replan(
    tasks: &[crate::state::ChunkTaskInfo],
    ranges: &[(i64, i64)],
) -> Result<(bool, Vec<(i64, i64)>)> {
    if !tasks.iter().any(|t| t.file_name.is_some()) {
        return Ok((!ranges.is_empty(), ranges.to_vec()));
    }
    let windows = stored_windows(tasks).map_err(|why| {
        crate::error::coded(
            crate::error::codes::STATE_CHUNK_WINDOWS_BROKEN,
            format!("stored chunk windows {why}"),
        )
    })?;
    Ok((false, math::ranges_uncovered(ranges, &windows)))
}

/// The stored windows of a chunk run, ordered by their first key; `Err` says why they cannot be read as windows: a bound that is not an integer, a window that ends before it starts, or keys that two windows hold.
pub(crate) fn stored_windows(
    tasks: &[crate::state::ChunkTaskInfo],
) -> std::result::Result<Vec<(i64, i64)>, String> {
    let mut windows = Vec::with_capacity(tasks.len());
    for t in tasks {
        let (Ok(start), Ok(end)) = (t.start_key.parse::<i64>(), t.end_key.parse::<i64>()) else {
            return Err(format!(
                "hold a bound that is not an integer (chunk {}: {:?}..{:?})",
                t.chunk_index, t.start_key, t.end_key
            ));
        };
        if end < start {
            return Err(format!(
                "hold a window that ends before it starts (chunk {}: {start}..={end})",
                t.chunk_index
            ));
        }
        windows.push((start, end));
    }
    windows.sort_unstable();
    match windows.windows(2).find(|pair| pair[1].0 <= pair[0].1) {
        Some(pair) => Err(format!(
            "put keys {}..={} in two windows",
            pair[1].0,
            pair[0].1.min(pair[1].1)
        )),
        None => Ok(windows),
    }
}

/// Bring the stored plan of the resumed run `run_id` up to `ranges`, the source's key span now; a sealed plan passes none and keeps its tasks.
fn replan_resumed_run(
    state: &StateStore,
    export: &str,
    run_id: &str,
    ranges: &[(i64, i64)],
) -> Result<()> {
    let (replace, planned) = replan(&state.list_chunk_tasks_for_run(run_id)?, ranges)?;
    if planned.is_empty() {
        return Ok(());
    }
    state.replan_chunk_tasks(run_id, replace, &planned)?;
    if replace {
        log::info!(
            "export '{export}': run '{run_id}' committed no part before it stopped — planned \
             again over the key span the source holds now ({} chunk task(s))",
            planned.len()
        );
    } else {
        log::warn!(
            "export '{export}': the source holds keys no window of run '{run_id}' covers — \
             {} chunk task(s) added for them; the windows that run committed are not read \
             again",
            planned.len()
        );
    }
    Ok(())
}

/// Whether precomputed ranges read rows, so the drift gate must run: no ranges, no read, no connection.
pub(super) fn precomputed_ranges_need_the_drift_gate(ranges: &[(i64, i64)]) -> bool {
    !ranges.is_empty()
}

/// Idle source connections shared by one parallel chunked run: a chunk reuses one instead of reconnecting per chunk.
#[derive(Default)]
pub(super) struct IdleSources(std::sync::Mutex<Vec<Box<dyn crate::source::Source>>>);

impl IdleSources {
    /// An idle connection, or a fresh one when none is idle.
    pub(super) fn take(
        &self,
        cfg: &crate::config::SourceConfig,
    ) -> Result<Box<dyn crate::source::Source>> {
        let idle = self.0.lock().unwrap_or_else(|p| p.into_inner()).pop();
        match idle {
            Some(src) => Ok(src),
            None => crate::source::create_source(cfg),
        }
    }

    /// Return a connection after a chunk that SUCCEEDED; a failed chunk's connection is dropped instead.
    pub(super) fn give(&self, src: Box<dyn crate::source::Source>) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).push(src);
    }
}

/// Extract the `ChunkedPlan` from a `ResolvedRunPlan`. Panics if the strategy
/// is not `Chunked` — all callers in this module only run for chunked plans.
pub(super) fn chunked_plan(plan: &ResolvedRunPlan) -> &ChunkedPlan {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => cp,
        _ => unreachable!("chunked runner called with non-chunked plan"),
    }
}

/// What a checkpointed run does about a chunk run a DEAD process left in progress.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CrashedChunkRun {
    /// Nothing to recover (or `--resume` already asked for it).
    Nothing,
    /// Resume the crashed plan, as `--resume` would.
    Resume,
    /// `--resume --force` with no chunk run to continue: run as a plain run does.
    ExportAgain,
}

/// Decide the recovery for a crashed chunk run once this process holds the export's run lease.
pub(crate) fn crashed_chunk_run_action(
    explicit_resume: bool,
    crashed_run: bool,
    force: bool,
) -> CrashedChunkRun {
    match (explicit_resume, crashed_run) {
        (false, true) => CrashedChunkRun::Resume,
        (true, false) if force => CrashedChunkRun::ExportAgain,
        _ => CrashedChunkRun::Nothing,
    }
}

/// The refusal a run gives when a LIVE process holds the export's run lease: `run_id` names the live run when the state records one, and a checkpointed plan says what the next run does with what it left.
pub(crate) fn live_run_refusal(export: &str, run_id: Option<&str>, resumable: bool) -> String {
    let (kind, then) = match resumable {
        true => ("chunk checkpoint run", "the next run resumes it"),
        false => ("run", "the next run starts over"),
    };
    let run = match run_id {
        Some(id) => format!("{kind} '{id}'"),
        None => format!("a {kind}"),
    };
    format!(
        "export '{export}': {run} still in progress in another live rivet process (it holds \
         the export's run lease); wait for it to finish — if it is stopped instead, {then}"
    )
}

/// The key of an export's run lease on one source: held by every run of the export that reads `source`, taken by every command that rewrites its stored progress or its destination. The source enters as a hash, so a lease file name stays short and names no host.
pub fn run_lease_key(export: &str, source: &str) -> String {
    format!(
        "chunk-run:{export}@{:016x}",
        xxhash_rust::xxh3::xxh3_64(source.as_bytes())
    )
}

/// `export`'s run lease on `source`, or `None` while a live process holds it.
pub(crate) fn try_run_lease<'s>(
    state: &'s StateStore,
    export: &str,
    source: &str,
) -> Result<Option<crate::state::LoadLease<'s>>> {
    state.try_load_lease(&run_lease_key(export, source))
}

/// How a refusal names the live run of `export` on `source`: by its run-status id, whose last field is the pid.
pub(crate) fn live_run_label(state: &StateStore, export: &str, source: &str) -> Result<String> {
    Ok(match state.newest_running_run(export, source)? {
        Some(run_id) => format!("run '{run_id}'"),
        None => "a run".to_string(),
    })
}

/// Take `export`'s run lease on `source` to rewrite its stored progress (`what`); refuse while a live run holds it.
pub(crate) fn claim_export_progress<'s>(
    state: &'s StateStore,
    export: &str,
    source: &str,
    what: &str,
) -> Result<crate::state::LoadLease<'s>> {
    if let Some(lease) = try_run_lease(state, export, source)? {
        return Ok(lease);
    }
    crate::rivet_bail!(
        crate::error::codes::STATE_RUN_IN_PROGRESS,
        "export '{export}': cannot {what}: {} is in progress in a live rivet process, which \
         holds the export's run lease. Wait for it to finish, or stop that process (its pid \
         ends the run id), then repeat this command.",
        live_run_label(state, export, source)?
    )
}

/// Hold the export's run lease for the whole run, whatever its mode: refused by code while a live process holds it (released by the OS or the keeper when that process dies, no clock).
pub(crate) fn claim_run<'s>(
    state: &'s StateStore,
    plan: &ResolvedRunPlan,
) -> Result<crate::state::LoadLease<'s>> {
    let export = &plan.export_name;
    let source = plan.source.state_key();
    if let Some(lease) = try_run_lease(state, export, &source)? {
        return Ok(lease);
    }
    crate::rivet_bail!(
        crate::error::codes::STATE_RUN_IN_PROGRESS,
        "{}",
        live_run_refusal(
            export,
            state.newest_running_run(export, &source)?.as_deref(),
            plan.strategy.is_resumable()
        )
    )
}

/// Under the run lease, recover a chunk run whose process died: the plan to run in its place, if any; `force` is the run's `--force`.
pub(crate) fn recover_checkpoint_run(
    plan: &ResolvedRunPlan,
    progress: &crate::state::ProgressClaim<'_>,
    force: bool,
) -> Result<Option<ResolvedRunPlan>> {
    let export = &plan.export_name;
    let ExtractionStrategy::Chunked(_) = &plan.strategy else {
        return Ok(None);
    };
    if !plan.strategy.is_resumable() {
        return Ok(None);
    }
    let crashed = progress.chunk_run()?;
    match (
        crashed_chunk_run_action(plan.resume, crashed.is_some(), force),
        crashed,
    ) {
        (CrashedChunkRun::Resume, Some((rid, _))) => {
            log::warn!(
                "export '{export}': chunk checkpoint run '{rid}' was left in progress by a \
                 process that is no longer running — resuming it"
            );
            let mut resumed = plan.clone();
            resumed.resume = true;
            Ok(Some(resumed))
        }
        (CrashedChunkRun::ExportAgain, _) => {
            log::info!(
                "export '{export}': --resume --force and no chunk run in progress: nothing to \
                 continue, running as a plain run"
            );
            let mut fresh = plan.clone();
            fresh.resume = false;
            Ok(Some(fresh))
        }
        _ => Ok(None),
    }
}

/// Render the `--config <path>` argument used in user-facing recovery hints,
/// or a `<CONFIG>` placeholder when the config path is not available (e.g.
/// the `rivet apply` path which only knows the plan file).
/// The refusal of a plain run that met chunk checkpoint run `rid` of `export` still in progress; `raced` when a concurrent run opened it under this one.
fn chunk_run_unfinished(export: &str, rid: &str, config_path: &str, raced: bool) -> anyhow::Error {
    let raced = if raced {
        " (a concurrent run won the race)"
    } else {
        ""
    };
    let hint = config_hint(config_path);
    crate::error::coded(
        crate::error::codes::STATE_CHUNK_RUN_UNFINISHED,
        format!(
            "export '{export}': chunk checkpoint run '{rid}' still in progress{raced}; use `rivet run {hint} --export {export} --resume` or `rivet state reset-chunks {hint} --export {export}`"
        ),
    )
}

pub(super) fn config_hint(config_path: &str) -> String {
    if config_path.is_empty() {
        "--config <CONFIG>".to_string()
    } else {
        format!("--config {}", config_path)
    }
}

pub(super) fn ensure_chunk_checkpoint_plan(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    cp: &ChunkedPlan,
    summary: &mut RunSummary,
    chunks: &[(i64, i64)],
    config_path: &str,
    progress: &crate::state::ProgressClaim<'_>,
) -> Result<String> {
    let plan_hash = chunk_plan_fingerprint(
        &plan.base_query,
        &cp.column,
        cp.chunk_size,
        cp.chunk_count,
        cp.by_days,
    );
    let max_att = cp.max_attempts;

    // A CDC baseline leg is rivet's OWN export: the operator never named it, so the
    // "use `--export <leg> --resume`" remediation below is one they cannot follow —
    // both commands reject a name absent from the config. An interrupted leg
    // resumes on the next plain run instead.
    // ponytail: leg-ness via the legacy name fold; a flag on the plan if a user
    // export ever legitimately carries the infix.
    let is_leg = crate::manifest::snapshot_family(&plan.export_name) != plan.export_name;
    let crashed = progress.chunk_run()?;
    let resume = plan.resume || (is_leg && crashed.is_some());

    if resume {
        match crashed {
            Some((rid, stored_hash)) => {
                if stored_hash != plan_hash {
                    crate::rivet_bail!(
                        crate::error::codes::STATE_CHUNK_PLAN_CHANGED,
                        "export '{}': chunk plan fingerprint mismatch (query, chunk_column, chunk_size, chunk_count or chunk_by_days changed); cannot resume. \
                         To finish it: restore the query and the chunk settings the interrupted run had and run once to finish it. \
                         To abandon it: `rivet state reset-chunks -c {config_path} -e {}` abandons it and the next run starts over",
                        plan.export_name,
                        plan.export_name
                    );
                }
                if let Err(why) = stored_windows(&state.list_chunk_tasks_for_run(&rid)?) {
                    crate::rivet_bail!(
                        crate::error::codes::STATE_CHUNK_WINDOWS_BROKEN,
                        "export '{}': the chunk windows stored for interrupted run '{rid}' {why}. \
                         rivet never writes such windows, so the checkpoint was changed outside \
                         rivet, and resuming it would read keys twice or not at all. `rivet state \
                         reset-chunks -c {config_path} -e {}` abandons the run; the next run plans the \
                         export again and reads every key",
                        plan.export_name,
                        plan.export_name
                    );
                }
                replan_resumed_run(state, &plan.export_name, &rid, chunks)?;
                summary.run_id = rid.clone();
                state.set_resume_run_id(progress.key(), &rid)?;
                let n = state.reset_stale_running_chunk_tasks(&rid)?;
                if n > 0 {
                    log::warn!(
                        "export '{}': reset {} stale 'running' chunk task(s) after resume",
                        plan.export_name,
                        n
                    );
                }
                // Un-pin chunks a prior run gave up on (retryable budget
                // exhausted OR permanent `retryable=false`): `--resume` is the
                // operator's deliberate retry after fixing the cause, so the
                // permanently-failed pin — which `claim_next_chunk_task` would
                // otherwise skip forever — must be lifted here or the run's own
                // "fix and --resume" remediation is a false promise (#200-4).
                let f = state.reset_failed_chunk_tasks_for_resume(&rid)?;
                if f > 0 {
                    log::warn!(
                        "export '{}': re-queued {} failed chunk task(s) for retry after resume",
                        plan.export_name,
                        f
                    );
                }
                return Ok(rid);
            }
            None => {
                crate::rivet_bail!(
                    crate::error::codes::CLI_OPTION_UNSUPPORTED,
                    "export '{}': --resume but no in-progress chunk checkpoint — nothing to \
                     continue; run without --resume (a crashed run is resumed by a plain run)",
                    plan.export_name
                );
            }
        }
    }

    if let Some((rid, _)) = crashed {
        return Err(chunk_run_unfinished(
            &plan.export_name,
            &rid,
            config_path,
            false,
        ));
    }

    // Round-2 audit #13: the v15 partial-unique index (`idx_chunk_run_one_inprogress`,
    // one in_progress run per export) is the REAL guard against a concurrent
    // second run — the check-then-act above has a TOCTOU window two overlapping
    // scheduler ticks can slip through. When the index rejects this create, another
    // run won the race: map it to the same 'still in progress' bail, not a raw DB
    // constraint error.
    if let Err(e) =
        state.open_chunk_run(progress.key(), &summary.run_id, &plan_hash, max_att, chunks)
    {
        if let Ok(Some((rid, _))) = progress.chunk_run() {
            return Err(chunk_run_unfinished(
                &plan.export_name,
                &rid,
                config_path,
                true,
            ));
        }
        return Err(e);
    }
    log::info!(
        "export '{}': chunk checkpoint: {} tasks saved (run_id={})",
        plan.export_name,
        chunks.len(),
        summary.run_id
    );
    Ok(summary.run_id.clone())
}

/// Epic G: record the highest completed `chunk_index` for this run as the new
/// committed boundary. Failures are logged — progression is observational and
/// must not fail the pipeline.
///
/// Visibility `pub(crate)` so [`super::run_store::RunStore`] can dispatch
/// the chunked variant of `Progression` through this helper. The walk of
/// `chunk_task` is chunked-mode-specific, so the logic stays in this
/// module rather than moving to `state::progression`.
pub(crate) fn record_chunked_commit(state: &StateStore, export_name: &str, run_id: &str) {
    let tasks = match state.list_chunk_tasks_for_run(run_id) {
        Ok(t) => t,
        Err(e) => {
            log::warn!(
                "export '{}': committed boundary: could not read chunk tasks: {:#}",
                export_name,
                e
            );
            return;
        }
    };
    let highest = tasks
        .iter()
        .filter(|t| t.status == "completed")
        .map(|t| t.chunk_index)
        .max();
    if let Some(idx) = highest
        && let Err(e) = state.record_committed_chunked(export_name, idx, run_id)
    {
        log::warn!(
            "export '{}': committed boundary update failed: {:#}",
            export_name,
            e
        );
    }
}

#[cfg(test)]
mod tests {
    //! Unit coverage for the checkpoint state machine in this module.
    //!
    //! These tests use `StateStore::open_in_memory()` so they exercise the
    //! real SQLite schema and the real `state::checkpoint::*` methods —
    //! no mocks, no docker. They are *intentionally* the only unit cover
    //! for this file's recovery logic; everything that touches a live
    //! `Source` is exercised by `tests/live_*.rs` instead.

    #[test]
    fn only_precomputed_ranges_that_read_rows_need_the_drift_gate() {
        assert!(!super::precomputed_ranges_need_the_drift_gate(&[]));
        assert!(super::precomputed_ranges_need_the_drift_gate(&[(1, 10)]));
    }

    #[test]
    fn a_dead_owners_plan_resumes_unless_resume_was_asked_or_nothing_crashed() {
        use super::{CrashedChunkRun::*, crashed_chunk_run_action};
        for force in [false, true] {
            assert_eq!(crashed_chunk_run_action(false, true, force), Resume);
            assert_eq!(crashed_chunk_run_action(true, true, force), Nothing);
            assert_eq!(crashed_chunk_run_action(false, false, force), Nothing);
        }
        assert_eq!(crashed_chunk_run_action(true, false, false), Nothing);
        assert_eq!(crashed_chunk_run_action(true, false, true), ExportAgain);
    }

    /// `--resume --force` with no chunk run in progress runs as a plain run; without `--force` the plan stays a resume, which the runner refuses.
    #[test]
    fn a_forced_resume_with_nothing_to_continue_exports_again() {
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = make_plan("orders");
        plan.resume = true;
        let claim = |force| {
            recover_checkpoint_run(&plan, &state.claim(plan.progress_key()).unwrap(), force)
                .unwrap()
        };
        assert!(claim(false).is_none());
        let fresh = claim(true).expect("the forced resume becomes a fresh run");
        assert!(!fresh.resume);
    }

    #[test]
    fn a_live_owner_is_refused_without_a_remedy_that_touches_it() {
        assert_eq!(
            super::live_run_refusal("t", Some("r1"), true),
            "export 't': chunk checkpoint run 'r1' still in progress in another live rivet \
             process (it holds the export's run lease); wait for it to finish — if it is \
             stopped instead, the next run resumes it"
        );
        assert_eq!(
            super::live_run_refusal("t", None, false),
            "export 't': a run still in progress in another live rivet process (it holds the \
             export's run lease); wait for it to finish — if it is stopped instead, the next \
             run starts over"
        );
    }

    /// Every run takes its export's lease, whatever its mode: a second run of the export is refused by code while the first is alive, another export is not, and the lease is free once the run ends.
    #[test]
    fn a_run_of_any_mode_holds_its_exports_lease_until_it_ends() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("state.db");
        let (first, second) = (
            StateStore::open_at_path(&db).unwrap(),
            StateStore::open_at_path(&db).unwrap(),
        );
        let checkpointed = make_plan("orders");
        let mut full = make_plan("orders");
        full.strategy = ExtractionStrategy::Snapshot;
        assert!(checkpointed.strategy.is_resumable() && !full.strategy.is_resumable());
        for (live, late) in [
            (&full, &full),
            (&full, &checkpointed),
            (&checkpointed, &full),
        ] {
            let held = claim_run(&first, live).expect("the first run takes the lease");
            let err = claim_run(&second, late).err().expect("refused beside it");
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_STATE_RUN_IN_PROGRESS")
            );
            assert!(
                err.to_string()
                    .contains("still in progress in another live rivet process"),
                "{err:#}"
            );
            let other = make_plan("customers");
            drop(claim_run(&second, &other).expect("another export is free"));
            drop(held);
            drop(claim_run(&second, late).expect("free once the first run ended"));
        }
    }

    /// An export whose NAME contains `_chunk<digits>_` must not fool the index
    /// parser — and the fixture must cross that threshold, or the two ends of the
    /// name are indistinguishable.
    ///
    /// The parser split on the FIRST `_chunk`, so `orders_chunk1_daily` fed it the
    /// name's digit instead of the writer's index. On a KEYSET export that is
    /// total loss on the recovery path: keyset writes no `chunk_task` rows, so
    /// `rehydrate_manifest_parts_from_file_log`'s completed set is EMPTY and every
    /// part whose name yielded an index was dropped from the reconstructed
    /// manifest — durable pages silently absent from what the run declares.
    #[test]
    fn the_chunk_index_comes_from_the_writers_suffix_not_the_export_name() {
        // Round-trip through the real formatter, so the reader is graded against
        // what the writer actually emits rather than a hand-typed shape.
        let name = chunk_part_filename("orders_chunk1_daily", 7, "parquet");
        assert_eq!(
            chunk_index_of(&name),
            Some("7"),
            "the writer wrote chunk 7; the export NAME's `_chunk1_` must not win: {name}"
        );

        // A plain name still works — the fix must not lose the ordinary case.
        let plain = chunk_part_filename("orders", 12, "parquet");
        assert_eq!(chunk_index_of(&plain), Some("12"), "{plain}");

        // A ROTATED part — the shape `part_indexed_name` produces when a chunk
        // exceeds `max_file_size`. Its last field is `p0`, not the nonce, and an
        // earlier version of this parser therefore returned None for every one of
        // them. The caller treats "not a chunk part" as KEEP, so an interrupted
        // chunk's abandoned siblings were resurrected into a resumed run's
        // manifest. Built through BOTH real writers, never typed by hand.
        for i in 0..2 {
            let rotated = crate::pipeline::commit::part_indexed_name(&name, i, 2);
            assert_eq!(
                chunk_index_of(&rotated),
                Some("7"),
                "a rotation sibling is still a part of chunk 7: {rotated}"
            );
        }

        // A keyset part carries no chunk token and must stay unclassified, even
        // when the export name contains one: classifying it makes the rehydration
        // filter discard it against an empty completed set.
        assert_eq!(
            chunk_index_of("orders_chunk1_daily_20260101_000000_123_keyset3.parquet"),
            None,
            "a keyset page is not a chunk part — treating it as chunk 1 drops it on resume"
        );

        // Non-numeric and absent tokens stay `None`.
        assert_eq!(chunk_index_of("orders_20260101_000000.parquet"), None);
        assert_eq!(chunk_index_of("orders_20260101_chunkzz_aabb.parquet"), None);
    }

    /// A name is a chunk part only with BOTH halves of the writer's suffix: a digit index and the 16-hex nonce.
    #[test]
    fn a_name_is_a_chunk_part_only_with_a_digit_index_and_the_16_hex_nonce() {
        const NONCE: &str = "00000000deadbeef";
        let named = |index: &str, nonce: &str| format!("orders_20260101_000000_{index}_{nonce}");
        for (stem, index) in [
            (named("chunk7", NONCE), Some("7")),
            (named("chunk7", "aabb"), None),
            (named("chunk7", "00000000deadbeef0"), None),
            (named("chunk7", "zzzzzzzzzzzzzzzz"), None),
            (named("chunk7", "0000000-deadbeef"), None),
            (named("chunkzz", NONCE), None),
            (named("chunk7a", NONCE), None),
            (named("chunk", NONCE), None),
            (named("keyset7", NONCE), None),
        ] {
            for name in [format!("{stem}.parquet"), format!("{stem}_p3.parquet")] {
                assert_eq!(chunk_index_of(&name), index, "{name}");
            }
        }

        // Only `_p<digits>` at the very end is the rotation suffix: an export
        // whose NAME holds `_p` still has its parts classified.
        for export in ["user_profiles", "orders_p", "metrics_p95"] {
            let name = chunk_part_filename(export, 7, "parquet");
            assert_eq!(chunk_index_of(&name), Some("7"), "{name}");
            let rotated = crate::pipeline::commit::part_indexed_name(&name, 1, 2);
            assert_eq!(chunk_index_of(&rotated), Some("7"), "{rotated}");
        }
    }

    use super::*;

    #[test]
    fn chunk_part_filename_is_collision_proof_for_same_chunk() {
        // The hard guarantee behind RR5: two writes of the same (export, chunk)
        // — e.g. a repair re-export in the same wall-clock second as the
        // original — must NEVER produce the same name, or the new file would
        // overwrite the original. The nonce, not the clock, guarantees this.
        let a = chunk_part_filename("orders", 0usize, "parquet");
        let b = chunk_part_filename("orders", 0usize, "parquet");
        assert_ne!(a, b, "same (export, chunk) must yield distinct filenames");
        for n in [&a, &b] {
            assert!(n.starts_with("orders_"), "keeps export prefix: {n}");
            assert!(n.contains("_chunk0_"), "keeps chunk index: {n}");
            assert!(n.ends_with(".parquet"), "keeps extension: {n}");
        }
    }

    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, SourceConfig, SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
    use crate::tuning::SourceTuning;

    fn make_plan(export_name: &str) -> ResolvedRunPlan {
        ResolvedRunPlan {
            split_window: None,
            bytes_read: Default::default(),
            parts_landed: Default::default(),
            single_pass_of: None,
            export_name: export_name.into(),
            partition_rollover: None,
            source_table: None,
            base_query: "SELECT id FROM orders".into(),
            query_template: None,
            catalog_query: None,
            destination_written: None,
            is_split_unit: false,
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                chunk_count: None,
                parallel: 1,
                by_days: None,
                checkpoint: true,
                max_attempts: 3,
            }),
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
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
                mongo: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn make_summary(plan: &ResolvedRunPlan, run_id: &str) -> RunSummary {
        let mut s = RunSummary::stub_for_testing(run_id, plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "chunked".into();
        s.compression = "none".into();
        s
    }

    // ── config_hint ────────────────────────────────────────────────────────

    #[test]
    fn config_hint_uses_explicit_path_when_set() {
        assert_eq!(config_hint("rivet.yaml"), "--config rivet.yaml");
    }

    #[test]
    fn config_hint_uses_placeholder_when_empty() {
        assert_eq!(config_hint(""), "--config <CONFIG>");
    }

    // ── ensure_chunk_checkpoint_plan: 5 state-machine transitions ──────────

    /// Fresh run (no resume, no prior checkpoint): must create chunk_run +
    /// insert tasks, and the returned run_id matches summary.run_id.
    #[test]
    fn ensure_chunk_checkpoint_fresh_run_creates_state() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan, "run-fresh");
        let chunks = vec![(1, 100), (101, 200), (201, 300)];

        let rid = ensure_chunk_checkpoint_plan(
            &state,
            &plan,
            &cp,
            &mut summary,
            &chunks,
            "rivet.yaml",
            &state.claim(plan.progress_key()).unwrap(),
        )
        .expect("fresh run must succeed");
        assert_eq!(rid, "run-fresh");

        let total = state.count_chunk_tasks_total(&rid).unwrap();
        assert_eq!(total, 3, "all 3 chunk tasks must be persisted");
    }

    /// `--resume` with no in-progress run → actionable error.
    #[test]
    fn ensure_chunk_checkpoint_resume_without_prior_run_bails() {
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = make_plan("orders");
        plan.resume = true;
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan, "run-x");

        let err = ensure_chunk_checkpoint_plan(
            &state,
            &plan,
            &cp,
            &mut summary,
            &[],
            "rivet.yaml",
            &state.claim(plan.progress_key()).unwrap(),
        )
        .expect_err("resume without prior run must error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("--resume but no in-progress chunk checkpoint"),
            "got: {msg}"
        );
        assert!(
            msg.contains("run without --resume") && !msg.contains("reset-chunks"),
            "a reset cannot create a checkpoint to resume: {msg}"
        );
    }

    /// `--resume` with a prior run that has a *different* plan fingerprint
    /// (e.g. chunk_size changed) → bail with fingerprint-mismatch error.
    #[test]
    fn ensure_chunk_checkpoint_resume_with_hash_mismatch_bails() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        state
            .create_chunk_run("run-old", "orders", "DIFFERENT_HASH", 3)
            .unwrap();

        let mut plan_resume = plan.clone();
        plan_resume.resume = true;
        let cp = match &plan_resume.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan_resume, "run-new");

        let err = ensure_chunk_checkpoint_plan(
            &state,
            &plan_resume,
            &cp,
            &mut summary,
            &[],
            "rivet.yaml",
            &state.claim(plan_resume.progress_key()).unwrap(),
        )
        .expect_err("hash mismatch must error");
        let msg = format!("{:#}", err);
        assert!(msg.contains("fingerprint mismatch"), "got: {msg}");
        assert!(msg.contains("cannot resume"), "got: {msg}");
    }

    /// `--resume` with a prior run whose fingerprint matches: must adopt the
    /// existing run_id, *not* create a new one.
    #[test]
    fn ensure_chunk_checkpoint_resume_matching_hash_adopts_old_run_id() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let expected_hash = chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.chunk_count,
            cp.by_days,
        );
        state
            .create_chunk_run("run-prior", "orders", &expected_hash, 3)
            .unwrap();

        let mut plan_resume = plan.clone();
        plan_resume.resume = true;
        let mut summary = make_summary(&plan_resume, "run-this-attempt");

        let rid = ensure_chunk_checkpoint_plan(
            &state,
            &plan_resume,
            &cp,
            &mut summary,
            &[],
            "rivet.yaml",
            &state.claim(plan_resume.progress_key()).unwrap(),
        )
        .expect("matching resume must succeed");
        assert_eq!(
            rid, "run-prior",
            "must adopt the prior run_id, not create new"
        );
        assert_eq!(
            summary.run_id, "run-prior",
            "summary.run_id must also be rewritten so downstream writes target the existing run"
        );
    }

    /// A stored task over `lo..=hi`; `file` is the part that completed it.
    fn stored_task(
        index: i64,
        lo: &str,
        hi: &str,
        file: Option<&str>,
    ) -> crate::state::ChunkTaskInfo {
        crate::state::ChunkTaskInfo {
            chunk_index: index,
            start_key: lo.into(),
            end_key: hi.into(),
            status: if file.is_some() {
                "completed"
            } else {
                "pending"
            }
            .into(),
            attempts: 0,
            last_error: None,
            rows_written: None,
            file_name: file.map(Into::into),
        }
    }

    #[test]
    fn a_resumed_plan_gains_the_keys_outside_its_span_and_is_planned_again_while_it_holds_no_part()
    {
        let now = [(1, 10), (11, 20), (21, 23)];
        let empty_table = [stored_task(0, "0", "0", None)];
        assert_eq!(replan(&empty_table, &now).unwrap(), (true, now.to_vec()));
        assert_eq!(replan(&[], &now).unwrap(), (true, now.to_vec()));
        assert_eq!(
            replan(&empty_table, &[]).unwrap(),
            (false, vec![]),
            "a sealed plan"
        );
        let committed = [
            stored_task(0, "5", "10", Some("p0")),
            stored_task(1, "11", "20", None),
        ];
        assert_eq!(
            replan(&committed, &now).unwrap(),
            (false, vec![(1, 4), (21, 23)])
        );
        assert_eq!(replan(&committed, &[(5, 20)]).unwrap(), (false, vec![]));
        assert_eq!(replan(&committed, &[]).unwrap(), (false, vec![]));
        let said = replan(&[stored_task(0, "a", "9", Some("p0"))], &now).unwrap_err();
        assert!(
            said.to_string()
                .contains("hold a bound that is not an integer (chunk 0: \"a\"..\"9\")"),
            "{said}"
        );
    }

    /// Keys a stored window was cut away from are planned again, whether the hole is between two windows or the source moved away from the stored span; windows that share keys are never planned over.
    /// A window of one key is a window; one that ends before it starts is refused.
    #[test]
    fn a_window_of_one_key_is_stored_and_an_inverted_one_is_refused() {
        let one = [
            stored_task(0, "5", "5", Some("p0")),
            stored_task(1, "6", "9", None),
        ];
        assert_eq!(stored_windows(&one).unwrap(), vec![(5, 5), (6, 9)]);
        assert!(stored_windows(&[stored_task(0, "6", "5", None)]).is_err());
    }

    #[test]
    fn a_hole_in_the_stored_windows_is_planned_again_and_shared_keys_are_refused() {
        let now = [(1, 10), (11, 20), (21, 30)];
        let cut_short = [
            stored_task(0, "1", "10", Some("p0")),
            stored_task(1, "11", "13", None),
            stored_task(2, "21", "30", None),
        ];
        assert_eq!(replan(&cut_short, &now).unwrap(), (false, vec![(14, 20)]));
        let moved_away = [stored_task(0, "1", "10", Some("p0"))];
        assert_eq!(
            replan(&moved_away, &[(200, 250)]).unwrap(),
            (false, vec![(200, 250)]),
            "a gap between the stored span and the keys the source holds now is not damage"
        );
        assert_eq!(
            stored_windows(&cut_short),
            Ok(vec![(1, 10), (11, 13), (21, 30)])
        );
        for (tasks, why) in [
            (
                vec![
                    stored_task(0, "1", "10", Some("p0")),
                    stored_task(1, "8", "20", None),
                ],
                "put keys 8..=10 in two windows",
            ),
            (
                vec![
                    stored_task(0, "1", "30", Some("p0")),
                    stored_task(1, "11", "20", None),
                ],
                "put keys 11..=20 in two windows",
            ),
            (
                vec![stored_task(0, "9", "3", Some("p0"))],
                "hold a window that ends before it starts (chunk 0: 9..=3)",
            ),
        ] {
            assert_eq!(stored_windows(&tasks), Err(why.to_string()));
            assert!(replan(&tasks, &now).unwrap_err().to_string().contains(why));
        }
    }

    /// A source whose key spans 1..=230, with no NULL key and no column to drift.
    struct KeysTo230;

    impl crate::source::Source for KeysTo230 {
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unreachable!("planning reads no rows")
        }
        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
            let sql = sql.to_lowercase();
            let bound = [("min(", "1"), ("max(", "230")];
            let asked = bound.iter().find(|(f, _)| sql.contains(f));
            Ok(asked
                .filter(|_| !sql.contains("is null"))
                .map(|(_, v)| v.to_string()))
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    #[test]
    fn a_resume_detects_the_key_span_again_and_a_sealed_plan_plans_nothing() {
        let state = StateStore::open_in_memory().unwrap();
        let plan = make_plan("orders");
        let mut summary = make_summary(&plan, "run-1");
        let mut planned = |source: &ChunkSource| {
            resumed_chunk_plan(&mut KeysTo230, &plan, &state, &mut summary, source).unwrap()
        };
        assert_eq!(
            planned(&ChunkSource::Detect),
            vec![(1, 100), (101, 200), (201, 230)]
        );
        assert_eq!(planned(&ChunkSource::Precomputed(vec![(1, 100)])), vec![]);
    }

    /// A resume hands the keys the source holds past the stored plan to new tasks, and a sealed plan keeps its tasks.
    #[test]
    fn a_resume_extends_the_stored_plan_to_the_ranges_detected_now() {
        let state = StateStore::open_in_memory().unwrap();
        let mut plan = make_plan("orders");
        let cp = chunked_plan(&plan).clone();
        let mut summary = make_summary(&plan, "run-1");
        let claim = state.claim(plan.progress_key()).unwrap();
        let mut ensure = |plan: &ResolvedRunPlan, chunks: &[(i64, i64)]| {
            ensure_chunk_checkpoint_plan(&state, plan, &cp, &mut summary, chunks, "c.yaml", &claim)
                .unwrap()
        };
        let rid = ensure(&plan, &[(1, 10), (11, 20)]);
        state
            .complete_chunk_task(&rid, 0, 10, Some("p0.parquet"))
            .unwrap();
        plan.resume = true;
        let spans = |state: &StateStore| -> Vec<(String, String, String)> {
            let tasks = state.list_chunk_tasks_for_run(&rid).unwrap();
            tasks
                .into_iter()
                .map(|t| (t.start_key, t.end_key, t.status))
                .collect()
        };
        let task =
            |lo: &str, hi: &str, status: &str| (lo.to_string(), hi.to_string(), status.to_string());
        let stored = [task("1", "10", "completed"), task("11", "20", "pending")];
        assert_eq!(ensure(&plan, &[]), rid);
        assert_eq!(spans(&state), stored, "a sealed plan");
        assert_eq!(ensure(&plan, &[(1, 10), (11, 20), (21, 23)]), rid);
        let grown = [
            stored[0].clone(),
            stored[1].clone(),
            task("21", "23", "pending"),
        ];
        assert_eq!(spans(&state), grown);
        assert_eq!(ensure(&plan, &[(1, 10), (11, 20), (21, 23)]), rid);
        assert_eq!(spans(&state), grown, "the same span adds nothing");
    }

    /// Existing in-progress run *without* `--resume` → bail with a message
    /// that tells the operator exactly which recovery command to run.
    #[test]
    fn ensure_chunk_checkpoint_existing_run_without_resume_bails() {
        let state = StateStore::open_in_memory().unwrap();
        state
            .create_chunk_run("run-stuck", "orders", "ANY_HASH", 3)
            .unwrap();

        let plan = make_plan("orders");
        let cp = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => cp.clone(),
            _ => unreachable!(),
        };
        let mut summary = make_summary(&plan, "run-new");

        let err = ensure_chunk_checkpoint_plan(
            &state,
            &plan,
            &cp,
            &mut summary,
            &[],
            "rivet.yaml",
            &state.claim(plan.progress_key()).unwrap(),
        )
        .expect_err("existing run without resume must error");
        let msg = format!("{:#}", err);
        assert!(msg.contains("still in progress"), "got: {msg}");
        assert!(msg.contains("--resume"), "must hint at --resume");
        assert!(msg.contains("reset-chunks"), "must hint at reset-chunks");
    }

    /// A resume says how many chunk tasks it put back in the queue, and says nothing when it put back none.
    #[test]
    fn a_resume_reports_the_chunk_tasks_it_requeued_and_is_silent_when_it_requeued_none() {
        use crate::pipeline::run::run_tail_tests::{
            captured_warnings_mentioning, install_warn_capture,
        };
        install_warn_capture();
        const RANGES: [(i64, i64); 4] = [(1, 10), (11, 20), (21, 30), (31, 40)];
        let resumed = |export: &str, crashed_mid_chunk: bool| {
            let state = StateStore::open_in_memory().unwrap();
            let mut plan = make_plan(export);
            let cp = chunked_plan(&plan).clone();
            let mut summary = make_summary(&plan, "run-1");
            let claim = state.claim(plan.progress_key()).unwrap();
            let mut ensure = |plan: &ResolvedRunPlan| {
                ensure_chunk_checkpoint_plan(
                    &state,
                    plan,
                    &cp,
                    &mut summary,
                    &RANGES,
                    "c.yaml",
                    &claim,
                )
                .unwrap()
            };
            let rid = ensure(&plan);
            let claim_next = || state.claim_next_chunk_task(&rid).unwrap().unwrap().0;
            state
                .complete_chunk_task(&rid, claim_next(), 10, Some("p0.parquet"))
                .unwrap();
            if crashed_mid_chunk {
                state
                    .fail_chunk_task(&rid, claim_next(), "boom", false)
                    .unwrap();
                claim_next();
                claim_next();
            }
            plan.resume = true;
            assert_eq!(ensure(&plan), rid);
            let statuses: Vec<String> = state
                .list_chunk_tasks_for_run(&rid)
                .unwrap()
                .into_iter()
                .map(|t| t.status)
                .collect();
            assert_eq!(
                statuses,
                ["completed", "pending", "pending", "pending"],
                "{export}"
            );
            let mut said = captured_warnings_mentioning(&format!("export '{export}'"));
            said.retain(|line| line.contains("chunk task(s)"));
            said
        };

        assert_eq!(
            resumed("requeue_none", false),
            Vec::<String>::new(),
            "nothing was requeued, so there is nothing to report"
        );
        assert_eq!(
            resumed("requeue_some", true),
            [
                "export 'requeue_some': reset 2 stale 'running' chunk task(s) after resume",
                "export 'requeue_some': re-queued 1 failed chunk task(s) for retry after resume",
            ]
        );
    }

    /// A plain run that finds a chunk run left in progress (its process gone) runs as its resume; a run that already is a resume, and a plan of another mode, are left as they are.
    #[test]
    fn a_plain_run_over_a_chunk_run_left_in_progress_becomes_its_resume() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        let plan = make_plan("orders");
        let cp = chunked_plan(&plan).clone();
        let mut summary = make_summary(&plan, "run-1");
        let progress = state.claim(plan.progress_key()).unwrap();
        ensure_chunk_checkpoint_plan(
            &state,
            &plan,
            &cp,
            &mut summary,
            &[(1, 100), (101, 200)],
            "rivet.yaml",
            &progress,
        )
        .expect("the chunk run is opened");
        assert!(!plan.resume);
        let resumed = recover_checkpoint_run(&plan, &progress, false)
            .unwrap()
            .expect("the plain run takes the crashed run over");
        assert!(resumed.resume);
        assert_eq!(resumed.export_name, plan.export_name);

        let mut already = plan.clone();
        already.resume = true;
        assert!(
            recover_checkpoint_run(&already, &progress, false)
                .unwrap()
                .is_none()
        );
        let mut full = plan.clone();
        full.strategy = ExtractionStrategy::Snapshot;
        assert!(
            recover_checkpoint_run(&full, &progress, false)
                .unwrap()
                .is_none()
        );
    }

    /// A chunk run whose task list failed to land must not be left for a plain run to resume as a zero-chunk success.
    #[test]
    fn a_failed_task_insert_leaves_no_chunk_run_to_resume_as_an_empty_success() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateStore::open_at_path(&dir.path().join("state.db")).unwrap();
        let plan = make_plan("orders");
        let cp = chunked_plan(&plan).clone();
        let mut summary = make_summary(&plan, "run-1");
        state.exec_for_test(
            "CREATE TRIGGER fail_task BEFORE INSERT ON chunk_task \
             BEGIN SELECT RAISE(ABORT, 'injected task insert failure'); END;",
        );
        ensure_chunk_checkpoint_plan(
            &state,
            &plan,
            &cp,
            &mut summary,
            &[(1, 100), (101, 200)],
            "rivet.yaml",
            &state.claim(plan.progress_key()).unwrap(),
        )
        .expect_err("the task insert fails");
        state.exec_for_test("DROP TRIGGER fail_task;");

        let resumed =
            recover_checkpoint_run(&plan, &state.claim(plan.progress_key()).unwrap(), false)
                .unwrap();
        assert!(
            resumed.is_none(),
            "a plain run must start fresh, not resume a chunk run that has no tasks"
        );
        assert!(
            state
                .find_in_progress_chunk_run("orders")
                .unwrap()
                .is_none()
        );
    }

    /// A command that rewrites an export's progress is refused while a checkpointed run of that
    /// export holds its lease (naming the run and the way out), another export is unaffected,
    /// and the same command is admitted once the run lets go.
    #[test]
    fn a_progress_rewrite_is_refused_while_a_run_holds_the_exports_lease() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("state.db");
        let (run, cmd) = (
            StateStore::open_at_path(&db).unwrap(),
            StateStore::open_at_path(&db).unwrap(),
        );
        let plan = make_plan("orders");
        let lease = claim_run(&run, &plan).expect("a run takes the run lease");
        let source = plan.source.state_key();

        let unnamed = claim_export_progress(&cmd, "orders", &source, "reset its state")
            .err()
            .expect("refused while the run is alive");
        assert!(
            unnamed.to_string().contains(": a run is in progress"),
            "{unnamed:#}"
        );
        run.begin_run(
            "orders_20260101T000000.000_4242",
            "orders",
            "file:///out",
            "2026-01-01T00:00:00Z",
        )
        .unwrap();
        run.record_run_source("orders_20260101T000000.000_4242", &source)
            .unwrap();
        let err = claim_export_progress(&cmd, "orders", &source, "reset its state")
            .err()
            .expect("refused while the run is alive");
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_STATE_RUN_IN_PROGRESS")
        );
        assert_eq!(
            err.to_string(),
            "export 'orders': cannot reset its state: run 'orders_20260101T000000.000_4242' is \
             in progress in a live rivet process, which holds the export's run lease. Wait for \
             it to finish, or stop that process (its pid ends the run id), then repeat this \
             command."
        );
        assert!(
            claim_export_progress(&cmd, "invoices", &source, "reset its state").is_ok(),
            "the lease is per export"
        );
        assert!(
            claim_export_progress(
                &cmd,
                "orders",
                "postgres://another.host/db",
                "reset its state"
            )
            .is_ok(),
            "the lease is per source"
        );

        drop(lease);
        assert!(
            claim_export_progress(&cmd, "orders", &source, "reset its state").is_ok(),
            "admitted once the run has let go"
        );
    }

    // ── record_chunked_commit ─────────────────────────────────────────────

    /// Picks the highest `chunk_index` among completed tasks and persists it
    /// as the committed boundary.
    #[test]
    fn record_chunked_commit_picks_highest_completed_chunk() {
        let state = StateStore::open_in_memory().unwrap();
        state.create_chunk_run("run-a", "orders", "h", 3).unwrap();
        state
            .insert_chunk_tasks("run-a", &[(1, 10), (11, 20), (21, 30)])
            .unwrap();
        state
            .complete_chunk_task("run-a", 0, 10, Some("c0.parquet"))
            .unwrap();
        state
            .complete_chunk_task("run-a", 2, 30, Some("c2.parquet"))
            .unwrap();

        record_chunked_commit(&state, "orders", "run-a");

        let p = state.get_progression("orders").unwrap();
        let boundary = p.committed.expect("must have committed boundary");
        assert_eq!(boundary.strategy, "chunked");
        assert_eq!(
            boundary.chunk_index,
            Some(2),
            "committed boundary must be the highest completed chunk index"
        );
    }

    /// No completed tasks at all: function must be a silent no-op
    /// (must NOT crash, must NOT write a bogus -1 sentinel).
    #[test]
    fn record_chunked_commit_with_no_completed_tasks_is_noop() {
        let state = StateStore::open_in_memory().unwrap();
        state.create_chunk_run("run-b", "orders", "h", 3).unwrap();
        state
            .insert_chunk_tasks("run-b", &[(1, 10), (11, 20)])
            .unwrap();

        record_chunked_commit(&state, "orders", "run-b");

        let p = state.get_progression("orders").unwrap();
        assert!(
            p.committed.is_none(),
            "no completed tasks → no committed boundary"
        );
    }

    /// A connection given back is the one the next chunk takes, so no new one is dialled; with none idle the take dials.
    #[test]
    fn a_connection_given_back_is_taken_again_without_a_new_dial() {
        let mut cfg = make_plan("orders").source;
        cfg.url = Some("postgresql://nobody@127.0.0.1:1/x".into());
        let idle = IdleSources::default();
        assert!(idle.take(&cfg).is_err(), "nothing idle: the take must dial");
        idle.give(Box::new(KeysTo230));
        let mut reused = idle.take(&cfg).expect("the idle connection, not a dial");
        assert_eq!(
            reused.query_scalar("SELECT max(id) FROM orders").unwrap(),
            Some("230".to_string())
        );
        assert!(
            idle.take(&cfg).is_err(),
            "a taken connection is no longer idle"
        );
    }

    #[test]
    fn an_unfinished_chunk_run_is_refused_by_code_raced_or_not() {
        let err = chunk_run_unfinished("orders", "r1", "", false);
        crate::error::assert_coded(&err, "RIVET_STATE_CHUNK_RUN_UNFINISHED", 5);
        let plain = format!("{err}");
        assert!(
            plain.contains("--config <CONFIG> --export orders --resume"),
            "{plain}"
        );
        assert!(!plain.contains("won the race"));
        let raced = format!("{}", chunk_run_unfinished("orders", "r1", "c.yaml", true));
        assert!(raced.contains("(a concurrent run won the race)"), "{raced}");
    }
}
