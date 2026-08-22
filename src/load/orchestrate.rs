//! The `rivet load` ORCHESTRATOR — evicted wholesale from `cli/dispatch.rs`
//! (arch-roast 2026-08-21, Strong, challenge-confirmed).
//!
//! dispatch.rs promises "every arm is a thin adapter... call exactly one
//! function", and 12 of 13 commands honored it — Load embedded ~800 lines of
//! business logic in the router: plan resolution, the active-run/GC guards,
//! the per-mode load drivers (full / incremental / CDC), the LoadCtx ledger,
//! and failure aggregation. It now lives beside the load layer it drives; the
//! dispatch arm is one call again (`run_loads`).
//!
//! Moved verbatim (the bughunt over the sibling split graded transplant
//! fidelity line-by-line; this move is the same discipline): behavior deltas
//! are none, the tests moved with their subjects.

use crate::error::Result;
use crate::load;
use crate::state::{LoadRecord, StateStore};

pub struct LoadArgs {
    pub config: String,
    pub run_id: Option<String>,
}

/// `rivet load`: config-driven warehouse load. The top-level `load:` block
/// declares the target once, and each export resolves to a table. A multi-table
/// config loads every export into the shared target, one after another.
pub fn run_loads(args: LoadArgs) -> Result<()> {
    let plans = load::plan::plan_loads(&args.config)?;
    // One run id for the whole invocation, shared across every table — so warehouse
    // cost slices per load run (all tables together) as well as per table.
    let run_id = resolve_run_id(args.run_id.clone());
    // The load ledger: the state DB — not the file prefix — is the source of
    // truth for what's loaded, so cleanup is safe for every mode and retry is
    // DB-driven (the GCS listing is only a fallback). A state-DB problem must
    // never fail a load — degrade to the stateless path.
    let state = match StateStore::open(&args.config) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!(
                "  warning: state store unavailable ({e:#}); loading without a ledger \
                 (no incremental skip / audit log)"
            );
            None
        }
    };
    let tables: Vec<&str> = plans.iter().map(|p| p.table.as_str()).collect();
    eprintln!(
        "{}: resolved {} table(s) → {} [run_id={}]: {}",
        args.config,
        plans.len(),
        plans.first().map(|p| p.load.target.name()).unwrap_or("?"),
        run_id,
        tables.join(", ")
    );

    // The `__pos` parse engine is config-level — resolve it once, and only if a
    // table actually needs it (a `mode: cdc` export).
    let engine = if needs_source_engine(&plans) {
        Some(load::plan::source_engine(&args.config)?)
    } else {
        None
    };
    // Route each table by its declared `mode:`; `pk:` and `allow_source_drift:`
    // come from the `load:` block, so the CLI carries no per-mode flags.
    // Per-table FAULT ISOLATION, mirroring `rivet run` (pipeline/run.rs): collect
    // failures and keep going, then aggregate. A `?` inside this loop abandoned
    // every LATER table in the config — silently, since a table that never ran
    // gets no ledger row either, so `rivet state loads` cannot tell "failed" from
    // "never attempted". The durable trigger is a per-table PERMANENT error
    // raised before the run closure (`open_store`, `prepare_load` — which carries
    // `ensure_single_export` and `reconcile`), so one poisoned prefix starved
    // every other table, every cycle, indefinitely. The CLI reference already
    // promised "loads every export into the shared target, one after another".
    let mut failures: Vec<anyhow::Error> = Vec::new();
    for plan in &plans {
        let load_id = format!("{run_id}:{}", plan.table);
        let drift = plan.load.allow_source_drift;
        let outcome = (|| -> Result<()> {
            match plan.mode {
                // CDC: APPEND the change log + rebuild the current-state dedup view.
                load::plan::LoadMode::Cdc => {
                    let pk = require_pk(plan, "cdc")?;
                    match load_one_cdc(
                        plan,
                        &run_id,
                        engine.expect("engine resolved above for a cdc plan"),
                        pk,
                        drift,
                        state.as_ref(),
                        &load_id,
                    )? {
                        Some(report) => println!("CDC LOAD OK [{}]: {report:#?}", plan.table),
                        None => println!("CDC LOAD SKIP [{}]: up to date", plan.table),
                    }
                }
                // Incremental: APPEND the delta + a cursor-ordered current-state view.
                load::plan::LoadMode::Incremental => {
                    let pk = require_pk(plan, "incremental")?;
                    match load_one_incremental(plan, &run_id, pk, drift, state.as_ref(), &load_id)?
                    {
                        Some(report) => {
                            println!("INCREMENTAL LOAD OK [{}]: {report:#?}", plan.table)
                        }
                        None => println!("INCREMENTAL LOAD SKIP [{}]: up to date", plan.table),
                    }
                }
                // Full/chunked: ledger-driven latest-run OVERWRITE.
                //
                // Named, not a `_` catch-all: this match is the mode ROUTER, and
                // the in-diff mutation gate reported both of the arms above alive
                // as `delete match arm …` — a deleted arm fell through to `_` and
                // silently loaded a CDC change log as a full-snapshot OVERWRITE.
                // Exhaustive over `LoadMode`, the arm deletions stop compiling
                // (the mutants are unviable rather than uncaught) and a NEW mode
                // has to be routed deliberately instead of inheriting this one.
                load::plan::LoadMode::Full => {
                    match load_one(plan, &run_id, drift, state.as_ref(), &load_id)? {
                        Some(report) => println!("LOAD OK [{}]: {report:#?}", plan.table),
                        None => println!("LOAD SKIP [{}]: up to date", plan.table),
                    }
                }
            }
            Ok(())
        })();
        if let Err(e) = outcome {
            // Name the table on the way out: the aggregate must say WHICH load
            // failed, or an operator reading a mixed batch cannot act on it.
            eprintln!("  LOAD FAILED [{}]: {e:#}", plan.table);
            failures.push(e.context(format!("load '{}'", plan.table)));
            continue;
        }
        if plan.load.gc_orphans {
            // The store is opened HERE rather than inside `maybe_gc_orphans` so
            // the GC body itself takes a store and is offline-testable against a
            // filesystem-backed one (`GcsStore::open_fs`) — its whole-function
            // stub was one of the in-diff gate's misses, and a stubbed orphan GC
            // is a delete that silently stops happening.
            match load::open_store(&plan.destination) {
                Ok(store) => maybe_gc_orphans(&store, plan, state.as_ref()),
                Err(e) => eprintln!(
                    "  gc-orphans [{}]: skipped (store unavailable): {e:#}",
                    plan.table
                ),
            }
        }
    }
    match aggregate_load_failures(failures) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Does this invocation have to resolve the source's `__pos` parse ENGINE?
///
/// Only a `mode: cdc` table needs it, and resolving it opens the source config —
/// so the answer decides whether a pure-batch load touches the source at all.
/// Pure because [`run_loads`] is live-only glue the in-diff mutation gate cannot
/// grade: it reported this comparison alive (`replace == with != in run_loads`),
/// and inverted it resolves an engine for every BATCH config while leaving every
/// CDC config with `None` — where `engine.expect(..)` then PANICS on exactly the
/// configs the parse engine exists for.
fn needs_source_engine(plans: &[load::plan::LoadPlan]) -> bool {
    plans.iter().any(|p| p.mode == load::plan::LoadMode::Cdc)
}

/// Fold every per-plan failure into ONE error, or `None` when nothing failed.
///
/// Extracted so a test can call the REAL producer instead of re-typing the fold
/// into its own body. The test that guarded this used to build three errors,
/// re-implement `remove(idx)` + the `others` join + this exact format string, and
/// assert on the string IT had produced — so putting `?` back on the first
/// failure (the fault-isolation regression it was written to catch) left it
/// green. It held both sides of the comparison.
///
/// Same aggregation shape as `rivet run`: carry a representative TYPED failure so
/// `classify_exit` still downcasts the marker through anyhow's context chain, and
/// list the rest as context.
pub(crate) fn aggregate_load_failures(mut failures: Vec<anyhow::Error>) -> Option<anyhow::Error> {
    if failures.is_empty() {
        return None;
    }
    let primary_idx = crate::pipeline::run::representative_failure_idx(&failures)?;
    let primary = failures.remove(primary_idx);
    if failures.is_empty() {
        return Some(primary);
    }
    let others = failures
        .iter()
        .map(|e| format!("{e:#}"))
        .collect::<Vec<_>>()
        .join("; ");
    Some(primary.context(format!(
        "{} load(s) failed; representative error follows (also: {others})",
        failures.len() + 1
    )))
}

/// The dedup view's primary key for an append mode (`cdc` / `incremental`), read
/// from the export's `load:` block. Bails with a config-fix hint when absent.
fn require_pk<'a>(plan: &'a load::plan::LoadPlan, mode: &str) -> Result<&'a [String]> {
    if plan.load.pk.is_empty() {
        anyhow::bail!(
            "export `{}` is mode: {mode} but its `load:` block has no `pk:` — the current-state \
             dedup view needs a primary key (e.g. `pk: [id]`)",
            plan.export_name
        );
    }
    Ok(&plan.load.pk)
}

/// What the run-status LEDGER says about a prefix, folded from the three answers
/// it can give. Pure, because the fold IS the decision and both its callers are
/// live-only bodies (a real bucket, a real state DB) that the in-diff mutation
/// gate reports MISSED whatever the assertions say.
///
/// * `None` — there is no state store to ask (a stateless or foreign-host load).
///   NOT active: the manifest signal decides alone, exactly as the orphan path
///   does.
/// * `Some(Err(_))` — the query failed. ACTIVE, conservatively: a delete that
///   spares too much costs disk, while one that removes a live run's committed
///   parts costs data — and on a CDC/incremental export the source position has
///   already advanced past them.
/// * `Some(Ok(b))` — the ledger's own answer, used as given.
fn ledger_says_active(answer: Option<Result<bool>>) -> bool {
    match answer {
        None => false,
        Some(Ok(active)) => active,
        Some(Err(_)) => true,
    }
}

/// Fold the two INDEPENDENT activity signals into one verdict: the run-status
/// ledger (precise when this load shares the extract's state — co-located /
/// shared Postgres) and a `running` MARKER manifest projected into the bucket
/// (the cross-boundary signal a stateless / foreign-host load reads when it
/// cannot see the extract's state DB).
///
/// Either one alone must be enough to spare the prefix: they answer for
/// DIFFERENT deployments, so each is structurally silent where the other speaks.
/// An `&&` here would demand agreement from a signal that cannot give it and
/// delete a live run's committed parts — and that is precisely the mutant the
/// in-diff gate reported alive (`replace || with && in maybe_gc_orphans`), in a
/// body no offline test can reach.
fn prefix_is_active(ledger_active: bool, manifest_active: bool) -> bool {
    ledger_active || manifest_active
}

/// Is a run writing into this prefix right now?
///
/// The verdict `maybe_gc_orphans` already computes, extracted so the DESTRUCTIVE
/// delete can ask the same question. `cleanup_source` recursively removes the
/// whole prefix — every part, every manifest, `_SUCCESS` — while `gc_orphans`
/// removes only parts no `Success` manifest references. The gentler of the two
/// consulted the ledger and the total one did not, which is the guard placed in
/// inverse proportion to what it protects. `src/load/plan.rs` states the
/// relationship in its own words: gc_orphans is "strictly gentler than
/// `cleanup_source`, which wipes the whole prefix".
///
/// Conservative in both directions, deliberately — see [`ledger_says_active`]
/// for what each ledger answer means and [`prefix_is_active`] for why the two
/// signals fold with `||`.
///
/// Takes the store the CALLER already opened instead of re-opening one from
/// `plan.destination` (the two were always the same object). That makes this
/// guard reachable from an offline test over a filesystem-backed store, which is
/// the whole reason its `-> true` / `-> false` body stubs no longer need a
/// mutation-config exclusion: a guard on a recursive delete should not be
/// gradable only against a real bucket.
fn prefix_has_active_run(
    store: &crate::destination::gcs::GcsStore,
    prefix: &str,
    state: Option<&StateStore>,
) -> bool {
    let ledger_active = ledger_says_active(state.map(|s| s.has_active_run_on_prefix(prefix)));
    if ledger_active {
        // Short-circuit: the manifest signal costs a bucket LISTING and cannot
        // change a `true` — `prefix_is_active(true, _)` is `true` either way.
        return true;
    }
    let manifest_active = match load::reconcile::fetch_manifests_keyed(store, prefix) {
        Ok(keyed) => load::reconcile::has_active_running_manifest(&keyed),
        // Cannot read the manifests → cannot rule a live run out. Spare.
        Err(_) => true,
    };
    prefix_is_active(ledger_active, manifest_active)
}

/// Whether `cleanup_source` actually deletes a prefix — the THREE outcomes the
/// caller's `Option` collapses into one `None`.
///
/// Separated from the `Option` because "nobody asked" and "asked, and REFUSED
/// because a run is writing here" are the same value to the caller and very
/// different things to an operator, and because the `!` in front of the request
/// flag is a decision the in-diff gate reported alive (`delete ! in
/// cleanup_target`) inside a live-only body. Inverted, it deletes the whole
/// prefix for every config that did NOT ask for cleanup and spares every config
/// that did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupVerdict {
    /// `cleanup_source` is off — no delete, and nothing to announce.
    NotRequested,
    /// Cleanup was requested but a run is writing into the prefix.
    RefusedRunActive,
    /// Delete the staged Parquet under the prefix.
    Delete,
}

/// The verdict, pure. `active` is a CLOSURE so the activity probe — a state-DB
/// query plus a bucket listing — still runs ONLY when cleanup was requested,
/// exactly as the `if` chain it replaced did.
fn cleanup_verdict(requested: bool, active: impl FnOnce() -> bool) -> CleanupVerdict {
    if !requested {
        return CleanupVerdict::NotRequested;
    }
    if active() {
        return CleanupVerdict::RefusedRunActive;
    }
    CleanupVerdict::Delete
}

/// The delete target for `cleanup_source`, or `None` when a run is writing here.
///
/// Refusing is announced, not silent: an operator who asked for cleanup and did
/// not get it must know the prefix still holds the staged Parquet.
fn cleanup_target<'a>(
    plan: &'a load::plan::LoadPlan,
    store: &'a crate::destination::gcs::GcsStore,
    state: Option<&StateStore>,
) -> Option<(&'a crate::destination::gcs::GcsStore, &'a str)> {
    match cleanup_verdict(plan.load.cleanup_source, || {
        prefix_has_active_run(store, &plan.gcs_prefix, state)
    }) {
        CleanupVerdict::NotRequested => None,
        CleanupVerdict::RefusedRunActive => {
            eprintln!(
                "  cleanup [{}]: SKIPPED — a run is writing into {} right now. Deleting the \
                 prefix would remove parts that run has already committed, and on a \
                 CDC/incremental export the source position has advanced past them. Re-run the \
                 load once the extract has finished.",
                plan.table, plan.gcs_prefix
            );
            None
        }
        CleanupVerdict::Delete => Some((store, plan.gcs_prefix.as_str())),
    }
}

/// Best-effort orphan-Parquet GC for one table's prefix (config `gc_orphans`):
/// delete staged `.parquet` no `Success` manifest references — an interrupted
/// extract's leftovers. A GC failure only warns; it NEVER fails the load, which
/// already succeeded before this runs.
///
/// Gated on whether a run is ACTIVE on the prefix ([`prefix_is_active`]), so it
/// never deletes a CONCURRENT extract's committed-but-not-yet-manifested parts.
/// Only when NEITHER signal says active does a no-manifest part count as dead
/// crash debris.
fn maybe_gc_orphans(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
) {
    let keyed = match load::reconcile::fetch_manifests_keyed(store, &plan.gcs_prefix) {
        Ok(k) => k,
        Err(e) => {
            eprintln!(
                "  gc-orphans [{}]: skipped (manifest fetch failed): {e:#}",
                plan.table
            );
            return;
        }
    };
    // A query ERROR stays conservative (assume active → spare); a clean `false`
    // (no running row) lets the manifest signal decide.
    let ledger_active =
        ledger_says_active(state.map(|s| s.has_active_run_on_prefix(&plan.gcs_prefix)));
    let active = prefix_is_active(
        ledger_active,
        load::reconcile::has_active_running_manifest(&keyed),
    );
    match load::reconcile::gc_orphans(store, &plan.gcs_prefix, &keyed, active) {
        Ok((0, _)) => {}
        Ok((n, bytes)) => {
            println!(
                "  gc-orphans [{}]: removed {n} orphan part(s) ({bytes} bytes)",
                plan.table
            )
        }
        Err(e) => eprintln!(
            "  gc-orphans [{}]: failed (load unaffected): {e:#}",
            plan.table
        ),
    }
}

/// What a load will consume: the reconciled integrity, the parquet URIs to load,
/// and the extraction run_ids covered. `None` from [`prepare_load`] means the
/// ledger already has every run — nothing new to load.
struct LoadInputs {
    integrity: load::reconcile::LoadIntegrity,
    uris: Vec<String>,
    source_run_ids: Vec<String>,
    /// `engine:schema.table` of the manifests this load consumes — recorded so a
    /// LATER load of the same warehouse table from a DIFFERENT database can be
    /// refused instead of silently replacing these rows.
    source_ident: String,
}

/// The prior source identity that CONFLICTS with the one this load carries, or
/// `None` when the warehouse table may accept these rows.
///
/// THE WAREHOUSE TABLE BELONGS TO ONE SOURCE — this is the comparison that says
/// so, and both of its operators were reported alive by the in-diff mutation
/// gate inside live-only [`prepare_load`] (`delete !` and `replace != with ==`).
/// Each inverts the guard into its own opposite: dropping the `!` refuses every
/// load whose manifests carry NO identity (i.e. every artifact written before
/// the ledger recorded one — an upgrade that starts refusing loads that were
/// fine yesterday), and `==` refuses a load from the SAME source it always came
/// from while waving through the cross-source overwrite the guard exists to
/// stop, both commands reporting success.
fn conflicting_source_ident<'a>(mine: &str, prior: &'a [String]) -> Option<&'a String> {
    if mine.is_empty() {
        // Rows written before the ledger carried the identity read as UNKNOWN
        // and never block.
        return None;
    }
    prior.iter().find(|p| p.as_str() != mine)
}

/// Reconcile the manifests under a load's prefix into its [`LoadInputs`],
/// mode-aware and ledger-filtered.
///
/// The mode→run selection ([`load::reconcile::select_runs`]) runs on BOTH the
/// stateful and stateless paths — they differ ONLY in whether `loaded` is the
/// ledger's set or empty. So Full always OVERWRITEs with the LATEST run (a
/// stateless Full never blanket-loads every accumulated snapshot = the
/// duplicate-rows bug), and Incremental/Cdc append the not-yet-loaded runs
/// (all of them when stateless — absorbed by the dedup view). `Ok(None)` = an
/// empty selection (nothing new / empty staging → the caller no-ops).
fn prepare_load(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    target_fqtn: &str,
    allow_source_drift: bool,
) -> Result<Option<LoadInputs>> {
    let keyed = load::reconcile::fetch_manifests_keyed(store, &plan.gcs_prefix)?;
    // Refuse a prefix shared by two exports BEFORE selecting/summing/cleaning:
    // the load sums every manifest here and cleanup wipes the prefix recursively,
    // so a shared base prefix would cross-contaminate the count and delete a
    // sibling export's parts (there is no source export_name on the plan to
    // disambiguate). Covers Full (wrong-export snapshot pick), incremental, and
    // CDC in one place, before any irreversible step.
    load::reconcile::ensure_single_export(&keyed)?;
    // THE WAREHOUSE TABLE BELONGS TO ONE SOURCE.
    //
    // `ensure_single_export` above refuses two sources sharing a PREFIX. Two
    // configs with SEPARATE prefixes pointed at one `dataset.table` get past it
    // and the second load simply replaces the first's rows — both reporting
    // success, because nothing recorded where the existing rows came from. The
    // ledger now does, so the mismatch is answerable.
    //
    // Refuse rather than warn: by the time a load runs, the alternative is
    // deleting someone else's data. Rows written before the ledger carried the
    // identity read as unknown and never block — an upgrade must not start
    // refusing loads that were fine yesterday.
    if let Some(s) = state
        && let Some((_, m)) = keyed.first()
    {
        let mine = crate::manifest::identity_source(m);
        if let Ok(prior) = s.loaded_source_idents(target_fqtn)
            && let Some(other) = conflicting_source_ident(&mine, &prior)
        {
            anyhow::bail!(
                "target table `{target_fqtn}` was last loaded from `{other}` and this load \
                 carries `{mine}` — loading would REPLACE the other source's rows, and both \
                 commands would report success. Name a different `dataset:`/table for this \
                 source, or load them into one table deliberately by giving them one export \
                 name and one prefix."
            );
        }
    }
    // The ledger's already-loaded run_ids — empty when stateless (no state DB),
    // so `select_runs` degrades safely rather than dropping the mode selection.
    let loaded = match state {
        Some(s) => s.loaded_source_run_ids(target_fqtn).unwrap_or_default(),
        None => std::collections::HashSet::new(),
    };
    let new = load::reconcile::select_runs(keyed, &loaded, plan.mode)?;
    if new.is_empty() {
        return Ok(None);
    }
    let manifests: Vec<_> = new.iter().map(|(_, m)| m.clone()).collect();
    let integrity = load::reconcile::reconcile(&manifests, allow_source_drift)?;
    let uris = load::reconcile::select_load_uris(store, &plan.gcs_prefix, &new)?;
    let source_run_ids: Vec<String> = new.iter().map(|(_, m)| m.run_id.clone()).collect();
    if uris.is_empty() {
        // Unloaded manifests that resolve to NO files: runs that legitimately
        // produced nothing (a CDC cycle with no changes, the anchor cycle of
        // `initial: snapshot`). That is "up to date", not an error — the loader
        // bails deeper down with "no Parquet URIs to append", which surfaced as a
        // failed load the moment a zero-part manifest stopped dragging the whole
        // prefix in behind it.
        //
        // NOT recorded consumed: the caller's skip path records no run ids, so
        // an empty cycle is re-evaluated on every later load. Harmless (it
        // resolves to nothing again and skips again) but it does mean the skip
        // set omits runs that are, in fact, fully consumed — recording them is
        // the follow-up.
        println!(
            "  {} → {}: {} run(s) produced no files — nothing to load",
            plan.table,
            plan.load.target.name(),
            source_run_ids.len()
        );
        return Ok(None);
    }
    // The manifests agree on their source — `ensure_single_export` refused the
    // prefix otherwise — so the first one speaks for all of them.
    let source_ident = new
        .first()
        .map(|(_, m)| crate::manifest::identity_source(m))
        .unwrap_or_default();
    Ok(Some(LoadInputs {
        integrity,
        uris,
        source_run_ids,
        source_ident,
    }))
}

/// The inputs every load shares; the full/incremental/CDC specifics are the three
/// closures [`execute_load`] takes. `mode` is the load strategy — its
/// [`LoadMode::ledger_str`] is the ledger's `mode` discriminator.
struct LoadJob<'a> {
    plan: &'a load::plan::LoadPlan,
    run_id: &'a str,
    state: Option<&'a StateStore>,
    load_id: &'a str,
    allow_source_drift: bool,
    mode: load::plan::LoadMode,
}

/// The audit + skip-ledger writer for one load. A struct (not a bare closure) so
/// the "which exit path writes which ledger row" invariant is unit-testable with
/// an in-memory [`StateStore`] — no live warehouse or bucket.
struct LoadCtx<'a> {
    state: Option<&'a StateStore>,
    load_id: &'a str,
    export_name: &'a str,
    target_fqtn: &'a str,
    warehouse: &'a str,
    mode: load::plan::LoadMode,
    /// The source prefix this load consumes — needed to ask the ledger which
    /// runs are still WRITING into it, so their (still-growing) manifests are
    /// not recorded as fully consumed.
    source_prefix: &'a str,
    /// Set once the manifests are known (after `prepare_load`), so the ledger row
    /// records WHERE the rows came from and not merely that they arrived.
    source_ident: String,
}

/// The source runs this load may record as CONSUMED: everything it read, MINUS
/// the runs still WRITING into the prefix.
///
/// Pure, and the `!` is the whole rule. A run still active can still GROW its
/// manifest (the CDC sink rewrites a `Success` superset at every commit-boundary
/// roll under ONE run_id), and the skip set is keyed on the run_id alone — so
/// recording an in-flight run as consumed strands every part it writes
/// afterwards, permanently and silently. Inverted, this records ONLY the
/// in-flight runs and re-loads every finished one forever: both directions are
/// data-visible and neither changes a row count.
fn consumable_run_ids(
    source_run_ids: &[String],
    active: &std::collections::HashSet<String>,
) -> Vec<String> {
    source_run_ids
        .iter()
        .filter(|id| !active.contains(*id))
        .cloned()
        .collect()
}

/// The operator note for source runs still writing into the prefix — `None` when
/// there are none, so the caller prints nothing rather than a note about zero
/// runs. Pure: the `is_empty` guard lives in a body (`LoadCtx::record`) whose
/// only offline fixtures have an EMPTY active set, which is exactly the state
/// that cannot tell the guard from its inverse.
fn active_run_note(active_runs: usize, prefix: &str) -> Option<String> {
    if active_runs == 0 {
        return None;
    }
    Some(format!(
        "  note: {active_runs} source run(s) still writing into {prefix} — loaded now, kept \
         retryable so their later parts are not skipped"
    ))
}

impl LoadCtx<'_> {
    /// Best-effort ledger write — a state-DB failure warns but never fails a load.
    fn record(&self, source_run_ids: &[String], rows_loaded: i64, status: &str) {
        let Some(s) = self.state else { return };
        // A run still ACTIVE on this prefix can still GROW its manifest: the CDC
        // sink rewrites a `Success` superset at every commit-boundary roll under
        // ONE run_id, and `list_manifest_keys` deliberately prefers that
        // run-unique copy. The skip set is keyed on the run_id ALONE, so
        // recording an in-flight run as consumed strands every part it writes
        // afterwards — permanently, and silently: the next load prints
        // "up to date". With `until_current: false` the id never rotates, so the
        // loss is unbounded.
        //
        // Excluding exactly the active runs leaves them retryable while every
        // terminal run is still recorded (so a completed run is never
        // re-loaded). Their parts ARE loaded now — re-appending them next cycle
        // is at-least-once, which the current-state view absorbs: it keeps
        // ROW_NUMBER() … = 1 per pk, so a duplicated change row cannot change
        // what the view reports.
        //
        // A stateless or foreign-host load has no ledger to ask (`self.state` is
        // None above, or the query fails) — see `warn_if_racing_an_active_run`,
        // which tells that operator to load AFTER the extract instead.
        // A query failure must fail SAFE, and "safe" here is the opposite of the
        // default: an empty set excludes nothing, so every in-flight run gets
        // recorded as consumed and every part it writes afterwards is skipped
        // forever — the harm the comment above describes. Treating the answer as
        // "assume they are all active" records none of them, and the next cycle
        // re-evaluates: at-least-once, which the current-state view absorbs.
        let active = match s.active_run_ids_on_prefix(self.source_prefix) {
            Ok(a) => a,
            Err(e) => {
                log::warn!(
                    "load: cannot tell which runs are still writing into {} ({e:#}) — not \
                     recording any run as consumed this cycle, so nothing they write later is \
                     stranded. The next load re-evaluates them.",
                    self.source_prefix
                );
                source_run_ids.iter().cloned().collect()
            }
        };
        let source_run_ids = consumable_run_ids(source_run_ids, &active);
        if let Some(note) = active_run_note(active.len(), self.source_prefix) {
            eprintln!("{note}");
        }
        let source_run_ids = &source_run_ids[..];
        let rec = LoadRecord {
            source_ident: self.source_ident.clone(),
            load_id: self.load_id.to_string(),
            export_name: self.export_name.to_string(),
            target_table: self.target_fqtn.to_string(),
            warehouse: self.warehouse.to_string(),
            mode: self.mode.ledger_str().to_string(),
            source_run_ids: source_run_ids.to_vec(),
            rows_loaded,
            status: status.to_string(),
            finished_at: chrono::Utc::now().to_rfc3339(),
        };
        if let Err(e) = s.store_load(&rec) {
            eprintln!("  warning: load ledger write failed (load itself proceeded): {e:#}");
        }
    }
    /// Nothing new to load — the ledger already covers every run.
    fn record_skip(&self) {
        self.record(&[], 0, "success");
    }
    /// The load errored after consuming `run_ids`.
    fn record_failed(&self, run_ids: &[String]) {
        self.record(run_ids, 0, "failed");
    }
    /// The load appended/loaded `rows` from `run_ids`.
    fn record_success(&self, run_ids: &[String], rows: i64) {
        self.record(run_ids, rows, "success");
    }
}

/// How the "up to date — every extraction run already loaded" line names this
/// load. Pure so the mode fork is graded: it sat in live-only [`execute_load`]
/// and the in-diff gate reported its `==` alive, which swaps the two labels and
/// tells an operator watching a CDC drain that a plain `load` is up to date —
/// the one line they have to reason about a stalled change stream.
fn up_to_date_label(mode: load::plan::LoadMode) -> &'static str {
    match mode {
        load::plan::LoadMode::Cdc => "cdc load",
        load::plan::LoadMode::Full | load::plan::LoadMode::Incremental => "load",
    }
}

/// The shared load envelope: open the store, build the loader, reconcile via the
/// ledger, then run + record. Batch vs CDC differ ONLY in `progress` (the
/// per-load log line), `run` (the load call, returning its row count + report),
/// and `done` (the success trace). Every exit path records the load EXACTLY once
/// — skip ⇒ `success`/0, run-`Err` ⇒ `failed`, run-`Ok` ⇒ `success`/rows — the
/// ledger invariant in one place instead of copy-pasted across batch and CDC.
fn execute_load<R>(
    job: LoadJob<'_>,
    progress: impl FnOnce(&LoadInputs),
    run: impl FnOnce(
        &dyn load::TargetLoader,
        &crate::destination::gcs::GcsStore,
        &LoadInputs,
    ) -> Result<(u64, R)>,
    done: impl FnOnce(&LoadInputs, &R),
) -> Result<Option<R>> {
    let store = load::open_store(&job.plan.destination)?;
    let loader = load::build_loader(job.plan, job.run_id);
    let target_fqtn = loader.fqtn(&job.plan.table);
    let mut ctx = LoadCtx {
        state: job.state,
        load_id: job.load_id,
        export_name: job.plan.table.as_str(),
        target_fqtn: target_fqtn.as_str(),
        warehouse: job.plan.load.target.name(),
        mode: job.mode,
        source_prefix: job.plan.gcs_prefix.as_str(),
        source_ident: String::new(),
    };
    let inputs = match prepare_load(
        &store,
        job.plan,
        job.state,
        &target_fqtn,
        job.allow_source_drift,
    )? {
        Some(i) => {
            ctx.source_ident = i.source_ident.clone();
            i
        }
        None => {
            let label = up_to_date_label(job.mode);
            eprintln!(
                "  {label} {} → {}: up to date — every extraction run already loaded",
                job.plan.table,
                job.plan.load.target.name(),
            );
            ctx.record_skip();
            return Ok(None);
        }
    };
    progress(&inputs);
    let (rows, report) = match run(&*loader, &store, &inputs) {
        Ok(v) => v,
        Err(e) => {
            ctx.record_failed(&inputs.source_run_ids);
            return Err(e);
        }
    };
    ctx.record_success(&inputs.source_run_ids, rows as i64);
    done(&inputs, &report);
    Ok(Some(report))
}

/// The `(source cleaned)` suffix: a load that deleted its staged Parquet says so,
/// because the prefix an operator would go looking at afterwards is now empty.
fn cleaned_suffix(source_cleaned: bool) -> &'static str {
    if source_cleaned {
        " (source cleaned)"
    } else {
        ""
    }
}

/// The success trace shared by the CDC + incremental loads (byte-identical): the
/// integrity chain, the appended rows, the change-log table, and the view.
///
/// Renders rather than prints, so the ONE end-to-end integrity line each append
/// load emits has an offline test with a hand-written expected string. It used
/// to be an `eprintln!`-only `fn`, and its whole-function `-> ()` stub was one of
/// the in-diff gate's misses: stubbed, every append load goes quiet about what it
/// appended and where, and nothing fails.
fn append_done_line(inputs: &LoadInputs, report: &load::CdcLoadReport) -> String {
    format!(
        "  integrity ✓ {} → appended {} to {} | current-state view {}{}",
        inputs.integrity.chain_prefix(),
        report.rows_appended,
        report.changes_table,
        report.view,
        cleaned_suffix(report.source_cleaned),
    )
}

/// The full-load sibling of [`append_done_line`] — the whole chain, now that the
/// warehouse leg is known. The loader already proved `warehouse == file` (its
/// count gate) before returning, so this is an all-green trace, not an assertion.
fn full_done_line(inputs: &LoadInputs, report: &load::LoadReport) -> String {
    format!(
        "  integrity ✓ {} → warehouse {} rows in {}{}",
        inputs.integrity.chain_prefix(),
        report.rows_loaded,
        report.target_table,
        cleaned_suffix(report.source_cleaned),
    )
}

/// Load a single export's CDC change log: reconcile the run manifests, **append**
/// the change Parquet into `<table>__changes`, and rebuild the current-state
/// dedup view over it. The manifests' summed `row_count` gates the rows *this*
/// load appends (before/after the append) — the file→warehouse leg for an
/// accumulating, at-least-once log.
fn load_one_cdc(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    engine: load::cdc::SourceEngine,
    pk: &[String],
    allow_source_drift: bool,
    state: Option<&StateStore>,
    load_id: &str,
) -> Result<Option<load::CdcLoadReport>> {
    let job = LoadJob {
        plan,
        run_id,
        state,
        load_id,
        allow_source_drift,
        mode: plan.mode,
    };
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  cdc load {} → {} | engine={:?} pk={} manifests={} parquet_files={} expected_delta={}",
                plan.table,
                plan.load.target.name(),
                engine,
                pk.join(","),
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs| {
            // The driver gates the appended delta against the manifests' summed
            // `row_count` and cleans up (only) after the gate passes.
            let cleanup = cleanup_target(plan, store, state);
            let report = load::run_load_cdc(
                loader,
                &plan.table,
                &plan.specs,
                &inputs.uris,
                pk,
                engine,
                Some(inputs.integrity.file_rows),
                cleanup,
            )?;
            Ok((report.rows_appended, report))
        },
        |inputs, report| eprintln!("{}", append_done_line(inputs, report)),
    )
}

/// Load a single export's INCREMENTAL delta: APPEND the delta Parquet into
/// `<table>__changes` and (re)build a current-state view deduped to the latest
/// row per PK by the export's `cursor_column`. Ledger-driven exactly like CDC —
/// only the not-yet-loaded runs are appended, so re-loads don't double and
/// `cleanup_source` is safe.
fn load_one_incremental(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    pk: &[String],
    allow_source_drift: bool,
    state: Option<&StateStore>,
    load_id: &str,
) -> Result<Option<load::CdcLoadReport>> {
    let cursor = plan.cursor_column.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "incremental load of `{}` needs the export's `cursor_column:` — the current-state \
             view's latest-per-PK ordering key",
            plan.table
        )
    })?;
    let job = LoadJob {
        plan,
        run_id,
        state,
        load_id,
        allow_source_drift,
        mode: plan.mode,
    };
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  incremental load {} → {} | pk={} cursor={} manifests={} parquet_files={} expected_delta={}",
                plan.table,
                plan.load.target.name(),
                pk.join(","),
                cursor,
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs| {
            let cleanup = cleanup_target(plan, store, state);
            let report = load::run_load_incremental(
                loader,
                &plan.table,
                &plan.specs,
                &inputs.uris,
                pk,
                &cursor,
                Some(inputs.integrity.file_rows),
                cleanup,
            )?;
            Ok((report.rows_appended, report))
        },
        |inputs, report| eprintln!("{}", append_done_line(inputs, report)),
    )
}

/// Load a single resolved table into its warehouse target, reconciling
/// **source → file → warehouse** row counts end-to-end.
///
/// The run manifests under the export prefix are the file-side source of truth:
/// they must describe a complete, self-consistent `Success` export, and their
/// summed `row_count` becomes the loader's `expected_rows` gate — so the load
/// `bail!`s unless the warehouse `COUNT(*)` matches. Loading unverified Parquet
/// "because it's in the bucket" is exactly what this prevents.
fn load_one(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    allow_source_drift: bool,
    state: Option<&StateStore>,
    load_id: &str,
) -> Result<Option<load::LoadReport>> {
    // Full loads OVERWRITE with the latest snapshot; the ledger (when `state`)
    // selects that single latest run, skips a re-load of it, and makes cleanup
    // safe. `state = None` ⇒ the stateless fallback (reconcile + load all).
    let job = LoadJob {
        plan,
        run_id,
        state,
        load_id,
        allow_source_drift,
        mode: plan.mode,
    };
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  load {} → {} | columns={} partition={:?} manifests={} parquet_files={} expected_rows={}",
                plan.table,
                plan.load.target.name(),
                plan.specs.len(),
                plan.partition_by,
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs| {
            let cleanup = cleanup_target(plan, store, state);
            let report = load::run_load(
                loader,
                &plan.table,
                &plan.specs,
                &inputs.uris,
                Some(inputs.integrity.file_rows),
                cleanup,
            )?;
            Ok((report.rows_loaded, report))
        },
        |inputs, report| eprintln!("{}", full_done_line(inputs, report)),
    )
}

/// The correlation run-id for a load: the explicit `--run-id` / `RIVET_RUN_ID`
/// if it carries a non-blank value, else a generated one. A blank string (clap
/// yields `Some("")` for `--run-id ""` / `RIVET_RUN_ID=""`) is treated as absent
/// — otherwise it became an empty warehouse tag + empty-derived ledger load_id
/// (dogfood LOW).
fn resolve_run_id(explicit: Option<String>) -> String {
    explicit
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(generate_run_id)
}

/// A per-invocation load-run id: microsecond-since-epoch hex + zero-padded pid
/// hex. Pure lowercase hex, so it survives both BigQuery's `[a-z0-9_-]` label
/// charset and Snowflake's alphanumeric `QUERY_TAG` sanitizer unchanged — the
/// same id reads back identically from either warehouse's cost views.
fn generate_run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    format!("{micros:x}{:08x}", std::process::id())
}

#[cfg(test)]
mod load_ledger_tests {
    use super::*;

    #[test]
    fn resolve_run_id_treats_blank_as_absent() {
        // #dogfood LOW: `--run-id ""` / RIVET_RUN_ID="" (clap → Some("")) must not
        // become the correlation label verbatim — blank is treated as absent.
        assert_eq!(resolve_run_id(Some("abc".into())), "abc");
        for blank in [Some(String::new()), Some("   ".into())] {
            let id = resolve_run_id(blank.clone());
            assert!(
                !id.trim().is_empty(),
                "blank {blank:?} must yield a generated id, got {id:?}"
            );
        }
        assert!(!resolve_run_id(None).is_empty());
    }

    #[test]
    fn require_pk_error_names_the_export_not_the_table() {
        // #dogfood LOW: the require_pk message labelled the TABLE as the export
        // (`export content_items` for an export named `c1`).
        use load::plan::{LoadMode, LoadPlan, LoadSection, LoadTarget};
        let plan = LoadPlan {
            export_name: "c1".into(),
            table: "content_items".into(),
            partition_by: None,
            specs: vec![],
            gcs_prefix: String::new(),
            destination: crate::config::DestinationConfig::default(),
            load: LoadSection {
                target: LoadTarget::Bigquery {
                    project: "p".into(),
                    dataset: "d".into(),
                },
                cleanup_source: false,
                pk: vec![], // empty → require_pk bails
                allow_source_drift: false,
                gc_orphans: false,
                cluster_by: vec![],
            },
            mode: LoadMode::Cdc,
            cursor_column: None,
        };
        let err = require_pk(&plan, "cdc").unwrap_err().to_string();
        assert!(err.contains("export `c1`"), "must name the export: {err}");
        assert!(
            !err.contains("content_items"),
            "must NOT label the table as the export: {err}"
        );
    }

    const TARGET: &str = "proj.ds.orders";

    fn ctx<'a>(state: &'a StateStore, load_id: &'a str) -> LoadCtx<'a> {
        LoadCtx {
            source_ident: String::new(),
            source_prefix: "gs://b/p/",
            state: Some(state),
            load_id,
            export_name: "orders",
            target_fqtn: TARGET,
            warehouse: "bigquery",
            mode: load::plan::LoadMode::Cdc,
        }
    }

    // The three `record_*` methods ARE the ledger invariant `execute_load`
    // enforces per exit path — pinned here offline instead of only live.

    #[test]
    fn record_success_logs_the_load_and_marks_its_runs_loaded() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_success(&["r1".into(), "r2".into()], 5);
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 5);
        let loaded = s.loaded_source_run_ids(TARGET).unwrap();
        assert!(
            loaded.contains("r1") && loaded.contains("r2"),
            "a successful load marks its runs so the next load skips them"
        );
    }

    #[test]
    fn record_success_marks_its_run_even_at_zero_rows() {
        // A NEW run that legitimately produced 0 rows (an empty CDC drain) still
        // SUCCEEDED — its run must be marked loaded, or every later load re-picks
        // it forever. Guards the 0-row *success* (marks its run) vs *skip* (no
        // new runs, marks nothing) distinction: marking is gated on status, not
        // on rows > 0.
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_success(&["r_empty".into()], 0);
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 0);
        assert!(
            s.loaded_source_run_ids(TARGET).unwrap().contains("r_empty"),
            "a 0-row successful load still marks its run — not re-processed forever"
        );
    }

    #[test]
    fn record_skip_logs_a_zero_row_success_and_marks_nothing() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_skip();
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "success");
        assert_eq!(loads[0].rows_loaded, 0);
        assert!(
            s.loaded_source_run_ids(TARGET).unwrap().is_empty(),
            "an up-to-date no-op consumes no runs"
        );
    }

    #[test]
    fn record_failed_logs_a_failed_audit_row() {
        let s = StateStore::open_in_memory().unwrap();
        ctx(&s, "L1").record_failed(&["r1".into()]);
        let loads = s.recent_loads(Some(TARGET), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "failed");
        assert_eq!(loads[0].rows_loaded, 0);
    }

    /// Per-table fault isolation. `rivet load` used `?` inside its per-plan loop,
    /// so the FIRST table's permanent error abandoned every later table in the
    /// config — and silently: a table that never ran gets no ledger row, so
    /// `rivet state loads` cannot tell "failed" from "never attempted". Combined
    /// with a prefix bricked by an aborted run, that starved every other table,
    /// every cycle, indefinitely.
    ///
    /// Asserts the AGGREGATION contract the loop now shares with `rivet run`:
    /// several failures collapse to one representative error that still names
    /// how many failed and lists the others, so the marker survives the
    /// downcast in `classify_exit` and the operator learns about ALL of them.
    #[test]
    fn several_load_failures_aggregate_instead_of_stopping_at_the_first() {
        // The oracle is the PRODUCT's fold, not a copy of it. The previous
        // version called `representative_failure_idx` and then re-typed
        // `remove(idx)`, the `others` join and the format string into its own
        // body — so it asserted on a value the TEST had produced, and putting `?`
        // back on the first failure (the regression it exists to catch) left it
        // green. Every expectation below is a hand-written literal or a property
        // of the INPUT, never a re-derivation of the code under test.
        let failures: Vec<anyhow::Error> = vec![
            anyhow::anyhow!("boom alpha").context("load 'alpha'"),
            anyhow::anyhow!("boom beta").context("load 'beta'"),
            anyhow::anyhow!("boom gamma").context("load 'gamma'"),
        ];
        let text = format!(
            "{:#}",
            aggregate_load_failures(failures).expect("three failures must aggregate to an error")
        );
        assert!(
            text.contains("3 load(s) failed"),
            "the aggregate must say how many failed; got: {text}"
        );
        for t in ["alpha", "beta", "gamma"] {
            assert!(
                text.contains(t),
                "every failed table must be named — a table missing from the aggregate is one \
                 an operator never learns about; got: {text}"
            );
        }

        // One failure is NOT dressed up as an aggregate: no count, no "also".
        let one = format!(
            "{:#}",
            aggregate_load_failures(vec![anyhow::anyhow!("boom solo").context("load 'solo'")])
                .expect("one failure is still an error")
        );
        assert!(one.contains("solo"), "got: {one}");
        assert!(
            !one.contains("load(s) failed"),
            "a single failure must surface as itself, not as a 1-of-1 aggregate; got: {one}"
        );

        // And nothing failed is not an error at all.
        assert!(
            aggregate_load_failures(Vec::new()).is_none(),
            "an empty failure set must not manufacture an error"
        );
    }

    #[test]
    fn record_is_a_noop_without_a_state_store() {
        // Stateless load (state=None): recording must not panic and writes nothing.
        let c = LoadCtx {
            source_ident: String::new(),
            state: None,
            load_id: "L1",
            export_name: "orders",
            target_fqtn: TARGET,
            warehouse: "bigquery",
            mode: load::plan::LoadMode::Full,
            source_prefix: "gs://b/p/",
        };
        c.record_success(&["r1".into()], 3);
        c.record_skip();
        c.record_failed(&["r2".into()]);
    }
}

/// The DECISIONS the live-only load orchestrator makes, graded offline.
///
/// `run_loads`, `prepare_load`, `execute_load` and the three `load_one*` drivers
/// need a real bucket, a real state DB and a real warehouse, so `cargo mutants
/// --in-diff` reports every mutant inside them MISSED whatever the assertions
/// say — the documented "`--lib` on a live-only path" class, and the reason
/// `.cargo/mutants.toml` excludes those BODIES wholesale. The exclusion is honest
/// about glue and dishonest about logic, which is what
/// `tests/offline/live_only_purity_gate.rs` exists to stop: every `&&`, `||`, `!`
/// and comparison those bodies used to make inline is now a NAMED PREDICATE
/// here, with a truth table over it.
///
/// Every test below was RED-proven against the exact mutant the in-diff gate
/// reported alive at the site the predicate came from — the mutant is named in
/// the test's own doc.
#[cfg(test)]
mod live_only_decisions {
    use super::*;
    use crate::destination::gcs::GcsStore;
    use load::plan::{LoadMode, LoadPlan, LoadSection, LoadTarget};

    /// A resolved plan, so a test can vary the ONE field it is about.
    fn plan_at(mode: LoadMode, gcs_prefix: &str) -> LoadPlan {
        LoadPlan {
            export_name: "orders".into(),
            table: "orders".into(),
            partition_by: None,
            specs: vec![],
            gcs_prefix: gcs_prefix.into(),
            destination: crate::config::DestinationConfig::default(),
            load: LoadSection {
                target: LoadTarget::Bigquery {
                    project: "p".into(),
                    dataset: "d".into(),
                },
                cleanup_source: false,
                pk: vec!["id".into()],
                allow_source_drift: false,
                gc_orphans: false,
                cluster_by: vec![],
            },
            mode,
            cursor_column: None,
        }
    }

    /// An fs-backed store over `dir`, standing in for the bucket. `gs://b/base`
    /// then addresses `<dir>/base` — the same (bucket, bucket-relative key) split
    /// every load op goes through.
    fn fs_store(dir: &tempfile::TempDir) -> GcsStore {
        GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap()
    }

    fn write_at(dir: &tempfile::TempDir, rel: &str, bytes: &[u8]) {
        let p = dir.path().join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    /// A state store with `run` recorded `running` on `prefix` — the ledger's
    /// "a run is writing here right now".
    fn state_with_active_run(prefix: &str) -> StateStore {
        let s = StateStore::open_in_memory().unwrap();
        s.begin_run("run-live", "orders", prefix, "2026-08-21T00:00:00Z")
            .unwrap();
        s
    }

    /// Only a `mode: cdc` table needs the `__pos` parse engine resolved. Kills
    /// `replace == with != in run_loads`, which resolves an engine for every
    /// batch config and leaves every CDC config's `engine.expect(..)` to panic.
    #[test]
    fn only_a_cdc_plan_needs_the_source_engine() {
        assert!(!needs_source_engine(&[]), "no tables, no engine");
        for batch in [LoadMode::Full, LoadMode::Incremental] {
            assert!(
                !needs_source_engine(&[plan_at(batch, "gs://b/base")]),
                "{batch:?} is a batch mode and must not open the source"
            );
        }
        assert!(needs_source_engine(&[plan_at(
            LoadMode::Cdc,
            "gs://b/base"
        )]));
        // A MIXED config needs it once: the engine is config-level, and the
        // fixture has to cross that threshold or `any` is indistinguishable from
        // "the first plan decides".
        assert!(needs_source_engine(&[
            plan_at(LoadMode::Full, "gs://b/base"),
            plan_at(LoadMode::Cdc, "gs://b/base"),
        ]));
    }

    /// The ledger's three answers, each decisive. A query ERROR must read as
    /// ACTIVE (spare) and a MISSING store as not-active (let the manifest signal
    /// decide) — collapsing either into the other is a delete that either never
    /// happens or happens under a live writer.
    #[test]
    fn ledger_says_active_is_conservative_on_error_and_silent_when_absent() {
        assert!(
            !ledger_says_active(None),
            "no state store to ask: the manifest signal decides alone"
        );
        assert!(!ledger_says_active(Some(Ok(false))));
        assert!(ledger_says_active(Some(Ok(true))));
        assert!(
            ledger_says_active(Some(Err(anyhow::anyhow!("state db unreachable")))),
            "a ledger the load cannot read must not license a delete"
        );
    }

    /// The two-signal fold as a truth table. Kills `replace || with && in
    /// maybe_gc_orphans`: with `&&`, a co-located load whose ledger says ACTIVE
    /// but whose bucket carries no running marker (a batch run that never
    /// projected one) deletes the live run's parts.
    #[test]
    fn either_activity_signal_alone_spares_the_prefix() {
        assert!(!prefix_is_active(false, false), "nothing is writing here");
        assert!(
            prefix_is_active(true, false),
            "the ledger alone is enough — a foreign bucket may carry no marker"
        );
        assert!(
            prefix_is_active(false, true),
            "the marker alone is enough — a stateless load has no ledger to read"
        );
        assert!(prefix_is_active(true, true));
    }

    /// The whole guard over a real (filesystem-backed) store: both of its
    /// whole-function stubs (`-> true` / `-> false`) die here, which is why it
    /// carries no mutation-config exclusion. `-> false` licenses the recursive
    /// `cleanup_source` delete under a live writer; `-> true` disables cleanup
    /// and orphan GC forever.
    #[test]
    fn prefix_has_active_run_reads_the_ledger_and_fails_safe() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        write_at(&dir, "base/part-000000.parquet", b"rows");
        let prefix = "gs://b/base";

        // Nothing running, nothing marked: the prefix is idle. (Kills `-> true`.)
        assert!(!prefix_has_active_run(&store, prefix, None));
        let idle = StateStore::open_in_memory().unwrap();
        assert!(!prefix_has_active_run(&store, prefix, Some(&idle)));

        // A `running` row on the prefix: active. (Kills `-> false`.)
        let live = state_with_active_run(prefix);
        assert!(prefix_has_active_run(&store, prefix, Some(&live)));

        // A prefix the store cannot even parse — the manifests are unreadable, so
        // a live run cannot be ruled OUT. Fail safe, never fail open.
        assert!(
            prefix_has_active_run(&store, "not-a-gs-uri", Some(&idle)),
            "an unreadable prefix must count as active: a delete needs proof, not silence"
        );
    }

    /// The cleanup truth table, including the LAZINESS the `if` chain had: the
    /// activity probe is a state-DB query plus a bucket listing and must not run
    /// for a config that never asked for cleanup. Kills `delete ! in
    /// cleanup_target`, which deletes the prefix of every config that did NOT ask
    /// and spares every config that did.
    #[test]
    fn cleanup_verdict_refuses_under_a_live_run_and_never_probes_unrequested() {
        assert_eq!(
            cleanup_verdict(false, || panic!(
                "must not probe when cleanup was not requested"
            )),
            CleanupVerdict::NotRequested
        );
        assert_eq!(
            cleanup_verdict(true, || true),
            CleanupVerdict::RefusedRunActive
        );
        assert_eq!(cleanup_verdict(true, || false), CleanupVerdict::Delete);
    }

    /// The wiring: the delete target only materialises when cleanup was asked for
    /// AND the prefix is idle. Kills `replace cleanup_target -> … with None`
    /// (cleanup silently stops happening) — the other two body stubs are unviable
    /// (`GcsStore` has no `Default`).
    #[test]
    fn cleanup_target_is_the_prefix_only_when_asked_and_idle() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        let idle = StateStore::open_in_memory().unwrap();

        let mut plan = plan_at(LoadMode::Full, prefix);
        assert!(
            cleanup_target(&plan, &store, Some(&idle)).is_none(),
            "cleanup_source is off — nothing may be deleted"
        );

        plan.load.cleanup_source = true;
        assert_eq!(
            cleanup_target(&plan, &store, Some(&idle)).map(|(_, p)| p),
            Some(prefix),
            "asked for, and idle: the staged prefix is the delete target"
        );

        let live = state_with_active_run(prefix);
        assert!(
            cleanup_target(&plan, &store, Some(&live)).is_none(),
            "a run is writing here — the recursive delete must be refused"
        );
    }

    /// Orphan GC over a real store, both directions. Kills `replace
    /// maybe_gc_orphans with ()` (the GC silently stops collecting) and pins the
    /// `active` gate end to end: the SAME unmanifested part is debris when the
    /// prefix is idle and in-flight data when a run is writing.
    #[test]
    fn gc_collects_an_unmanifested_part_only_while_no_run_is_writing() {
        let prefix = "gs://b/base";
        let orphan = "base/part-000000.parquet";

        // A run is writing here: the part may be its committed-but-not-yet-
        // manifested output. Spare it.
        let dir = tempfile::tempdir().unwrap();
        write_at(&dir, orphan, b"rows");
        let live = state_with_active_run(prefix);
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&live),
        );
        assert!(
            dir.path().join(orphan).exists(),
            "an unmanifested part under a LIVE run must survive gc — deleting it loses data \
             the source side has already advanced past"
        );

        // Nothing running: the same part is crash debris.
        let idle = StateStore::open_in_memory().unwrap();
        maybe_gc_orphans(
            &fs_store(&dir),
            &plan_at(LoadMode::Full, prefix),
            Some(&idle),
        );
        assert!(
            !dir.path().join(orphan).exists(),
            "with no run active, an unmanifested part is crash debris and must be collected"
        );
    }

    /// The warehouse table belongs to ONE source. Kills both mutants the in-diff
    /// gate found in `prepare_load`: `delete !` (refuse every pre-ledger artifact)
    /// and `replace != with ==` (refuse the SAME source, admit a different one —
    /// the cross-source overwrite the guard exists to stop).
    #[test]
    fn conflicting_source_ident_names_a_different_source_and_only_that() {
        let mine = "postgres:public.orders";
        assert!(
            conflicting_source_ident(mine, &[]).is_none(),
            "a table nothing was loaded into yet accepts this source"
        );
        assert!(
            conflicting_source_ident(mine, &[mine.to_string()]).is_none(),
            "the SAME source must keep loading into its own table"
        );
        assert_eq!(
            conflicting_source_ident(mine, &[mine.to_string(), "mysql:app.orders".to_string()])
                .map(String::as_str),
            Some("mysql:app.orders"),
            "a second source must be named, not silently overwritten"
        );
        assert!(
            conflicting_source_ident("", &["mysql:app.orders".to_string()]).is_none(),
            "an artifact written before the ledger recorded an identity reads as UNKNOWN and \
             must never block — an upgrade may not start refusing yesterday's loads"
        );
    }

    /// A run still WRITING into the prefix stays retryable: its id is not
    /// recorded as consumed, because its manifest can still grow. Kills `delete !
    /// in LoadCtx::record`, whose inverse records only the in-flight runs and
    /// re-loads every finished one forever.
    #[test]
    fn only_finished_runs_are_recorded_as_consumed() {
        let read = ["r1".to_string(), "r2".to_string()];
        let none: std::collections::HashSet<String> = Default::default();
        assert_eq!(consumable_run_ids(&read, &none), vec!["r1", "r2"]);
        let active: std::collections::HashSet<String> = ["r2".to_string()].into_iter().collect();
        assert_eq!(
            consumable_run_ids(&read, &active),
            vec!["r1"],
            "r2 is still writing — recording it consumed strands every part it writes later"
        );
        let all: std::collections::HashSet<String> = read.iter().cloned().collect();
        assert!(consumable_run_ids(&read, &all).is_empty());
    }

    /// The note about in-flight runs exists only when there ARE some. Kills
    /// `delete !` on the `is_empty` guard, whose inverse prints a note about zero
    /// runs on every load and stays silent on the one that matters.
    #[test]
    fn active_run_note_speaks_only_when_runs_are_still_writing() {
        assert_eq!(active_run_note(0, "gs://b/base"), None);
        let note = active_run_note(2, "gs://b/base").expect("two active runs must be announced");
        assert!(note.contains('2') && note.contains("gs://b/base"), "{note}");
        assert!(note.contains("still writing"), "{note}");
    }

    /// The up-to-date line names the right load. Kills `replace == with != in
    /// execute_load`, which swaps the two labels and tells an operator watching a
    /// CDC drain that a plain `load` is up to date.
    #[test]
    fn up_to_date_label_names_cdc_and_only_cdc() {
        assert_eq!(up_to_date_label(LoadMode::Cdc), "cdc load");
        for batch in [LoadMode::Full, LoadMode::Incremental] {
            assert_eq!(up_to_date_label(batch), "load", "{batch:?}");
        }
    }

    /// The success traces, against HAND-WRITTEN expected strings — an independent
    /// oracle, not a re-derivation of the format the code uses. Kills the
    /// whole-function stubs of both renderers (a load that goes quiet about what
    /// it appended, and where) and the `source_cleaned` suffix fork.
    #[test]
    fn done_lines_render_the_whole_integrity_chain() {
        let inputs = LoadInputs {
            integrity: load::reconcile::LoadIntegrity {
                source_rows: Some(100),
                file_rows: 100,
                manifests: 2,
            },
            uris: vec!["gs://b/base/part-000000.parquet".into()],
            source_run_ids: vec!["r1".into()],
            source_ident: "postgres:public.orders".into(),
        };

        let appended = load::CdcLoadReport {
            rows_appended: 40,
            changes_table: "p.d.orders__changes".into(),
            view: "p.d.orders".into(),
            source_cleaned: false,
        };
        assert_eq!(
            append_done_line(&inputs, &appended),
            "  integrity ✓ source 100 → files 100 → appended 40 to p.d.orders__changes | \
             current-state view p.d.orders"
        );

        let cleaned = load::CdcLoadReport {
            source_cleaned: true,
            ..appended
        };
        assert_eq!(
            append_done_line(&inputs, &cleaned),
            "  integrity ✓ source 100 → files 100 → appended 40 to p.d.orders__changes | \
             current-state view p.d.orders (source cleaned)",
            "a load that deleted the staged Parquet must SAY so — the prefix is empty now"
        );

        let full = load::LoadReport {
            rows_loaded: 100,
            target_table: "p.d.orders".into(),
            source_cleaned: false,
        };
        assert_eq!(
            full_done_line(&inputs, &full),
            "  integrity ✓ source 100 → files 100 → warehouse 100 rows in p.d.orders"
        );
        assert_eq!(
            full_done_line(
                &inputs,
                &load::LoadReport {
                    source_cleaned: true,
                    ..full
                }
            ),
            "  integrity ✓ source 100 → files 100 → warehouse 100 rows in p.d.orders \
             (source cleaned)"
        );
    }

    /// The generated correlation id is pure lowercase hex ending in this
    /// process's pid — the charset both BigQuery labels (`[a-z0-9_-]`) and
    /// Snowflake's `QUERY_TAG` sanitizer pass through unchanged, so the same id
    /// reads back identically from either warehouse's cost views. Kills `replace
    /// generate_run_id -> String with "xyzzy".into()`: the only existing
    /// assertion was "not blank", which a constant satisfies while making every
    /// load run in history share one id.
    #[test]
    fn generated_run_id_is_lowercase_hex_ending_in_the_pid() {
        let id = generate_run_id();
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "must survive both warehouses' tag sanitizers unchanged: {id}"
        );
        assert!(
            id.ends_with(&format!("{:08x}", std::process::id())),
            "the id carries THIS process's pid, so two concurrent loads cannot collide: {id}"
        );
        assert!(
            id.len() > 8,
            "a pid alone is not a per-invocation id — the microsecond stamp is missing: {id}"
        );
    }
}
