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
use anyhow::Context as _;

pub struct LoadArgs {
    pub config: String,
    pub run_id: Option<String>,
    /// Rebuild a change log whose partition differs from the config (ADR-0034 D5).
    pub rebuild_changelog: bool,
}

/// `rivet compact` arguments.
pub struct CompactArgs {
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
    let (state, ledger_errored) = match StateStore::open(&args.config) {
        Ok(s) => (Some(s), false),
        Err(e) => {
            eprintln!(
                "  warning: state store unavailable ({e:#}); loading without a ledger \
                 (no incremental skip / audit log)"
            );
            // The ERRORED half of the tri-state (round-9): `state=None` alone
            // conflated a DB blip with absent-by-design, and the re-baseline
            // guard then note-and-proceeded a doomed post-gap baseline on the
            // very host whose ledger just blipped.
            (None, true)
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
    let cfg = crate::config::Config::load(&args.config).context("parsing rivet config")?;
    for plan in &plans {
        let load_id = format!("{run_id}:{}", plan.table);
        let drift = plan.load.allow_source_drift;
        let outcome = (|| -> Result<()> {
            // Typed from the spec of the run this load consumes, not the by-name
            // row. Inside the per-table closure: a spec the config does not fit is
            // THIS table's failure, and the others still load.
            let pinned = pin_plan_to_its_run(plan, state.as_ref(), &cfg, "load")?;
            let plan = &pinned;
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
                        args.rebuild_changelog,
                        state.as_ref(),
                        ledger_errored,
                        &load_id,
                    )? {
                        Some(report) => {
                            println!("CDC LOAD OK [{}]: {}", plan.table, cdc_ok_line(&report))
                        }
                        None => println!("CDC LOAD SKIP [{}]: up to date", plan.table),
                    }
                }
                // Incremental: APPEND the delta + a cursor-ordered current-state view.
                load::plan::LoadMode::Incremental => {
                    let pk = require_pk(plan, "incremental")?;
                    match load_one_incremental(
                        plan,
                        &run_id,
                        pk,
                        drift,
                        args.rebuild_changelog,
                        state.as_ref(),
                        &load_id,
                    )? {
                        Some(report) => {
                            println!("INCREMENTAL LOAD OK [{}]: {}", plan.table, report.summary())
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
                        Some(report) => println!(
                            "LOAD OK [{}]: {} row(s) in `{}`{}",
                            plan.table,
                            report.rows_loaded,
                            report.target_table,
                            cleaned_suffix(report.source_cleaned)
                        ),
                        None => println!("LOAD SKIP [{}]: up to date", plan.table),
                    }
                }
            }
            Ok(())
        })();
        if let Err(e) = outcome {
            // Name the table on the way out: the aggregate must say WHICH load
            // failed, or an operator reading a mixed batch cannot act on it.
            // Through redact (round-8): a raw eprintln bypasses both the log
            // sink and main's top-level redactor — a future URL-bearing load
            // error would print credentials unredacted.
            eprintln!(
                "  LOAD FAILED [{}]: {}",
                plan.table,
                crate::redact::redact_secrets(&format!("{e:#}"))
            );
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

/// `plan` retyped from the spec of the NEWEST loadable run under its own prefix.
///
/// `plan_loads` types every table from `export_load_spec`, one row per export
/// NAME — last writer wins. On a state DB shared by two configs whose exports
/// share a name, the other config's run retypes this table between the run and
/// its load (the release gate's engine matrix does exactly this with `users`:
/// a `_id` key on a PostgreSQL table, another engine's DDL). The per-run table
/// (`export_load_spec_run`) is written only by the run that produced the parts,
/// so pinning the plan to that run's spec removes the race by construction.
///
/// Glue: one extra manifest listing per table, then `retype_plan`. Every path
/// that cannot pin — no state, no store, no manifest, a run older than the
/// per-run table — keeps the plan as typed and says so; it never fails the load.
fn pin_plan_to_its_run(
    plan: &load::plan::LoadPlan,
    state: Option<&StateStore>,
    cfg: &crate::config::Config,
    op: &str,
) -> Result<load::plan::LoadPlan> {
    // The by-name plan was built with its fit DEFERRED (`SpecFit::Deferred`), so
    // a path that keeps it owes the strict check the pin would have done.
    let unpinned = |why: &str| {
        eprintln!(
            "  {op} [{}]: typed from the by-name load spec — {why}",
            plan.table
        );
        load::plan::check_spec_fit(plan)?;
        Ok(plan.clone())
    };
    let Some(s) = state else {
        return unpinned("no state DB, so no per-run spec to pin to");
    };
    // Newest first. The listing under `<table>/` also holds the baseline LEGS'
    // manifests (`snapshot/`, their own run ids, no spec of their own — a leg is a
    // read recipe), so "the newest run" is "the newest run that RECORDED a spec".
    let runs = match load::open_store(&plan.destination)
        .and_then(|store| load::reconcile::fetch_manifests_keyed(&store, &plan.gcs_prefix))
        .and_then(|keyed| {
            load::reconcile::select_runs(keyed, &std::collections::HashSet::new(), plan.mode)
        }) {
        Ok(runs) => runs,
        Err(e) => return unpinned(&format!("could not list its runs ({e:#})")),
    };
    let mut newest_first: Vec<(String, String)> = runs
        .into_iter()
        .map(|(_, m)| (m.finished_at.clone(), m.run_id))
        .collect();
    if newest_first.is_empty() {
        return unpinned("no run left to load under its prefix");
    }
    // ONE definition of "newer" — the census's instant compare, not a byte compare
    // that mis-orders mixed RFC3339 precision; ties fall to the run id.
    newest_first.sort_by(|a, b| {
        use std::cmp::Ordering;
        if crate::manifest::census::finished_after(&a.0, &b.0) {
            Ordering::Less
        } else if crate::manifest::census::finished_after(&b.0, &a.0) {
            Ordering::Greater
        } else {
            b.1.cmp(&a.1)
        }
    });
    let mut pinned: Option<(String, String, crate::state::LoadSpec)> = None;
    // Newer Success runs that recorded no spec (a crash after the manifest and
    // before the spec write; a drain that acked parts then failed) are typed from
    // the older pinned run — said so, since a column added between them is
    // exactly what the pin exists to type.
    let mut skipped: Vec<&str> = Vec::new();
    for (finished_at, run_id) in &newest_first {
        // With the init-recorded key when the run recorded none (a `query:` export
        // has no key to read) — never with a key another run wrote by name.
        match s.load_spec_of_run_with_init_key(&plan.export_name, plan.unit.as_deref(), run_id) {
            Ok(Some(spec)) => {
                pinned = Some((run_id.clone(), finished_at.clone(), spec));
                break;
            }
            Ok(None) => {
                skipped.push(run_id);
                continue;
            }
            Err(e) => {
                return unpinned(&format!("run {run_id}'s own spec is unreadable ({e:#})"));
            }
        }
    }
    let Some((run_id, finished_at, spec)) = pinned else {
        return unpinned(&format!(
            "none of its {} loadable run(s) recorded a per-run spec (runs older than this \
             release, baseline legs only, a run that crashed after its manifest and before \
             its spec write, or a continuous stream — `until_current: false` — that was \
             stopped rather than finished, which records nothing)",
            newest_first.len()
        ));
    };
    if let Some(note) = skipped_runs_note(&plan.table, &run_id, &skipped) {
        eprintln!("{note}");
    }
    let Some(target) = crate::types::target::ExportTarget::parse(plan.load.target.name()) else {
        return unpinned("unknown load target");
    };
    // A readable spec the config does not fit is a REFUSAL, not a fallback: the
    // by-name spec is exactly what this pin exists to distrust.
    let mut retyped = load::plan::retype_plan(cfg, plan, &spec, target).with_context(|| {
        format!(
            "load [{}]: the config does not fit the columns run {run_id} recorded — if the \
             config changed after that run (a new `pk:` / `partition.column`), run `rivet run \
             -e {}` once so a run records the column, then load again",
            plan.table, plan.export_name
        )
    })?;
    retyped.pinned_run = Some((run_id, finished_at));
    Ok(retyped)
}

/// Runs in this listing that finished AFTER the run the plan was typed from: a
/// run landing between the pin's listing and the load's would be loaded with an
/// older spec's columns. Refused for this cycle; the next load pins it.
fn late_runs_refusal(
    table: &str,
    runs: &[(String, crate::manifest::RunManifest)],
    pin: Option<&(String, String)>,
) -> Option<String> {
    let (pinned_id, pinned_at) = pin?;
    let late: Vec<&str> = runs
        .iter()
        .filter(|(_, m)| crate::manifest::census::finished_after(&m.finished_at, pinned_at))
        .map(|(_, m)| m.run_id.as_str())
        .collect();
    (!late.is_empty()).then(|| {
        format!(
            "load [{table}]: run(s) {} finished after run {pinned_id}, which this load was \
             typed from — a run landed between typing and listing. Run `rivet load` again to \
             type from the newest run.",
            late.join(", ")
        )
    })
}

/// The refusal when another `rivet load` holds the table's lease.
fn lease_busy_message(target_fqtn: &str) -> String {
    format!(
        "load: another `rivet load` is writing `{target_fqtn}` right now (the lease is held; \
         it is released when that process ends, crash included). Wait for it, then retry."
    )
}

/// The stderr line naming the newer runs the pin passed over, or `None` when the
/// pinned run is the newest. Pure: the live-only pin decides through it.
fn skipped_runs_note(table: &str, pinned: &str, skipped: &[&str]) -> Option<String> {
    (!skipped.is_empty()).then(|| {
        format!(
            "  load [{table}]: typed from run {pinned}; {} newer run(s) recorded no spec and \
             are loaded with its columns: {}",
            skipped.len(),
            skipped.join(", ")
        )
    })
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

/// The resolved dedup key for an append mode (`cdc` / `incremental`); bails with a
/// config-fix hint when neither the config nor the recorded source key gives one.
fn require_pk<'a>(plan: &'a load::plan::LoadPlan, mode: &str) -> Result<&'a [String]> {
    if plan.pk.is_empty() {
        anyhow::bail!(
            "export `{}` is mode: {mode} but has no primary key for the current-state dedup \
             view — `rivet run` recorded none (a `query:` export, or a table without one), so \
             declare it in the export's `load:` block (e.g. `pk: [id]`)",
            plan.export_name
        );
    }
    Ok(&plan.pk)
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
                 load once the extract has finished — or, if this is a DEAD crash remnant \
                 (no extract is actually running), inspect it with `rivet state runs -c \
                 <config> --running` and close it with `rivet state finish-run -c <config> \
                 --run-id <id>`.",
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
    // Which `running` MARKERS the ledger already knows are dead (`state
    // finish-run`, the split ceased-ordinal stamp): their run_id has a
    // TERMINAL row, so the sweep may retire the marker even though no Success
    // ever superseded it. Stateless → empty → conservative sweep only.
    let dead_marker_run_ids: std::collections::HashSet<String> = match state {
        Some(s) => keyed
            .iter()
            .filter(|(_, m)| m.status == crate::manifest::ManifestStatus::Running)
            .filter(|(_, m)| {
                matches!(
                    s.run_status_of(&m.run_id),
                    Ok(Some(status)) if status != "running"
                )
            })
            .map(|(_, m)| m.run_id.clone())
            .collect(),
        None => Default::default(),
    };
    match load::reconcile::gc_orphans(
        store,
        &plan.gcs_prefix,
        &keyed,
        active,
        &dead_marker_run_ids,
    ) {
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
    /// Which runs were still WRITING at the moment the manifests were FETCHED —
    /// sampled BEFORE the fetch, in [`prepare_load`]. `record` unions this with
    /// its own record-time sample: a run that finishes DURING the warehouse copy
    /// is active in neither sample alone taken at record time, yet the manifest
    /// this load consumed was its mid-flight snapshot — recording it consumed
    /// would strand every part it flushed after the fetch, permanently
    /// (round-4 TOCTOU). `None` = the sample could not be taken (stateless, or
    /// the query failed): record then consumes NOTHING this cycle.
    active_at_fetch: Option<std::collections::HashSet<String>>,
    /// The selected run manifests, keyed by their bucket path.
    runs: Vec<(String, crate::manifest::RunManifest)>,
    /// Whether the target table, if it exists, is one rivet loaded (per the ledger).
    ownership: load::Ownership,
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
    // A BARE engine (`mysql`, no table recorded) is "this engine, table unknown"
    // — the same coarsening `ensure_single_source` applies to the manifests under
    // one prefix. Bare vs qualified of ONE engine is not two sources; two
    // different engines are, however coarse either side is.
    prior
        .iter()
        .find(|p| p.as_str() != mine && identity_engine(p) != identity_engine(mine))
        .or_else(|| {
            prior.iter().find(|p| {
                p.as_str() != mine
                    && p.contains(':')
                    && mine.contains(':')
                    && identity_engine(p) == identity_engine(mine)
            })
        })
}

/// The engine half of an `engine[:schema.table]` identity.
fn identity_engine(ident: &str) -> &str {
    ident.split(':').next().unwrap_or(ident)
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
    // Sampled BEFORE the manifests are read, deliberately: a run that finishes
    // between this sample and the fetch stays in the set and is merely
    // re-appended next cycle (at-least-once, absorbed by the current-state
    // view). The reverse order — sample after fetch — reopens the TOCTOU this
    // exists to close: finish-then-fetch would read the run as consumable
    // against a manifest snapshot older than its last parts.
    let active_at_fetch = state.and_then(|s| match s.active_run_ids_on_prefix(&plan.gcs_prefix) {
        Ok(a) => Some(a),
        Err(e) => {
            log::warn!(
                "load: cannot tell which runs are writing into {} at fetch time ({e:#}) — \
                 this cycle will not record any run as consumed, so nothing they write \
                 later is stranded",
                plan.gcs_prefix
            );
            None
        }
    });
    let keyed = load::reconcile::fetch_manifests_keyed(store, &plan.gcs_prefix)?;
    // Round-6: "up to date — every extraction run already loaded" was printed
    // for BOTH "all runs consumed" and "this prefix holds NOTHING" — and the
    // second is what a typo'd/mis-encoded prefix produces, forever, exit 0.
    // Say the empty-prefix truth before the optimistic line.
    if keyed.is_empty() {
        eprintln!(
            "  load [{}]: found NO manifests under {} — nothing was ever staged \
             here. If an export should have landed, check the prefix for typos \
             (a wrong prefix reads as permanently 'up to date').",
            plan.table, plan.gcs_prefix
        );
    }
    // Refuse a prefix shared by two exports BEFORE selecting/summing/cleaning:
    // the load sums every manifest here and cleanup wipes the prefix recursively,
    // so a shared base prefix would cross-contaminate the count and delete a
    // sibling export's parts (there is no source export_name on the plan to
    // disambiguate). Covers Full (wrong-export snapshot pick), incremental, and
    // CDC in one place, before any irreversible step.
    load::reconcile::ensure_single_export(&keyed)?;
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
    if let Some(why) = late_runs_refusal(&plan.table, &new, plan.pinned_run.as_ref()) {
        anyhow::bail!(why);
    }
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
    //
    // Read from the SAME population the recorder below writes — `new`, the
    // `Success` manifests `select_runs` kept — never the raw listing: a `Running`
    // marker carries no schema/table and renders as the bare engine, so keyed on
    // the raw listing one crashed run's marker refused every later load of the
    // table, forever, with a remediation that named the wrong cause.
    if let Some(s) = state
        && let Some((_, m)) = new.first()
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
    let manifests: Vec<_> = new.iter().map(|(_, m)| m.clone()).collect();
    // Best-effort column-drift check (only manifests with Form B record
    // column names — a checksum-less prefix yields no notes, silently-honest).
    let spec_names: Vec<String> = plan.specs.iter().map(|s| s.column_name.clone()).collect();
    for note in spec_manifest_column_drift(&spec_names, &manifests) {
        eprintln!("{note}");
    }
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
    let ownership = match state {
        Some(s) => match s.has_load_attempt(target_fqtn) {
            Ok(true) => load::Ownership::Own,
            Ok(false) => load::Ownership::Foreign,
            Err(_) => load::Ownership::Unknown,
        },
        None => load::Ownership::Unknown,
    };
    Ok(Some(LoadInputs {
        integrity,
        uris,
        source_run_ids,
        source_ident,
        active_at_fetch,
        runs: new,
        ownership,
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
    /// [`LoadInputs::active_at_fetch`], copied beside `source_ident` — `record`
    /// unions it with its own record-time sample so a run that finished DURING
    /// the copy is still excluded from the consumed set.
    active_at_fetch: Option<std::collections::HashSet<String>>,
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
        let mut active = match s.active_run_ids_on_prefix(self.source_prefix) {
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
        // UNION with the fetch-time sample: this method runs AFTER the warehouse
        // copy, so a run that finished (or was resumed and finished) during the
        // copy is absent from the record-time set — but the manifest this load
        // consumed was fetched while it was still writing, i.e. a mid-flight
        // snapshot. Consuming it would strand its post-fetch parts forever.
        // No fetch-time sample at all (the query failed) → consume nothing;
        // the next cycle re-evaluates, which the dedup view absorbs.
        let fetch_sample_missing = self.active_at_fetch.is_none();
        match &self.active_at_fetch {
            Some(at_fetch) => active.extend(at_fetch.iter().cloned()),
            None => active.extend(source_run_ids.iter().cloned()),
        }
        let source_run_ids = consumable_run_ids(source_run_ids, &active);
        // The "still writing" note is for runs OBSERVED active. When the
        // fetch-time sample is missing, `active` was padded with every run as
        // a consume-nothing fail-safe — printing "N runs still writing" about
        // runs that are merely unverifiable is false (round-5); the fetch-time
        // warn already told the operator nothing will be recorded this cycle.
        if !fetch_sample_missing
            && let Some(note) = active_run_note(active.len(), self.source_prefix)
        {
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
            eprintln!(
                "  warning: load ledger write failed (load itself proceeded): {}",
                crate::redact::redact_secrets(&format!("{e:#}"))
            );
        }
    }
    /// Nothing new to load — the ledger already covers every run.
    fn record_skip(&self) {
        self.record(&[], 0, "success");
    }
    /// The load errored after consuming `run_ids`.
    #[cfg(test)]
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
        &mut LegLedger<'_>,
    ) -> Result<(u64, R)>,
    done: impl FnOnce(&LoadInputs, &R),
) -> Result<Option<R>> {
    let store = load::open_store(&job.plan.destination)?;
    let loader = load::build_loader(job.plan, job.run_id);
    let target_fqtn = loader.fqtn(&job.plan.table);
    // One load per table at a time: two concurrent loads both read the ledger
    // before either writes it and append the same runs twice.
    let _lease = match job
        .state
        .map(|s| s.try_load_lease(&target_fqtn))
        .transpose()?
    {
        Some(None) => anyhow::bail!("{}", lease_busy_message(&target_fqtn)),
        held => held.flatten(),
    };
    let mut ctx = LoadCtx {
        state: job.state,
        load_id: job.load_id,
        export_name: job.plan.table.as_str(),
        target_fqtn: target_fqtn.as_str(),
        warehouse: job.plan.load.target.name(),
        mode: job.mode,
        source_prefix: job.plan.gcs_prefix.as_str(),
        source_ident: String::new(),
        active_at_fetch: None,
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
            ctx.active_at_fetch = i.active_at_fetch.clone();
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
    let mut legs = LegLedger {
        ctx: &ctx,
        consumed: Vec::new(),
    };
    // The budget measures what lands in the PARTITIONED target only: a
    // disposable buffer takes no partition, so its files are not its business.
    let budgeted = budgeted_uris(job.plan.layout, &inputs.runs, &inputs.uris);
    let (rows, report) = match load::before_write(partition_budget_ok(&store, job.plan, &budgeted))
        .and_then(|()| run(&*loader, &store, &inputs, &mut legs))
    {
        Ok(v) => v,
        Err(e) => {
            let remaining = remaining_run_ids(&inputs.source_run_ids, &legs.consumed);
            ctx.record(&remaining, 0, ledger_status(&e));
            return Err(e);
        }
    };
    let remaining = remaining_run_ids(&inputs.source_run_ids, &legs.consumed);
    if closing_record_applies(&remaining, &legs.consumed) {
        ctx.record_success(&remaining, rows as i64);
    }
    done(&inputs, &report);
    Ok(Some(report))
}

/// The base-and-buffer CDC load: the baseline legs OVERWRITE the physical base
/// (`<table>`, source columns + `__is_deleted`), the stream's runs APPEND into the
/// per-cycle buffer `<table>__changes` that `rivet compact` merges and drops. No
/// view, no adoption, no re-baseline refusal — a new baseline replaces the base.
fn load_one_cdc_base(
    job: LoadJob<'_>,
    pk: &[String],
    allow_source_drift: bool,
    state: Option<&StateStore>,
) -> Result<Option<load::CdcLoadReport>> {
    let plan = job.plan;
    execute_load(
        job,
        |inputs| {
            eprintln!(
                "  cdc load {} → {} | layout=base+buffer pk={} manifests={} parquet_files={} rows={}",
                plan.table,
                plan.load.target.name(),
                pk.join(","),
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs, legs| {
            let (baseline, stream): (Vec<_>, Vec<_>) = inputs
                .runs
                .iter()
                .cloned()
                .partition(|(_, m)| is_baseline_leg(m));
            // Rows this cycle landed, per leg then the buffer; summed for the ledger.
            let mut landed: Vec<u64> = Vec::new();
            let mut report: Option<load::CdcLoadReport> = None;
            if let [_, ..] = baseline.as_slice() {
                let uris = load::reconcile::select_load_uris(store, &plan.gcs_prefix, &baseline)?;
                let manifests: Vec<_> = baseline.iter().map(|(_, m)| m.clone()).collect();
                let integrity = load::reconcile::reconcile(&manifests, allow_source_drift)?;
                let mut specs = plan.specs.clone();
                specs.push(load::cdc::flag_spec(loader.warehouse()));
                let r = load::run_load(
                    loader,
                    &plan.table,
                    &specs,
                    &uris,
                    Some(integrity.file_rows),
                    None,
                    inputs.ownership,
                )?;
                eprintln!(
                    "  baseline → `{}`: {} rows from {} leg(s) (source columns + `__is_deleted`)",
                    r.target_table,
                    r.rows_loaded,
                    baseline.len()
                );
                let ids: Vec<String> = baseline.iter().map(|(_, m)| m.run_id.clone()).collect();
                legs.landed(&ids, r.rows_loaded);
                landed.push(r.rows_loaded);
            }
            let stream_uris = if stream.is_empty() {
                Vec::new()
            } else {
                load::reconcile::select_load_uris(store, &plan.gcs_prefix, &stream)?
            };
            // An idle drain writes a Success manifest with no parts: nothing to
            // buffer, and the run is still recorded consumed by the closing record.
            if let Some(uris) = buffer_uris(stream_uris) {
                let manifests: Vec<_> = stream.iter().map(|(_, m)| m.clone()).collect();
                let integrity = load::reconcile::reconcile(&manifests, allow_source_drift)?;
                let cleanup = cleanup_target(plan, store, state);
                let r = load::run_load_buffer(
                    loader,
                    &plan.table,
                    &plan.specs,
                    &uris,
                    pk,
                    Some(integrity.file_rows),
                    cleanup,
                )?;
                landed.push(r.rows_appended);
                report = Some(r);
            }
            let report = report.unwrap_or_else(|| load::CdcLoadReport {
                rows_appended: 0,
                changes_table: loader.fqtn(&format!("{}__changes", plan.table)),
                target: loader.fqtn(&plan.table),
                target_kind: load::ChangelogTarget::Base,
                source_cleaned: false,
            });
            Ok((landed.iter().sum(), report))
        },
        |inputs, report| eprintln!("{}", cdc_done_line(&inputs.integrity, report)),
    )
}

/// The one-line `CDC LOAD OK` verdict: what landed where and what the operator
/// does next — never a struct dump. The report says which target it produced.
fn cdc_ok_line(report: &load::CdcLoadReport) -> String {
    let cleaned = cleaned_suffix(report.source_cleaned);
    match report.target_kind {
        load::ChangelogTarget::Base if report.rows_appended == 0 => format!(
            "no changes buffered by this load into `{}`; `rivet compact` has nothing new for `{}`{cleaned}",
            report.changes_table, report.target
        ),
        load::ChangelogTarget::Base => format!(
            "{} change row(s) buffered into `{}` — `rivet compact` merges them into `{}`{cleaned}",
            report.rows_appended, report.changes_table, report.target
        ),
        load::ChangelogTarget::View => format!(
            "{} row(s) appended to `{}` | current-state view `{}`{cleaned}",
            report.rows_appended, report.changes_table, report.target
        ),
    }
}

/// The buffer's Parquet, or `None` when the stream's runs produced no parts (an
/// idle drain) — pure, so the live-only load body decides through it.
fn buffer_uris(uris: Vec<String>) -> Option<Vec<String>> {
    (!uris.is_empty()).then_some(uris)
}

/// A baseline leg is a BATCH run under the stream's prefix; the stream's own
/// drains are `mode: cdc`. Names are no tell — a stream named `users` over table
/// `t` writes `export_name = t`, exactly what a leg's family/name pair looks like.
fn is_baseline_leg(m: &crate::manifest::RunManifest) -> bool {
    m.mode != "cdc"
}

/// Why `rivet compact` passes a table by, or `None` for a base-and-buffer CDC table.
fn compact_skip_reason(
    mode: &load::plan::LoadMode,
    layout: &load::plan::CdcLayout,
) -> Option<&'static str> {
    match mode {
        // A full load OVERWRITES the whole table on every pass, so there is never
        // an accumulated buffer to merge — whatever a layout says.
        load::plan::LoadMode::Full => Some("a full load overwrites its table; nothing to merge"),
        // Otherwise compaction belongs to the LAYOUT, not the mode: any table kept
        // as a physical base with a disposable buffer has something to merge. An
        // incremental export asks for that with `load.layout: base_buffer`.
        _ if layout.compacts() => None,
        load::plan::LoadMode::Cdc => {
            Some("a changelog + view table (`initial: snapshot`); nothing to merge")
        }
        load::plan::LoadMode::Incremental => Some(
            "a changelog + view table; `load.layout: base_buffer` gives it a base to merge into",
        ),
    }
}

/// What decides the WINNER in this table's compaction: a CDC stream ranks by its
/// log position, an incremental export by the cursor its current state is ordered
/// on. One resolver so a mode that gains compaction cannot forget to say.
fn compact_order_of(
    plan: &load::plan::LoadPlan,
    engine: Option<load::cdc::SourceEngine>,
) -> Result<load::cdc::CompactOrder> {
    match plan.mode {
        load::plan::LoadMode::Cdc => Ok(load::cdc::CompactOrder::Cdc(
            engine.context("a cdc plan needs its source engine to rank the buffer")?,
        )),
        _ => plan
            .cursor_column
            .clone()
            .map(load::cdc::CompactOrder::Cursor)
            .with_context(|| {
                format!(
                    "compacting `{}` needs the export's `cursor_column:` — the buffer's \
                     latest-per-key ordering",
                    plan.table
                )
            }),
    }
}

/// The two metadata reads [`load::compact_gate`] decides on, and the note it may
/// print. Glue: it fetches the facts, the decision itself is the pure predicate.
fn compact_gate_of(
    loader: &dyn load::TargetLoader,
    table: &str,
    state: Option<&StateStore>,
) -> Result<()> {
    let buffer = format!("{table}__changes");
    if !matches!(loader.object_kind(&buffer)?, load::ObjectKind::Table) {
        // No buffer: `compact` says that no-op itself, and a missing base is not a
        // problem when there is nothing to merge into it.
        return Ok(());
    }
    let base_fqtn = loader.fqtn(table);
    let ownership = match state {
        Some(s) => match s.has_load_attempt(&base_fqtn) {
            Ok(true) => load::Ownership::Own,
            Ok(false) => load::Ownership::Foreign,
            Err(_) => load::Ownership::Unknown,
        },
        None => load::Ownership::Unknown,
    };
    match load::compact_gate(
        loader.object_kind(table)?,
        ownership,
        &base_fqtn,
        &loader.fqtn(&buffer),
    ) {
        load::CompactGate::Go => Ok(()),
        load::CompactGate::Note(note) => {
            eprintln!("{note}");
            Ok(())
        }
        load::CompactGate::Refuse(msg) => Err(load::refused(msg)),
    }
}

/// `rivet compact`: merge every base-and-buffer table's buffer into its base and
/// drop the buffer. One MERGE per table (per partition window), labelled
/// `rivet_op:merge`; a table without a buffer is a no-op, said so.
pub fn run_compacts(args: CompactArgs) -> Result<()> {
    let plans = load::plan::plan_loads(&args.config)?;
    let run_id = resolve_run_id(args.run_id.clone());
    let state = match StateStore::open(&args.config) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("  warning: state store unavailable ({e:#}); compacting without a ledger");
            None
        }
    };
    let cfg = crate::config::Config::load(&args.config).context("parsing rivet config")?;
    let engine = if needs_source_engine(&plans) {
        Some(load::plan::source_engine(&args.config)?)
    } else {
        None
    };
    let mut failures: Vec<anyhow::Error> = Vec::new();
    for plan in &plans {
        if let Some(why) = compact_skip_reason(&plan.mode, &plan.layout) {
            eprintln!("  compact [{}]: skipped — {why}", plan.table);
            continue;
        }
        let load_id = format!("{run_id}:{}", plan.table);
        let outcome = (|| -> Result<()> {
            let pinned = pin_plan_to_its_run(plan, state.as_ref(), &cfg, "compact")?;
            let loader = load::build_loader(&pinned, &run_id);
            let target_fqtn = loader.fqtn(&pinned.table);
            let _lease = match state
                .as_ref()
                .map(|s| s.try_load_lease(&target_fqtn))
                .transpose()?
            {
                Some(None) => anyhow::bail!("{}", lease_busy_message(&target_fqtn)),
                held => held.flatten(),
            };
            let pk = require_pk(&pinned, "cdc")?;
            // The base is checked BEFORE the MERGE, and only when a buffer exists:
            // an absent base surfaced as BigQuery's own `Not found: Table`, and a
            // base rivet never loaded was not checked at all. Metadata, no job.
            let report = match compact_gate_of(loader.as_ref(), &pinned.table, state.as_ref()) {
                Err(e) => Err(e),
                Ok(()) => compact_order_of(&pinned, engine)
                    .and_then(|order| loader.compact(&pinned.table, &pinned.specs, pk, order)),
            };
            if let Some(s) = state.as_ref() {
                let rec = LoadRecord {
                    load_id: load_id.clone(),
                    export_name: pinned.table.clone(),
                    target_table: target_fqtn.clone(),
                    warehouse: pinned.load.target.name().to_string(),
                    mode: "compact".to_string(),
                    source_run_ids: Vec::new(),
                    source_ident: String::new(),
                    rows_loaded: report.as_ref().map_or(0, |r| r.changes_rows as i64),
                    status: match &report {
                        Ok(_) => "success".to_string(),
                        // A refusal is a stop before the write, exactly as on the
                        // load path — never a `failed` row that makes the target
                        // look like rivet's own on the next attempt.
                        Err(e) => ledger_status(e).to_string(),
                    },
                    finished_at: chrono::Utc::now().to_rfc3339(),
                };
                if let Err(e) = s.store_load(&rec) {
                    eprintln!("  warning: compact ledger write failed for `{target_fqtn}`: {e:#}");
                }
            }
            let report = report?;
            if report.had_buffer {
                println!(
                    "COMPACT OK [{}]: {} change row(s) merged into `{}` in {} MERGE statement(s); buffer dropped",
                    plan.table, report.changes_rows, report.base, report.merge_jobs
                );
            } else {
                println!(
                    "COMPACT SKIP [{}]: no `{}__changes` buffer — nothing to merge",
                    plan.table, plan.table
                );
            }
            Ok(())
        })();
        if let Err(e) = outcome {
            eprintln!("  COMPACT FAILED [{}]: {e:#}", plan.table);
            failures.push(e.context(format!("compact '{}'", plan.table)));
        }
    }
    match failures.len() {
        0 => Ok(()),
        1 => Err(failures.pop().unwrap()),
        n => anyhow::bail!(
            "{n} of {} table(s) failed to compact: {}",
            plans.len(),
            failures
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join(" | ")
        ),
    }
}

/// How the ledger records a load that did not complete: `refused` when it stopped before
/// any warehouse write (the target is not rivet's own for having been refused), `failed`
/// otherwise.
fn ledger_status(e: &anyhow::Error) -> &'static str {
    match e.downcast_ref::<load::Refused>() {
        Some(_) => "refused",
        None => "failed",
    }
}

/// The ledger rows of one load, written per LEG: a run closure that lands some of
/// its runs before the rest (the incremental first pass, then the deltas) records the
/// landed part at once, so a failure further on leaves it consumed — never re-loaded,
/// never marked failed for a leg that succeeded.
struct LegLedger<'a> {
    ctx: &'a LoadCtx<'a>,
    consumed: Vec<String>,
}

impl LegLedger<'_> {
    /// Record `run_ids` as loaded with `rows`, ahead of the load's closing record.
    fn landed(&mut self, run_ids: &[String], rows: u64) {
        self.ctx.record_success(run_ids, rows as i64);
        self.consumed.extend(run_ids.iter().cloned());
    }
}

/// Whether the load's closing ledger row is written: there is something left to
/// record, or no leg recorded anything (the ordinary one-row load, an up-to-date one
/// included) — never a second empty row after the legs covered every run.
fn closing_record_applies(remaining: &[String], consumed: &[String]) -> bool {
    !remaining.is_empty() || consumed.is_empty()
}

/// The run ids a load's closing record still covers: every selected run a leg has not
/// already recorded.
fn remaining_run_ids(all: &[String], consumed: &[String]) -> Vec<String> {
    all.iter()
        .filter(|id| !consumed.contains(id))
        .cloned()
        .collect()
}

/// The URIs the partition budget applies to: the files that land in the
/// PARTITIONED target. Under `BaseAndBuffer` the stream's files land in the
/// BUFFER, which is created WITHOUT a partition and read whole by one MERGE, so
/// budgeting them against the base's granularity refuses a load that would have
/// worked. Found by dogfooding (2026-09-18): a 5,000-day buffer file on a
/// day-partitioned base was refused by name, although no job would ever write
/// those partitions — the adapter had already stopped packing the buffer, but
/// this preflight still measured it.
///
/// A baseline manifest that resolves to no present part makes `select_load_keys`
/// fall back to the whole listing; the check then covers everything again, which
/// is the conservative direction.
fn budgeted_uris(
    layout: load::plan::CdcLayout,
    runs: &[(String, crate::manifest::RunManifest)],
    uris: &[String],
) -> Vec<String> {
    if !layout.log_is_disposable() {
        return uris.to_vec();
    }
    let baseline: Vec<(String, crate::manifest::RunManifest)> = runs
        .iter()
        .filter(|(_, m)| is_baseline_leg(m))
        .cloned()
        .collect();
    if baseline.is_empty() {
        return Vec::new();
    }
    let keys: Vec<String> = uris
        .iter()
        .filter_map(|u| load::split_gs_uri(u).ok().map(|(_, k)| k.to_string()))
        .collect();
    let want: std::collections::HashSet<String> =
        load::reconcile::select_load_keys(&baseline, &keys)
            .into_iter()
            .collect();
    uris.iter()
        .filter(|u| load::split_gs_uri(u).is_ok_and(|(_, k)| want.contains(k)))
        .cloned()
        .collect()
}

/// The pre-load partition budget of a BigQuery plan (ADR-0034 D4); no other target
/// caps the partitions one job writes.
fn partition_budget_ok(
    store: &crate::destination::gcs::GcsStore,
    plan: &load::plan::LoadPlan,
    uris: &[String],
) -> Result<()> {
    match (&plan.load.target, &plan.partition) {
        (load::plan::LoadTarget::Bigquery { .. }, Some(partition)) => {
            load::partition_budget::check_partition_budget(store, uris, partition)
                .with_context(|| format!("export `{}`", plan.export_name))
        }
        _ => Ok(()),
    }
}

/// The partition a load declares, for the progress line.
fn partition_label(plan: &load::plan::LoadPlan) -> String {
    plan.partition
        .as_ref()
        .map_or_else(|| "none".to_string(), |p| p.key.describe())
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

/// The success trace shared by the CDC + incremental loads: the integrity chain,
/// the appended rows, the change-log table, and the target the report names — the
/// dedup view, or the base a buffer is compacted into.
///
/// Renders rather than prints, so the ONE end-to-end integrity line each append
/// load emits has an offline test with a hand-written expected string. It used
/// to be an `eprintln!`-only `fn`, and its whole-function `-> ()` stub was one of
/// the in-diff gate's misses: stubbed, every append load goes quiet about what it
/// appended and where, and nothing fails.
fn cdc_done_line(
    integrity: &load::reconcile::LoadIntegrity,
    report: &load::CdcLoadReport,
) -> String {
    let cleaned = cleaned_suffix(report.source_cleaned);
    match report.target_kind {
        load::ChangelogTarget::View => format!(
            "  integrity ✓ {} → appended {} to {} | current-state view {}{cleaned}",
            integrity.chain_prefix(),
            report.rows_appended,
            report.changes_table,
            report.target,
        ),
        load::ChangelogTarget::Base => format!(
            "  integrity ✓ {} → base {} | buffered {} row(s) into {}{cleaned}",
            integrity.chain_prefix(),
            report.target,
            report.rows_appended,
            report.changes_table,
        ),
    }
}

/// The full-load sibling of [`cdc_done_line`] — the whole chain, now that the
/// warehouse leg is known. The loader already proved `warehouse == file` (its
/// count gate) before returning, so this is an all-green trace, not an assertion.
fn full_done_line(integrity: &load::reconcile::LoadIntegrity, report: &load::LoadReport) -> String {
    format!(
        "  integrity ✓ {} → warehouse {} rows in {}{}",
        integrity.chain_prefix(),
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
/// The RE-baseline warning, or `None`. Round-6 (proven on the verbatim view
/// SQL): a re-snapshot row carries NULL `__pos` and LOSES the dedup to every
/// already-loaded change row — so appending a snapshot leg into a `__changes`
/// that prior cycles already fed serves pre-gap values silently for exactly
/// the PKs the re-snapshot fixed. The shape is detectable co-located: snapshot
/// URIs in THIS load + a non-empty loaded-ledger for the target. First-cycle
/// snapshot+changes (no prior loads) is the normal shape — no warning.
/// Is this load's URI set a RE-baseline shape — a snapshot leg under THIS
/// plan's own prefix? Prefix-anchored (round-7 refuter): a bare
/// `contains("/snapshot/")` false-fired forever on an operator prefix with a
/// `snapshot` segment and on a multiplex TABLE literally named `snapshot`,
/// prescribing a destructive truncate every cycle.
/// Column drift between the SPEC the plan is typed from (the columns the run
/// `rivet load` pins to recorded in the state DB — never the live source) and
/// the STAGED parquet (its manifests record the columns when Form B is on).
/// Round-10, closing the round-6 find: a column an OLDER staged run carries but
/// the newest run's spec lacks is silently never loaded — Snowflake's COPY
/// projects only spec columns, so the staged data vanishes with every count
/// gate green (rows agree; columns were never compared). Detection is
/// best-effort by construction: manifests without checksums record no column
/// names, and this then returns empty — said in the caller's comment, not
/// silently.
fn spec_manifest_column_drift(
    spec_columns: &[String],
    manifests: &[crate::manifest::RunManifest],
) -> Vec<String> {
    use std::collections::BTreeSet;
    let specs: BTreeSet<&str> = spec_columns.iter().map(|s| s.as_str()).collect();
    let mut notes = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for m in manifests {
        let Some(cols) = m.column_checksums.as_deref() else {
            continue;
        };
        for c in cols {
            if !specs.contains(c.name.as_str()) && seen.insert(c.name.clone()) {
                notes.push(format!(
                    "  WARNING: staged parquet carries column `{}` (recorded by run {}), \
                     but the spec this load is typed from (the newest run's recorded \
                     columns) lacks it — the load projects only those columns, so this \
                     column's data will be SILENTLY omitted from the warehouse. \
                     Re-extract after aligning the schema, or add the column back.",
                    c.name, m.run_id
                ));
            }
        }
    }
    notes
}

/// A CDC load carrying a snapshot leg over a `<table>` an earlier full load left: two baselines.
fn snapshot_over_full_table(snapshot_leg: bool, table: load::ObjectKind) -> bool {
    snapshot_leg && table == load::ObjectKind::Table
}

fn rebaseline_shape(uris: &[String], plan_prefix: &str) -> bool {
    let base = plan_prefix.trim_end_matches('/');
    uris.iter().any(|u| {
        u.strip_prefix(base)
            .map(|rest| rest.trim_start_matches('/'))
            .is_some_and(|rest| rest.starts_with("snapshot/"))
    })
}

/// What the re-baseline guard does, given the two signals it can read.
///
/// Round-8 lifecycle HIGH: a STATELESS load re-selects EVERY Success run each
/// cycle by design (at-least-once, absorbed by the view) — so its uris carry
/// the snapshot leg forever, and the warehouse probe is true from cycle 2 on:
/// the refusal wedged a HEALTHY pipeline into a routine-TRUNCATE loop (an
/// operator truncating a serving table every cycle). Only a LEDGERED load can
/// distinguish "genuine re-snapshot after a gap" (snapshot run NOT consumed)
/// from "routine re-selection" (it was) — so the refusal requires the ledger,
/// and stateless degrades to a note.
#[derive(Debug, PartialEq, Eq)]
enum RebaselineAction {
    Proceed,
    WarnStateless,
    Refuse,
}

/// `ledger` is a TRI-STATE (round-9): `state.is_some()` alone conflated a DB
/// BLIP with absent-by-design — the blip arm note-and-proceeded a genuine
/// post-gap baseline on exactly the co-located host whose ledger just errored
/// (and cleanup_source then deleted the evidence). An errored ledger fails
/// SAFE: refuse the snapshot-carrying load (nothing consumed), retry when the
/// DB is back. Known residual, documented rather than hidden: `StateStore::
/// open` auto-creates, so an EPHEMERAL-ledger host reads `Available` with an
/// empty consumed-set and still refuse-loops on `initial: snapshot` streams —
/// loud and actionable (keep a persistent ledger for CDC loads), never
/// silent. The anchor stamp does NOT lift this: a snapshot cannot express a
/// PK deleted during the gap, so the refusal stands for stamped legs too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerSignal {
    Available,
    AbsentByDesign,
    Errored,
}

impl LedgerSignal {
    /// The round-9 tri-state decision, NAMED (arch critic: the inline bool
    /// ladder in the excluded body had zero counted tokens — invisible to the
    /// purity gate and to mutants alike).
    fn classify(errored: bool, present: bool) -> Self {
        if errored {
            LedgerSignal::Errored
        } else if present {
            LedgerSignal::Available
        } else {
            LedgerSignal::AbsentByDesign
        }
    }
}

fn rebaseline_action(warehouse_has_changes: bool, ledger: LedgerSignal) -> RebaselineAction {
    match (warehouse_has_changes, ledger) {
        (false, _) => RebaselineAction::Proceed, // empty/truncated log: the recovery load
        (true, LedgerSignal::Available) => RebaselineAction::Refuse,
        (true, LedgerSignal::Errored) => RebaselineAction::Refuse,
        (true, LedgerSignal::AbsentByDesign) => RebaselineAction::WarnStateless,
    }
}

/// The RE-baseline REFUSAL (round-7 refuter, HIGH): the first cut was a warn
/// printed while the load PROCEEDED — so the runs got consumed, and the
/// prescribed "truncate + re-run" then selected nothing: the operator who
/// obeyed verbatim emptied the warehouse table with exit 0. Now the load
/// REFUSES BEFORE appending (nothing consumed), and the condition comes from
/// the WAREHOUSE (`changes_has_prior_changes`), never the ledger — after the
/// prescribed truncate the recovery re-run probes an empty log and sails
/// through, while ledger rows (which survive a truncate) would have deadlocked
/// it forever.
fn rebaseline_refusal(target_fqtn: &str, warehouse: crate::load::cdc::Warehouse) -> String {
    // The remedy must PARSE where the operator pastes it (round-8): backticks
    // are BigQuery-only; Snowflake takes the bare fqtn.
    let quoted = match warehouse {
        crate::load::cdc::Warehouse::BigQuery => format!("`{target_fqtn}__changes`"),
        crate::load::cdc::Warehouse::Snowflake => format!("{target_fqtn}__changes"),
    };
    rebaseline_refusal_text(&quoted)
}

fn rebaseline_refusal_text(quoted_changes: &str) -> String {
    format!(
        "refusing to append a RE-baseline: this load carries snapshot parquet, but \
         {quoted_changes} already holds real change rows — a re-snapshot row \
         carries NULL `__pos` and LOSES the dedup to every prior change, so the \
         current-state view would keep serving PRE-GAP values for exactly the rows \
         this re-snapshot fixed — and even an anchor-stamped baseline cannot \
         express a PK DELETED during the gap (no row, no tombstone: its stale \
         pre-gap rows would win). Nothing was consumed by this refusal. Recovery: \
         1) TRUNCATE TABLE {quoted_changes}; 2) re-run this same `rivet load` \
         — it will then append the baseline into the empty log and proceed \
         (cdc-failure-modes.md)."
    )
}

#[allow(clippy::too_many_arguments)] // the tri-state rides beside `state` (round-9)
fn load_one_cdc(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    engine: load::cdc::SourceEngine,
    pk: &[String],
    allow_source_drift: bool,
    rebuild_changelog: bool,
    state: Option<&StateStore>,
    ledger_errored: bool,
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
    if plan.layout.compacts() {
        return load_one_cdc_base(job, pk, allow_source_drift, state);
    }
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
        |loader, store, inputs, _legs| {
            // Round-6 re-baseline guard: warn BEFORE appending a snapshot leg
            // into a __changes prior cycles already fed (see rebaseline_shape/rebaseline_action).
            let shape = rebaseline_shape(&inputs.uris, &plan.gcs_prefix);
            let kind = load::before_write(loader.object_kind(&plan.table))?;
            if snapshot_over_full_table(shape, kind) {
                return Err(load::refused(format!(
                    "`{}` is a table from an earlier full load, and this CDC load carries an \
                     initial snapshot of the same table — the snapshot is a new baseline, so \
                     keep one: drop the table (the snapshot replaces it), or run the stream \
                     without `initial: snapshot` to keep the table's rows as the baseline",
                    loader.fqtn(&plan.table)
                )));
            }
            if shape {
                // The refusal stands EVEN FOR A STAMPED baseline (round-10
                // refuter, HIGH): the anchor stamp fixes the ORDERING half
                // (adds/updates rank correctly), but no snapshot can express a
                // PK DELETED during the gap — it has no row and no tombstone,
                // so its pre-gap change rows would win the dedup and the view
                // would serve the PK live-with-stale-values, silently and
                // permanently. TRUNCATE remains the only complete remedy when
                // the log already holds changes; a first cut here bypassed the
                // guard for stamped legs and reopened exactly that hole.
                // LEDGER FIRST (hostile-reviewer MUST-2): on the documented
                // STATELESS deployment the uris carry the snapshot leg every
                // cycle, and AbsentByDesign can never Refuse — probing there
                // billed a full __pos column scan (BigQuery) or a warehouse
                // resume (Snowflake) per table per cycle to choose between a
                // note and silence. The probe runs only when its answer can
                // change the decision.
                let ledger = LedgerSignal::classify(ledger_errored, state.is_some());
                let prior = match ledger {
                    LedgerSignal::AbsentByDesign => true, // any value: same arm
                    _ => load::before_write(loader.changes_has_prior_changes(&plan.table))?,
                };
                match rebaseline_action(prior, ledger) {
                    RebaselineAction::Refuse => {
                        return Err(load::refused(rebaseline_refusal(
                            &loader.fqtn(&plan.table),
                            loader.warehouse(),
                        )));
                    }
                    RebaselineAction::WarnStateless => eprintln!(
                        "  note: this STATELESS load re-selects the snapshot leg every \
                         cycle (at-least-once, absorbed by the dedup view) — the \
                         re-baseline refusal needs the ledger to tell a genuine \
                         re-snapshot from a routine re-load, so it does not apply here.",
                    ),
                    RebaselineAction::Proceed => {}
                }
            }
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
                inputs.ownership,
                rebuild_changelog,
            )?;
            Ok((report.rows_appended, report))
        },
        |inputs, report| eprintln!("{}", cdc_done_line(&inputs.integrity, report)),
    )
}

/// What an incremental load did: landed a whole-table run as `<table>`, or appended
/// deltas to `<table>__changes` behind the view.
#[derive(Debug)]
pub enum IncrementalReport {
    Table(load::LoadReport),
    Changelog(load::CdcLoadReport),
}

impl IncrementalReport {
    /// One line saying what landed where.
    pub fn summary(&self) -> String {
        match self {
            IncrementalReport::Table(r) => format!(
                "{} rows landed as table {}{}",
                r.rows_loaded,
                r.target_table,
                cleaned_suffix(r.source_cleaned)
            ),
            // The report says which target it produced: under base+buffer there is
            // no view to name, and telling an operator to read one would send them
            // to an object that does not exist (found by dogfooding the batch cycle).
            IncrementalReport::Changelog(r) => match r.target_kind {
                load::ChangelogTarget::View => format!(
                    "{} rows appended to {} | current-state view {}{}",
                    r.rows_appended,
                    r.changes_table,
                    r.target,
                    cleaned_suffix(r.source_cleaned)
                ),
                load::ChangelogTarget::Base => format!(
                    "{} rows buffered into {} | `rivet compact` merges them into {}{}",
                    r.rows_appended,
                    r.changes_table,
                    r.target,
                    cleaned_suffix(r.source_cleaned)
                ),
            },
        }
    }
}

/// Whether an incremental run's manifest says it re-read the whole table: a cursor
/// column, but no cursor value it resumed from.
fn is_first_pass(m: &crate::manifest::RunManifest) -> bool {
    m.source
        .extraction
        .as_ref()
        .is_some_and(|e| e.cursor_column.is_some() && e.cursor_low.is_none())
}

/// The pending incremental runs, split: the latest whole-table run (if any) lands as the
/// table, runs started after it are deltas, and runs it supersedes are only recorded.
struct SplitRuns {
    first_pass: Option<(String, crate::manifest::RunManifest)>,
    deltas: Vec<(String, crate::manifest::RunManifest)>,
    superseded: Vec<String>,
}

impl SplitRuns {
    fn has_deltas(&self) -> bool {
        !self.deltas.is_empty()
    }

    /// The whole-table run joins the deltas: appended into the change log (at least
    /// once — the view keeps the latest row per key) instead of landing as the table.
    fn whole_table_run_joins_the_log(&mut self) {
        if let Some(first) = self.first_pass.take() {
            self.deltas.push(first);
            self.deltas
                .sort_by(|a, b| a.1.started_at.cmp(&b.1.started_at));
        }
    }
}

/// Whether a whole-table run joins the change log rather than landing as `<table>`:
/// whenever that name is already taken. An incremental load never overwrites an existing
/// table — the table becomes the log's baseline and the run is appended to it — and it
/// cannot replace a view at all. Only an absent name is landed as a new table.
fn whole_table_run_joins_the_log(kind: load::ObjectKind) -> bool {
    matches!(kind, load::ObjectKind::Table | load::ObjectKind::View)
}

/// Why a whole-table run is appended to the change log instead of landing as `<table>`.
fn whole_table_run_note(kind: load::ObjectKind, fqtn: &str, run_id: &str) -> String {
    match kind {
        load::ObjectKind::View => format!(
            "  note: `{fqtn}` is already the current-state view over its change log — run \
             {run_id} re-read the whole table, so it is appended to the log (at least once; the \
             view keeps the latest row per key) instead of replacing it"
        ),
        _ => format!(
            "  note: `{fqtn}` already holds rows from an earlier load, and an incremental load \
             never overwrites a table that exists — it becomes the change log `{fqtn}__changes` \
             and run {run_id}'s whole pass is appended to it (at least once; the view keeps the \
             latest row per key), so rows the source has since dropped stay in the log"
        ),
    }
}

fn split_runs(runs: &[(String, crate::manifest::RunManifest)]) -> SplitRuns {
    let first_pass = runs
        .iter()
        .filter(|(_, m)| is_first_pass(m))
        .max_by(|a, b| a.1.started_at.cmp(&b.1.started_at))
        .cloned();
    let mut out = SplitRuns {
        deltas: Vec::new(),
        superseded: Vec::new(),
        first_pass,
    };
    for (key, m) in runs {
        match &out.first_pass {
            Some((_, f)) if f.run_id == m.run_id => {}
            Some((_, f)) if m.started_at <= f.started_at => out.superseded.push(m.run_id.clone()),
            _ => out.deltas.push((key.clone(), m.clone())),
        }
    }
    out.deltas
        .sort_by(|a, b| a.1.started_at.cmp(&b.1.started_at));
    out
}

/// Load a single export's INCREMENTAL runs. A run that re-read the whole table (the
/// first run, or one after `state reset`) lands as `<table>` exactly like a full load;
/// a delta APPENDs into `<table>__changes` — turning a `<table>` table into the log
/// first — behind a current-state view deduped to the latest row per PK by the
/// export's `cursor_column`. Ledger-driven exactly like CDC — only the not-yet-loaded
/// runs are loaded, so re-loads don't double and `cleanup_source` is safe.
fn load_one_incremental(
    plan: &load::plan::LoadPlan,
    run_id: &str,
    pk: &[String],
    allow_source_drift: bool,
    rebuild_changelog: bool,
    state: Option<&StateStore>,
    load_id: &str,
) -> Result<Option<IncrementalReport>> {
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
        |loader, store, inputs, legs| {
            // Under base+buffer the first pass IS the base: it lands as a table
            // (`run_load` refuses one that is not rivet's own) and the deltas go to
            // the buffer. Folding a whole pass into the log is the view layout's
            // answer, where the name is a view and cannot be overwritten.
            let base_and_buffer = plan.layout.log_is_disposable();
            let mut split = split_runs(&inputs.runs);
            if let Some((_, first)) = &split.first_pass
                && !base_and_buffer
            {
                let kind = load::before_write(loader.object_kind(&plan.table))?;
                if whole_table_run_joins_the_log(kind) {
                    eprintln!(
                        "{}",
                        whole_table_run_note(kind, &loader.fqtn(&plan.table), &first.run_id)
                    );
                    split.whole_table_run_joins_the_log();
                }
            }
            for id in &split.superseded {
                eprintln!(
                    "  note: run {id} started before the latest whole-table run and is superseded \
                     by it — recorded as loaded, its files are not read"
                );
            }
            let mut rows = 0u64;
            let mut report = None;
            let landed_table = split.first_pass.is_some();
            let has_deltas = split.has_deltas();
            if let Some(first) = split.first_pass {
                // The runs this leg consumes: the whole-table run and those it supersedes.
                let mut landed_ids = split.superseded.clone();
                landed_ids.push(first.1.run_id.clone());
                let uris = load::reconcile::select_load_uris(
                    store,
                    &plan.gcs_prefix,
                    std::slice::from_ref(&first),
                )?;
                let integrity =
                    load::reconcile::reconcile(std::slice::from_ref(&first.1), allow_source_drift)?;
                eprintln!(
                    "  incremental load {}: run {} holds the whole table (no cursor to resume \
                     from) — landing it as `{}`",
                    plan.table,
                    first.1.run_id,
                    loader.fqtn(&plan.table)
                );
                let cleanup = if has_deltas {
                    None
                } else {
                    cleanup_target(plan, store, state)
                };
                // The base carries the delete flag as DATA, like a CDC baseline:
                // the buffer's tombstones flip it, and the column must exist from
                // the first pass or the MERGE has nothing to set.
                let mut base_specs = plan.specs.clone();
                if base_and_buffer {
                    base_specs.push(load::cdc::flag_spec(loader.warehouse()));
                }
                let r = load::run_load(
                    loader,
                    &plan.table,
                    &base_specs,
                    &uris,
                    Some(integrity.file_rows),
                    cleanup,
                    inputs.ownership,
                )?;
                eprintln!("{}", full_done_line(&integrity, &r));
                legs.landed(&landed_ids, r.rows_loaded);
                report = Some(IncrementalReport::Table(r));
            }
            if has_deltas {
                let uris =
                    load::reconcile::select_load_uris(store, &plan.gcs_prefix, &split.deltas)?;
                let manifests: Vec<_> = split.deltas.iter().map(|(_, m)| m.clone()).collect();
                let integrity = load::reconcile::reconcile(&manifests, allow_source_drift)?;
                let ownership = if landed_table {
                    load::Ownership::Own
                } else {
                    inputs.ownership
                };
                let cleanup = cleanup_target(plan, store, state);
                let r = if base_and_buffer {
                    load::run_load_buffer(
                        loader,
                        &plan.table,
                        &plan.specs,
                        &uris,
                        pk,
                        Some(integrity.file_rows),
                        cleanup,
                    )?
                } else {
                    load::run_load_incremental(
                        loader,
                        &plan.table,
                        &plan.specs,
                        &uris,
                        pk,
                        &cursor,
                        Some(integrity.file_rows),
                        cleanup,
                        ownership,
                        rebuild_changelog,
                    )?
                };
                eprintln!("{}", cdc_done_line(&integrity, &r));
                rows += r.rows_appended;
                report = Some(IncrementalReport::Changelog(r));
            }
            let report = report
                .ok_or_else(|| anyhow::anyhow!("no loadable run among the selected manifests"))?;
            Ok((rows, report))
        },
        |_, _| {},
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
                "  load {} → {} | columns={} partition={} manifests={} parquet_files={} expected_rows={}",
                plan.table,
                plan.load.target.name(),
                plan.specs.len(),
                partition_label(plan),
                inputs.integrity.manifests,
                inputs.uris.len(),
                inputs.integrity.file_rows,
            );
        },
        |loader, store, inputs, _legs| {
            let cleanup = cleanup_target(plan, store, state);
            let report = load::run_load(
                loader,
                &plan.table,
                &plan.specs,
                &inputs.uris,
                Some(inputs.integrity.file_rows),
                cleanup,
                inputs.ownership,
            )?;
            Ok((report.rows_loaded, report))
        },
        |inputs, report| eprintln!("{}", full_done_line(&inputs.integrity, report)),
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
        use load::plan::{CdcLayout, LoadMode, LoadPlan, LoadSection, LoadTarget};
        let plan = LoadPlan {
            export_name: "c1".into(),
            unit: None,
            table: "content_items".into(),
            partition: None,
            specs: vec![],
            gcs_prefix: String::new(),
            destination: crate::config::DestinationConfig::default(),
            load: LoadSection {
                layout: None,
                target: LoadTarget::Bigquery {
                    project: "p".into(),
                    dataset: "d".into(),
                },
                cleanup_source: false,
                pk: load::plan::KeyColumns::Auto,
                allow_source_drift: false,
                gc_orphans: false,
                cluster_by: load::plan::KeyColumns::Auto,
                partition: None,
            },
            mode: LoadMode::Cdc,
            cursor_column: None,
            pk: vec![],
            clustering: load::plan::Clustering::Auto(vec![]),
            pinned_run: None,
            layout: CdcLayout::LogAndView,
        };
        let err = require_pk(&plan, "cdc").unwrap_err().to_string();
        assert!(err.contains("export `c1`"), "must name the export: {err}");
        assert!(
            !err.contains("content_items"),
            "must NOT label the table as the export: {err}"
        );
    }

    /// Round-7 rebuild of the round-6 guard: the SHAPE is prefix-anchored (a
    /// `snapshot` segment in the operator's own prefix, or a multiplex table
    /// named `snapshot`, must not read as a re-baseline), and the REFUSAL
    /// message prescribes the truncate-then-rerun sequence that the
    /// warehouse-probed condition makes safe. RED against un-anchoring the
    /// shape (bare contains) and against dropping either message half.
    #[test]
    fn rebaseline_shape_is_prefix_anchored_and_the_refusal_names_the_sequence() {
        let plan_prefix = "gs://b/exports/orders";
        // The real snapshot leg under THIS plan's prefix.
        assert!(rebaseline_shape(
            &["gs://b/exports/orders/snapshot/part-0.parquet".into()],
            plan_prefix
        ));
        // A cdc-only cycle is not a baseline.
        assert!(!rebaseline_shape(
            &["gs://b/exports/orders/cdc-000000.parquet".into()],
            plan_prefix
        ));
        // An operator prefix CONTAINING a snapshot segment is not a baseline
        // (the round-6 substring bug fired forever here).
        assert!(!rebaseline_shape(
            &["gs://b/analytics/snapshot/orders/cdc-000000.parquet".into()],
            "gs://b/analytics/snapshot/orders"
        ));
        // A multiplex TABLE named `snapshot`: its sub-prefix is the PLAN prefix
        // for its own load, so its cdc parts do not read as a baseline either.
        assert!(!rebaseline_shape(
            &["gs://b/exports/base/snapshot/cdc-000000.parquet".into()],
            "gs://b/exports/base/snapshot"
        ));
        // The action table (round-8): refusal requires BOTH a non-empty log and
        // a ledger; stateless notes, an empty log always proceeds (the recovery
        // load). RED against collapsing the ledgered arm.
        use LedgerSignal::*;
        use RebaselineAction::*;
        assert_eq!(LedgerSignal::classify(true, true), Errored, "errored wins");
        assert_eq!(LedgerSignal::classify(false, true), Available);
        assert_eq!(LedgerSignal::classify(false, false), AbsentByDesign);
        assert_eq!(rebaseline_action(true, Available), Refuse);
        assert_eq!(
            rebaseline_action(true, Errored),
            Refuse,
            "a ledger BLIP must fail safe — note-and-proceed appended a doomed \
             baseline and cleanup then deleted the evidence (round-9)"
        );
        assert_eq!(rebaseline_action(true, AbsentByDesign), WarnStateless);
        for l in [Available, Errored, AbsentByDesign] {
            assert_eq!(rebaseline_action(false, l), Proceed);
        }
        let msg = rebaseline_refusal("p.d.t", crate::load::cdc::Warehouse::BigQuery);
        assert!(msg.contains("TRUNCATE TABLE `p.d.t__changes`"));
        let sf = rebaseline_refusal("d.s.t", crate::load::cdc::Warehouse::Snowflake);
        assert!(
            sf.contains("TRUNCATE TABLE d.s.t__changes;") && !sf.contains("TRUNCATE TABLE `"),
            "the pasted remedy must parse on Snowflake (bare fqtn in the STATEMENT): {sf}"
        );
        assert!(msg.contains("re-run this same"));
        assert!(msg.contains("Nothing was consumed"));
    }

    const TARGET: &str = "proj.ds.orders";

    fn ctx<'a>(state: &'a StateStore, load_id: &'a str) -> LoadCtx<'a> {
        LoadCtx {
            source_ident: String::new(),
            active_at_fetch: Some(Default::default()),
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
            active_at_fetch: None,
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
    use load::plan::{CdcLayout, LoadMode, LoadPlan, LoadSection, LoadTarget};

    /// `ledger_status`: a `Refused` stop, however deep under context, is `refused`;
    /// anything else is `failed`.
    #[test]
    fn ledger_status_tells_a_stop_before_the_write_from_a_failure() {
        let stop = load::refused("refusing to overwrite `p.d.t`".into());
        assert_eq!(ledger_status(&stop), "refused");
        let wrapped = load::before_write::<()>(Err(anyhow::anyhow!("no partition statistics")))
            .unwrap_err()
            .context("loading orders");
        assert_eq!(ledger_status(&wrapped), "refused");
        assert_eq!(
            format!("{wrapped:#}"),
            "loading orders: no partition statistics",
            "the message keeps its chain"
        );
        assert_eq!(
            ledger_status(&anyhow::anyhow!("count validation failed")),
            "failed"
        );
    }

    /// The ownership guard must hold on EVERY `rivet load`, not just the first: the
    /// refusal it raises is journaled, and a `failed` row would make the foreign table
    /// rivet's own on the next cycle (a scheduler retry), which then overwrites it. The
    /// stop is journaled as `refused`, which `has_load_attempt` never counts. RED against
    /// recording the stop as `failed` (the pre-fix `record_failed` catch-all).
    #[test]
    fn a_refused_load_does_not_make_a_foreign_table_rivets_own() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        write_at(&dir, "base/part-0.parquet", b"x");
        let m = success_manifest("run-1", "part-0.parquet");
        write_at(
            &dir,
            "base/manifest-run-1.json",
            &serde_json::to_vec(&m).unwrap(),
        );
        let state = StateStore::open_in_memory().unwrap();
        let plan = plan_at(LoadMode::Full, prefix);
        let target = "p.d.orders";
        let ownership = |state: &StateStore| {
            prepare_load(&store, &plan, Some(state), target, false)
                .unwrap()
                .expect("the run is unloaded")
                .ownership
        };
        assert_eq!(ownership(&state), load::Ownership::Foreign);

        // What execute_load records when the run closure stops before writing.
        let ctx = LoadCtx {
            state: Some(&state),
            load_id: "load-1",
            export_name: "orders",
            target_fqtn: target,
            warehouse: "bigquery",
            mode: LoadMode::Full,
            source_prefix: prefix,
            source_ident: String::new(),
            active_at_fetch: Some(Default::default()),
        };
        let stop = load::refused("refusing to overwrite".into());
        ctx.record(&["run-1".to_string()], 0, ledger_status(&stop));
        assert_eq!(
            ownership(&state),
            load::Ownership::Foreign,
            "the second cycle refuses again"
        );
        assert_eq!(
            state.recent_loads(Some(target), 10).unwrap()[0].status,
            "refused",
            "and the stop is on the record"
        );

        // A failure AFTER the write is what makes the table rivet's own.
        ctx.record_failed(&["run-1".to_string()]);
        assert_eq!(ownership(&state), load::Ownership::Own);
    }

    /// The identity guard reads the SAME population the recorder writes. A live
    /// run's `Running` marker carries no schema/table and renders as the bare
    /// engine; when it sorted first in the listing it WAS `mine`, so one crashed
    /// run's leftover marker refused every later load of the table, forever.
    #[test]
    fn an_idle_drain_leaves_nothing_to_buffer() {
        assert_eq!(super::buffer_uris(Vec::new()), None);
        assert_eq!(
            super::buffer_uris(vec!["gs://b/p/a.parquet".to_string()]),
            Some(vec!["gs://b/p/a.parquet".to_string()])
        );
    }

    /// A stream named `users` over table `t` writes `export_name = t` under family
    /// `users` — the SAME name/family shape as a baseline leg. Classifying by names
    /// overwrote the base with the cycle's delta (live: 0 of 7 rows); the mode tells.
    #[test]
    fn a_stream_drain_is_never_taken_for_a_baseline_leg() {
        let mut drain = success_manifest("r1", "cdc-000.parquet");
        drain.export_name = "t".into();
        drain.export_family = "users".into();
        assert!(!super::is_baseline_leg(&drain), "mode: cdc is the stream");
        let mut leg = drain.clone();
        leg.mode = "batch".into();
        assert!(
            super::is_baseline_leg(&leg),
            "a batch run under the prefix is a leg"
        );
    }

    /// `rivet compact` merges every base-and-buffer table, CDC or incremental; every
    /// other plan is passed by with a reason that names what it is, never silently.
    #[test]
    fn compact_passes_by_everything_but_a_base_and_buffer_cdc_table() {
        use crate::load::plan::{CdcLayout, LoadMode};
        assert_eq!(
            super::compact_skip_reason(&LoadMode::Cdc, &CdcLayout::BaseAndBuffer),
            None
        );
        assert!(
            super::compact_skip_reason(&LoadMode::Cdc, &CdcLayout::LogAndView)
                .is_some_and(|w| w.contains("initial: snapshot"))
        );
        assert!(
            super::compact_skip_reason(&LoadMode::Full, &CdcLayout::LogAndView)
                .is_some_and(|w| w.contains("overwrites its table"))
        );
        // The LAYOUT decides, not the mode: an ordinary incremental export that
        // asked for `load.layout: base_buffer` has a buffer to merge, and one that
        // did not is skipped with the key that would give it one.
        assert_eq!(
            super::compact_skip_reason(&LoadMode::Incremental, &CdcLayout::BaseAndBuffer),
            None
        );
        assert!(
            super::compact_skip_reason(&LoadMode::Incremental, &CdcLayout::LogAndView)
                .is_some_and(|w| w.contains("base_buffer"))
        );
    }

    #[test]
    fn the_pin_names_the_newer_runs_it_passed_over_and_stays_quiet_otherwise() {
        assert_eq!(super::skipped_runs_note("orders", "r1", &[]), None);
        let note = super::skipped_runs_note("orders", "r1", &["r3", "r2"]).expect("named");
        assert!(note.contains("orders") && note.contains("r1"), "{note}");
        assert!(
            note.contains("2 newer run(s)") && note.contains("r3, r2"),
            "{note}"
        );
    }

    #[test]
    fn a_running_marker_does_not_impersonate_the_source_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        write_at(&dir, "base/part-1.parquet", b"x");
        let good = success_manifest("run-1", "part-1.parquet");
        write_at(
            &dir,
            "base/manifest-run-1.json",
            &serde_json::to_vec(&good).unwrap(),
        );
        // The marker: sorts FIRST (`run-0` < `run-1`), no committed parts, bare source.
        let mut marker = success_manifest("run-0", "part-0.parquet");
        marker.status = crate::manifest::ManifestStatus::Running;
        marker.source.schema = None;
        marker.source.table = None;
        marker.parts.clear();
        marker.part_count = 0;
        marker.row_count = 0;
        write_at(
            &dir,
            "base/manifest-run-0.json",
            &serde_json::to_vec(&marker).unwrap(),
        );

        // An earlier load of this table recorded the QUALIFIED identity.
        let state = StateStore::open_in_memory().unwrap();
        let target = "p.d.orders";
        LoadCtx {
            state: Some(&state),
            load_id: "load-0",
            export_name: "orders",
            target_fqtn: target,
            warehouse: "bigquery",
            mode: LoadMode::Cdc,
            source_prefix: prefix,
            source_ident: crate::manifest::identity_source(&good),
            active_at_fetch: Some(Default::default()),
        }
        .record(&["run-9".to_string()], 1, "success");

        let plan = plan_at(LoadMode::Cdc, prefix);
        let prepared = prepare_load(&store, &plan, Some(&state), target, false)
            .expect("a live run's marker is not another source");
        assert!(prepared.is_some(), "run-1 is unloaded and must be selected");
    }

    /// A resolved plan, so a test can vary the ONE field it is about.
    fn plan_at(mode: LoadMode, gcs_prefix: &str) -> LoadPlan {
        LoadPlan {
            export_name: "orders".into(),
            unit: None,
            table: "orders".into(),
            partition: None,
            specs: vec![],
            gcs_prefix: gcs_prefix.into(),
            destination: crate::config::DestinationConfig::default(),
            load: LoadSection {
                layout: None,
                target: LoadTarget::Bigquery {
                    project: "p".into(),
                    dataset: "d".into(),
                },
                cleanup_source: false,
                pk: load::plan::KeyColumns::Columns(vec!["id".into()]),
                allow_source_drift: false,
                gc_orphans: false,
                cluster_by: load::plan::KeyColumns::None,
                partition: None,
            },
            mode,
            cursor_column: None,
            pk: vec!["id".into()],
            clustering: load::plan::Clustering::Auto(vec![]),
            pinned_run: None,
            layout: CdcLayout::LogAndView,
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

    /// An incremental run's manifest: `cursor_low` is the value it resumed from, `None` on a whole-table pass.
    fn incremental_manifest(
        run: &str,
        started_at: &str,
        cursor_low: Option<&str>,
    ) -> (String, crate::manifest::RunManifest) {
        let mut m = success_manifest(run, "part.parquet");
        m.mode = "incremental".into();
        m.started_at = started_at.into();
        m.source.extraction = Some(
            serde_json::from_value(serde_json::json!({
                "strategy": "incremental",
                "cursor_column": "id",
                "cursor_low": cursor_low,
                "cursor_high": "9",
            }))
            .unwrap(),
        );
        (format!("base/{run}/manifest.json"), m)
    }

    fn ids(runs: &[(String, crate::manifest::RunManifest)]) -> Vec<String> {
        runs.iter().map(|(_, m)| m.run_id.clone()).collect()
    }

    #[test]
    fn the_latest_whole_table_run_lands_first_and_only_later_deltas_follow() {
        let split = split_runs(&[
            incremental_manifest("d0", "2026-09-01T00:00:00Z", Some("5")),
            incremental_manifest("f1", "2026-09-02T00:00:00Z", None),
            incremental_manifest("d3", "2026-09-04T00:00:00Z", Some("9")),
            incremental_manifest("d2", "2026-09-03T00:00:00Z", Some("7")),
        ]);
        assert_eq!(
            split.first_pass.map(|(_, m)| m.run_id).as_deref(),
            Some("f1")
        );
        assert_eq!(ids(&split.deltas), ["d2", "d3"], "deltas in start order");
        assert_eq!(split.superseded, ["d0"]);
    }

    #[test]
    fn two_whole_table_runs_keep_only_the_latest() {
        let split = split_runs(&[
            incremental_manifest("f1", "2026-09-01T00:00:00Z", None),
            incremental_manifest("d2", "2026-09-02T00:00:00Z", Some("3")),
            incremental_manifest("f3", "2026-09-03T00:00:00Z", None),
        ]);
        assert_eq!(
            split.first_pass.map(|(_, m)| m.run_id).as_deref(),
            Some("f3")
        );
        assert!(split.deltas.is_empty());
        assert_eq!(split.superseded, ["f1", "d2"]);
    }

    #[test]
    fn deltas_and_manifests_without_cursor_data_never_land_as_a_table() {
        let mut legacy = success_manifest("old", "part.parquet");
        legacy.started_at = "2026-08-01T00:00:00Z".into();
        let split = split_runs(&[
            incremental_manifest("d2", "2026-09-02T00:00:00Z", Some("3")),
            ("base/old/manifest.json".to_string(), legacy),
            incremental_manifest("d1", "2026-09-01T00:00:00Z", Some("1")),
        ]);
        assert!(split.first_pass.is_none());
        assert_eq!(ids(&split.deltas), ["old", "d1", "d2"]);
        assert!(split.superseded.is_empty());
    }

    #[test]
    fn a_landed_leg_leaves_only_the_rest_for_the_closing_record() {
        let all = ids(&[
            incremental_manifest("f1", "2026-09-01T00:00:00Z", None),
            incremental_manifest("d2", "2026-09-02T00:00:00Z", Some("3")),
            incremental_manifest("d3", "2026-09-03T00:00:00Z", Some("5")),
        ]);
        assert_eq!(remaining_run_ids(&all, &["f1".to_string()]), ["d2", "d3"]);
        assert_eq!(remaining_run_ids(&all, &[]), all);
        assert!(remaining_run_ids(&all, &all).is_empty());
        let one = ["f1".to_string()];
        assert!(closing_record_applies(&all, &[]), "one-row load");
        assert!(
            closing_record_applies(&[], &[]),
            "an up-to-date load still records"
        );
        assert!(
            closing_record_applies(&one, &one),
            "deltas after a landed leg"
        );
        assert!(
            !closing_record_applies(&[], &one),
            "the legs covered everything: no empty second row"
        );
    }

    #[test]
    fn has_deltas_reads_the_split() {
        let delta = incremental_manifest("d1", "2026-09-02T00:00:00Z", Some("1"));
        let first = incremental_manifest("f1", "2026-09-01T00:00:00Z", None);
        assert!(split_runs(std::slice::from_ref(&delta)).has_deltas());
        assert!(!split_runs(std::slice::from_ref(&first)).has_deltas());
        assert!(split_runs(&[first, delta]).has_deltas());
    }

    /// A whole-table run is landed as `<table>` ONLY when that name is free. An existing
    /// TABLE becomes the change log's baseline and the run is appended to it — an
    /// incremental load never overwrites a table that exists — and an existing VIEW (a
    /// re-run after `state reset`, a stateless cycle) is likewise appended, never replaced.
    /// RED against both pre-fix paths: `run_load` overwriting the table, and stopping on
    /// the view.
    #[test]
    fn a_whole_table_run_joins_the_log_whenever_the_target_already_exists() {
        use load::ObjectKind::*;
        assert!(whole_table_run_joins_the_log(View));
        assert!(whole_table_run_joins_the_log(Table));
        assert!(!whole_table_run_joins_the_log(Absent));

        let on_table = whole_table_run_note(Table, "p.d.orders", "f1");
        assert!(
            on_table.contains("never overwrites a table that exists"),
            "{on_table}"
        );
        assert!(on_table.contains("p.d.orders__changes"), "{on_table}");
        let on_view = whole_table_run_note(View, "p.d.orders", "f1");
        assert!(on_view.contains("instead of replacing it"), "{on_view}");

        let first = incremental_manifest("f1", "2026-09-02T00:00:00Z", None);
        let before = incremental_manifest("d0", "2026-09-01T00:00:00Z", Some("1"));
        let after = incremental_manifest("d2", "2026-09-03T00:00:00Z", Some("9"));
        let mut split = split_runs(&[after.clone(), first.clone(), before]);
        assert_eq!(split.superseded, ["d0"]);
        split.whole_table_run_joins_the_log();
        assert!(split.first_pass.is_none());
        let deltas: Vec<&str> = split
            .deltas
            .iter()
            .map(|(_, m)| m.run_id.as_str())
            .collect();
        assert_eq!(deltas, ["f1", "d2"], "in started-at order");
        assert_eq!(
            split.superseded,
            ["d0"],
            "a run the whole pass covers stays superseded"
        );

        let mut none = split_runs(std::slice::from_ref(&after));
        none.whole_table_run_joins_the_log();
        assert_eq!(none.deltas.len(), 1, "nothing to demote");
    }

    #[test]
    fn a_snapshot_leg_conflicts_only_with_a_full_load_table() {
        use load::ObjectKind::*;
        assert!(snapshot_over_full_table(true, Table));
        assert!(!snapshot_over_full_table(false, Table));
        assert!(!snapshot_over_full_table(true, View));
        assert!(!snapshot_over_full_table(true, Absent));
    }

    /// A minimal Success manifest whose one part exists in the store — enough for
    /// `prepare_load` to fetch, select and integrity-check it.
    /// A run that finished after the one the plan was typed from is refused this
    /// cycle; the pinned run itself and older runs pass, and an unpinned plan
    /// (stateless load) refuses nothing.
    #[test]
    fn a_run_that_finished_after_the_pinned_one_is_refused_this_cycle() {
        let mut older = success_manifest("r1", "p1.parquet");
        older.finished_at = "2026-08-21T00:00:30Z".into();
        let pinned = success_manifest("r2", "p2.parquet"); // 00:01:00Z
        let mut late = success_manifest("r3", "p3.parquet");
        late.finished_at = "2026-08-21T00:01:00.250Z".into();
        let pin = ("r2".to_string(), "2026-08-21T00:01:00Z".to_string());
        let runs = |ms: Vec<crate::manifest::RunManifest>| {
            ms.into_iter()
                .map(|m| (m.run_id.clone(), m))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            super::late_runs_refusal(
                "orders",
                &runs(vec![older.clone(), pinned.clone()]),
                Some(&pin)
            ),
            None
        );
        let why = super::late_runs_refusal("orders", &runs(vec![pinned, late]), Some(&pin))
            .expect("r3 is late");
        assert!(
            why.contains("r3") && why.contains("r2") && why.contains("orders"),
            "{why}"
        );
        assert_eq!(
            super::late_runs_refusal("orders", &runs(vec![older]), None),
            None
        );
    }

    /// The partition budget measures the files that land in the PARTITIONED
    /// target. Under base+buffer the stream's file goes into the buffer, which
    /// takes no partition — measuring it refused a load nothing would have
    /// written (a 5,000-day buffer file on a day-partitioned base, found by
    /// dogfooding). RED against returning every uri for a disposable log.
    #[test]
    fn the_partition_budget_measures_the_base_leg_only_under_base_and_buffer() {
        let mut baseline = success_manifest("r1", "r1-000.parquet");
        baseline.mode = "chunked".into();
        let stream = success_manifest("r2", "cdc-000.parquet");
        let runs = vec![
            ("base/manifest-r1.json".to_string(), baseline),
            ("base/manifest-r2.json".to_string(), stream.clone()),
        ];
        let uris = vec![
            "gs://b/base/r1-000.parquet".to_string(),
            "gs://b/base/cdc-000.parquet".to_string(),
        ];

        assert_eq!(
            budgeted_uris(CdcLayout::BaseAndBuffer, &runs, &uris),
            vec!["gs://b/base/r1-000.parquet".to_string()],
            "the buffer's file is never partitioned, so it is not budgeted"
        );
        assert_eq!(
            budgeted_uris(CdcLayout::LogAndView, &runs, &uris),
            uris,
            "a changelog IS partitioned like its table — every file is budgeted"
        );
        let stream_only = vec![("base/manifest-r2.json".to_string(), stream)];
        assert!(
            budgeted_uris(CdcLayout::BaseAndBuffer, &stream_only, &uris).is_empty(),
            "a cycle with no baseline leg writes nothing partitioned"
        );
    }

    fn success_manifest(run: &str, part: &str) -> crate::manifest::RunManifest {
        use crate::manifest::*;
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: run.into(),
            export_name: "orders".into(),
            export_family: "orders".into(),
            mode: "cdc".into(),
            started_at: "2026-08-21T00:00:00Z".into(),
            finished_at: "2026-08-21T00:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "gcs".into(),
                uri: "gs://b/base".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count: 1,
            part_count: 1,
            parts: vec![ManifestPart {
                part_id: 0,
                path: part.into(),
                rows: 1,
                size_bytes: 1,
                content_fingerprint: "xxh3:0123456789abcdef".into(),
                content_md5: String::new(),
                status: PartStatus::Committed,
            }],
            column_checksums: None,
            checksum_render: None,
            checksum_key_column: None,
            row_hash: None,
            split_window: None,
        }
    }

    /// Round-10: a column the staged parquet records but the live source lost
    /// must WARN (Snowflake silently omits it; counts stay green). Additive
    /// columns and checksum-less manifests stay silent. RED against dropping
    /// the contains-check.
    #[test]
    fn a_dropped_source_column_warns_and_an_added_one_does_not() {
        use crate::manifest::ColumnChecksum;
        let mut m = success_manifest("r1", "p.parquet");
        m.column_checksums = Some(vec![
            ColumnChecksum {
                name: "id".into(),
                checksum: "0".into(),
            },
            ColumnChecksum {
                name: "legacy_col".into(),
                checksum: "0".into(),
            },
        ]);
        let notes = spec_manifest_column_drift(&["id".into(), "brand_new".into()], &[m.clone()]);
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("legacy_col") && notes[0].contains("SILENTLY"));
        m.column_checksums = None;
        assert!(
            spec_manifest_column_drift(&["id".into()], &[m]).is_empty(),
            "checksum-less manifests carry no column names — nothing to compare"
        );
    }

    /// THE ROUND-4 TOCTOU, at the boundary and through the real producer: a run
    /// LIVE when `prepare_load` fetched the manifests finishes DURING the
    /// warehouse copy — by record time it is active in neither a record-time
    /// sample nor the ledger, yet the manifest this load consumed was its
    /// mid-flight snapshot. Recording it consumed strands every part it flushed
    /// after the fetch, permanently (`select_runs` skips consumed run_ids).
    ///
    /// `active_at_fetch` is produced by the REAL `prepare_load` over a real fs
    /// store + state DB (not fabricated — the fabricated-input class is exactly
    /// how this went unobserved), then `finish_run` lands between prepare and
    /// record, as the copy window does. RED against dropping the fetch-time
    /// union in `record` (the pre-fix record-time-only sampling).
    #[test]
    fn a_run_finishing_during_the_copy_is_not_recorded_as_consumed() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(&dir);
        let prefix = "gs://b/base";
        for (run, part) in [("run-live", "live-0.parquet"), ("r-done", "done-0.parquet")] {
            write_at(&dir, &format!("base/{part}"), b"x");
            let m = success_manifest(run, part);
            write_at(
                &dir,
                &format!("base/manifest-{run}.json"),
                &serde_json::to_vec(&m).unwrap(),
            );
        }
        let state = state_with_active_run(prefix); // r-live is WRITING at fetch time
        // …under the run_id the manifests carry:
        let plan = plan_at(LoadMode::Cdc, prefix);
        let inputs = prepare_load(&store, &plan, Some(&state), "p.d.orders", false)
            .unwrap()
            .expect("two unloaded Success runs must select");
        // Not inert: the fetch-time sample really names the live run.
        assert_eq!(
            inputs
                .active_at_fetch
                .as_ref()
                .map(|a| a.contains("run-live")),
            Some(true),
            "the fixture must catch the run mid-write, or this test grades nothing"
        );
        assert_eq!(inputs.source_run_ids.len(), 2);

        // The copy window: the run finishes AFTER the fetch, BEFORE record.
        state
            .finish_run("run-live", "success", "2026-08-21T00:02:00Z")
            .unwrap();

        let ctx = LoadCtx {
            state: Some(&state),
            load_id: "load-1",
            export_name: "orders",
            target_fqtn: "proj.ds.orders",
            warehouse: "bigquery",
            mode: LoadMode::Cdc,
            source_prefix: prefix,
            source_ident: inputs.source_ident.clone(),
            active_at_fetch: inputs.active_at_fetch.clone(), // execute_load's copy
        };
        ctx.record_success(&inputs.source_run_ids, 2);

        let loaded = state.loaded_source_run_ids("proj.ds.orders").unwrap();
        assert!(
            loaded.contains("r-done"),
            "positive control: the run terminal at FETCH time is consumed, got {loaded:?}"
        );
        assert!(
            !loaded.contains("run-live") && !loaded.contains("r-live"),
            "a run live at fetch time finished during the copy — consuming it strands its \
             post-fetch parts: {loaded:?}"
        );
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
        // A BARE engine is "this engine, table unrecorded" — an UNKNOWN, never
        // evidence of a second source (the rule `ensure_single_source` already
        // applies to the manifests under one prefix). A ledger row written from a
        // bare identity, or a load carrying one, must not refuse a qualified
        // sibling of the same engine: a snapshot-only cycle recorded `mysql`, the
        // next drain cycle carried `mysql:orders`, and every load was refused.
        assert!(
            conflicting_source_ident("mysql:orders", &["mysql".to_string()]).is_none(),
            "a bare prior of the same engine is not another source"
        );
        assert!(
            conflicting_source_ident("mysql", &["mysql:orders".to_string()]).is_none(),
            "a bare carrier of the same engine is not another source"
        );
        assert_eq!(
            conflicting_source_ident("mysql:orders", &["postgres".to_string()]).map(String::as_str),
            Some("postgres"),
            "a bare identity of a DIFFERENT engine is still evidence"
        );
        // Two QUALIFIED tables of the SAME engine are two sources: the coarsening
        // forgives a missing table, never a different one (in-diff mutant `==`→`!=`
        // on the same-engine arm survived without this case).
        assert_eq!(
            conflicting_source_ident("mysql:app.orders", &["mysql:app.payments".to_string()])
                .map(String::as_str),
            Some("mysql:app.payments"),
            "a different qualified table of the same engine is another source"
        );

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
            active_at_fetch: Some(Default::default()),
            runs: Vec::new(),
            ownership: load::Ownership::Own,
        };

        let appended = load::CdcLoadReport {
            rows_appended: 40,
            changes_table: "p.d.orders__changes".into(),
            target: "p.d.orders".into(),
            target_kind: load::ChangelogTarget::View,
            source_cleaned: false,
        };
        assert_eq!(
            cdc_done_line(&inputs.integrity, &appended),
            "  integrity ✓ source 100 → files 100 → appended 40 to p.d.orders__changes | \
             current-state view p.d.orders"
        );
        assert_eq!(
            cdc_ok_line(&appended),
            "40 row(s) appended to `p.d.orders__changes` | current-state view `p.d.orders`"
        );

        // The report says what it produced; the renderers ask it, not the plan.
        let buffered = load::CdcLoadReport {
            target_kind: load::ChangelogTarget::Base,
            ..appended.clone()
        };
        assert_eq!(
            cdc_done_line(&inputs.integrity, &buffered),
            "  integrity ✓ source 100 → files 100 → base p.d.orders | buffered 40 row(s) into \
             p.d.orders__changes",
            "the base layout names the base, never a view"
        );
        assert_eq!(
            cdc_ok_line(&buffered),
            "40 change row(s) buffered into `p.d.orders__changes` — `rivet compact` merges them \
             into `p.d.orders`"
        );
        let idle = load::CdcLoadReport {
            rows_appended: 0,
            ..buffered.clone()
        };
        assert_eq!(
            cdc_ok_line(&idle),
            "no changes buffered by this load into `p.d.orders__changes`; `rivet compact` has \
             nothing new for `p.d.orders`",
            "an idle drain must not promise a merge of nothing"
        );

        let cleaned = load::CdcLoadReport {
            source_cleaned: true,
            ..appended
        };
        assert_eq!(
            cdc_done_line(&inputs.integrity, &cleaned),
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
            full_done_line(&inputs.integrity, &full),
            "  integrity ✓ source 100 → files 100 → warehouse 100 rows in p.d.orders"
        );
        assert_eq!(
            full_done_line(
                &inputs.integrity,
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

#[cfg(test)]
mod load_message_tests {
    use super::*;

    /// The lease refusal names the table AND the remedy: an operator who reads it
    /// must know which load is blocked and that waiting is the whole fix.
    #[test]
    fn the_lease_refusal_names_the_table_and_the_remedy() {
        let m = lease_busy_message("p.d.orders");
        assert!(m.contains("`p.d.orders`"), "{m}");
        assert!(m.contains("another `rivet load`"), "{m}");
        assert!(m.contains("Wait for it, then retry."), "{m}");
    }

    /// Both incremental outcomes say what landed where: a whole table names the
    /// table, a delta names the changelog and the view it feeds, and a cleaned
    /// source is stated rather than silent.
    #[test]
    fn the_incremental_summary_says_what_landed_where() {
        let table = IncrementalReport::Table(load::LoadReport {
            rows_loaded: 7,
            target_table: "p.d.orders".into(),
            source_cleaned: false,
        });
        assert_eq!(table.summary(), "7 rows landed as table p.d.orders");
        let delta = IncrementalReport::Changelog(load::CdcLoadReport {
            rows_appended: 3,
            changes_table: "p.d.orders__changes".into(),
            target: "p.d.orders".into(),
            target_kind: load::ChangelogTarget::View,
            source_cleaned: true,
        });
        assert_eq!(
            delta.summary(),
            "3 rows appended to p.d.orders__changes | current-state view p.d.orders (source cleaned)"
        );
        // Under base+buffer the same delta lands in a BUFFER and there is no view
        // to send the operator to — the line must say what actually happened.
        let buffered = IncrementalReport::Changelog(load::CdcLoadReport {
            rows_appended: 3,
            changes_table: "p.d.orders__changes".into(),
            target: "p.d.orders".into(),
            target_kind: load::ChangelogTarget::Base,
            source_cleaned: false,
        });
        assert_eq!(
            buffered.summary(),
            "3 rows buffered into p.d.orders__changes | `rivet compact` merges them into p.d.orders"
        );
    }
}
