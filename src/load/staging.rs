//! The staged-prefix lifecycle: which runs are writing into an export prefix, and the
//! two deletes gated on that answer — `cleanup_source` and orphan GC.

use crate::destination::gcs::GcsStore;
use crate::error::Result;
use crate::load;
use crate::state::StateStore;
use anyhow::Context as _;

/// The runs the run-status ledger says are writing into `prefix`: `None` when stateless, `Some(Err)` when the query failed.
pub(super) fn ledger_writers(
    state: Option<&StateStore>,
    prefix: &str,
) -> Option<Result<std::collections::HashSet<String>>> {
    state.map(|s| s.active_run_ids_on_prefix(prefix))
}

/// Whether the run-status ledger says any run is writing into `prefix`, folded for the delete guards by [`ledger_says_active`].
fn ledger_says_writing(state: Option<&StateStore>, prefix: &str) -> bool {
    ledger_says_active(state.map(|s| s.has_active_run_on_prefix(prefix)))
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
pub(super) fn ledger_says_active(answer: Option<Result<bool>>) -> bool {
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
pub(super) fn prefix_is_active(ledger_active: bool, manifest_active: bool) -> bool {
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
pub(super) fn prefix_has_active_run(
    store: &crate::destination::gcs::GcsStore,
    prefix: &str,
    state: Option<&StateStore>,
) -> bool {
    let ledger_active = ledger_says_writing(state, prefix);
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
pub(super) enum CleanupVerdict {
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
pub(super) fn cleanup_verdict(requested: bool, active: impl FnOnce() -> bool) -> CleanupVerdict {
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
pub(super) fn cleanup_target<'a>(
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

/// [`cleanup_target`], plus the PREFIX lease the delete has to hold while it runs.
///
/// The table lease dies with `execute_load`'s frame, and the cleanup runs AFTER
/// that — so load A could finish, release the table, and only then wipe the prefix,
/// while load B of the SAME table had already taken the freed lease and begun
/// reading the manifests A was about to delete. A scheduler whose cycles overlap is
/// all it takes; `prefix_has_active_run` cannot see it, because it reads
/// `run_status`, which only EXTRACT runs write.
///
/// A separate PREFIX lease rather than a longer table lease, deliberately: widening
/// the table lease would hold it across network deletes and lengthen the window in
/// which `rivet compact` is refused, for a resource compact never touches.
///
/// Every answer but "held" cancels the delete. Stateless is the exception and stays
/// as it was — there is no lease to take and no second rivet to coordinate with, so
/// refusing there would break the documented stateless path for nothing.
pub(super) fn cleanup_target_leased<'a>(
    plan: &'a load::plan::LoadPlan,
    store: &'a crate::destination::gcs::GcsStore,
    state: Option<&'a StateStore>,
) -> (
    Option<(&'a crate::destination::gcs::GcsStore, &'a str)>,
    Option<crate::state::LoadLease<'a>>,
) {
    let target = cleanup_target(plan, store, state);
    if target.is_none() {
        return (None, None);
    }
    let Some(s) = state else {
        return (target, None);
    };
    match s.try_load_lease(&plan.gcs_prefix) {
        Ok(Some(lease)) => (target, Some(lease)),
        Ok(None) => {
            eprintln!(
                "  cleanup [{}]: SKIPPED — another rivet holds {} right now. The load itself \
                 succeeded; only the staged Parquet is left in place, and the next load with \
                 `cleanup_source` removes it.",
                plan.table, plan.gcs_prefix
            );
            (None, None)
        }
        Err(e) => {
            eprintln!(
                "  cleanup [{}]: SKIPPED — could not take the prefix lease on {} ({e:#}). \
                 Not deleting what cannot be confirmed idle; the load itself succeeded.",
                plan.table, plan.gcs_prefix
            );
            (None, None)
        }
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
pub(super) fn maybe_gc_orphans(
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
    let ledger_active = ledger_says_writing(state, &plan.gcs_prefix);
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

/// Clean up iff `cleanup` is `Some`, downgrading a failure to a warning — the
/// data is loaded and gated, so a stuck delete must not fail the load. Cleanup
/// runs the driver's own [`delete_under`] over an injected [`GcsStore`], so no
/// adapter owns a delete path. Returns whether the source was actually cleaned.
pub(super) fn maybe_cleanup(cleanup: Option<(&GcsStore, &str)>) -> bool {
    match cleanup {
        Some((store, prefix)) => match delete_under(store, prefix) {
            Ok(()) => true,
            Err(e) => {
                eprintln!(
                    "warning: source cleanup failed (data is safely loaded): {}",
                    crate::redact::redact_secrets(&format!("{e:#}"))
                );
                false
            }
        },
        None => false,
    }
}

/// Recursively delete a whole export-dedicated `gs://…/` prefix through an
/// injected [`GcsStore`] — the driver's post-gate source cleanup, over the same
/// native opendal GCS client the export destination uses (no `gcloud`). Taking
/// the store as an argument (rather than each adapter building one from a
/// config) is what lets an fs-backed store exercise this delete offline.
pub(crate) fn delete_under(store: &GcsStore, gs_prefix: &str) -> Result<()> {
    let (_, rel) = load::split_object_uri(gs_prefix)?;
    store
        .remove_all(rel)
        .with_context(|| format!("source cleanup (recursive delete of {gs_prefix}) failed"))
}
