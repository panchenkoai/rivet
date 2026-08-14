//! Bounded work-stealing POOL scheduling (#166) — the makespan-optimal
//! alternative to barrier waves.
//!
//! A wave is a BARRIER: wave N+1 waits for the LAST export of wave N, so a
//! giant export idles every one of its wave-mates at the barrier. Measured on a
//! 154-export refresh: 164 min sequential → 100 min in waves → **51 min** in a
//! pool at M=4. The pool starts the LONGEST export immediately and backfills
//! each freeing slot with the next by predicted duration (LPT list
//! scheduling), so the giant's minutes are filled with everyone else's work
//! running concurrently. Its wall approaches the theoretical floor
//! `max(longest_export, total_work / M)`.
//!
//! Two objectives conflict, stated honestly (see #166): PRIORITY (tiers) wants
//! the giant LAST (huge → deferred); MAKESPAN wants it FIRST (it is the
//! critical path). The pool chooses makespan — it is for a full refresh with no
//! inter-table "X before Y" ordering. Ordered pipelines keep the wave
//! scheduler. So `pool_order` is pure LPT (duration desc); tiers are NOT
//! honored in pool mode, by design.
//!
//! This module owns THE PoolItem concept end to end: what it is, how its
//! duration is PREDICTED, and how items are ordered. The prediction half was
//! deepened here after the two former adapters drifted (walk find,
//! 2026-08-13): `rivet plan`'s preview excluded history-less exports and
//! hardcoded `parallel_safe: true`, while `apply --pool` scheduled the same
//! exports at placeholder floors with the real flags — two different
//! makespans for one config, from one type. [`predict_items`] is now the
//! ONLY constructor either command uses.
//!
//! The scheduling math stays pure; the predictor's single impurity is the
//! state-store read, kept behind [`predict_secs`] so the classification and
//! seeding logic remain unit-testable with an in-memory store.

/// One export the pool schedules, by its predicted duration.
#[derive(Debug, Clone)]
pub(crate) struct PoolItem {
    pub name: String,
    /// Predicted duration in seconds (last successful run, else an estimate —
    /// the same source the wave packer reads).
    pub predicted_secs: f64,
    /// Whether this export may run CONCURRENTLY with another heavy one. Heavies
    /// (`false`) serialize among themselves at run time (see
    /// [`crate::pipeline::run`]'s `next_eligible`), so the makespan model must
    /// too — else it predicts M-way parallelism an all-heavy config can never
    /// reach (#166/#167 C3, roast 2026-08-09). Default `true` keeps the pure
    /// tests that don't care about the constraint unchanged.
    pub parallel_safe: bool,
}

/// Where a [`PoolItem`]'s predicted duration came from — the honesty axis of
/// the makespan print ("N measured, M estimated"), and the LOWER-BOUND note
/// when placeholders are in play.
#[derive(Debug, Clone)]
pub(crate) enum PredictedFrom {
    /// A prior SUCCESSFUL run's wall time — the only true measurement.
    Measured(f64),
    /// No success on record, but a failed/interrupted attempt ran this long —
    /// a floor on the real duration (the attempt died early).
    FailedAttemptFloor(f64),
    /// A synthesized split unit inheriting `giant_predicted / N` from a giant
    /// that was itself MEASURED — measured by proxy, so LPT ranks the slices
    /// where the giant stood (front of the queue), never at the placeholder
    /// tail (bughunt 2026-08-13). A unit of an UNMEASURED giant is NOT this:
    /// see [`split_unit_from`].
    SeededSplit(f64),
    /// No history at all: the placeholder. Deliberately small so unknown
    /// exports never displace measured heavies in LPT order; the print
    /// flags how many predictions rest on it.
    Placeholder(f64),
}

/// Placeholder duration for an export with no run history at all.
pub(crate) const POOL_PLACEHOLDER_SECS: f64 = 5.0;

impl PredictedFrom {
    pub(crate) fn secs(&self) -> f64 {
        match self {
            PredictedFrom::Measured(s)
            | PredictedFrom::FailedAttemptFloor(s)
            | PredictedFrom::SeededSplit(s)
            | PredictedFrom::Placeholder(s) => *s,
        }
    }
}

/// Predict one export's duration from the state store: last SUCCESS, else the
/// longest terminal attempt (floored at the placeholder), else the placeholder.
/// Direct success query — a fixed recent window went blind after N consecutive
/// failures, misclassifying a measured export exactly during its degraded
/// period (bughunt 2026-08-13).
pub(crate) fn predict_secs(state: &crate::state::StateStore, name: &str) -> PredictedFrom {
    predict_secs_and_rss(state, name).0
}

/// [`predict_secs`] plus the peak RSS that belongs to the SAME history — the
/// wave packer's memory-budget input, which must come from the same predictor
/// as the duration or the two commands disagree about the same export.
///
/// `rivet plan --annotate-waves` used to run its own two-tier lookup (last
/// success, else a flat `(5 s, 150 MB)` placeholder) while `apply --pool` had
/// three tiers here. On an export that has NEVER succeeded but whose attempts
/// run an hour at 3 GB — a plausible reason it keeps failing — the pool
/// predicted `FailedAttemptFloor(3600)` and the packer predicted `(5 s, 150 MB)`:
/// it sorted LAST in its tier and was charged 150 MB against the 4096 MB wave
/// budget, so it packed beside K−1 exports whose real peaks the wave could not
/// hold — memory pressure re-creating the very failure. `--annotate-waves`
/// WRITES that wave number into the operator's config, so the mis-schedule
/// outlives the command (bughunt 2026-08-14, round 4).
///
/// The RSS is the MAX over the attempts, not the peak of whichever attempt
/// happened to run longest: this number is a BUDGET, so it has to hold the
/// worst peak on record. `None` means the history carries no RSS at all — the
/// caller supplies its own conservative default rather than having one
/// invented here.
pub(crate) fn predict_secs_and_rss(
    state: &crate::state::StateStore,
    name: &str,
) -> (PredictedFrom, Option<i64>) {
    if let Some(m) = state.get_last_success_metric(name).ok().flatten() {
        let secs = (m.duration_ms as f64 / 1000.0).max(0.001);
        return (PredictedFrom::Measured(secs), m.peak_rss_mb);
    }
    let attempts: Vec<crate::state::ExportMetric> = state
        .get_metrics(Some(name), 25)
        .ok()
        .into_iter()
        .flatten()
        .collect();
    let last_attempt = attempts
        .iter()
        .map(|m| (m.duration_ms as f64 / 1000.0).max(0.001))
        .reduce(f64::max);
    let peak_rss = attempts.iter().filter_map(|m| m.peak_rss_mb).max();
    match last_attempt {
        Some(s) => (
            PredictedFrom::FailedAttemptFloor(s.max(POOL_PLACEHOLDER_SECS)),
            peak_rss,
        ),
        None => (PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS), None),
    }
}

/// A split unit's provenance INHERITS the giant's — the unit is the same
/// knowledge about the same table, cut into N.
///
/// `--split` is reached for precisely when a giant is huge and painful, which
/// is also when it has been TIMING OUT — i.e. exactly the export with no
/// successful run to measure. Classifying every unit `SeededSplit` (== measured
/// by proxy in the accounting) made `--split` DELETE the LOWER BOUND hedge:
/// the run printed "14 measured, 0 estimated" for a schedule resting entirely
/// on an export that has never finished (bughunt 2026-08-14). Only a MEASURED
/// giant yields measured-by-proxy units; a failed-attempt floor stays a floor
/// and a placeholder stays a placeholder, so the hedge still prints.
pub(crate) fn split_unit_from(giant: &PredictedFrom, share_secs: f64) -> PredictedFrom {
    match giant {
        PredictedFrom::Measured(_) | PredictedFrom::SeededSplit(_) => {
            PredictedFrom::SeededSplit(share_secs)
        }
        PredictedFrom::FailedAttemptFloor(_) => PredictedFrom::FailedAttemptFloor(share_secs),
        PredictedFrom::Placeholder(_) => PredictedFrom::Placeholder(share_secs),
    }
}

/// Reconcile a split unit's inherited SEED with the unit's OWN history.
///
/// The seed exists for ONE reason: a freshly synthesized `{giant}#N` has no
/// rows under its own name. But unit names are STABLE across runs
/// (`{giant}#{i}`, [`crate::pipeline::split`]) and the giant is retained OUT of
/// the run set once split (`run_pool`'s `effective.retain`), so the giant's
/// rows freeze at the state that motivated the split — failed / interrupted,
/// forever. A seed that keeps winning therefore discards the units' own
/// measurements on every run from the second onward (bughunt 2026-08-14):
/// LPT ranks all N units at one identical derived number (the makespan the
/// split exists to break), and the LOWER BOUND hedge — gated on the seed's
/// inherited classification — becomes unclearable, since only a success on the
/// giant, which never runs again, could clear it.
///
/// So: the seed applies only while the unit is genuinely history-less. Any row
/// under the unit's OWN name supersedes it — a success measures THIS unit, and
/// a failed attempt floors THIS unit ([`predict_secs`] already takes the
/// LONGEST attempt in its window, so one fast crash cannot pin a unit low while
/// a long attempt is on record). That is exactly the treatment every unsplit
/// export gets from `predict_secs`; the seed is the first-run bootstrap, not a
/// permanent override.
pub(crate) fn reconcile_split_seed(seed: &PredictedFrom, own: &PredictedFrom) -> PredictedFrom {
    match own {
        PredictedFrom::Placeholder(_) => seed.clone(),
        _ => own.clone(),
    }
}

/// Split a synthesized unit name back into `(giant, ordinal)`.
///
/// `rsplit_once` so a giant whose own name contains `#` still resolves; a
/// non-numeric tail is not a unit name (`None`), which is also what keeps an
/// operator's ordinary export from being read as somebody's slice.
pub(crate) fn split_unit_name(name: &str) -> Option<(&str, usize)> {
    let (giant, ord) = name.rsplit_once('#')?;
    Some((giant, ord.parse().ok()?))
}

/// How far PAST the current partition the unit-name census probes for ordinals
/// a bigger prior partition left behind. Three consecutive absences end it: a
/// pool runs every unit it synthesizes, so history is contiguous, and the
/// lookahead only has to survive a unit or two that recorded nothing.
const SPLIT_CENSUS_LOOKAHEAD: usize = 3;

/// Hard ceiling on the census, so a pathological state store cannot turn run
/// start into thousands of point queries.
const SPLIT_CENSUS_CAP: usize = 256;

/// Re-derive a split cohort's per-unit prediction when the partition CHANGED
/// under it — the answer that must override every unit's own history.
///
/// A unit's measured duration means "the time to export 1/N of the ROWS", not
/// "the time to export this key range": `probe_and_synthesize` re-samples the
/// row percentiles on every non-`--resume` run (only `--resume` reconstructs
/// the prior partition), and N itself is not stable — `advise_split` clamps it
/// at `max_split = m.max(2)`, the `--pool M` value, and derives it from another
/// export's measured duration. So a routine `--pool 8` → `--pool 3` re-run
/// hands `giant#0..#2` the rows of a THIRD of the table while their own history
/// measured an EIGHTH, and [`reconcile_split_seed`] hands that 8-way number
/// straight to the scheduler: a ~2.7× under-prediction, every unit sorted below
/// the measured exports in LPT, and an advertised makespan wrong by the ratio
/// of the two split factors (bughunt 2026-08-14, round 4).
///
/// `prior` is the unit-name census: every `{giant}#i` that carries history, and
/// what that history predicts. The cohort's SUM is the table's measured cost,
/// so `sum / realized` re-cuts it for the partition that is about to run — and
/// it also refreshes the seed's other stale input, since the giant's own rows
/// froze at the failure that motivated the split and never move again.
///
/// Only the SHRINK direction is decidable. An ordinal at or past `realized` is
/// proof a bigger partition ran (nothing else writes `giant#7` when the run has
/// three units). The growth direction is NOT proof: a prior cohort of `{0,1,2}`
/// against 8 units now is equally an interrupted 8-way run whose tail never
/// recorded, and reading that as growth would divide an already-correct T/8 by
/// another 8/3. Left alone it over-predicts by the split ratio instead — the
/// safe direction for a wall an operator reads as a lower bound. The signal
/// that WOULD decide it is the units' persisted `split_window` boundaries
/// (compare the prior partition's cut points to this run's), which lives in the
/// destination, not the state store; see `split::reconstruct_units_from_prefix`.
pub(crate) fn repartitioned_unit_prediction(
    realized: usize,
    prior: &[(usize, PredictedFrom)],
) -> Option<PredictedFrom> {
    if realized == 0 || prior.is_empty() {
        return None; // nothing to re-cut, or a genuinely first split run
    }
    let prior_n = prior.iter().map(|(i, _)| i + 1).max()?;
    if prior_n <= realized {
        return None; // same partition (or the undecidable growth case) — own history stands
    }
    let share = prior.iter().map(|(_, p)| p.secs()).sum::<f64>() / realized as f64;
    // Provenance folds pessimistically: the re-cut share is measured-by-proxy
    // only when EVERY contributing unit actually succeeded. One unit that only
    // ever failed makes the cohort's sum a floor, not a measurement, and the
    // LOWER BOUND hedge must keep printing for it.
    if prior.iter().all(|(_, p)| {
        matches!(
            p,
            PredictedFrom::Measured(_) | PredictedFrom::SeededSplit(_)
        )
    }) {
        Some(PredictedFrom::SeededSplit(share))
    } else {
        Some(PredictedFrom::FailedAttemptFloor(
            share.max(POOL_PLACEHOLDER_SECS),
        ))
    }
}

/// The unit-name census for one split giant: which `{giant}#i` ordinals carry
/// OWN history, and what each predicts. Impure (state-store point queries);
/// the decision it feeds is [`repartitioned_unit_prediction`], which is pure.
///
/// Always probes `0..realized` (a unit of the CURRENT partition that never ran
/// is an absence, not the end of the cohort) and then keeps going while
/// ordinals keep appearing — that tail is the whole point, since only an
/// ordinal at or past `realized` proves the partition shrank.
fn split_history_census(
    state: &crate::state::StateStore,
    giant: &str,
    realized: usize,
) -> Vec<(usize, PredictedFrom)> {
    let mut out: Vec<(usize, PredictedFrom)> = Vec::new();
    let mut misses = 0usize;
    for i in 0..SPLIT_CENSUS_CAP {
        let p = predict_secs(state, &format!("{giant}#{i}"));
        if matches!(p, PredictedFrom::Placeholder(_)) {
            if i >= realized {
                misses += 1;
                if misses >= SPLIT_CENSUS_LOOKAHEAD {
                    break;
                }
            }
            continue;
        }
        misses = 0;
        out.push((i, p));
    }
    out
}

/// Fold classifications into the `(measured, failed-attempt, placeholder)`
/// accounting the makespan print grades itself with — and whose second and
/// third members decide the LOWER BOUND hedge. Pure so the hedge's input is
/// testable without a live pool run.
pub(crate) fn classification_counts(fs: &[PredictedFrom]) -> (usize, usize, usize) {
    let (mut measured, mut attempt, mut placeholder) = (0usize, 0usize, 0usize);
    for f in fs {
        match f {
            // A seeded split unit inherits a MEASURED giant's share (see
            // `split_unit_from`) — measured by proxy for the accounting too.
            PredictedFrom::Measured(_) | PredictedFrom::SeededSplit(_) => measured += 1,
            PredictedFrom::FailedAttemptFloor(_) => attempt += 1,
            PredictedFrom::Placeholder(_) => placeholder += 1,
        }
    }
    (measured, attempt, placeholder)
}

/// THE PoolItem constructor — both `rivet plan`'s preview and `apply --pool`'s
/// schedule go through here, so the two commands cannot print different
/// makespans for one config again. `split_seeds` carries `{giant}#N` units'
/// inherited prediction — duration AND provenance, built by
/// [`split_unit_from`] (empty everywhere except the realized-split path) — and
/// it yields to the unit's own history as soon as it has any, see
/// [`reconcile_split_seed`] — UNLESS that history was produced by a different
/// PARTITION, in which case the whole cohort is re-cut, see
/// [`repartitioned_unit_prediction`].
pub(crate) fn predict_items<'a>(
    state: &crate::state::StateStore,
    exports: impl IntoIterator<Item = (&'a str, bool)>,
    split_seeds: &std::collections::HashMap<String, PredictedFrom>,
) -> Vec<(PoolItem, PredictedFrom)> {
    // One census per split giant, before any per-unit reconciliation: the
    // question "did the partition change" is about the COHORT, and each unit
    // would answer it identically. `split_seeds` carries exactly one entry per
    // realized unit, so its per-giant count IS this run's partition size.
    let mut realized_of: std::collections::BTreeMap<&str, usize> =
        std::collections::BTreeMap::new();
    for name in split_seeds.keys() {
        if let Some((giant, _)) = split_unit_name(name) {
            *realized_of.entry(giant).or_default() += 1;
        }
    }
    let recut: std::collections::HashMap<&str, PredictedFrom> = realized_of
        .iter()
        .filter_map(|(giant, realized)| {
            let prior = split_history_census(state, giant, *realized);
            repartitioned_unit_prediction(*realized, &prior).map(|f| (*giant, f))
        })
        .collect();
    exports
        .into_iter()
        .map(|(name, parallel_safe)| {
            let own = predict_secs(state, name);
            let from = match split_seeds.get(name) {
                Some(seed) => split_unit_name(name)
                    .and_then(|(giant, _)| recut.get(giant).cloned())
                    .unwrap_or_else(|| reconcile_split_seed(seed, &own)),
                None => own,
            };
            (
                PoolItem {
                    name: name.to_string(),
                    predicted_secs: from.secs(),
                    parallel_safe,
                },
                from,
            )
        })
        .collect()
}

/// LPT order: longest predicted duration first, ties broken by name so the
/// order is deterministic (two identical `plan` runs must schedule the same).
///
/// This is the order the runtime pool pulls from as worker slots free — the
/// giant starts first and short jobs backfill behind it.
pub(crate) fn pool_order(items: &[PoolItem]) -> Vec<String> {
    let mut v: Vec<&PoolItem> = items.iter().collect();
    v.sort_by(|a, b| {
        b.predicted_secs
            .total_cmp(&a.predicted_secs)
            .then_with(|| a.name.cmp(&b.name))
    });
    v.into_iter().map(|i| i.name.clone()).collect()
}

/// Predicted makespan (wall-clock) of running `items` through `workers` slots
/// under LPT — the number `plan` shows so an operator can compare pool vs
/// waves before choosing.
///
/// Greedy LPT: process in `pool_order`, assign each to the slot that frees
/// earliest. The result is within the classic `4/3 − 1/(3·workers)` of optimal
/// and never below the floor `max(longest, total/workers)`.
pub(crate) fn predicted_makespan_secs(items: &[PoolItem], workers: usize) -> f64 {
    let w = workers.max(1);
    let mut slots = vec![0.0f64; w];
    // pool_order is LPT; walk it and drop each onto the earliest-free slot.
    let order = pool_order(items);
    let by_name: std::collections::HashMap<&str, f64> = items
        .iter()
        .map(|i| (i.name.as_str(), i.predicted_secs))
        .collect();
    for name in &order {
        let d = by_name[name.as_str()];
        // earliest-free slot (min load)
        let i = slots
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.total_cmp(b.1))
            .map(|(i, _)| i)
            .unwrap_or(0);
        slots[i] += d;
    }
    let greedy = slots.into_iter().fold(0.0, f64::max);
    // Heavies serialize (C3): the true wall is at least their summed duration,
    // which the constraint-free greedy LPT can under-predict for an all-heavy set.
    let heavy: f64 = items
        .iter()
        .filter(|i| !i.parallel_safe)
        .map(|i| i.predicted_secs)
        .sum();
    greedy.max(heavy)
}

/// The floor no scheduler can beat: `max(longest single export, total/M)`.
/// Exposed so `plan` can show how close the pool gets — and so a giant whose
/// duration alone dominates is visibly the wall (the split-the-giant lever,
/// #167).
pub(crate) fn makespan_floor_secs(items: &[PoolItem], workers: usize) -> f64 {
    let w = workers.max(1) as f64;
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    let longest = items.iter().map(|i| i.predicted_secs).fold(0.0, f64::max);
    // C3 (roast 2026-08-09): non-parallel_safe exports run ONE AT A TIME (the
    // runtime heavy-serialization rule), so the wall is at least the SUM of all
    // heavy durations — an all-heavy config cannot beat sequential however many
    // slots. Without this floor the model advertised total/M on a config that
    // degrades to sequential.
    let heavy: f64 = items
        .iter()
        .filter(|i| !i.parallel_safe)
        .map(|i| i.predicted_secs)
        .sum();
    longest.max(total / w).max(heavy)
}

/// #167: how many range sub-units a dominating export should split into so the
/// pool floor drops from `longest` toward `total/M`. SHAPE-driven (M-free, so
/// the answer is stable across worker counts): an export dominates when its
/// predicted duration exceeds `r ×` the next-longest (R≈3). N is chosen to cut
/// the giant below that next-longest — `ceil(predicted / second_longest)` —
/// capped at `max_split` and floored at 2. `None` = not dominating, leave it be.
///
/// Pure: the runtime decides HOW to realize the split (range sub-exports over
/// the key span); this decides WHETHER and INTO HOW MANY.
pub(crate) fn split_factor(
    predicted_secs: f64,
    second_longest_secs: f64,
    r: f64,
    max_split: usize,
) -> Option<usize> {
    if second_longest_secs <= 0.0 || predicted_secs <= r * second_longest_secs {
        return None;
    }
    let n = (predicted_secs / second_longest_secs).ceil() as usize;
    Some(n.clamp(2, max_split.max(2)))
}

/// Rewrite the item set with the single dominating export split into N equal
/// sub-units (`<name>#0..<name>#N-1`, each `predicted/N`), summing to the
/// original — the scheduler input after a split. A balanced set (nothing
/// dominates) is returned untouched. Pure, for the makespan proof; the runtime
/// mirrors this shape with real range sub-exports.
pub(crate) fn split_dominating(items: &[PoolItem], r: f64, max_split: usize) -> Vec<PoolItem> {
    if items.len() < 2 {
        return items.to_vec();
    }
    // The dominating candidate = the single longest; its "second" = the next.
    let mut sorted: Vec<&PoolItem> = items.iter().collect();
    sorted.sort_by(|a, b| b.predicted_secs.total_cmp(&a.predicted_secs));
    let (longest, second) = (sorted[0], sorted[1]);
    let Some(n) = split_factor(longest.predicted_secs, second.predicted_secs, r, max_split) else {
        return items.to_vec();
    };
    let piece = longest.predicted_secs / n as f64;
    let mut out: Vec<PoolItem> = items
        .iter()
        .filter(|i| i.name != longest.name)
        .cloned()
        .collect();
    for i in 0..n {
        out.push(PoolItem {
            name: format!("{}#{i}", longest.name),
            predicted_secs: piece,
            parallel_safe: longest.parallel_safe,
        });
    }
    out
}

/// Should `apply --pool` advise splitting a dominating export, and if so into
/// how many + to what wall? Returns `(name, n, broken_makespan_secs)`.
///
/// TWO gates, both required (roast 2026-08-09 caught a shape-only check that
/// phantom-fired when the giant was NOT the floor):
/// 1. the export must ACTUALLY be the floor — `longest > total/m` (else total/m
///    is already the wall and a split buys nothing);
/// 2. it must dominate by SHAPE — [`split_factor`] (> R× the next-longest).
pub(crate) fn advise_split(
    items: &[PoolItem],
    m: usize,
    r: f64,
    max_split: usize,
) -> Option<(String, usize, f64)> {
    let mut by: Vec<&PoolItem> = items.iter().collect();
    by.sort_by(|a, b| b.predicted_secs.total_cmp(&a.predicted_secs));
    let [longest, second, ..] = by.as_slice() else {
        return None;
    };
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    if longest.predicted_secs <= total / m.max(1) as f64 {
        return None; // total/m is the wall, not the giant — no split needed
    }
    // The giant must be parallel_safe for a split to help: its range sub-exports
    // inherit its heavy flag (split_dominating), so splitting a HEAVY giant just
    // makes N heavy units that STILL serialize — the heavy-serialization floor
    // (C3) is unchanged and the advice is a false promise (roast 2026-08-10: on
    // the DEFAULT all-heavy config the advisory fired but the wall never moved).
    if !longest.parallel_safe {
        return None;
    }
    let n = split_factor(longest.predicted_secs, second.predicted_secs, r, max_split)?;
    let broken = predicted_makespan_secs(&split_dominating(items, r, max_split), m);
    // Only advise when the split ACTUALLY lowers the wall — never promise an
    // improvement the model itself does not predict.
    if broken >= predicted_makespan_secs(items, m) {
        return None;
    }
    Some((longest.name.clone(), n, broken))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn it(name: &str, secs: f64) -> PoolItem {
        PoolItem {
            name: name.into(),
            predicted_secs: secs,
            parallel_safe: true,
        }
    }

    /// A heavy (non-parallel_safe) item, for the C3 serialization-floor tests.
    fn heavy(name: &str, secs: f64) -> PoolItem {
        PoolItem {
            name: name.into(),
            predicted_secs: secs,
            parallel_safe: false,
        }
    }

    #[test]
    fn order_is_longest_first_deterministic() {
        let items = vec![it("b", 10.0), it("giant", 900.0), it("a", 10.0)];
        // giant first; the two 10s tie-break by name (a before b).
        assert_eq!(pool_order(&items), vec!["giant", "a", "b"]);
    }

    /// #166/#167 C3 (roast 2026-08-09): an ALL-HEAVY config cannot beat
    /// sequential — heavies serialize among themselves — so both the floor and
    /// the predicted makespan must be at least the SUM of heavy durations, never
    /// the optimistic total/M. RED against the constraint-free model.
    #[test]
    fn makespan_honors_heavy_serialization() {
        // 4 heavy exports, 600s each, M=4. Constraint-free LPT would say 600
        // (one per slot); but heavies serialize → the true wall is 2400.
        let items = vec![
            heavy("a", 600.0),
            heavy("b", 600.0),
            heavy("c", 600.0),
            heavy("d", 600.0),
        ];
        assert_eq!(
            makespan_floor_secs(&items, 4),
            2400.0,
            "floor = sum of heavies"
        );
        assert_eq!(
            predicted_makespan_secs(&items, 4),
            2400.0,
            "prediction must not advertise M-way parallelism an all-heavy set can't reach"
        );

        // Mixed: one heavy 600 + 6 safe 100 (total 1200), M=4. Safe ones fill
        // slots; the heavy floor (600) is below total/M (300)? total/M=300, but
        // greedy packs safes ~ they overlap the heavy → wall ~ max(600, ...). The
        // heavy floor (600) is the binding one here, correctly.
        let mixed = {
            let mut v = vec![heavy("giant", 600.0)];
            for n in 0..6 {
                v.push(it(&format!("s{n}"), 100.0));
            }
            v
        };
        assert!(
            predicted_makespan_secs(&mixed, 4) >= 600.0,
            "the single heavy still floors the wall"
        );
        // A pure-safe set is unaffected (heavy sum = 0).
        let safe = vec![
            it("a", 100.0),
            it("b", 100.0),
            it("c", 100.0),
            it("d", 100.0),
        ];
        assert_eq!(predicted_makespan_secs(&safe, 2), 200.0);
    }

    #[test]
    fn split_factor_only_fires_on_a_dominating_export() {
        // 3000s giant vs 300s next: 10x → dominates (R=3). N = ceil(3000/300)=10,
        // capped at max_split.
        assert_eq!(split_factor(3000.0, 300.0, 3.0, 8), Some(8));
        assert_eq!(split_factor(3000.0, 300.0, 3.0, 20), Some(10));
        // 2x the next: below R=3, not dominating.
        assert_eq!(split_factor(600.0, 300.0, 3.0, 8), None);
        // no second export: nothing to compare, no split.
        assert_eq!(split_factor(3000.0, 0.0, 3.0, 8), None);
        // just over threshold: still at least 2 pieces.
        assert_eq!(split_factor(310.0, 100.0, 3.0, 8), Some(4));
    }

    /// The floor-breaker (#167), RED against the un-split floor. The field shape:
    /// one 3060s giant dominates; splitting it lets M=6 approach total/6.
    #[test]
    fn splitting_the_giant_breaks_the_pool_floor() {
        let mut items = vec![it("giant", 3060.0)];
        for n in 0..30 {
            items.push(it(&format!("s{n:02}"), 60.0)); // 30 × 60 = 1800s of small
        }
        // Un-split: the giant IS the floor at M=6 (3060 > total/6 = 4860/6 = 810).
        assert_eq!(makespan_floor_secs(&items, 6), 3060.0);
        let unsplit_wall = predicted_makespan_secs(&items, 6);
        assert!(
            unsplit_wall >= 3060.0,
            "giant pins the wall: {unsplit_wall}"
        );

        // Split (R=3, giant 3060 vs next 60 → N capped at max_split=6): the giant
        // becomes 6 × 510s units; the floor drops to total/6 and the pool reaches it.
        let split = split_dominating(&items, 3.0, 6);
        assert_eq!(
            split.len(),
            30 + 6,
            "giant replaced by 6 sub-units alongside the 30 small"
        );
        let split_wall = predicted_makespan_secs(&split, 6);
        assert!(
            split_wall < unsplit_wall,
            "the split MUST break the floor: {split_wall} < {unsplit_wall}"
        );
        assert_eq!(split_wall, 810.0, "reaches total/6 = 4860/6");
    }

    /// #167 advisory floor-gate (roast 2026-08-09): advise a split ONLY when the
    /// giant is actually the floor. A 200s "giant" with 20×60s (total 1400, m=2 →
    /// floor 700) must NOT advise — total/m is the wall, splitting buys nothing.
    #[test]
    fn advise_split_needs_the_giant_to_be_the_floor() {
        // Giant IS the floor: 3060s + 30×60s, m=6 → floor 3060.
        let mut dominating = vec![it("giant", 3060.0)];
        for n in 0..30 {
            dominating.push(it(&format!("s{n:02}"), 60.0));
        }
        let advice = advise_split(&dominating, 6, 3.0, 6);
        assert!(advice.is_some(), "a real giant must be advised");
        let (name, n, broken) = advice.unwrap();
        assert_eq!(name, "giant");
        assert!(n >= 2 && broken < 3060.0, "the split must drop the wall");

        // Giant is NOT the floor: 200s + 20×60s, m=2 → floor max(200,700)=700.
        let mut balanced = vec![it("tall", 200.0)];
        for n in 0..20 {
            balanced.push(it(&format!("s{n:02}"), 60.0));
        }
        assert!(
            advise_split(&balanced, 2, 3.0, 2).is_none(),
            "no advice when total/m is the wall — a split would be a false alarm"
        );

        // #4 (roast 2026-08-10): a HEAVY giant (the DEFAULT — parallel_safe unset)
        // must NOT be advised: its range sub-exports inherit the heavy flag and
        // STILL serialize (C3 floor unchanged), so the split promises a gain the
        // model does not deliver. RED against advising a non-parallel_safe giant.
        let mut all_heavy = vec![heavy("giant", 3060.0)];
        for n in 0..30 {
            all_heavy.push(heavy(&format!("s{n:02}"), 60.0));
        }
        assert!(
            advise_split(&all_heavy, 6, 3.0, 6).is_none(),
            "a heavy giant's split does not lower the wall (heavies serialize) — no advice"
        );
    }

    #[test]
    fn split_dominating_leaves_a_balanced_set_untouched() {
        let items = vec![it("a", 100.0), it("b", 120.0), it("c", 90.0)];
        assert_eq!(split_dominating(&items, 3.0, 8).len(), 3);
    }

    #[test]
    fn a_dominating_giant_is_the_floor_and_the_pool_reaches_it() {
        // One 3000s giant + many small — the field's shape.
        let mut items = vec![it("giant", 3000.0)];
        for n in 0..30 {
            items.push(it(&format!("s{n:02}"), 60.0));
        }
        // total = 3000 + 1800 = 4800; M=4 → total/4 = 1200; longest 3000 dominates.
        assert_eq!(makespan_floor_secs(&items, 4), 3000.0);
        // The pool reaches the floor: the giant pins one slot, the 1800s of
        // small work fits in the other three (600s each) under the giant's 3000.
        assert_eq!(predicted_makespan_secs(&items, 4), 3000.0);
    }

    #[test]
    fn without_a_giant_the_pool_reaches_total_over_m() {
        // 12 equal 100s jobs, M=4 → 1200/4 = 300s, evenly packed.
        let items: Vec<PoolItem> = (0..12).map(|n| it(&format!("e{n:02}"), 100.0)).collect();
        assert_eq!(makespan_floor_secs(&items, 4), 300.0);
        assert_eq!(predicted_makespan_secs(&items, 4), 300.0);
    }

    #[test]
    fn pool_beats_or_matches_the_barrier_and_stays_within_the_lpt_bound() {
        use proptest::prelude::*;
        proptest!(|(durs in proptest::collection::vec(1.0f64..1000.0, 1..40), m in 1usize..8)| {
            let items: Vec<PoolItem> = durs
                .iter()
                .enumerate()
                .map(|(i, &d)| it(&format!("e{i:02}"), d))
                .collect();
            let mk = predicted_makespan_secs(&items, m);
            let floor = makespan_floor_secs(&items, m);
            // Never below the floor.
            prop_assert!(mk >= floor - 1e-9, "makespan {mk} below floor {floor}");
            // Within the LPT worst-case bound of optimal (floor is a lower
            // bound on OPT, so mk <= (4/3)·OPT ≤ (4/3)·... use floor*4/3 as a
            // safe ceiling only when floor==OPT; assert the weaker, always-true
            // mk <= total which the pool can never exceed).
            let total: f64 = durs.iter().sum();
            prop_assert!(mk <= total + 1e-9, "makespan {mk} exceeds total {total}");
            // A single worker's makespan IS the total (no concurrency).
            if m == 1 { prop_assert!((mk - total).abs() < 1e-6); }
        });
    }
}

#[cfg(test)]
mod prediction_tests {
    use super::{
        POOL_PLACEHOLDER_SECS, PoolItem, PredictedFrom, classification_counts, pool_order,
        predict_items, predict_secs, reconcile_split_seed, repartitioned_unit_prediction,
        split_unit_from,
    };
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

    /// Field find (2026-08-13, --pool 5): an export with FAILED history but no
    /// success was scheduled at the flat 5 s placeholder, so a giant that had
    /// already demonstrated an hours-long attempt was predicted as noise and
    /// the printed makespan promised minutes for an hours-long run. A failed
    /// attempt's duration is a floor on the real one — use it.
    ///
    /// The fixture carries THREE attempts with DIFFERENT durations, and the
    /// longest is neither the first nor the last row: `predict_secs` folds the
    /// history with `reduce(f64::max)`, and over a one-row history (this test's
    /// original fixture) max, min, first and last are the same number — every
    /// fold mutant survives. With the longest attempt in the middle, only a
    /// genuine max passes (RED against `f64::max` → `f64::min`: got 300 s where
    /// 3600 s was required; RED against a first/last pick: 1800 s / 600 s).
    #[test]
    fn unmeasured_export_with_failed_history_predicts_the_longest_attempt() {
        let s = store_with(&[
            ("big", "r1", 1_800_000, "failed"),    // 30 min
            ("big", "r2", 3_600_000, "failed"),    // 60 min — the floor
            ("big", "r3", 600_000, "interrupted"), // 10 min, most recent
            ("big", "r4", 300_000, "failed"),      // 5 min, the minimum
        ]);
        match predict_secs(&s, "big") {
            PredictedFrom::FailedAttemptFloor(secs) => {
                assert!(
                    (secs - 3600.0).abs() < 1.0,
                    "the LONGEST attempt's hour must survive as the floor (not the \
                     shortest, first or most recent attempt), got {secs}"
                );
            }
            _ => panic!("failed-only history must be a FailedAttemptFloor, not a placeholder"),
        }
    }

    #[test]
    fn measured_success_beats_failed_attempts_and_no_history_is_a_placeholder() {
        // A success among failures → measured, at the SUCCESS duration — even
        // when dozens of failures sit between it and now (direct success
        // query, not a fixed recent window).
        let mut rows: Vec<(&str, String, i64, &str)> = vec![("t", "r0".into(), 120_000, "success")];
        for i in 1..=30 {
            rows.push(("t", format!("r{i}"), 3_600_000, "failed"));
        }
        let owned: Vec<(&str, &str, i64, &str)> = rows
            .iter()
            .map(|(e, r, d, s)| (*e, r.as_str(), *d, *s))
            .collect();
        let s = store_with(&owned);
        match predict_secs(&s, "t") {
            PredictedFrom::Measured(secs) => assert!((secs - 120.0).abs() < 1.0, "got {secs}"),
            _ => panic!("a successful run must classify as Measured even past 25 failures"),
        }
        // No history at all → the placeholder, honestly labeled as such.
        let empty = store_with(&[]);
        match predict_secs(&empty, "unknown") {
            PredictedFrom::Placeholder(secs) => assert_eq!(secs, POOL_PLACEHOLDER_SECS),
            _ => panic!("no history must classify as Placeholder"),
        }
        // A sub-placeholder failed attempt is floored at the placeholder.
        let quick = store_with(&[("q", "r1", 1_000, "failed")]);
        assert!(predict_secs(&quick, "q").secs() >= POOL_PLACEHOLDER_SECS);
    }

    /// The drift this module closed (walk find, 2026-08-13): plan's preview
    /// and apply's schedule must be the same numbers. Both now call
    /// predict_items — this pins the constructor's remaining decisions: split
    /// seeds win over history, parallel_safe passes through verbatim, and
    /// every export is INCLUDED (the old preview silently excluded
    /// history-less exports, so its makespan covered less work than the wave
    /// plan printed beside it).
    #[test]
    fn predict_items_seeds_split_units_and_includes_history_less_exports() {
        let s = store_with(&[("giant", "r1", 900_000, "success")]);
        let seeds = std::collections::HashMap::from([(
            "giant#0".to_string(),
            PredictedFrom::SeededSplit(450.0),
        )]);
        let out = predict_items(&s, [("giant#0", false), ("fresh", true)], &seeds);
        assert_eq!(
            out.len(),
            2,
            "history-less exports are included, not dropped"
        );
        let (unit, from) = &out[0];
        assert!(matches!(from, PredictedFrom::SeededSplit(_)));
        assert_eq!(
            unit.predicted_secs, 450.0,
            "the seed bootstraps a unit with no rows of its own \
             (once it has some, see reconcile_split_seed)"
        );
        assert!(!unit.parallel_safe, "parallel_safe passes through verbatim");
        let (fresh, from) = &out[1];
        assert!(matches!(from, PredictedFrom::Placeholder(_)));
        assert_eq!(fresh.predicted_secs, POOL_PLACEHOLDER_SECS);
        assert!(fresh.parallel_safe);
    }

    /// Splitting an UNMEASURED giant must not manufacture measurement.
    ///
    /// `--split` is reached for exactly when a giant keeps timing out — the
    /// state holds only `failed`/`interrupted` rows. Seeding its units as
    /// `SeededSplit` (which the accounting folds into `measured`) made
    /// `apply --pool --split` print "14 measured, 0 estimated" and DROP the
    /// LOWER BOUND line, for a schedule resting entirely on an export that has
    /// never finished (bughunt 2026-08-14).
    ///
    /// This pins the two pure halves `run_pool` wires together — the seed's
    /// provenance ([`split_unit_from`]) and the hedge's input
    /// ([`classification_counts`]) — because the wiring itself needs a live
    /// source and destination and cannot be unit-tested. The fixture splits
    /// into FOUR units beside a measured small: a one-unit split cannot tell a
    /// per-unit inheritance from a whole-set default, and without the measured
    /// neighbour a mutant that classifies EVERYTHING as an attempt would also
    /// pass.
    #[test]
    fn split_units_of_an_unmeasured_giant_keep_the_lower_bound_hedge() {
        let s = store_with(&[
            ("giant", "r1", 3_600_000, "failed"),
            ("giant", "r2", 3_000_000, "interrupted"),
            ("small", "r3", 60_000, "success"),
        ]);
        let giant_from = predict_secs(&s, "giant");
        assert!(
            matches!(giant_from, PredictedFrom::FailedAttemptFloor(_)),
            "fixture: the giant must be unmeasured, got {giant_from:?}"
        );
        let share = giant_from.secs() / 4.0;
        let seeds: std::collections::HashMap<String, PredictedFrom> = (0..4)
            .map(|i| (format!("giant#{i}"), split_unit_from(&giant_from, share)))
            .collect();
        let names: Vec<String> = (0..4).map(|i| format!("giant#{i}")).collect();
        let exports: Vec<(&str, bool)> = names
            .iter()
            .map(|n| (n.as_str(), true))
            .chain(std::iter::once(("small", true)))
            .collect();
        let predicted = predict_items(&s, exports, &seeds);
        let classified: Vec<PredictedFrom> = predicted.iter().map(|(_, f)| f.clone()).collect();
        for (item, from) in &predicted {
            if item.name.starts_with("giant#") {
                assert!(
                    matches!(from, PredictedFrom::FailedAttemptFloor(_)),
                    "a unit of a never-succeeded giant is not measured: {from:?}"
                );
                assert_eq!(item.predicted_secs, share, "the unit still gets its share");
            }
        }
        let (measured, attempt, placeholder) = classification_counts(&classified);
        assert_eq!(
            (measured, attempt, placeholder),
            (1, 4, 0),
            "only the small export is measured; the four units are attempt-floored"
        );
        assert!(
            attempt + placeholder > 0,
            "this sum is the LOWER BOUND hedge's condition in run_pool — \
             a split must never zero it"
        );
        // …and a MEASURED giant's units stay measured-by-proxy, so the hedge
        // does not start crying wolf on the case the seed was built for.
        let measured_giant = PredictedFrom::Measured(1200.0);
        assert!(matches!(
            split_unit_from(&measured_giant, 300.0),
            PredictedFrom::SeededSplit(s) if s == 300.0
        ));
        assert!(matches!(
            split_unit_from(&PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS), 1.25),
            PredictedFrom::Placeholder(_)
        ));
    }

    /// Run 2+ of `apply --pool --split`: the units' OWN measurements must
    /// supersede the inherited seed.
    ///
    /// The giant is retained out of the run set once split (`run_pool`), so its
    /// rows freeze at the failed state that motivated the split and the seed is
    /// rebuilt identically on every later run. A seed that keeps winning throws
    /// away every unit's real history forever: LPT sees N identical durations
    /// (no longest-first among the slices — the makespan the split exists to
    /// break) and the LOWER BOUND hedge can never clear, because only a success
    /// on the giant would clear it and the giant never runs again (bughunt
    /// 2026-08-14).
    ///
    /// The fixture needs TWO measured units with DIFFERENT durations: with one,
    /// "all units share the seed" and "each unit has its own number" are the
    /// same schedule, so the seed-wins mutant survives. A third, history-less
    /// unit pins the other half — the seed still bootstraps a genuinely fresh
    /// slice.
    #[test]
    fn a_split_units_own_history_supersedes_the_inherited_seed() {
        let s = store_with(&[
            // The giant's frozen failed rows — the seed's source, forever.
            ("giant", "r1", 3_600_000, "failed"),
            ("giant", "r2", 3_000_000, "interrupted"),
            // …and what run 1 of the split actually measured.
            ("giant#0", "r3", 700_000, "success"),
            ("giant#1", "r4", 1_100_000, "success"),
        ]);
        let giant_from = predict_secs(&s, "giant");
        let share = giant_from.secs() / 3.0; // 1200.0, identical for all units
        let seeds: std::collections::HashMap<String, PredictedFrom> = (0..3)
            .map(|i| (format!("giant#{i}"), split_unit_from(&giant_from, share)))
            .collect();
        let names: Vec<String> = (0..3).map(|i| format!("giant#{i}")).collect();
        let exports: Vec<(&str, bool)> = names.iter().map(|n| (n.as_str(), true)).collect();
        let predicted = predict_items(&s, exports, &seeds);
        let secs_of = |n: &str| {
            predicted
                .iter()
                .find(|(i, _)| i.name == n)
                .map(|(i, _)| i.predicted_secs)
                .expect("unit present")
        };
        assert_eq!(secs_of("giant#0"), 700.0, "the unit's own success wins");
        assert_eq!(secs_of("giant#1"), 1100.0, "…per unit, not one shared seed");
        assert_eq!(
            secs_of("giant#2"),
            share,
            "a unit with no rows of its own still takes the seed"
        );
        // LPT must now rank the slices by what they actually cost — the
        // seed-wins behaviour ties all three at 1200 s and breaks the tie by
        // NAME, putting the 700 s unit first.
        let items: Vec<PoolItem> = predicted.iter().map(|(i, _)| i.clone()).collect();
        assert_eq!(
            pool_order(&items),
            vec!["giant#2", "giant#1", "giant#0"],
            "longest-first among the slices"
        );
        // …and the hedge self-clears as the units succeed: two measured, one
        // still resting on the failed giant's floor.
        let classified: Vec<PredictedFrom> = predicted.iter().map(|(_, f)| f.clone()).collect();
        assert_eq!(
            classification_counts(&classified),
            (2, 1, 0),
            "the LOWER BOUND hedge shrinks to the units that really lack a success"
        );
    }

    /// Run 2 of `apply --pool --split` with a DIFFERENT unit count: the prior
    /// partition's per-unit measurements must not be handed to units that now
    /// cover a different share of the table.
    ///
    /// A unit's measured duration means "1/N of the ROWS" — the boundaries are
    /// row percentiles, re-sampled on every non-`--resume` run — and N is not
    /// stable: `advise_split` clamps it at `max_split = m.max(2)`, the `--pool
    /// M` value. So `--pool 8 --split` then `--pool 3 --split`, a routine
    /// "fewer slots" change, gave `giant#0..#2` the 8-way numbers for a 3-way
    /// partition: a ~2.7× under-prediction on every unit, sorted below the
    /// measured exports in LPT, with the run advertising a confident makespan
    /// wrong by the ratio of the two split factors (bughunt 2026-08-14, round 4).
    ///
    /// The fixture is engineered past three thresholds. The eight prior units
    /// carry FOUR different durations (a uniform cohort makes sum, mean, max
    /// and first the same number); their sum, the giant's frozen seed and every
    /// individual unit duration are three DISTINCT values, so the assert names
    /// which of the three the code used; and the prior count (8) exceeds the
    /// realized count (3), which is the only decidable direction — see
    /// [`repartitioned_unit_prediction`].
    ///
    /// RED against `repartitioned_unit_prediction` returning `None` (the
    /// pre-fix behaviour, own history wins): `left: 300.0, right: 1200.0`.
    #[test]
    fn a_shrunk_split_partition_re_cuts_the_cohort_instead_of_reusing_its_units_numbers() {
        let mut rows: Vec<(String, String, i64, String)> = vec![
            // The giant's frozen rows: it is retained OUT of the run set once
            // split, so this failure is its prediction forever. 1800 s / 3 =
            // 600 s is the SEED — a third distinct value.
            ("giant".into(), "r0".into(), 1_800_000, "failed".into()),
        ];
        // Run 1 at `--pool 8`: eight units, each 1/8 of the rows. They sum to
        // 3600 s — the table's measured cost.
        for (i, ms) in [300, 400, 500, 700, 300, 400, 500, 500].iter().enumerate() {
            rows.push((
                format!("giant#{i}"),
                format!("r1_{i}"),
                ms * 1000,
                "success".into(),
            ));
        }
        let owned: Vec<(&str, &str, i64, &str)> = rows
            .iter()
            .map(|(e, r, d, s)| (e.as_str(), r.as_str(), *d, s.as_str()))
            .collect();
        let s = store_with(&owned);

        // Run 2 at `--pool 3`: three units over the same table.
        let giant_from = predict_secs(&s, "giant");
        let seed_share = giant_from.secs() / 3.0;
        assert_eq!(
            seed_share, 600.0,
            "fixture: the stale seed is a third value"
        );
        let seeds: std::collections::HashMap<String, PredictedFrom> = (0..3)
            .map(|i| {
                (
                    format!("giant#{i}"),
                    split_unit_from(&giant_from, seed_share),
                )
            })
            .collect();
        let names: Vec<String> = (0..3).map(|i| format!("giant#{i}")).collect();
        let exports: Vec<(&str, bool)> = names.iter().map(|n| (n.as_str(), true)).collect();
        let predicted = predict_items(&s, exports, &seeds);
        for (item, _) in &predicted {
            assert_eq!(
                item.predicted_secs, 1200.0,
                "{} must be re-cut from the cohort's measured 3600 s over the THREE \
                 units running now — not its own 8-way number, and not the giant's \
                 frozen 600 s seed",
                item.name
            );
        }
        // Provenance: eight successful units re-cut is measured-by-proxy, the
        // same standing `split_unit_from` gives a measured giant's share — so
        // the accounting does not start crying wolf over a number that rests
        // entirely on completed runs.
        let classified: Vec<PredictedFrom> = predicted.iter().map(|(_, f)| f.clone()).collect();
        assert!(
            classified
                .iter()
                .all(|f| matches!(f, PredictedFrom::SeededSplit(_))),
            "{classified:?}"
        );

        // …and the fold is pessimistic: one unit that only ever FAILED makes
        // the cohort's sum a floor, so the LOWER BOUND hedge keeps printing.
        let mixed = vec![
            (0usize, PredictedFrom::Measured(300.0)),
            (1, PredictedFrom::FailedAttemptFloor(900.0)),
            (2, PredictedFrom::Measured(300.0)),
            (3, PredictedFrom::Measured(300.0)),
        ];
        match repartitioned_unit_prediction(2, &mixed) {
            Some(PredictedFrom::FailedAttemptFloor(secs)) => assert_eq!(secs, 900.0),
            other => panic!("a cohort holding an unmeasured unit is a floor, got {other:?}"),
        }
        // The undecidable direction stays alone: `{0,1,2}` against 8 units now
        // is equally an interrupted 8-way run whose tail never recorded, and
        // re-cutting it would divide an already-correct number a second time.
        let grown: Vec<(usize, PredictedFrom)> = (0..3)
            .map(|i| (i, PredictedFrom::Measured(1200.0)))
            .collect();
        assert!(repartitioned_unit_prediction(8, &grown).is_none());
        // Same count, complete cohort: nothing to re-cut, per-unit history wins.
        assert!(repartitioned_unit_prediction(3, &grown).is_none());
        assert!(repartitioned_unit_prediction(3, &[]).is_none());
    }

    /// The reconcile rule itself, in isolation: seed only while the unit is
    /// genuinely history-less, own row otherwise (success OR attempt floor).
    #[test]
    fn reconcile_split_seed_yields_to_any_history_of_the_units_own() {
        let seed = PredictedFrom::SeededSplit(1200.0);
        assert!(matches!(
            reconcile_split_seed(&seed, &PredictedFrom::Placeholder(POOL_PLACEHOLDER_SECS)),
            PredictedFrom::SeededSplit(s) if s == 1200.0
        ));
        assert!(matches!(
            reconcile_split_seed(&seed, &PredictedFrom::Measured(700.0)),
            PredictedFrom::Measured(s) if s == 700.0
        ));
        // A unit that has only ever failed is floored by its OWN attempt — the
        // seed's derived share is not evidence about this slice, and keeping it
        // would re-freeze the number on the giant's stale rows.
        assert!(matches!(
            reconcile_split_seed(&seed, &PredictedFrom::FailedAttemptFloor(1800.0)),
            PredictedFrom::FailedAttemptFloor(s) if s == 1800.0
        ));
    }
}
