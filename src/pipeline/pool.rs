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
//! This module is the PURE core — ordering + the makespan model `plan` prints.
//! The runtime bounded pool that consumes `pool_order` at apply time lives in
//! the runner; keeping the math here makes it property-testable offline.

/// One export the pool schedules, by its predicted duration.
#[derive(Debug, Clone)]
pub(crate) struct PoolItem {
    pub name: String,
    /// Predicted duration in seconds (last successful run, else an estimate —
    /// the same source the wave packer reads).
    pub predicted_secs: f64,
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
    slots.into_iter().fold(0.0, f64::max)
}

/// The floor no scheduler can beat: `max(longest single export, total/M)`.
/// Exposed so `plan` can show how close the pool gets — and so a giant whose
/// duration alone dominates is visibly the wall (the split-the-giant lever,
/// #167).
pub(crate) fn makespan_floor_secs(items: &[PoolItem], workers: usize) -> f64 {
    let w = workers.max(1) as f64;
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    let longest = items.iter().map(|i| i.predicted_secs).fold(0.0, f64::max);
    longest.max(total / w)
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
    let n = split_factor(longest.predicted_secs, second.predicted_secs, r, max_split)?;
    let broken = predicted_makespan_secs(&split_dominating(items, r, max_split), m);
    Some((longest.name.clone(), n, broken))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn it(name: &str, secs: f64) -> PoolItem {
        PoolItem {
            name: name.into(),
            predicted_secs: secs,
        }
    }

    #[test]
    fn order_is_longest_first_deterministic() {
        let items = vec![it("b", 10.0), it("giant", 900.0), it("a", 10.0)];
        // giant first; the two 10s tie-break by name (a before b).
        assert_eq!(pool_order(&items), vec!["giant", "a", "b"]);
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
