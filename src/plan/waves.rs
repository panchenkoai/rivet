//! Wave packing from MEASURED run history (#150, the feature half).
//!
//! The scheduler's wave is a BARRIER: wave N+1 starts when the LAST export of
//! wave N finishes, so a wave's wall-clock is the MAX of its members, not the
//! sum. Two consequences drive everything here:
//!
//! 1. **Equals pack with equals.** For a fixed group size K, cutting the
//!    duration-DESC-sorted list into CONSECUTIVE groups minimizes the sum of
//!    per-wave maxima — neighbours in sorted order waste the least at the
//!    barrier. (Property-tested below, not argued.)
//! 2. **K comes from the per-export write ceiling, not from optimism.** A
//!    field run measured every export plateauing at ~8–12 MB/s of written
//!    parquet regardless of reader parallelism — the write leg (compression +
//!    upload) is the per-export ceiling, so wall-time scales with how many
//!    WHOLE exports run at once. Cheap exports share wide waves; giants pair.
//!
//! Durations come from the state store's own `export_metrics` — the last
//! successful run is the best predictor of the next one, and the catalog's row
//! estimate is the WORST (measured 2.9× under on a versioned table). Where no
//! measurement exists the caller passes its estimate and says so: every number
//! the packer consumes carries its provenance, and the plan output prints it
//! (the report names what it stands on — #149's rule).
//!
//! The packer never crosses an operator TIER: tiers say what must land before
//! what (meaning, the operator's), waves inside a tier say only how fast
//! (speed, the machine's). Wave order within a tier is heavy-first — pinned,
//! not incidental: a giant that is going to fail (retention, grants, disk)
//! fails on the first wave, not at minute forty.

use crate::plan::CostClass;

/// One export, as the packer sees it.
#[derive(Debug, Clone)]
pub(crate) struct PackItem {
    pub name: String,
    /// The operator's tier (the export's existing `wave:`), or the cost
    /// model's recommendation when the operator set none.
    pub tier: u32,
    pub cost_class: CostClass,
    /// Predicted duration, seconds.
    pub predicted_secs: f64,
    /// Last observed peak RSS, MB (a conservative default when unknown).
    pub peak_rss_mb: i64,
}

/// A packed wave: sequential number and its members (names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackedWave {
    pub number: u32,
    pub members: Vec<String>,
}

/// Concurrent-export cap per wave, by the heaviest member's cost class.
///
/// From the measured ~10 MB/s per-export write ceiling: K exports ≈ K× that.
/// Low-cost exports finish before contention matters; giants pair so the box
/// carries ~20 MB/s without betting the replica on more.
fn k_for(class: CostClass) -> usize {
    match class {
        CostClass::Low => 10,
        CostClass::Medium => 3,
        CostClass::High | CostClass::VeryHigh => 2,
    }
}

/// Pack items into waves: within each tier, duration-DESC, consecutive groups
/// of K (K from the group's FIRST — heaviest — member), split early when the
/// group's summed last-known RSS would exceed `rss_budget_mb`.
///
/// Deterministic by construction: ties in duration break by name, so the same
/// input always yields the same waves — a schedule that changes between two
/// identical `plan` runs would be its own kind of clobber.
pub(crate) fn pack(items: &[PackItem], rss_budget_mb: i64) -> Vec<PackedWave> {
    let mut tiers: std::collections::BTreeMap<u32, Vec<&PackItem>> =
        std::collections::BTreeMap::new();
    for it in items {
        tiers.entry(it.tier).or_default().push(it);
    }

    let mut waves = Vec::new();
    let mut number = 1u32;
    for (_tier, mut members) in tiers {
        members.sort_by(|a, b| {
            b.predicted_secs
                .total_cmp(&a.predicted_secs)
                .then_with(|| a.name.cmp(&b.name))
        });
        let mut i = 0;
        while i < members.len() {
            let mut group: Vec<&PackItem> = Vec::new();
            let mut rss = 0i64;
            while i < members.len() {
                let m = members[i];
                // K is the MOST RESTRICTIVE cost class in the group INCLUDING
                // this candidate — not `members[i]`'s at the group's head. The
                // members are sorted by DURATION, so the head can be a Low-cost
                // export that merely ran long; taking its K=10 would widen a
                // wave that also holds High-cost giants to ten concurrent heavy
                // writers (bug hunt 2026-08-08). Recomputing the min as each
                // member joins caps the wave at its heaviest member's K.
                let k = group
                    .iter()
                    .map(|g| k_for(g.cost_class))
                    .chain(std::iter::once(k_for(m.cost_class)))
                    .min()
                    .unwrap_or(1);
                // Stop before over-filling past the running K, or past the RSS
                // budget — but never leave a group empty (nothing would run).
                if !group.is_empty() && (group.len() >= k || rss + m.peak_rss_mb > rss_budget_mb) {
                    break;
                }
                rss += m.peak_rss_mb;
                group.push(m);
                i += 1;
            }
            waves.push(PackedWave {
                number,
                members: group.iter().map(|m| m.name.clone()).collect(),
            });
            number += 1;
        }
    }
    waves
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(name: &str, tier: u32, class: CostClass, secs: f64) -> PackItem {
        PackItem {
            name: name.into(),
            tier,
            cost_class: class,
            predicted_secs: secs,
            peak_rss_mb: 100,
        }
    }

    /// Wall of a schedule = sum over waves of the max duration inside each —
    /// the barrier model, verbatim.
    fn wall(waves: &[PackedWave], secs: &std::collections::HashMap<String, f64>) -> f64 {
        waves
            .iter()
            .map(|w| w.members.iter().map(|m| secs[m]).fold(0.0f64, f64::max))
            .sum()
    }

    #[test]
    fn equals_pack_with_equals_and_heavy_goes_first() {
        // Two giants + two quick ones, one tier, K=2 (High): the packing that
        // pairs 900s with 10s wastes ~890s at the barrier; ours must not.
        let items = vec![
            item("g1", 4, CostClass::High, 900.0),
            item("q1", 4, CostClass::High, 10.0),
            item("g2", 4, CostClass::High, 880.0),
            item("q2", 4, CostClass::High, 12.0),
        ];
        let waves = pack(&items, 4096);
        assert_eq!(waves.len(), 2);
        // heavy-first is pinned: the giants are wave 1, not wherever sorting
        // happened to leave them.
        assert_eq!(waves[0].members, vec!["g1", "g2"]);
        assert_eq!(waves[1].members, vec!["q2", "q1"]);
        let secs = items
            .iter()
            .map(|i| (i.name.clone(), i.predicted_secs))
            .collect();
        assert_eq!(wall(&waves, &secs), 900.0 + 12.0);
    }

    /// The RSS split is STRICT: `rss + next.peak_rss_mb > budget` — a member
    /// that fills the budget EXACTLY still joins the wave. Two 100 MB Low-cost
    /// exports (K=10, so only RSS can split them) with a 200 MB budget must pack
    /// into ONE wave. The `>`→`>=` mutant (waves.rs:109) breaks at the exact-fit
    /// boundary and makes two; a budget one below the fit proves the boundary is
    /// where the assertion says it is.
    #[test]
    fn rss_split_is_strict_an_exact_fit_still_joins_the_wave() {
        let items = vec![
            item("a", 1, CostClass::Low, 10.0), // peak_rss_mb = 100 (helper default)
            item("b", 1, CostClass::Low, 9.0),
        ];
        // budget == a.rss + b.rss exactly.
        let waves = pack(&items, 200);
        assert_eq!(
            waves.len(),
            1,
            "exact fit must not split — `>=` would wrongly make two waves"
        );
        assert_eq!(waves[0].members, vec!["a", "b"]);
        // One MB below the exact fit DOES split: the boundary is at the sum.
        assert_eq!(
            pack(&items, 199).len(),
            2,
            "199 < 200 must split into two waves"
        );
    }

    #[test]
    fn tiers_are_never_crossed_and_numbering_is_sequential() {
        let items = vec![
            item("t2_a", 2, CostClass::Low, 1.0),
            item("t4_a", 4, CostClass::High, 100.0),
            item("t2_b", 2, CostClass::Low, 2.0),
        ];
        let waves = pack(&items, 4096);
        // tier 2 packs first (ascending), tier 4 after — priority is the
        // operator's ordering and survives packing.
        assert_eq!(waves[0].members, vec!["t2_b", "t2_a"]);
        assert_eq!(waves[1].members, vec!["t4_a"]);
        assert_eq!(
            waves.iter().map(|w| w.number).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn rss_guard_splits_a_group_and_loses_nobody() {
        // Three Medium exports (K=3) whose last-known RSS sums past the budget
        // — the group must SPLIT (≥2 waves), and the union of members must be
        // intact. ≥2 members over the threshold, per the activation-threshold
        // fixture rule: one member can never split anything.
        let mut items = vec![
            item("a", 3, CostClass::Medium, 30.0),
            item("b", 3, CostClass::Medium, 29.0),
            item("c", 3, CostClass::Medium, 28.0),
        ];
        for it in &mut items {
            it.peak_rss_mb = 300;
        }
        let waves = pack(&items, 500); // 300+300 > 500 → split after the first
        assert!(waves.len() >= 2, "over-budget group must split: {waves:?}");
        let mut all: Vec<_> = waves.iter().flat_map(|w| w.members.clone()).collect();
        all.sort();
        assert_eq!(all, vec!["a", "b", "c"], "the guard splits, never drops");
        assert!(
            waves.iter().all(|w| !w.members.is_empty()),
            "no empty waves"
        );
    }

    /// K is the heaviest member's, not the duration-front-runner's. A Low-cost
    /// export that merely ran long must not widen a wave that holds High giants
    /// to K=10 (bug hunt 2026-08-08).
    #[test]
    fn k_is_the_heaviest_class_in_the_group_not_the_front_runner() {
        // One long-running Low export, then several High giants — one tier.
        // Sorted by duration the Low one leads. It must NOT pull K to 10.
        let mut items = vec![item("low_but_long", 4, CostClass::Low, 500.0)];
        for n in 0..5 {
            items.push(item(
                &format!("giant{n}"),
                4,
                CostClass::High,
                400.0 - n as f64,
            ));
        }
        let waves = pack(&items, i64::MAX);
        // The wave containing the giants must never exceed K=2 (High), even
        // though a Low member shares the tier.
        for w in &waves {
            let has_giant = w.members.iter().any(|m| m.starts_with("giant"));
            if has_giant {
                assert!(
                    w.members.len() <= 2,
                    "a wave with a High giant must cap at K=2, got {:?}",
                    w.members
                );
            }
        }
    }

    #[test]
    fn determinism_ties_break_by_name() {
        let items = vec![
            item("b", 2, CostClass::Low, 5.0),
            item("a", 2, CostClass::Low, 5.0),
        ];
        let w1 = pack(&items, 4096);
        let rev: Vec<PackItem> = items.iter().rev().cloned().collect();
        let w2 = pack(&rev, 4096);
        assert_eq!(w1, w2, "same schedule whatever the input order");
        assert_eq!(w1[0].members, vec!["a", "b"]);
    }

    /// The optimality claim as a PROPERTY, not an example: for random
    /// durations, consecutive-K on the sorted list never loses to a shuffled
    /// grouping of the same K.
    #[test]
    fn consecutive_k_never_loses_to_a_shuffle() {
        use proptest::prelude::*;
        proptest!(|(durs in proptest::collection::vec(1.0f64..1000.0, 4..24), seed in 0u64..1000)| {
            let items: Vec<PackItem> = durs
                .iter()
                .enumerate()
                .map(|(i, &d)| item(&format!("e{i:02}"), 1, CostClass::Medium, d))
                .collect();
            let secs: std::collections::HashMap<String, f64> =
                items.iter().map(|i| (i.name.clone(), i.predicted_secs)).collect();
            let ours = wall(&pack(&items, i64::MAX), &secs);

            // a deterministic pseudo-shuffle of the same items, same K=3 cuts
            let mut shuffled = items.clone();
            let mut s = seed;
            for i in (1..shuffled.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                shuffled.swap(i, (s as usize) % (i + 1));
            }
            let theirs: f64 = shuffled
                .chunks(3)
                .map(|g| g.iter().map(|m| m.predicted_secs).fold(0.0f64, f64::max))
                .sum();
            prop_assert!(ours <= theirs + 1e-9, "ours={ours} shuffle={theirs}");
        });
    }

    /// Independent oracle from the field (2026-08-07): the python prototype
    /// packed a real 154-export run's HEAVY tier (top 12 giants, measured
    /// minutes) into these exact pairs. Two implementations, one answer — the
    /// Rust packer must reproduce the prototype, not merely satisfy the same
    /// invariants.
    #[test]
    fn reproduces_the_field_prototypes_packing() {
        let mins = [
            ("boss", 51.1),
            ("admitads", 15.9),
            ("taobao", 12.4),
            ("cb_usd", 9.9),
            ("cb_eur", 9.5),
            ("allegro", 7.9),
            ("pay_usd", 7.0),
            ("pay_eur", 6.7),
            ("lety", 5.9),
            ("misb", 5.1),
            ("backup", 3.2),
            ("products", 2.5),
        ];
        let items: Vec<PackItem> = mins
            .iter()
            .map(|(n, m)| item(n, 4, CostClass::VeryHigh, m * 60.0))
            .collect();
        let waves = pack(&items, 4096);
        let got: Vec<Vec<String>> = waves.iter().map(|w| w.members.clone()).collect();
        let want: Vec<Vec<String>> = [
            vec!["boss", "admitads"],
            vec!["taobao", "cb_usd"],
            vec!["cb_eur", "allegro"],
            vec!["pay_usd", "pay_eur"],
            vec!["lety", "misb"],
            vec!["backup", "products"],
        ]
        .iter()
        .map(|v| v.iter().map(|s| s.to_string()).collect())
        .collect();
        assert_eq!(got, want);
        // Wall check by HAND, not by re-running our own fold: the pair maxima
        // are 51.1, 12.4, 9.5, 7.0, 5.9, 3.2 — summed on paper: 89.1 minutes.
        // (The prototype's headline "92 min" covered the whole 18-export tier;
        // this fixture is its top 12.)
        let secs: std::collections::HashMap<String, f64> = items
            .iter()
            .map(|i| (i.name.clone(), i.predicted_secs))
            .collect();
        let wall_min = wall(&waves, &secs) / 60.0;
        assert!(
            (wall_min - 89.1).abs() < 0.05,
            "hand-summed 89.1 min, got {wall_min:.2}"
        );
    }
}
