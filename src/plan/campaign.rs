//! Campaign-level ordering and waves from per-export recommendations (ADR-0006).

use std::collections::{HashMap, HashSet};

use super::{
    CampaignRecommendation, CostClass, ExportRecommendation, PrioritizationInputs,
    RecommendationReason, RecommendationReasonKind, RecommendedWave,
};

/// Build campaign-level output: ordering, waves, and shared-source warnings.
pub fn recommend_campaign(
    pairs: Vec<(PrioritizationInputs, ExportRecommendation)>,
) -> CampaignRecommendation {
    if pairs.is_empty() {
        return CampaignRecommendation {
            ordered_exports: vec![],
            waves: vec![],
            source_group_warnings: vec![],
        };
    }

    let (source_group_warnings, warned_groups) = build_source_group_warnings(&pairs);

    let mut ordered: Vec<ExportRecommendation> = pairs.into_iter().map(|(_, r)| r).collect();

    ordered.sort_by(|a, b| {
        b.priority_score
            .cmp(&a.priority_score)
            .then(a.export_name.cmp(&b.export_name))
    });

    apply_isolation_flags(&mut ordered, &warned_groups);

    let waves = group_by_wave(&ordered);

    CampaignRecommendation {
        ordered_exports: ordered,
        waves,
        source_group_warnings,
    }
}

fn group_by_wave(ordered: &[ExportRecommendation]) -> Vec<RecommendedWave> {
    let mut wave_to_exports: HashMap<u32, Vec<String>> = HashMap::new();
    for r in ordered {
        wave_to_exports
            .entry(r.recommended_wave)
            .or_default()
            .push(r.export_name.clone());
    }

    let mut wave_nums: Vec<u32> = wave_to_exports.keys().copied().collect();
    wave_nums.sort_unstable();

    wave_nums
        .into_iter()
        .map(|wave| RecommendedWave {
            wave,
            exports: wave_to_exports.remove(&wave).unwrap_or_default(),
        })
        .collect()
}

fn build_source_group_warnings(
    pairs: &[(PrioritizationInputs, ExportRecommendation)],
) -> (Vec<String>, HashSet<String>) {
    let mut by_group: HashMap<String, Vec<(CostClass, String)>> = HashMap::new();
    for (inp, rec) in pairs {
        if let Some(g) = inp.source_group.as_ref() {
            by_group
                .entry(g.clone())
                .or_default()
                .push((rec.cost_class, inp.export_name.clone()));
        }
    }

    let mut out = Vec::new();
    let mut warned_groups: HashSet<String> = HashSet::new();
    for (g, members) in by_group {
        if members.len() < 2 {
            continue;
        }
        let heavy = members
            .iter()
            .filter(|(c, _)| matches!(c, CostClass::High | CostClass::VeryHigh))
            .count();
        if heavy >= 2 {
            let names: Vec<_> = members.iter().map(|(_, n)| n.as_str()).collect();
            warned_groups.insert(g.clone());
            out.push(format!(
                "Source group '{g}': {} heavy-cost exports ({}) — avoid running them concurrently; stagger or isolate.",
                heavy,
                names.join(", ")
            ));
        } else if members.len() >= 3 {
            warned_groups.insert(g.clone());
            out.push(format!(
                "Source group '{g}': {} exports share this source — stagger large runs to reduce replica pressure.",
                members.len()
            ));
        }
    }

    out.sort();
    (out, warned_groups)
}

fn apply_isolation_flags(ordered: &mut [ExportRecommendation], warned_groups: &HashSet<String>) {
    if warned_groups.is_empty() {
        return;
    }

    for rec in ordered.iter_mut() {
        if let Some(g) = &rec.source_group
            && warned_groups.contains(g)
            && matches!(rec.cost_class, CostClass::High | CostClass::VeryHigh)
        {
            rec.isolate_on_source = true;
            rec.reasons.push(RecommendationReason {
                kind: RecommendationReasonKind::SharedSourceHeavyConflict,
                message: format!(
                    "Shared source group '{g}' has multiple heavy exports — run this export alone on that source."
                ),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{CursorQuality, PrioritizationStrategyKind, PriorityClass, RiskClass};

    fn inp(
        name: &str,
        group: Option<&str>,
        cost: CostClass,
    ) -> (PrioritizationInputs, ExportRecommendation) {
        let i = PrioritizationInputs {
            export_name: name.into(),
            source_group: group.map(String::from),
            strategy: PrioritizationStrategyKind::Snapshot,
            estimated_rows: Some(20_000_000),
            estimated_size_bytes: None,
            chunk_count: None,
            sparse_range_risk: false,
            cursor_quality: CursorQuality::None,
            reconcile_required: false,
            source_freshness_hint: None,
            history: None,
        };
        let r = ExportRecommendation {
            export_name: name.into(),
            source_group: group.map(String::from),
            priority_score: 40,
            priority_class: PriorityClass::Medium,
            cost_class: cost,
            risk_class: RiskClass::Low,
            recommended_wave: 2,
            isolate_on_source: false,
            reasons: vec![],
        };
        (i, r)
    }

    #[test]
    fn campaign_orders_by_score() {
        let mut a = inp("a", None, CostClass::Low).1;
        a.priority_score = 30;
        let mut b = inp("b", None, CostClass::Low).1;
        b.priority_score = 80;
        let p1 = (
            PrioritizationInputs {
                export_name: "a".into(),
                source_group: None,
                strategy: PrioritizationStrategyKind::Snapshot,
                estimated_rows: None,
                estimated_size_bytes: None,
                chunk_count: None,
                sparse_range_risk: false,
                cursor_quality: CursorQuality::None,
                reconcile_required: false,
                source_freshness_hint: None,
                history: None,
            },
            a,
        );
        let p2 = (
            PrioritizationInputs {
                export_name: "b".into(),
                source_group: None,
                strategy: PrioritizationStrategyKind::Snapshot,
                estimated_rows: None,
                estimated_size_bytes: None,
                chunk_count: None,
                sparse_range_risk: false,
                cursor_quality: CursorQuality::None,
                reconcile_required: false,
                source_freshness_hint: None,
                history: None,
            },
            b,
        );
        let c = recommend_campaign(vec![p1, p2]);
        assert_eq!(c.ordered_exports[0].export_name, "b");
        assert_eq!(c.ordered_exports[1].export_name, "a");
    }

    #[test]
    fn shared_group_emits_warning() {
        let p1 = inp("big_a", Some("rep1"), CostClass::VeryHigh);
        let p2 = inp("big_b", Some("rep1"), CostClass::High);
        let c = recommend_campaign(vec![p1, p2]);
        assert!(!c.source_group_warnings.is_empty());
        assert!(c.ordered_exports.iter().any(|e| e.isolate_on_source));
    }
}
