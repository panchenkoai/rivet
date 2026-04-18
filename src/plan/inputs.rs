//! Build [`PrioritizationInputs`](super::PrioritizationInputs) from resolved plan, computed data,
//! preflight diagnostics, and optional preflight hints.

use crate::config::ExportConfig;

use crate::config::IncrementalCursorMode;

use super::{
    ComputedPlanData, CursorQuality, ExtractionStrategy, HistorySnapshot, PlanDiagnostics,
    PrioritizationInputs, PrioritizationStrategyKind, ResolvedRunPlan, SourceFreshnessHint,
};

/// Extra signals available only when preflight succeeded (index use, cursor bounds).
#[derive(Debug, Clone, Default)]
pub struct PrioritizationHints {
    pub incremental_uses_index: bool,
    pub cursor_range_observed: bool,
}

/// Assemble prioritization inputs for one export.
///
/// `history` carries optional signals from recent runs (`export_metrics`). When
/// `None` or empty, history contribution to scoring is skipped.
pub fn build_prioritization_inputs(
    export: &ExportConfig,
    plan: &ResolvedRunPlan,
    computed: &ComputedPlanData,
    diagnostics: &PlanDiagnostics,
    hints: PrioritizationHints,
    history: Option<HistorySnapshot>,
) -> PrioritizationInputs {
    let sparse_range_risk = diagnostics.warnings.iter().any(|w| {
        let lower = w.to_lowercase();
        lower.contains("sparse") && lower.contains("range")
    });

    let cursor_quality = infer_cursor_quality(plan, &hints);

    let estimated_rows = computed.row_estimate.map(|r| r.max(0) as u64);

    let chunk_count = if computed.chunk_count > 0 {
        Some(computed.chunk_count as u32)
    } else {
        None
    };

    let reconcile_required = plan.reconcile || export.reconcile_required;

    let source_freshness_hint = infer_freshness_hint(plan, computed, diagnostics);

    let history = history.filter(|h| !h.is_empty());

    PrioritizationInputs {
        export_name: export.name.clone(),
        source_group: export.source_group.clone(),
        strategy: PrioritizationStrategyKind::from_extraction(&plan.strategy),
        estimated_rows,
        estimated_size_bytes: None,
        chunk_count,
        sparse_range_risk,
        cursor_quality,
        reconcile_required,
        source_freshness_hint,
        history,
    }
}

fn infer_cursor_quality(plan: &ResolvedRunPlan, hints: &PrioritizationHints) -> CursorQuality {
    match &plan.strategy {
        ExtractionStrategy::Incremental(p) => match p.mode {
            IncrementalCursorMode::Coalesce => {
                if hints.incremental_uses_index && hints.cursor_range_observed {
                    CursorQuality::WeakMultiCandidate
                } else if hints.incremental_uses_index {
                    CursorQuality::WeakTime
                } else if hints.cursor_range_observed {
                    CursorQuality::WeakMultiCandidate
                } else {
                    CursorQuality::None
                }
            }
            IncrementalCursorMode::SingleColumn => {
                if hints.incremental_uses_index && hints.cursor_range_observed {
                    CursorQuality::StrongMonotonic
                } else if hints.incremental_uses_index {
                    CursorQuality::WeakTime
                } else if hints.cursor_range_observed {
                    CursorQuality::WeakMultiCandidate
                } else {
                    CursorQuality::None
                }
            }
        },
        ExtractionStrategy::TimeWindow { .. } => {
            if hints.cursor_range_observed {
                CursorQuality::WeakTime
            } else {
                CursorQuality::None
            }
        }
        _ => CursorQuality::None,
    }
}

/// Heuristic placeholder: timewindow with a short window may be "fresher" than a wide scan.
fn infer_freshness_hint(
    plan: &ResolvedRunPlan,
    _computed: &ComputedPlanData,
    _diagnostics: &PlanDiagnostics,
) -> Option<SourceFreshnessHint> {
    match &plan.strategy {
        ExtractionStrategy::TimeWindow { days_window, .. } if *days_window <= 7 => {
            Some(SourceFreshnessHint::Recent)
        }
        _ => None,
    }
}
