//! Deterministic per-export prioritization scoring (ADR-0006).

use super::{
    CostClass, CursorQuality, ExportRecommendation, PlanDiagnostics, PrioritizationInputs,
    PrioritizationStrategyKind, PriorityClass, RecommendationReason, RecommendationReasonKind,
    RiskClass, SourceFreshnessHint,
};

/// Produce an advisory recommendation for one export.
pub fn recommend_export(
    inputs: &PrioritizationInputs,
    diagnostics: &PlanDiagnostics,
) -> ExportRecommendation {
    let mut reasons: Vec<RecommendationReason> = Vec::new();
    let mut score: i32 = 50;

    let verdict = diagnostics.verdict.to_ascii_lowercase();

    // --- Row / size classes (metadata is signal, not truth) ---
    let cost_class = classify_cost(inputs, &mut reasons, &mut score);
    let risk_class = classify_risk(inputs, diagnostics, verdict.as_str(), &mut score);

    // --- Strategy & cursor ---
    match inputs.strategy {
        PrioritizationStrategyKind::Snapshot => {
            score += 5;
        }
        PrioritizationStrategyKind::Incremental => match inputs.cursor_quality {
            CursorQuality::StrongMonotonic => {
                score += 18;
                reasons.push(RecommendationReason {
                    kind: RecommendationReasonKind::StrongCursor,
                    message: "Incremental export with indexed cursor — reliable continuation."
                        .into(),
                });
            }
            CursorQuality::WeakTime | CursorQuality::WeakMultiCandidate => {
                score -= 8;
                reasons.push(RecommendationReason {
                    kind: RecommendationReasonKind::WeakCursor,
                    message: "Weaker cursor signal — treat ordering as advisory.".into(),
                });
            }
            CursorQuality::FallbackOnly => {
                score -= 14;
                reasons.push(RecommendationReason {
                    kind: RecommendationReasonKind::WeakCursor,
                    message: "Fallback cursor quality — higher reconcile and ordering risk.".into(),
                });
            }
            CursorQuality::None => {
                score -= 12;
                reasons.push(RecommendationReason {
                    kind: RecommendationReasonKind::WeakCursor,
                    message:
                        "Cursor quality unknown or weak — prefer validation before large runs."
                            .into(),
                });
            }
        },
        PrioritizationStrategyKind::Chunked => {
            let chunks = inputs.chunk_count.unwrap_or(0);
            if chunks > 1 || inputs.estimated_rows.unwrap_or(0) > 1_000_000 {
                score -= 10;
                reasons.push(RecommendationReason {
                    kind: RecommendationReasonKind::ChunkingHeavy,
                    message: format!(
                        "Chunked extraction ({chunks} chunk windows) — higher source load and runtime."
                    ),
                });
            } else {
                reasons.push(RecommendationReason {
                    kind: RecommendationReasonKind::ChunkingHeavy,
                    message: "Chunked extraction — monitor source during long runs.".into(),
                });
            }
        }
        PrioritizationStrategyKind::TimeWindow => {
            reasons.push(RecommendationReason {
                kind: RecommendationReasonKind::NoTimeCursor,
                message:
                    "Time-window strategy — bounded scan; ordering balances freshness vs cost."
                        .into(),
            });
        }
    }

    if inputs.sparse_range_risk {
        score -= 18;
        reasons.push(RecommendationReason {
            kind: RecommendationReasonKind::SparseRangeRisk,
            message: "Sparse key range detected — many empty chunks possible; defer relative to dense exports."
                .into(),
        });
    }

    if inputs.reconcile_required {
        score -= 12;
        reasons.push(RecommendationReason {
            kind: RecommendationReasonKind::ReconcileRequired,
            message: "Reconcile enabled — extra source read; schedule after cheaper exports unless critical."
                .into(),
        });
    }

    if matches!(
        inputs.source_freshness_hint,
        Some(SourceFreshnessHint::Recent)
    ) {
        score += 6;
        reasons.push(RecommendationReason {
            kind: RecommendationReasonKind::FreshnessHintRecent,
            message:
                "Narrow time window — likely fresher subset; good candidate for an early wave."
                    .into(),
        });
    }

    // Verdict adjustment (preflight)
    if verdict.contains("unsafe") {
        score -= 25;
    } else if verdict.contains("degraded") {
        score -= 12;
    } else if verdict.contains("efficient") {
        score += 8;
    }

    // Epic I: historical refinement (bounded ±15 total to avoid overriding preflight).
    apply_history_signals(inputs, &mut score, &mut reasons);

    score = score.clamp(0, 100);

    let priority_class = classify_priority(score);
    let recommended_wave = wave_from_score(score);

    ExportRecommendation {
        export_name: inputs.export_name.clone(),
        source_group: inputs.source_group.clone(),
        priority_score: score,
        priority_class,
        cost_class,
        risk_class,
        recommended_wave,
        isolate_on_source: false,
        reasons,
    }
}

fn classify_cost(
    inputs: &PrioritizationInputs,
    reasons: &mut Vec<RecommendationReason>,
    score: &mut i32,
) -> CostClass {
    let rows = inputs.estimated_rows;
    match rows {
        Some(r) if r < 100_000 => {
            *score += 12;
            reasons.push(RecommendationReason {
                kind: RecommendationReasonKind::SmallTable,
                message: format!("Smaller estimated row count (~{r}) — lower extraction cost."),
            });
            CostClass::Low
        }
        Some(r) if r < 10_000_000 => {
            reasons.push(RecommendationReason {
                kind: RecommendationReasonKind::LargeTable,
                message: format!("Medium/large estimated row count (~{r})."),
            });
            CostClass::Medium
        }
        Some(r) => {
            *score -= 15;
            reasons.push(RecommendationReason {
                kind: RecommendationReasonKind::HugeTable,
                message: format!(
                    "Very large estimated row count (~{r}) — defer and isolate if possible."
                ),
            });
            let heavy_chunk = inputs.chunk_count.unwrap_or(0) >= 8
                || (inputs.strategy == PrioritizationStrategyKind::Chunked
                    && inputs.chunk_count.unwrap_or(0) > 16);
            if heavy_chunk {
                CostClass::VeryHigh
            } else {
                CostClass::High
            }
        }
        None => {
            *score -= 3;
            reasons.push(RecommendationReason {
                kind: RecommendationReasonKind::LowConfidenceMetadata,
                message: "No row estimate — assume medium cost until preflight succeeds.".into(),
            });
            CostClass::Medium
        }
    }
}

fn classify_risk(
    inputs: &PrioritizationInputs,
    diagnostics: &PlanDiagnostics,
    verdict: &str,
    score: &mut i32,
) -> RiskClass {
    if inputs.sparse_range_risk {
        return RiskClass::High;
    }
    if verdict.contains("unsafe") {
        return RiskClass::High;
    }
    if verdict.contains("degraded") || diagnostics.warnings.len() > 3 {
        *score -= 4;
        RiskClass::Medium
    } else {
        RiskClass::Low
    }
}

fn apply_history_signals(
    inputs: &PrioritizationInputs,
    score: &mut i32,
    reasons: &mut Vec<RecommendationReason>,
) {
    let Some(h) = inputs.history.as_ref() else {
        return;
    };

    if h.last_run_failed() {
        *score -= 8;
        reasons.push(RecommendationReason {
            kind: RecommendationReasonKind::RecentFailureHistory,
            message:
                "Most recent run ended in failure — investigate before scheduling a large retry."
                    .into(),
        });
    }

    if h.retry_rate_is_high() {
        *score -= 5;
        reasons.push(RecommendationReason {
            kind: RecommendationReasonKind::HighRetryRateHistory,
            message: format!(
                "Recent runs retried frequently ({:.1} retries/run over {} runs).",
                h.retry_rate, h.sample_size
            ),
        });
    }

    if h.is_slow_historically() {
        *score -= 4;
        reasons.push(RecommendationReason {
            kind: RecommendationReasonKind::SlowHistory,
            message: format!(
                "Historical average duration is {} min — expect similar runtime.",
                h.avg_duration_ms / 60_000
            ),
        });
    }
}

fn classify_priority(score: i32) -> PriorityClass {
    match score {
        70..=100 => PriorityClass::High,
        40..=69 => PriorityClass::Medium,
        _ => PriorityClass::Low,
    }
}

fn wave_from_score(score: i32) -> u32 {
    match score {
        75..=100 => 1,
        55..=74 => 2,
        35..=54 => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlanDiagnostics;

    fn diag_efficient() -> PlanDiagnostics {
        PlanDiagnostics {
            verdict: "EFFICIENT".into(),
            warnings: vec![],
            recommended_profile: "fast".into(),
        }
    }

    fn base_inputs() -> PrioritizationInputs {
        PrioritizationInputs {
            export_name: "e".into(),
            source_group: None,
            strategy: PrioritizationStrategyKind::Snapshot,
            estimated_rows: Some(10_000),
            estimated_size_bytes: None,
            chunk_count: None,
            sparse_range_risk: false,
            cursor_quality: CursorQuality::None,
            reconcile_required: false,
            source_freshness_hint: None,
            history: None,
        }
    }

    #[test]
    fn tiny_snapshot_scores_high_wave() {
        let mut i = base_inputs();
        i.estimated_rows = Some(5000);
        let r = recommend_export(&i, &diag_efficient());
        assert!(r.priority_score >= 60);
        assert_eq!(r.recommended_wave, 1);
    }

    #[test]
    fn huge_sparse_chunked_scores_low() {
        let mut i = base_inputs();
        i.strategy = PrioritizationStrategyKind::Chunked;
        i.estimated_rows = Some(50_000_000);
        i.chunk_count = Some(40);
        i.sparse_range_risk = true;
        let mut d = diag_efficient();
        d.verdict = "DEGRADED".into();
        let r = recommend_export(&i, &d);
        assert!(r.priority_score < 50);
        assert!(r.recommended_wave >= 3);
    }

    #[test]
    fn reconcile_required_adds_reason() {
        let mut i = base_inputs();
        i.reconcile_required = true;
        let r = recommend_export(&i, &diag_efficient());
        assert!(
            r.reasons
                .iter()
                .any(|x| x.kind == RecommendationReasonKind::ReconcileRequired)
        );
    }

    #[test]
    fn medium_incremental_with_strong_cursor_classes_medium_cost() {
        let mut i = base_inputs();
        i.strategy = PrioritizationStrategyKind::Incremental;
        i.estimated_rows = Some(2_000_000);
        i.cursor_quality = CursorQuality::StrongMonotonic;
        let r = recommend_export(&i, &diag_efficient());
        assert_eq!(r.cost_class, CostClass::Medium);
        assert!(
            r.reasons
                .iter()
                .any(|x| x.kind == RecommendationReasonKind::StrongCursor),
            "expected strong_cursor reason, got: {:?}",
            r.reasons
        );
    }

    #[test]
    fn weak_cursor_incremental_is_penalized() {
        let mut i = base_inputs();
        i.strategy = PrioritizationStrategyKind::Incremental;
        i.estimated_rows = Some(500_000);
        i.cursor_quality = CursorQuality::WeakMultiCandidate;
        let r = recommend_export(&i, &diag_efficient());
        assert!(
            r.reasons
                .iter()
                .any(|x| x.kind == RecommendationReasonKind::WeakCursor),
            "expected weak_cursor reason"
        );
        // Weak cursor must not earn a strong-cursor score bump.
        let mut strong = i.clone();
        strong.cursor_quality = CursorQuality::StrongMonotonic;
        let r_strong = recommend_export(&strong, &diag_efficient());
        assert!(
            r.priority_score < r_strong.priority_score,
            "weak cursor should score lower than strong cursor ({} vs {})",
            r.priority_score,
            r_strong.priority_score
        );
    }

    #[test]
    fn history_with_recent_failure_lowers_score_and_adds_reason() {
        use crate::plan::history::{HistorySnapshot, LastStatus};
        let mut i = base_inputs();
        i.estimated_rows = Some(10_000);
        let baseline = recommend_export(&i, &diag_efficient()).priority_score;
        i.history = Some(HistorySnapshot {
            sample_size: 5,
            success_rate: 0.6,
            retry_rate: 0.0,
            avg_duration_ms: 1000,
            last_status: Some(LastStatus::Failed),
        });
        let r = recommend_export(&i, &diag_efficient());
        assert!(
            r.priority_score < baseline,
            "score should decrease with recent failure ({} vs {})",
            r.priority_score,
            baseline
        );
        assert!(
            r.reasons
                .iter()
                .any(|x| x.kind == RecommendationReasonKind::RecentFailureHistory)
        );
    }

    #[test]
    fn high_retry_rate_history_adds_reason() {
        use crate::plan::history::HistorySnapshot;
        let mut i = base_inputs();
        i.history = Some(HistorySnapshot {
            sample_size: 10,
            success_rate: 1.0,
            retry_rate: 0.8,
            avg_duration_ms: 1000,
            last_status: Some(crate::plan::history::LastStatus::Succeeded),
        });
        let r = recommend_export(&i, &diag_efficient());
        assert!(
            r.reasons
                .iter()
                .any(|x| x.kind == RecommendationReasonKind::HighRetryRateHistory)
        );
    }

    #[test]
    fn no_history_means_no_historical_reasons() {
        let i = base_inputs();
        let r = recommend_export(&i, &diag_efficient());
        assert!(
            r.reasons.iter().all(|x| !matches!(
                x.kind,
                RecommendationReasonKind::RecentFailureHistory
                    | RecommendationReasonKind::HighRetryRateHistory
                    | RecommendationReasonKind::SlowHistory,
            )),
            "no history should produce no historical reasons"
        );
    }

    #[test]
    fn no_row_estimate_attaches_low_confidence_reason() {
        let mut i = base_inputs();
        i.estimated_rows = None;
        let r = recommend_export(&i, &diag_efficient());
        assert!(
            r.reasons
                .iter()
                .any(|x| x.kind == RecommendationReasonKind::LowConfidenceMetadata),
            "expected low_confidence_metadata reason, got: {:?}",
            r.reasons
        );
        assert_eq!(r.cost_class, CostClass::Medium);
    }
}
