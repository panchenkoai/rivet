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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, ExportConfig, FormatType, MetaColumns,
        SourceConfig, SourceType, TimeColumnType,
    };
    use crate::plan::{ChunkedPlan, IncrementalCursorPlan, PlanDiagnostics};
    use crate::tuning::SourceTuning;

    fn export_cfg(name: &str) -> ExportConfig {
        let yaml =
            format!("name: {name}\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n");
        serde_yaml_ng::from_str(&yaml).expect("parse ExportConfig")
    }

    fn minimal_plan(strategy: ExtractionStrategy) -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "test".into(),
            base_query: "SELECT 1".into(),
            strategy,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("/tmp".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://localhost/test".into()),
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
            },
            column_overrides: Default::default(),
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn empty_computed() -> ComputedPlanData {
        ComputedPlanData {
            chunk_ranges: vec![],
            chunk_count: 0,
            cursor_snapshot: None,
            row_estimate: None,
        }
    }

    fn empty_diagnostics() -> PlanDiagnostics {
        PlanDiagnostics {
            verdict: "ok".into(),
            warnings: vec![],
            recommended_profile: "balanced".into(),
        }
    }

    fn incremental_plan(mode: IncrementalCursorMode) -> ResolvedRunPlan {
        minimal_plan(ExtractionStrategy::Incremental(IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode,
        }))
    }

    // ── snapshot strategy → None cursor quality ──────────────────────────────

    #[test]
    fn snapshot_strategy_cursor_quality_is_none() {
        let plan = minimal_plan(ExtractionStrategy::Snapshot);
        let export = export_cfg("orders");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::None);
        assert!(inputs.source_freshness_hint.is_none());
    }

    // ── incremental + SingleColumn cursor quality ────────────────────────────

    #[test]
    fn single_column_both_hints_strong_monotonic() {
        let plan = incremental_plan(IncrementalCursorMode::SingleColumn);
        let export = export_cfg("orders");
        let hints = PrioritizationHints {
            incremental_uses_index: true,
            cursor_range_observed: true,
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            hints,
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::StrongMonotonic);
    }

    #[test]
    fn single_column_index_only_weak_time() {
        let plan = incremental_plan(IncrementalCursorMode::SingleColumn);
        let export = export_cfg("orders");
        let hints = PrioritizationHints {
            incremental_uses_index: true,
            cursor_range_observed: false,
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            hints,
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::WeakTime);
    }

    #[test]
    fn single_column_range_only_weak_multi_candidate() {
        let plan = incremental_plan(IncrementalCursorMode::SingleColumn);
        let export = export_cfg("orders");
        let hints = PrioritizationHints {
            incremental_uses_index: false,
            cursor_range_observed: true,
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            hints,
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::WeakMultiCandidate);
    }

    #[test]
    fn single_column_no_hints_cursor_quality_none() {
        let plan = incremental_plan(IncrementalCursorMode::SingleColumn);
        let export = export_cfg("orders");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::None);
    }

    // ── incremental + Coalesce cursor quality ────────────────────────────────

    #[test]
    fn coalesce_both_hints_weak_multi_candidate() {
        let plan = incremental_plan(IncrementalCursorMode::Coalesce);
        let export = export_cfg("orders");
        let hints = PrioritizationHints {
            incremental_uses_index: true,
            cursor_range_observed: true,
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            hints,
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::WeakMultiCandidate);
    }

    #[test]
    fn coalesce_index_only_weak_time() {
        let plan = incremental_plan(IncrementalCursorMode::Coalesce);
        let export = export_cfg("orders");
        let hints = PrioritizationHints {
            incremental_uses_index: true,
            cursor_range_observed: false,
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            hints,
            None,
        );
        assert_eq!(inputs.cursor_quality, CursorQuality::WeakTime);
    }

    // ── TimeWindow freshness hint ─────────────────────────────────────────────

    #[test]
    fn timewindow_7_days_is_recent() {
        let plan = minimal_plan(ExtractionStrategy::TimeWindow {
            column: "created_at".into(),
            column_type: TimeColumnType::Timestamp,
            days_window: 7,
        });
        let export = export_cfg("events");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert_eq!(
            inputs.source_freshness_hint,
            Some(SourceFreshnessHint::Recent)
        );
    }

    #[test]
    fn timewindow_8_days_no_freshness_hint() {
        let plan = minimal_plan(ExtractionStrategy::TimeWindow {
            column: "created_at".into(),
            column_type: TimeColumnType::Timestamp,
            days_window: 8,
        });
        let export = export_cfg("events");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert!(inputs.source_freshness_hint.is_none());
    }

    // ── sparse_range_risk detection ──────────────────────────────────────────

    #[test]
    fn sparse_range_warning_sets_risk_flag() {
        let plan = minimal_plan(ExtractionStrategy::Snapshot);
        let export = export_cfg("orders");
        let diags = PlanDiagnostics {
            verdict: "warning".into(),
            warnings: vec!["sparse range detected — consider smaller chunks".into()],
            recommended_profile: "safe".into(),
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &diags,
            PrioritizationHints::default(),
            None,
        );
        assert!(inputs.sparse_range_risk);
    }

    #[test]
    fn unrelated_warning_does_not_set_risk_flag() {
        let plan = minimal_plan(ExtractionStrategy::Snapshot);
        let export = export_cfg("orders");
        let diags = PlanDiagnostics {
            verdict: "warning".into(),
            warnings: vec!["index missing on cursor column".into()],
            recommended_profile: "balanced".into(),
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &diags,
            PrioritizationHints::default(),
            None,
        );
        assert!(!inputs.sparse_range_risk);
    }

    // ── computed fields mapped correctly ─────────────────────────────────────

    #[test]
    fn estimated_rows_from_row_estimate() {
        let plan = minimal_plan(ExtractionStrategy::Snapshot);
        let export = export_cfg("orders");
        let computed = ComputedPlanData {
            chunk_ranges: vec![],
            chunk_count: 0,
            cursor_snapshot: None,
            row_estimate: Some(50_000),
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &computed,
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert_eq!(inputs.estimated_rows, Some(50_000));
    }

    #[test]
    fn chunk_count_zero_maps_to_none() {
        let plan = minimal_plan(ExtractionStrategy::Snapshot);
        let export = export_cfg("orders");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert!(inputs.chunk_count.is_none());
    }

    #[test]
    fn chunk_count_nonzero_maps_to_some() {
        let plan = minimal_plan(ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 1000,
            chunk_count: None,
            parallel: 1,
            dense: false,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        }));
        let export = export_cfg("orders");
        let computed = ComputedPlanData {
            chunk_ranges: vec![],
            chunk_count: 5,
            cursor_snapshot: None,
            row_estimate: None,
        };
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &computed,
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert_eq!(inputs.chunk_count, Some(5));
    }

    // ── reconcile_required propagation ───────────────────────────────────────

    #[test]
    fn reconcile_required_from_plan_reconcile() {
        let mut plan = minimal_plan(ExtractionStrategy::Snapshot);
        plan.reconcile = true;
        let export = export_cfg("orders");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert!(inputs.reconcile_required);
    }

    // ── export_name and source_group forwarded ───────────────────────────────

    #[test]
    fn export_name_forwarded_to_inputs() {
        let plan = minimal_plan(ExtractionStrategy::Snapshot);
        let export = export_cfg("my_export");
        let inputs = build_prioritization_inputs(
            &export,
            &plan,
            &empty_computed(),
            &empty_diagnostics(),
            PrioritizationHints::default(),
            None,
        );
        assert_eq!(inputs.export_name, "my_export");
    }
}
