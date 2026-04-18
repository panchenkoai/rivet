//! Source-aware extraction prioritization — planning-time advisory models (ADR-0006).
//!
//! These types are **advisory only**: they classify and rank exports for human review
//! and external orchestration; they do not control runtime execution.
//!
use serde::{Deserialize, Serialize};

/// High-level extraction mode used as a prioritization signal.
///
/// Mirrors [`crate::plan::ExtractionStrategy`] without nested parameters so inputs
/// stay easy to serialize and score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrioritizationStrategyKind {
    Snapshot,
    Incremental,
    Chunked,
    TimeWindow,
}

impl PrioritizationStrategyKind {
    /// Derive the prioritization kind from a resolved extraction strategy.
    pub fn from_extraction(strategy: &super::ExtractionStrategy) -> Self {
        match strategy {
            super::ExtractionStrategy::Snapshot => Self::Snapshot,
            super::ExtractionStrategy::Incremental(_) => Self::Incremental,
            super::ExtractionStrategy::Chunked(_) => Self::Chunked,
            super::ExtractionStrategy::TimeWindow { .. } => Self::TimeWindow,
        }
    }
}

/// Cursor reliability tier consumed by the scoring engine (heuristic; see ADR-0007 and `plan/inputs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorQuality {
    StrongMonotonic,
    WeakTime,
    WeakMultiCandidate,
    FallbackOnly,
    None,
}

/// Optional hint that the source row set was observed as recently fresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFreshnessHint {
    Recent,
    Stale,
    Unknown,
}

/// Taxonomy of machine-readable recommendation reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationReasonKind {
    SmallTable,
    LargeTable,
    HugeTable,
    StrongCursor,
    WeakCursor,
    NoTimeCursor,
    SparseRangeRisk,
    ChunkingHeavy,
    ReconcileRequired,
    FreshnessHintRecent,
    SharedSourceHeavyConflict,
    LowConfidenceMetadata,
    /// Epic I — recent runs retried frequently.
    HighRetryRateHistory,
    /// Epic I — the most recent run ended in failure.
    RecentFailureHistory,
    /// Epic I — historical average duration is notably high.
    SlowHistory,
}

impl RecommendationReasonKind {
    /// Stable snake_case label (matches JSON `rename_all = "snake_case"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SmallTable => "small_table",
            Self::LargeTable => "large_table",
            Self::HugeTable => "huge_table",
            Self::StrongCursor => "strong_cursor",
            Self::WeakCursor => "weak_cursor",
            Self::NoTimeCursor => "no_time_cursor",
            Self::SparseRangeRisk => "sparse_range_risk",
            Self::ChunkingHeavy => "chunking_heavy",
            Self::ReconcileRequired => "reconcile_required",
            Self::FreshnessHintRecent => "freshness_hint_recent",
            Self::SharedSourceHeavyConflict => "shared_source_heavy_conflict",
            Self::LowConfidenceMetadata => "low_confidence_metadata",
            Self::HighRetryRateHistory => "high_retry_rate_history",
            Self::RecentFailureHistory => "recent_failure_history",
            Self::SlowHistory => "slow_history",
        }
    }
}

/// One explainable reason attached to a recommendation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendationReason {
    pub kind: RecommendationReasonKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostClass {
    Low,
    Medium,
    High,
    VeryHigh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskClass {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriorityClass {
    Low,
    Medium,
    High,
}

/// Per-export advisory recommendation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportRecommendation {
    pub export_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_group: Option<String>,
    pub priority_score: i32,
    pub priority_class: PriorityClass,
    pub cost_class: CostClass,
    pub risk_class: RiskClass,
    pub recommended_wave: u32,
    pub isolate_on_source: bool,
    pub reasons: Vec<RecommendationReason>,
}

/// One execution wave (advisory grouping).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecommendedWave {
    pub wave: u32,
    pub exports: Vec<String>,
}

/// Logical source capacity boundary shared by multiple exports (for future campaign summaries / JSON helpers).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceGroupInfo {
    pub name: String,
    pub export_names: Vec<String>,
}

/// Campaign-level advisory output for a set of exports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CampaignRecommendation {
    pub ordered_exports: Vec<ExportRecommendation>,
    pub waves: Vec<RecommendedWave>,
    pub source_group_warnings: Vec<String>,
}

/// Normalized inputs for the per-export scoring engine (see `plan::recommend` in Stage 2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrioritizationInputs {
    pub export_name: String,
    pub source_group: Option<String>,
    pub strategy: PrioritizationStrategyKind,
    pub estimated_rows: Option<u64>,
    pub estimated_size_bytes: Option<u64>,
    pub chunk_count: Option<u32>,
    pub sparse_range_risk: bool,
    pub cursor_quality: CursorQuality,
    pub reconcile_required: bool,
    pub source_freshness_hint: Option<SourceFreshnessHint>,
    /// Epic I — historical signals; `None` when history lookup is skipped or unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<super::HistorySnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IncrementalCursorMode;
    use crate::plan::{ChunkedPlan, ExtractionStrategy, IncrementalCursorPlan};

    #[test]
    fn strategy_kind_from_extraction_variants() {
        assert_eq!(
            PrioritizationStrategyKind::from_extraction(&ExtractionStrategy::Snapshot),
            PrioritizationStrategyKind::Snapshot
        );
        assert_eq!(
            PrioritizationStrategyKind::from_extraction(&ExtractionStrategy::Incremental(
                IncrementalCursorPlan {
                    primary_column: "x".into(),
                    fallback_column: None,
                    mode: IncrementalCursorMode::SingleColumn,
                }
            )),
            PrioritizationStrategyKind::Incremental
        );
        assert_eq!(
            PrioritizationStrategyKind::from_extraction(&ExtractionStrategy::Chunked(
                ChunkedPlan {
                    column: "id".into(),
                    chunk_size: 1,
                    parallel: 1,
                    dense: false,
                    by_days: None,
                    checkpoint: false,
                    max_attempts: 1,
                }
            )),
            PrioritizationStrategyKind::Chunked
        );
        assert_eq!(
            PrioritizationStrategyKind::from_extraction(&ExtractionStrategy::TimeWindow {
                column: "t".into(),
                column_type: crate::plan::TimeColumnType::Timestamp,
                days_window: 1,
            }),
            PrioritizationStrategyKind::TimeWindow
        );
    }

    #[test]
    fn json_round_trip_export_recommendation() {
        let rec = ExportRecommendation {
            export_name: "orders".into(),
            source_group: None,
            priority_score: 10,
            priority_class: PriorityClass::High,
            cost_class: CostClass::Medium,
            risk_class: RiskClass::Low,
            recommended_wave: 1,
            isolate_on_source: false,
            reasons: vec![RecommendationReason {
                kind: RecommendationReasonKind::StrongCursor,
                message: "Monotonic cursor configured".into(),
            }],
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: ExportRecommendation = serde_json::from_str(&json).unwrap();
        assert_eq!(back, rec);
    }
}
