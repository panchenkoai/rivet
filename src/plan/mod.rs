pub mod artifact;
pub mod campaign;
pub mod history;
pub mod inputs;
pub mod prioritization;
pub mod recommend;
pub mod reconcile;
pub mod repair;
pub mod validate;

#[allow(unused_imports)]
pub use history::{HistorySnapshot, LastStatus};

pub use artifact::{
    ComputedPlanData, PlanArtifact, PlanDiagnostics, PlanPrioritizationSnapshot, StalenessCheck,
};
#[allow(unused_imports)]
pub use prioritization::{
    CampaignRecommendation, CostClass, CursorQuality, ExportRecommendation, PrioritizationInputs,
    PrioritizationStrategyKind, PriorityClass, RecommendationReason, RecommendationReasonKind,
    RecommendedWave, RiskClass, SourceFreshnessHint, SourceGroupInfo,
};
#[allow(unused_imports)]
pub use reconcile::{
    PartitionKind, PartitionResult, PartitionStatus, ReconcileReport, ReconcileSummary,
};
#[allow(unused_imports)]
pub use repair::{RepairAction, RepairOutcome, RepairPlan, RepairReport, RepairSummary};
pub use validate::{DiagnosticLevel, validate_plan};

// Re-export value types so pipeline modules import from `crate::plan`, not `crate::config`.
// `DestinationType` is used by validate.rs internally and by external callers of the plan API.
#[allow(unused_imports)]
pub use crate::config::{
    CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, QualityConfig,
    SchemaDriftPolicy, SourceConfig, TimeColumnType,
};

pub(crate) mod build;
mod contract;

pub use build::{build_plan, parse_column_overrides_pub};
pub use contract::{
    ChunkedPlan, ExtractionStrategy, IncrementalCursorPlan, ResolvedRunPlan,
    build_time_window_query,
};
