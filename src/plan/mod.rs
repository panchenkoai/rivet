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
    SourceConfig, TimeColumnType,
};

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::{Config, ExportConfig, ExportMode, IncrementalCursorMode};
use crate::error::Result;
use crate::tuning::{SourceTuning, TuningProfile, merge_tuning_config};

/// Fully resolved execution plan for a single export.
///
/// All execution decisions are derived before the pipeline starts.
/// Pipeline modules must not read raw config structures or CLI flags
/// once `build_plan` completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedRunPlan {
    pub export_name: String,
    /// Final query string (params substituted, query_file loaded).
    pub base_query: String,
    pub strategy: ExtractionStrategy,
    pub format: FormatType,
    pub compression: CompressionType,
    pub compression_level: Option<u32>,
    pub max_file_size_bytes: Option<u64>,
    pub skip_empty: bool,
    pub meta_columns: MetaColumns,
    pub destination: DestinationConfig,
    pub quality: Option<QualityConfig>,
    pub tuning: SourceTuning,
    pub tuning_profile_label: String,
    pub validate: bool,
    pub reconcile: bool,
    pub resume: bool,
    /// Source connection parameters — resolved from config at plan time so pipeline
    /// functions receive the complete execution contract in a single struct.
    pub source: SourceConfig,
}

/// Resolved incremental cursor semantics (Epic D / ADR-0007).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalCursorPlan {
    pub primary_column: String,
    pub fallback_column: Option<String>,
    pub mode: IncrementalCursorMode,
}

impl IncrementalCursorPlan {
    /// Synthetic column name in the result set for [`IncrementalCursorMode::Coalesce`] (stripped before write).
    pub const RIVET_COALESCE_CURSOR_COL: &'static str = "_rivet_coalesced_cursor";

    /// Column to read when advancing stored cursor after export (primary name, or synthetic coalesce column).
    pub fn column_for_storage_extract(&self) -> &str {
        match self.mode {
            IncrementalCursorMode::SingleColumn => self.primary_column.as_str(),
            IncrementalCursorMode::Coalesce => Self::RIVET_COALESCE_CURSOR_COL,
        }
    }
}

/// Extraction strategy and all parameters needed to execute it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtractionStrategy {
    Snapshot,
    Incremental(IncrementalCursorPlan),
    Chunked(ChunkedPlan),
    TimeWindow {
        column: String,
        column_type: TimeColumnType,
        days_window: u32,
    },
}

impl ExtractionStrategy {
    pub fn mode_label(&self) -> &'static str {
        match self {
            ExtractionStrategy::Snapshot => "full",
            ExtractionStrategy::Incremental(_) => "incremental",
            ExtractionStrategy::Chunked(_) => "chunked",
            ExtractionStrategy::TimeWindow { .. } => "timewindow",
        }
    }

    /// True for strategies that must load the cursor store before execution.
    ///
    /// Only `Incremental` reads the last cursor value to build the WHERE clause
    /// inside the source driver.  All other strategies are stateless at query time.
    pub fn needs_cursor_state(&self) -> bool {
        matches!(self, ExtractionStrategy::Incremental(_))
    }

    /// True for strategies that spawn parallel worker threads during execution.
    ///
    /// Only `Chunked` plans with `parallel > 1` use a thread pool.  All other
    /// strategies (including sequential chunked) run on the calling thread.
    pub fn requires_parallel_execution(&self) -> bool {
        matches!(self, ExtractionStrategy::Chunked(cp) if cp.parallel > 1)
    }

    /// True for strategies that support crash-resume via a persisted checkpoint.
    ///
    /// Only `Chunked` with `checkpoint: true` can resume mid-run.  All other
    /// strategies restart from scratch on retry.
    pub fn is_resumable(&self) -> bool {
        matches!(self, ExtractionStrategy::Chunked(cp) if cp.checkpoint)
    }

    /// Primary cursor column name for incremental exports (`None` for other strategies).
    pub fn cursor_column(&self) -> Option<&str> {
        match self {
            ExtractionStrategy::Incremental(p) => Some(p.primary_column.as_str()),
            _ => None,
        }
    }

    /// Resolved incremental cursor plan when strategy is incremental.
    pub fn incremental_plan(&self) -> Option<&IncrementalCursorPlan> {
        match self {
            ExtractionStrategy::Incremental(p) => Some(p),
            _ => None,
        }
    }

    /// Column name used to read the last cursor value from the final Arrow batch (may be synthetic).
    pub fn cursor_extract_column(&self) -> Option<&str> {
        match self {
            ExtractionStrategy::Incremental(p) => Some(p.column_for_storage_extract()),
            _ => None,
        }
    }

    /// Resolve the concrete SQL query for non-chunked strategies.
    ///
    /// Returns `None` for `Chunked` — chunked execution builds per-chunk queries
    /// inside the chunked pipeline and does not use a single resolved query string.
    ///
    /// | Strategy    | Query returned                                               |
    /// |-------------|--------------------------------------------------------------|
    /// | Snapshot    | `base_query` unchanged                                       |
    /// | Incremental | `base_query` unchanged (cursor WHERE added by source driver) |
    /// | TimeWindow  | `base_query` wrapped with a time-range predicate             |
    /// | Chunked     | `None`                                                       |
    pub fn resolve_query(
        &self,
        base_query: &str,
        source_type: crate::config::SourceType,
    ) -> Option<String> {
        match self {
            ExtractionStrategy::Snapshot | ExtractionStrategy::Incremental(_) => {
                Some(base_query.to_string())
            }
            ExtractionStrategy::TimeWindow {
                column,
                column_type,
                days_window,
            } => Some(build_time_window_query(
                base_query,
                column,
                *column_type,
                *days_window,
                source_type,
            )),
            ExtractionStrategy::Chunked(_) => None,
        }
    }
}

/// Wrap `base_query` with a trailing time-range predicate for `TimeWindow` exports.
///
/// The predicate anchors to midnight at the start of the window so the boundary is
/// stable for the entire run regardless of when within the day it executes.
///
/// - `Timestamp` columns compare against an ISO-8601 datetime literal.
/// - `Unix` columns compare against a Unix epoch integer.
///
/// The column name is quoted via `crate::sql::quote_ident` to prevent injection
/// when the name comes from user configuration.
pub fn build_time_window_query(
    base_query: &str,
    time_column: &str,
    time_type: TimeColumnType,
    days_window: u32,
    source_type: crate::config::SourceType,
) -> String {
    let quoted_col = crate::sql::quote_ident(source_type, time_column);

    let now = chrono::Utc::now();
    // `days_window` is a u32 — at the upper end (~4.3 billion days, i.e. ~12
    // million years) naive `now - Duration::days(n)` falls outside chrono's
    // representable range and panics. Use checked arithmetic and saturate
    // at chrono's MIN_UTC; the resulting SQL literal will be rejected by
    // the source if it is nonsensical, but the planner never panics.
    let window_start = chrono::Duration::try_days(days_window as i64)
        .and_then(|d| now.checked_sub_signed(d))
        .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
    let truncated = window_start
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always valid");

    let condition = match time_type {
        TimeColumnType::Timestamp => {
            format!(
                "{} >= '{}'",
                quoted_col,
                truncated.format("%Y-%m-%d %H:%M:%S")
            )
        }
        TimeColumnType::Unix => {
            format!("{} >= {}", quoted_col, truncated.and_utc().timestamp())
        }
    };

    format!(
        "SELECT * FROM ({base}) AS _rivet WHERE {cond}",
        base = base_query,
        cond = condition,
    )
}

/// Parameters for chunked extraction, pre-resolved from config and tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkedPlan {
    pub column: String,
    pub chunk_size: usize,
    pub parallel: usize,
    pub dense: bool,
    pub by_days: Option<u32>,
    pub checkpoint: bool,
    /// Resolved from `chunk_max_attempts` or `tuning.max_retries + 1`.
    pub max_attempts: u32,
}

/// Build a [`ResolvedRunPlan`] from config and CLI flags.
///
/// This is the only place where raw `ExportConfig` fields and CLI flags
/// are read for execution decisions. After this call the pipeline operates
/// only on `ResolvedRunPlan`.
pub fn build_plan(
    config: &Config,
    export: &ExportConfig,
    config_dir: &Path,
    validate: bool,
    reconcile: bool,
    resume: bool,
    params: Option<&HashMap<String, String>>,
) -> Result<ResolvedRunPlan> {
    let base_query = export.resolve_query(config_dir, params)?;

    let merged = merge_tuning_config(config.source.tuning.as_ref(), export.tuning.as_ref());
    let tuning = SourceTuning::from_config(merged.as_ref());
    let tuning_profile_label = match merged.as_ref().and_then(|t| t.profile) {
        Some(TuningProfile::Fast) => "fast".to_string(),
        Some(TuningProfile::Balanced) => "balanced".to_string(),
        Some(TuningProfile::Safe) => "safe".to_string(),
        None => "balanced (default)".to_string(),
    };

    let strategy = match export.mode {
        ExportMode::Full => ExtractionStrategy::Snapshot,
        ExportMode::Incremental => {
            let primary_column = export.cursor_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': incremental mode requires 'cursor_column'",
                    export.name
                )
            })?;
            let fallback_column = export.cursor_fallback_column.clone();
            let mode = export.incremental_cursor_mode;
            ExtractionStrategy::Incremental(IncrementalCursorPlan {
                primary_column,
                fallback_column,
                mode,
            })
        }
        ExportMode::Chunked => {
            let column = export.chunk_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': chunked mode requires 'chunk_column'",
                    export.name
                )
            })?;
            let max_attempts = export
                .chunk_max_attempts
                .unwrap_or_else(|| tuning.max_retries.saturating_add(1).max(1));
            ExtractionStrategy::Chunked(ChunkedPlan {
                column,
                chunk_size: export.chunk_size,
                parallel: export.parallel,
                dense: export.chunk_dense,
                by_days: export.chunk_by_days,
                checkpoint: export.chunk_checkpoint,
                max_attempts,
            })
        }
        ExportMode::TimeWindow => {
            let column = export.time_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': time_window mode requires 'time_column'",
                    export.name
                )
            })?;
            let days_window = export.days_window.ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': time_window mode requires 'days_window'",
                    export.name
                )
            })?;
            ExtractionStrategy::TimeWindow {
                column,
                column_type: export.time_column_type,
                days_window,
            }
        }
    };

    Ok(ResolvedRunPlan {
        export_name: export.name.clone(),
        base_query,
        strategy,
        format: export.format,
        compression: export.compression,
        compression_level: export.compression_level,
        max_file_size_bytes: export.max_file_size_bytes(),
        skip_empty: export.skip_empty,
        meta_columns: export.meta_columns.clone(),
        destination: export.destination.clone(),
        quality: export.quality.clone(),
        tuning,
        tuning_profile_label,
        validate,
        reconcile,
        resume,
        source: config.source.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, IncrementalCursorMode,
        MetaColumns, SourceConfig, SourceType,
    };

    fn minimal_source_config() -> SourceConfig {
        SourceConfig {
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
            tuning: None,
            tls: None,
        }
    }

    fn minimal_config() -> Config {
        Config {
            source: minimal_source_config(),
            exports: vec![],
            notifications: None,
            parallel_exports: false,
            parallel_export_processes: false,
        }
    }

    fn minimal_export() -> ExportConfig {
        ExportConfig {
            name: "test_export".into(),
            query: Some("SELECT 1".into()),
            query_file: None,
            mode: ExportMode::Full,
            cursor_column: None,
            cursor_fallback_column: None,
            incremental_cursor_mode: IncrementalCursorMode::SingleColumn,
            chunk_column: None,
            chunk_size: 100_000,
            chunk_dense: false,
            chunk_by_days: None,
            parallel: 1,
            time_column: None,
            time_column_type: TimeColumnType::Timestamp,
            days_window: None,
            format: FormatType::Parquet,
            compression: CompressionType::Zstd,
            compression_level: None,
            skip_empty: false,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("./out".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
            },
            meta_columns: MetaColumns::default(),
            quality: None,
            max_file_size: None,
            chunk_checkpoint: false,
            chunk_max_attempts: None,
            tuning: None,
            source_group: None,
            reconcile_required: false,
        }
    }

    #[test]
    fn snapshot_plan_from_full_mode() {
        let export = minimal_export();
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        assert!(matches!(plan.strategy, ExtractionStrategy::Snapshot));
        assert_eq!(plan.strategy.mode_label(), "full");
        assert_eq!(plan.export_name, "test_export");
        assert_eq!(plan.base_query, "SELECT 1");
        assert!(!plan.validate);
        assert!(!plan.reconcile);
        assert!(!plan.resume);
    }

    #[test]
    fn incremental_plan_resolves_cursor_column() {
        let mut export = minimal_export();
        export.mode = ExportMode::Incremental;
        export.cursor_column = Some("updated_at".into());
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Incremental(p) => {
                assert_eq!(p.primary_column, "updated_at");
                assert_eq!(p.mode, IncrementalCursorMode::SingleColumn);
            }
            _ => panic!("expected Incremental"),
        }
        assert_eq!(plan.strategy.mode_label(), "incremental");
    }

    #[test]
    fn chunked_plan_resolves_all_fields() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size = 50_000;
        export.parallel = 4;
        export.chunk_checkpoint = true;
        export.chunk_max_attempts = Some(5);
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            true,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => {
                assert_eq!(cp.column, "id");
                assert_eq!(cp.chunk_size, 50_000);
                assert_eq!(cp.parallel, 4);
                assert!(cp.checkpoint);
                assert_eq!(cp.max_attempts, 5);
            }
            _ => panic!("expected Chunked"),
        }
        assert_eq!(plan.strategy.mode_label(), "chunked");
        assert!(plan.validate);
    }

    #[test]
    fn chunked_max_attempts_defaults_from_tuning() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        // No chunk_max_attempts → balanced: max_retries=3 → 3+1=4
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => assert_eq!(cp.max_attempts, 4),
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn time_window_plan_resolves_column_and_days() {
        let mut export = minimal_export();
        export.mode = ExportMode::TimeWindow;
        export.time_column = Some("created_at".into());
        export.time_column_type = TimeColumnType::Unix;
        export.days_window = Some(30);
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::TimeWindow {
                column,
                column_type,
                days_window,
            } => {
                assert_eq!(column, "created_at");
                assert_eq!(*column_type, TimeColumnType::Unix);
                assert_eq!(*days_window, 30);
            }
            _ => panic!("expected TimeWindow"),
        }
        assert_eq!(plan.strategy.mode_label(), "timewindow");
    }

    #[test]
    fn plan_carries_cli_flags() {
        let export = minimal_export();
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            true,
            true,
            true,
            None,
        )
        .unwrap();
        assert!(plan.validate);
        assert!(plan.reconcile);
        assert!(plan.resume);
    }

    #[test]
    fn plan_resolves_tuning_profile_label() {
        let export = minimal_export();
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        assert_eq!(plan.tuning_profile_label, "balanced (default)");
    }

    // ─── ExtractionStrategy behavioral contracts ──────────────────────────────

    #[test]
    fn snapshot_strategy_contracts() {
        let s = ExtractionStrategy::Snapshot;
        assert!(!s.needs_cursor_state());
        assert!(!s.is_resumable());
        assert!(s.cursor_column().is_none());
        let q = s.resolve_query("SELECT 1", SourceType::Postgres).unwrap();
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn incremental_strategy_contracts() {
        let s = ExtractionStrategy::Incremental(IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
        });
        assert!(s.needs_cursor_state());
        assert!(!s.is_resumable());
        assert_eq!(s.cursor_column(), Some("updated_at"));
        // base query is passed through; cursor WHERE is added by the source driver
        let q = s
            .resolve_query("SELECT * FROM orders", SourceType::Postgres)
            .unwrap();
        assert_eq!(q, "SELECT * FROM orders");
    }

    #[test]
    fn chunked_without_checkpoint_contracts() {
        let s = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 10_000,
            parallel: 1,
            dense: false,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        });
        assert!(!s.needs_cursor_state());
        assert!(!s.is_resumable());
        assert!(s.cursor_column().is_none());
        assert!(s.resolve_query("SELECT 1", SourceType::Postgres).is_none()); // chunked builds per-chunk queries
    }

    #[test]
    fn chunked_with_checkpoint_is_resumable() {
        let s = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 10_000,
            parallel: 1,
            dense: false,
            by_days: None,
            checkpoint: true,
            max_attempts: 3,
        });
        assert!(s.is_resumable());
        assert!(s.resolve_query("SELECT 1", SourceType::Postgres).is_none());
    }

    #[test]
    fn time_window_strategy_contracts() {
        let s = ExtractionStrategy::TimeWindow {
            column: "created_at".into(),
            column_type: TimeColumnType::Timestamp,
            days_window: 7,
        };
        assert!(!s.needs_cursor_state());
        assert!(!s.is_resumable());
        assert!(s.cursor_column().is_none());
        let q = s
            .resolve_query("SELECT * FROM events", SourceType::Postgres)
            .unwrap();
        assert!(q.contains("_rivet WHERE"));
        // Column is now quoted
        assert!(q.contains("\"created_at\" >="));
    }

    #[test]
    fn build_time_window_query_timestamp() {
        let q = build_time_window_query(
            "SELECT * FROM events",
            "created_at",
            TimeColumnType::Timestamp,
            7,
            SourceType::Postgres,
        );
        // Column is quoted; value uses single-quoted datetime literal
        assert!(q.contains("\"created_at\" >= '"), "got: {}", q);
        assert!(q.contains("_rivet WHERE"));
    }

    #[test]
    fn build_time_window_query_unix() {
        let q = build_time_window_query(
            "SELECT * FROM events",
            "ts",
            TimeColumnType::Unix,
            30,
            SourceType::Postgres,
        );
        // Column is quoted; Unix timestamps have no surrounding single-quotes
        assert!(q.contains("\"ts\" >= "), "got: {}", q);
        assert!(
            !q.contains("'"),
            "unix should not have value quotes, got: {}",
            q
        );
    }
}
