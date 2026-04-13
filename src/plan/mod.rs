pub mod validate;

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

use crate::config::{Config, ExportConfig, ExportMode};
use crate::error::Result;
use crate::tuning::{SourceTuning, TuningProfile, merge_tuning_config};

/// Fully resolved execution plan for a single export.
///
/// All execution decisions are derived before the pipeline starts.
/// Pipeline modules must not read raw config structures or CLI flags
/// once `build_plan` completes.
#[derive(Debug, Clone)]
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

/// Extraction strategy and all parameters needed to execute it.
#[derive(Debug, Clone)]
pub enum ExtractionStrategy {
    Snapshot,
    Incremental {
        cursor_column: String,
    },
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
            ExtractionStrategy::Incremental { .. } => "incremental",
            ExtractionStrategy::Chunked(_) => "chunked",
            ExtractionStrategy::TimeWindow { .. } => "timewindow",
        }
    }
}

/// Parameters for chunked extraction, pre-resolved from config and tuning.
#[derive(Debug, Clone)]
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
            let cursor_column = export.cursor_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': incremental mode requires 'cursor_column'",
                    export.name
                )
            })?;
            ExtractionStrategy::Incremental { cursor_column }
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
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
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
            ExtractionStrategy::Incremental { cursor_column } => {
                assert_eq!(cursor_column, "updated_at");
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
}
