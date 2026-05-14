//! Build a [`ResolvedRunPlan`](super::contract::ResolvedRunPlan) from config and CLI flags.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig, ExportMode};
use crate::error::Result;
use crate::tuning::{SourceTuning, TuningProfile, merge_tuning_config};

use super::contract::{ChunkedPlan, ExtractionStrategy, IncrementalCursorPlan, ResolvedRunPlan};

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
                chunk_count: export.chunk_count,
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
        column_overrides: parse_column_overrides(&export.columns, &export.name)?,
        schema_drift_policy: export.on_schema_drift,
        shape_drift_warn_factor: export.shape_drift_warn_factor.unwrap_or(2.0),
    })
}

/// Parse the raw `columns:` map from `ExportConfig` into typed [`ColumnOverrides`].
///
/// Fails early (at plan-build time) with an actionable error so the user
/// fixes their `rivet.yaml` before the export runs.
/// Public re-export for callers outside `plan` (e.g. `preflight::type_report`).
pub fn parse_column_overrides_pub(
    raw: &std::collections::HashMap<String, String>,
    export_name: &str,
) -> Result<crate::types::ColumnOverrides> {
    parse_column_overrides(raw, export_name)
}

fn parse_column_overrides(
    raw: &std::collections::HashMap<String, String>,
    export_name: &str,
) -> Result<crate::types::ColumnOverrides> {
    raw.iter()
        .map(|(col, type_str)| {
            crate::types::parse_type_str(type_str)
                .map(|t| (col.clone(), t))
                .map_err(|e| {
                    anyhow::anyhow!(
                        "export '{}': column override for '{}': {}",
                        export_name,
                        col,
                        e
                    )
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, IncrementalCursorMode,
        MetaColumns, SourceConfig, SourceType, TimeColumnType,
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
            chunk_count: None,
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
            columns: Default::default(),
            on_schema_drift: Default::default(),
            shape_drift_warn_factor: None,
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
