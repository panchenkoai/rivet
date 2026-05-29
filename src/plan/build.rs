//! Build a [`ResolvedRunPlan`](super::contract::ResolvedRunPlan) from config and CLI flags.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig, ExportMode};
use crate::error::Result;
use crate::tuning::{SourceTuning, TuningProfile, merge_tuning_config};

use super::contract::{
    ChunkedPlan, ExtractionStrategy, IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan,
};

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
    // When no `tuning.profile:` is set, fall back to the env-derived default —
    // `Local` shaves the throttle, `Production` keeps the safety contract.
    let fallback_profile = config
        .source
        .environment
        .map(|e| e.default_profile())
        .unwrap_or(TuningProfile::Balanced);
    let tuning = SourceTuning::from_config_with_default_profile(merged.as_ref(), fallback_profile);
    let profile_label = |p: TuningProfile| match p {
        TuningProfile::Fast => "fast",
        TuningProfile::Balanced => "balanced",
        TuningProfile::Safe => "safe",
    };
    let env_label = |e: crate::config::SourceEnvironment| match e {
        crate::config::SourceEnvironment::Local => "local",
        crate::config::SourceEnvironment::Replica => "replica",
        crate::config::SourceEnvironment::Production => "production",
    };
    let tuning_profile_label = match merged.as_ref().and_then(|t| t.profile) {
        Some(p) => profile_label(p).to_string(),
        None => match config.source.environment {
            Some(env) => format!(
                "{} (default for environment: {})",
                profile_label(fallback_profile),
                env_label(env)
            ),
            None => "balanced (default)".to_string(),
        },
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
        ExportMode::Chunked => resolve_chunked_strategy(config, export, &tuning)?,
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

    let (compression, compression_level) = export.effective_compression();
    Ok(ResolvedRunPlan {
        export_name: export.name.clone(),
        base_query,
        strategy,
        format: export.format,
        compression,
        compression_level,
        max_file_size_bytes: export.max_file_size_bytes(),
        skip_empty: export.skip_empty,
        meta_columns: export.meta_columns.clone(),
        destination: expand_destination_templates(export.destination.clone(), &export.name),
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
        parquet: export.parquet.clone(),
    })
}

/// Resolve the strategy for `mode: chunked`.
///
/// Encapsulates three plan-time decisions that all share the same Postgres
/// introspection round-trip:
///
/// 1. **chunk_column** — explicit `chunk_column:` wins; otherwise the `table:`
///    shortcut on a Postgres source triggers auto-detection of a single
///    integer-family PK via
///    [`crate::source::postgres::introspect_pg_table_for_chunking`].
/// 2. **chunk_size** — explicit `chunk_size:` wins; if `chunk_size_memory_mb:`
///    is set, derive the row count from `<budget_mb> / avg_row_bytes`,
///    clamped to `[10_000, 5_000_000]`. Falls back to the YAML-default
///    `chunk_size` (100k) when neither is helpful.
/// 3. **small-table escape** — if the planner's `reltuples` estimate is
///    below the resolved `chunk_size`, downgrade `Chunked → Snapshot` since
///    a single chunk would offer no benefit and adds ~10 ms of plumbing
///    overhead per export. The downgrade is informational (log::info!) and
///    safe (a `Snapshot` writes one file like `mode: full`).
///
/// Steps 1–3 each only fire on PG with `table:` shortcut; everything else
/// falls back to the original "explicit column required" path.
fn resolve_chunked_strategy(
    config: &Config,
    export: &ExportConfig,
    tuning: &SourceTuning,
) -> Result<ExtractionStrategy> {
    // chunk_count / chunk_dense / chunk_by_days mutual-exclusion is shared
    // between the introspected and non-introspected paths.
    if let Some(count) = export.chunk_count {
        if count == 0 {
            anyhow::bail!("export '{}': chunk_count must be >= 1 (got 0)", export.name);
        }
        if export.chunk_dense {
            anyhow::bail!(
                "export '{}': chunk_count and chunk_dense are mutually exclusive",
                export.name
            );
        }
        if export.chunk_by_days.is_some() {
            anyhow::bail!(
                "export '{}': chunk_count and chunk_by_days are mutually exclusive",
                export.name
            );
        }
    }

    // Keyset (OPT-4) is its own shape — incompatible with the range / dense /
    // date / count knobs. Validated up front so the conflict is reported
    // regardless of which downstream path runs.
    if export.chunk_by_key.is_some() {
        for (conflict, name) in [
            (export.chunk_column.is_some(), "chunk_column"),
            (export.chunk_dense, "chunk_dense"),
            (export.chunk_by_days.is_some(), "chunk_by_days"),
            (export.chunk_count.is_some(), "chunk_count"),
        ] {
            if conflict {
                anyhow::bail!(
                    "export '{}': chunk_by_key and {} are mutually exclusive",
                    export.name,
                    name
                );
            }
        }
    }

    let max_attempts = export
        .chunk_max_attempts
        .unwrap_or_else(|| tuning.max_retries.saturating_add(1).max(1));

    // Fast path: explicit column AND no memory-budget knob → no DB probe at all.
    // Preserves the no-network plan-build invariant for users who hand-tune
    // chunked-mode the old way.
    if export.chunk_column.is_some() && export.chunk_size_memory_mb.is_none() {
        return Ok(ExtractionStrategy::Chunked(ChunkedPlan {
            column: export.chunk_column.clone().unwrap(),
            chunk_size: export.chunk_size,
            chunk_count: export.chunk_count,
            parallel: export.parallel,
            dense: export.chunk_dense,
            by_days: export.chunk_by_days,
            checkpoint: export.chunk_checkpoint,
            max_attempts,
        }));
    }

    // Anything beyond the fast path requires the `table:` shortcut so we have
    // a known relation to probe in either Postgres or MySQL.
    let Some(tbl) = export.table.as_ref() else {
        if export.chunk_by_key.is_some() {
            anyhow::bail!(
                "export '{}': `chunk_by_key:` needs the `table:` shortcut so the planner can \
                 verify the key is backed by a unique index (an unindexed ORDER BY key would \
                 filesort the whole table). Set `table:` instead of `query:`.",
                export.name
            );
        }
        if export.chunk_size_memory_mb.is_some() {
            anyhow::bail!(
                "export '{}': `chunk_size_memory_mb:` only applies with the `table:` shortcut — \
                 set `table:` (preferred) or remove `chunk_size_memory_mb:` and keep an explicit `chunk_size:`",
                export.name
            );
        }
        anyhow::bail!(
            "export '{}': chunked mode requires 'chunk_column' \
             (auto-resolve from PK is only supported with the `table:` shortcut)",
            export.name
        );
    };

    let url = config.source.resolve_url().map_err(|e| {
        anyhow::anyhow!(
            "export '{}': chunked mode needs the source URL for the introspection probe: {e}",
            export.name
        )
    })?;
    let introspection = match config.source.source_type {
        crate::config::SourceType::Postgres => {
            crate::source::postgres::introspect_pg_table_for_chunking(
                &url,
                config.source.tls.as_ref(),
                tbl,
            )
        }
        crate::config::SourceType::Mysql => {
            crate::source::mysql::introspect_mysql_table_for_chunking(
                &url,
                config.source.tls.as_ref(),
                tbl,
            )
        }
    }
    .map_err(|e| {
        anyhow::anyhow!(
            "export '{}': chunked-mode introspection probe failed: {e}. \
             Set `chunk_column:` (and `chunk_size:`) explicitly or check connectivity.",
            export.name
        )
    })?;

    // (1) Resolve chunk_size — explicit overrides the budget; otherwise compute
    // from the memory budget and the planner's row-width estimate. Shared by the
    // range-chunked and keyset paths.
    let chunk_size = if let Some(mb) = export.chunk_size_memory_mb {
        let row_bytes = introspection.avg_row_bytes.filter(|b| *b > 0).unwrap_or_else(|| {
            // Empty / un-analyzed table — fall back to a defensive 512 B so the
            // computed chunk_size lands near the YAML default of 100k for a
            // typical narrow table. The user can still override.
            log::warn!(
                "export '{}': chunk_size_memory_mb set but {} has no pg_class stats yet (run ANALYZE?) — \
                 defaulting to 512 B/row for sizing",
                export.name,
                tbl
            );
            512
        });
        let computed = (mb as i64 * 1024 * 1024 / row_bytes).max(1);
        let clamped = computed.clamp(10_000, 5_000_000) as usize;
        log::info!(
            "export '{}': chunk_size_memory_mb={} MB ÷ {} B/row ≈ {} rows (clamped to {})",
            export.name,
            mb,
            row_bytes,
            computed,
            clamped
        );
        clamped
    } else {
        export.chunk_size
    };

    // (2) Small-table escape: a single chunk/page has no benefit and adds
    // plumbing latency. Downgrade to Snapshot (bounded + simplest). Only fires
    // when reltuples is a meaningful positive number — un-analyzed tables
    // (reltuples=0) keep the chunked/keyset plan to preserve existing semantics.
    if introspection.row_estimate > 0 && (introspection.row_estimate as usize) <= chunk_size {
        log::info!(
            "export '{}': {} has ~{} rows ≤ chunk_size {}; downgrading chunked → snapshot",
            export.name,
            tbl,
            introspection.row_estimate,
            chunk_size,
        );
        return Ok(ExtractionStrategy::Snapshot);
    }

    // (3) Explicit keyset key (OPT-4): page by a single index-backed unique key.
    // Validate it is a usable keyset key (single-column, NOT NULL, UNIQUE/PK);
    // refuse otherwise rather than emit a full-scan + filesort `ORDER BY` query.
    if let Some(key) = export.chunk_by_key.as_deref() {
        if !introspection.is_usable_keyset_key(key) {
            anyhow::bail!(
                "export '{}': chunk_by_key '{}' is not a usable keyset key on {} — it must be a \
                 single-column, NOT NULL, UNIQUE or PRIMARY key. Without a unique index, \
                 `ORDER BY {} LIMIT n` would full-scan + filesort the table. Add a unique index \
                 on it, pick another key, or use `mode: full` to accept one long snapshot query.",
                export.name,
                key,
                tbl,
                key
            );
        }
        log::info!(
            "export '{}': keyset (seek) pagination on '{}' (chunk_size={})",
            export.name,
            key,
            chunk_size
        );
        return Ok(ExtractionStrategy::Keyset(KeysetPlan {
            key_column: key.to_string(),
            chunk_size,
        }));
    }

    // (4) Resolve chunk_column for range chunking, with an auto-keyset fallback
    // on MySQL when there is no single-integer PK but a usable unique key exists.
    let column = if let Some(c) = export.chunk_column.clone() {
        c
    } else {
        match introspection.single_int_pk.clone() {
            Some(col) => {
                // `info!` was below the default `warn` log level — operators
                // never saw which column the planner had picked, so a config
                // that intended `mode: chunked` + `chunk_column: created_at`
                // but typo'd the column name silently fell back to PK with
                // no signal. Elevate to `warn!` so the implicit choice is
                // visible in every run; tell the operator how to silence it.
                log::warn!(
                    "export '{}': chunk_column not set — auto-resolved to '{}' \
                     from the single-integer primary key on {}. \
                     Set `chunk_column:` explicitly to pin the choice and silence this warning.",
                    export.name,
                    col,
                    tbl
                );
                col
            }
            None => {
                // MySQL has no server-side cursor, so a non-int-PK table has no
                // safe range-chunk shape. If a single-column unique key exists,
                // page it with keyset instead of refusing (OPT-4). PG keeps
                // refusing — its `DECLARE CURSOR` snapshot is already bounded, so
                // `mode: full` is the safe answer there.
                if config.source.source_type == crate::config::SourceType::Mysql
                    && let Some(key) = introspection.auto_keyset_key()
                {
                    log::warn!(
                        "export '{}': {} has no single-integer PK — auto-selected keyset \
                         (seek) pagination on unique key '{}'. Set `chunk_by_key:` explicitly \
                         to pin the choice and silence this warning.",
                        export.name,
                        tbl,
                        key
                    );
                    return Ok(ExtractionStrategy::Keyset(KeysetPlan {
                        key_column: key.to_string(),
                        chunk_size,
                    }));
                }
                anyhow::bail!(
                    "export '{}': chunked mode found no safe shape on {} — no single-integer PK \
                     to range-chunk and no single-column UNIQUE/PRIMARY key to keyset-page. \
                     Set `chunk_column:` (integer) or `chunk_by_key:` (unique key) explicitly, \
                     add a unique index, or use `mode: full` to accept one long snapshot query.",
                    export.name,
                    tbl
                );
            }
        }
    };

    Ok(ExtractionStrategy::Chunked(ChunkedPlan {
        column,
        chunk_size,
        chunk_count: export.chunk_count,
        parallel: export.parallel,
        dense: export.chunk_dense,
        by_days: export.chunk_by_days,
        checkpoint: export.chunk_checkpoint,
        max_attempts,
    }))
}

/// Public re-export for callers outside `plan` (e.g. `preflight::type_report`).
pub fn parse_column_overrides_pub(
    raw: &std::collections::HashMap<String, String>,
    export_name: &str,
) -> Result<crate::types::ColumnOverrides> {
    parse_column_overrides(raw, export_name)
}

/// Parse the raw `columns:` map from `ExportConfig` into typed [`ColumnOverrides`].
///
/// Fails early (at plan-build time) with an actionable error so the user
/// fixes their `rivet.yaml` before the export runs.
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

/// Substitute placeholders in `destination.path` and `destination.prefix`.
///
/// Thin wrapper around [`crate::destination::placeholder::expand_destination`]
/// anchored at today's UTC date.  Kept here so existing call sites
/// (`run`, the in-process plan builder) keep their stable signature; callers
/// that need to re-target an earlier run's prefix (e.g. `validate --date`,
/// `validate --run-id`) should construct a `PlaceholderContext` directly
/// and call [`crate::destination::placeholder::expand_destination`].
pub(crate) fn expand_destination_templates(
    dest: crate::config::DestinationConfig,
    export_name: &str,
) -> crate::config::DestinationConfig {
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(export_name);
    crate::destination::placeholder::expand_destination(dest, &ctx)
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
            environment: None,
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
            table: None,
            mode: ExportMode::Full,
            cursor_column: None,
            cursor_fallback_column: None,
            incremental_cursor_mode: IncrementalCursorMode::SingleColumn,
            chunk_column: None,
            chunk_size: 100_000,
            chunk_size_memory_mb: None,
            chunk_count: None,
            chunk_dense: false,
            chunk_by_days: None,
            chunk_by_key: None,
            parallel: 1,
            time_column: None,
            time_column_type: TimeColumnType::Timestamp,
            days_window: None,
            format: FormatType::Parquet,
            compression: CompressionType::Zstd,
            compression_level: None,
            compression_profile: None,
            skip_empty: false,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
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
            parquet: None,
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

    // ── keyset (OPT-4) planner gates (no DB) ──────────────────────────────

    #[test]
    fn chunk_by_key_conflicts_with_chunk_column() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_by_key = Some("uuid".into());
        export.chunk_column = Some("id".into());
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("mutually exclusive"), "got: {msg}");
    }

    #[test]
    fn chunk_by_key_without_table_requires_table_shortcut() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_by_key = Some("uuid".into());
        export.table = None;
        export.chunk_column = None;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("table:") && msg.contains("chunk_by_key"),
            "should require the table shortcut to verify the index, got: {msg}"
        );
    }

    // ── auto-resolve chunk_column friendly errors (no DB) ─────────────────

    #[test]
    fn chunked_without_column_or_table_returns_explicit_error() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = None;
        export.table = None; // query: form, no auto-resolve possible
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("chunked mode requires 'chunk_column'")
                && msg.contains("`table:` shortcut"),
            "expected actionable error about table: shortcut; got: {msg}"
        );
    }

    #[test]
    fn chunked_explicit_column_skips_auto_resolve_and_no_db_call() {
        // Sanity: an explicit chunk_column means we never even try to open a
        // connection — the resolver short-circuits on the explicit value.
        // (Otherwise this test would have to hit a real Postgres.)
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("explicit_pk".into());
        export.table = Some("public.something".into());
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .expect("explicit chunk_column must short-circuit auto-resolve");
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => assert_eq!(cp.column, "explicit_pk"),
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn chunked_size_memory_mb_without_table_errors_with_hint() {
        // `chunk_size_memory_mb:` needs `table:` for the row-size probe.
        // Without it we should bail with a message that tells the user how
        // to fix the config, instead of silently falling back to chunk_size.
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size_memory_mb = Some(256);
        // No table: shortcut.
        export.table = None;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("chunk_size_memory_mb") && msg.contains("`table:` shortcut"),
            "expected actionable error mentioning chunk_size_memory_mb + table:; got: {msg}"
        );
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

    #[test]
    fn expand_destination_templates_substitutes_all_placeholders() {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let dest = DestinationConfig {
            destination_type: DestinationType::Local,
            prefix: Some("exports/{date}/{export}/".into()),
            path: Some("/data/{table}/{date}".into()),
            ..Default::default()
        };
        let expanded = expand_destination_templates(dest, "orders");
        assert_eq!(
            expanded.path.as_deref(),
            Some(format!("/data/orders/{today}").as_str())
        );
        assert_eq!(
            expanded.prefix.as_deref(),
            Some(format!("exports/{today}/orders/").as_str())
        );
    }

    // ── chunk_count validation ────────────────────────────────────────────────

    fn chunked_export() -> ExportConfig {
        let mut e = minimal_export();
        e.mode = ExportMode::Chunked;
        e.chunk_column = Some("id".into());
        e
    }

    #[test]
    fn chunk_count_zero_is_rejected() {
        let mut export = chunked_export();
        export.chunk_count = Some(0);
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("chunk_count") && err.to_string().contains("1"),
            "expected 'chunk_count must be >= 1', got: {err}"
        );
    }

    #[test]
    fn chunk_count_with_chunk_dense_is_rejected() {
        let mut export = chunked_export();
        export.chunk_count = Some(10);
        export.chunk_dense = true;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected 'mutually exclusive', got: {err}"
        );
    }

    #[test]
    fn chunk_count_with_chunk_by_days_is_rejected() {
        let mut export = chunked_export();
        export.chunk_count = Some(10);
        export.chunk_by_days = Some(7);
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected 'mutually exclusive', got: {err}"
        );
    }

    #[test]
    fn chunk_count_valid_threads_through_to_plan() {
        let mut export = chunked_export();
        export.chunk_count = Some(5);
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
            ExtractionStrategy::Chunked(cp) => assert_eq!(cp.chunk_count, Some(5)),
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn chunk_count_none_is_accepted_with_dense() {
        let mut export = chunked_export();
        export.chunk_count = None;
        export.chunk_dense = true;
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
            ExtractionStrategy::Chunked(cp) => {
                assert!(cp.dense);
                assert!(cp.chunk_count.is_none());
            }
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn expand_destination_templates_no_placeholders_unchanged() {
        let dest = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("./out".into()),
            ..Default::default()
        };
        let expanded = expand_destination_templates(dest, "orders");
        assert_eq!(expanded.path.as_deref(), Some("./out"));
        assert!(expanded.prefix.is_none());
    }

    #[test]
    fn expand_destination_templates_is_idempotent_on_already_expanded_strings() {
        // Regression guard for the validate/doctor wiring (2026-05-21):
        // both commands now call `expand_destination_templates` on the same
        // config `run` already expanded for.  Calling twice must NOT mangle
        // a string that has no remaining placeholders.
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let once = expand_destination_templates(
            DestinationConfig {
                destination_type: DestinationType::Local,
                prefix: Some("runs/{date}/{export}/".into()),
                ..Default::default()
            },
            "orders",
        );
        let twice = expand_destination_templates(once.clone(), "orders");
        assert_eq!(once.prefix, twice.prefix);
        assert_eq!(
            once.prefix.as_deref(),
            Some(format!("runs/{today}/orders/").as_str())
        );
    }
}
