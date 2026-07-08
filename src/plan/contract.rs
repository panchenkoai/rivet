//! Resolved run contract: strategies, plan struct, and time-window SQL.

use serde::{Deserialize, Serialize};

use crate::config::{
    CompressionType, DestinationConfig, FormatType, IncrementalCursorMode, MetaColumns,
    ParquetConfig, QualityConfig, SchemaDriftPolicy, SourceConfig, TimeColumnType,
};
use crate::tuning::SourceTuning;

/// Parameters for chunked extraction, pre-resolved from config and tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkedPlan {
    pub column: String,
    pub chunk_size: usize,
    /// Divide the column range into exactly this many equal chunks.
    /// When Some, `chunk_size` is recomputed at detect time from min/max.
    pub chunk_count: Option<usize>,
    pub parallel: usize,
    pub dense: bool,
    pub by_days: Option<u32>,
    pub checkpoint: bool,
    /// Resolved from `chunk_max_attempts` or `tuning.max_retries + 1`.
    pub max_attempts: u32,
}

/// Parameters for keyset (seek) pagination — the source-safe shape for tables
/// without a single-integer PK (OPT-4). Pages the table by one index-backed,
/// NOT NULL unique key with `WHERE key > last ORDER BY key LIMIT chunk_size`,
/// bounding both peak RSS and longest-query time. The key is index-backed by
/// construction (see `plan::build`), so the `ORDER BY` is an index range scan,
/// never a filesort.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysetPlan {
    pub key_column: String,
    pub chunk_size: usize,
    /// Persist the page's max key after each commit and resume from it next run
    /// (crash-recovery / incremental-append). Opt-in — default `false` keeps
    /// `mode: full` re-reading the whole key range every run (full semantics).
    /// Set for a MongoDB source via `source.mongo.resume`. `#[serde(default)]`
    /// so a pre-existing plan artifact (no field) deserializes as non-resumable.
    #[serde(default)]
    pub checkpoint: bool,
}

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
    /// Integrity depth `--validate` must reach for this export's parts
    /// (`exports[].verify`).  `Content` makes size-only parts a verification
    /// failure.
    pub verify: crate::config::VerifyMode,
    /// Source connection parameters — resolved from config at plan time so pipeline
    /// functions receive the complete execution contract in a single struct.
    pub source: SourceConfig,
    /// Per-column type overrides parsed from `exports[].columns:` in `rivet.yaml`
    /// (roadmap §8). Passed to the source driver so it can use declared
    /// precision/scale instead of autodetected (often unavailable) metadata.
    pub column_overrides: crate::types::ColumnOverrides,
    /// What to do when structural schema drift is detected (Epic 7).
    pub schema_drift_policy: SchemaDriftPolicy,
    /// Growth-factor threshold for data shape drift warnings (Epic 8).
    /// Warn when a column's current-run max byte length exceeds `stored × factor`.
    /// 0.0 disables shape tracking.
    pub shape_drift_warn_factor: f64,
    /// Parquet row group tuning (resolved from export config). `None` = library default.
    pub parquet: Option<ParquetConfig>,
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
    Keyset(KeysetPlan),
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
            ExtractionStrategy::Keyset(_) => "keyset",
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
            || matches!(self, ExtractionStrategy::Keyset(kp) if kp.checkpoint)
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
            // Keyset pages by this key; the sink tracks its per-page max so the
            // runner can advance to the next page (OPT-4).
            ExtractionStrategy::Keyset(k) => Some(k.key_column.as_str()),
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
            ExtractionStrategy::Chunked(_) | ExtractionStrategy::Keyset(_) => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;

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
            chunk_count: None,
            parallel: 1,
            dense: false,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        });
        assert!(!s.needs_cursor_state());
        assert!(!s.is_resumable());
        assert!(s.cursor_column().is_none());
        assert!(s.resolve_query("SELECT 1", SourceType::Postgres).is_none());
    }

    #[test]
    fn chunked_with_checkpoint_is_resumable() {
        let s = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 10_000,
            chunk_count: None,
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
        assert!(q.contains("\"ts\" >= "), "got: {}", q);
        assert!(
            !q.contains("'"),
            "unix should not have value quotes, got: {}",
            q
        );
    }
}
