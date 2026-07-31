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
    /// Persist the page's max key after each commit so a **crashed** run resumes
    /// from it (crash-recovery). Detected via the in-progress run_id: a run that
    /// finished clears it, so a *clean* re-run does NOT continue from the key —
    /// it re-reads the whole range (full semantics), never silently skipping
    /// already-exported rows. The clean-re-run "continue from key" behaviour is
    /// the separate opt-in [`incremental`](Self::incremental). Set for a MongoDB
    /// source via `source.mongo.resume`. `#[serde(default)]` so a pre-existing
    /// plan artifact (no field) deserializes as non-resumable.
    #[serde(default)]
    pub checkpoint: bool,
    /// Append-only opt-in (SQL `keyset_incremental`, or Mongo `source.mongo.resume`):
    /// on a CLEAN re-run, continue from the last exported key — pull only keys past
    /// the high-water mark. Distinct from [`checkpoint`](Self::checkpoint) (which is
    /// crash-recovery only): incremental is correct ONLY for append-only tables, so
    /// it is off unless explicitly requested. `#[serde(default)]` for old artifacts.
    #[serde(default)]
    pub incremental: bool,
    /// Concurrent `_id`-range workers for a MongoDB parallel read (`export.
    /// parallel`): each worker keyset-pages a disjoint `$sample`-bounded slice.
    /// Only the Mongo reader acts on it; SQL keyset stays sequential and ignores
    /// it. `#[serde(default)]` ⇒ absent/0 reads with a single cursor (`.max(1)`).
    #[serde(default)]
    pub parallel: usize,
}

/// Fully resolved execution plan for a single export.
///
/// All execution decisions are derived before the pipeline starts.
/// Pipeline modules must not read raw config structures or CLI flags
/// once `build_plan` completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedRunPlan {
    pub export_name: String,
    /// The export FAMILY this run belongs to — the parent export for a CDC
    /// snapshot leg, the export's own name otherwise. Recorded into the manifest
    /// so consumers that group by export never re-derive it from the name (see
    /// `RunManifest::export_family`).
    ///
    /// `#[serde(default)]` because this type is SEALED INTO `plan.json`: a
    /// required field here rejects every artifact a user planned with an older
    /// rivet at `apply` time. That exact break shipped once (`verify`, added
    /// required, silently rejecting two months of artifacts) — the frozen-plan
    /// compat test caught this one before commit. An empty family falls back to
    /// the export name at the writers, matching a pre-field plan's behaviour.
    #[serde(default)]
    pub export_family: String,
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
    ///
    /// `#[serde(default)]` for old artifacts, like its siblings above: this
    /// struct is SERIALIZED into a plan artifact, and `rivet apply` must keep
    /// accepting one written by an older rivet. Added as a required field, it
    /// made every pre-existing plan unparseable — `missing field `verify`` —
    /// which is exactly what the frozen v0.7.5 fixture exists to catch.
    /// `VerifyMode::Size` is the pre-field behaviour (size-only accepted), so
    /// defaulting reproduces what those artifacts meant.
    #[serde(default)]
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

    /// The key column that drives chunking: the range/dense `chunk_column`, or the
    /// keyset `chunk_by_key`. `None` for snapshot/incremental/time-window. Recorded
    /// per run so a post-mortem knows WHICH column was chunked (the state DB
    /// otherwise keeps only the chunk *values*, never the column name).
    pub fn chunk_key(&self) -> Option<&str> {
        match self {
            ExtractionStrategy::Chunked(cp) => Some(cp.column.as_str()),
            ExtractionStrategy::Keyset(kp) => Some(kp.key_column.as_str()),
            _ => None,
        }
    }

    /// Primary cursor column name for incremental exports (`None` for other strategies).
    pub fn cursor_column(&self) -> Option<&str> {
        match self {
            ExtractionStrategy::Incremental(p) => Some(p.primary_column.as_str()),
            _ => None,
        }
    }

    /// Why `--reconcile` must SKIP the full-source `COUNT(*)` check for this
    /// strategy, or `None` when the strategy is a complete snapshot the count
    /// should match. Reconcile means "did the export capture everything the source
    /// has", which is only meaningful for a full pass (full / chunked / plain
    /// keyset). Every SUBSET/DELTA strategy — incremental cursor, keyset_incremental
    /// (append-only, pulls keys past the high-water mark), time_window (a bounded
    /// window) — legitimately exports fewer rows than the table holds, so the count
    /// mismatch is STRUCTURAL, not data loss. The exit gate must not fail-loud on
    /// it (else every healthy scheduled delta `run --reconcile` exits 3 and stops
    /// the chain).
    pub fn reconcile_subset_skip(&self) -> Option<&'static str> {
        match self {
            ExtractionStrategy::Incremental(_) => {
                Some("incremental cursor — pulls only rows past the cursor")
            }
            ExtractionStrategy::Keyset(kp) if kp.incremental => {
                Some("keyset_incremental — pulls only keys past the high-water mark")
            }
            ExtractionStrategy::TimeWindow { .. } => {
                Some("time_window — pulls only the bounded window")
            }
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
    fn reconcile_subset_skip_covers_every_delta_strategy() {
        // #bughunt HIGH: the #102 reconcile exit gate turned a STRUCTURAL count
        // mismatch into a false exit-3 for keyset_incremental + time_window (only
        // the Incremental cursor was skipped). Every subset/delta strategy must
        // skip the full-source COUNT(*); a full pass must NOT.
        let keyset = |incremental| {
            ExtractionStrategy::Keyset(KeysetPlan {
                key_column: "id".into(),
                chunk_size: 1000,
                checkpoint: true,
                incremental,
                parallel: 1,
            })
        };
        assert!(
            keyset(true).reconcile_subset_skip().is_some(),
            "keyset_incremental pulls only new keys — reconcile must skip"
        );
        assert!(
            keyset(false).reconcile_subset_skip().is_none(),
            "plain keyset is a FULL pass — reconcile applies"
        );
        assert!(
            ExtractionStrategy::TimeWindow {
                column: "d".into(),
                column_type: TimeColumnType::Timestamp,
                days_window: 7,
            }
            .reconcile_subset_skip()
            .is_some()
        );
        assert!(
            ExtractionStrategy::Incremental(IncrementalCursorPlan {
                primary_column: "updated_at".into(),
                fallback_column: None,
                mode: IncrementalCursorMode::SingleColumn,
            })
            .reconcile_subset_skip()
            .is_some()
        );
        assert!(
            ExtractionStrategy::Snapshot
                .reconcile_subset_skip()
                .is_none(),
            "a full snapshot must be reconciled"
        );
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
