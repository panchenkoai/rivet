//! Per-export configuration: query/table/mode, chunking, format, destination link.
//!
//! `SchemaDriftPolicy` lives here because it is only ever read via
//! [`ExportConfig::on_schema_drift`].

use std::path::Path;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::IncrementalCursorMode;
use super::destination::DestinationConfig;
use super::format::{CompressionProfile, CompressionType, FormatType, ParquetConfig};
use super::resolve::{parse_file_size, resolve_vars};
use crate::tuning::TuningConfig;

/// What to do when structural schema drift is detected (column added, removed, or retyped).
///
/// ```yaml
/// exports:
///   - name: orders
///     on_schema_drift: fail   # warn (default), continue, fail
/// ```
/// How deep `--validate` must verify each part's integrity.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum VerifyMode {
    /// Accept size-only verification when no content checksum is available.
    #[default]
    Size,
    /// Require every part's content to be MD5-verified against the store's
    /// listing; fail validation for any part that is only size-verified.
    Content,
}

impl VerifyMode {
    /// Whether content (not just size) verification is required.
    pub fn requires_content(self) -> bool {
        matches!(self, VerifyMode::Content)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDriftPolicy {
    /// Log a warning and continue. The new schema fingerprint is stored. (Default.)
    #[default]
    Warn,
    /// Silently accept schema changes — store the new schema, no log output.
    Continue,
    /// Abort the run with a non-zero exit. The schema store is NOT updated so the
    /// next run will detect the same change again.
    Fail,
}
/// The synthesis marker a range sub-export carries under `apply --pool --split`
/// (#167). Never deserialized from YAML — set only by the pool's synthesizer, so
/// it needs no `JsonSchema`/`Deserialize`. The key window is HALF-OPEN `(lo, hi]`
/// so N adjacent windows partition the key gap-free with no overlap (the same
/// convention `keyset::partition_ranges` uses): `lo = None` is the run's floor
/// (first window), `hi = None` is the ceil (last window).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitSynth {
    /// The parent export's FAMILY — every sub-export folds to it so the load view
    /// recombines them as one table.
    pub parent: String,
    /// The key column the window is expressed over (the export's `chunk_by_key` /
    /// `chunk_column`).
    pub key_column: String,
    /// Exclusive lower bound (`key > lo`); `None` for the first window (no floor).
    pub lo: Option<String>,
    /// Inclusive upper bound (`key <= hi`); `None` for the last window (no ceil).
    pub hi: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    pub name: String,
    /// Set ONLY on the CDC snapshot leg `cdc_job` synthesizes: the parent
    /// export's name, recorded by the code that builds the leg rather than
    /// re-derived from the `{parent}__snapshot_{table}` name downstream.
    ///
    /// Deriving it was wrong in both directions. Folding the name at the writer
    /// makes a USER export literally named `daily__snapshot_v2` record family
    /// `daily` — silently merging it with a real sibling export `daily` under a
    /// shared prefix, which is exactly the cross-contamination the load guard
    /// exists to refuse. And a CDC export whose own name contains the infix
    /// folds at the FIRST one, so its leg and drain land in different families
    /// and the documented `initial: snapshot` flow self-refuses again.
    ///
    /// `skip` (not `skip_serializing_if`): this is an internal synthesis marker,
    /// never a user-writable key, so it must not appear in the schema or the
    /// config reference.
    #[serde(skip)]
    pub snapshot_parent: Option<String>,
    /// Set ONLY on a range sub-export the pool synthesizes under `apply --pool
    /// --split` (#167): a single dominating export is emitted as N units over its
    /// key span, each a first-class scheduler unit the pool places concurrently,
    /// so the giant stops being the makespan floor. Carries the parent FAMILY and
    /// this unit's half-open key window `(lo, hi]`. `None` on every ordinary
    /// export.
    ///
    /// `skip` (not `skip_serializing_if`): an internal synthesis marker like
    /// [`snapshot_parent`](Self::snapshot_parent), never a user-writable key — it
    /// must not appear in the schema or the config reference.
    #[serde(skip)]
    pub split: Option<SplitSynth>,
    #[serde(default)]
    pub query: Option<String>,
    pub query_file: Option<String>,
    /// Shortcut for `query: "SELECT * FROM <schema>.<table>"`.
    ///
    /// Accepts `table` or `schema.table` with ASCII-only identifiers
    /// (`[A-Za-z_][A-Za-z0-9_]*`). Generates an unquoted single-table
    /// query so the Postgres NUMERIC catalog-hint resolver recognises it
    /// and auto-types `numeric(p,s)` columns without manual overrides.
    ///
    /// Mutually exclusive with `query` and `query_file`.
    #[serde(default)]
    pub table: Option<String>,
    /// CDC only: capture **several** tables through ONE change stream (one
    /// PostgreSQL slot / one MySQL binlog connection) instead of one export —
    /// and one slot — per table. Each table's parts land under
    /// `<destination>/<table>/` with their own `manifest.json` + `_SUCCESS`;
    /// the checkpoint (stream position) is shared. Mutually exclusive with
    /// `table:`. Not yet supported for SQL Server (capture instances are
    /// per-table).
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    #[serde(default = "default_mode")]
    pub mode: ExportMode,
    /// Change-data-capture settings, required when `mode: cdc`. Reuses the
    /// export's `table`, `destination`, and `format`; carries only the
    /// CDC-specific knobs (resume checkpoint, per-engine stream params).
    #[serde(default)]
    pub cdc: Option<CdcExportConfig>,
    pub cursor_column: Option<String>,
    /// Secondary column for [`IncrementalCursorMode::Coalesce`] only (see ADR-0007).
    #[serde(default)]
    pub cursor_fallback_column: Option<String>,
    /// How primary (and optional fallback) columns drive incremental progression.
    #[serde(default)]
    pub incremental_cursor_mode: IncrementalCursorMode,
    pub chunk_column: Option<String>,
    #[serde(default)]
    pub chunk_dense: bool,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    /// Target memory budget per chunk in MB. When set, `chunk_size` is derived
    /// from this budget at plan-build time using the engine's row-size estimate
    /// (PostgreSQL `pg_relation_size / reltuples`; MySQL `information_schema`
    /// average row length; a defensive 512 B/row default when no estimate
    /// exists, e.g. SQL Server), clamped to `[10_000, 5_000_000]` rows.
    ///
    /// Mutually exclusive with an explicit non-default `chunk_size:`. Requires
    /// `mode: chunked` and the `table:` shortcut (the row-size probe needs a
    /// known relation); any SQL engine works.
    ///
    /// ```yaml
    /// exports:
    ///   - name: page_views
    ///     table: public.page_views
    ///     mode: chunked
    ///     chunk_size_memory_mb: 256
    /// ```
    #[serde(default)]
    pub chunk_size_memory_mb: Option<u64>,
    /// Divide the column range into exactly this many equal chunks.
    /// Mutually exclusive with `chunk_dense` and `chunk_by_days`.
    /// When set, `chunk_size` is computed dynamically from min/max.
    pub chunk_count: Option<usize>,
    pub chunk_by_days: Option<u32>,
    /// Keyset (seek) pagination on this single index-backed unique key — the
    /// source-safe shape for tables without a single-integer PK (OPT-4). The
    /// column MUST be backed by a usable index (PK or unique); the planner
    /// refuses a non-indexed key rather than emit a full-scan + filesort query.
    pub chunk_by_key: Option<String>,
    /// Concurrent chunk/page workers (default 1). On a RANGE chunk (`chunk_column`)
    /// or KEYSET (`chunk_by_key`) export, `parallel: N` fans the table into N
    /// ROW-percentile ranges that seek concurrently over separate connections — the
    /// half-open intervals partition the key, so the union reads every row exactly
    /// once (structural parity, all engines). Extraction is I/O-bound, so the win
    /// plateaus early (~3x at N=4, little beyond). SWEET SPOT: indexed tables up to
    /// ~10M rows at `parallel: 4`. `rivet init` scaffolds a row-scaled value
    /// (<=500K -> 1, <5M -> 2, >=5M -> 4); a preflight warns past ~5M rows (peak RSS
    /// ~= N x chunk_size). Beyond ~10M the KEYSET boundary sampler (an index OFFSET
    /// skip) grows costly at setup — prefer a range `chunk_column` there.
    #[serde(default = "default_parallel")]
    pub parallel: usize,

    /// Advisory execution wave (1 = highest priority, run first). Written by
    /// `rivet plan` from the source-aware prioritization score (see ADR-0006)
    /// and consumed by `rivet apply`, which runs exports wave-by-wave in
    /// ascending order. `None` = unscheduled (apply treats it as the last wave).
    /// Operators may hand-edit it; a later `rivet plan` refreshes it in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wave: Option<u32>,

    /// Whether this export is cheap enough to run concurrently with its
    /// wave-mates under `rivet apply --parallel-export-processes`. Written by
    /// `rivet plan` (true when the source-aware cost class is `Low`, i.e.
    /// < ~100K rows); a heavier table already chunk-parallelizes internally, so
    /// two of them at once would overload the source. `None`/`false` → the
    /// export runs alone within its wave. Operators may hand-edit it; a later
    /// `rivet plan` refreshes it in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_safe: Option<bool>,
    pub time_column: Option<String>,
    #[serde(default = "default_time_column_type")]
    pub time_column_type: TimeColumnType,
    pub days_window: Option<u32>,

    /// Date/time output partitioning: split this export's rows into one
    /// destination sub-prefix per calendar bucket of this **DATE or TIMESTAMP**
    /// column, bucketed by `partition_granularity`
    /// (`day` / `month` / `year`), in a Hive-style `col=value/` layout
    /// (`created_at=2023-01-01/`, `created_at=2023-01/`, `created_at=2023/`).
    /// Requires a `{partition}` token in `destination.path` /
    /// `destination.prefix`.
    ///
    /// This is **not** arbitrary value partitioning: the column's min/max is
    /// read and parsed as a date to generate contiguous calendar buckets, so a
    /// non-temporal column (e.g. `partition_by: status`) fails at run time with
    /// "could not parse partition min `<value>` from column `<col>` as a date".
    /// To split by a categorical column, write one export per value with a
    /// `WHERE` filter instead.
    ///
    /// Orthogonal to `mode`: each partition runs the export's own mode, so
    /// `mode: chunked` chunks *within* a day. Rows whose partition column is
    /// NULL land in `col=__HIVE_DEFAULT_PARTITION__/` (Hive default partition)
    /// so no row is silently dropped. Not compatible with `mode: time_window`.
    ///
    /// ```yaml
    /// exports:
    ///   - name: events
    ///     table: events
    ///     partition_by: created_at        # must be a DATE or TIMESTAMP column
    ///     partition_granularity: day
    ///     destination:
    ///       type: s3
    ///       bucket: my-bucket
    ///       prefix: "events/{partition}/"   # → events/created_at=2023-01-01/
    /// ```
    #[serde(default)]
    pub partition_by: Option<String>,

    /// Calendar bucket width for `partition_by`:
    /// `day` (default), `month`, or `year`. Determines how the partition
    /// column's date/timestamp range is split into contiguous Hive buckets
    /// (`col=2023-01-01/` / `col=2023-01/` / `col=2023/`). Has no effect
    /// unless `partition_by` is set.
    #[serde(default)]
    pub partition_granularity: PartitionGranularity,
    pub format: FormatType,
    #[serde(default)]
    pub compression: CompressionType,
    pub compression_level: Option<u32>,
    pub compression_profile: Option<CompressionProfile>,
    #[serde(default)]
    pub skip_empty: bool,
    pub destination: DestinationConfig,
    /// Integrity depth required of `--validate` for this export's parts.
    /// `size` (default) accepts size-only verification; `content` requires every
    /// part's content MD5 to be checked against the store's listing (no
    /// download) and **fails** validation for any part that could only be
    /// size-verified — e.g. a part too large to upload as a single PUT (lower
    /// `max_file_size` so it fits), or a backend that exposes no checksum.
    #[serde(default)]
    pub verify: VerifyMode,
    #[serde(default)]
    pub meta_columns: MetaColumns,
    #[serde(default)]
    pub quality: Option<QualityConfig>,
    /// Rotate to a new part when the current file reaches this size.
    /// Accepts `B`/`KB`/`MB`/`GB` (case-insensitive) or a bare byte count;
    /// a fractional value is allowed (`1.5GB`). Units are binary (IEC-style):
    /// `KB` = 1024 bytes, `MB` = 1024 KB, `GB` = 1024 MB. Example: `256MB`.
    pub max_file_size: Option<String>,
    /// Persist per-chunk / per-page progress so a **crashed** run resumes from the
    /// last durably committed point instead of re-reading from the start. This is
    /// pure crash-recovery: a *clean* re-run (the prior run finished) still does a
    /// full pass — it never silently skips already-exported rows. Safe to enable
    /// on any table; `rivet init` defaults it on for chunked and keyset exports.
    #[serde(default)]
    pub chunk_checkpoint: bool,
    /// Keyset only (`chunk_by_key`): on a **clean** re-run, continue from the last
    /// exported key — pull ONLY rows with a key past the high-water mark. This is
    /// incremental-by-key, correct ONLY for APPEND-ONLY tables (a mutable row whose
    /// key already passed is silently never re-read). Opt-in and off by default;
    /// crash-recovery does not need it (that is `chunk_checkpoint`). For a mutable
    /// table use `mode: incremental` on a timestamp cursor instead.
    #[serde(default)]
    pub keyset_incremental: bool,
    pub chunk_max_attempts: Option<u32>,
    #[serde(default)]
    pub tuning: Option<TuningConfig>,
    /// Optional logical group for shared source capacity (replica, host). Advisory prioritization only.
    #[serde(default)]
    pub source_group: Option<String>,
    /// Hint (Epic C / ADR-0006) that this export should always be treated as reconcile-heavy
    /// by planning, independent of the `--reconcile` CLI flag. Advisory only.
    #[serde(default)]
    pub reconcile_required: bool,

    /// Per-column type overrides (roadmap §8). Keys are column names; values
    /// are short type strings such as `decimal(18,2)`, `timestamp_tz`, `json`.
    ///
    /// ```yaml
    /// exports:
    ///   - name: payments
    ///     columns:
    ///       amount: decimal(18,2)
    ///       fee: decimal(18,6)
    ///       created_at: timestamp_tz
    /// ```
    ///
    /// Overrides take priority over autodetection and are validated at
    /// plan time — an invalid type string fails before the export runs.
    #[serde(default)]
    pub columns: std::collections::HashMap<String, String>,

    /// Downstream warehouse this export targets (`bigquery` / `bq`,
    /// `duckdb`). When set, `rivet check --type-report` resolves each column
    /// against it (native type, honest autoload type, recovery hint) without
    /// needing `--target` on the CLI — the CLI flag still wins when both are
    /// present. The Parquet interchange stays target-neutral (ADR-0014 T2);
    /// `target:` only drives guidance and the future load-schema artifact.
    ///
    /// ```yaml
    /// exports:
    ///   - name: payments
    ///     target: bigquery
    /// ```
    #[serde(default)]
    pub target: Option<String>,

    /// Per-export overrides for the top-level `load:` block (`pk`,
    /// `cleanup_source`, `gc_orphans`, `cluster_by`, `allow_source_drift`); any
    /// field omitted here inherits the top-level value. The warehouse `target`
    /// is shared and stays in the top-level `load:` — it cannot be overridden
    /// per export.
    ///
    /// ```yaml
    /// load: { target: bigquery, project: p, dataset: d }   # shared default
    /// exports:
    ///   - name: orders
    ///     table: orders
    ///     mode: cdc
    ///     load: { pk: [id] }                                # this table's pk
    /// ```
    ///
    /// Raw JSON (parsed by the load module) so `config` carries no load types —
    /// mirrors the top-level [`crate::config::Config::load`].
    #[serde(default)]
    pub load: Option<serde_json::Value>,

    /// Policy applied when structural schema drift is detected (column added, removed, or retyped).
    /// Defaults to `warn`: log a warning and continue.
    #[serde(default)]
    pub on_schema_drift: SchemaDriftPolicy,

    /// Growth-factor threshold for data shape drift warnings (Epic 8).
    /// When a string/binary column's max observed byte length in the current run
    /// exceeds `stored_max * shape_drift_warn_factor`, Rivet logs a warning.
    /// `None` uses the default of 2.0. Set to `0.0` to disable shape tracking.
    ///
    /// **Scope: single-batch exports only** (`mode: full` / `incremental`). Shape
    /// tracking needs the run-wide per-column max byte length, which only the
    /// single sink accumulates; the multi-part runners (chunked, keyset, and
    /// parallel-Mongo) each write through several sinks and do not aggregate it,
    /// so this factor is inert there. (Schema drift — added/dropped/retyped
    /// columns — IS enforced on every path via `on_schema_drift`.)
    #[serde(default)]
    pub shape_drift_warn_factor: Option<f64>,

    /// Parquet row group tuning. Only meaningful when `format: parquet`.
    /// When absent, the parquet library default (1,048,576 rows/group) is used.
    #[serde(default)]
    pub parquet: Option<ParquetConfig>,
}

impl ExportConfig {
    /// The export FAMILY this export's runs belong to: the RECORDED
    /// `snapshot_parent` for the CDC snapshot leg `cdc_job` synthesizes, the
    /// parent family for a range sub-export the pool `--split` synthesizes, the
    /// export's own name for everything else.
    ///
    /// The single place this is decided. It used to be re-derived at each
    /// writer by folding the name on `__snapshot_`, which merged a user export
    /// literally named `daily__snapshot_v2` into family `daily` — silently
    /// disarming the load's shared-prefix guard against its sibling `daily`.
    ///
    /// A `--split` sub-export named `daily#0` MUST fold to family `daily` so the
    /// load view recombines the N range units as ONE logical table (the
    /// shared-prefix merge-back, #167) — the same fold `snapshot_parent` gives the
    /// CDC snapshot leg, via a dedicated field so neither path triggers the
    /// other's behaviour.
    pub fn family(&self) -> String {
        if let Some(s) = &self.split {
            return s.parent.clone();
        }
        self.snapshot_parent
            .clone()
            .unwrap_or_else(|| self.name.clone())
    }

    /// The tables of a MULTIPLEX capture — a `mode: cdc` export whose `tables:`
    /// list drives several tables through one change stream — or `None` when this
    /// export is a single unit (one `table:`/`query:`, or any batch mode).
    ///
    /// Every consumer of the multiplex layout answers the same question first
    /// ("is this export one table or N?"), and they must answer it identically:
    /// `rivet validate` descends one sub-prefix per table, the type resolver
    /// produces one document per table, and `rivet load` builds one plan per
    /// table. A `tables:` list on a non-CDC export is already refused at
    /// config-load, and an empty one is refused too — the filter here is
    /// belt-and-suspenders so a caller can never fan out over nothing.
    pub fn multiplex_tables(&self) -> Option<&[String]> {
        if self.mode != ExportMode::Cdc {
            return None;
        }
        self.tables.as_deref().filter(|t| !t.is_empty())
    }

    /// Resolve the effective `(CompressionType, level)` for this export.
    /// `compression_profile` takes precedence over `compression` + `compression_level`.
    ///
    /// L24: when a profile is set *and* a conflicting explicit codec/level was
    /// written, warn once that the profile wins rather than silently dropping the
    /// explicit choice. An explicit codec is only detectable when it differs from
    /// the `#[serde(default)]` (Zstd) — a literal `compression: zstd` alongside a
    /// profile is indistinguishable from an omitted field and stays silent.
    pub fn effective_compression(&self) -> (CompressionType, Option<u32>) {
        if let Some(profile) = self.compression_profile {
            let explicit_codec =
                (self.compression != CompressionType::default()).then_some(self.compression);
            if let Some(msg) = super::format::compression_profile_override_warning(
                profile,
                explicit_codec,
                self.compression_level,
            ) {
                log::warn!("export '{}': {}", self.name, msg);
            }
            profile.to_codec()
        } else {
            (self.compression, self.compression_level)
        }
    }

    pub fn max_file_size_bytes(&self) -> Option<u64> {
        self.max_file_size
            .as_ref()
            .and_then(|s| parse_file_size(s).ok())
    }

    pub fn resolve_query(
        &self,
        config_dir: &Path,
        params: Option<&std::collections::HashMap<String, String>>,
    ) -> crate::error::Result<String> {
        // table: shortcut takes precedence — already validated by
        // `validate_business_rules` to be mutually exclusive with query/query_file.
        if let Some(tbl) = &self.table {
            validate_table_shortcut_ident(&self.name, tbl)?;
            return Ok(format!("SELECT * FROM {tbl}"));
        }
        match (&self.query, &self.query_file) {
            (Some(q), None) => {
                if params.is_some() {
                    resolve_vars(q, params)
                } else {
                    Ok(q.clone())
                }
            }
            (None, Some(file)) => {
                let file_path = std::path::Path::new(file);
                // SecOps: block absolute paths and `..` traversal components.
                if file_path.is_absolute() {
                    anyhow::bail!(
                        "export '{}': query_file must be a relative path: '{}'",
                        self.name,
                        file
                    );
                }
                if file_path
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
                {
                    anyhow::bail!(
                        "export '{}': query_file path must not contain '..': '{}'",
                        self.name,
                        file
                    );
                }
                let joined = config_dir.join(file);
                // Canonicalize-based check catches symlink-based evasion for files
                // that already exist on disk.
                if let Ok(canonical) = joined.canonicalize() {
                    let base = config_dir
                        .canonicalize()
                        .unwrap_or_else(|_| config_dir.to_path_buf());
                    if !canonical.starts_with(&base) {
                        anyhow::bail!(
                            "export '{}': query_file '{}' resolves outside the config directory",
                            self.name,
                            file
                        );
                    }
                }
                let raw = std::fs::read_to_string(&joined)?;
                resolve_vars(&raw, params)
            }
            (Some(_), Some(_)) => {
                anyhow::bail!(
                    "export '{}': specify either 'query' or 'query_file', not both",
                    self.name
                )
            }
            (None, None) => {
                anyhow::bail!(
                    "export '{}': must specify exactly one of 'query', 'query_file', or 'table'",
                    self.name
                )
            }
        }
    }
}

/// Validate the value of the `table:` YAML shortcut.
///
/// Accepts ASCII identifiers in the form `<table>` or `<schema>.<table>`. Each
/// segment must match `[A-Za-z_][A-Za-z0-9_]*`. Anything else (quoted
/// identifiers, exotic chars, three-part names, SQL injection attempts) is
/// rejected — the user should fall back to `query:` for those cases.
///
/// The bound on identifier shape keeps generated SQL safe to interpolate
/// without quoting and ensures the generated `SELECT * FROM <ident>` form is
/// recognised by the PG catalog-hint parser ([src/source/postgres.rs]).
fn validate_table_shortcut_ident(export_name: &str, raw: &str) -> crate::error::Result<()> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("export '{export_name}': 'table' is empty");
    }
    let parts: Vec<&str> = trimmed.split('.').collect();
    if parts.len() > 2 {
        anyhow::bail!(
            "export '{export_name}': 'table' must be `<name>` or `<schema>.<name>` (got '{raw}')"
        );
    }
    for part in &parts {
        if part.is_empty() {
            anyhow::bail!("export '{export_name}': 'table' has an empty segment in '{raw}'");
        }
        let mut chars = part.chars();
        let first = chars.next().unwrap();
        if !(first.is_ascii_alphabetic() || first == '_') {
            anyhow::bail!(
                "export '{export_name}': 'table' segment '{part}' must start with a letter or underscore (use 'query:' for quoted identifiers)"
            );
        }
        if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
            anyhow::bail!(
                "export '{export_name}': 'table' segment '{part}' contains non-identifier characters (use 'query:' for quoted identifiers)"
            );
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct QualityConfig {
    pub row_count_min: Option<usize>,
    pub row_count_max: Option<usize>,
    #[serde(default)]
    pub null_ratio_max: std::collections::HashMap<String, f64>,
    #[serde(default)]
    pub unique_columns: Vec<String>,
    /// Cap on the number of distinct values tracked per column during uniqueness checks.
    /// When the limit is hit, a Warn issue is emitted and tracking stops for that column.
    /// Prevents unbounded HashSet growth on high-cardinality columns.
    pub unique_max_entries: Option<usize>,
}

/// What `_rivet_row_hash` covers.
///
/// `true` folds every column of the export's result set — the original
/// behaviour, kept because it is what existing configs say. It is the weaker
/// form: neither side of a later comparison can state WHICH columns were
/// hashed, so coverage has to be inferred from the query, and a column whose
/// value legitimately differs between two systems (a load timestamp, a
/// surrogate key) cannot be excluded without dropping it from the export.
///
/// A declared list fixes both. The covered set is recorded in the run
/// manifest, so a reader can check coverage instead of assuming it.
///
/// ```yaml
/// meta_columns:
///   row_hash: [id, status, updated_at]   # or: row_hash: true
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum RowHash {
    /// `row_hash: true` / `false` — every column, or none.
    All(bool),
    /// `row_hash: [a, b]` — exactly these, in this order. Order matters: it is
    /// the order the canonical text is built in.
    Columns(Vec<String>),
}

impl Default for RowHash {
    fn default() -> Self {
        RowHash::All(false)
    }
}

impl RowHash {
    /// Whether a hash column is emitted at all.
    pub fn enabled(&self) -> bool {
        match self {
            RowHash::All(b) => *b,
            // An empty list is a config error, not "off" — `enabled()` says
            // yes so validation can reject it with a message, rather than
            // silently producing an export with no hash the operator asked for.
            RowHash::Columns(_) => true,
        }
    }

    /// The declared column list, if the operator gave one.
    pub fn declared(&self) -> Option<&[String]> {
        match self {
            RowHash::Columns(c) => Some(c),
            RowHash::All(_) => None,
        }
    }

    /// Config-time validation — the shape errors that need no schema.
    pub fn validate(&self, export_name: &str) -> crate::error::Result<()> {
        let Some(cols) = self.declared() else {
            return Ok(());
        };
        if cols.is_empty() {
            anyhow::bail!(
                "export '{export_name}': meta_columns.row_hash is an empty list — that attests \
                 nothing. Use `row_hash: true` for every column, or name the columns to cover."
            );
        }
        let mut seen = std::collections::HashSet::new();
        if let Some(dup) = cols.iter().find(|c| !seen.insert(c.as_str())) {
            anyhow::bail!(
                "export '{export_name}': meta_columns.row_hash lists '{dup}' twice — the covered \
                 set must be a set"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct MetaColumns {
    #[serde(default)]
    pub exported_at: bool,
    #[serde(default)]
    pub row_hash: RowHash,
}

// `any_enabled()` lived here to answer "does the CDC path need to warn that
// meta columns were dropped". It has no callers since §5h: the two fields no
// longer share an answer — `exported_at` is still dropped on the CDC leg,
// `row_hash` is emitted there — so every caller now asks about one of them
// specifically. Reintroducing a combined predicate would just invite the
// blanket warning back.

fn default_mode() -> ExportMode {
    ExportMode::Full
}

pub(crate) fn default_chunk_size() -> usize {
    100_000
}

fn default_parallel() -> usize {
    1
}

fn default_time_column_type() -> TimeColumnType {
    TimeColumnType::Timestamp
}

/// `until_current` defaults to `true` — the OSS model is the BOUNDED, scheduler-
/// driven drain ("read to the log end and exit"). `until_current: false` is an
/// explicit opt-in to the continuous model; making it the default would silently
/// put a hand-written CDC config onto it.
///
/// What `false` actually means is ENGINE-SPECIFIC, and the earlier wording
/// ("the never-terminating streaming path") was only true for one of them:
///
/// * **MySQL** — genuinely long-lived: a continuous run omits
///   `BINLOG_DUMP_NON_BLOCK` (`source/mysql/cdc.rs`), so the dump BLOCKS on the
///   binlog and the process stays up with an idle source.
/// * **PostgreSQL / SQL Server** — POLL adapters. `false` removes the open-time
///   ceiling, nothing more: the run still exits on CATCH-UP, so any moment the
///   source falls quiet ends it. `DrainMode`'s own doc says these "exit on
///   catch-up and an outer loop re-wraps them" — that outer loop does not exist
///   (`run_capture` has exactly two call sites, neither looping), so the run is
///   one unbounded pass, not a daemon. Measured on the PG CDC stand: exit 0
///   within seconds, both idle and under a 200-row/0.3 s writer.
/// * **MongoDB** — a tailable change stream: it keeps returning events under
///   sustained writes and stops on an empty poll, so it behaves like MySQL while
///   traffic lasts and like the poll adapters when it stops.
///
/// No data is at risk either way — the checkpoint stops at the last committed
/// position and the next run resumes there (defer-not-drop). The hazard is
/// operational: on a poll engine, `false` needs a supervisor to re-run it, or the
/// stream simply stops. If you want a scheduler, use the default.
fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExportMode {
    Full,
    Incremental,
    Chunked,
    TimeWindow,
    /// Log-based change data capture (see [`CdcExportConfig`]): stream
    /// INSERT/UPDATE/DELETE from the source's transaction log instead of querying
    /// the table. Reuses the export's `table` / `destination` / `format`.
    Cdc,
}

/// Default PostgreSQL logical slot when `cdc.slot` is omitted — shared by the
/// runner ([`crate::pipeline`]'s cdc job) and config validation, so the
/// same-slot conflict check sees the value that will actually be used.
pub const DEFAULT_PG_SLOT: &str = "rivet_slot";
/// Default MySQL replica `server_id` when `cdc.server_id` is omitted (see
/// [`DEFAULT_PG_SLOT`] for why this is a shared const).
pub const DEFAULT_MYSQL_SERVER_ID: u32 = 4271;

/// What the FIRST CDC run does before draining changes.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CdcInitialMode {
    /// Anchor-then-snapshot: create the resume anchor (PostgreSQL slot /
    /// MySQL binlog checkpoint / SQL Server LSN checkpoint) FIRST, then run a
    /// full batch snapshot of each table into `<destination>[/<table>]/snapshot/`,
    /// then drain CDC. Because the anchor predates the snapshot read, anything
    /// changed during the snapshot also appears in the change stream — an
    /// overlap (dedupe by PK + `__op`), never a gap. The safe switch ordering,
    /// enforced by construction instead of operator discipline.
    Snapshot,
    // NOTE: `adopt` (anchor an already-loaded table, move no rows) is NOT an OSS
    // mode — it belongs to the paid tier, whose audit is what consumes the
    // anchor (the one-way extension seam, ADR-0026; docs may name the crate,
    // this tree may not). Adding a variant here would put it in the published
    // config schema, the generated reference and the OSS binary — irreversibly,
    // once the crate is published. `oss_rejects_the_pro_only_initial_adopt` is
    // the guard.
}

/// Per-export CDC settings, required when `mode: cdc`. The output `table`,
/// `destination`, and `format` come from the export itself; this carries only the
/// CDC-specific knobs (resume + per-engine stream params).
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct CdcExportConfig {
    /// First-run behaviour: `snapshot` = anchor → full snapshot → drain (see
    /// [`CdcInitialMode`]). Omitted ⇒ capture changes only, with no anchor step
    /// (the default; the operator owns the initial load).
    ///
    /// `snapshot` anchors, and on engines with no server-side anchor (MySQL, SQL
    /// Server) that makes `checkpoint:` mandatory — the checkpoint file IS the
    /// anchor there.
    ///
    /// This doc line is what the generated config reference renders, so every
    /// accepted value must be explained HERE: the reference lists the variants
    /// from the enum but describes only this sentence, so an explanation left on
    /// a variant alone documents a value the reader is told exists and never told
    /// the meaning of.
    #[serde(default)]
    pub initial: Option<CdcInitialMode>,
    /// Persist/resume the source log position to this file. Omit to tail from the
    /// current position without checkpointing.
    pub checkpoint: Option<String>,
    /// Catch up to the source's current end and exit (a bounded run), instead of
    /// streaming indefinitely — ideal for a scheduler. For MySQL this is a
    /// non-blocking binlog dump; PostgreSQL / SQL Server already drain-and-exit.
    /// **Defaults to `true`** (bounded): the OSS model is scheduler-driven, and
    /// omitting this must NOT silently start a never-terminating stream. Setting
    /// `false` opts into the continuous model, which is engine-specific: a true
    /// daemon on MySQL (blocking binlog dump) and MongoDB (the change stream
    /// blocks awaiting events; ends only if the stream is invalidated/closed);
    /// PostgreSQL / SQL Server still exit on catch-up — one unbounded pass, run
    /// it under a supervisor.
    #[serde(default = "default_true")]
    pub until_current: bool,
    /// Stop at the first COMMIT BOUNDARY once N change events have been
    /// captured (default: until end of stream / interrupted). A soft cap, like
    /// `rollover`: a transaction is never split, so the run may overshoot N by
    /// the remainder of the transaction the cap landed in — a hard per-event
    /// stop cut transactions mid-flight and left the stream unable to advance
    /// past them.
    pub max_events: Option<usize>,
    /// Rows per output part file (default 100000). A part also rolls at a
    /// transaction boundary, so it never splits a transaction. Larger ⇒ fewer,
    /// bigger files but more drain memory — the PostgreSQL peek reads a part's
    /// worth per batch, so drain RSS is O(rollover). Tune per workload: raise it
    /// to cut file count, lower it to cap memory on a small extractor.
    pub rollover: Option<usize>,
    /// Roll a part once its buffered changes reach this many MB, whichever comes
    /// first with `rollover`. Caps the in-memory buffer and the part file size by
    /// bytes instead of a fixed row count — predictable for tables with wide
    /// (large JSON / blob) rows, mirroring the batch path's `batch_size_memory_mb`.
    pub rollover_memory_mb: Option<usize>,
    /// MySQL replica server-id for the binlog connection (default 4271; must be
    /// distinct from the source's and any other replica).
    pub server_id: Option<u32>,
    /// PostgreSQL logical replication slot name (default `rivet_slot`).
    pub slot: Option<String>,
    /// SQL Server CDC capture instance, e.g. `dbo_orders` — required for
    /// `sqlserver://` sources.
    pub capture_instance: Option<String>,
}

// Hand-written so the Rust `Default` MATCHES the serde default: `until_current`
// must be `true` (bounded). The derived `Default` would use `bool::default()` =
// `false`, and serde's `default = "default_true"` only affects Deserialize — so
// `CdcExportConfig::default()` would silently mean `DrainMode::Continuous`. That
// default is reached on the drain path (`cdc_job.rs`: `export.cdc.clone()
// .unwrap_or_default()`) whenever a `mode: cdc` export omits the whole `cdc:`
// block (valid for PG/MySQL), turning a minimal bounded drain into a
// never-terminating daemon that persists no resume position — the exact footgun
// `default_true` was added to kill. Mirrors `MongoConfig`'s hand-written Default.
impl Default for CdcExportConfig {
    fn default() -> Self {
        Self {
            initial: None,
            checkpoint: None,
            until_current: true,
            max_events: None,
            rollover: None,
            rollover_memory_mb: None,
            server_id: None,
            slot: None,
            capture_instance: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeColumnType {
    Timestamp,
    Unix,
}

/// Calendar bucket width for date/timestamp output partitioning
/// ([`ExportConfig::partition_by`]). The partition column must be a DATE or
/// TIMESTAMP column; this picks how its range is split into contiguous Hive
/// buckets. It is not a knob for partitioning by arbitrary column values.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PartitionGranularity {
    /// One bucket per calendar day (`col=2023-01-01/`). Default.
    #[default]
    Day,
    /// One bucket per calendar month (`col=2023-01/`).
    Month,
    /// One bucket per calendar year (`col=2023/`).
    Year,
}

/// Canonical fully-populated [`ExportConfig`] for tests across the crate.
///
/// One place lists every field, so adding a field is a single-site edit (the
/// compiler still flags this literal if a field is missing). Test call sites
/// take this baseline and override only the fields they exercise, rather than
/// hand-writing the full struct — see `plan::build` and `preflight` tests.
#[cfg(test)]
pub(crate) fn sample_export(name: &str) -> ExportConfig {
    ExportConfig {
        snapshot_parent: None,
        split: None,
        name: name.into(),
        target: None,
        load: None,
        verify: VerifyMode::Size,
        query: Some("SELECT 1".into()),
        query_file: None,
        table: None,
        tables: None,
        mode: ExportMode::Full,
        cdc: None,
        cursor_column: None,
        cursor_fallback_column: None,
        incremental_cursor_mode: Default::default(),
        chunk_column: None,
        chunk_dense: false,
        chunk_size: 100_000,
        chunk_size_memory_mb: None,
        chunk_count: None,
        chunk_by_days: None,
        chunk_by_key: None,
        parallel: 1,
        wave: None,
        parallel_safe: None,
        time_column: None,
        time_column_type: TimeColumnType::Timestamp,
        days_window: None,
        partition_by: None,
        partition_granularity: PartitionGranularity::Day,
        format: FormatType::Parquet,
        compression: CompressionType::None,
        compression_level: None,
        compression_profile: None,
        skip_empty: false,
        destination: crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some("/tmp".into()),
            ..Default::default()
        },
        meta_columns: MetaColumns::default(),
        quality: None,
        max_file_size: None,
        chunk_checkpoint: false,
        keyset_incremental: false,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// #167: a `--split` sub-export named `daily#0` must fold to family `daily`
    /// so the load view recombines the N range units as one logical table. RED
    /// against a `family()` that ignores `split` (returns the unit name `daily#0`,
    /// which would give each range its own family and defeat the merge-back).
    #[test]
    fn split_subexport_folds_to_parent_family() {
        let mut e = sample_export("daily#0");
        assert_eq!(
            e.family(),
            "daily#0",
            "no split marker → family is the name"
        );
        e.split = Some(SplitSynth {
            parent: "daily".into(),
            key_column: "id".into(),
            lo: Some("1000".into()),
            hi: Some("2000".into()),
        });
        assert_eq!(
            e.family(),
            "daily",
            "a split sub-export must fold to the parent family for the merge-back"
        );
    }

    // ── ExportConfig::multiplex_tables ──────────────────────────────────────

    /// The one decision "is this export ONE unit or N?" — read by `rivet
    /// validate` (descend a sub-prefix per table), the type resolver (one
    /// resolver document per table) and `rivet load` (one plan per table). All
    /// three must answer the same, which is why they all call THIS.
    ///
    /// Both negative arms matter: a `tables:` list on a non-CDC export is not a
    /// multiplex (config-load refuses it outright), and an empty list must never
    /// present as one — a fan-out over zero tables produces zero plans and loads
    /// nothing, silently.
    #[test]
    fn multiplex_tables_is_some_only_for_a_cdc_export_with_a_non_empty_list() {
        let mut e = sample_export("cdc");
        assert_eq!(e.multiplex_tables(), None, "no `tables:` → not a multiplex");

        e.tables = Some(vec!["orders".into(), "customers".into()]);
        assert_eq!(
            e.multiplex_tables(),
            None,
            "`mode: full` is never a multiplex, whatever `tables:` says"
        );

        e.mode = ExportMode::Cdc;
        assert_eq!(
            e.multiplex_tables(),
            Some(&["orders".to_string(), "customers".to_string()][..]),
            "a `mode: cdc` export with `tables:` is N units"
        );

        e.tables = Some(vec![]);
        assert_eq!(
            e.multiplex_tables(),
            None,
            "an EMPTY list must not present as a multiplex — fanning out over zero \
             tables plans zero loads and moves no data, silently"
        );
    }

    // ── ExportConfig::max_file_size_bytes ───────────────────────────────────

    fn make_export_yaml(name: &str, extra: &str) -> ExportConfig {
        let yaml = format!(
            "name: {name}\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n{extra}"
        );
        serde_yaml_ng::from_str(&yaml).expect("parse ExportConfig")
    }

    #[test]
    fn max_file_size_bytes_none_when_unset() {
        let exp = make_export_yaml("no_limit", "");
        assert!(exp.max_file_size_bytes().is_none());
    }

    #[test]
    fn max_file_size_bytes_parses_mb() {
        let exp = make_export_yaml("sized", "max_file_size: \"128MB\"\n");
        assert_eq!(exp.max_file_size_bytes(), Some(128 * 1024 * 1024));
    }

    #[test]
    fn max_file_size_bytes_parses_gb() {
        let exp = make_export_yaml("sized_gb", "max_file_size: \"2GB\"\n");
        assert_eq!(exp.max_file_size_bytes(), Some(2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn max_file_size_bytes_returns_none_on_invalid() {
        let exp = make_export_yaml("bad_size", "max_file_size: \"notanumber\"\n");
        assert!(exp.max_file_size_bytes().is_none());
    }

    // ── ExportConfig::resolve_query ─────────────────────────────────────────

    // Build a minimal ExportConfig directly, bypassing Config::from_yaml validation.
    // This lets us test the four branches inside resolve_query itself, including
    // the (both-set / neither-set) error paths that are normally prevented by the
    // top-level validator.
    fn make_export_direct(query: Option<&str>, query_file: Option<&str>) -> ExportConfig {
        ExportConfig {
            query: query.map(|s| s.to_string()),
            query_file: query_file.map(|s| s.to_string()),
            ..sample_export("test")
        }
    }

    fn params(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn resolve_query_inline_no_params_returns_query_as_is() {
        let exp = make_export_direct(Some("SELECT id FROM orders"), None);
        let q = exp.resolve_query(Path::new("/tmp"), None).unwrap();
        assert_eq!(q, "SELECT id FROM orders");
    }

    #[test]
    fn resolve_query_inline_with_params_substitutes_vars() {
        let exp = make_export_direct(Some("SELECT ${col} FROM ${table}"), None);
        let p = params(&[("col", "id"), ("table", "orders")]);
        let q = exp.resolve_query(Path::new("/tmp"), Some(&p)).unwrap();
        assert_eq!(q, "SELECT id FROM orders");
    }

    #[test]
    fn resolve_query_inline_params_empty_map_is_noop() {
        let exp = make_export_direct(Some("SELECT 1"), None);
        let p = params(&[]);
        let q = exp.resolve_query(Path::new("/tmp"), Some(&p)).unwrap();
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn resolve_query_inline_missing_var_returns_error() {
        // SAFETY: test-only; this binary is single-threaded in the test runner context.
        unsafe { std::env::remove_var("UNSET_RIVET_TEST_VAR") };
        let exp = make_export_direct(Some("SELECT ${UNSET_RIVET_TEST_VAR}"), None);
        let p = params(&[]);
        let result = exp.resolve_query(Path::new("/tmp"), Some(&p));
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("UNSET_RIVET_TEST_VAR") || msg.contains("not set"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_query_file_reads_content() {
        let dir = tempfile::TempDir::new().unwrap();
        let sql_path = dir.path().join("query.sql");
        std::fs::write(&sql_path, "SELECT * FROM customers").unwrap();
        let exp = make_export_direct(None, Some("query.sql"));
        let q = exp.resolve_query(dir.path(), None).unwrap();
        assert_eq!(q, "SELECT * FROM customers");
    }

    #[test]
    fn resolve_query_file_with_params_substitutes() {
        let dir = tempfile::TempDir::new().unwrap();
        let sql_path = dir.path().join("q.sql");
        std::fs::write(&sql_path, "SELECT ${col} FROM ${tbl}").unwrap();
        let exp = make_export_direct(None, Some("q.sql"));
        let p = params(&[("col", "name"), ("tbl", "users")]);
        let q = exp.resolve_query(dir.path(), Some(&p)).unwrap();
        assert_eq!(q, "SELECT name FROM users");
    }

    // ── `table:` shortcut ───────────────────────────────────────────────────

    #[test]
    fn resolve_query_table_shortcut_qualified() {
        let mut exp = make_export_direct(None, None);
        exp.table = Some("public.users".into());
        let q = exp.resolve_query(Path::new("/tmp"), None).unwrap();
        assert_eq!(q, "SELECT * FROM public.users");
    }

    #[test]
    fn resolve_query_table_shortcut_unqualified() {
        let mut exp = make_export_direct(None, None);
        exp.table = Some("orders".into());
        let q = exp.resolve_query(Path::new("/tmp"), None).unwrap();
        assert_eq!(q, "SELECT * FROM orders");
    }

    #[test]
    fn resolve_query_table_shortcut_rejects_three_part_name() {
        let mut exp = make_export_direct(None, None);
        exp.table = Some("db.public.users".into());
        let err = exp.resolve_query(Path::new("/tmp"), None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("<schema>.<name>"), "got: {msg}");
    }

    #[test]
    fn resolve_query_table_shortcut_rejects_sql_injection() {
        for bad in [
            "users; DROP TABLE x",
            "users--",
            "users'",
            "users\"",
            "public.\"My Table\"",
            "0starts_with_digit",
            "",
            ".trailing",
            "leading.",
            "two..dots",
        ] {
            let mut exp = make_export_direct(None, None);
            exp.table = Some(bad.into());
            assert!(
                exp.resolve_query(Path::new("/tmp"), None).is_err(),
                "should reject `table:` value '{bad}'",
            );
        }
    }

    #[test]
    fn resolve_query_table_shortcut_takes_precedence_over_query() {
        let mut exp = make_export_direct(Some("SELECT id FROM x"), None);
        exp.table = Some("public.y".into());
        let q = exp.resolve_query(Path::new("/tmp"), None).unwrap();
        assert_eq!(q, "SELECT * FROM public.y");
    }

    #[test]
    fn resolve_query_file_missing_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let exp = make_export_direct(None, Some("nonexistent.sql"));
        let result = exp.resolve_query(dir.path(), None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("nonexistent.sql") || msg.contains("No such file"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_query_both_set_returns_error() {
        let mut exp = make_export_direct(Some("SELECT 1"), None);
        exp.query_file = Some("file.sql".into());
        let result = exp.resolve_query(Path::new("/tmp"), None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("not both") || msg.contains("query_file"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_query_neither_set_returns_error() {
        let exp = make_export_direct(None, None);
        let result = exp.resolve_query(Path::new("/tmp"), None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("query") || msg.contains("query_file"),
            "got: {msg}"
        );
    }

    // ── SecOps: query_file path traversal prevention ──────────────────────────

    #[test]
    fn resolve_query_file_dotdot_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let exp = make_export_direct(None, Some("../secret.sql"));
        let result = exp.resolve_query(dir.path(), None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("..") || msg.contains("traversal"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_query_file_nested_dotdot_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let exp = make_export_direct(None, Some("subdir/../../etc/passwd"));
        let result = exp.resolve_query(dir.path(), None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("..") || msg.contains("traversal"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_query_file_absolute_path_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let exp = make_export_direct(None, Some("/etc/passwd"));
        let result = exp.resolve_query(dir.path(), None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("relative") || msg.contains("absolute"),
            "got: {msg}"
        );
    }

    #[test]
    fn resolve_query_file_in_subdir_is_allowed() {
        let dir = tempfile::TempDir::new().unwrap();
        let subdir = dir.path().join("queries");
        std::fs::create_dir(&subdir).unwrap();
        std::fs::write(subdir.join("orders.sql"), "SELECT * FROM orders").unwrap();
        let exp = make_export_direct(None, Some("queries/orders.sql"));
        let q = exp.resolve_query(dir.path(), None).unwrap();
        assert_eq!(q, "SELECT * FROM orders");
    }
}
