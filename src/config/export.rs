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
#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExportConfig {
    pub name: String,
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
    #[serde(default = "default_mode")]
    pub mode: ExportMode,
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
    /// from this budget at plan-build time using a `pg_class` row-size estimate
    /// (`pg_relation_size / reltuples`), clamped to `[10_000, 5_000_000]` rows.
    ///
    /// Mutually exclusive with an explicit non-default `chunk_size:`. Only
    /// applies to `mode: chunked` on a Postgres source using the `table:`
    /// shortcut (the row-size probe needs a known relation).
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
    #[serde(default = "default_parallel")]
    pub parallel: usize,
    pub time_column: Option<String>,
    #[serde(default = "default_time_column_type")]
    pub time_column_type: TimeColumnType,
    pub days_window: Option<u32>,
    pub format: FormatType,
    #[serde(default)]
    pub compression: CompressionType,
    pub compression_level: Option<u32>,
    pub compression_profile: Option<CompressionProfile>,
    #[serde(default)]
    pub skip_empty: bool,
    pub destination: DestinationConfig,
    #[serde(default)]
    pub meta_columns: MetaColumns,
    #[serde(default)]
    pub quality: Option<QualityConfig>,
    pub max_file_size: Option<String>,
    #[serde(default)]
    pub chunk_checkpoint: bool,
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

    /// Policy applied when structural schema drift is detected (column added, removed, or retyped).
    /// Defaults to `warn`: log a warning and continue.
    #[serde(default)]
    pub on_schema_drift: SchemaDriftPolicy,

    /// Growth-factor threshold for data shape drift warnings (Epic 8).
    /// When a string/binary column's max observed byte length in the current run
    /// exceeds `stored_max * shape_drift_warn_factor`, Rivet logs a warning.
    /// `None` uses the default of 2.0. Set to `0.0` to disable shape tracking.
    #[serde(default)]
    pub shape_drift_warn_factor: Option<f64>,

    /// Parquet row group tuning. Only meaningful when `format: parquet`.
    /// When absent, the parquet library default (1,048,576 rows/group) is used.
    #[serde(default)]
    pub parquet: Option<ParquetConfig>,
}

impl ExportConfig {
    /// Resolve the effective `(CompressionType, level)` for this export.
    /// `compression_profile` takes precedence over `compression` + `compression_level`.
    pub fn effective_compression(&self) -> (CompressionType, Option<u32>) {
        if let Some(profile) = self.compression_profile {
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct MetaColumns {
    #[serde(default)]
    pub exported_at: bool,
    #[serde(default)]
    pub row_hash: bool,
}

fn default_mode() -> ExportMode {
    ExportMode::Full
}

fn default_chunk_size() -> usize {
    100_000
}

fn default_parallel() -> usize {
    1
}

fn default_time_column_type() -> TimeColumnType {
    TimeColumnType::Timestamp
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExportMode {
    Full,
    Incremental,
    Chunked,
    TimeWindow,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeColumnType {
    Timestamp,
    Unix,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};

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
            name: "test".into(),
            target: None,
            query: query.map(|s| s.to_string()),
            query_file: query_file.map(|s| s.to_string()),
            table: None,
            mode: ExportMode::Full,
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
            time_column: None,
            time_column_type: TimeColumnType::Timestamp,
            days_window: None,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            compression_profile: None,
            skip_empty: false,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
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
