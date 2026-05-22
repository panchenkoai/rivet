use std::path::Path;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::IncrementalCursorMode;
use super::resolve::{parse_file_size, resolve_env_vars, resolve_vars};
use crate::tuning::{TuningConfig, TuningProfile};

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct NotificationsConfig {
    pub slack: Option<SlackConfig>,
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct SlackConfig {
    pub webhook_url: Option<String>,
    pub webhook_url_env: Option<String>,
    #[serde(default)]
    pub on: Vec<NotifyEvent>,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotifyEvent {
    Failure,
    SchemaChange,
    Degraded,
}

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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: SourceType,

    pub url: Option<String>,
    pub url_env: Option<String>,
    pub url_file: Option<String>,

    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub password_env: Option<String>,
    pub database: Option<String>,

    /// Operational profile of the source database.
    ///
    /// Selects the **default** tuning profile when none is explicitly set in
    /// `source.tuning.profile` or `export.tuning.profile`:
    ///
    /// | `environment`           | default profile |
    /// |-------------------------|------------------|
    /// | `production` (default)  | `balanced` (50 ms throttle, 10 k batch, retries) |
    /// | `replica`               | `balanced` |
    /// | `local`                 | `fast` (no throttle, 50 k batch — saves ~30% wall on localhost) |
    ///
    /// Explicit `tuning.profile:` always wins over this hint.
    #[serde(default)]
    pub environment: Option<SourceEnvironment>,

    #[serde(default)]
    pub tuning: Option<TuningConfig>,

    /// Transport security settings (ADR: SecOps). When absent, Rivet connects
    /// without TLS — a warning is emitted so operators are aware. See [`TlsConfig`].
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

/// Operational environment of the source database — drives the default tuning
/// profile when none is explicitly set. Opt-in: existing configs without
/// `environment:` continue to use `balanced` as today.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceEnvironment {
    /// Localhost / Docker compose / read-only container — no throttle by default
    /// (compiles to `fast` profile defaults). Use when DB load is not a concern.
    Local,
    /// Read replica — `balanced` default. Same throttle as production, but free
    /// to dial up `tuning.batch_size`.
    Replica,
    /// Live production primary — `balanced` default. Bias toward source-safety.
    Production,
}

impl SourceEnvironment {
    /// Default tuning profile selected by this environment when the user has
    /// not set `tuning.profile:` explicitly.
    pub fn default_profile(self) -> TuningProfile {
        match self {
            SourceEnvironment::Local => TuningProfile::Fast,
            SourceEnvironment::Replica | SourceEnvironment::Production => TuningProfile::Balanced,
        }
    }
}

/// Transport security for the source database connection.
///
/// Credentials and exported data cross the wire on every connection; without TLS
/// they are visible to anyone on the network path (cloud inter-VPC, cross-AZ, or
/// a compromised upstream). The default for all new connections is
/// [`TlsMode::Require`] when `tls:` is present; setting `tls: { mode: disable }`
/// is explicit opt-out.
///
/// ```yaml
/// source:
///   type: postgres
///   url_env: DATABASE_URL
///   tls:
///     mode: verify-full
///     ca_file: /etc/ssl/certs/rds-ca-2019-root.pem
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Enforcement level. See [`TlsMode`].
    #[serde(default)]
    pub mode: TlsMode,
    /// PEM-encoded CA certificate to trust for server verification. Required
    /// for [`TlsMode::VerifyCa`] and [`TlsMode::VerifyFull`] against a private CA.
    pub ca_file: Option<String>,
    /// Accept certificates not chained to a trusted CA. Dangerous — disables
    /// server authentication — and only honored when explicitly `true`.
    #[serde(default)]
    pub accept_invalid_certs: bool,
    /// Accept certificates whose subjectAltName does not match the connection
    /// hostname. Dangerous — disables hostname verification.
    #[serde(default)]
    pub accept_invalid_hostnames: bool,
}

/// TLS enforcement mode, mirroring libpq's `sslmode` semantics where possible.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    /// Plaintext. Use only inside trusted networks (loopback, cgroup-private).
    Disable,
    /// Require a TLS handshake; accept the server certificate without verifying
    /// issuer or hostname. Protects against passive sniffing, not MITM.
    Require,
    /// TLS + verify certificate chains to the configured / system trust store.
    /// Does not check hostname (useful for IP-addressed or internal names).
    VerifyCa,
    /// TLS + verify chain **and** hostname against the server cert's SAN/CN.
    /// Recommended default for production.
    #[default]
    VerifyFull,
}

impl TlsMode {
    pub fn is_enforced(self) -> bool {
        !matches!(self, TlsMode::Disable)
    }
}

impl SourceConfig {
    /// Return a copy of this config with **all plaintext credential material stripped**,
    /// safe to embed in a persisted [`crate::plan::PlanArtifact`] (ADR-0005 PA9).
    ///
    /// Redaction rules:
    /// - `password` → always `None` (plaintext password never leaves the process).
    /// - `url` containing `user[:password]@` → userinfo segment replaced with `"REDACTED"`.
    /// - `url_env`, `url_file`, `password_env` — kept (env var **names** and file paths
    ///   are references, not secrets; `apply` needs them to re-resolve credentials).
    /// - `host`, `port`, `user`, `database` — kept (structured connection metadata).
    ///
    /// If a plaintext `password` or `url` is redacted, callers should surface a warning
    /// to the operator so env/file-based auth is available at apply time.
    pub fn redact_for_artifact(&self) -> (Self, bool) {
        let mut out = self.clone();
        let mut redacted = false;

        if out.password.is_some() {
            out.password = None;
            redacted = true;
        }

        if let Some(ref raw) = out.url
            && let Some((userinfo_end, scheme_end)) = find_userinfo(raw)
        {
            let mut s = String::with_capacity(raw.len());
            s.push_str(&raw[..scheme_end]); // "postgresql://"
            s.push_str("REDACTED");
            s.push_str(&raw[userinfo_end..]); // "@host:port/db…"
            out.url = Some(s);
            redacted = true;
        }

        (out, redacted)
    }

    pub(crate) fn has_structured_fields(&self) -> bool {
        self.host.is_some()
            || self.user.is_some()
            || self.database.is_some()
            || self.password.is_some()
            || self.password_env.is_some()
    }

    pub(crate) fn has_url_fields(&self) -> bool {
        self.url.is_some() || self.url_env.is_some() || self.url_file.is_some()
    }

    fn build_url_from_fields(&self) -> crate::error::Result<String> {
        let host = self
            .host
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("source: structured config requires 'host'"))?;
        let user = self
            .user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("source: structured config requires 'user'"))?;
        let database = self
            .database
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("source: structured config requires 'database'"))?;

        // SecOps: keep the plaintext password inside a `Zeroizing<String>` until it
        // is spliced into the final URL, so the standalone password buffer is
        // wiped on drop (the final URL still lives as a plain String but is
        // shorter-lived and dropped by the driver constructor).
        let password: zeroize::Zeroizing<String> =
            zeroize::Zeroizing::new(match (&self.password, &self.password_env) {
                (Some(_), Some(_)) => {
                    anyhow::bail!("source: specify 'password' or 'password_env', not both");
                }
                (Some(p), None) => {
                    log::warn!(
                        "source config contains plaintext password -- consider using password_env"
                    );
                    resolve_env_vars(p)?
                }
                (None, Some(env)) => std::env::var(env).map_err(|_| {
                    anyhow::anyhow!("source: env var '{}' not set (password_env)", env)
                })?,
                (None, None) => String::new(),
            });

        let default_port = match self.source_type {
            SourceType::Postgres => 5432,
            SourceType::Mysql => 3306,
        };
        let port = self.port.unwrap_or(default_port);

        let scheme = match self.source_type {
            SourceType::Postgres => "postgresql",
            SourceType::Mysql => "mysql",
        };

        if password.is_empty() {
            Ok(format!(
                "{}://{}@{}:{}/{}",
                scheme, user, host, port, database
            ))
        } else {
            Ok(format!(
                "{}://{}:{}@{}:{}/{}",
                scheme,
                user,
                password.as_str(),
                host,
                port,
                database
            ))
        }
    }

    pub fn resolve_url(&self) -> crate::error::Result<String> {
        if self.has_url_fields() && self.has_structured_fields() {
            anyhow::bail!(
                "source: use either URL-based config (url/url_env/url_file) or structured fields (host/user/database/...), not both"
            );
        }

        if self.has_structured_fields() {
            return self.build_url_from_fields();
        }

        let raw = match (&self.url, &self.url_env, &self.url_file) {
            (Some(u), None, None) => u.clone(),
            (None, Some(env), None) => {
                std::env::var(env).map_err(|_| anyhow::anyhow!("env var '{}' not set", env))?
            }
            (None, None, Some(file)) => std::fs::read_to_string(file)
                .map_err(|e| anyhow::anyhow!("cannot read url_file '{}': {}", file, e))?
                .trim()
                .to_string(),
            _ => anyhow::bail!(
                "source: specify exactly one of 'url', 'url_env', 'url_file', or structured fields (host/user/database)"
            ),
        };

        let resolved = resolve_env_vars(&raw)?;

        if resolved.contains('@')
            && resolved.contains(':')
            && let Some(userinfo) = resolved.split('@').next()
            && userinfo.contains(':')
            && !userinfo.ends_with(':')
        {
            log::warn!(
                "source URL contains plaintext password -- consider using url_env or url_file"
            );
        }

        Ok(resolved)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Postgres,
    Mysql,
}

/// Locate `user[:password]@` userinfo inside a standard URL.
///
/// Returns `(userinfo_end_index, scheme_end_index)` where:
/// - `scheme_end_index` points right after `"://"` (start of userinfo)
/// - `userinfo_end_index` points at the `@` separator (exclusive of `@`)
///
/// Returns `None` if the URL has no userinfo segment.
fn find_userinfo(raw: &str) -> Option<(usize, usize)> {
    let scheme = raw.find("://")? + 3;
    let rest = &raw[scheme..];
    let at = rest.find('@')?;
    // `@` must appear before the path/query start so we don't match `?foo=a@b` etc.
    if let Some(path) = rest.find('/')
        && path < at
    {
        return None;
    }
    Some((scheme + at, scheme))
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FormatType {
    Parquet,
    Csv,
}

impl FormatType {
    /// Stable lowercase string label for persistence and display.
    /// Prefer this over `format!("{:?}", self).to_lowercase()` — `Debug` output
    /// is not a stable format contract.
    pub fn label(self) -> &'static str {
        match self {
            FormatType::Parquet => "parquet",
            FormatType::Csv => "csv",
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    #[default]
    Zstd,
    Snappy,
    Gzip,
    Lz4,
    None,
}

impl CompressionType {
    /// Stable lowercase string label for persistence and display.
    pub fn label(self) -> &'static str {
        match self {
            CompressionType::Zstd => "zstd",
            CompressionType::Snappy => "snappy",
            CompressionType::Gzip => "gzip",
            CompressionType::Lz4 => "lz4",
            CompressionType::None => "none",
        }
    }
}

/// Parquet row group tuning strategy.
///
/// Controls how many rows Rivet places in each Parquet row group. Row group size
/// affects memory usage during write, compression ratio, and downstream read
/// performance (predicate pushdown, column skipping).
///
/// ```yaml
/// exports:
///   - name: events
///     parquet:
///       row_group_strategy: auto          # compute from schema + target_row_group_mb
///       target_row_group_mb: 128          # default target; auto + fixed_memory only
///       max_row_group_mb: 256             # optional upper bound (all strategies)
///       # row_group_strategy: fixed_rows  # exact row count
///       # row_group_rows: 500000          # used with fixed_rows
///       # row_group_strategy: fixed_memory  # same math as auto, made explicit
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RowGroupStrategy {
    /// Compute rows-per-group from schema column types and `target_row_group_mb`.
    /// For narrow tables this produces large groups (efficient). For wide tables
    /// it reduces group size to stay within the memory target.
    #[default]
    Auto,
    /// Use `row_group_rows` as a literal row count. Ignores memory targets.
    FixedRows,
    /// Identical math to `auto`, but the strategy label is explicit in logs.
    FixedMemory,
}

/// Parquet-specific tuning for row group sizing.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ParquetConfig {
    /// How to determine the row group size. Default: `auto`.
    pub row_group_strategy: Option<RowGroupStrategy>,
    /// Exact number of rows per group (`fixed_rows` only).
    pub row_group_rows: Option<usize>,
    /// Target Arrow buffer memory per row group in MB (`auto` and `fixed_memory`). Default: 128.
    pub target_row_group_mb: Option<usize>,
    /// Hard upper bound on row group memory in MB. When set, further reduces computed row count.
    pub max_row_group_mb: Option<usize>,
}

impl ParquetConfig {
    pub const DEFAULT_TARGET_ROW_GROUP_MB: usize = 128;

    /// Compute the effective rows-per-group from schema column types.
    ///
    /// Returns `None` for `fixed_rows` when `row_group_rows` is not set (caller
    /// falls back to the parquet library default of 1,048,576 rows).
    pub fn effective_row_group_rows(&self, schema: &arrow::datatypes::SchemaRef) -> Option<usize> {
        let strategy = self.row_group_strategy.unwrap_or_default();
        match strategy {
            RowGroupStrategy::FixedRows => self.row_group_rows,
            RowGroupStrategy::Auto | RowGroupStrategy::FixedMemory => {
                let target_mb = self
                    .target_row_group_mb
                    .unwrap_or(Self::DEFAULT_TARGET_ROW_GROUP_MB);
                let row_bytes = crate::tuning::estimate_row_bytes(schema).max(1);
                let rows = (target_mb * 1024 * 1024) / row_bytes;
                // Clamp to a safe range: at least 1 000 rows, at most 10 M rows.
                let rows = rows.clamp(1_000, 10_000_000);
                // Apply optional max_row_group_mb cap.
                let rows = if let Some(max_mb) = self.max_row_group_mb {
                    let max_rows = ((max_mb * 1024 * 1024) / row_bytes).max(1_000);
                    rows.min(max_rows)
                } else {
                    rows
                };
                Some(rows)
            }
        }
    }
}

/// High-level compression preset. Maps to a `(CompressionType, level)` pair.
///
/// ```yaml
/// exports:
///   - name: events
///     compression_profile: fast   # snappy — fastest, larger files
///     # compression_profile: balanced  # zstd level 3 — default for production
///     # compression_profile: compact   # zstd level 9 — smallest files, more CPU
///     # compression_profile: none      # no compression
/// ```
///
/// When set, takes precedence over `compression` and `compression_level`.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionProfile {
    None,
    Fast,
    Balanced,
    Compact,
}

impl CompressionProfile {
    #[allow(dead_code)]
    pub fn label(self) -> &'static str {
        match self {
            CompressionProfile::None => "none",
            CompressionProfile::Fast => "fast",
            CompressionProfile::Balanced => "balanced",
            CompressionProfile::Compact => "compact",
        }
    }

    pub fn to_codec(self) -> (CompressionType, Option<u32>) {
        match self {
            CompressionProfile::None => (CompressionType::None, None),
            CompressionProfile::Fast => (CompressionType::Snappy, None),
            CompressionProfile::Balanced => (CompressionType::Zstd, Some(3)),
            CompressionProfile::Compact => (CompressionType::Zstd, Some(9)),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub destination_type: DestinationType,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub path: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials_file: Option<String>,
    pub access_key_env: Option<String>,
    pub secret_key_env: Option<String>,
    /// Name of an env var holding an AWS STS session token, for use with
    /// short-lived credentials issued by AWS IAM Identity Center / SSO,
    /// `aws sts assume-role`, MFA-protected sessions, EKS IAM Roles for
    /// Service Accounts, etc.  Pair with `access_key_env` + `secret_key_env`.
    /// See `docs/cloud-auth.md` for the AWS auth-flow matrix.
    pub session_token_env: Option<String>,
    pub aws_profile: Option<String>,
    /// Azure storage account name (the prefix in `<account>.blob.core.windows.net`).
    /// Plain string — not a secret. Pair with `account_key_env`.
    /// See `docs/cloud-auth.md` for the Azure auth-flow matrix.
    pub account_name: Option<String>,
    /// Name of an env var holding the Azure Storage account key.  Treated as
    /// a credential and wiped from heap on drop — same SecOps treatment as
    /// `access_key_env`.  Pair with `account_name`.  Mutually exclusive with
    /// `sas_token_env`.
    pub account_key_env: Option<String>,
    /// Name of an env var holding an Azure Storage **SAS token** — typically
    /// a short-lived, scope-limited credential issued out-of-band (Azure
    /// portal / `az storage container generate-sas` / Azure SDK).  Use this
    /// instead of `account_key_env` when the operator does not have the
    /// long-lived account key or wants per-job scoped access.  Pair with
    /// `account_name`.  Mutually exclusive with `account_key_env`.
    ///
    /// The token value is wiped from heap on drop via the same
    /// `Zeroizing<String>` wrapper as `account_key_env`.  Leading `?` is
    /// trimmed transparently so the operator can paste either the full
    /// `?sv=…&sig=…` query string or the raw token body.
    pub sas_token_env: Option<String>,
    #[serde(default)]
    pub allow_anonymous: bool,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DestinationType {
    #[default]
    Local,
    S3,
    Gcs,
    Azure,
    Stdout,
}

impl DestinationType {
    /// Stable lowercase string label for persistence and display.
    pub fn label(self) -> &'static str {
        match self {
            DestinationType::Local => "local",
            DestinationType::S3 => "s3",
            DestinationType::Gcs => "gcs",
            DestinationType::Azure => "azure",
            DestinationType::Stdout => "stdout",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── TlsMode::is_enforced ────────────────────────────────────────────────

    #[test]
    fn tls_mode_disable_not_enforced() {
        assert!(!TlsMode::Disable.is_enforced());
    }

    #[test]
    fn tls_mode_require_is_enforced() {
        assert!(TlsMode::Require.is_enforced());
        assert!(TlsMode::VerifyCa.is_enforced());
        assert!(TlsMode::VerifyFull.is_enforced());
    }

    // ── SourceConfig::redact_for_artifact ───────────────────────────────────

    fn make_source(source_type: SourceType) -> SourceConfig {
        SourceConfig {
            source_type,
            url: None,
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

    #[test]
    fn redact_plaintext_password() {
        let mut src = make_source(SourceType::Postgres);
        src.password = Some("s3cr3t".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "redaction should be flagged");
        assert!(
            redacted.password.is_none(),
            "plaintext password must be stripped"
        );
    }

    #[test]
    fn redact_url_with_password() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://user:hunter2@db.example.com:5432/app".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "URL redaction flagged");
        let url = redacted.url.unwrap();
        assert!(!url.contains("hunter2"), "password must not appear: {url}");
        assert!(url.contains("REDACTED"), "placeholder must appear: {url}");
        assert!(url.contains("@db.example.com"), "host retained: {url}");
    }

    #[test]
    fn redact_url_without_at_sign_not_flagged() {
        let mut src = make_source(SourceType::Postgres);
        // URL with no userinfo (no @ sign) — nothing to redact.
        src.url = Some("postgresql://db.example.com:5432/app".into());
        let (_, flag) = src.redact_for_artifact();
        assert!(!flag, "URL with no userinfo must not be flagged");
    }

    #[test]
    fn redact_url_with_user_but_no_password_is_flagged() {
        let mut src = make_source(SourceType::Postgres);
        // "user@host" — find_userinfo matches any @ so userinfo is redacted.
        src.url = Some("postgresql://user@db.example.com:5432/app".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag, "bare user@ is still userinfo and gets redacted");
        let url = redacted.url.unwrap();
        assert!(url.contains("REDACTED"), "userinfo replaced: {url}");
        assert!(!url.contains("user@"), "bare username removed: {url}");
    }

    #[test]
    fn redact_env_var_reference_kept_intact() {
        let mut src = make_source(SourceType::Mysql);
        src.url_env = Some("DB_URL".into());
        src.password_env = Some("DB_PASS".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(!flag, "env var references are not secrets");
        assert_eq!(redacted.url_env.as_deref(), Some("DB_URL"));
        assert_eq!(redacted.password_env.as_deref(), Some("DB_PASS"));
    }

    #[test]
    fn redact_mysql_url_with_password() {
        let mut src = make_source(SourceType::Mysql);
        src.url = Some("mysql://root:pass@127.0.0.1:3306/mydb".into());
        let (redacted, flag) = src.redact_for_artifact();
        assert!(flag);
        let url = redacted.url.unwrap();
        assert!(url.contains("REDACTED"), "{url}");
        assert!(!url.contains("pass"), "{url}");
    }

    // ── SourceConfig::resolve_url (structured fields) ───────────────────────

    #[test]
    fn resolve_url_from_structured_fields_postgres() {
        let mut src = make_source(SourceType::Postgres);
        src.host = Some("pg.internal".into());
        src.user = Some("alice".into());
        src.database = Some("warehouse".into());
        src.port = Some(5433);
        let url = src.resolve_url().unwrap();
        assert_eq!(url, "postgresql://alice@pg.internal:5433/warehouse");
    }

    #[test]
    fn resolve_url_from_structured_fields_defaults_port() {
        let mut src = make_source(SourceType::Mysql);
        src.host = Some("mysql.internal".into());
        src.user = Some("bob".into());
        src.database = Some("sales".into());
        // No explicit port → defaults to 3306.
        let url = src.resolve_url().unwrap();
        assert_eq!(url, "mysql://bob@mysql.internal:3306/sales");
    }

    #[test]
    fn resolve_url_direct_url_passthrough() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://user@db:5432/mydb".into());
        let url = src.resolve_url().unwrap();
        assert_eq!(url, "postgresql://user@db:5432/mydb");
    }

    #[test]
    fn resolve_url_rejects_mixed_url_and_structured() {
        let mut src = make_source(SourceType::Postgres);
        src.url = Some("postgresql://user@db:5432/mydb".into());
        src.host = Some("other.host".into());
        let err = src.resolve_url().unwrap_err();
        assert!(err.to_string().contains("not both"), "{err}");
    }

    #[test]
    fn resolve_url_rejects_missing_host() {
        let mut src = make_source(SourceType::Postgres);
        src.user = Some("user".into());
        src.database = Some("db".into());
        // host is None → should error
        let err = src.resolve_url().unwrap_err();
        assert!(err.to_string().contains("host"), "{err}");
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

    // ── find_userinfo ────────────────────────────────────────────────────────

    #[test]
    fn find_userinfo_detects_password_in_url() {
        let url = "postgresql://user:pass@host/db";
        let result = find_userinfo(url);
        assert!(result.is_some(), "should detect user:pass@");
    }

    #[test]
    fn find_userinfo_no_password_no_at_returns_none() {
        assert!(find_userinfo("postgresql://host/db").is_none());
    }

    #[test]
    fn find_userinfo_user_only_at_sign_matches() {
        // "user@host" — find_userinfo matches any @ in the host position.
        // The whole userinfo segment (user@) gets replaced in redact_for_artifact.
        let url = "postgresql://user@host/db";
        assert!(find_userinfo(url).is_some(), "bare user@ should match");
    }

    #[test]
    fn find_userinfo_no_at_sign_returns_none() {
        // URL with no @ at all → no userinfo.
        assert!(find_userinfo("postgresql://db.example.com:5432/app").is_none());
    }

    // ── Label methods stability ──────────────────────────────────────────────

    #[test]
    fn format_type_labels_stable() {
        assert_eq!(FormatType::Parquet.label(), "parquet");
        assert_eq!(FormatType::Csv.label(), "csv");
    }

    #[test]
    fn compression_type_labels_stable() {
        assert_eq!(CompressionType::Zstd.label(), "zstd");
        assert_eq!(CompressionType::Snappy.label(), "snappy");
        assert_eq!(CompressionType::Gzip.label(), "gzip");
        assert_eq!(CompressionType::Lz4.label(), "lz4");
        assert_eq!(CompressionType::None.label(), "none");
    }

    #[test]
    fn destination_type_labels_stable() {
        assert_eq!(DestinationType::Local.label(), "local");
        assert_eq!(DestinationType::S3.label(), "s3");
        assert_eq!(DestinationType::Gcs.label(), "gcs");
        assert_eq!(DestinationType::Azure.label(), "azure");
        assert_eq!(DestinationType::Stdout.label(), "stdout");
    }

    // ── ExportConfig::resolve_query ─────────────────────────────────────────

    // Build a minimal ExportConfig directly, bypassing Config::from_yaml validation.
    // This lets us test the four branches inside resolve_query itself, including
    // the (both-set / neither-set) error paths that are normally prevented by the
    // top-level validator.
    fn make_export_direct(query: Option<&str>, query_file: Option<&str>) -> ExportConfig {
        ExportConfig {
            name: "test".into(),
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
        // No ${VAR} in the query, empty params → query passes through unchanged.
        let exp = make_export_direct(Some("SELECT 1"), None);
        let p = params(&[]);
        let q = exp.resolve_query(Path::new("/tmp"), Some(&p)).unwrap();
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn resolve_query_inline_missing_var_returns_error() {
        // ${UNSET_RIVET_TEST_VAR} is not in params and not in env → strict-mode error.
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
        // Any non-identifier character (semicolon, quote, space, dash) is rejected.
        // Falls back to `query:` for non-trivial cases.
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
        // Mutual exclusion is enforced by validate_business_rules; here we
        // verify that resolve_query itself prefers `table:` if set, so that
        // accidental dual-set won't change historical behaviour silently.
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
        // std::fs::read_to_string returns an IO error — the message includes the path or "No such file"
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

    // ── ParquetConfig::effective_row_group_rows ─────────────────────────────

    fn narrow_schema() -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("created_at", DataType::Int64, false),
        ]))
    }

    fn wide_schema() -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        let fields: Vec<Field> = (0..50)
            .map(|i| Field::new(format!("col{i}"), DataType::Utf8, true))
            .collect();
        std::sync::Arc::new(Schema::new(fields))
    }

    #[test]
    fn parquet_config_fixed_rows_returns_explicit_count() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedRows),
            row_group_rows: Some(250_000),
            ..Default::default()
        };
        assert_eq!(pc.effective_row_group_rows(&narrow_schema()), Some(250_000));
    }

    #[test]
    fn parquet_config_fixed_rows_without_row_group_rows_returns_none() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedRows),
            row_group_rows: None,
            ..Default::default()
        };
        assert_eq!(pc.effective_row_group_rows(&narrow_schema()), None);
    }

    #[test]
    fn parquet_config_auto_narrow_table_produces_large_groups() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(128),
            ..Default::default()
        };
        // narrow: 2 columns × (8 + 1) bytes = ~18 B/row → 128 MB / 18 B ≈ 7.5 M clamped to 10 M
        let rows = pc.effective_row_group_rows(&narrow_schema()).unwrap();
        assert!(
            rows >= 1_000_000,
            "narrow table should get large groups, got {rows}"
        );
    }

    #[test]
    fn parquet_config_auto_wide_table_produces_smaller_groups() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(128),
            ..Default::default()
        };
        // wide: 50 cols × (256 + 1) bytes = ~12 850 B/row → 128 MB / 12 850 ≈ 10 436 rows
        let rows = pc.effective_row_group_rows(&wide_schema()).unwrap();
        assert!(
            rows < 100_000,
            "wide table should get smaller groups, got {rows}"
        );
        assert!(rows >= 1_000, "should be at least the minimum, got {rows}");
    }

    #[test]
    fn parquet_config_max_row_group_mb_caps_result() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(128),
            max_row_group_mb: Some(1), // ~55 rows for narrow schema (18 B/row)
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&narrow_schema()).unwrap();
        // 1 MB / 18 B ≈ 58 254 but clamped to 1 000 minimum
        assert!(
            rows <= 100_000,
            "max_row_group_mb should cap rows, got {rows}"
        );
    }

    #[test]
    fn parquet_config_deserializes_from_yaml() {
        let yaml = "row_group_strategy: auto\ntarget_row_group_mb: 64\n";
        let pc: ParquetConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(pc.row_group_strategy, Some(RowGroupStrategy::Auto));
        assert_eq!(pc.target_row_group_mb, Some(64));
    }

    #[test]
    fn parquet_config_fixed_memory_same_math_as_auto() {
        // FixedMemory and Auto are identical in computation — only the strategy label differs.
        let auto_pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(64),
            ..Default::default()
        };
        let fixed_mem_pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedMemory),
            target_row_group_mb: Some(64),
            ..Default::default()
        };
        assert_eq!(
            auto_pc.effective_row_group_rows(&narrow_schema()),
            fixed_mem_pc.effective_row_group_rows(&narrow_schema()),
            "FixedMemory and Auto must produce identical row counts for the same target"
        );
        assert_eq!(
            auto_pc.effective_row_group_rows(&wide_schema()),
            fixed_mem_pc.effective_row_group_rows(&wide_schema()),
        );
    }

    #[test]
    fn parquet_config_auto_without_target_uses_default_128mb() {
        // When target_row_group_mb is absent, the 128 MB default is used.
        // For the narrow schema (18 B/row): 128 MB / 18 B ≈ 7.5 M → clamped to 10 M.
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: None,
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&narrow_schema()).unwrap();
        // With default 128 MB target, narrow schema produces large groups.
        assert!(
            rows >= 1_000_000,
            "default 128 MB target should give large groups for narrow table; got {rows}"
        );
    }

    #[test]
    fn parquet_config_no_block_gives_none_for_row_group_rows() {
        // When ExportConfig.parquet is None, the sink never calls effective_row_group_rows.
        // This test documents that Default::default() for ParquetConfig (strategy = None)
        // is equivalent to Auto with the default target — both return Some(rows).
        let pc = ParquetConfig::default(); // strategy = None → unwrap_or_default() → Auto
        let rows = pc.effective_row_group_rows(&narrow_schema());
        assert!(
            rows.is_some(),
            "default ParquetConfig (strategy: None) must return Some, got None"
        );
    }

    #[test]
    fn parquet_config_small_target_clamps_to_minimum_1000_rows() {
        // Even if the math gives fewer than 1 000 rows (e.g., a super-wide table with
        // a very small target), the result is clamped to 1 000 — never 0 or tiny.
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(1), // tiny target
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&wide_schema()).unwrap();
        assert!(
            rows >= 1_000,
            "must not go below minimum 1 000 rows; got {rows}"
        );
    }
}
