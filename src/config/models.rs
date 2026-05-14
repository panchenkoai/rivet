use std::path::Path;

use serde::{Deserialize, Serialize};

use super::IncrementalCursorMode;
use super::resolve::{parse_file_size, resolve_env_vars, resolve_vars};
use crate::tuning::TuningConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct NotificationsConfig {
    pub slack: Option<SlackConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SlackConfig {
    pub webhook_url: Option<String>,
    pub webhook_url_env: Option<String>,
    #[serde(default)]
    pub on: Vec<NotifyEvent>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
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
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
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

    #[serde(default)]
    pub tuning: Option<TuningConfig>,

    /// Transport security settings (ADR: SecOps). When absent, Rivet connects
    /// without TLS — a warning is emitted so operators are aware. See [`TlsConfig`].
    #[serde(default)]
    pub tls: Option<TlsConfig>,
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
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
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
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Deserialize, Clone)]
pub struct ExportConfig {
    pub name: String,
    #[serde(default)]
    pub query: Option<String>,
    pub query_file: Option<String>,
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
}

impl ExportConfig {
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
                    "export '{}': must specify 'query' or 'query_file'",
                    self.name
                )
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct QualityConfig {
    pub row_count_min: Option<usize>,
    pub row_count_max: Option<usize>,
    #[serde(default)]
    pub null_ratio_max: std::collections::HashMap<String, f64>,
    #[serde(default)]
    pub unique_columns: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
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

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExportMode {
    Full,
    Incremental,
    Chunked,
    TimeWindow,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeColumnType {
    Timestamp,
    Unix,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
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
    pub aws_profile: Option<String>,
    #[serde(default)]
    pub allow_anonymous: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DestinationType {
    Local,
    S3,
    Gcs,
    Stdout,
}

impl DestinationType {
    /// Stable lowercase string label for persistence and display.
    pub fn label(self) -> &'static str {
        match self {
            DestinationType::Local => "local",
            DestinationType::S3 => "s3",
            DestinationType::Gcs => "gcs",
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
            mode: ExportMode::Full,
            cursor_column: None,
            cursor_fallback_column: None,
            incremental_cursor_mode: Default::default(),
            chunk_column: None,
            chunk_dense: false,
            chunk_size: 100_000,
            chunk_by_days: None,
            parallel: 1,
            time_column: None,
            time_column_type: TimeColumnType::Timestamp,
            days_window: None,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            skip_empty: false,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("/tmp".into()),
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
}
