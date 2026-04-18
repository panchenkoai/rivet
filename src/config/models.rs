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
                let path = config_dir.join(file);
                let raw = std::fs::read_to_string(&path)?;
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
