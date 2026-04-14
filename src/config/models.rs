use std::path::Path;

use serde::{Deserialize, Serialize};

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
}

impl SourceConfig {
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

        let password = match (&self.password, &self.password_env) {
            (Some(_), Some(_)) => {
                anyhow::bail!("source: specify 'password' or 'password_env', not both");
            }
            (Some(p), None) => {
                log::warn!(
                    "source config contains plaintext password -- consider using password_env"
                );
                resolve_env_vars(p)
            }
            (None, Some(env)) => std::env::var(env)
                .map_err(|_| anyhow::anyhow!("source: env var '{}' not set (password_env)", env))?,
            (None, None) => String::new(),
        };

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
                scheme, user, password, host, port, database
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

        let resolved = resolve_env_vars(&raw);

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

#[derive(Debug, Deserialize, Clone)]
pub struct ExportConfig {
    pub name: String,
    #[serde(default)]
    pub query: Option<String>,
    pub query_file: Option<String>,
    #[serde(default = "default_mode")]
    pub mode: ExportMode,
    pub cursor_column: Option<String>,
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
                    Ok(resolve_vars(q, params))
                } else {
                    Ok(q.clone())
                }
            }
            (None, Some(file)) => {
                let path = config_dir.join(file);
                let raw = std::fs::read_to_string(&path)?;
                Ok(resolve_vars(&raw, params))
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
