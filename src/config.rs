use std::path::Path;

use serde::Deserialize;

use crate::tuning::TuningConfig;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub exports: Vec<ExportConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: SourceType,
    /// Direct URL (may contain ${ENV_VAR} placeholders)
    pub url: Option<String>,
    /// Read URL from this environment variable
    pub url_env: Option<String>,
    /// Read URL from this file (e.g. /run/secrets/db_url)
    pub url_file: Option<String>,
    #[serde(default)]
    pub tuning: Option<TuningConfig>,
}

impl SourceConfig {
    pub fn resolve_url(&self) -> crate::error::Result<String> {
        let raw = match (&self.url, &self.url_env, &self.url_file) {
            (Some(u), None, None) => u.clone(),
            (None, Some(env), None) => std::env::var(env)
                .map_err(|_| anyhow::anyhow!("env var '{}' not set", env))?,
            (None, None, Some(file)) => std::fs::read_to_string(file)
                .map_err(|e| anyhow::anyhow!("cannot read url_file '{}': {}", file, e))?
                .trim()
                .to_string(),
            _ => anyhow::bail!("source: specify exactly one of 'url', 'url_env', or 'url_file'"),
        };

        let resolved = resolve_env_vars(&raw);

        if resolved.contains('@') && resolved.contains(':')
            && let Some(userinfo) = resolved.split('@').next()
                && userinfo.contains(':') && !userinfo.ends_with(':') {
                    log::warn!("source URL contains plaintext password -- consider using url_env or url_file");
                }

        Ok(resolved)
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Postgres,
    Mysql,
}

#[derive(Debug, Deserialize)]
pub struct ExportConfig {
    pub name: String,
    #[serde(default)]
    pub query: Option<String>,
    pub query_file: Option<String>,
    #[serde(default = "default_mode")]
    pub mode: ExportMode,
    pub cursor_column: Option<String>,
    pub chunk_column: Option<String>,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    #[serde(default = "default_parallel")]
    pub parallel: usize,
    pub time_column: Option<String>,
    #[serde(default = "default_time_column_type")]
    pub time_column_type: TimeColumnType,
    pub days_window: Option<u32>,
    pub format: FormatType,
    pub destination: DestinationConfig,
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

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeColumnType {
    Timestamp,
    Unix,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FormatType {
    Parquet,
    Csv,
}

#[derive(Debug, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub destination_type: DestinationType,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub path: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// Path to GCS service account JSON or AWS credentials file
    pub credentials_file: Option<String>,
    /// AWS access key (or env var name with access_key_env)
    pub access_key_env: Option<String>,
    /// AWS secret key env var name
    pub secret_key_env: Option<String>,
    /// AWS profile name (reserved for future use)
    #[allow(dead_code)]
    pub aws_profile: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DestinationType {
    Local,
    S3,
    Gcs,
}

impl ExportConfig {
    pub fn resolve_query(&self, config_dir: &Path) -> crate::error::Result<String> {
        match (&self.query, &self.query_file) {
            (Some(q), None) => Ok(q.clone()),
            (None, Some(file)) => {
                let path = config_dir.join(file);
                Ok(std::fs::read_to_string(&path)?)
            }
            (Some(_), Some(_)) => {
                anyhow::bail!("export '{}': specify either 'query' or 'query_file', not both", self.name)
            }
            (None, None) => {
                anyhow::bail!("export '{}': must specify 'query' or 'query_file'", self.name)
            }
        }
    }
}

/// Replaces `${VAR}` and `$VAR` patterns with environment variable values.
pub fn resolve_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    // ${VAR} syntax
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_name = &result[start + 2..start + end];
            let value = std::env::var(var_name).unwrap_or_default();
            result = format!("{}{}{}", &result[..start], value, &result[start + end + 1..]);
        } else {
            break;
        }
    }
    result
}

impl Config {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let resolved = resolve_env_vars(&contents);
        Self::from_yaml(&resolved)
    }

    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> crate::error::Result<()> {
        let url_sources = [&self.source.url, &self.source.url_env, &self.source.url_file];
        let url_count = url_sources.iter().filter(|u| u.is_some()).count();
        if url_count == 0 {
            anyhow::bail!("source: must specify one of 'url', 'url_env', or 'url_file'");
        }
        if url_count > 1 {
            anyhow::bail!("source: specify exactly one of 'url', 'url_env', or 'url_file'");
        }

        for export in &self.exports {
            if export.query.is_none() && export.query_file.is_none() {
                anyhow::bail!(
                    "export '{}': must specify 'query' or 'query_file'",
                    export.name
                );
            }
            if export.query.is_some() && export.query_file.is_some() {
                anyhow::bail!(
                    "export '{}': specify either 'query' or 'query_file', not both",
                    export.name
                );
            }
            match export.mode {
                ExportMode::Incremental => {
                    if export.cursor_column.is_none() {
                        anyhow::bail!(
                            "export '{}': incremental mode requires cursor_column",
                            export.name
                        );
                    }
                }
                ExportMode::Chunked => {
                    if export.chunk_column.is_none() {
                        anyhow::bail!(
                            "export '{}': chunked mode requires chunk_column",
                            export.name
                        );
                    }
                }
                ExportMode::TimeWindow => {
                    if export.time_column.is_none() {
                        anyhow::bail!(
                            "export '{}': time_window mode requires time_column",
                            export.name
                        );
                    }
                    if export.days_window.is_none() {
                        anyhow::bail!(
                            "export '{}': time_window mode requires days_window",
                            export.name
                        );
                    }
                }
                ExportMode::Full => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_YAML: &str = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: users
    query: "SELECT * FROM users"
    format: csv
    destination:
      type: local
      path: ./out
"#;

    #[test]
    fn parse_minimal_config() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.source.source_type, SourceType::Postgres);
        assert_eq!(cfg.exports.len(), 1);
        assert_eq!(cfg.exports[0].mode, ExportMode::Full);
    }

    #[test]
    fn parse_config_with_tuning() {
        let yaml = r#"
source:
  type: mysql
  url: "mysql://localhost/test"
  tuning:
    profile: safe
    batch_size: 3000
exports:
  - name: orders
    query: "SELECT * FROM orders"
    format: parquet
    destination:
      type: s3
      bucket: my-bucket
"#;
        let cfg = Config::from_yaml(yaml).unwrap();
        let tuning = cfg.source.tuning.as_ref().unwrap();
        assert_eq!(tuning.profile, Some(crate::tuning::TuningProfile::Safe));
        assert_eq!(tuning.batch_size, Some(3000));
    }

    #[test]
    fn incremental_without_cursor_column_is_rejected() {
        let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: incremental
    format: csv
    destination:
      type: local
      path: ./out
"#;
        let err = Config::from_yaml(yaml).unwrap_err();
        assert!(err.to_string().contains("cursor_column"));
    }

    #[test]
    fn incremental_with_cursor_column_is_accepted() {
        Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: good
    query: "SELECT * FROM t"
    mode: incremental
    cursor_column: updated_at
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
    }

    #[test]
    fn default_export_mode_is_full() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.exports[0].mode, ExportMode::Full);
    }

    #[test]
    fn chunked_mode_requires_chunk_column() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: chunked
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("chunk_column"));
    }

    #[test]
    fn chunked_mode_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    query: "SELECT * FROM orders"
    mode: chunked
    chunk_column: id
    chunk_size: 50000
    parallel: 4
    format: parquet
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert_eq!(cfg.exports[0].mode, ExportMode::Chunked);
        assert_eq!(cfg.exports[0].chunk_column.as_deref(), Some("id"));
        assert_eq!(cfg.exports[0].chunk_size, 50000);
        assert_eq!(cfg.exports[0].parallel, 4);
    }

    #[test]
    fn time_window_requires_time_column_and_days() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: time_window
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("time_column"));
    }

    #[test]
    fn time_window_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: events
    query: "SELECT * FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: timestamp
    days_window: 7
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert_eq!(cfg.exports[0].mode, ExportMode::TimeWindow);
        assert_eq!(cfg.exports[0].time_column.as_deref(), Some("created_at"));
        assert_eq!(cfg.exports[0].days_window, Some(7));
    }

    #[test]
    fn query_and_query_file_both_set_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT 1"
    query_file: sql/test.sql
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("query"));
    }

    #[test]
    fn neither_query_nor_query_file_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("query"));
    }

    #[test]
    fn url_env_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert!(cfg.source.url.is_none());
        assert_eq!(cfg.source.url_env.as_deref(), Some("DATABASE_URL"));
    }

    #[test]
    fn url_and_url_env_both_set_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  url_env: DATABASE_URL
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("exactly one"));
    }

    #[test]
    fn no_url_at_all_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("must specify"));
    }

    #[test]
    fn resolve_url_from_env() {
        unsafe { std::env::set_var("RIVET_TEST_DB_URL", "postgresql://test:test@localhost/db"); }
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url_env: RIVET_TEST_DB_URL
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
        let url = cfg.source.resolve_url().unwrap();
        assert_eq!(url, "postgresql://test:test@localhost/db");
        unsafe { std::env::remove_var("RIVET_TEST_DB_URL"); }
    }

    #[test]
    fn env_var_substitution_in_url() {
        unsafe { std::env::set_var("RIVET_TEST_PASS", "s3cret"); }
        let resolved = resolve_env_vars("postgresql://user:${RIVET_TEST_PASS}@localhost/db");
        assert_eq!(resolved, "postgresql://user:s3cret@localhost/db");
        unsafe { std::env::remove_var("RIVET_TEST_PASS"); }
    }

    #[test]
    fn env_var_substitution_missing_var() {
        unsafe { std::env::remove_var("RIVET_NONEXISTENT_VAR"); }
        let resolved = resolve_env_vars("prefix_${RIVET_NONEXISTENT_VAR}_suffix");
        assert_eq!(resolved, "prefix__suffix");
    }

    #[test]
    fn credentials_file_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: my-bucket
      credentials_file: /path/to/sa.json
"#).unwrap();
        assert_eq!(
            cfg.exports[0].destination.credentials_file.as_deref(),
            Some("/path/to/sa.json")
        );
    }

    #[test]
    fn s3_access_key_env_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: s3
      bucket: my-bucket
      region: us-east-1
      access_key_env: MY_AWS_KEY
      secret_key_env: MY_AWS_SECRET
"#).unwrap();
        assert_eq!(cfg.exports[0].destination.access_key_env.as_deref(), Some("MY_AWS_KEY"));
        assert_eq!(cfg.exports[0].destination.secret_key_env.as_deref(), Some("MY_AWS_SECRET"));
    }
}
