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

    // --- URL-based auth (provide exactly one, or use structured fields instead) ---
    pub url: Option<String>,
    pub url_env: Option<String>,
    pub url_file: Option<String>,

    // --- Structured auth (alternative to URL) ---
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Name of env var holding the password.
    pub password_env: Option<String>,
    pub database: Option<String>,

    #[serde(default)]
    pub tuning: Option<TuningConfig>,
}

impl SourceConfig {
    fn has_structured_fields(&self) -> bool {
        self.host.is_some()
            || self.user.is_some()
            || self.database.is_some()
            || self.password.is_some()
            || self.password_env.is_some()
    }

    fn has_url_fields(&self) -> bool {
        self.url.is_some() || self.url_env.is_some() || self.url_file.is_some()
    }

    fn build_url_from_fields(&self) -> crate::error::Result<String> {
        let host = self.host.as_deref()
            .ok_or_else(|| anyhow::anyhow!("source: structured config requires 'host'"))?;
        let user = self.user.as_deref()
            .ok_or_else(|| anyhow::anyhow!("source: structured config requires 'user'"))?;
        let database = self.database.as_deref()
            .ok_or_else(|| anyhow::anyhow!("source: structured config requires 'database'"))?;

        let password = match (&self.password, &self.password_env) {
            (Some(_), Some(_)) => {
                anyhow::bail!("source: specify 'password' or 'password_env', not both");
            }
            (Some(p), None) => {
                log::warn!("source config contains plaintext password -- consider using password_env");
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
            Ok(format!("{}://{}@{}:{}/{}", scheme, user, host, port, database))
        } else {
            Ok(format!("{}://{}:{}@{}:{}/{}", scheme, user, password, host, port, database))
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
            (None, Some(env), None) => std::env::var(env)
                .map_err(|_| anyhow::anyhow!("env var '{}' not set", env))?,
            (None, None, Some(file)) => std::fs::read_to_string(file)
                .map_err(|e| anyhow::anyhow!("cannot read url_file '{}': {}", file, e))?
                .trim()
                .to_string(),
            _ => anyhow::bail!(
                "source: specify exactly one of 'url', 'url_env', 'url_file', or structured fields (host/user/database)"
            ),
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
    #[serde(default)]
    pub compression: CompressionType,
    pub compression_level: Option<u32>,
    #[serde(default)]
    pub skip_empty: bool,
    pub destination: DestinationConfig,
    #[serde(default)]
    pub meta_columns: MetaColumns,
}

#[derive(Debug, Deserialize, Clone, Default)]
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

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    Zstd,
    Snappy,
    Gzip,
    Lz4,
    None,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Zstd
    }
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
    /// GCS: service account JSON path; if set, overrides GOOGLE_APPLICATION_CREDENTIALS / ADC.
    pub credentials_file: Option<String>,
    /// S3: name of env var holding AWS access key id. Must be paired with secret_key_env, or omit both.
    pub access_key_env: Option<String>,
    /// S3: name of env var holding AWS secret access key. Must be paired with access_key_env, or omit both.
    pub secret_key_env: Option<String>,
    /// AWS profile name (reserved for future use)
    #[allow(dead_code)]
    pub aws_profile: Option<String>,
    /// GCS only: use unsigned requests (for local emulators such as fake-gcs-server).
    #[serde(default)]
    pub allow_anonymous: bool,
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
        // Structured fields and URL fields are mutually exclusive (checked at resolve_url time).
        // At parse time, we only need to ensure at least one credential source exists.
        if !self.source.has_url_fields() && !self.source.has_structured_fields() {
            anyhow::bail!("source: must specify url, url_env, url_file, or structured fields (host/user/database)");
        }

        if self.source.has_url_fields() {
            let url_count = [&self.source.url, &self.source.url_env, &self.source.url_file]
                .iter().filter(|u| u.is_some()).count();
            if url_count > 1 {
                anyhow::bail!("source: specify exactly one of 'url', 'url_env', or 'url_file'");
            }
        }

        if self.source.has_url_fields() && self.source.has_structured_fields() {
            anyhow::bail!(
                "source: use either URL-based config (url/url_env/url_file) or structured fields (host/user/database/...), not both"
            );
        }

        if self.source.has_structured_fields() {
            if self.source.host.is_none() {
                anyhow::bail!("source: structured config requires 'host'");
            }
            if self.source.user.is_none() {
                anyhow::bail!("source: structured config requires 'user'");
            }
            if self.source.database.is_none() {
                anyhow::bail!("source: structured config requires 'database'");
            }
            if self.source.password.is_some() && self.source.password_env.is_some() {
                anyhow::bail!("source: specify 'password' or 'password_env', not both");
            }
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
            if export.destination.destination_type == DestinationType::S3 {
                let ak = export.destination.access_key_env.is_some();
                let sk = export.destination.secret_key_env.is_some();
                if ak != sk {
                    anyhow::bail!(
                        "export '{}': S3 requires both access_key_env and secret_key_env, or neither (use default AWS credential chain)",
                        export.name
                    );
                }
            }

            if export.destination.destination_type == DestinationType::Gcs
                && export.destination.allow_anonymous
                && export.destination.credentials_file.is_some()
            {
                anyhow::bail!(
                    "export '{}': GCS allow_anonymous cannot be used together with credentials_file",
                    export.name
                );
            }

            if let Some(cred_path) = &export.destination.credentials_file {
                if !std::path::Path::new(cred_path).exists() {
                    anyhow::bail!(
                        "export '{}': credentials_file '{}' does not exist",
                        export.name, cred_path
                    );
                }
            }

            if let Some(level) = export.compression_level {
                match export.compression {
                    CompressionType::Zstd => {
                        if !(1..=22).contains(&level) {
                            anyhow::bail!(
                                "export '{}': zstd compression_level must be 1..22, got {}",
                                export.name, level
                            );
                        }
                    }
                    CompressionType::Gzip => {
                        if level > 10 {
                            anyhow::bail!(
                                "export '{}': gzip compression_level must be 0..10, got {}",
                                export.name, level
                            );
                        }
                    }
                    _ => {
                        anyhow::bail!(
                            "export '{}': compression_level is only supported for zstd and gzip",
                            export.name
                        );
                    }
                }
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
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let yaml = format!(r#"
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
      credentials_file: "{}"
"#, tmp.path().display());
        let cfg = Config::from_yaml(&yaml).unwrap();
        assert!(cfg.exports[0].destination.credentials_file.is_some());
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

    #[test]
    fn s3_only_one_of_access_key_env_rejected() {
        let err = Config::from_yaml(r#"
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
"#).unwrap_err();
        assert!(err.to_string().contains("access_key_env"));
        assert!(err.to_string().contains("secret_key_env"));
    }

    #[test]
    fn gcs_allow_anonymous_parses() {
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
      bucket: b
      endpoint: http://127.0.0.1:4443
      allow_anonymous: true
"#).unwrap();
        assert!(cfg.exports[0].destination.allow_anonymous);
    }

    #[test]
    fn gcs_allow_anonymous_with_credentials_file_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: b
      allow_anonymous: true
      credentials_file: /x/sa.json
"#).unwrap_err();
        assert!(err.to_string().contains("allow_anonymous"));
    }

    // --- A4: Structured DB credentials ---

    #[test]
    fn structured_pg_credentials_build_url() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  host: db.example.com
  port: 5433
  user: admin
  password: s3cret
  database: mydb
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
        let url = cfg.source.resolve_url().unwrap();
        assert_eq!(url, "postgresql://admin:s3cret@db.example.com:5433/mydb");
    }

    #[test]
    fn structured_mysql_credentials_default_port() {
        let cfg = Config::from_yaml(r#"
source:
  type: mysql
  host: localhost
  user: root
  database: app
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
        let url = cfg.source.resolve_url().unwrap();
        assert_eq!(url, "mysql://root@localhost:3306/app");
    }

    #[test]
    fn structured_with_password_env() {
        unsafe { std::env::set_var("RIVET_TEST_DBPASS", "topSecret"); }
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  host: localhost
  user: rivet
  password_env: RIVET_TEST_DBPASS
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
        let url = cfg.source.resolve_url().unwrap();
        assert_eq!(url, "postgresql://rivet:topSecret@localhost:5432/rivet");
        unsafe { std::env::remove_var("RIVET_TEST_DBPASS"); }
    }

    #[test]
    fn structured_missing_host_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  user: rivet
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("host"));
    }

    #[test]
    fn structured_missing_user_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  host: localhost
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("user"));
    }

    #[test]
    fn structured_missing_database_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  host: localhost
  user: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("database"));
    }

    #[test]
    fn structured_and_url_both_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  host: localhost
  user: rivet
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("not both"));
    }

    #[test]
    fn gcs_credentials_file_missing_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: b
      credentials_file: /nonexistent/path/sa.json
"#).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {}", err);
    }

    #[test]
    fn structured_password_and_password_env_both_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  host: localhost
  user: rivet
  password: abc
  password_env: MY_PASS
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("password"));
    }

    // --- Compression ---

    #[test]
    fn compression_defaults_to_zstd() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.exports[0].compression, CompressionType::Zstd);
        assert!(cfg.exports[0].compression_level.is_none());
    }

    #[test]
    fn compression_snappy_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: snappy
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert_eq!(cfg.exports[0].compression, CompressionType::Snappy);
    }

    #[test]
    fn compression_zstd_with_level() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: zstd
    compression_level: 9
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert_eq!(cfg.exports[0].compression, CompressionType::Zstd);
        assert_eq!(cfg.exports[0].compression_level, Some(9));
    }

    #[test]
    fn compression_gzip_with_level() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: gzip
    compression_level: 6
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert_eq!(cfg.exports[0].compression, CompressionType::Gzip);
        assert_eq!(cfg.exports[0].compression_level, Some(6));
    }

    #[test]
    fn compression_none_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: none
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert_eq!(cfg.exports[0].compression, CompressionType::None);
    }

    #[test]
    fn compression_level_rejected_for_snappy() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: snappy
    compression_level: 5
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("compression_level"), "got: {}", err);
    }

    #[test]
    fn zstd_compression_level_out_of_range_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: zstd
    compression_level: 30
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("1..22"), "got: {}", err);
    }

    #[test]
    fn gzip_compression_level_out_of_range_rejected() {
        let err = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: gzip
    compression_level: 15
    destination:
      type: local
      path: ./out
"#).unwrap_err();
        assert!(err.to_string().contains("0..10"), "got: {}", err);
    }

    // --- Skip empty ---

    #[test]
    fn skip_empty_defaults_to_false() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert!(!cfg.exports[0].skip_empty);
    }

    #[test]
    fn skip_empty_true_parses() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    skip_empty: true
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert!(cfg.exports[0].skip_empty);
    }

    // --- Meta columns ---

    #[test]
    fn meta_columns_defaults_to_disabled() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert!(!cfg.exports[0].meta_columns.exported_at);
        assert!(!cfg.exports[0].meta_columns.row_hash);
    }

    #[test]
    fn meta_columns_both_enabled() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    meta_columns:
      exported_at: true
      row_hash: true
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert!(cfg.exports[0].meta_columns.exported_at);
        assert!(cfg.exports[0].meta_columns.row_hash);
    }

    #[test]
    fn meta_columns_partial() {
        let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    meta_columns:
      row_hash: true
    destination:
      type: local
      path: ./out
"#).unwrap();
        assert!(!cfg.exports[0].meta_columns.exported_at);
        assert!(cfg.exports[0].meta_columns.row_hash);
    }
}
