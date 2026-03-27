use serde::Deserialize;

use crate::tuning::TuningConfig;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub exports: Vec<ExportConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: SourceType,
    pub url: String,
    #[serde(default)]
    pub tuning: Option<TuningConfig>,
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
    pub query: String,
    #[serde(default = "default_mode")]
    pub mode: ExportMode,
    pub cursor_column: Option<String>,
    pub format: FormatType,
    pub destination: DestinationConfig,
}

fn default_mode() -> ExportMode {
    ExportMode::Full
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExportMode {
    Full,
    Incremental,
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
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DestinationType {
    Local,
    S3,
    Gcs,
}

impl Config {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_yaml(&contents)
    }

    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        let config: Config = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> crate::error::Result<()> {
        for export in &self.exports {
            if export.mode == ExportMode::Incremental && export.cursor_column.is_none() {
                anyhow::bail!(
                    "export '{}': incremental mode requires cursor_column",
                    export.name
                );
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
    fn parse_minimal_config() -> anyhow::Result<()> {
        let cfg = Config::from_yaml(MINIMAL_YAML)?;
        assert_eq!(cfg.source.source_type, SourceType::Postgres);
        assert_eq!(cfg.exports.len(), 1);
        assert_eq!(cfg.exports[0].name, "users");
        assert_eq!(cfg.exports[0].format, FormatType::Csv);
        assert!(cfg.source.tuning.is_none(), "minimal config should have no tuning");
        Ok(())
    }

    #[test]
    fn parse_config_with_tuning() -> anyhow::Result<()> {
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
        let cfg = Config::from_yaml(yaml)?;
        let tuning = cfg.source.tuning.as_ref().expect("tuning block should be parsed");
        assert_eq!(tuning.profile, Some(crate::tuning::TuningProfile::Safe));
        assert_eq!(tuning.batch_size, Some(3000));
        Ok(())
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
        let err = Config::from_yaml(yaml).expect_err("should reject incremental without cursor");
        assert!(
            err.to_string().contains("cursor_column"),
            "error should mention cursor_column, got: {err}",
        );
    }

    #[test]
    fn incremental_with_cursor_column_is_accepted() -> anyhow::Result<()> {
        let yaml = r#"
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
"#;
        Config::from_yaml(yaml)?;
        Ok(())
    }

    #[test]
    fn default_export_mode_is_full() -> anyhow::Result<()> {
        let cfg = Config::from_yaml(MINIMAL_YAML)?;
        assert_eq!(cfg.exports[0].mode, ExportMode::Full);
        Ok(())
    }
}
