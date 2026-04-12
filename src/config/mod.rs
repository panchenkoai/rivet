mod models;
pub mod resolve;

pub use models::*;
#[allow(unused_imports)]
pub(crate) use resolve::resolve_env_vars;
pub use resolve::{parse_file_size, resolve_vars};

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub source: SourceConfig,
    pub exports: Vec<ExportConfig>,
    #[serde(default)]
    pub notifications: Option<NotificationsConfig>,
    #[serde(default)]
    pub parallel_exports: bool,
    #[serde(default)]
    pub parallel_export_processes: bool,
}

impl Config {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        Self::load_with_params(path, None)
    }

    pub fn load_with_params(
        path: &str,
        params: Option<&std::collections::HashMap<String, String>>,
    ) -> crate::error::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let resolved = resolve_vars(&contents, params);
        Self::from_yaml(&resolved)
    }

    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        Self::check_misplaced_tuning_fields(yaml)?;
        let config: Config = serde_yaml_ng::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// Detect tuning-related fields placed directly under `source:` or an
    /// `exports[]` entry instead of inside the `tuning:` sub-key. Without this
    /// check serde silently ignores unknown keys and the user gets unexpected
    /// defaults (e.g. batch_size=10 000 instead of the intended 1 000).
    fn check_misplaced_tuning_fields(yaml: &str) -> crate::error::Result<()> {
        const TUNING_FIELDS: &[&str] = &[
            "batch_size",
            "batch_size_memory_mb",
            "throttle_ms",
            "statement_timeout_s",
            "max_retries",
            "retry_backoff_ms",
            "lock_timeout_s",
            "memory_threshold_mb",
            "profile",
        ];

        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;

        if let Some(source) = root.get("source") {
            let misplaced: Vec<&str> = TUNING_FIELDS
                .iter()
                .copied()
                .filter(|&f| source.get(f).is_some())
                .collect();
            if !misplaced.is_empty() {
                anyhow::bail!(
                    "source: field(s) [{}] belong under 'source.tuning:', not directly under 'source:'. \
                     Example:\n  source:\n    tuning:\n      {}: <value>",
                    misplaced.join(", "),
                    misplaced[0],
                );
            }
        }

        if let Some(exports) = root.get("exports").and_then(|e| e.as_sequence()) {
            for (i, export) in exports.iter().enumerate() {
                let name = export
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("<unnamed>");
                let misplaced: Vec<&str> = TUNING_FIELDS
                    .iter()
                    .copied()
                    .filter(|&f| export.get(f).is_some())
                    .collect();
                if !misplaced.is_empty() {
                    anyhow::bail!(
                        "export '{}' (index {}): field(s) [{}] belong under 'exports[].tuning:', \
                         not directly in the export. Example:\n  exports:\n    - name: {}\n      tuning:\n        {}: <value>",
                        name,
                        i,
                        misplaced.join(", "),
                        name,
                        misplaced[0],
                    );
                }
            }
        }

        Ok(())
    }

    fn validate(&self) -> crate::error::Result<()> {
        if let Some(t) = &self.source.tuning
            && t.batch_size.is_some()
            && t.batch_size_memory_mb.is_some()
        {
            anyhow::bail!("tuning: batch_size and batch_size_memory_mb are mutually exclusive");
        }

        for export in &self.exports {
            let merged = crate::tuning::merge_tuning_config(
                self.source.tuning.as_ref(),
                export.tuning.as_ref(),
            );
            if let Some(t) = merged
                && t.batch_size.is_some()
                && t.batch_size_memory_mb.is_some()
            {
                anyhow::bail!(
                    "export '{}': effective tuning has both batch_size and batch_size_memory_mb (mutually exclusive)",
                    export.name
                );
            }
            if let Some(et) = &export.tuning
                && et.batch_size.is_some()
                && et.batch_size_memory_mb.is_some()
            {
                anyhow::bail!(
                    "export '{}': tuning.batch_size and tuning.batch_size_memory_mb are mutually exclusive",
                    export.name
                );
            }
        }

        if !self.source.has_url_fields() && !self.source.has_structured_fields() {
            anyhow::bail!(
                "source: must specify url, url_env, url_file, or structured fields (host/user/database)"
            );
        }

        if self.source.has_url_fields() {
            let url_count = [
                &self.source.url,
                &self.source.url_env,
                &self.source.url_file,
            ]
            .iter()
            .filter(|u| u.is_some())
            .count();
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

            if let Some(cred_path) = &export.destination.credentials_file
                && !std::path::Path::new(cred_path).exists()
            {
                anyhow::bail!(
                    "export '{}': credentials_file '{}' does not exist",
                    export.name,
                    cred_path
                );
            }

            if let Some(ref size_str) = export.max_file_size {
                parse_file_size(size_str).map_err(|_| {
                    anyhow::anyhow!(
                        "export '{}': invalid max_file_size '{}'",
                        export.name,
                        size_str
                    )
                })?;
            }

            if let Some(level) = export.compression_level {
                match export.compression {
                    CompressionType::Zstd => {
                        if !(1..=22).contains(&level) {
                            anyhow::bail!(
                                "export '{}': zstd compression_level must be 1..22, got {}",
                                export.name,
                                level
                            );
                        }
                    }
                    CompressionType::Gzip => {
                        if level > 10 {
                            anyhow::bail!(
                                "export '{}': gzip compression_level must be 0..10, got {}",
                                export.name,
                                level
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

            if export.chunk_dense && export.mode != ExportMode::Chunked {
                anyhow::bail!(
                    "export '{}': chunk_dense is only valid with mode: chunked",
                    export.name
                );
            }

            if let Some(days) = export.chunk_by_days {
                if export.mode != ExportMode::Chunked {
                    anyhow::bail!(
                        "export '{}': chunk_by_days requires mode: chunked",
                        export.name
                    );
                }
                if export.chunk_dense {
                    anyhow::bail!(
                        "export '{}': chunk_by_days cannot be combined with chunk_dense",
                        export.name
                    );
                }
                if days == 0 {
                    anyhow::bail!(
                        "export '{}': chunk_by_days must be at least 1",
                        export.name
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
