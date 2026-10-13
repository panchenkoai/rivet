//! A live Snowflake target for a rig: the `load:` line, and the cleanup of what a cell created.

use super::bigquery::{gcs_delete_prefix, warehouse_or_skip};

/// One cell's Snowflake target: a schema the stand owns and the cell's own GCS prefix.
pub struct SfLive {
    connection: String,
    warehouse: String,
    database: String,
    schema: String,
    integration: String,
    pub bucket: String,
    pub prefix: String,
}

/// Drops the cell's tables and staged objects when the cell ends, however it ends.
pub struct SfCleanup {
    connection: String,
    schema: String,
    tables: Vec<String>,
    bucket: String,
    prefix: String,
}

impl SfLive {
    /// The stand's Snowflake target, or `None` (a named skip) when `RIVET_SF_CONNECTION` or the Google credential is absent.
    pub fn from_env(label: &str) -> Option<Self> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let Some(connection) = var("RIVET_SF_CONNECTION") else {
            super::skip_live(&format!(
                "{label}: RIVET_SF_CONNECTION is unset (no Snowflake stand)"
            ));
            return None;
        };
        let (_, bucket) = warehouse_or_skip(label)?;
        let need = |k: &str| {
            var(k).unwrap_or_else(|| panic!("{k} must be set beside RIVET_SF_CONNECTION"))
        };
        Some(Self {
            connection,
            warehouse: need("RIVET_SF_WAREHOUSE"),
            database: need("RIVET_SF_DATABASE"),
            schema: need("RIVET_SF_SCHEMA"),
            integration: need("RIVET_SF_STORAGE_INTEGRATION"),
            bucket,
            prefix: format!("rivet-live/{}", super::unique_name(label)),
        })
    }

    /// The top-level `load:` line for a rig, with `extra` appended inside the mapping.
    pub fn load_line(&self, extra: &str) -> String {
        format!(
            "load: {{ target: snowflake, connection: {}, warehouse: {}, database: {}, schema: {}, storage_integration: {}{extra} }}",
            self.connection, self.warehouse, self.database, self.schema, self.integration
        )
    }

    /// A guard that drops `tables` (each with its `__changes` log) and the cell's GCS prefix.
    pub fn cleanup(&self, tables: &[&str]) -> SfCleanup {
        SfCleanup {
            connection: self.connection.clone(),
            schema: format!("{}.{}", self.database, self.schema),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

impl Drop for SfCleanup {
    fn drop(&mut self) {
        // One statement per call: `DROP VIEW IF EXISTS` over a table is an error in Snowflake, and it must not stop the rest.
        for t in &self.tables {
            for (kind, name) in [
                ("VIEW", t.clone()),
                ("TABLE", t.clone()),
                ("TABLE", format!("{t}__changes")),
            ] {
                let sql = format!("DROP {kind} IF EXISTS {}.{name}", self.schema);
                let _ = std::process::Command::new("snow")
                    .args(["sql", "-c", &self.connection, "-q", &sql])
                    .output();
            }
        }
        gcs_delete_prefix(&self.bucket, &self.prefix);
    }
}
