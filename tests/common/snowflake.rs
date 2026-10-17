//! A live Snowflake target for a rig: the `load:` line, and the cleanup of what a cell created.

use super::bigquery::{gcs_delete_prefix, gcs_list, gcs_put, warehouse_or_skip};

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

    /// Every row of `sql`, read through the DuckDB reader (never through `snow`, the loader's own tool).
    pub fn read(&self, sql: &str) -> Vec<Vec<serde_json::Value>> {
        super::sf_reader::snowflake_rows(&self.connection, &self.database, &self.warehouse, sql)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// `BASE TABLE` / `VIEW` for `table`, or `None` when nothing has that name.
    pub fn kind(&self, table: &str) -> Option<String> {
        self.read(&format!(
            "SELECT table_type FROM {}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '{}' AND table_name = '{}'",
            self.database,
            self.schema.to_uppercase(),
            table.to_uppercase()
        ))
        .first()
        .map(|r| r[0].as_str().expect("table_type").to_string())
    }

    /// `(COUNT(*), COUNT(DISTINCT key))` of `table`.
    pub fn counts(&self, table: &str, key: &str) -> (i64, i64) {
        let rows = self.read(&format!(
            "SELECT COUNT(*), COUNT(DISTINCT {key}) FROM {}.{}.{table}",
            self.database, self.schema
        ));
        let n = |v: &serde_json::Value| match v {
            serde_json::Value::String(s) => s.parse().expect("a count"),
            other => other.as_i64().expect("a count"),
        };
        (n(&rows[0][0]), n(&rows[0][1]))
    }

    /// The `(id, v)` integer pairs of `table`, ordered by id, read through the second client.
    pub fn id_v_pairs(&self, table: &str) -> Vec<(i64, i64)> {
        let n = |v: &serde_json::Value| match v {
            serde_json::Value::String(s) => s.parse().expect("an integer"),
            other => other.as_i64().expect("an integer"),
        };
        self.read(&format!("SELECT id, v FROM {} ORDER BY id", self.fq(table)))
            .iter()
            .map(|r| (n(&r[0]), n(&r[1])))
            .collect()
    }

    /// Every table or view of the schema whose name starts with `table`'s, upper-cased and sorted.
    pub fn names_from(&self, table: &str) -> Vec<String> {
        self.read(&format!(
            "SELECT table_name FROM {}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '{}' \
             AND STARTSWITH(table_name, '{}') ORDER BY table_name",
            self.database,
            self.schema.to_uppercase(),
            table.to_uppercase()
        ))
        .iter()
        .map(|r| r[0].as_str().expect("table_name").to_string())
        .collect()
    }

    /// `privilege grantee` of every grant on `table`, sorted.
    pub fn grants(&self, table: &str) -> Vec<String> {
        self.read(&format!(
            "SELECT privilege_type || ' ' || grantee FROM {}.INFORMATION_SCHEMA.TABLE_PRIVILEGES \
             WHERE table_schema = '{}' AND table_name = '{}' ORDER BY 1",
            self.database,
            self.schema.to_uppercase(),
            table.to_uppercase()
        ))
        .iter()
        .map(|r| r[0].as_str().expect("a grant").to_string())
        .collect()
    }

    /// Every object name under this cell's GCS prefix.
    pub fn gcs_objects(&self) -> Vec<String> {
        gcs_list(&self.bucket, &self.prefix)
    }

    /// Overwrite the object `name` with bytes no Parquet reader accepts.
    pub fn corrupt(&self, name: &str) {
        gcs_put(&self.bucket, name, b"not a parquet file");
    }

    /// Run one statement as the stand's own setup (through `snow`); panics with what Snowflake said.
    pub fn exec(&self, sql: &str) {
        let out = std::process::Command::new("snow")
            .args(["sql", "-c", &self.connection, "-q", sql])
            .output()
            .expect("spawn snow");
        assert!(
            out.status.success(),
            "snow `{sql}`: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// The database the stand owns.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The schema the stand owns.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// `<database>.<schema>.<table>`.
    pub fn fq(&self, table: &str) -> String {
        format!("{}.{}.{table}", self.database, self.schema)
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
                ("STAGE", format!("rivet_stage_{t}")),
                ("STAGE", format!("rivet_stage_{t}__changes")),
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
