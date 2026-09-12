//! BigQuery and real-GCS helpers for live `rivet load` tests; they skip when the warehouse env is unset.

#![allow(dead_code)]

use std::process::Command;

/// The live warehouse a test loads into, with a run-unique GCS prefix.
pub struct BqLive {
    pub project: String,
    pub dataset: String,
    pub bucket: String,
    pub prefix: String,
}

impl BqLive {
    /// `BIGQUERY_TEST_PROJECT` + `RIVET_TEST_GCS_BUCKET` (+ `RIVET_TEST_BQ_DATASET`, default
    /// `rivet_e2e`), or `None` with a skip note.
    pub fn from_env(label: &str) -> Option<Self> {
        let (Ok(project), Ok(bucket)) = (
            std::env::var("BIGQUERY_TEST_PROJECT"),
            std::env::var("RIVET_TEST_GCS_BUCKET"),
        ) else {
            super::skip_live(&format!(
                "{label}: BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset"
            ));
            return None;
        };
        Some(Self {
            project,
            dataset: std::env::var("RIVET_TEST_BQ_DATASET").unwrap_or_else(|_| "rivet_e2e".into()),
            bucket,
            prefix: format!("rivet-live/{}", super::unique_name(label)),
        })
    }

    /// The top-level `load:` line for a rig, with `extra` appended inside the mapping.
    pub fn load_line(&self, extra: &str) -> String {
        format!(
            "load: {{ target: bigquery, project: {}, dataset: {}{extra} }}",
            self.project, self.dataset
        )
    }

    /// Rows of `sql` as JSON objects (every value a string, as `bq` renders them).
    pub fn read_bq_rows(&self, sql: &str) -> Vec<serde_json::Value> {
        let out = Command::new("bq")
            .arg(format!("--project_id={}", self.project))
            .args([
                "query",
                "--use_legacy_sql=false",
                "--format=json",
                "--max_rows=100000",
            ])
            .arg(sql)
            .output()
            .expect("`bq query` must run");
        assert!(
            out.status.success(),
            "bq query failed: {sql}\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8_lossy(&out.stdout);
        let json = text.trim();
        if json.is_empty() {
            return Vec::new();
        }
        serde_json::from_str::<Vec<serde_json::Value>>(json)
            .unwrap_or_else(|e| panic!("bq returned non-JSON for {sql}: {e}\n{json}"))
    }

    /// The clustering columns of `table`, in clustering order.
    pub fn read_bq_clustering(&self, table: &str) -> Vec<String> {
        self.read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{table}' AND clustering_ordinal_position IS NOT NULL \
             ORDER BY clustering_ordinal_position",
            self.project, self.dataset
        ))
        .iter()
        .map(|r| r["column_name"].as_str().expect("column_name").to_string())
        .collect()
    }

    /// The column `table` is partitioned on, if any.
    pub fn read_bq_partitioning(&self, table: &str) -> Option<String> {
        self.read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{table}' AND is_partitioning_column = 'YES'",
            self.project, self.dataset
        ))
        .first()
        .map(|r| r["column_name"].as_str().expect("column_name").to_string())
    }

    /// `BASE TABLE` / `VIEW` for `table`, or `None` when nothing has that name.
    pub fn read_bq_table_type(&self, table: &str) -> Option<String> {
        self.read_bq_rows(&format!(
            "SELECT table_type FROM `{}.{}`.INFORMATION_SCHEMA.TABLES \
             WHERE table_name = '{table}'",
            self.project, self.dataset
        ))
        .first()
        .map(|r| r["table_type"].as_str().expect("table_type").to_string())
    }

    /// `COUNT(*)` of `table` as text.
    pub fn read_bq_count(&self, table: &str) -> String {
        self.read_bq_rows(&format!(
            "SELECT COUNT(*) AS n FROM `{}.{}.{table}`",
            self.project, self.dataset
        ))[0]["n"]
            .as_str()
            .expect("count")
            .to_string()
    }

    /// One table option of `table` as BigQuery renders it, or `None` when unset.
    pub fn read_bq_option(&self, table: &str, option: &str) -> Option<String> {
        self.read_bq_rows(&format!(
            "SELECT option_value FROM `{}.{}`.INFORMATION_SCHEMA.TABLE_OPTIONS \
             WHERE table_name = '{table}' AND option_name = '{option}'",
            self.project, self.dataset
        ))
        .first()
        .map(|r| {
            r["option_value"]
                .as_str()
                .expect("option_value")
                .to_string()
        })
    }

    /// Run one DDL statement, panicking on failure.
    pub fn exec(&self, sql: &str) {
        let out = Command::new("bq")
            .arg(format!("--project_id={}", self.project))
            .args(["query", "--use_legacy_sql=false"])
            .arg(sql)
            .output()
            .expect("`bq query` must run");
        assert!(
            out.status.success(),
            "bq failed: {sql}\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Drops `tables` and the staged GCS prefix when the returned guard goes out of scope.
    pub fn cleanup(&self, tables: &[&str]) -> BqCleanup {
        BqCleanup {
            project: self.project.clone(),
            dataset: self.dataset.clone(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            gcs: format!("gs://{}/{}/**", self.bucket, self.prefix),
        }
    }
}

/// See [`BqLive::cleanup`].
pub struct BqCleanup {
    project: String,
    dataset: String,
    tables: Vec<String>,
    gcs: String,
}

impl Drop for BqCleanup {
    fn drop(&mut self) {
        for t in &self.tables {
            for kind in ["TABLE", "VIEW"] {
                let _ = Command::new("timeout")
                    .arg("120")
                    .arg("bq")
                    .arg(format!("--project_id={}", self.project))
                    .args(["query", "--use_legacy_sql=false"])
                    .arg(format!(
                        "DROP {kind} IF EXISTS `{}.{}.{t}`",
                        self.project, self.dataset
                    ))
                    .output();
            }
        }
        let _ = Command::new("timeout")
            .args(["300", "gcloud", "storage", "rm", "-r", "--quiet", &self.gcs])
            .output();
    }
}
