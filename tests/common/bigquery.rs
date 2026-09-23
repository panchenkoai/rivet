//! BigQuery and real-GCS helpers for live `rivet load` tests; they skip when the warehouse env is unset.

#![allow(dead_code)]

use std::process::Command;

/// The live warehouse a test loads into, with a run-unique GCS prefix.
pub struct BqLive {
    pub project: String,
    pub dataset: String,
    pub bucket: String,
    pub prefix: String,
    /// The test created `dataset`, so dropping this drops it (with the GCS prefix).
    pub owned: bool,
}

impl BqLive {
    /// `BIGQUERY_TEST_PROJECT` + `RIVET_TEST_GCS_BUCKET`, or `None` with a skip note; the dataset is this test's own disposable one unless `RIVET_TEST_BQ_DATASET` names a shared one.
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
        let unique = super::unique_name(label);
        let (dataset, owned) = match std::env::var("RIVET_TEST_BQ_DATASET") {
            Ok(shared) => (shared, false),
            Err(_) => {
                let safe: String = unique
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect();
                let dataset = super::registry::stand_bq_tmp(&safe);
                let out = Command::new("timeout")
                    .args(["120", "bq"])
                    .arg(format!("--project_id={project}"))
                    .arg(format!(
                        "--location={}",
                        super::registry::stand_bq_location()
                    ))
                    .args(["mk", "-f", "--dataset"])
                    .arg(format!("{project}:{dataset}"))
                    .output()
                    .expect("`bq mk` must run");
                assert!(
                    out.status.success(),
                    "bq mk {dataset}: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                (dataset, true)
            }
        };
        Some(Self {
            project,
            dataset,
            bucket,
            prefix: format!("rivet-live/{unique}"),
            owned,
        })
    }

    /// The top-level `load:` line for a rig, with `extra` appended inside the mapping.
    pub fn load_line(&self, extra: &str) -> String {
        format!(
            "load: {{ target: bigquery, project: {}, dataset: {}{extra} }}",
            self.project, self.dataset
        )
    }

    /// Rows of `sql` as JSON objects (every value a string, as `bq` renders them). Every `bq`
    /// call runs under `timeout`: the CLI can hang on a finished job, and a hang that fails
    /// loudly is rerun, one that never returns eats the whole run.
    pub fn read_bq_rows(&self, sql: &str) -> Vec<serde_json::Value> {
        let run = || {
            Command::new("timeout")
                .args(["120", "bq"])
                .arg(format!("--project_id={}", self.project))
                .args([
                    "query",
                    "--use_legacy_sql=false",
                    "--format=json",
                    "--max_rows=100000",
                ])
                .arg(sql)
                .output()
                .expect("`bq query` must run")
        };
        let mut out = run();
        // A hang (exit 124 from `timeout`) after the job finished is the CLI's, not
        // the query's: seen twice on the same count right after a load. One retry.
        if out.status.code() == Some(124) {
            eprintln!("bq query hung and was killed; retrying once: {sql}");
            out = run();
        }
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

    /// The `tables.get` resource of `table` (`bq show --format=json`); the table must exist.
    pub fn read_bq_meta(&self, table: &str) -> serde_json::Value {
        let out = Command::new("timeout")
            .args(["120", "bq"])
            .arg(format!("--project_id={}", self.project))
            .args(["show", "--format=json"])
            .arg(format!("{}.{table}", self.dataset))
            .output()
            .expect("`bq show` must run");
        assert!(
            out.status.success(),
            "bq show failed for {table}:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|e| panic!("bq show returned non-JSON for {table}: {e}"))
    }

    /// `(type, field)` of `table`'s time partitioning, e.g. `("DAY", Some("ts"))`;
    /// `field` is `None` for load-time partitions, the whole thing for no time partitioning.
    pub fn read_bq_time_partitioning(&self, table: &str) -> Option<(String, Option<String>)> {
        time_partitioning(&self.read_bq_meta(table))
    }

    /// `(column, start, end, interval)` of `table`'s integer-range partitioning.
    pub fn read_bq_range_partitioning(&self, table: &str) -> Option<(String, i64, i64, i64)> {
        let meta = self.read_bq_meta(table);
        let rp = meta.get("rangePartitioning")?;
        let int = |k: &str| rp["range"][k].as_str()?.parse::<i64>().ok();
        Some((
            rp["field"].as_str()?.to_string(),
            int("start")?,
            int("end")?,
            int("interval")?,
        ))
    }

    /// `partition_expiration_days` of `table`, from its `expirationMs`.
    pub fn read_bq_partition_expiration_days(&self, table: &str) -> Option<f64> {
        let meta = self.read_bq_meta(table);
        let ms = meta["timePartitioning"]["expirationMs"]
            .as_str()?
            .parse::<f64>()
            .ok()?;
        Some(ms / 86_400_000.0)
    }

    /// Whether `table` requires a partition filter.
    pub fn read_bq_requires_partition_filter(&self, table: &str) -> bool {
        let meta = self.read_bq_meta(table);
        meta["requirePartitionFilter"].as_bool().unwrap_or(false)
            || meta["timePartitioning"]["requirePartitionFilter"]
                .as_bool()
                .unwrap_or(false)
    }

    /// `COUNT(*)` of `table` under `where_sql` — for a table that requires a partition filter.
    pub fn read_bq_count_where(&self, table: &str, where_sql: &str) -> String {
        self.read_bq_rows(&format!(
            "SELECT COUNT(*) AS n FROM `{}.{}.{table}` WHERE {where_sql}",
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
        let out = Command::new("timeout")
            .args(["120", "bq"])
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

impl Drop for BqLive {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }
        let _ = Command::new("timeout")
            .args(["120", "bq"])
            .arg(format!("--project_id={}", self.project))
            .args(["rm", "-r", "-f", "--dataset"])
            .arg(format!("{}:{}", self.project, self.dataset))
            .output();
        let _ = Command::new("timeout")
            .args(["300", "gcloud", "storage", "rm", "-r", "--quiet"])
            .arg(format!("gs://{}/{}/**", self.bucket, self.prefix))
            .output();
    }
}

/// `(type, field)` of a `tables.get` resource's time partitioning.
pub fn time_partitioning(meta: &serde_json::Value) -> Option<(String, Option<String>)> {
    let tp = meta.get("timePartitioning")?;
    Some((
        tp["type"].as_str()?.to_string(),
        tp["field"].as_str().map(String::from),
    ))
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
