//! The `TargetLoader` seam and the first live loader — **BigQuery**.
//!
//! OSS decides *what* a column becomes in the warehouse (`TargetColumnSpec`,
//! via `ExportTarget::resolve_table`). This module executes that plan against
//! a live warehouse.
//!
//! ## BigQuery load model — one free path
//!
//! Batch-loading Parquet from GCS is **free** in BigQuery (load jobs use the
//! ingestion slot pool, not query slots). The loader declares each column's
//! native `target_type` **inline in the `LOAD DATA` statement**, e.g.
//!
//! ```sql
//! LOAD DATA OVERWRITE `p.d.t` (id INT64, json_col JSON, dt_col DATETIME)
//! PARTITION BY d FROM FILES (format = 'PARQUET', uris = [...]);
//! ```
//!
//! With the schema declared, BigQuery **coerces the Parquet to native types on
//! load** — JSON, DATETIME (wall-clock), TIME, NUMERIC, … all land natively,
//! **for free** (a load job, not a query). No autoload-then-CTAS recovery is
//! needed. Verified live against a full MySQL type matrix: every column loaded
//! natively with `total_bytes_billed = 0`.
//!
//! (This corrects an earlier premise — the OSS resolver's `cast_sql` recovery
//! assumes a *bare* autoload rejects native types; declaring the schema in the
//! `LOAD DATA` statement itself coerces them for free. The one exception is a
//! *value* transform like UUID `bytes → TO_HEX(hex)`, which a type declaration
//! cannot perform; such a column lands as its declared type and may need a
//! downstream transform.)
//!
//! Idempotent under Rivet's at-least-once file delivery: `LOAD DATA OVERWRITE`
//! reproduces the same table on a retry.
//!
//! ## Two BigQuery limits this respects
//!
//! - `PARTITION BY` / `CLUSTER BY` apply **only when the table is created**;
//!   you cannot convert an existing table by overwriting it, and clustering is
//!   capped at 4 columns. The loader manages its own target table.
//! - A single load *or* query job may modify at most **4,000 partitions**. A
//!   partitioned load spanning more is split into several `LOAD DATA` jobs, each
//!   under the cap (see `plan_load_batches`); a non-splittable overflow surfaces
//!   an actionable error telling you to split the URIs by partition range.
//!
//! ## Cost attribution via job labels
//!
//! Every BigQuery job the loader creates is labeled so its cost is
//! automatically attributable: `managed_by:rivet`, `rivet_op:<load|count>`,
//! `rivet_table:<table>`, `rivet_run:<id>` (the load-run correlation id, when
//! set).
//! The batch ops are free load/metadata jobs (`total_bytes_billed = 0`); the
//! CDC path adds billed `merge` / `compact` ops on the same `run_sql(sql, op,
//! table)` seam, so a billed dedup step shows on its own cost line (see
//! `docs/cdc-bigquery-load.md`). The labels flow into
//! `INFORMATION_SCHEMA.JOBS` and the billing export, so cost per
//! operation/table is one query:
//!
//! ```sql
//! SELECT
//!   (SELECT value FROM UNNEST(labels) WHERE key = 'rivet_run')   AS run,
//!   (SELECT value FROM UNNEST(labels) WHERE key = 'rivet_op')    AS op,
//!   (SELECT value FROM UNNEST(labels) WHERE key = 'rivet_table') AS tbl,
//!   COUNT(*)                              AS jobs,
//!   SUM(total_bytes_billed)               AS bytes_billed,
//!   SUM(total_bytes_billed) / POW(1024, 4) * 6.25 AS est_usd  -- ~$6.25/TiB on-demand
//! FROM `region-us`.INFORMATION_SCHEMA.JOBS
//! WHERE EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'managed_by' AND value = 'rivet')
//! GROUP BY run, op, tbl ORDER BY run, bytes_billed DESC;
//! ```
//!
//! Transport is the `bq` CLI (Google Cloud SDK) — the same tool the OSS
//! BigQuery live tests use, so auth is whatever `bq` is configured with (ADC /
//! service account) and no credentials touch this crate.

use super::TargetLoader;
use crate::types::target::TargetColumnSpec;
use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::process::{Command, Output};
// ── BigQuery ─────────────────────────────────────────────────────────────────

/// Maximum clustering columns BigQuery allows.
const MAX_CLUSTER_COLUMNS: usize = 4;

/// BigQuery's hard cap on partitions modified by a single job.
const DEFAULT_MAX_PARTITIONS_PER_JOB: usize = 4000;

/// Loads Rivet Parquet into a BigQuery dataset via the `bq` CLI.
#[derive(Debug, Clone)]
pub struct BigQueryLoader {
    pub project: String,
    pub dataset: String,
    /// Partition expression for table creation, e.g. `DATE(created_at)` or a
    /// `DATE`/`TIMESTAMP` column. Applied only when the table is created.
    pub partition_by: Option<String>,
    /// Up to 4 clustering columns. Applied only when the table is created.
    pub cluster_by: Vec<String>,
    /// Load-run correlation id, emitted as the automatic `rivet_run:<id>` job
    /// label so every job of one `rivet load` invocation shares a run key —
    /// cost slices per run (across tables) as well as per table. `None` omits
    /// the label entirely.
    pub run_id: Option<String>,
    /// Max distinct partitions a single load job may create — BigQuery's hard
    /// limit is 4,000. When a daily-partitioned, Hive-prefixed input
    /// (`<col>=YYYY-MM-DD/…`, as rivet's `partition_by` writes) spans more than
    /// this, the free load is split into several `LOAD DATA` jobs, each under
    /// the cap.
    pub max_partitions_per_job: usize,
}

impl BigQueryLoader {
    pub fn new(project: impl Into<String>, dataset: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            dataset: dataset.into(),
            partition_by: None,
            cluster_by: Vec::new(),
            run_id: None,
            max_partitions_per_job: DEFAULT_MAX_PARTITIONS_PER_JOB,
        }
    }

    pub fn partition_by(mut self, expr: impl Into<String>) -> Self {
        self.partition_by = Some(expr.into());
        self
    }

    /// Set the load-run correlation id, emitted as the `rivet_run` job label.
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    pub fn cluster_by(mut self, columns: Vec<String>) -> Self {
        self.cluster_by = columns;
        self
    }

    /// Run `bq --project_id=<p> <args…>`. On failure, `bq` prints the actual
    /// reason (e.g. "Too many partitions … allowed 4000") to **stdout** while
    /// stderr carries only the "Waiting…/DONE" spinner — so the error detail
    /// combines both streams (spinner lines stripped).
    fn run_bq(&self, args: &[String]) -> Result<Output> {
        let out = Command::new("bq")
            .arg(format!("--project_id={}", self.project))
            .args(args)
            .output()
            .context("failed to run `bq` — is the Google Cloud SDK installed and on PATH?")?;
        if !out.status.success() {
            let detail = [clean_bq_output(&out.stdout), clean_bq_output(&out.stderr)]
                .into_iter()
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(" | ");
            bail!(
                "bq {} failed: {detail}",
                args.first()
                    .map(String::as_str)
                    .unwrap_or("<no-subcommand>"),
            );
        }
        Ok(out)
    }

    /// The automatic + user labels for a job, as repeated `--label k:v` args.
    fn label_flags(&self, op: &str, table: &str) -> Vec<String> {
        build_label_flags(op, table, self.run_id.as_deref())
    }

    /// Run a SQL statement (free `LOAD DATA` load job or a billed CTAS/query),
    /// tagged with `rivet_op:<op>` + `rivet_table:<table>` for cost attribution.
    fn run_sql(&self, sql: &str, op: &str, table: &str) -> Result<Output> {
        self.run_bq(&query_args(sql, &self.label_flags(op, table)))
            .map_err(augment_partition_limit)
    }

    fn count_rows(&self, fqtn: &str, table: &str) -> Result<u64> {
        // COUNT(*) reads table metadata — 0 bytes billed.
        let out = self.run_bq(&count_args(fqtn, &self.label_flags("count", table)))?;
        parse_count_csv(&String::from_utf8_lossy(&out.stdout))
    }

    /// Split `uris` into free-load batches that each stay under the per-job
    /// partition cap. Splits only when partitioning on a bare column whose
    /// Hive `<col>=value/` prefix is present on the URIs and the distinct
    /// partition count exceeds the cap; otherwise the whole set is one batch
    /// (non-Hive inputs load in one job, as before).
    fn plan_load_batches(&self, uris: &[String]) -> Vec<Vec<String>> {
        match self.partition_by.as_deref() {
            Some(col) if is_bare_column(col) => {
                plan_hive_batches(uris, col, self.max_partitions_per_job)
                    .unwrap_or_else(|_| vec![uris.to_vec()])
            }
            _ => vec![uris.to_vec()],
        }
    }
}

impl TargetLoader for BigQueryLoader {
    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}.{}", self.project, self.dataset, table)
    }

    fn materialize(&self, table: &str, specs: &[TargetColumnSpec], uris: &[String]) -> Result<u64> {
        if self.cluster_by.len() > MAX_CLUSTER_COLUMNS {
            bail!(
                "BigQuery allows at most {MAX_CLUSTER_COLUMNS} clustering columns, got {}",
                self.cluster_by.len()
            );
        }
        let target = self.fqtn(table);
        let schema = build_schema(specs);

        // ONE free path: declaring each column's native `target_type` inline in
        // LOAD DATA makes BigQuery coerce the Parquet on load — JSON, DATETIME,
        // NUMERIC, … land natively for FREE (a load job, not a query). A
        // daily-partitioned, Hive-prefixed input over the per-job partition cap
        // is split into several free LOAD DATA jobs: batch 0 OVERWRITEs the
        // table, later batches append so they add to — not clobber — it.
        for (i, batch) in self.plan_load_batches(uris).iter().enumerate() {
            let sql = build_load_data_sql(
                &target,
                i == 0, // overwrite the first batch, append the rest
                &schema,
                &self.partition_by,
                &self.cluster_by,
                batch,
            );
            self.run_sql(&sql, "load", table)?;
        }
        // ponytail: rows via COUNT(*) (a 0-byte-billed metadata read); can become
        // the load job's `outputRows` (also metadata) behind this seam, no driver
        // change.
        self.count_rows(&target, table)
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        pk: &[String],
    ) -> Result<u64> {
        use crate::load::cdc::Warehouse;
        // Full change-log schema: rivet's `__op`/`__pos`/`__seq` meta columns
        // (not reported by `rivet check`) ahead of the resolved data columns.
        let mut full = crate::load::cdc::meta_column_specs(Warehouse::BigQuery);
        full.extend(
            specs
                .iter()
                .filter(|s| !is_meta_column(&s.column_name))
                .cloned(),
        );
        let schema = build_schema(&full);

        let changes = format!("{table}__changes");
        let changes_fqtn = self.fqtn(&changes);

        // Ensure the append-only log exists, clustered on the PK so the dedup
        // view prunes efficiently. Idempotent: created once, appended forever.
        let create = build_create_changes_sql(&changes_fqtn, &schema, pk);
        self.run_sql(&create, "create", &changes)?;

        // Count before / append (free LOAD DATA INTO) / count after — the delta
        // is what THIS load added; the driver gates it against the manifest total.
        let before = self.count_rows(&changes_fqtn, &changes)?;
        let load = build_load_data_sql(&changes_fqtn, false, &schema, &None, &[], uris);
        self.run_sql(&load, "load", &changes)?;
        let after = self.count_rows(&changes_fqtn, &changes)?;
        Ok(after.saturating_sub(before))
    }

    fn warehouse(&self) -> crate::load::cdc::Warehouse {
        crate::load::cdc::Warehouse::BigQuery
    }

    fn create_view(&self, table: &str, view_sql: &str) -> Result<()> {
        self.run_sql(view_sql, "view", table)?;
        Ok(())
    }
}

/// Whether a column name is one of rivet's CDC meta columns — filtered out of
/// the data specs before the meta columns are prepended, so a schema can never
/// declare `__op`/`__pos`/`__seq` twice.
fn is_meta_column(name: &str) -> bool {
    matches!(name, "__op" | "__pos" | "__seq")
}

/// `CREATE TABLE IF NOT EXISTS` for the change log, clustered on the PK (capped
/// at BigQuery's 4 clustering columns). Idempotent — the log is created once and
/// appended to on every CDC load.
fn build_create_changes_sql(fqtn: &str, schema: &str, pk: &[String]) -> String {
    let cluster_cols = pk
        .iter()
        .take(MAX_CLUSTER_COLUMNS)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE IF NOT EXISTS `{fqtn}` (\n{schema}\n)\nCLUSTER BY {cluster_cols};")
}

/// Whether `c` is a bare column identifier (so it matches a Hive path key),
/// not an expression like `DATE(x)` or `DATE_TRUNC(d, MONTH)`.
fn is_bare_column(c: &str) -> bool {
    !c.is_empty() && c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// The Hive partition value for `column` in a URI path, e.g.
/// `gs://b/t/d=2023-01-01/part-0.parquet` + `d` → `2023-01-01`.
fn hive_partition_value(uri: &str, column: &str) -> Option<String> {
    let needle = format!("{column}=");
    uri.split('/')
        .find_map(|seg| seg.strip_prefix(&needle).map(str::to_string))
}

/// Group `uris` so each batch holds at most `max` distinct Hive partition
/// values of `column`. URIs sharing a value stay together. Errors if any URI
/// lacks the `<column>=` segment (caller falls back to a single batch).
fn plan_hive_batches(uris: &[String], column: &str, max: usize) -> Result<Vec<Vec<String>>> {
    let pairs: Vec<(&String, String)> = uris
        .iter()
        .map(|u| {
            hive_partition_value(u, column)
                .map(|v| (u, v))
                .ok_or_else(|| anyhow::anyhow!("uri has no `{column}=` Hive segment: {u}"))
        })
        .collect::<Result<_>>()?;

    let mut values: Vec<&str> = pairs.iter().map(|(_, v)| v.as_str()).collect();
    values.sort_unstable();
    values.dedup();
    if values.len() <= max {
        return Ok(vec![uris.to_vec()]);
    }

    // Contiguous windows of `max` distinct (sorted) values → one batch each.
    let batch_of: HashMap<&str, usize> = values
        .iter()
        .enumerate()
        .map(|(i, v)| (*v, i / max))
        .collect();
    let mut batches: Vec<Vec<String>> = vec![Vec::new(); values.len().div_ceil(max)];
    for (u, v) in &pairs {
        batches[batch_of[v.as_str()]].push((*u).clone());
    }
    Ok(batches)
}

/// `PARTITION BY … / CLUSTER BY …` clauses (empty when unset). Both apply only
/// at table creation, per BigQuery.
fn table_shape_clauses(partition_by: &Option<String>, cluster_by: &[String]) -> String {
    let mut s = String::new();
    if let Some(expr) = partition_by {
        s.push_str(&format!("\nPARTITION BY {expr}"));
    }
    if !cluster_by.is_empty() {
        s.push_str(&format!("\nCLUSTER BY {}", cluster_by.join(", ")));
    }
    s
}

/// A `FROM FILES(...)` Parquet source list.
fn from_files(uris: &[String]) -> String {
    let list = uris
        .iter()
        .map(|u| format!("    '{u}'"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!("FROM FILES (\n  format = 'PARQUET',\n  uris = [\n{list}\n  ]\n)")
}

/// The BigQuery column schema declared inline in LOAD DATA, from each spec's
/// native `target_type`. Declaring native types makes BigQuery coerce the
/// Parquet on load — for FREE (a load job, not a query) — so JSON / DATETIME /
/// TIME / NUMERIC / … land natively without a post-load CTAS. Verified live.
fn build_schema(specs: &[TargetColumnSpec]) -> String {
    specs
        .iter()
        .map(|s| format!("  {} {}", s.column_name, s.target_type))
        .collect::<Vec<_>>()
        .join(",\n")
}

/// A free `LOAD DATA` batch-load statement declaring the native `schema`, so
/// BigQuery coerces the Parquet to native types on load.
fn build_load_data_sql(
    fqtn: &str,
    overwrite: bool,
    schema: &str,
    partition_by: &Option<String>,
    cluster_by: &[String],
    uris: &[String],
) -> String {
    let kw = if overwrite { "OVERWRITE" } else { "INTO" };
    let clauses = table_shape_clauses(partition_by, cluster_by);
    format!(
        "LOAD DATA {kw} `{fqtn}` (\n{schema}\n){clauses}\n{};",
        from_files(uris)
    )
}

fn query_args(sql: &str, labels: &[String]) -> Vec<String> {
    // Labels are flags — they MUST precede the positional SQL string.
    let mut a = vec![
        "query".into(),
        "--use_legacy_sql=false".into(),
        "--format=none".into(),
    ];
    a.extend_from_slice(labels);
    a.push(sql.into());
    a
}

fn count_args(fqtn: &str, labels: &[String]) -> Vec<String> {
    let mut a = vec![
        "query".into(),
        "--use_legacy_sql=false".into(),
        "--format=csv".into(),
    ];
    a.extend_from_slice(labels);
    a.push(format!("SELECT COUNT(*) AS n FROM `{fqtn}`"));
    a
}

/// Build `--label k:v` args: the automatic `managed_by:rivet` /
/// `rivet_op:<op>` / `rivet_table:<table>` labels, the `rivet_run:<id>` label
/// when a run id is set, plus any `extra` (user) labels. These land in
/// `INFORMATION_SCHEMA.JOBS.labels` and the billing export, so cost can be
/// attributed per run and per table.
fn build_label_flags(op: &str, table: &str, run_id: Option<&str>) -> Vec<String> {
    let mut labels: Vec<(String, String)> = vec![
        ("managed_by".into(), "rivet".into()),
        ("rivet_op".into(), sanitize_label(op)),
        ("rivet_table".into(), sanitize_label(table)),
    ];
    if let Some(id) = run_id {
        labels.push(("rivet_run".into(), sanitize_label(id)));
    }
    labels
        .into_iter()
        .flat_map(|(k, v)| ["--label".to_string(), format!("{k}:{v}")])
        .collect()
}

/// Coerce a string into BigQuery's label charset: lowercase `[a-z0-9_-]`, other
/// characters become `_`, truncated to 63 chars. Empty maps to `unnamed`.
fn sanitize_label(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    out.truncate(63);
    if out.is_empty() {
        "unnamed".clone_into(&mut out);
    }
    out
}

/// Parse a `bq query --format=csv` count result: a `n` header line then the
/// value. Take the last line that parses as an integer.
fn parse_count_csv(stdout: &str) -> Result<u64> {
    stdout
        .lines()
        .rev()
        .find_map(|l| l.trim().parse::<u64>().ok())
        .context("could not parse a row count from bq output")
}

/// Normalize `bq` output for an error message: split the `\r`-driven progress
/// spinner into lines, drop the "Waiting…/Current status:" noise, and join the
/// rest — leaving the real error `bq` printed (e.g. the partition-quota text).
fn clean_bq_output(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .replace('\r', "\n")
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("Waiting on") && !l.contains("Current status:"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Turn BigQuery's partition-quota failure into an actionable error.
fn augment_partition_limit(e: anyhow::Error) -> anyhow::Error {
    let s = e.to_string().to_lowercase();
    if s.contains("partition")
        && (s.contains("4000") || s.contains("quota") || s.contains("exceed"))
    {
        return e.context(
            "BigQuery caps a single load/query job at 4,000 modified partitions — split the \
             Parquet URIs into batches whose partition span is <= 4,000 (e.g. load by date range)",
        );
    }
    e
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::target::TargetStatus;

    fn spec(name: &str, cast: Option<&str>, status: TargetStatus) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.into(),
            target_type: "X".into(),
            autoload_type: "Y".into(),
            status,
            note: None,
            cast_sql: cast.map(String::from),
        }
    }

    fn uris() -> Vec<String> {
        vec!["gs://b/a.parquet".into(), "gs://b/b.parquet".into()]
    }

    fn typed(name: &str, target_type: &str) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.into(),
            target_type: target_type.into(),
            autoload_type: "BYTES".into(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        }
    }

    #[test]
    fn schema_declares_each_columns_native_target_type() {
        let s = build_schema(&[
            typed("id", "INT64"),
            typed("json_col", "JSON"),
            typed("dt_col", "DATETIME"),
        ]);
        assert!(s.contains("id INT64"));
        assert!(s.contains("json_col JSON"));
        assert!(s.contains("dt_col DATETIME"));
    }

    #[test]
    fn load_data_declares_native_schema_and_is_a_free_batch_load() {
        let schema = build_schema(&[typed("id", "INT64"), typed("json_col", "JSON")]);
        let sql = build_load_data_sql("p.d.orders", true, &schema, &None, &[], &uris());
        assert!(sql.starts_with("LOAD DATA OVERWRITE `p.d.orders` ("));
        // Native types declared inline → BigQuery coerces on load, for free.
        assert!(sql.contains("json_col JSON"));
        assert!(sql.contains("format = 'PARQUET'"));
        assert!(sql.contains("'gs://b/a.parquet'"));
        assert!(!sql.contains("PARTITION BY"));
    }

    #[test]
    fn load_data_append_uses_into() {
        let schema = build_schema(&[typed("id", "INT64")]);
        let sql = build_load_data_sql("p.d.orders", false, &schema, &None, &[], &uris());
        assert!(sql.starts_with("LOAD DATA INTO `p.d.orders`"));
    }

    #[test]
    fn load_data_emits_partition_and_cluster_when_configured() {
        let schema = build_schema(&[typed("id", "INT64")]);
        let sql = build_load_data_sql(
            "p.d.orders",
            true,
            &schema,
            &Some("DATE(created_at)".into()),
            &["customer_id".into(), "region".into()],
            &uris(),
        );
        assert!(sql.contains("PARTITION BY DATE(created_at)"));
        assert!(sql.contains("CLUSTER BY customer_id, region"));
    }

    #[test]
    fn create_changes_clusters_on_pk_capped_at_four_columns() {
        let schema = build_schema(&[typed("__op", "STRING"), typed("id", "INT64")]);
        let sql = build_create_changes_sql("p.d.orders__changes", &schema, &["id".into()]);
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `p.d.orders__changes` ("));
        assert!(sql.contains("CLUSTER BY id"));
        // A >4-column PK is capped to BigQuery's clustering limit.
        let wide: Vec<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sql2 = build_create_changes_sql("t", &schema, &wide);
        assert!(sql2.contains("CLUSTER BY a, b, c, d"));
        assert!(!sql2.contains(", e"));
    }

    #[test]
    fn is_meta_column_matches_only_the_three_cdc_columns() {
        assert!(is_meta_column("__op") && is_meta_column("__pos") && is_meta_column("__seq"));
        assert!(!is_meta_column("id") && !is_meta_column("__op_code"));
    }

    #[test]
    fn count_csv_skips_header() {
        assert_eq!(parse_count_csv("n\n42\n").unwrap(), 42);
        assert_eq!(parse_count_csv("n\n0\n").unwrap(), 0);
        assert!(parse_count_csv("n\n").is_err());
    }

    #[test]
    fn clean_bq_output_drops_standalone_status_and_waiting_lines() {
        // A bare "Waiting on" line (no status) AND a bare "Current status:" line
        // (not part of a Waiting line) must BOTH be dropped — pins each `&&` in
        // the filter (an `||` there would leak one of them into the message).
        let raw = b"Waiting on bqjob_x\nCurrent status: RUNNING\nError: boom\n";
        assert_eq!(clean_bq_output(raw), "Error: boom");
    }

    #[test]
    fn augment_partition_limit_fires_only_on_partition_plus_signal() {
        let aug = |m: &str| augment_partition_limit(anyhow::anyhow!("{m}")).to_string();
        // partition + exactly one of {4000, quota, exceed} → augmented (pins each `||`).
        assert!(aug("too many partitions, allowed 4000").contains("split the"));
        assert!(aug("partition quota reached").contains("split the"));
        assert!(aug("partition count will exceed the limit").contains("split the"));
        // partition alone, or a signal alone → NOT augmented (pins the outer `&&`).
        assert!(!aug("partition pruning is disabled").contains("split the"));
        assert!(!aug("row quota 4000 reached").contains("split the"));
    }

    #[test]
    fn partition_limit_error_is_augmented() {
        let raw = anyhow::anyhow!("Too many partitions: cannot modify more than 4000 partitions");
        let msg = augment_partition_limit(raw).to_string();
        assert!(
            msg.contains("split the"),
            "expected the actionable hint: {msg}"
        );
    }

    #[test]
    fn job_labels_tag_managed_by_op_and_table() {
        let flags = build_label_flags("recover", "Orders", Some("Run-7"));
        let kv: Vec<&String> = flags.iter().skip(1).step_by(2).collect();
        assert!(kv.iter().any(|s| *s == "managed_by:rivet"));
        assert!(kv.iter().any(|s| *s == "rivet_op:recover"));
        assert!(kv.iter().any(|s| *s == "rivet_table:orders")); // sanitized to lowercase
        assert!(kv.iter().any(|s| *s == "rivet_run:run-7")); // sanitized to lowercase
        // Each label value is preceded by a `--label` flag.
        assert!(flags.iter().step_by(2).all(|s| s == "--label"));
    }

    #[test]
    fn no_run_id_omits_the_rivet_run_label() {
        let flags = build_label_flags("load", "orders", None);
        let kv: Vec<&String> = flags.iter().skip(1).step_by(2).collect();
        assert!(kv.iter().any(|s| *s == "rivet_table:orders"));
        assert!(!kv.iter().any(|s| s.starts_with("rivet_run:")));
    }

    #[test]
    fn fqtn_qualifies_project_dataset_table() {
        let l = BigQueryLoader::new("proj", "ds");
        assert_eq!(l.fqtn("orders"), "proj.ds.orders");
    }

    #[test]
    fn sanitize_label_coerces_to_bq_charset() {
        assert_eq!(sanitize_label("My.Table!"), "my_table_");
        assert_eq!(sanitize_label(""), "unnamed");
        assert_eq!(sanitize_label("ok-name_1"), "ok-name_1");
        assert_eq!(sanitize_label(&"x".repeat(80)).len(), 63);
    }

    #[test]
    fn clean_bq_output_keeps_real_error_drops_spinner() {
        // Regression: bq prints the failure reason on STDOUT; stderr is just
        // the spinner. run_bq must surface stdout so augment_partition_limit
        // can see the quota text (live-caught: the reason was being dropped).
        let stdout = b"Error in query string: Too many partitions produced by query, \
                       allowed 4000, query produces at least 4200 partitions";
        let cleaned = clean_bq_output(stdout);
        assert!(cleaned.contains("Too many partitions") && cleaned.contains("4000"));
        // The augment fires end-to-end on the cleaned stdout.
        let augmented = augment_partition_limit(anyhow::anyhow!("{cleaned}")).to_string();
        assert!(augmented.contains("split the"), "{augmented}");
        // The stderr spinner collapses away.
        let stderr = "Waiting on bqjob_x ... (0s) Current status: RUNNING\r\
                      Waiting on bqjob_x ... (0s) Current status: DONE";
        assert!(clean_bq_output(stderr.as_bytes()).is_empty());
    }

    #[test]
    fn materialize_refuses_too_many_cluster_columns() {
        // A >4-column CLUSTER BY is a below-the-seam adapter limit (BigQuery's),
        // caught in `materialize` before any `bq` call. (Empty-URI and Fail-spec
        // refusals are the driver's — see `load::tests`.)
        let l = BigQueryLoader::new("p", "d").cluster_by(vec![
            "a".into(),
            "b".into(),
            "c".into(),
            "d".into(),
            "e".into(),
        ]);
        let err = l
            .materialize("t", &[spec("id", None, TargetStatus::Ok)], &uris())
            .unwrap_err()
            .to_string();
        assert!(err.contains("clustering"), "{err}");
    }

    #[test]
    fn hive_partition_value_parses_col_segment() {
        assert_eq!(
            hive_partition_value("gs://b/t/d=2023-01-01/part-0.parquet", "d").as_deref(),
            Some("2023-01-01")
        );
        assert_eq!(
            hive_partition_value("gs://b/t/created_at=2023-01-01/p.parquet", "created_at")
                .as_deref(),
            Some("2023-01-01")
        );
        assert!(hive_partition_value("gs://b/t/part-0.parquet", "d").is_none());
    }

    #[test]
    fn is_bare_column_rejects_expressions() {
        assert!(is_bare_column("d"));
        assert!(is_bare_column("created_at"));
        assert!(!is_bare_column("DATE(d)"));
        assert!(!is_bare_column("DATE_TRUNC(d, MONTH)"));
        assert!(!is_bare_column(""));
    }

    #[test]
    fn hive_batches_split_by_distinct_partition_cap() {
        // 5 distinct days (day 01-01 has 2 files), cap 2 → 3 batches.
        let uris: Vec<String> = [
            "gs://b/t/d=2023-01-01/a.parquet",
            "gs://b/t/d=2023-01-01/b.parquet",
            "gs://b/t/d=2023-01-02/a.parquet",
            "gs://b/t/d=2023-01-03/a.parquet",
            "gs://b/t/d=2023-01-04/a.parquet",
            "gs://b/t/d=2023-01-05/a.parquet",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let batches = plan_hive_batches(&uris, "d", 2).unwrap();
        assert_eq!(batches.len(), 3);
        for b in &batches {
            let mut days: Vec<_> = b
                .iter()
                .map(|u| hive_partition_value(u, "d").unwrap())
                .collect();
            days.sort();
            days.dedup();
            assert!(
                days.len() <= 2,
                "batch touches {} distinct days",
                days.len()
            );
        }
        // Files that share a day stay together; the union is the whole input.
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), uris.len());
    }

    #[test]
    fn hive_batches_single_when_under_cap() {
        let uris = vec![
            "gs://b/t/d=2023-01-01/a.parquet".to_string(),
            "gs://b/t/d=2023-01-02/a.parquet".to_string(),
        ];
        assert_eq!(plan_hive_batches(&uris, "d", 4000).unwrap().len(), 1);
    }

    #[test]
    fn hive_batches_error_when_uri_lacks_segment() {
        let uris = vec!["gs://b/t/no-hive/a.parquet".to_string()];
        assert!(plan_hive_batches(&uris, "d", 2).is_err());
    }

    /// Live BigQuery load. Requires the `bq` CLI + ADC, a dataset, and a GCS
    /// Parquet URI. NOT run offline; drive it with:
    ///
    ///   BIGQUERY_TEST_PROJECT=my-proj RIVET_BQ_TEST_DATASET=rivet_test \
    ///   RIVET_BQ_TEST_PARQUET_URI=gs://bucket/orders/part-0.parquet \
    ///   cargo test -- --ignored bigquery_live
    #[test]
    #[ignore = "live: needs bq CLI + ADC + a GCS Parquet fixture"]
    fn bigquery_live_load_round_trips() {
        let project = std::env::var("BIGQUERY_TEST_PROJECT")
            .expect("set BIGQUERY_TEST_PROJECT for the live BigQuery test");
        let dataset =
            std::env::var("RIVET_BQ_TEST_DATASET").unwrap_or_else(|_| "rivet_test".to_string());
        let uri = std::env::var("RIVET_BQ_TEST_PARQUET_URI").expect(
            "set RIVET_BQ_TEST_PARQUET_URI to a GCS Parquet object matching the specs below",
        );

        // A plain column (no cast) exercises the FREE LOAD DATA path.
        let specs = vec![spec("id", None, TargetStatus::Ok)];

        let loader = BigQueryLoader::new(project, dataset);
        // Drive it through the real driver (no gate, no cleanup) — same path prod
        // takes, exercising validate → materialize.
        let report =
            crate::load::run_load(&loader, "rivet_bq_live_test", &specs, &[uri], None, None)
                .expect("live load should succeed");
        assert!(
            report.rows_loaded > 0,
            "expected rows, got {}",
            report.rows_loaded
        );
    }

    /// Live BigQuery CDC round-trip: append a change-log Parquet into
    /// `<table>__changes` and build the dedup view. Loading the **same** file
    /// twice exercises the at-least-once path — `<table>__changes` doubles, but
    /// the current-state view must be unchanged (duplicates lose the
    /// `(__pos,__seq)` tiebreak). Soft delete: the view keeps one row per PK
    /// including tombstones (`__is_deleted = true`), so `RIVET_BQ_CDC_EXPECTED_STATE`
    /// is the distinct-PK count *including* deleted rows. Drive it with:
    ///
    ///   BIGQUERY_TEST_PROJECT=my-proj RIVET_BQ_TEST_DATASET=rivet_test \
    ///   RIVET_BQ_CDC_PARQUET_URI=gs://bucket/orders_cdc/part-0.parquet \
    ///   RIVET_BQ_CDC_PK=id RIVET_BQ_CDC_DATA_COLS=id:INT64,val:STRING \
    ///   RIVET_BQ_CDC_EXPECTED_STATE=3 \
    ///   cargo test -- --ignored bigquery_live_cdc
    #[test]
    #[ignore = "live: needs bq CLI + ADC + a CDC change-log Parquet fixture"]
    fn bigquery_live_cdc_view_dedups_at_least_once() {
        let project = std::env::var("BIGQUERY_TEST_PROJECT")
            .expect("set BIGQUERY_TEST_PROJECT for the live BigQuery CDC test");
        let dataset =
            std::env::var("RIVET_BQ_TEST_DATASET").unwrap_or_else(|_| "rivet_test".to_string());
        let uri = std::env::var("RIVET_BQ_CDC_PARQUET_URI")
            .expect("set RIVET_BQ_CDC_PARQUET_URI to a CDC change-log Parquet object");
        let pk = std::env::var("RIVET_BQ_CDC_PK").unwrap_or_else(|_| "id".to_string());
        // The fixture's data columns as `name:TYPE,name:TYPE` (meta columns are
        // prepended by the loader). Defaults to a minimal `id:INT64`.
        let data_cols =
            std::env::var("RIVET_BQ_CDC_DATA_COLS").unwrap_or_else(|_| "id:INT64".to_string());
        let specs: Vec<TargetColumnSpec> = data_cols
            .split(',')
            .map(|c| {
                let (name, ty) = c.split_once(':').expect("data col must be name:TYPE");
                typed(name, ty)
            })
            .collect();
        let expected_state: u64 = std::env::var("RIVET_BQ_CDC_EXPECTED_STATE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let table = "rivet_bq_live_cdc_test";
        let pk_cols: Vec<String> = pk.split(',').map(str::to_string).collect();
        let loader = BigQueryLoader::new(&project, &dataset);

        // Load the same change log twice (at-least-once). No delta gate here —
        // the fixture's row count is the operator's to assert externally.
        crate::load::run_load_cdc(
            &loader,
            table,
            &specs,
            std::slice::from_ref(&uri),
            &pk_cols,
            crate::load::cdc::SourceEngine::MySql,
            None,
            None,
        )
        .expect("first CDC append + view build should succeed");
        let second = crate::load::run_load_cdc(
            &loader,
            table,
            &specs,
            &[uri],
            &pk_cols,
            crate::load::cdc::SourceEngine::MySql,
            None,
            None,
        )
        .expect("second CDC append (at-least-once) should succeed");
        assert!(second.rows_appended > 0, "second append added rows");

        // The dedup VIEW must report the current state, independent of how many
        // times the log was appended.
        let state_rows = loader
            .count_rows(&second.view, table)
            .expect("counting the dedup view should succeed");
        if expected_state > 0 {
            assert_eq!(
                state_rows, expected_state,
                "the view must collapse duplicates to {expected_state} distinct-PK rows \
                 (incl tombstones), got {state_rows}"
            );
        }
    }
}
