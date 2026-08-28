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
//! Transport is BigQuery's REST API, in process (`bq_rest`) — `jobs.insert`
//! plus a poll, on the blocking `reqwest` client. Auth comes from the SAME ADC
//! seam the GCS destination signs with (`destination::gcs_auth`), so a laptop
//! with `gcloud auth application-default login`, a container with a
//! service-account key file in `GOOGLE_APPLICATION_CREDENTIALS`, and a CI box
//! with a token all work without the Google Cloud SDK on PATH — and all three
//! run the job as the identity the operator configured, which is the property
//! that matters (a load acting as a different principal has an audit trail
//! that is fiction). The remaining shape rivet cannot mint in process
//! (`external_account` / workload identity, which needs an STS exchange) falls
//! back to `gcloud auth print-access-token` — a TOKEN, not the transport; see
//! `bq_rest::mint_token_via_gcloud_cli`.

use super::TargetLoader;
use super::bq_rest::BigQueryApi;
use crate::types::target::TargetColumnSpec;
use anyhow::{Result, bail};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
// ── BigQuery ─────────────────────────────────────────────────────────────────

/// Maximum clustering columns BigQuery allows.
const MAX_CLUSTER_COLUMNS: usize = 4;

/// BigQuery's hard cap on partitions modified by a single job.
const DEFAULT_MAX_PARTITIONS_PER_JOB: usize = 4000;

/// Loads Rivet Parquet into a BigQuery dataset over the REST API.
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
    /// The REST client, built on first use and shared by every clone — so one
    /// access token serves a whole load instead of one per statement. Not part
    /// of the loader's identity: constructing a loader must stay free of I/O
    /// (the offline `materialize` refusal tests build one and never reach the
    /// network).
    api: Arc<OnceLock<BigQueryApi>>,
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
            api: Arc::new(OnceLock::new()),
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

    /// The REST client, built once per loader (and shared with its clones).
    ///
    /// `OnceLock::get_or_try_init` is unstable, so this is the hand-rolled
    /// equivalent: a concurrent loser's client is simply dropped — both are
    /// equivalent, and `set` never overwrites a winner.
    fn api(&self) -> Result<&BigQueryApi> {
        if let Some(api) = self.api.get() {
            return Ok(api);
        }
        let built = BigQueryApi::new(&self.project)?;
        let _ = self.api.set(built);
        Ok(self.api.get().expect("the client was just set"))
    }

    /// The automatic + user labels for a job, keyed for `configuration.labels`.
    fn labels(&self, op: &str, table: &str) -> BTreeMap<String, String> {
        build_labels(op, table, self.run_id.as_deref())
    }

    /// Run a SQL statement (free `LOAD DATA` load job or a billed CTAS/query),
    /// tagged with `rivet_op:<op>` + `rivet_table:<table>` for cost attribution.
    fn run_sql(&self, sql: &str, op: &str, table: &str) -> Result<()> {
        self.api()?
            .run_query(sql, &self.labels(op, table))
            .map_err(augment_partition_limit)
            .map(|_job_id| ())
    }

    fn count_rows(&self, fqtn: &str, table: &str) -> Result<u64> {
        // COUNT(*) reads table metadata — 0 bytes billed.
        self.api()?.run_query_scalar(
            &format!("SELECT COUNT(*) AS n FROM `{fqtn}`"),
            &self.labels("count", table),
        )
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
        // Gate each clustering column: it splices raw into `CLUSTER BY <cols>`
        // (an identifier list, no quoting) — the same is_safe_load_ident gate the
        // table / column / pk names get. Config-derived, so operator self-harm,
        // but gated for consistency with the round-5/6 injection surface.
        // (`partition_by` is intentionally NOT gated here — it is a BigQuery
        // partition EXPRESSION, e.g. `DATE(created_at)`, not a bare identifier.)
        for c in &self.cluster_by {
            if !super::is_safe_load_ident(c) {
                bail!(
                    "BigQuery load: clustering column `{}` is not a plain SQL identifier \
                     ([A-Za-z_][A-Za-z0-9_]*) — it splices into CLUSTER BY. Rename it.",
                    c.escape_default()
                );
            }
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

        // …and, for a log that ALREADY existed, add whatever the declared
        // schema has and it does not. The CREATE above is a no-op on such a
        // table, so without this a log rivet did not create — or one that predates a new
        // meta column — fails the LOAD below on a schema mismatch. ALTER ADD,
        // never a replace: the table may hold the customer's history.
        if let Some(alter) = build_alter_add_columns_sql(&changes_fqtn, &full) {
            self.run_sql(&alter, "alter", &changes)?;
        }

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
    crate::load::cdc::is_meta_column(name)
}

/// `CREATE TABLE IF NOT EXISTS` for the change log, clustered on the PK (capped
/// at BigQuery's 4 clustering columns). Idempotent — the log is created once and
/// appended to on every CDC load.
fn build_create_changes_sql(fqtn: &str, schema: &str, pk: &[String]) -> String {
    let cluster_cols = pk
        .iter()
        .take(MAX_CLUSTER_COLUMNS)
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE IF NOT EXISTS `{fqtn}` (\n{schema}\n)\nCLUSTER BY {cluster_cols};")
}

/// Bring an EXISTING table's schema up to the declared one by ADDING what is
/// missing — never by replacing the table.
///
/// `CREATE TABLE IF NOT EXISTS` is a no-op on a table that already exists, so a
/// table rivet did not create — one an operator pointed rivet at — keeps
/// whatever shape its previous owner gave it. The next `LOAD DATA` then declares
/// columns the table does not have and fails; and a load written to overwrite
/// instead would impose our schema and destroy the customer's data. Neither is
/// acceptable on a table we were handed rather than created.
///
/// `ADD COLUMN IF NOT EXISTS` is the only verb that is safe here: additive,
/// idempotent, and metadata-only on BigQuery — no rewrite, no scan, and existing
/// rows read NULL for the new column, which is exactly the state §5i's per-key
/// fallback is built to handle.
///
/// `None` when there is nothing to add, so the caller skips the round trip
/// rather than sending a statement with an empty body.
fn build_alter_add_columns_sql(fqtn: &str, specs: &[TargetColumnSpec]) -> Option<String> {
    if specs.is_empty() {
        return None;
    }
    let adds = specs
        .iter()
        .map(|s| {
            format!(
                "ADD COLUMN IF NOT EXISTS `{}` {}",
                s.column_name, s.target_type
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    Some(format!("ALTER TABLE `{fqtn}`\n  {adds};"))
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
        let quoted: Vec<String> = cluster_by.iter().map(|c| format!("`{c}`")).collect();
        s.push_str(&format!("\nCLUSTER BY {}", quoted.join(", ")));
    }
    s
}

/// A `FROM FILES(...)` Parquet source list.
///
/// `enable_list_inference = true` collapses rivet's 3-level Parquet LIST
/// (`col.list.item`) one level, so an array column loads as the declared
/// `ARRAY<STRUCT<item T>>` (== REPEATED RECORD{item}) instead of empty. It is a
/// no-op for non-list columns, so it is always safe to set.
fn from_files(uris: &[String]) -> String {
    let list = uris
        .iter()
        .map(|u| format!("    '{u}'"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "FROM FILES (\n  format = 'PARQUET',\n  enable_list_inference = true,\n  uris = [\n{list}\n  ]\n)"
    )
}

/// The BigQuery column schema declared inline in LOAD DATA, from each spec's
/// native `target_type`. Declaring native types makes BigQuery coerce the
/// Parquet on load — for FREE (a load job, not a query) — so JSON / DATETIME /
/// TIME / NUMERIC / … land natively without a post-load CTAS. Verified live.
fn build_schema(specs: &[TargetColumnSpec]) -> String {
    // Backticked, like build_alter_add_columns_sql always was: names are
    // pre-gated to plain idents, so quoting is always safe — and without it a
    // reserved-word column (`end`, `order`, `interval`; `start`/`end` pairs
    // are everywhere) died on BigQuery's raw syntax error AFTER the extract
    // was paid, while cdc.rs promised backticks made it safe (round-6).
    specs
        .iter()
        .map(|s| format!("  `{}` {}", s.column_name, s.target_type))
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

/// The job's label SET: the automatic `managed_by:rivet` / `rivet_op:<op>` /
/// `rivet_table:<table>` labels, plus `rivet_run:<id>` when a run id is set.
/// Sent as `configuration.labels`, which is what `INFORMATION_SCHEMA.JOBS.labels`
/// and the billing export project — so cost stays attributable per run and per
/// table exactly as the module docs' query describes.
///
/// (Was `--label k:v` flag pairs under the CLI transport. The keys and values
/// are unchanged; only the wire shape moved.)
fn build_labels(op: &str, table: &str, run_id: Option<&str>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::from([
        ("managed_by".to_string(), "rivet".to_string()),
        ("rivet_op".to_string(), sanitize_label(op)),
        ("rivet_table".to_string(), sanitize_label(table)),
    ]);
    if let Some(id) = run_id {
        labels.insert("rivet_run".to_string(), sanitize_label(id));
    }
    labels
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

/// Turn BigQuery's partition-quota failure into an actionable error.
///
/// TEXT-MATCHED, deliberately: the quota comes back as an ordinary
/// `invalidQuery` job error whose MESSAGE names the limit ("Too many
/// partitions produced by query, allowed 4000, …") — the REST envelope carries
/// no distinct machine-readable code for it, so there is nothing sharper to
/// match on. `bq_rest` puts that message verbatim into the error this reads.
pub(crate) fn augment_partition_limit(e: anyhow::Error) -> anyhow::Error {
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
        assert!(s.contains("`id` INT64"));
        assert!(s.contains("`json_col` JSON"));
        assert!(s.contains("`dt_col` DATETIME"));
    }

    #[test]
    fn load_data_declares_native_schema_and_is_a_free_batch_load() {
        let schema = build_schema(&[typed("id", "INT64"), typed("json_col", "JSON")]);
        let sql = build_load_data_sql("p.d.orders", true, &schema, &None, &[], &uris());
        assert!(sql.starts_with("LOAD DATA OVERWRITE `p.d.orders` ("));
        // Native types declared inline → BigQuery coerces on load, for free.
        // Backticked (round-6): a reserved-word column must survive the DDL.
        assert!(sql.contains("`json_col` JSON"));
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
        assert!(sql.contains("CLUSTER BY `customer_id`, `region`"));
    }

    #[test]
    fn create_changes_clusters_on_pk_capped_at_four_columns() {
        let schema = build_schema(&[typed("__op", "STRING"), typed("id", "INT64")]);
        let sql = build_create_changes_sql("p.d.orders__changes", &schema, &["id".into()]);
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `p.d.orders__changes` ("));
        assert!(sql.contains("CLUSTER BY `id`"));
        // A >4-column PK is capped to BigQuery's clustering limit.
        let wide: Vec<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sql2 = build_create_changes_sql("t", &schema, &wide);
        let bt = |c: &str| format!("`{c}`");
        assert!(sql2.contains(&format!(
            "CLUSTER BY {}, {}, {}, {}",
            bt("a"),
            bt("b"),
            bt("c"),
            bt("d")
        )));
        assert!(!sql2.contains(&bt("e")));
    }

    #[test]
    fn is_meta_column_matches_only_the_three_cdc_columns() {
        assert!(is_meta_column("__op") && is_meta_column("__pos") && is_meta_column("__seq"));
        assert!(!is_meta_column("id") && !is_meta_column("__op_code"));
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

    /// The label SET is the cost-attribution contract; the transport that
    /// carries it is not. This asserted `--label k:v` CLI pairs before the REST
    /// rewrite — same keys, same (sanitized) values, now read as a map.
    #[test]
    fn job_labels_tag_managed_by_op_and_table() {
        let labels = build_labels("recover", "Orders", Some("Run-7"));
        assert_eq!(labels["managed_by"], "rivet");
        assert_eq!(labels["rivet_op"], "recover");
        assert_eq!(labels["rivet_table"], "orders"); // sanitized to lowercase
        assert_eq!(labels["rivet_run"], "run-7"); // sanitized to lowercase
        assert_eq!(
            labels.len(),
            4,
            "no label beyond the documented four: {labels:?}"
        );
    }

    #[test]
    fn no_run_id_omits_the_rivet_run_label() {
        let labels = build_labels("load", "orders", None);
        assert_eq!(labels["rivet_table"], "orders");
        assert!(!labels.contains_key("rivet_run"), "{labels:?}");
    }

    /// The labels the loader actually SENDS, taken from the loader (not
    /// hand-built), and placed in the body BigQuery reads them from. The
    /// producer-side half of the label contract: a run id that never reached
    /// `configuration.labels` is a silent loss of cost attribution — every job
    /// still runs, and the billing query returns nothing for the run.
    #[test]
    fn the_loader_sends_its_labels_in_the_job_configuration() {
        let l = BigQueryLoader::new("p", "d").run_id("Run-9");
        let body = crate::load::bq_rest::query_job_body(
            "SELECT 1",
            &l.labels("load", "Orders"),
            "p",
            None,
        );
        assert_eq!(body["configuration"]["labels"]["managed_by"], "rivet");
        assert_eq!(body["configuration"]["labels"]["rivet_op"], "load");
        assert_eq!(body["configuration"]["labels"]["rivet_table"], "orders");
        assert_eq!(body["configuration"]["labels"]["rivet_run"], "run-9");
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

    /// THE foreign-table safety test. A table rivet did not create keeps its
    /// previous owner's shape — `CREATE TABLE IF NOT EXISTS` is a no-op on it —
    /// so the only way to add a column is ALTER. A replace would impose our
    /// schema on the customer's history and destroy it.
    #[test]
    fn schema_reconciliation_adds_columns_and_never_replaces() {
        let specs = [
            spec("id", None, TargetStatus::Ok),
            spec("_rivet_row_hash", None, TargetStatus::Ok),
        ];
        let sql = build_alter_add_columns_sql("p.d.t__changes", &specs).unwrap();
        assert!(sql.starts_with("ALTER TABLE `p.d.t__changes`"), "{sql}");
        // IF NOT EXISTS on every column: the statement runs on every load, and
        // a load must not fail because a column it declares is already there.
        assert_eq!(sql.matches("ADD COLUMN IF NOT EXISTS").count(), 2, "{sql}");
        assert!(sql.contains("`_rivet_row_hash` X"), "{sql}");
        for forbidden in ["REPLACE", "DROP", "CREATE", "TRUNCATE", "OVERWRITE"] {
            assert!(
                !sql.contains(forbidden),
                "reconciliation must be additive only, found {forbidden}: {sql}"
            );
        }
    }

    /// Nothing to add ⇒ no statement, so the loader skips the round trip
    /// instead of sending `ALTER TABLE t ;`.
    #[test]
    fn schema_reconciliation_emits_nothing_for_an_empty_spec_list() {
        assert!(build_alter_add_columns_sql("p.d.t", &[]).is_none());
    }

    /// The changelog is only ever CREATEd IF NOT EXISTS and LOADed INTO —
    /// never OVERWRITE. This pins the pairing: a pre-existing `__changes` table
    /// must survive a load with its rows intact.
    #[test]
    fn changelog_sql_is_create_if_not_exists_plus_append_only() {
        let create = build_create_changes_sql("p.d.t__changes", "  `id` INT64", &["id".into()]);
        assert!(create.starts_with("CREATE TABLE IF NOT EXISTS"), "{create}");
        let load =
            build_load_data_sql("p.d.t__changes", false, "  `id` INT64", &None, &[], &uris());
        assert!(load.starts_with("LOAD DATA INTO"), "{load}");
        assert!(!load.contains("OVERWRITE"), "{load}");
    }

    #[test]
    fn materialize_refuses_too_many_cluster_columns() {
        // A >4-column CLUSTER BY is a below-the-seam adapter limit (BigQuery's),
        // caught in `materialize` before any BigQuery job is enqueued. (Empty-URI and Fail-spec
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
    fn materialize_refuses_a_non_identifier_cluster_column() {
        // A clustering column splices raw into `CLUSTER BY <cols>`; a
        // non-identifier name is an injection vector and must be refused in
        // `materialize` before any BigQuery job is enqueued — the sibling of the table/column/pk
        // gate for the BigQuery shape clause.
        let l = BigQueryLoader::new("p", "d").cluster_by(vec!["id) FROM secrets; --".into()]);
        let err = l
            .materialize("t", &[spec("id", None, TargetStatus::Ok)], &uris())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not a plain SQL identifier") && err.contains("CLUSTER BY"),
            "{err}"
        );
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

    /// Live BigQuery load. Requires ADC, a dataset, and a GCS Parquet URI —
    /// the transport is the REST API, so no `bq` CLI on PATH. NOT run offline;
    /// drive it with:
    ///
    ///   BIGQUERY_TEST_PROJECT=my-proj RIVET_BQ_TEST_DATASET=rivet_test \
    ///   RIVET_BQ_TEST_PARQUET_URI=gs://bucket/orders/part-0.parquet \
    ///   cargo test -- --ignored bigquery_live
    #[test]
    #[ignore = "live: needs a BigQuery project + ADC + a GCS Parquet fixture"]
    fn bigquery_live_load_round_trips() {
        // Soft-skip when the live BigQuery project isn't configured: CI sweeps
        // `--ignored` (ci.yml) without warehouse creds, so a hard `.expect` here
        // would fail the run. With the project set (a live/nightly box) it runs.
        let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
            eprintln!("skipping bigquery_live_load_round_trips: BIGQUERY_TEST_PROJECT unset");
            return;
        };
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

    /// THE live proof of the REST transport, needing no GCS fixture: a real
    /// query job through `jobs.insert` → poll → `getQueryResults`, then the
    /// cost-attribution labels read back from BigQuery's OWN catalog
    /// (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`) rather than from the request body
    /// this crate built — an independent oracle for the one contract the CLI
    /// rewrite could silently drop. Also drives the failure path, so the error
    /// mapping is exercised against a real `status.errorResult` and not only a
    /// fixture. Drive it with:
    ///
    ///   BIGQUERY_TEST_PROJECT=rivet-data-tool RIVET_BQ_TEST_DATASET=rivet_type_lab \
    ///   BIGQUERY_TEST_LOCATION=EU \
    ///   cargo test --lib -- --ignored bigquery_rest_transport_live
    #[test]
    #[ignore = "live: needs a BigQuery project + ADC (no GCS fixture required)"]
    fn bigquery_rest_transport_live_round_trips_a_query_job() {
        // Soft-skip when unconfigured — see bigquery_live_load_round_trips.
        let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
            eprintln!("skipping bigquery_rest_transport_live: BIGQUERY_TEST_PROJECT unset");
            return;
        };
        let dataset = std::env::var("RIVET_BQ_TEST_DATASET")
            .or_else(|_| std::env::var("BIGQUERY_TEST_DATASET"))
            .unwrap_or_else(|_| "rivet_test".to_string());
        let region = std::env::var("BIGQUERY_TEST_LOCATION")
            .unwrap_or_else(|_| "US".to_string())
            .to_lowercase();

        // A run id unique to this invocation, so the label read-back below is
        // scoped to THIS run's jobs and cannot be satisfied by history.
        let run_id = format!(
            "rest-live-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let table = "rivet_bq_rest_live";
        let loader = BigQueryLoader::new(&project, &dataset).run_id(&run_id);
        let fqtn = loader.fqtn(table);

        // 1. A statement job: insert → poll → terminal verdict.
        loader
            .run_sql(
                &format!("CREATE OR REPLACE TABLE `{fqtn}` AS SELECT 1 AS id UNION ALL SELECT 2"),
                "create",
                table,
            )
            .expect("CREATE OR REPLACE through the REST transport");

        // 2. A scalar job: the getQueryResults leg, against a count this test
        //    seeded itself (not one rivet reported).
        assert_eq!(
            loader.count_rows(&fqtn, table).expect("count over REST"),
            2,
            "the count must come back from getQueryResults"
        );

        // 3. The labels, read back from BigQuery's catalog. `run_id` is unique
        //    per invocation, so a nonzero count can only come from the jobs
        //    THIS test just ran.
        let labelled = loader
            .api()
            .unwrap()
            .run_query_scalar(
                &format!(
                    "SELECT COUNT(*) FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
                     WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) \
                     AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'rivet_run' AND value = '{run_id}') \
                     AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'managed_by' AND value = 'rivet')",
                ),
                &loader.labels("audit", table),
            )
            .expect("reading INFORMATION_SCHEMA.JOBS_BY_PROJECT");
        assert!(
            labelled >= 2,
            "the run's jobs must carry rivet_run:{run_id} + managed_by:rivet in \
             configuration.labels — INFORMATION_SCHEMA saw {labelled}"
        );

        // 4. The failure path: a real `status.errorResult` must reach the caller
        //    with BigQuery's own reason text, not a bare "failed".
        let err = loader
            .run_sql(
                &format!("SELECT * FROM `{project}.{dataset}.no_such_table_ever`"),
                "probe",
                table,
            )
            .expect_err("a missing table must fail the job");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("Not found") && rendered.contains("no_such_table_ever"),
            "the REST error detail must name the reason: {rendered}"
        );

        // 5. Clean up after ourselves.
        loader
            .run_sql(&format!("DROP TABLE IF EXISTS `{fqtn}`"), "drop", table)
            .expect("dropping the live fixture table");
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
    #[ignore = "live: needs a BigQuery project + ADC + a CDC change-log Parquet fixture"]
    fn bigquery_live_cdc_view_dedups_at_least_once() {
        // Soft-skip when unconfigured — see bigquery_live_load_round_trips.
        let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
            eprintln!(
                "skipping bigquery_live_cdc_view_dedups_at_least_once: BIGQUERY_TEST_PROJECT unset"
            );
            return;
        };
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
