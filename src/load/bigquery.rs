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
//!   an overwrite must repeat them, and BigQuery refuses a changed spec — so an
//!   existing table's shape is read from `tables.get` and compared before the
//!   load (`table_shape_conflict`). Clustering is capped at 4 columns. Partition
//!   options (`partition_expiration_days`, `require_partition_filter`) are set
//!   at creation and changed in place with `ALTER TABLE SET OPTIONS`, since an
//!   overwrite that declares different ones is refused too (verified 2026-09-12).
//! - A single load *or* query job may modify at most **4,000 partitions**. The
//!   driver estimates the partitions a load touches from the Parquet footers
//!   before the job runs (`partition_budget`); BigQuery's own refusal is the
//!   backstop, surfaced as an actionable error.
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
use crate::load::plan::{Granularity, PartitionKey, TablePartition};
use crate::types::target::TargetColumnSpec;
use anyhow::{Result, bail};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
// ── BigQuery ─────────────────────────────────────────────────────────────────

/// Maximum clustering columns BigQuery allows.
pub(crate) const MAX_CLUSTER_COLUMNS: usize = 4;

/// Loads Rivet Parquet into a BigQuery dataset over the REST API.
#[derive(Debug, Clone)]
pub struct BigQueryLoader {
    pub project: String,
    pub dataset: String,
    /// The resolved `load.partition` of the table the load writes. Applied only when
    /// the table is created; its options are kept in step on a rivet-owned table.
    pub partition: Option<TablePartition>,
    /// Up to 4 clustering columns. Applied only when the table is created.
    pub cluster_by: Vec<String>,
    /// Load-run correlation id, emitted as the automatic `rivet_run:<id>` job
    /// label so every job of one `rivet load` invocation shares a run key —
    /// cost slices per run (across tables) as well as per table. `None` omits
    /// the label entirely.
    pub run_id: Option<String>,
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
            partition: None,
            cluster_by: Vec::new(),
            run_id: None,
            api: Arc::new(OnceLock::new()),
        }
    }

    /// Partition the table the load creates.
    pub fn partition(mut self, partition: TablePartition) -> Self {
        self.partition = Some(partition);
        self
    }

    /// The `PARTITION BY` expression, when the load partitions.
    fn partition_expr(&self) -> Option<&str> {
        self.partition.as_ref().map(|p| p.expr.as_str())
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

    /// Rows in `table`: table metadata for a table, a `COUNT(*)` for a view.
    fn count_rows(&self, table: &str) -> Result<u64> {
        let api = self.api()?;
        match api.table_num_rows(&self.dataset, table)? {
            Some(rows) => Ok(rows),
            None => api.run_query_scalar(
                &format!("SELECT COUNT(*) AS n FROM `{}`", self.fqtn(table)),
                &self.labels("count", table),
            ),
        }
    }

    /// Refuse a clustering list BigQuery would reject or that is not a plain identifier.
    fn check_cluster_by(&self) -> Result<()> {
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
        // (The partition expression is not gated here — its column was gated as
        // a plain identifier when the plan resolved it, and the rest is rivet's.)
        for c in &self.cluster_by {
            if !super::is_safe_load_ident(c) {
                bail!(
                    "BigQuery load: clustering column `{}` is not a plain SQL identifier \
                     ([A-Za-z_][A-Za-z0-9_]*) — it splices into CLUSTER BY. Rename it.",
                    c.escape_default()
                );
            }
        }
        Ok(())
    }

    /// The partitioning, clustering and partition options of `table` from `tables.get`,
    /// or `None` when it is no base table.
    fn existing_shape(&self, table: &str) -> Result<Option<TableShape>> {
        Ok(self
            .api()?
            .table_metadata(&self.dataset, table)?
            .as_ref()
            .map(parse_table_shape))
    }
}

impl TargetLoader for BigQueryLoader {
    fn changes_has_prior_changes(&self, table: &str) -> Result<bool> {
        let fqtn = self.fqtn(&format!("{table}__changes"));
        // LIMIT 1 inside: existence, not a full count, on an unbounded log.
        let sql = format!(
            "SELECT COUNT(*) AS n FROM (SELECT 1 FROM `{fqtn}` WHERE __op IS NOT NULL LIMIT 1)"
        );
        match self
            .api()?
            .run_query_scalar(&sql, &self.labels("probe", table))
        {
            Ok(n) => Ok(n > 0),
            // A missing __changes table is the FIRST cycle, not an error.
            Err(e) if format!("{e:#}").contains("Not found") => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}.{}", self.project, self.dataset, table)
    }

    fn object_kind(&self, table: &str) -> Result<super::ObjectKind> {
        let sql = build_object_kind_sql(&self.project, &self.dataset, table);
        super::ObjectKind::from_probe(
            self.api()?
                .run_query_scalar(&sql, &self.labels("probe", table))?,
        )
    }

    fn column_overlap(&self, table: &str, names: &[&str]) -> Result<(u64, u64)> {
        let (total, matched) = build_column_overlap_sql(&self.project, &self.dataset, table, names);
        let (api, labels) = (self.api()?, self.labels("probe", table));
        Ok((
            api.run_query_scalar(&total, &labels)?,
            api.run_query_scalar(&matched, &labels)?,
        ))
    }

    fn row_count(&self, table: &str) -> Result<u64> {
        self.count_rows(table)
    }

    fn adopt_as_changelog(&self, table: &str) -> Result<()> {
        let src = self.fqtn(table);
        let changes = self.fqtn(&format!("{table}__changes"));
        let shape = self.existing_shape(table)?.unwrap_or_default();
        for sql in build_adoption_sql(&src, table, &changes, &shape) {
            self.run_sql(&sql, "baseline", table)?;
        }
        if shape.require_partition_filter {
            eprintln!(
                "  note: `{changes}` does not require a partition filter, unlike `{src}` did — \
                 the current-state view reads all of it"
            );
        }
        if shape.expires_load_dates() {
            eprintln!(
                "  note: `{changes}` keeps its load-date partitions without expiry — expiring them \
                 would drop rows that never changed from the current-state view"
            );
        }
        if !same_columns(&shape.cluster, &self.cluster_by) {
            eprintln!(
                "  note: `{changes}` keeps the clustering `{src}` had ({}); `cluster_by` applies \
                 to a change log rivet creates",
                shape.cluster.join(", ")
            );
        }
        Ok(())
    }

    fn table_shape_conflict(&self, table: &str) -> Result<Option<String>> {
        let want = self.partition.as_ref().map(|p| &p.key);
        Ok(self
            .existing_shape(table)?
            .and_then(|shape| shape_conflict(&shape, want, &self.cluster_by)))
    }

    fn materialize(&self, table: &str, specs: &[TargetColumnSpec], uris: &[String]) -> Result<u64> {
        self.check_cluster_by()?;
        let target = self.fqtn(table);
        let schema = build_schema(specs);

        // ONE free path: declaring each column's native `target_type` inline in
        // LOAD DATA makes BigQuery coerce the Parquet on load — JSON, DATETIME,
        // NUMERIC, … land natively for FREE (a load job, not a query). Partition
        // options ride on the statement only when it CREATES the table; on an
        // existing one BigQuery refuses different options, so they are altered.
        let existing = self.existing_shape(table)?;
        let options = creation_options(existing.is_none(), self.partition.as_ref());
        let sql = build_load_data_sql(
            &target,
            true,
            &schema,
            self.partition_expr(),
            &self.cluster_by,
            options.as_deref(),
            uris,
        );
        self.run_sql(&sql, "load", table)?;
        if let Some(alter) = options_drift(&target, existing.as_ref(), self.partition.as_ref()) {
            self.run_sql(&alter, "alter", table)?;
            eprintln!("  note: `{target}` partition options changed: {alter}");
        }
        self.count_rows(table)
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        _pk: &[String],
    ) -> Result<u64> {
        use crate::load::cdc::Warehouse;
        self.check_cluster_by()?;
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

        // Ensure the append-only log exists, partitioned and clustered as the load
        // declares. Idempotent: created once, appended forever.
        let create = build_create_changes_sql(
            &changes_fqtn,
            &schema,
            self.partition.as_ref(),
            &self.cluster_by,
        );
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
        let before = self.count_rows(&changes)?;
        let load = build_load_data_sql(&changes_fqtn, false, &schema, None, &[], None, uris);
        self.run_sql(&load, "load", &changes)?;
        let after = self.count_rows(&changes)?;
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

/// `CREATE TABLE IF NOT EXISTS` for the change log, partitioned as the load declares and
/// clustered on `cluster_by` (capped at BigQuery's 4 clustering columns; none when empty).
/// Idempotent — the log is created once and appended to on every CDC load.
fn build_create_changes_sql(
    fqtn: &str,
    schema: &str,
    partition: Option<&TablePartition>,
    cluster_by: &[String],
) -> String {
    let cluster: Vec<String> = cluster_by
        .iter()
        .take(MAX_CLUSTER_COLUMNS)
        .cloned()
        .collect();
    let options = partition.and_then(changelog_options_sql);
    format!(
        "CREATE TABLE IF NOT EXISTS `{fqtn}` (\n{schema}\n){};",
        table_shape_clauses(
            partition.map(|p| p.expr.as_str()),
            &cluster,
            options.as_deref()
        )
    )
}

/// Milliseconds in a day, the unit `tables.get` reports partition expiry in.
const DAY_MS: u64 = 86_400_000;

/// The partitioning, clustering and partition options of an existing table, from `tables.get`.
#[derive(Debug, Clone, Default, PartialEq)]
struct TableShape {
    partition: Option<PartitionKey>,
    cluster: Vec<String>,
    require_partition_filter: bool,
    expiration_ms: Option<u64>,
}

impl TableShape {
    /// Partitioned by load time with an expiry.
    fn expires_load_dates(&self) -> bool {
        let ingestion = matches!(
            self.partition,
            Some(PartitionKey::Time { column: None, .. })
        );
        ingestion && self.expiration_ms.is_some()
    }
}

/// The shape a `tables.get` resource describes (`timePartitioning`, `rangePartitioning`,
/// `clustering`, `requirePartitionFilter`).
fn parse_table_shape(meta: &serde_json::Value) -> TableShape {
    use serde_json::Value;
    let text = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(String::from);
    let int = |v: &Value, key: &str| text(v, key).and_then(|s| s.parse::<i64>().ok());
    let mut shape = TableShape::default();
    if let Some(tp) = meta.get("timePartitioning") {
        let granularity = text(tp, "type")
            .as_deref()
            .and_then(Granularity::parse_sql)
            .unwrap_or(Granularity::Day);
        shape.partition = Some(PartitionKey::Time {
            column: text(tp, "field"),
            granularity,
        });
        shape.expiration_ms = text(tp, "expirationMs").and_then(|s| s.parse().ok());
        shape.require_partition_filter = tp
            .get("requirePartitionFilter")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    }
    if let Some(rp) = meta.get("rangePartitioning")
        && let (Some(column), Some(range)) = (text(rp, "field"), rp.get("range"))
        && let (Some(start), Some(end), Some(interval)) = (
            int(range, "start"),
            int(range, "end"),
            int(range, "interval"),
        )
    {
        shape.partition = Some(PartitionKey::Range {
            column,
            start,
            end,
            interval,
        });
    }
    if let Some(required) = meta.get("requirePartitionFilter").and_then(Value::as_bool) {
        shape.require_partition_filter |= required;
    }
    shape.cluster = meta
        .pointer("/clustering/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    shape
}

/// Whether two clustering lists name the same columns in the same order.
fn same_columns(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Whether an existing table's partition key is the one the load declares.
fn same_partition(existing: Option<&PartitionKey>, want: Option<&PartitionKey>) -> bool {
    match (existing, want) {
        (None, None) => true,
        (Some(a), Some(b)) => a.same_as(b),
        _ => false,
    }
}

/// How an existing table's partitioning or clustering differs from what this load declares, or `None`.
fn shape_conflict(
    shape: &TableShape,
    partition: Option<&PartitionKey>,
    cluster_by: &[String],
) -> Option<String> {
    let describe =
        |k: Option<&PartitionKey>, none: &str| k.map_or(none.to_string(), PartitionKey::describe);
    let list = |cols: &[String]| {
        if cols.is_empty() {
            "nothing".to_string()
        } else {
            cols.iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    };
    let mut diffs = Vec::new();
    if !same_partition(shape.partition.as_ref(), partition) {
        diffs.push(format!(
            "it is partitioned by {}, the load declares {}",
            describe(shape.partition.as_ref(), "nothing"),
            describe(partition, "no partitioning")
        ));
    }
    if !same_columns(&shape.cluster, cluster_by) {
        diffs.push(format!(
            "it is clustered on {}, `cluster_by` resolves to {}",
            list(&shape.cluster),
            list(cluster_by)
        ));
    }
    (!diffs.is_empty()).then(|| diffs.join("; "))
}

/// Probe whose scalar is `1·table + 2·view + 4·other` for `table` in the dataset.
fn build_object_kind_sql(project: &str, dataset: &str, table: &str) -> String {
    format!(
        "SELECT COUNTIF(table_type = 'BASE TABLE') + 2 * COUNTIF(table_type = 'VIEW') \
         + 4 * COUNTIF(table_type NOT IN ('BASE TABLE', 'VIEW')) AS n \
         FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES WHERE table_name = '{table}'"
    )
}

/// Probes counting all columns of `table` and those among `names`.
fn build_column_overlap_sql(
    project: &str,
    dataset: &str,
    table: &str,
    names: &[&str],
) -> (String, String) {
    let from = format!(
        "FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}'"
    );
    let list = if names.is_empty() {
        "''".to_string()
    } else {
        names
            .iter()
            .map(|n| format!("'{}'", n.to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    (
        format!("SELECT COUNT(*) AS n {from}"),
        format!("SELECT COUNT(*) AS n {from} AND LOWER(column_name) IN ({list})"),
    )
}

/// Turn the full-load table into `<table>__changes` without a query: a rename keeps its
/// rows, partitioning, clustering and options, then the meta columns are added (NULL on
/// every existing row). A partition filter requirement is dropped, since the current-state
/// view reads the whole log, and so is expiry of load-date partitions.
fn build_adoption_sql(
    src_fqtn: &str,
    table: &str,
    changes_fqtn: &str,
    shape: &TableShape,
) -> Vec<String> {
    let meta = crate::load::cdc::meta_column_specs(crate::load::cdc::Warehouse::BigQuery);
    let mut out = vec![format!(
        "ALTER TABLE `{src_fqtn}` RENAME TO {table}__changes;"
    )];
    out.extend(build_alter_add_columns_sql(changes_fqtn, &meta));
    if shape.require_partition_filter {
        out.push(format!(
            "ALTER TABLE `{changes_fqtn}` SET OPTIONS(require_partition_filter = false);"
        ));
    }
    if shape.expires_load_dates() {
        out.push(format!(
            "ALTER TABLE `{changes_fqtn}` SET OPTIONS(partition_expiration_days = NULL);"
        ));
    }
    out
}

/// The `OPTIONS(...)` a load creating the full table declares, or `None` when there is
/// no partition or nothing to set. `creating` is false when the table already exists —
/// BigQuery refuses an overwrite that declares different options, so they go through
/// [`options_drift`] instead.
fn creation_options(creating: bool, partition: Option<&TablePartition>) -> Option<String> {
    if !creating {
        return None;
    }
    let p = partition?;
    let mut opts = Vec::new();
    if let Some(days) = p.expiration_days {
        opts.push(format!("partition_expiration_days = {days}"));
    }
    if p.require_filter {
        opts.push("require_partition_filter = true".to_string());
    }
    (!opts.is_empty()).then(|| opts.join(", "))
}

/// The `OPTIONS(...)` of a change log rivet creates: the expiry of a column or range
/// partition only. Load-date partitions never expire (expiring them would drop rows that
/// never changed from the view), and the log never requires a partition filter.
fn changelog_options_sql(partition: &TablePartition) -> Option<String> {
    let days = partition.expiration_days?;
    partition
        .key
        .column()
        .map(|_| format!("partition_expiration_days = {days}"))
}

/// `ALTER TABLE … SET OPTIONS(...)` bringing an existing table's partition options to
/// what the load declares, or `None` when they already match (or the table is new).
fn options_drift(
    fqtn: &str,
    existing: Option<&TableShape>,
    partition: Option<&TablePartition>,
) -> Option<String> {
    let (shape, want) = (existing?, partition?);
    let mut opts = Vec::new();
    let want_ms = want.expiration_days.map(|d| u64::from(d) * DAY_MS);
    if shape.expiration_ms != want_ms {
        opts.push(match want.expiration_days {
            Some(days) => format!("partition_expiration_days = {days}"),
            None => "partition_expiration_days = NULL".to_string(),
        });
    }
    if shape.require_partition_filter != want.require_filter {
        opts.push(format!(
            "require_partition_filter = {}",
            want.require_filter
        ));
    }
    (!opts.is_empty()).then(|| format!("ALTER TABLE `{fqtn}` SET OPTIONS({});", opts.join(", ")))
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

/// `PARTITION BY … / CLUSTER BY … / OPTIONS(…)` clauses (empty when unset). All three
/// apply only at table creation, per BigQuery.
fn table_shape_clauses(
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
) -> String {
    let mut s = String::new();
    if let Some(expr) = partition_expr {
        s.push_str(&format!("\nPARTITION BY {expr}"));
    }
    if !cluster_by.is_empty() {
        let quoted: Vec<String> = cluster_by.iter().map(|c| format!("`{c}`")).collect();
        s.push_str(&format!("\nCLUSTER BY {}", quoted.join(", ")));
    }
    if let Some(opts) = options {
        s.push_str(&format!("\nOPTIONS({opts})"));
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
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
    uris: &[String],
) -> String {
    let kw = if overwrite { "OVERWRITE" } else { "INTO" };
    let clauses = table_shape_clauses(partition_expr, cluster_by, options);
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

    #[test]
    fn object_kind_probe_reads_the_dataset_catalog() {
        let sql = build_object_kind_sql("p", "d", "orders");
        assert!(
            sql.contains("FROM `p.d`.INFORMATION_SCHEMA.TABLES WHERE table_name = 'orders'"),
            "{sql}"
        );
        assert!(
            sql.contains("COUNTIF(table_type = 'BASE TABLE') + 2 * COUNTIF(table_type = 'VIEW')"),
            "{sql}"
        );
    }

    #[test]
    fn column_overlap_probes_lowercase_the_export_names() {
        let (total, matched) = build_column_overlap_sql("p", "d", "orders", &["Id", "amount"]);
        assert_eq!(
            total,
            "SELECT COUNT(*) AS n FROM `p.d`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'orders'"
        );
        assert!(
            matched.ends_with("AND LOWER(column_name) IN ('id', 'amount')"),
            "{matched}"
        );
    }

    #[test]
    fn adoption_renames_the_full_table_then_adds_the_meta_columns() {
        let sql = build_adoption_sql(
            "p.d.orders",
            "orders",
            "p.d.orders__changes",
            &TableShape::default(),
        );
        assert_eq!(sql.len(), 2, "{sql:?}");
        assert_eq!(
            sql[0],
            "ALTER TABLE `p.d.orders` RENAME TO orders__changes;"
        );
        assert!(sql[1].starts_with("ALTER TABLE `p.d.orders__changes`"));
        for col in ["`__op` STRING", "`__pos` STRING", "`__seq` INT64"] {
            assert!(sql[1].contains(col), "{}", sql[1]);
        }
    }

    /// A `tables.get` resource with the given partitioning, clustering and options.
    fn meta(
        partition: serde_json::Value,
        cluster: &[&str],
        require_filter: bool,
    ) -> serde_json::Value {
        let mut m = serde_json::json!({ "type": "TABLE", "numRows": "3" });
        if let Some(tp) = partition.get("time") {
            m["timePartitioning"] = tp.clone();
        }
        if let Some(rp) = partition.get("range") {
            m["rangePartitioning"] = rp.clone();
        }
        if !cluster.is_empty() {
            m["clustering"] = serde_json::json!({ "fields": cluster });
        }
        if require_filter {
            m["requirePartitionFilter"] = serde_json::json!(true);
        }
        m
    }

    fn time_key(column: Option<&str>, granularity: Granularity) -> PartitionKey {
        PartitionKey::Time {
            column: column.map(String::from),
            granularity,
        }
    }

    fn partition_at(
        key: PartitionKey,
        expiration_days: Option<u32>,
        require_filter: bool,
    ) -> TablePartition {
        TablePartition {
            expr: "TIMESTAMP_TRUNC(ts, DAY)".into(),
            key,
            expiration_days,
            require_filter,
        }
    }

    #[test]
    fn adoption_drops_the_partition_filter_and_load_date_expiry() {
        let shape = parse_table_shape(&meta(
            serde_json::json!({ "time": { "type": "DAY", "expirationMs": "2592000000", "requirePartitionFilter": true } }),
            &[],
            false,
        ));
        let sql = build_adoption_sql("p.d.t", "t", "p.d.t__changes", &shape).join("\n");
        assert!(
            sql.contains("SET OPTIONS(require_partition_filter = false)"),
            "{sql}"
        );
        assert!(
            sql.contains("SET OPTIONS(partition_expiration_days = NULL)"),
            "{sql}"
        );

        let column_partitioned = parse_table_shape(&meta(
            serde_json::json!({ "time": { "type": "DAY", "field": "d", "expirationMs": "2592000000" } }),
            &[],
            false,
        ));
        let sql =
            build_adoption_sql("p.d.t", "t", "p.d.t__changes", &column_partitioned).join("\n");
        assert!(!sql.contains("partition_expiration_days"), "{sql}");
        assert!(!sql.contains("require_partition_filter"), "{sql}");
    }

    #[test]
    fn tables_get_metadata_yields_the_shape() {
        let shape = parse_table_shape(&meta(
            serde_json::json!({ "time": { "type": "HOUR", "field": "ts", "expirationMs": "86400000" } }),
            &["v", "order"],
            true,
        ));
        assert_eq!(
            shape,
            TableShape {
                partition: Some(time_key(Some("ts"), Granularity::Hour)),
                cluster: vec!["v".into(), "order".into()],
                require_partition_filter: true,
                expiration_ms: Some(DAY_MS),
            }
        );
        let range = parse_table_shape(&meta(
            serde_json::json!({ "range": { "field": "n", "range": { "start": "0", "end": "1000", "interval": "10" } } }),
            &[],
            false,
        ));
        assert_eq!(
            range.partition,
            Some(PartitionKey::Range {
                column: "n".into(),
                start: 0,
                end: 1000,
                interval: 10
            })
        );
        let ingestion = parse_table_shape(&meta(
            serde_json::json!({ "time": { "type": "DAY", "expirationMs": "3" } }),
            &[],
            false,
        ));
        assert_eq!(ingestion.partition, Some(time_key(None, Granularity::Day)));
        assert!(ingestion.expires_load_dates());
        assert!(
            !shape.expires_load_dates(),
            "a column partition's expiry is not a load-date one"
        );
        let plain = parse_table_shape(&serde_json::json!({ "type": "TABLE" }));
        assert_eq!(plain, TableShape::default());
    }

    #[test]
    fn creation_options_ride_only_on_a_creating_load() {
        let p = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
        assert_eq!(
            creation_options(true, Some(&p)).as_deref(),
            Some("partition_expiration_days = 400, require_partition_filter = true")
        );
        assert_eq!(
            creation_options(false, Some(&p)),
            None,
            "an existing table is altered instead"
        );
        assert_eq!(creation_options(true, None), None);
        let bare = partition_at(time_key(Some("ts"), Granularity::Day), None, false);
        assert_eq!(creation_options(true, Some(&bare)), None);
        let sql = build_load_data_sql(
            "p.d.t",
            true,
            "  `ts` TIMESTAMP",
            Some(&p.expr),
            &[],
            creation_options(true, Some(&p)).as_deref(),
            &uris(),
        );
        assert!(
            sql.contains("PARTITION BY TIMESTAMP_TRUNC(ts, DAY)\nOPTIONS(partition_expiration_days = 400, require_partition_filter = true)\nFROM FILES"),
            "{sql}"
        );
    }

    #[test]
    fn options_drift_alters_only_what_differs() {
        let shape = TableShape {
            partition: Some(time_key(Some("ts"), Granularity::Day)),
            cluster: vec![],
            require_partition_filter: true,
            expiration_ms: Some(400 * DAY_MS),
        };
        let same = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
        assert_eq!(options_drift("p.d.t", Some(&shape), Some(&same)), None);
        let shorter = partition_at(time_key(Some("ts"), Granularity::Day), Some(30), true);
        assert_eq!(
            options_drift("p.d.t", Some(&shape), Some(&shorter)).as_deref(),
            Some("ALTER TABLE `p.d.t` SET OPTIONS(partition_expiration_days = 30);")
        );
        let cleared = partition_at(time_key(Some("ts"), Granularity::Day), None, false);
        assert_eq!(
            options_drift("p.d.t", Some(&shape), Some(&cleared)).as_deref(),
            Some(
                "ALTER TABLE `p.d.t` SET OPTIONS(partition_expiration_days = NULL, require_partition_filter = false);"
            )
        );
        assert_eq!(
            options_drift("p.d.t", None, Some(&shorter)),
            None,
            "a new table took its options at creation"
        );
        assert_eq!(options_drift("p.d.t", Some(&shape), None), None);
    }

    #[test]
    fn a_changelog_is_partitioned_like_the_table_but_never_requires_a_filter() {
        let p = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
        let sql = build_create_changes_sql(
            "p.d.t__changes",
            "  `ts` TIMESTAMP",
            Some(&p),
            &["id".into()],
        );
        assert!(sql.contains("PARTITION BY TIMESTAMP_TRUNC(ts, DAY)\nCLUSTER BY `id`\nOPTIONS(partition_expiration_days = 400)"), "{sql}");
        assert!(!sql.contains("require_partition_filter"), "{sql}");
        let mut ingestion = partition_at(time_key(None, Granularity::Day), Some(3), false);
        ingestion.expr = "_PARTITIONDATE".into();
        let sql = build_create_changes_sql("p.d.t__changes", "  `id` INT64", Some(&ingestion), &[]);
        assert!(sql.ends_with("PARTITION BY _PARTITIONDATE;"), "{sql}");
        assert!(
            !sql.contains("expiration"),
            "load-date partitions of a log never expire: {sql}"
        );
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
        let sql = build_load_data_sql("p.d.orders", true, &schema, None, &[], None, &uris());
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
        let sql = build_load_data_sql("p.d.orders", false, &schema, None, &[], None, &uris());
        assert!(sql.starts_with("LOAD DATA INTO `p.d.orders`"));
    }

    #[test]
    fn load_data_emits_partition_and_cluster_when_configured() {
        let schema = build_schema(&[typed("id", "INT64")]);
        let sql = build_load_data_sql(
            "p.d.orders",
            true,
            &schema,
            Some("DATE(created_at)"),
            &["customer_id".into(), "region".into()],
            None,
            &uris(),
        );
        assert!(sql.contains("PARTITION BY DATE(created_at)"));
        assert!(sql.contains("CLUSTER BY `customer_id`, `region`"));
        assert!(!sql.contains("OPTIONS"));
    }

    #[test]
    fn an_existing_tables_shape_conflicts_when_partitioning_or_clustering_differ() {
        let existing = TableShape {
            partition: Some(time_key(Some("d"), Granularity::Day)),
            cluster: vec!["v".into()],
            require_partition_filter: false,
            expiration_ms: None,
        };
        let id = vec!["id".to_string()];
        let diff = shape_conflict(&existing, None, &id).expect("differs");
        assert!(diff.contains("partitioned by `d` by day"), "{diff}");
        assert!(diff.contains("declares no partitioning"), "{diff}");
        assert!(
            diff.contains("clustered on `v`, `cluster_by` resolves to `id`"),
            "{diff}"
        );

        let same_key = time_key(Some("D"), Granularity::Day);
        assert_eq!(
            shape_conflict(&existing, Some(&same_key), &["V".to_string()]),
            None,
            "the same shape, names compared without case"
        );
        let monthly = time_key(Some("d"), Granularity::Month);
        let diff = shape_conflict(&existing, Some(&monthly), &["v".to_string()]).expect("differs");
        assert!(
            diff.contains("partitioned by `d` by day, the load declares `d` by month"),
            "{diff}"
        );
        assert!(!diff.contains("clustered"), "{diff}");

        let with_options = TableShape {
            require_partition_filter: true,
            expiration_ms: Some(DAY_MS),
            ..existing.clone()
        };
        assert_eq!(
            shape_conflict(&with_options, Some(&same_key), &["v".to_string()]),
            None,
            "options are altered in place, never a conflict"
        );
        let plain = TableShape::default();
        assert_eq!(shape_conflict(&plain, None, &[]), None);
        let diff = shape_conflict(&plain, None, &id).expect("differs");
        assert!(diff.contains("clustered on nothing"), "{diff}");
        assert!(!diff.contains("partitioned"), "{diff}");
    }

    #[test]
    fn an_unclustered_changelog_carries_no_cluster_clause() {
        let create = build_create_changes_sql("p.d.t__changes", "  `id` INT64", None, &[]);
        assert!(!create.contains("CLUSTER BY"), "{create}");
        assert!(
            create.ends_with(")\n;") || create.ends_with(");"),
            "{create}"
        );
    }

    #[test]
    fn create_changes_clusters_on_pk_capped_at_four_columns() {
        let schema = build_schema(&[typed("__op", "STRING"), typed("id", "INT64")]);
        let sql = build_create_changes_sql("p.d.orders__changes", &schema, None, &["id".into()]);
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `p.d.orders__changes` ("));
        assert!(sql.contains("CLUSTER BY `id`"));
        // A >4-column PK is capped to BigQuery's clustering limit.
        let wide: Vec<String> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let sql2 = build_create_changes_sql("t", &schema, None, &wide);
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
        let create =
            build_create_changes_sql("p.d.t__changes", "  `id` INT64", None, &["id".into()]);
        assert!(create.starts_with("CREATE TABLE IF NOT EXISTS"), "{create}");
        let load = build_load_data_sql(
            "p.d.t__changes",
            false,
            "  `id` INT64",
            None,
            &[],
            None,
            &uris(),
        );
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
        let report = crate::load::run_load(
            &loader,
            "rivet_bq_live_test",
            &specs,
            &[uri],
            None,
            None,
            crate::load::Ownership::Own,
        )
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
            loader
                .api()
                .unwrap()
                .run_query_scalar(
                    &format!("SELECT COUNT(*) AS n FROM `{fqtn}`"),
                    &loader.labels("count", table)
                )
                .expect("count over REST"),
            2,
            "the count must come back from getQueryResults"
        );
        assert_eq!(
            loader.count_rows(table).expect("numRows over REST"),
            2,
            "…and the same count from tables.get metadata"
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
            crate::load::Ownership::Own,
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
            crate::load::Ownership::Own,
        )
        .expect("second CDC append (at-least-once) should succeed");
        assert!(second.rows_appended > 0, "second append added rows");

        // The dedup VIEW must report the current state, independent of how many
        // times the log was appended.
        let state_rows = loader
            .api()
            .unwrap()
            .run_query_scalar(
                &format!("SELECT COUNT(*) AS n FROM `{}`", second.view),
                &loader.labels("count", table),
            )
            .expect("counting the dedup view should succeed");
        if expected_state > 0 {
            assert_eq!(
                state_rows, expected_state,
                "the view must collapse duplicates to {expected_state} distinct-PK rows \
                 (incl tombstones), got {state_rows}"
            );
        }
    }

    #[test]
    #[ignore = "live: requires BIGQUERY_TEST_PROJECT"]
    fn bigquery_live_adopts_a_full_load_table_as_the_changelog_baseline() {
        let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
            eprintln!("skipping: BIGQUERY_TEST_PROJECT unset");
            return;
        };
        let dataset =
            std::env::var("RIVET_BQ_TEST_DATASET").unwrap_or_else(|_| "rivet_test".to_string());
        let loader = BigQueryLoader::new(&project, &dataset);
        let table = format!("rivet_bq_live_adopt_{}", std::process::id());
        let changes = format!("{table}__changes");
        let (fq, changes_fq) = (loader.fqtn(&table), loader.fqtn(&changes));
        let probe = format!("{table}_probe");
        let probe_fq = loader.fqtn(&probe);
        let fixture = |fqtn: &str| {
            loader.run_sql(
                &format!(
                    "CREATE OR REPLACE TABLE `{fqtn}` AS \
                     SELECT id, CONCAT('v', CAST(id AS STRING)) AS v FROM UNNEST([1, 2, 3]) AS id"
                ),
                "fixture",
                &table,
            )
        };

        fixture(&probe_fq).expect("probe table");
        let collision = loader.create_view(
            &probe,
            &format!("CREATE OR REPLACE VIEW `{probe_fq}` AS SELECT 1 AS id"),
        );
        let probe_kind = loader.object_kind(&probe);
        let _ = loader.run_sql(
            &format!("DROP TABLE IF EXISTS `{probe_fq}`"),
            "cleanup",
            &probe,
        );
        let _ = loader.run_sql(
            &format!("DROP VIEW IF EXISTS `{probe_fq}`"),
            "cleanup",
            &probe,
        );
        eprintln!(
            "view over a full-load table without adoption: {collision:?}, kind after: {probe_kind:?}"
        );
        assert!(
            collision.is_err(),
            "CREATE OR REPLACE VIEW over a table must fail, not replace it"
        );
        assert_eq!(probe_kind.unwrap(), crate::load::ObjectKind::Table);

        fixture(&fq).expect("fixture table");
        let before = loader.object_kind(&table);
        let specs = [typed("id", "INT64"), typed("v", "STRING")];
        let adopted = crate::load::adopt_full_load_table(
            &loader,
            &table,
            &specs,
            crate::load::Ownership::Own,
        );
        let view_sql = crate::load::cdc::inc_dedup_view_sql(
            crate::load::cdc::Warehouse::BigQuery,
            &fq,
            &changes_fq,
            &["id"],
            "id",
        );
        let view = adopted
            .as_ref()
            .ok()
            .map(|_| loader.create_view(&table, &view_sql));
        let after = loader.object_kind(&table);
        let copied = loader.row_count(&changes);
        let viewed = loader.row_count(&table);
        for drop in [
            format!("DROP VIEW IF EXISTS `{fq}`"),
            format!("DROP TABLE IF EXISTS `{fq}`"),
            format!("DROP TABLE IF EXISTS `{changes_fq}`"),
        ] {
            let _ = loader.run_sql(&drop, "cleanup", &table);
        }

        assert_eq!(before.unwrap(), crate::load::ObjectKind::Table);
        assert_eq!(adopted.unwrap(), Some(3));
        view.unwrap().unwrap();
        assert_eq!(after.unwrap(), crate::load::ObjectKind::View);
        assert_eq!(copied.unwrap(), 3);
        assert_eq!(viewed.unwrap(), 3);
    }
}
