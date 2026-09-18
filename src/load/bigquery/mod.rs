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
use crate::load::plan::{Clustering, Granularity, PartitionForm, PartitionKey, TablePartition};
use crate::types::target::TargetColumnSpec;
use anyhow::{Context as _, Result, bail};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
// ── BigQuery ─────────────────────────────────────────────────────────────────

mod ddl;
mod shape;
#[cfg(test)]
mod tests;

use ddl::*;
use shape::*;

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
    /// Up to 4 clustering columns, and whether the config wrote them: a written
    /// clustering is applied to an existing change log; an `auto` one leaves the
    /// log's own. Applied only when a table is created.
    pub clustering: Clustering,
    /// Load-run correlation id, emitted as the automatic `rivet_run:<id>` job
    /// label so every job of one `rivet load` invocation shares a run key —
    /// cost slices per run (across tables) as well as per table. `None` omits
    /// the label entirely.
    pub run_id: Option<String>,
    /// Where the staged Parquet lives, for the footer reads that pack a load into
    /// jobs of at most 4,000 partitions each; `None` loads everything in one job.
    pub footer_source: Option<crate::config::DestinationConfig>,
    /// The CDC layout: under `BaseAndBuffer`, `<table>__changes` is a per-cycle
    /// buffer — created without a partition (whole-scanned by one MERGE, then
    /// dropped), never shape-settled — and `compact` merges it into the base.
    pub layout: crate::load::plan::CdcLayout,
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
            clustering: Clustering::Auto(Vec::new()),
            run_id: None,
            layout: crate::load::plan::CdcLayout::LogAndView,
            footer_source: None,
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
    /// Pack every load into jobs within BigQuery's partition cap, reading the partition
    /// column's range from each file's Parquet footer under `dest`.
    pub fn batched_by_footers(mut self, dest: crate::config::DestinationConfig) -> Self {
        self.footer_source = Some(dest);
        self
    }

    /// The CDC layout the load writes (see the field).
    pub fn layout(mut self, layout: crate::load::plan::CdcLayout) -> Self {
        self.layout = layout;
        self
    }

    /// The load jobs `uris` need under the TARGET's partition: one when nothing bounds
    /// them (an unpartitioned target, no partition column, or no footer source), else
    /// footer-packed batches. The partition cap is the target table's, so a
    /// disposable buffer (never partitioned) passes `None` whatever the base declares.
    fn batches(
        &self,
        uris: &[String],
        partition: Option<&TablePartition>,
    ) -> Result<Vec<Vec<String>>> {
        let keyed = partition.filter(|p| p.key.column().is_some());
        match self.footer_source.as_ref().zip(keyed) {
            Some((dest, partition)) => {
                let store = crate::load::open_store(dest)?;
                crate::load::partition_budget::plan_load_batches(&store, uris, partition)
            }
            None => Ok(vec![uris.to_vec()]),
        }
    }

    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = Some(id.into());
        self
    }

    /// Cluster the table the load creates on `columns`, as a written clustering.
    #[cfg(test)]
    pub fn clustered_on(mut self, columns: Vec<String>) -> Self {
        self.clustering = Clustering::Written(columns);
        self
    }

    /// The clustering columns.
    pub(crate) fn cluster_by(&self) -> &[String] {
        self.clustering.columns()
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
        let built = BigQueryApi::for_dataset(&self.project, &self.dataset)?;
        let _ = self.api.set(built);
        Ok(self.api.get().expect("the client was just set"))
    }

    /// The automatic + user labels for a job, keyed for `configuration.labels`.
    fn labels(&self, op: &str, table: &str) -> BTreeMap<String, String> {
        build_labels(op, table, self.run_id.as_deref())
    }

    /// Run a SQL statement (free `LOAD DATA` load job or a billed CTAS/query),
    /// tagged with `rivet_op:<op>` + `rivet_table:<table>` for cost attribution.
    /// Two operations exist, `load` and `merge`; every DDL, count, clone or view
    /// statement carries the operation it serves, so cost sums per table per op.
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
                &self.labels("load", table),
            ),
        }
    }

    /// Refuse a clustering list BigQuery would reject or that is not a plain identifier.
    fn check_cluster_by(&self) -> Result<()> {
        check_cluster_columns(self.cluster_by())
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
            .run_query_scalar(&sql, &self.labels("load", table))
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
                .run_query_scalar(&sql, &self.labels("load", table))?,
        )
    }

    fn column_overlap(&self, table: &str, names: &[&str]) -> Result<(u64, u64)> {
        let (total, matched) = build_column_overlap_sql(&self.project, &self.dataset, table, names);
        let (api, labels) = (self.api()?, self.labels("load", table));
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
            self.run_sql(&sql, "load", table)?;
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
        if keeps_own_clustering(&shape, self.cluster_by(), self.clustering.is_written()) {
            eprintln!(
                "  note: `{changes}` keeps the clustering `{src}` had ({}); a written `cluster_by` \
                 would re-cluster it",
                shape.cluster.join(", ")
            );
        }
        Ok(())
    }

    fn shape(&self) -> Option<&dyn super::ShapeControl> {
        Some(self)
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
        let cluster = table_clustering(&self.clustering, existing.as_ref());
        check_cluster_columns(cluster)?;
        let batches = self.batches(uris, self.partition.as_ref())?;
        // One job (or nothing to pack) OVERWRITES the target directly; several go
        // through staging below. A pattern, not a count compare: this body is
        // live-only and its decisions are graded here by shape, not by mutation.
        if matches!(batches.as_slice(), [] | [_]) {
            let sql = build_load_data_sql(
                &target,
                true,
                &schema,
                self.partition_expr(),
                cluster,
                options.as_deref(),
                uris,
            );
            self.run_sql(&sql, "load", table)?;
            if let Some(alter) = options_drift(&target, existing.as_ref(), self.partition.as_ref())
            {
                self.run_sql(&alter, "load", table)?;
                eprintln!("  note: `{target}` partition options changed: {alter}");
            }
            return self.count_rows(table);
        }
        // Several jobs cannot OVERWRITE one table: they fill a fresh staging table,
        // created with the declared shape by the first job, and the target becomes a
        // zero-copy CLONE of it in one statement. The shape conflict of an existing
        // target was refused before this point (`ensure_overwritable`), so the CLONE
        // never changes a partitioning D5 forbids changing.
        let staging = format!("{table}__staging");
        let staging_fqtn = self.fqtn(&staging);
        eprintln!(
            "  {target}: {} files in {} load jobs (≤ {} partitions each) via `{staging_fqtn}`",
            uris.len(),
            batches.len(),
            crate::load::partition_budget::MAX_PARTITIONS_PER_JOB
        );
        self.run_sql(
            &format!("DROP TABLE IF EXISTS `{staging_fqtn}`;"),
            "load",
            table,
        )?;
        let fresh = creation_options(true, self.partition.as_ref());
        // The first batch CREATES the staging table with the declared shape; the
        // rest append into it.
        if let Some((first, rest)) = batches.split_first() {
            let sql = build_load_data_sql(
                &staging_fqtn,
                true,
                &schema,
                self.partition_expr(),
                cluster,
                fresh.as_deref(),
                first,
            );
            self.run_sql(&sql, "load", table)?;
            for batch in rest {
                let sql =
                    build_load_data_sql(&staging_fqtn, false, &schema, None, &[], None, batch);
                self.run_sql(&sql, "load", table)?;
            }
        }
        self.run_sql(&build_clone_sql(&target, &staging_fqtn), "load", table)?;
        self.run_sql(&format!("DROP TABLE `{staging_fqtn}`;"), "load", table)?;
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
        // declares. Idempotent: created once, appended forever. A BUFFER takes no
        // partition: one MERGE reads all of it and `compact` drops it.
        let log_partition_decl = if self.layout.log_is_disposable() {
            None
        } else {
            self.partition.as_ref()
        };
        let existed = self
            .api()?
            .table_metadata(&self.dataset, &changes)?
            .is_some();
        let create = build_create_changes_sql(
            &changes_fqtn,
            &schema,
            log_partition_decl,
            self.cluster_by(),
        );
        self.run_sql(&create, "load", &changes)?;

        // …and, for a log that ALREADY existed, add whatever the declared
        // schema has and it does not. The CREATE above is a no-op on such a
        // table, so without this a log rivet did not create — or one that predates a new
        // meta column — fails the LOAD below on a schema mismatch. ALTER ADD,
        // never a replace: the table may hold the customer's history.
        // A log that did not exist a moment ago was just created from this very
        // spec: nothing to add (metadata, not a query, says which).
        let alter = if existed {
            build_alter_add_columns_sql(&changes_fqtn, &full)
        } else {
            None
        };
        if let Some(alter) = alter {
            self.run_sql(&alter, "load", &changes)?;
        }
        // …and its partition options, as the log takes them (no filter, no load-date expiry).
        // A buffer has no partition options to settle; a changelog's follow the config.
        let log_partition = self.partition.as_ref().map(changelog_partition);
        let settle = if self.layout.log_is_disposable() {
            None
        } else {
            options_drift(
                &changes_fqtn,
                self.existing_shape(&changes)?.as_ref(),
                log_partition.as_ref(),
            )
        };
        if let Some(alter) = settle {
            self.run_sql(&alter, "load", &changes)?;
            eprintln!("  note: `{changes_fqtn}` partition options changed: {alter}");
        }

        // Count before / append (free LOAD DATA INTO) / count after — the delta
        // is what THIS load added; the driver gates it against the manifest total.
        let before = self.count_rows(&changes)?;
        let batches = self.batches(uris, log_partition_decl)?;
        if let [_, _, ..] = batches.as_slice() {
            eprintln!(
                "  {changes_fqtn}: {} files in {} append jobs (≤ {} partitions each)",
                uris.len(),
                batches.len(),
                crate::load::partition_budget::MAX_PARTITIONS_PER_JOB
            );
        }
        for batch in &batches {
            let load = build_load_data_sql(&changes_fqtn, false, &schema, None, &[], None, batch);
            self.run_sql(&load, "load", &changes)?;
        }
        let after = self.count_rows(&changes)?;
        Ok(after.saturating_sub(before))
    }

    fn warehouse(&self) -> crate::load::cdc::Warehouse {
        crate::load::cdc::Warehouse::BigQuery
    }

    fn compact(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        pk: &[String],
        engine: crate::load::cdc::SourceEngine,
    ) -> Result<crate::load::CompactReport> {
        use crate::load::cdc::{
            CompactProbe, compact_probe_sql, compact_script_sql, plan_compact_merges,
        };
        let base = self.fqtn(table);
        let changes = format!("{table}__changes");
        let changes_fqtn = self.fqtn(&changes);
        let api = self.api()?;
        // No buffer table → nothing to merge, said so by the report. Metadata, not
        // a query job: `tables.get` is free and answers the same question.
        let Some(buffer) = api.table_metadata(&self.dataset, &changes)? else {
            return Ok(crate::load::CompactReport {
                base,
                changes_rows: 0,
                merge_jobs: 0,
                had_buffer: false,
            });
        };
        // An EMPTY buffer (the metadata row count is exact after a load job) needs
        // no probe and no MERGE — every statement that touches a table is billed a
        // 10 MB floor; the DROP alone is free.
        if buffer.get("numRows").and_then(serde_json::Value::as_str) == Some("0") {
            self.run_sql(&format!("DROP TABLE `{changes_fqtn}`;"), "merge", table)?;
            return Ok(crate::load::CompactReport {
                base,
                changes_rows: 0,
                merge_jobs: 0,
                had_buffer: true,
            });
        }
        // A crash BEFORE any MERGE: the buffer must survive whole, so the next
        // compact applies every change exactly once (the sibling of
        // `compact_after_merge`, where the script already dropped it).
        crate::test_hook::maybe_panic_at("compact_before_merge");
        let key = self.partition.as_ref().map(|p| &p.key);
        // A day-partitioned base (the partner shape, init's default) or an
        // unpartitioned one compacts in ONE scripted job: the buffer's distinct days
        // become a script variable and every MERGE prunes to exactly those partitions.
        let day_column = match key {
            None => Some(None),
            Some(PartitionKey::Time {
                column,
                granularity: Granularity::Day,
            }) => Some(column.as_deref()),
            Some(PartitionKey::Time { column: None, .. }) => Some(None),
            Some(_) => None,
        };
        if let Some(day_column) = day_column {
            let script = compact_script_sql(&base, &changes_fqtn, specs, pk, engine, day_column);
            let row = api.run_query_first_row(&script, &self.labels("merge", table))?;
            let (changes_rows, merge_jobs) = compact_summary(&row)?;
            // The buffer is gone with the script; a crash HERE loses nothing — the
            // next compact finds no buffer and says so.
            crate::test_hook::maybe_panic_at("compact_after_merge");
            return Ok(crate::load::CompactReport {
                base,
                changes_rows,
                merge_jobs,
                had_buffer: true,
            });
        }
        // Other keys (hour/month/year, integer ranges): the range probe, then one
        // MERGE per window of constant bounds, then the DROP — separate jobs.
        let part_col = key.and_then(PartitionKey::column);
        let time_key = matches!(key, Some(PartitionKey::Time { .. }));
        let probe = compact_probe_sql(&changes_fqtn, part_col, time_key);
        let row = self
            .api()?
            .run_query_first_row(&probe, &self.labels("merge", table))?;
        let cell = |i: usize| row.get(i).cloned().flatten().unwrap_or_default();
        let probe = CompactProbe {
            rows: cell(0).parse().unwrap_or(0),
            lo: cell(1),
            hi: cell(2),
            nulls: cell(3).parse().unwrap_or(0),
        };
        let changes_rows = probe.rows;
        let merges = plan_compact_merges(&base, &changes_fqtn, specs, pk, engine, key, &probe)?;
        for sql in &merges {
            self.run_sql(sql, "merge", table)?;
        }
        // The buffer is spent: the next load creates it anew from its run's spec. A
        // crash between the MERGE and this DROP re-merges the same rows next time —
        // the upsert is idempotent, so nothing is applied twice.
        crate::test_hook::maybe_panic_at("compact_after_merge");
        self.run_sql(&format!("DROP TABLE `{changes_fqtn}`;"), "merge", table)?;
        Ok(crate::load::CompactReport {
            base,
            changes_rows,
            merge_jobs: merges.len(),
            had_buffer: true,
        })
    }

    fn create_view(&self, table: &str, view_sql: &str) -> Result<()> {
        self.run_sql(view_sql, "load", table)?;
        Ok(())
    }
}

impl BigQueryLoader {
    /// How `object` differs from the declared partition/clustering, or `None`.
    fn drift_of(&self, object: &str) -> Result<Option<super::ChangelogDrift>> {
        let declared_cluster = self.clustering.is_written().then_some(self.cluster_by());
        Ok(self.existing_shape(object)?.and_then(|shape| {
            classify_drift(
                &shape,
                self.partition.as_ref().map(|p| &p.key),
                declared_cluster,
            )
        }))
    }
}

impl super::ShapeControl for BigQueryLoader {
    fn table_shape_conflict(&self, table: &str) -> Result<Option<String>> {
        let want = self.partition.as_ref().map(|p| &p.key);
        let written = self.clustering.is_written().then_some(self.cluster_by());
        Ok(self
            .existing_shape(table)?
            .and_then(|shape| shape_conflict(&shape, want, written)))
    }

    fn changelog_drift(&self, table: &str) -> Result<Option<super::ChangelogDrift>> {
        self.drift_of(&format!("{table}__changes"))
    }

    fn adoption_drift(&self, table: &str) -> Result<Option<super::ChangelogDrift>> {
        self.drift_of(table)
    }

    fn recluster_changelog(&self, table: &str) -> Result<()> {
        let changes = format!("{table}__changes");
        self.api()?.patch_table(
            &self.dataset,
            &changes,
            &clustering_patch(self.cluster_by()),
        )?;
        Ok(())
    }

    fn rebuild_changelog(&self, table: &str) -> Result<()> {
        let changes = format!("{table}__changes");
        let rebuild = format!("{changes}__rebuild");
        let old = format!("{changes}__old");
        let api = self.api()?;
        let meta = api
            .table_metadata(&self.dataset, &changes)?
            .ok_or_else(|| anyhow::anyhow!("`{}` is not a table", self.fqtn(&changes)))?;
        let props = parse_table_props(&meta);
        let row_policies = match api.row_access_policy_count(&self.dataset, &changes) {
            Ok(n) => n,
            Err(e) => {
                eprintln!(
                    "  warning: could not list the row access policies of `{}` ({e:#}) — a \
                     rebuilt log carries none; check by hand",
                    self.fqtn(&changes)
                );
                0
            }
        };
        if let Some(why) = rebuild_policy_refusal(&self.fqtn(&changes), &props, row_policies) {
            return Err(super::refused(why));
        }
        let before = self.count_rows(&changes)?;
        let copy = build_rebuild_copy_sql(
            &self.fqtn(&rebuild),
            &self.fqtn(&changes),
            self.partition.as_ref(),
            self.cluster_by(),
            &props,
        );
        self.run_sql(&copy, "load", table)?;
        let after = self.count_rows(&rebuild)?;
        if after != before {
            bail!(
                "`{}` holds {after} rows but `{}` holds {before} — the copy is left for \
                 inspection, nothing was swapped",
                self.fqtn(&rebuild),
                self.fqtn(&changes)
            );
        }
        for sql in build_rebuild_swap_sql(
            &self.fqtn(&changes),
            &self.fqtn(&rebuild),
            &self.fqtn(&old),
            &changes,
            &old,
        ) {
            self.run_sql(&sql, "load", table)?;
        }
        Ok(())
    }

    fn rebuild_leftovers(&self, table: &str) -> Result<Vec<String>> {
        // A buffer is never rebuilt in place (`compact` drops it), so it can leave
        // no `__rebuild` / `__old` behind: nothing to look for, no query job.
        if self.layout.log_is_disposable() {
            return Ok(Vec::new());
        }
        let changes = format!("{table}__changes");
        let names = [format!("{changes}__rebuild"), format!("{changes}__old")];
        let sql = build_leftovers_sql(&self.project, &self.dataset, &names);
        let code = self
            .api()?
            .run_query_scalar(&sql, &self.labels("load", table))?;
        Ok(leftover_names(code, &names)
            .iter()
            .map(|n| self.fqtn(n))
            .collect())
    }
}

/// The `PARTITION BY` expression for a `partition:` form, from the column's BigQuery
/// type (ADR-0034 D3); `column_type` looks a column up in the load's specs.
pub(crate) fn partition_expr(
    export: &str,
    form: &PartitionForm,
    column_type: &dyn Fn(&str) -> Result<String>,
) -> Result<(PartitionKey, String)> {
    Ok(match form {
        PartitionForm::Column {
            column,
            granularity,
        } => {
            let t = column_type(column)?;
            let g = granularity.as_sql();
            let expr = match (t.as_str(), granularity) {
                ("TIMESTAMP", _) => format!("TIMESTAMP_TRUNC({column}, {g})"),
                ("DATETIME", _) => format!("DATETIME_TRUNC({column}, {g})"),
                ("DATE", Granularity::Day) => column.clone(),
                ("DATE", Granularity::Hour) => bail!(
                    "export `{export}`: `{column}` is a DATE, which has no hours — partition it \
                     by day, month or year"
                ),
                ("DATE", _) => format!("DATE_TRUNC({column}, {g})"),
                _ => bail!(
                    "export `{export}`: cannot partition on `{column}` ({t}); BigQuery partitions \
                     a DATE, DATETIME or TIMESTAMP column by time, or an INT64 column with `range`"
                ),
            };
            (
                PartitionKey::Time {
                    column: Some(column.clone()),
                    granularity: *granularity,
                },
                expr,
            )
        }
        PartitionForm::Range {
            column,
            start,
            end,
            interval,
        } => {
            let t = column_type(column)?;
            if t != "INT64" {
                bail!(
                    "export `{export}`: `range` partitions an INT64 column, and `{column}` is {t}"
                );
            }
            (
                PartitionKey::Range {
                    column: column.clone(),
                    start: *start,
                    end: *end,
                    interval: *interval,
                },
                format!("RANGE_BUCKET({column}, GENERATE_ARRAY({start}, {end}, {interval}))"),
            )
        }
        PartitionForm::Ingestion(g) => {
            let expr = match g {
                Granularity::Day => "_PARTITIONDATE".to_string(),
                _ => format!("TIMESTAMP_TRUNC(_PARTITIONTIME, {})", g.as_sql()),
            };
            (
                PartitionKey::Time {
                    column: None,
                    granularity: *g,
                },
                expr,
            )
        }
    })
}

/// Whether BigQuery can cluster a column of this native type.
pub(crate) fn clusterable(target_type: &str) -> bool {
    let base = crate::load::plan::base_type(target_type);
    matches!(
        base.as_str(),
        "BIGNUMERIC"
            | "BOOL"
            | "DATE"
            | "DATETIME"
            | "GEOGRAPHY"
            | "INT64"
            | "NUMERIC"
            | "RANGE"
            | "STRING"
            | "TIMESTAMP"
    )
}

/// Whether a column name is one of rivet's CDC meta columns — filtered out of
/// the data specs before the meta columns are prepended, so a schema can never
/// declare `__op`/`__pos`/`__seq` twice.
/// The `(changes_rows, merge_jobs)` row the compaction script ends with; an
/// unreadable row is an error, never a report that reads like an empty buffer.
fn compact_summary(row: &[Option<String>]) -> Result<(u64, usize)> {
    let cell = |i: usize, what: &str| -> Result<u64> {
        row.get(i)
            .cloned()
            .flatten()
            .and_then(|s| s.parse().ok())
            .with_context(|| {
                format!(
                    "compaction ran (buffer dropped) but its summary row had no readable \
                     {what}: {row:?}"
                )
            })
    };
    Ok((cell(0, "changes_rows")?, cell(1, "merge_jobs")? as usize))
}

fn is_meta_column(name: &str) -> bool {
    crate::load::cdc::is_meta_column(name)
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
