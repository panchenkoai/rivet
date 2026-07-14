//! Warehouse loaders — the `TargetLoader` seam and its per-warehouse impls.
//!
//! OSS decides *what* a column becomes in the warehouse (`TargetColumnSpec`,
//! via `ExportTarget::resolve_table`); a [`TargetLoader`] executes that plan
//! against a live warehouse. Each warehouse lives in its own submodule:
//! [`bigquery`] (free `LOAD DATA`) and [`snowflake`] (`COPY` off a GCS external
//! stage; billed compute, `PARSE_JSON` transform for `VARIANT` columns).

use crate::types::target::{ExportTarget, TargetColumnSpec};
use anyhow::{Context, Result, bail};
use std::process::Command;

mod bigquery;
pub mod cdc;
pub mod plan;
pub mod reconcile;
mod snowflake;

pub use bigquery::BigQueryLoader;
pub use snowflake::SnowflakeLoader;

/// How a load writes the target table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Replace the table (`LOAD DATA OVERWRITE` / `CREATE OR REPLACE`).
    /// Idempotent under retry — the default.
    #[default]
    Overwrite,
    /// Append (`LOAD DATA INTO`). NOT idempotent under retry; not supported for
    /// tables that need native-type recovery (would need MERGE).
    Append,
}

/// Outcome of a successful load.
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub rows_loaded: u64,
    pub target_table: String,
    /// Number of free `LOAD DATA` jobs issued — 1 normally, N when a
    /// daily-partitioned input was split to stay under the per-job cap.
    pub load_jobs: usize,
    /// True when the source GCS objects were deleted after a verified load.
    pub source_cleaned: bool,
}

/// Outcome of a CDC change-log load: rows appended to the `<table>__changes`
/// log plus the current-state dedup view rebuilt over it.
#[derive(Debug, Clone)]
pub struct CdcLoadReport {
    /// Rows this load appended to the change log — the after-minus-before delta,
    /// gated against the manifests' summed `row_count`.
    pub rows_appended: u64,
    /// The fully-qualified `<table>__changes` append-only log.
    pub changes_table: String,
    /// The fully-qualified current-state view (`CREATE OR REPLACE`d each load).
    pub view: String,
}

/// Executes a resolved warehouse plan against a live target.
///
/// Implementations MUST be idempotent under retry: Rivet is at-least-once at
/// the file layer, so the same Parquet object can be presented more than once.
pub trait TargetLoader {
    /// Which OSS resolver this loader pairs with.
    fn target(&self) -> ExportTarget;

    /// Load exported Parquet at `parquet_uris` into `table`, materializing the
    /// native column types described by `specs` (from `resolve_table`).
    fn load(
        &mut self,
        table: &str,
        specs: &[TargetColumnSpec],
        parquet_uris: &[String],
    ) -> Result<LoadReport>;

    /// Load a CDC change log: **append** the change Parquet into the
    /// `<table>__changes` log (the free/`COPY` path, prepending the `__op` /
    /// `__pos` / `__seq` meta columns to `specs`), then `CREATE OR REPLACE` the
    /// current-state dedup view `<table>` over it (see [`crate::load::cdc`]).
    ///
    /// `pk` is the change log's primary key (the view's dedup partition);
    /// `engine` selects the `__pos` parse. `expected_delta` — the manifests'
    /// summed `row_count` — gates the count of rows *this* load appended
    /// (before/after the append), the file→warehouse leg for an accumulating
    /// log where the batch whole-table count gate does not apply.
    fn load_cdc(
        &mut self,
        table: &str,
        specs: &[TargetColumnSpec],
        parquet_uris: &[String],
        pk: &[String],
        engine: crate::load::cdc::SourceEngine,
        expected_delta: Option<u64>,
    ) -> Result<CdcLoadReport>;
}

/// Delete a whole export-dedicated `gs://…/` prefix in one recursive `gcloud`
/// call — fast (one call, not one per object) and removes the export's
/// `_SUCCESS`/manifest too, not just the loaded Parquet. `--continue-on-error`
/// so an already-gone object (a re-run) doesn't abort the cleanup. The
/// `destination` (GCS) concern shared by every loader Adapter, not per-target.
pub(crate) fn delete_prefix(prefix: &str) -> Result<()> {
    let out = Command::new("gcloud")
        .args(["storage", "rm", "-r", "-q", "--continue-on-error", prefix])
        .output()
        .context("running `gcloud storage rm -r` for source cleanup")?;
    if !out.status.success() {
        bail!(
            "source cleanup (`gcloud storage rm -r {prefix}`) failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// The one place a resolved plan's [`LoadTarget`](crate::load::plan::LoadTarget) maps
/// to a concrete [`TargetLoader`], wiring partition / cluster / cleanup /
/// connection from the config plus the load-run `run_id` (stamped as the
/// BigQuery `rivet_run` job label / Snowflake `QUERY_TAG`, so cost slices per
/// run as well as per table). The gated `load` command and the example drivers
/// all go through here, so the mapping can't drift between them.
///
/// `expected_rows` — the authoritative file-side row count reconciled from the
/// run manifests (see [`crate::load::reconcile`]) — is wired into the loader's own
/// post-load completeness gate: the load `bail!`s (before any source cleanup)
/// unless the warehouse `COUNT(*)` equals it, closing the file→warehouse leg of
/// the source→file→warehouse chain. `None` skips the gate (e.g. an ad-hoc load
/// with no manifest to reconcile against).
pub fn build_loader(
    plan: &crate::load::plan::LoadPlan,
    run_id: &str,
    expected_rows: Option<u64>,
) -> Box<dyn TargetLoader> {
    use crate::load::plan::LoadTarget;
    let load = &plan.load;
    match &load.target {
        LoadTarget::Bigquery { project, dataset } => {
            let mut l = BigQueryLoader::new(project.clone(), dataset.clone()).run_id(run_id);
            if let Some(part) = plan.partition_by.clone() {
                l = l.partition_by(part);
            }
            if !load.cluster_by.is_empty() {
                l = l.cluster_by(load.cluster_by.clone());
            }
            if let Some(n) = expected_rows {
                l = l.expect_rows(n);
            }
            if load.cleanup_source {
                l = l
                    .cleanup_source(true)
                    .cleanup_prefix(plan.gcs_prefix.clone());
            }
            Box::new(l)
        }
        LoadTarget::Snowflake {
            connection,
            warehouse,
            database,
            schema,
            storage_integration,
        } => {
            let mut l = SnowflakeLoader::new(connection.clone());
            l.warehouse = warehouse.clone();
            l.database = database.clone();
            l.schema = schema.clone();
            l.storage_integration = storage_integration.clone();
            l.cluster_by = load.cluster_by.clone();
            l.expected_rows = expected_rows;
            l.run_id = Some(run_id.to_string());
            // Snowflake's external stage wants the `gcs://` scheme, not `gs://`.
            l.gcs_url = plan.gcs_prefix.replacen("gs://", "gcs://", 1);
            // The `snow` CLI does not expand `~`; pass an absolute key path.
            l.private_key_path = std::env::var("RIVET_SNOWFLAKE_KEY").ok();
            if load.cleanup_source {
                l.cleanup_source = true;
                l.cleanup_prefix = Some(plan.gcs_prefix.clone());
            }
            Box::new(l)
        }
    }
}
