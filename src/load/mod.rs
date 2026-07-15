//! Warehouse load layer — the `TargetLoader` seam, its per-warehouse adapters,
//! and the warehouse-neutral load driver.
//!
//! OSS decides *what* a column becomes in the warehouse (`TargetColumnSpec` via
//! `ExportTarget::resolve_table`). A [`TargetLoader`] **adapter** runs the
//! warehouse-specific load ([`bigquery`] — free `LOAD DATA`; [`snowflake`] —
//! `COPY` off a GCS external stage). The **driver** ([`run_load`] /
//! [`run_load_cdc`]) owns the invariant orchestration — spec validation, the
//! count-integrity gate, the dedup-view wiring, and cleanup ordering — so those
//! invariants are exercised once through a fake adapter, not per warehouse.

use crate::types::target::{TargetColumnSpec, TargetStatus};
use anyhow::{Context, Result, bail};

mod bigquery;
pub mod cdc;
pub mod plan;
pub mod reconcile;
mod snowflake;

pub use bigquery::BigQueryLoader;
pub use snowflake::SnowflakeLoader;

/// Outcome of a successful batch load.
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub rows_loaded: u64,
    pub target_table: String,
    /// True when the source GCS objects were deleted after a verified load.
    pub source_cleaned: bool,
}

/// Outcome of a CDC change-log load: rows appended to the `<table>__changes`
/// log plus the current-state dedup view rebuilt over it.
#[derive(Debug, Clone)]
pub struct CdcLoadReport {
    pub rows_appended: u64,
    pub changes_table: String,
    pub view: String,
}

/// A warehouse **adapter** — the small, warehouse-specific seam the
/// [driver](run_load) drives. Dialect + CLI (`bq` / `snow`), the external stage,
/// BigQuery's 4,000-partition batch split, and `PARSE_JSON` all live *behind*
/// these primitives.
///
/// Idempotent under retry: Rivet is at-least-once at the file layer, so the same
/// Parquet object may be presented more than once; `materialize` overwrites.
pub trait TargetLoader {
    /// Fully-qualify `table` for this warehouse (`project.dataset.t` /
    /// `db.schema.t`).
    fn fqtn(&self, table: &str) -> String;

    /// Overwrite `table` with the Parquet at `uris`, materializing the native
    /// column types in `specs`. Returns the rows the load landed.
    fn materialize(&self, table: &str, specs: &[TargetColumnSpec], uris: &[String]) -> Result<u64>;

    /// Append the CDC change Parquet into `<table>__changes` (created if absent),
    /// prepending the `__op` / `__pos` / `__seq` meta columns to `specs`. Returns
    /// the rows this call appended.
    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        pk: &[String],
    ) -> Result<u64>;

    /// `CREATE OR REPLACE` the current-state dedup view `<table>` over its change
    /// log — the adapter builds its warehouse's dialect SQL via
    /// [`cdc::dedup_view_sql`].
    fn create_dedup_view(
        &self,
        table: &str,
        pk: &[String],
        engine: cdc::SourceEngine,
    ) -> Result<()>;

    /// Delete the export's source GCS `prefix` — called by the driver only after
    /// a verified load.
    fn cleanup(&self, prefix: &str) -> Result<()>;
}

/// Refuse a load whose specs can't materialize: empty, or any `Fail`-status
/// column (a silent-loss class — never drop it, name it).
fn validate_specs(table: &str, specs: &[TargetColumnSpec]) -> Result<()> {
    if specs.is_empty() {
        bail!("no column specs for `{table}` — nothing to build a schema from");
    }
    let failed: Vec<&str> = specs
        .iter()
        .filter(|s| s.status == TargetStatus::Fail)
        .map(|s| s.column_name.as_str())
        .collect();
    if !failed.is_empty() {
        bail!(
            "cannot load `{table}`: {} column(s) do not map to the warehouse: {}",
            failed.len(),
            failed.join(", ")
        );
    }
    Ok(())
}

/// Clean up iff `prefix` is `Some`, downgrading a failure to a warning — the
/// data is loaded and gated, so a stuck delete must not fail the load. Returns
/// whether the source was actually cleaned.
fn maybe_cleanup(loader: &dyn TargetLoader, prefix: Option<&str>) -> bool {
    match prefix {
        Some(p) => match loader.cleanup(p) {
            Ok(()) => true,
            Err(e) => {
                eprintln!("warning: source cleanup failed (data is safely loaded): {e:#}");
                false
            }
        },
        None => false,
    }
}

/// **Batch load driver.** Materialize `table` from `uris`, gate the landed rows
/// against `expected_rows` (the reconciled file count; `None` skips the gate),
/// and — only after the gate passes — clean up the source at `cleanup_prefix`.
pub fn run_load(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    expected_rows: Option<u64>,
    cleanup_prefix: Option<&str>,
) -> Result<LoadReport> {
    if uris.is_empty() {
        bail!("no Parquet URIs to load into `{table}`");
    }
    validate_specs(table, specs)?;

    let rows_loaded = loader.materialize(table, specs, uris)?;

    if let Some(expected) = expected_rows
        && rows_loaded != expected
    {
        bail!(
            "count validation failed for `{}`: loaded {rows_loaded} rows, expected {expected} — \
             NOT cleaning up source; investigate before re-running",
            loader.fqtn(table)
        );
    }

    let source_cleaned = maybe_cleanup(loader, cleanup_prefix);
    Ok(LoadReport {
        rows_loaded,
        target_table: loader.fqtn(table),
        source_cleaned,
    })
}

/// **CDC load driver.** Append the change log, gate the appended delta against
/// `expected_delta` (`None` skips the gate), (re)build the current-state dedup
/// view, then clean up the source.
// The arity is the CDC load's real surface: adapter + table + specs + uris are
// the load, pk + engine shape the dedup view, expected_delta + cleanup_prefix
// are the gate and cleanup. Bundling them would only move the fields elsewhere.
#[allow(clippy::too_many_arguments)]
pub fn run_load_cdc(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    pk: &[String],
    engine: cdc::SourceEngine,
    expected_delta: Option<u64>,
    cleanup_prefix: Option<&str>,
) -> Result<CdcLoadReport> {
    if uris.is_empty() {
        bail!("no Parquet URIs to append into `{table}__changes`");
    }
    if pk.is_empty() {
        bail!("CDC load of `{table}` needs a primary key for the dedup view (pass --pk)");
    }
    validate_specs(&format!("{table}__changes"), specs)?;

    let rows_appended = loader.append_changelog(table, specs, uris, pk)?;

    if let Some(expected) = expected_delta
        && rows_appended != expected
    {
        bail!(
            "CDC count validation failed for `{}__changes`: appended {rows_appended} rows, \
             expected {expected} from the run manifests — investigate before trusting the view",
            table
        );
    }

    loader.create_dedup_view(table, pk, engine)?;
    let _ = maybe_cleanup(loader, cleanup_prefix);

    Ok(CdcLoadReport {
        rows_appended,
        changes_table: loader.fqtn(&format!("{table}__changes")),
        view: loader.fqtn(table),
    })
}

/// Split a `gs://bucket/path` URI into `(bucket, bucket-relative path)` — the
/// shape opendal's bucket-scoped operator wants.
pub(crate) fn split_gs_uri(uri: &str) -> Result<(&str, &str)> {
    uri.strip_prefix("gs://")
        .and_then(|rest| rest.split_once('/'))
        .with_context(|| format!("not a `gs://bucket/path` URI: {uri}"))
}

/// Delete a whole export-dedicated `gs://…/` prefix — the shared cleanup every
/// adapter's [`TargetLoader::cleanup`] routes through, over the same native
/// opendal GCS client the export destination uses (no `gcloud`).
pub(crate) fn delete_prefix(dest: &crate::config::DestinationConfig, prefix: &str) -> Result<()> {
    let (_, rel) = split_gs_uri(prefix)?;
    crate::destination::gcs::GcsStore::new(dest)?
        .remove_all(rel)
        .with_context(|| format!("source cleanup (recursive delete of {prefix}) failed"))
}

/// The one place a resolved plan's [`LoadTarget`](plan::LoadTarget) maps to a
/// concrete [`TargetLoader`] adapter — wiring partition / cluster / connection /
/// run-id from the config. The count gate and cleanup are the driver's, so the
/// adapter carries no `expected_rows`.
pub fn build_loader(plan: &plan::LoadPlan, run_id: &str) -> Box<dyn TargetLoader> {
    use plan::LoadTarget;
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
            l.destination = Some(plan.destination.clone());
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
            l.run_id = Some(run_id.to_string());
            // Snowflake's external stage wants the `gcs://` scheme, not `gs://`.
            l.gcs_url = plan.gcs_prefix.replacen("gs://", "gcs://", 1);
            // The `snow` CLI does not expand `~`; pass an absolute key path.
            l.private_key_path = std::env::var("RIVET_SNOWFLAKE_KEY").ok();
            l.destination = Some(plan.destination.clone());
            Box::new(l)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Records every call and returns a canned row count — the seam the driver's
    /// invariants are asserted through, offline.
    #[derive(Default)]
    struct FakeLoader {
        rows: u64,
        materialized: RefCell<Vec<String>>,
        appended: RefCell<Vec<String>>,
        views: RefCell<Vec<String>>,
        cleaned: RefCell<Vec<String>>,
    }

    impl TargetLoader for FakeLoader {
        fn fqtn(&self, table: &str) -> String {
            format!("db.{table}")
        }
        fn materialize(&self, table: &str, _: &[TargetColumnSpec], _: &[String]) -> Result<u64> {
            self.materialized.borrow_mut().push(table.into());
            Ok(self.rows)
        }
        fn append_changelog(
            &self,
            table: &str,
            _: &[TargetColumnSpec],
            _: &[String],
            _: &[String],
        ) -> Result<u64> {
            self.appended.borrow_mut().push(table.into());
            Ok(self.rows)
        }
        fn create_dedup_view(&self, table: &str, _: &[String], _: cdc::SourceEngine) -> Result<()> {
            self.views.borrow_mut().push(table.into());
            Ok(())
        }
        fn cleanup(&self, prefix: &str) -> Result<()> {
            self.cleaned.borrow_mut().push(prefix.into());
            Ok(())
        }
    }

    fn spec(status: TargetStatus) -> Vec<TargetColumnSpec> {
        vec![TargetColumnSpec {
            column_name: "id".into(),
            target_type: "INT64".into(),
            autoload_type: String::new(),
            status,
            note: None,
            cast_sql: None,
        }]
    }
    fn uris() -> Vec<String> {
        vec!["gs://b/p/x.parquet".into()]
    }
    const PREFIX: &str = "gs://b/p/";

    #[test]
    fn empty_uris_bail_before_materialize() {
        let f = FakeLoader {
            rows: 10,
            ..Default::default()
        };
        assert!(run_load(&f, "t", &spec(TargetStatus::Ok), &[], Some(10), None).is_err());
        assert!(f.materialized.borrow().is_empty());
    }

    #[test]
    fn fail_spec_bails_before_materialize() {
        let f = FakeLoader::default();
        assert!(run_load(&f, "t", &spec(TargetStatus::Fail), &uris(), Some(10), None).is_err());
        assert!(f.materialized.borrow().is_empty());
    }

    #[test]
    fn count_mismatch_bails_without_cleanup() {
        let f = FakeLoader {
            rows: 7,
            ..Default::default()
        };
        let err = run_load(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(10),
            Some(PREFIX),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("count validation failed"), "{err}");
        assert!(
            f.cleaned.borrow().is_empty(),
            "cleanup must not run on a failed gate"
        );
    }

    #[test]
    fn match_with_prefix_cleans_once() {
        let f = FakeLoader {
            rows: 10,
            ..Default::default()
        };
        let r = run_load(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(10),
            Some(PREFIX),
        )
        .unwrap();
        assert!(r.source_cleaned);
        assert_eq!(*f.cleaned.borrow(), vec![PREFIX.to_string()]);
        assert_eq!(r.target_table, "db.t");
    }

    #[test]
    fn match_without_prefix_does_not_clean() {
        let f = FakeLoader {
            rows: 10,
            ..Default::default()
        };
        let r = run_load(&f, "t", &spec(TargetStatus::Ok), &uris(), Some(10), None).unwrap();
        assert!(!r.source_cleaned);
        assert!(f.cleaned.borrow().is_empty());
    }

    #[test]
    fn none_expected_skips_the_gate() {
        let f = FakeLoader {
            rows: 999,
            ..Default::default()
        };
        // No expected count → any landed rows pass (an ad-hoc load).
        assert!(run_load(&f, "t", &spec(TargetStatus::Ok), &uris(), None, None).is_ok());
    }

    #[test]
    fn cdc_delta_mismatch_bails_without_view() {
        let f = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        let err = run_load_cdc(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".into()],
            cdc::SourceEngine::MySql,
            Some(5),
            Some(PREFIX),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("CDC count validation failed"), "{err}");
        assert!(
            f.views.borrow().is_empty(),
            "view must not be built on a failed gate"
        );
        assert!(f.cleaned.borrow().is_empty());
    }

    #[test]
    fn cdc_match_builds_view_then_cleans() {
        let f = FakeLoader {
            rows: 5,
            ..Default::default()
        };
        let r = run_load_cdc(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".into()],
            cdc::SourceEngine::MySql,
            Some(5),
            Some(PREFIX),
        )
        .unwrap();
        assert_eq!(r.rows_appended, 5);
        assert_eq!(*f.views.borrow(), vec!["t".to_string()]);
        assert_eq!(*f.cleaned.borrow(), vec![PREFIX.to_string()]);
        assert_eq!(r.changes_table, "db.t__changes");
    }

    #[test]
    fn cdc_empty_pk_bails() {
        let f = FakeLoader::default();
        assert!(
            run_load_cdc(
                &f,
                "t",
                &spec(TargetStatus::Ok),
                &uris(),
                &[],
                cdc::SourceEngine::MySql,
                None,
                None
            )
            .is_err()
        );
        assert!(f.appended.borrow().is_empty());
    }
}
