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

use crate::destination::gcs::GcsStore;
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
    /// Whether `cleanup_source` wiped the staged Parquet after this load — mirrors
    /// [`LoadReport::source_cleaned`] so the report + logs reflect it for CDC/
    /// incremental too, instead of discarding it.
    pub source_cleaned: bool,
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

    /// The warehouse this adapter targets — lets the shared driver build the
    /// current-state view SQL (dialect keyword + identifier quoting) in ONE place
    /// per mode instead of once per adapter.
    fn warehouse(&self) -> cdc::Warehouse;

    /// `CREATE OR REPLACE` the current-state view `<table>` from pre-built
    /// `view_sql` (the driver builds it via [`cdc::dedup_view_sql`] for CDC or
    /// [`cdc::inc_dedup_view_sql`] for incremental). The adapter only executes it
    /// its way (e.g. Snowflake prefixes a `QUERY_TAG`).
    fn create_view(&self, table: &str, view_sql: &str) -> Result<()>;
}

/// A plain SQL identifier the load layer can safely interpolate into DDL/COPY
/// without quoting: `[A-Za-z_][A-Za-z0-9_]*`. Round-5: column names are
/// SOURCE-derived and spliced raw into executed warehouse SQL (build_schema,
/// build_copy_select, …), so a name outside this set is an injection vector.
fn is_safe_load_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Refuse any Parquet URI that can't be splice-safely single-quoted into the
/// warehouse load statement. The drivers emit each URI as `'{uri}'` into
/// Snowflake `COPY … FILES=(…)` and BigQuery `LOAD DATA … uris=[…]` with NO
/// escaping (snowflake::copy_files_clause, bigquery::from_files) — so a URI
/// carrying the string delimiter `'`, a backslash (Snowflake treats `\` as an
/// in-string escape), or a control char could break out of the literal and
/// inject SQL that runs with the warehouse's (broad) role. Unlike the operator-
/// typed dataset/warehouse names, these URIs come from the LIVE GCS object
/// listing (reconcile::select_load_uris → store.list_files), and GCS object
/// names legally permit `'`/`\`/`;` — so a party with staging-bucket write plus
/// a crafted passing manifest is otherwise an injection vector. This is the
/// storage-sourced sibling of the column/table/pk gate; rivet names its own
/// parts run-uniquely and filename-sanitized, so a legitimate URI never trips
/// it — default-deny, fail loud rather than escape-and-hope.
fn ensure_safe_load_uris(uris: &[String]) -> Result<()> {
    for u in uris {
        if let Some(bad) = u
            .chars()
            .find(|&c| c == '\'' || c == '\\' || c.is_control())
        {
            bail!(
                "refusing to load: Parquet URI `{}` contains {:?}, which is unsafe to splice \
                 into the warehouse load statement — the loader single-quotes URIs without \
                 escaping. rivet names its own parts safely, so this URI was not produced by a \
                 normal export; investigate the staging bucket before re-running.",
                u.escape_default(),
                bad
            );
        }
    }
    Ok(())
}

/// Refuse a load whose specs can't materialize: empty, any `Fail`-status column
/// (a silent-loss class — never drop it, name it), or an unsafe column identifier.
fn validate_specs(table: &str, specs: &[TargetColumnSpec]) -> Result<()> {
    if specs.is_empty() {
        bail!("no column specs for `{table}` — nothing to build a schema from");
    }
    // Round-6: the target table name is interpolated raw into the fqtn / DDL / dedup
    // view too (a sibling injection surface of the column names). Gate it, tolerating
    // a qualified `dataset.table` — each dot-separated component must be a plain ident.
    if table.is_empty() || !table.split('.').all(is_safe_load_ident) {
        bail!(
            "cannot load: target table `{}` is not a plain (optionally dotted) SQL identifier — \
             the loader splices it into DDL/COPY.",
            table.escape_default()
        );
    }
    // Round-5: refuse a source-derived column name that isn't a plain identifier —
    // the warehouse drivers interpolate it into executed DDL/COPY with no quoting,
    // so a hostile/odd name (`x); DROP TABLE …`, an embedded quote/backtick) must
    // fail LOUDLY here rather than run as SQL. The one gate covers every load target.
    for s in specs {
        if !is_safe_load_ident(&s.column_name) {
            bail!(
                "cannot load `{table}`: column name `{}` is not a plain SQL identifier \
                 ([A-Za-z_][A-Za-z0-9_]*) — the warehouse loader splices it into DDL/COPY. \
                 Rename or alias the column in the export query.",
                s.column_name.escape_default()
            );
        }
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

/// Clean up iff `cleanup` is `Some`, downgrading a failure to a warning — the
/// data is loaded and gated, so a stuck delete must not fail the load. Cleanup
/// runs the driver's own [`delete_under`] over an injected [`GcsStore`], so no
/// adapter owns a delete path. Returns whether the source was actually cleaned.
fn maybe_cleanup(cleanup: Option<(&GcsStore, &str)>) -> bool {
    match cleanup {
        Some((store, prefix)) => match delete_under(store, prefix) {
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
/// and — only after the gate passes — clean up the source via `cleanup`
/// (`Some((store, gs_prefix))` to delete, `None` to keep it).
///
/// `#[allow(private_interfaces)]` for the injected `GcsStore` — same rationale as
/// [`reconcile::fetch_manifests_keyed`]: a `pub` public-API root over a
/// deliberately crate-private `destination` type.
#[allow(private_interfaces)]
pub fn run_load(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    expected_rows: Option<u64>,
    cleanup: Option<(&GcsStore, &str)>,
) -> Result<LoadReport> {
    if uris.is_empty() {
        bail!("no Parquet URIs to load into `{table}`");
    }
    ensure_safe_load_uris(uris)?;
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

    let source_cleaned = maybe_cleanup(cleanup);
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
// the load, pk + engine shape the dedup view, expected_delta + cleanup are the
// gate and cleanup. Bundling them would only move the fields elsewhere.
// `allow(private_interfaces)` for the injected `GcsStore` — see [`run_load`].
#[allow(clippy::too_many_arguments, private_interfaces)]
/// The shared append-log + dedup-view driver for the two append modes (CDC and
/// incremental). They differ ONLY in a label (for error text) and which view the
/// `build_view` closure creates; everything else — the empty-uris/pk bails, the
/// `__changes` append, the count gate, cleanup ordering, and the report — is
/// identical, so it lives here. `label` is `"CDC"` / `"incremental"`.
fn append_and_view(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    pk: &[String],
    expected_delta: Option<u64>,
    cleanup: Option<(&GcsStore, &str)>,
    label: &str,
    build_view: impl FnOnce(&dyn TargetLoader) -> Result<()>,
) -> Result<CdcLoadReport> {
    if uris.is_empty() {
        bail!("no Parquet URIs to append into `{table}__changes`");
    }
    ensure_safe_load_uris(uris)?;
    if pk.is_empty() {
        bail!("{label} load of `{table}` needs a primary key for the dedup view (pass --pk)");
    }
    validate_specs(&format!("{table}__changes"), specs)?;

    let rows_appended = loader.append_changelog(table, specs, uris, pk)?;

    if let Some(expected) = expected_delta
        && rows_appended != expected
    {
        bail!(
            "{label} count validation failed for `{}__changes`: appended {rows_appended} rows, \
             expected {expected} from the run manifests — investigate before trusting the view",
            table
        );
    }

    build_view(loader)?;
    // Cleanup runs here (inside the driver, after the gate), BEFORE the caller
    // records the ledger in `execute_load`. A crash between the two re-appends
    // this run next load — an at-least-once double-append the dedup view absorbs
    // (and the count gate still guards) — accepted rather than ordering the
    // irreversible delete after the durable record.
    let source_cleaned = maybe_cleanup(cleanup);

    Ok(CdcLoadReport {
        rows_appended,
        changes_table: loader.fqtn(&format!("{table}__changes")),
        view: loader.fqtn(table),
        source_cleaned,
    })
}

#[allow(clippy::too_many_arguments, private_interfaces)]
pub fn run_load_cdc(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    pk: &[String],
    engine: cdc::SourceEngine,
    expected_delta: Option<u64>,
    cleanup: Option<(&GcsStore, &str)>,
) -> Result<CdcLoadReport> {
    // Round-6: the CDC primary-key columns are interpolated raw into the dedup view's
    // PARTITION BY — gate them like the column names (source-derived injection surface).
    for c in pk {
        if !is_safe_load_ident(c) {
            bail!(
                "cannot load `{table}`: CDC primary-key column `{}` is not a plain SQL \
                 identifier — it is spliced into the dedup view. Rename/alias it.",
                c.escape_default()
            );
        }
    }
    append_and_view(
        loader,
        table,
        specs,
        uris,
        pk,
        expected_delta,
        cleanup,
        "CDC",
        |l| {
            let pk_refs: Vec<&str> = pk.iter().map(String::as_str).collect();
            let sql = cdc::dedup_view_sql(
                l.warehouse(),
                &l.fqtn(table),
                &l.fqtn(&format!("{table}__changes")),
                &pk_refs,
                engine,
            );
            l.create_view(table, &sql)
        },
    )
}

/// Load an INCREMENTAL export's delta: APPEND the parquet into `<table>__changes`
/// (reusing the CDC changelog append — the delta's rows land with NULL `__op`/
/// `__pos`/`__seq`, which the view drops) and (re)build a current-state view
/// deduped to the latest row per PK by `cursor_column`. The manifests' summed
/// `row_count` gates the appended delta, and cleanup runs (only) after the gate —
/// safe because the ledger, not the file prefix, records what's loaded.
// Same arity shape as [`run_load_cdc`] (the cursor replaces the engine);
// `allow(private_interfaces)` for the injected `GcsStore` — see [`run_load`].
#[allow(clippy::too_many_arguments, private_interfaces)]
pub fn run_load_incremental(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    pk: &[String],
    cursor_column: &str,
    expected_delta: Option<u64>,
    cleanup: Option<(&GcsStore, &str)>,
) -> Result<CdcLoadReport> {
    // uris + pk are checked by `append_and_view`; the cursor guards are incremental-only.
    if cursor_column.is_empty() {
        bail!(
            "incremental load of `{table}` needs a cursor column (the export's `cursor_column:`) \
             for the dedup view's latest-per-PK ordering"
        );
    }
    // The cursor must be an EXPORTED column: the dedup view orders `__changes` by
    // it (`ORDER BY <cursor> DESC`). A cursor used only in the extract's WHERE and
    // not projected (e.g. `SELECT id, v` with `cursor_column: updated_at`, or
    // incremental-coalesce which strips its synthetic cursor) is absent from
    // `__changes`, so the view creation would fail AFTER the append — turn that
    // into a loud pre-append bail instead of a broken view + a retried re-append.
    if !specs.iter().any(|s| s.column_name == cursor_column) {
        let cols: Vec<&str> = specs.iter().map(|s| s.column_name.as_str()).collect();
        bail!(
            "incremental load of `{table}`: cursor_column `{cursor_column}` is not one of the \
             exported columns [{}] — add it to the export's SELECT so the dedup view can order \
             the change log by it",
            cols.join(", ")
        );
    }
    append_and_view(
        loader,
        table,
        specs,
        uris,
        pk,
        expected_delta,
        cleanup,
        "incremental",
        |l| {
            let pk_refs: Vec<&str> = pk.iter().map(String::as_str).collect();
            let sql = cdc::inc_dedup_view_sql(
                l.warehouse(),
                &l.fqtn(table),
                &l.fqtn(&format!("{table}__changes")),
                &pk_refs,
                cursor_column,
            );
            l.create_view(table, &sql)
        },
    )
}

/// Split a `gs://bucket/path` URI into `(bucket, bucket-relative path)` — the
/// shape opendal's bucket-scoped operator wants.
pub(crate) fn split_gs_uri(uri: &str) -> Result<(&str, &str)> {
    uri.strip_prefix("gs://")
        .and_then(|rest| rest.split_once('/'))
        .with_context(|| format!("not a `gs://bucket/path` URI: {uri}"))
}

/// Recursively delete a whole export-dedicated `gs://…/` prefix through an
/// injected [`GcsStore`] — the driver's post-gate source cleanup, over the same
/// native opendal GCS client the export destination uses (no `gcloud`). Taking
/// the store as an argument (rather than each adapter building one from a
/// config) is what lets an fs-backed store exercise this delete offline.
pub(crate) fn delete_under(store: &GcsStore, gs_prefix: &str) -> Result<()> {
    let (_, rel) = split_gs_uri(gs_prefix)?;
    store
        .remove_all(rel)
        .with_context(|| format!("source cleanup (recursive delete of {gs_prefix}) failed"))
}

/// Open the one [`GcsStore`] a load reuses for reconcile, URI listing, and
/// post-gate cleanup — the single production constructor `cli::dispatch` calls.
///
/// `pub` (a public-API root the lib keeps alive) even though its only caller is
/// the binary-only dispatch: it re-anchors `GcsStore`'s real-GCS constructor in
/// the lib compilation unit, which no longer reaches it through a load adapter.
/// `#[allow(private_interfaces)]` for the crate-private return — same rationale
/// as [`reconcile::fetch_manifests_keyed`].
#[allow(private_interfaces)]
pub fn open_store(dest: &crate::config::DestinationConfig) -> Result<GcsStore> {
    GcsStore::new(dest)
}

/// The one place a resolved plan's [`LoadTarget`](plan::LoadTarget) maps to a
/// concrete [`TargetLoader`] adapter — wiring partition / cluster / connection /
/// run-id from the config. The count gate and cleanup are the driver's, so the
/// adapter carries no `expected_rows`.
pub fn build_loader(plan: &plan::LoadPlan, run_id: &str) -> Box<dyn TargetLoader> {
    use plan::LoadTarget;
    let load = &plan.load;
    match &load.target {
        LoadTarget::Bigquery { project, dataset } => Box::new(build_bigquery_loader(
            project,
            dataset,
            plan.partition_by.as_deref(),
            &load.cluster_by,
            run_id,
        )),
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
            Box::new(l)
        }
    }
}

/// Wire a [`BigQueryLoader`] from a resolved plan's fields — `partition_by` and
/// `cluster_by` applied ONLY when set. A concrete return (not the boxed trait)
/// so the wiring is unit-testable: a mis-guarded key would silently DROP the
/// clustering/partitioning the config asked for — a degradation invisible
/// through `Box<dyn TargetLoader>`.
fn build_bigquery_loader(
    project: &str,
    dataset: &str,
    partition_by: Option<&str>,
    cluster_by: &[String],
    run_id: &str,
) -> BigQueryLoader {
    let mut l = BigQueryLoader::new(project, dataset).run_id(run_id);
    if let Some(part) = partition_by {
        l = l.partition_by(part);
    }
    if !cluster_by.is_empty() {
        l = l.cluster_by(cluster_by.to_vec());
    }
    l
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
        fn warehouse(&self) -> cdc::Warehouse {
            cdc::Warehouse::BigQuery
        }
        fn create_view(&self, table: &str, _view_sql: &str) -> Result<()> {
            self.views.borrow_mut().push(table.into());
            Ok(())
        }
    }

    /// An fs-backed [`GcsStore`] seeded with one object under the bucket-relative
    /// `rel` — stands in for the export's live GCS source prefix so the driver's
    /// real delete path (`delete_under` → `remove_all`) runs offline. Returns the
    /// store; the caller keeps `dir` alive for the store's lifetime.
    fn fs_store_with_prefix(dir: &tempfile::TempDir, rel: &str) -> GcsStore {
        let obj = dir.path().join(rel).join("x.parquet");
        std::fs::create_dir_all(obj.parent().unwrap()).unwrap();
        std::fs::write(obj, b"x").unwrap();
        GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap()
    }

    /// Whether the fs store still holds an object under bucket-relative `rel`.
    fn prefix_populated(store: &GcsStore, rel: &str) -> bool {
        !store.list_files(rel).unwrap().is_empty()
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
    /// The cleanup prefix the driver receives (a `gs://bucket/…` URI) and its
    /// bucket-relative form the fs store is keyed by.
    const PREFIX: &str = "gs://b/p";
    const REL: &str = "p";

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
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store_with_prefix(&dir, REL);
        let err = run_load(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(10),
            Some((&store, PREFIX)),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("count validation failed"), "{err}");
        assert!(
            prefix_populated(&store, REL),
            "cleanup must not run on a failed gate — the source prefix stays intact"
        );
    }

    #[test]
    fn match_with_prefix_cleans_once() {
        let f = FakeLoader {
            rows: 10,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store_with_prefix(&dir, REL);
        let r = run_load(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(10),
            Some((&store, PREFIX)),
        )
        .unwrap();
        assert!(r.source_cleaned);
        assert!(
            !prefix_populated(&store, REL),
            "a passed gate drains the source prefix through the injected store"
        );
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
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store_with_prefix(&dir, REL);
        let err = run_load_cdc(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".into()],
            cdc::SourceEngine::MySql,
            Some(5),
            Some((&store, PREFIX)),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("CDC count validation failed"), "{err}");
        assert!(
            f.views.borrow().is_empty(),
            "view must not be built on a failed gate"
        );
        assert!(
            prefix_populated(&store, REL),
            "cleanup must not run on a failed gate"
        );
    }

    #[test]
    fn cdc_match_builds_view_then_cleans() {
        let f = FakeLoader {
            rows: 5,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store_with_prefix(&dir, REL);
        let r = run_load_cdc(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".into()],
            cdc::SourceEngine::MySql,
            Some(5),
            Some((&store, PREFIX)),
        )
        .unwrap();
        assert_eq!(r.rows_appended, 5);
        assert_eq!(*f.views.borrow(), vec!["t".to_string()]);
        assert!(
            !prefix_populated(&store, REL),
            "a passed CDC gate drains the source prefix after the view is built"
        );
        assert_eq!(r.changes_table, "db.t__changes");
    }

    #[test]
    fn delete_under_drains_the_prefix_through_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store_with_prefix(&dir, REL);
        assert!(prefix_populated(&store, REL), "seeded object is present");
        delete_under(&store, PREFIX).unwrap();
        assert!(
            !prefix_populated(&store, REL),
            "delete_under recursively removes the bucket-relative prefix behind the gs:// URI"
        );
    }

    #[test]
    fn build_bigquery_loader_wires_partition_and_cluster_keys() {
        // A non-empty cluster/partition MUST reach the loader; if the guard
        // inverts, a real key is silently dropped and the load omits the
        // clustering the config asked for. Reading cluster_by/partition_by pins
        // the wiring that `Box<dyn TargetLoader>` hides.
        let l = build_bigquery_loader(
            "proj",
            "ds",
            Some("day"),
            &["customer_id".to_string(), "region".to_string()],
            "run-1",
        );
        assert_eq!(l.cluster_by, ["customer_id", "region"]);
        assert_eq!(l.partition_by.as_deref(), Some("day"));

        // No keys set → neither clause (the default), never a spurious one.
        let bare = build_bigquery_loader("proj", "ds", None, &[], "run-1");
        assert!(bare.cluster_by.is_empty());
        assert!(bare.partition_by.is_none());
    }

    #[test]
    fn split_gs_uri_parses_bucket_and_bucket_relative_key() {
        // The parse every load op addresses through: (bucket, bucket-relative
        // key). The `delete_under` test above can't pin this — it drains by REL
        // regardless of what split returns — so a mangled split (wrong bucket, or
        // an empty key that lists/deletes the whole bucket root) is invisible
        // there. Pin it directly.
        assert_eq!(split_gs_uri("gs://b/p").unwrap(), ("b", "p"));
        assert_eq!(
            split_gs_uri("gs://bucket/a/b/c.parquet").unwrap(),
            ("bucket", "a/b/c.parquet"),
            "only the FIRST '/' splits bucket from key; the rest is the key"
        );
        assert!(
            split_gs_uri("s3://b/p").is_err(),
            "a non-gs scheme is rejected"
        );
        assert!(
            split_gs_uri("gs://bucket-only").is_err(),
            "a bucket with no '/' has no (bucket, key) split"
        );
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

    #[test]
    fn a_uri_with_a_quote_backslash_or_control_char_bails_before_the_driver_runs() {
        // Storage-sourced injection: URIs come from the live GCS listing and are
        // spliced single-quoted, UNESCAPED, into COPY FILES=()/LOAD uris=[]. A
        // planted object key carrying the delimiter must be REFUSED loudly before
        // any driver SQL runs — never escaped-and-hoped. The driver must not be
        // touched (materialize/append never called).
        for bad in [
            "gs://b/p/x'; drop table t --.parquet", // breaks out of the '…' literal
            "gs://b/p/x\\.parquet",                 // backslash: Snowflake in-string escape
            "gs://b/p/x\n.parquet",                 // control char
        ] {
            let f = FakeLoader {
                rows: 1,
                ..Default::default()
            };
            let uris = vec![bad.to_string()];
            assert!(
                run_load(&f, "t", &spec(TargetStatus::Ok), &uris, Some(1), None).is_err(),
                "run_load must reject the injection URI {bad:?}"
            );
            assert!(
                f.materialized.borrow().is_empty(),
                "the driver must not be reached for {bad:?}"
            );
            assert!(
                run_load_cdc(
                    &f,
                    "t",
                    &spec(TargetStatus::Ok),
                    &uris,
                    &["id".to_string()],
                    cdc::SourceEngine::MySql,
                    Some(1),
                    None,
                )
                .is_err(),
                "run_load_cdc must reject the injection URI {bad:?}"
            );
            assert!(
                f.appended.borrow().is_empty(),
                "the CDC driver must not be reached for {bad:?}"
            );
        }
        // A normal rivet-produced URI still passes the gate.
        assert!(ensure_safe_load_uris(&uris()).is_ok());
    }

    #[test]
    fn incremental_cursor_not_in_specs_bails_before_append() {
        let f = FakeLoader::default();
        // The exported columns are just `id`; a cursor `updated_at` used only in
        // the extract's WHERE (not projected) is absent from `__changes`. The
        // driver must bail BEFORE appending — else the view creation fails after
        // the append and every retry re-appends (bloat).
        let err = run_load_incremental(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".to_string()],
            "updated_at",
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("updated_at") && err.contains("not one of the exported columns"),
            "{err}"
        );
        assert!(
            f.appended.borrow().is_empty(),
            "nothing appended before the bail"
        );
    }
}
