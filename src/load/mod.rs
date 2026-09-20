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
mod bq_rest;
pub mod cdc;
pub mod orchestrate;
pub(crate) mod partition_budget;
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
/// What one `rivet compact` did to one table.
#[derive(Debug, Clone)]
pub struct CompactReport {
    pub base: String,
    /// Rows the buffer held before the merge.
    pub changes_rows: u64,
    /// MERGE statements run (one per partition window).
    pub merge_jobs: usize,
    /// Whether a buffer existed to compact at all.
    pub had_buffer: bool,
}

#[derive(Debug, Clone)]
pub struct CdcLoadReport {
    pub rows_appended: u64,
    pub changes_table: String,
    /// The table consumers read, named by the driver that built it: the dedup VIEW
    /// over the changelog, or the physical BASE the buffer is compacted into.
    pub target: String,
    pub target_kind: ChangelogTarget,
    /// Whether `cleanup_source` wiped the staged Parquet after this load — mirrors
    /// [`LoadReport::source_cleaned`] so the report + logs reflect it for CDC/
    /// incremental too, instead of discarding it.
    pub source_cleaned: bool,
}

/// What `CdcLoadReport::target` is: the dedup view of the changelog + view layout,
/// or the physical base of the base-and-buffer layout (compacted from the buffer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangelogTarget {
    View,
    Base,
}

/// What a name currently is in the warehouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    Absent,
    Table,
    View,
    Other,
}

impl ObjectKind {
    /// Decode an adapter probe's `1·table + 2·view + 4·other` code.
    pub(crate) fn from_probe(code: u64) -> Result<Self> {
        Ok(match code {
            0 => ObjectKind::Absent,
            1 => ObjectKind::Table,
            2 => ObjectKind::View,
            4 => ObjectKind::Other,
            n => bail!("object-kind probe returned {n}, expected 0, 1, 2 or 4"),
        })
    }
}

/// A warehouse **adapter** — the small, warehouse-specific seam the
/// [driver](run_load) drives. Dialect + transport (BigQuery's REST API since
/// #264, Snowflake's `snow` CLI), the external stage,
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

    /// Merge `<table>__changes` (the base-and-buffer layout's per-cycle buffer)
    /// into the base `table` and drop the buffer. `specs` are the base's source
    /// columns, `pk` the merge key, `engine` how `__pos` orders the changes.
    fn compact(
        &self,
        table: &str,
        _specs: &[TargetColumnSpec],
        _pk: &[String],
        _order: cdc::CompactOrder,
    ) -> Result<CompactReport> {
        bail!(
            "`rivet compact` is BigQuery-only in this release — `{}` targets {:?}",
            self.fqtn(table),
            self.warehouse()
        )
    }

    /// The warehouse this adapter targets — lets the shared driver build the
    /// current-state view SQL (dialect keyword + identifier quoting) in ONE place
    /// per mode instead of once per adapter.
    fn warehouse(&self) -> cdc::Warehouse;

    /// `CREATE OR REPLACE` the current-state view `<table>` from pre-built
    /// `view_sql` (the driver builds it via [`cdc::dedup_view_sql`] for CDC or
    /// [`cdc::inc_dedup_view_sql`] for incremental). The adapter only executes it
    /// its way (e.g. Snowflake prefixes a `QUERY_TAG`).
    fn create_view(&self, table: &str, view_sql: &str) -> Result<()>;

    /// Does `<table>__changes` already hold REAL change rows (`__pos IS NOT
    /// NULL`)? The RE-baseline refusal's condition (round-7): the truth about
    /// whether a snapshot append would lose the dedup lives in the WAREHOUSE,
    /// never in rivet's ledger — after the prescribed TRUNCATE the recovery
    /// load must sail through, and ledger rows survive a truncate. A missing
    /// `__changes` table reads `false` (the first cycle).
    fn changes_has_prior_changes(&self, table: &str) -> Result<bool>;

    /// What `table` currently is: absent, a table, a view, or another object.
    fn object_kind(&self, table: &str) -> Result<ObjectKind>;

    /// `(columns in table, how many of them are among names)`, compared case-insensitively.
    fn column_overlap(&self, table: &str, names: &[&str]) -> Result<(u64, u64)>;

    /// Row count of `table`.
    fn row_count(&self, table: &str) -> Result<u64>;

    /// Rename `table` to `<table>__changes` and add `__op` / `__pos` / `__seq` (NULL on
    /// every existing row), keeping its rows, partitioning and clustering.
    fn adopt_as_changelog(&self, table: &str) -> Result<()>;

    /// The warehouse's shape control, when it has one: reading and changing the
    /// partitioning and clustering of the tables the load writes. `None` means the
    /// driver cannot verify a shape here and says so, rather than assuming it matches.
    fn shape(&self) -> Option<&dyn ShapeControl> {
        None
    }
}

/// Reading and changing the shape of the tables a load writes (ADR-0034 D5).
pub trait ShapeControl {
    /// How the existing `table` differs from what this load would create (partitioning,
    /// clustering), or `None` when it matches.
    fn table_shape_conflict(&self, table: &str) -> Result<Option<String>>;

    /// How `<table>__changes` differs from what the load DECLARES, or `None` when it
    /// matches, is absent, or nothing is declared.
    fn changelog_drift(&self, table: &str) -> Result<Option<ChangelogDrift>>;

    /// The same question of the whole-table `<table>` an append is about to ADOPT as its
    /// change log — asked BEFORE the rename, so a refusal here changes nothing.
    fn adoption_drift(&self, table: &str) -> Result<Option<ChangelogDrift>>;

    /// Re-cluster `<table>__changes` in place to the load's `cluster_by`.
    fn recluster_changelog(&self, table: &str) -> Result<()>;

    /// Rebuild `<table>__changes` with the load's partition — a billed copy of every
    /// row — and swap it in.
    fn rebuild_changelog(&self, table: &str) -> Result<()>;

    /// Objects an interrupted rebuild of `<table>__changes` left behind.
    fn rebuild_leftovers(&self, table: &str) -> Result<Vec<String>>;
}

/// The one line a load prints when its warehouse cannot verify a table's shape.
fn shape_unverified_note(warehouse: cdc::Warehouse, fqtn: &str) -> String {
    format!(
        "  note: `{fqtn}` — {} has no shape control here, so its partitioning and clustering \
         are not compared with the config",
        warehouse.label()
    )
}

/// How an existing change log differs from what the load declares (ADR-0034 D5).
#[derive(Debug, Clone, PartialEq)]
pub enum ChangelogDrift {
    /// A different partition: only a rebuild (a billed copy of `bytes`) can change it.
    Partition {
        existing: String,
        declared: String,
        bytes: Option<u64>,
    },
    /// A different clustering: applied to the table's metadata in place.
    Cluster {
        existing: Vec<String>,
        declared: Vec<String>,
    },
}

/// Bring `<table>__changes` to the shape the load declares: re-cluster in place; rebuild
/// a changed partition only when asked for, else refuse naming the cost.
fn settle_changelog_shape(loader: &dyn TargetLoader, table: &str, rebuild: bool) -> Result<()> {
    let changes = loader.fqtn(&format!("{table}__changes"));
    let Some(shape) = loader.shape() else {
        eprintln!("{}", shape_unverified_note(loader.warehouse(), &changes));
        return Ok(());
    };
    match before_write(shape.changelog_drift(table))? {
        None => Ok(()),
        Some(ChangelogDrift::Cluster { existing, declared }) => {
            shape.recluster_changelog(table)?;
            eprintln!(
                "  note: `{changes}` now clusters on {}, was {} — new rows land clustered and \
                 the warehouse re-clusters the rest in the background",
                column_list(&declared),
                column_list(&existing)
            );
            Ok(())
        }
        Some(ChangelogDrift::Partition {
            existing,
            declared,
            bytes,
        }) => {
            if !rebuild {
                return Err(refused(rebuild_refusal(
                    &changes, &existing, &declared, bytes,
                )));
            }
            shape.rebuild_changelog(table)?;
            eprintln!("  note: `{changes}` rebuilt: partitioned by {declared}, was {existing}");
            Ok(())
        }
    }
}

/// Why adopting a whole-table load with a different partition is refused before the rename.
fn adoption_refusal(
    table: &str,
    changes: &str,
    existing: &str,
    declared: &str,
    bytes: Option<u64>,
) -> String {
    let reads = bytes.map_or_else(
        || "every row".to_string(),
        |b| format!("every row ({})", crate::pipeline::format_bytes(b)),
    );
    format!(
        "`{table}` (an earlier whole-table load) is partitioned by {existing}, the append declares \
         {declared}; adopting it as `{changes}` cannot re-partition it in place. `rivet load \
         --rebuild-changelog` adopts it and rebuilds the log with a billed query reading {reads} \
         — nothing was changed"
    )
}

/// Why a changed partition of the change log is refused: the rebuild, and what it reads.
fn rebuild_refusal(changes: &str, existing: &str, declared: &str, bytes: Option<u64>) -> String {
    let reads = bytes.map_or_else(
        || "every row".to_string(),
        |b| format!("every row ({})", crate::pipeline::format_bytes(b)),
    );
    format!(
        "`{changes}` is partitioned by {existing}, the load declares {declared}; a table cannot \
         be re-partitioned in place. `rivet load --rebuild-changelog` rebuilds it with a billed \
         query reading {reads} and swaps it in — nothing was changed"
    )
}

/// Why a load stops at the remains of an interrupted rebuild.
fn leftovers_refusal(changes: &str, leftovers: &[String]) -> String {
    format!(
        "a rebuild of `{changes}` was interrupted and left {}: keep the one holding every row \
         under the name `{changes}`, drop the other, then re-run",
        leftovers
            .iter()
            .map(|l| format!("`{l}`"))
            .collect::<Vec<_>>()
            .join(" and ")
    )
}

/// `` `a`, `b` `` or `nothing`.
pub(crate) fn column_list(cols: &[String]) -> String {
    if cols.is_empty() {
        return "nothing".to_string();
    }
    cols.iter()
        .map(|c| format!("`{c}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A load that stopped before touching the warehouse. The ledger records such a stop as
/// `refused`, which never makes the target rivet's own — a `failed` row can.
#[derive(Debug)]
pub struct Refused(anyhow::Error);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Refused {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Mark whatever went wrong before any warehouse write as a stop, not a failure.
pub(crate) fn before_write<T>(r: Result<T>) -> Result<T> {
    r.map_err(|e| {
        if e.is::<Refused>() {
            e
        } else {
            anyhow::Error::new(Refused(e))
        }
    })
}

/// A stop before any warehouse write, with its reason.
/// A whole-table pass REPLACES the base, but a buffer still holding rows from before
/// it survives the replacement — and the next `rivet compact` would merge those OLDER
/// values over the new base (a re-snapshot after a gap restores exactly the pre-gap
/// rows it existed to fix). Refused by name, nothing consumed; the operator decides
/// whether the buffer belongs to the current base (compact first) or to the past
/// (drop it).
pub(crate) fn stale_buffer_refusal(
    loader: &dyn TargetLoader,
    table: &str,
) -> Result<Option<String>> {
    let changes = format!("{table}__changes");
    if loader.object_kind(&changes)? != ObjectKind::Table {
        return Ok(None);
    }
    let rows = loader.row_count(&changes)?;
    if rows == 0 {
        return Ok(None);
    }
    let (base, buffer) = (loader.fqtn(table), loader.fqtn(&changes));
    Ok(Some(format!(
        "refusing to land a whole-table pass of `{base}`: `{buffer}` still holds {rows} change \
         row(s) from BEFORE it, and the next `rivet compact` would merge those older values \
         over the new base. If they belong to the CURRENT base, run `rivet compact` first; if \
         this pass re-snapshots past them, drop `{buffer}`. Then re-run this `rivet load` — \
         nothing was consumed"
    )))
}

pub(crate) fn refused(reason: String) -> anyhow::Error {
    anyhow::Error::new(Refused(anyhow::anyhow!(reason)))
}

/// Whether an existing warehouse table is one rivet loaded, per the load ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    Own,
    Foreign,
    /// No ledger to ask (a stateless load).
    Unknown,
}

/// Refuse to touch a table rivet did not load; without a ledger, proceed with a note.
fn ensure_own(fqtn: &str, ownership: Ownership, verb: &str) -> Result<()> {
    match ownership {
        Ownership::Own => Ok(()),
        Ownership::Foreign => bail!(
            "refusing to {verb} `{fqtn}`: it exists, and this state DB's load ledger has no \
             record of rivet loading it — it may hold someone else's data. Drop or rename it, \
             or load into another table"
        ),
        Ownership::Unknown => {
            eprintln!(
                "  note: `{fqtn}` exists and there is no load ledger to confirm rivet loaded it — \
                 proceeding on its shape alone"
            );
            Ok(())
        }
    }
}

/// What `rivet compact` may do with the base it is about to MERGE into.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CompactGate {
    Go,
    /// Proceed, saying why the check could not be made.
    Note(String),
    Refuse(String),
}

/// Whether the buffer may be merged into `base_fqtn`, from the two facts the
/// glue can cheaply fetch: what the base currently IS, and whether the ledger
/// knows rivet loaded it.
///
/// The load path checks both before it overwrites a table; compaction wrote
/// through BigQuery's own error message instead — `Not found: Table ... in
/// location US` for an absent base, and NOTHING at all for a base rivet never
/// loaded, which a MERGE would happily rewrite.
pub(crate) fn compact_gate(
    base: ObjectKind,
    ownership: Ownership,
    base_fqtn: &str,
    buffer_fqtn: &str,
) -> CompactGate {
    match (base, ownership) {
        (ObjectKind::Absent, _) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: the base table does not \
             exist. The buffer holds changes for a table that was never loaded — load the \
             backfill first (the `cdc.backfill:` export builds the base), or drop the buffer \
             to discard EVERY change buffered since the last compaction (it accumulates \
             across loads). Nothing was merged and the buffer is untouched"
        )),
        (ObjectKind::View, _) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: that name is a VIEW — the \
             current-state view of the changelog+view layout, which has no base to merge into. \
             To move to base+buffer, drop the view and `{buffer_fqtn}`; to stay on the view, \
             remove `cdc.backfill:` from the export"
        )),
        (ObjectKind::Other, _) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: it exists and is neither a \
             table nor a view"
        )),
        (ObjectKind::Table, Ownership::Foreign) => CompactGate::Refuse(format!(
            "refusing to compact `{buffer_fqtn}` into `{base_fqtn}`: it exists, and this state \
             DB's load ledger has no record of rivet loading it — a MERGE would rewrite someone \
             else's rows. Drop or rename it, or point the export at another table"
        )),
        (ObjectKind::Table, Ownership::Unknown) => CompactGate::Note(format!(
            "  note: `{base_fqtn}` exists and there is no load ledger to confirm rivet loaded it \
             — compacting on its shape alone"
        )),
        (ObjectKind::Table, Ownership::Own) => CompactGate::Go,
    }
}

/// Refuse a whole-table load onto a table that is not rivet's own, differs in shape, or is a view.
fn ensure_overwritable(loader: &dyn TargetLoader, table: &str, ownership: Ownership) -> Result<()> {
    let fqtn = loader.fqtn(table);
    match loader.object_kind(table)? {
        ObjectKind::Absent => Ok(()),
        ObjectKind::Table => {
            ensure_own(&fqtn, ownership, "overwrite")?;
            let Some(shape) = loader.shape() else {
                eprintln!("{}", shape_unverified_note(loader.warehouse(), &fqtn));
                return Ok(());
            };
            if let Some(diff) = shape.table_shape_conflict(table)? {
                bail!(
                    "refusing to overwrite `{fqtn}`: {diff}. A warehouse table cannot change \
                     its partitioning or clustering in place — drop or rename it, or align the \
                     config with it"
                );
            }
            Ok(())
        }
        ObjectKind::View => bail!(
            "refusing to overwrite `{fqtn}`: it is a view over `{fqtn}__changes`, the change log \
             of an earlier incremental or CDC load, and this run holds the whole table (a full \
             load, or an incremental run with no cursor to resume from) — a full pass cannot be \
             appended to a change log. To start over, drop the view and `{fqtn}__changes`; to \
             keep the log, resume the incremental cursor"
        ),
        ObjectKind::Other => {
            bail!("refusing to overwrite `{fqtn}`: it exists and is neither a table nor a view")
        }
    }
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
        // `*` is in the deny set because BigQuery expands ONE wildcard per
        // `uris=[...]` entry — a planted object name containing `*` would widen
        // the load past the manifest-declared file set (round-6; Snowflake's
        // FILES= is literal, but one deny set guards both).
        if let Some(bad) = u
            .chars()
            .find(|&c| c == '\'' || c == '\\' || c == '*' || c.is_control())
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
                eprintln!(
                    "warning: source cleanup failed (data is safely loaded): {}",
                    crate::redact::redact_secrets(&format!("{e:#}"))
                );
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
    ownership: Ownership,
) -> Result<LoadReport> {
    before_write(whole_table_preflight(loader, table, specs, uris, ownership))?;

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

/// Everything a whole-table load checks before it writes.
fn whole_table_preflight(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    ownership: Ownership,
) -> Result<()> {
    if uris.is_empty() {
        bail!("no Parquet URIs to load into `{table}`");
    }
    ensure_safe_load_uris(uris)?;
    validate_specs(table, specs)?;
    ensure_overwritable(loader, table, ownership)
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
    ownership: Ownership,
    rebuild_changelog: bool,
    label: &str,
    build_view: impl FnOnce(&dyn TargetLoader) -> Result<()>,
) -> Result<CdcLoadReport> {
    before_write(append_preflight(loader, table, specs, uris, pk, label))?;

    if let Some(rows) = adopt_full_load_table(loader, table, specs, ownership, rebuild_changelog)? {
        eprintln!(
            "  note: `{}` held {rows} rows from an earlier full load — it is now `{}`, the change \
             log this load appends to, and the name becomes the current-state view",
            loader.fqtn(table),
            loader.fqtn(&format!("{table}__changes"))
        );
    }
    settle_changelog_shape(loader, table, rebuild_changelog)?;

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
        target: loader.fqtn(table),
        target_kind: ChangelogTarget::View,
        source_cleaned,
    })
}

/// Everything an append checks before it writes: URIs, the dedup key, the specs, and no
/// remains of an interrupted rebuild.
fn append_preflight(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    pk: &[String],
    label: &str,
) -> Result<()> {
    if uris.is_empty() {
        bail!("no Parquet URIs to append into `{table}__changes`");
    }
    ensure_safe_load_uris(uris)?;
    if pk.is_empty() {
        bail!(
            "{label} load of `{table}` needs a primary key for the dedup view — add \
             `pk: [<column>]` under the export's `load:` block (no --pk flag exists)"
        );
    }
    // Gate the PK columns at the SHARED seam: both the CDC and the INCREMENTAL
    // driver splice them into the dedup view's `PARTITION BY` via quote_ident
    // (Snowflake emits them bare; BigQuery wraps in backticks WITHOUT escaping an
    // internal backtick), so a name outside a plain identifier is an injection
    // vector. Round-6 gated this inline in the CDC path only — the incremental
    // path (`run_load_incremental`) reached the same splice UNGATED. Hoisting it
    // here covers both, so no load driver can bypass it (the runner-bypass class).
    for c in pk {
        if !is_safe_load_ident(c) {
            bail!(
                "cannot load `{table}`: primary-key column `{}` is not a plain SQL identifier \
                 ([A-Za-z_][A-Za-z0-9_]*) — it is spliced into the dedup view's PARTITION BY. \
                 Rename or alias it in the export.",
                c.escape_default()
            );
        }
    }
    validate_specs(&format!("{table}__changes"), specs)?;
    let leftovers = match loader.shape() {
        Some(shape) => shape.rebuild_leftovers(table)?,
        None => Vec::new(),
    };
    if !leftovers.is_empty() {
        bail!(
            "{}",
            leftovers_refusal(&loader.fqtn(&format!("{table}__changes")), &leftovers)
        );
    }
    Ok(())
}

/// Turn a table an earlier whole-table load left at the view's name into `<table>__changes`
/// by renaming it; its rows become the change log's baseline. `None` when there is no such table.
///
/// A partition the load cannot re-shape in place is refused HERE, before the rename:
/// refusing after it left the serving name gone and the view unbuilt, under an error
/// that said "nothing was changed".
pub(crate) fn adopt_full_load_table(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    ownership: Ownership,
    rebuild: bool,
) -> Result<Option<u64>> {
    if !before_write(adoptable(loader, table, specs, ownership))? {
        return Ok(None);
    }
    if !rebuild
        && let Some(shape) = loader.shape()
        && let Some(ChangelogDrift::Partition {
            existing,
            declared,
            bytes,
        }) = before_write(shape.adoption_drift(table))?
    {
        return Err(refused(adoption_refusal(
            &loader.fqtn(table),
            &loader.fqtn(&format!("{table}__changes")),
            &existing,
            &declared,
            bytes,
        )));
    }
    let changes = format!("{table}__changes");
    let rows = before_write(loader.row_count(table))?;
    loader.adopt_as_changelog(table)?;
    let after = loader.row_count(&changes)?;
    if after != rows {
        bail!(
            "`{}` holds {after} rows but `{}` held {rows} before the rename — investigate before \
             re-running",
            loader.fqtn(&changes),
            loader.fqtn(table)
        );
    }
    Ok(Some(rows))
}

/// Whether `<table>` is a whole-table load this append may turn into its change log:
/// `false` when nothing is there to adopt, an error when what is there cannot be.
fn adoptable(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    ownership: Ownership,
) -> Result<bool> {
    if loader.object_kind(table)? != ObjectKind::Table {
        return Ok(false);
    }
    let changes = format!("{table}__changes");
    if loader.object_kind(&changes)? != ObjectKind::Absent {
        bail!(
            "cannot load `{}`: it is a table and `{}` already exists, so this is not an untouched \
             full load — refusing to guess which one holds the data. Keep one: drop the table, or \
             rename it aside and re-run.",
            loader.fqtn(table),
            loader.fqtn(&changes)
        );
    }
    ensure_own(&loader.fqtn(table), ownership, "turn into a change log")?;
    let names: Vec<&str> = specs
        .iter()
        .map(|s| s.column_name.as_str())
        .filter(|c| !cdc::is_meta_column(c))
        .collect();
    let (total, matched) = loader.column_overlap(table, &names)?;
    if total != matched || matched != names.len() as u64 {
        bail!(
            "cannot turn `{}` (a table from an earlier full load) into the change-log \
             baseline: it has {total} column(s), {matched} of the export's {} — the view over \
             it would not match the export. Align the export with the table, or rename the \
             table aside and re-run.",
            loader.fqtn(table),
            names.len()
        );
    }
    Ok(true)
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
    ownership: Ownership,
    rebuild_changelog: bool,
) -> Result<CdcLoadReport> {
    // PK-injection gating lives in the SHARED seam `append_and_view` (below), which
    // covers both the CDC and incremental drivers — no per-driver copy (the old
    // Round-6 inline gate here was dead once the shared one was hoisted).
    append_and_view(
        loader,
        table,
        specs,
        uris,
        pk,
        expected_delta,
        cleanup,
        ownership,
        rebuild_changelog,
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

/// Append a base-and-buffer stream's change Parquet into `<table>__changes` — the
/// per-cycle BUFFER `rivet compact` merges into the base and drops. No view, no
/// adoption of an earlier full table (the base IS a table, by design), no shape
/// settling: the buffer is created fresh each cycle from the run's own spec.
#[allow(clippy::too_many_arguments, private_interfaces)]
pub fn run_load_buffer(
    loader: &dyn TargetLoader,
    table: &str,
    specs: &[TargetColumnSpec],
    uris: &[String],
    pk: &[String],
    expected_delta: Option<u64>,
    cleanup: Option<(&GcsStore, &str)>,
) -> Result<CdcLoadReport> {
    before_write(append_preflight(loader, table, specs, uris, pk, "CDC"))?;
    let rows_appended = loader.append_changelog(table, specs, uris, pk)?;
    if let Some(expected) = expected_delta
        && rows_appended != expected
    {
        bail!(
            "CDC count validation failed for `{}__changes`: appended {rows_appended} rows, \
             expected {expected} from the run manifests — investigate before compacting",
            table
        );
    }
    let source_cleaned = maybe_cleanup(cleanup);
    Ok(CdcLoadReport {
        rows_appended,
        changes_table: loader.fqtn(&format!("{table}__changes")),
        target: loader.fqtn(table),
        target_kind: ChangelogTarget::Base,
        source_cleaned,
    })
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
    ownership: Ownership,
    rebuild_changelog: bool,
) -> Result<CdcLoadReport> {
    // uris + pk are checked by `append_and_view`; the cursor guards are incremental-only.
    before_write(cursor_preflight(table, specs, cursor_column))?;
    append_and_view(
        loader,
        table,
        specs,
        uris,
        pk,
        expected_delta,
        cleanup,
        ownership,
        rebuild_changelog,
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

/// The cursor the dedup view orders by must be named and exported: a cursor used only in
/// the extract's WHERE (or a coalesce cursor stripped from the output) is absent from
/// `__changes`, so the view would fail AFTER the append and the delta be re-appended.
fn cursor_preflight(table: &str, specs: &[TargetColumnSpec], cursor_column: &str) -> Result<()> {
    if cursor_column.is_empty() {
        bail!(
            "incremental load of `{table}` needs a cursor column (the export's `cursor_column:`) \
             for the dedup view's latest-per-PK ordering"
        );
    }
    if !specs.iter().any(|s| s.column_name == cursor_column) {
        let cols: Vec<&str> = specs.iter().map(|s| s.column_name.as_str()).collect();
        bail!(
            "incremental load of `{table}`: cursor_column `{cursor_column}` is not one of the \
             exported columns [{}] — add it to the export's SELECT so the dedup view can order \
             the change log by it",
            cols.join(", ")
        );
    }
    Ok(())
}

/// Split a `gs://bucket/path` URI into `(bucket, bucket-relative path)` — the
/// shape opendal's bucket-scoped operator wants.
pub(crate) fn split_gs_uri(uri: &str) -> Result<(&str, &str)> {
    let (bucket, key) = uri
        .strip_prefix("gs://")
        .and_then(|rest| rest.split_once('/'))
        .with_context(|| format!("not a `gs://bucket/path` URI: {uri}"))?;
    // Refuse an EMPTY bucket-relative key. It addresses the bucket ROOT, and the
    // load's recursive cleanup (delete_under → remove_all) and gc_orphans (list +
    // remove) would then wipe the ENTIRE bucket — including unrelated exports and
    // pre-existing objects — on a LEGAL config: a GCS export with no
    // `destination.prefix`, or a prefix that leads with `{partition}` so the
    // pre-`{partition}` base collapses to "". No load/cleanup lifecycle ever
    // legitimately targets the bucket root, so fail LOUD here rather than delete
    // everything. (Trailing/only slashes collapse to empty too.)
    if key.trim_matches('/').is_empty() {
        anyhow::bail!(
            "refusing a bucket-root staging prefix `{uri}`: a GCS load stages into and cleans up a \
             DEDICATED prefix, so an empty prefix would list/delete the whole bucket. Set a \
             non-empty `destination.prefix`, and put any `{{partition}}` token AFTER a literal \
             segment (e.g. `exports/{{partition}}/`, not `{{partition}}/`)."
        );
    }
    Ok((bucket, key))
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
        LoadTarget::Bigquery { project, dataset } => Box::new(
            build_bigquery_loader(
                project,
                dataset,
                plan.partition.as_ref(),
                &plan.clustering,
                run_id,
            )
            .batched_by_footers(plan.destination.clone())
            .layout(plan.layout),
        ),
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
            l.cluster_by = plan.clustering.columns().to_vec();
            l.partition_expr = plan.partition.as_ref().map(|p| p.expr.clone());
            l.run_id = Some(run_id.to_string());
            // Snowflake's external stage wants the `gcs://` scheme, not `gs://`.
            l.gcs_url = plan.gcs_prefix.replacen("gs://", "gcs://", 1);
            // The `snow` CLI does not expand `~`; pass an absolute key path.
            l.private_key_path = std::env::var("RIVET_SNOWFLAKE_KEY").ok();
            Box::new(l)
        }
    }
}

/// Wire a [`BigQueryLoader`] from a resolved plan's fields — `partition` and
/// `cluster_by` applied ONLY when set. A concrete return (not the boxed trait)
/// so the wiring is unit-testable: a mis-guarded key would silently DROP the
/// clustering/partitioning the config asked for — a degradation invisible
/// through `Box<dyn TargetLoader>`.
fn build_bigquery_loader(
    project: &str,
    dataset: &str,
    partition: Option<&plan::TablePartition>,
    clustering: &plan::Clustering,
    run_id: &str,
) -> BigQueryLoader {
    let mut l = BigQueryLoader::new(project, dataset).run_id(run_id);
    if let Some(part) = partition {
        l = l.partition(part.clone());
    }
    if !clustering.columns().is_empty() || clustering.is_written() {
        l.clustering = clustering.clone();
    }
    l
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Records every call and returns a canned row count — the seam the driver's
    /// invariants are asserted through, offline.
    #[derive(Default)]
    pub(crate) struct FakeLoader {
        rows: u64,
        materialized: RefCell<Vec<String>>,
        appended: RefCell<Vec<String>>,
        views: RefCell<Vec<String>>,
        kinds: RefCell<std::collections::HashMap<String, ObjectKind>>,
        counts: RefCell<std::collections::HashMap<String, u64>>,
        overlap: Option<(u64, u64)>,
        prior_changes: bool,
        shape_conflict: Option<String>,
        drift: RefCell<Option<ChangelogDrift>>,
        leftovers: Vec<String>,
        /// A warehouse without shape control (the Snowflake shape).
        shapeless: bool,
        calls: RefCell<Vec<String>>,
    }

    impl ShapeControl for FakeLoader {
        fn table_shape_conflict(&self, _table: &str) -> Result<Option<String>> {
            Ok(self.shape_conflict.clone())
        }
        fn changelog_drift(&self, _table: &str) -> Result<Option<ChangelogDrift>> {
            Ok(self.drift.borrow().clone())
        }
        fn adoption_drift(&self, _table: &str) -> Result<Option<ChangelogDrift>> {
            Ok(self.drift.borrow().clone())
        }
        fn recluster_changelog(&self, table: &str) -> Result<()> {
            self.calls.borrow_mut().push(format!("recluster {table}"));
            *self.drift.borrow_mut() = None;
            Ok(())
        }
        fn rebuild_changelog(&self, table: &str) -> Result<()> {
            self.calls.borrow_mut().push(format!("rebuild {table}"));
            *self.drift.borrow_mut() = None;
            Ok(())
        }
        fn rebuild_leftovers(&self, _table: &str) -> Result<Vec<String>> {
            Ok(self.leftovers.clone())
        }
    }

    impl TargetLoader for FakeLoader {
        fn shape(&self) -> Option<&dyn ShapeControl> {
            if self.shapeless { None } else { Some(self) }
        }

        fn fqtn(&self, table: &str) -> String {
            format!("db.{table}")
        }

        fn changes_has_prior_changes(&self, _table: &str) -> Result<bool> {
            Ok(self.prior_changes)
        }
        fn object_kind(&self, table: &str) -> Result<ObjectKind> {
            Ok(self
                .kinds
                .borrow()
                .get(table)
                .copied()
                .unwrap_or(ObjectKind::Absent))
        }
        fn column_overlap(&self, _table: &str, names: &[&str]) -> Result<(u64, u64)> {
            let n = names.len() as u64;
            Ok(self.overlap.unwrap_or((n, n)))
        }
        fn row_count(&self, table: &str) -> Result<u64> {
            Ok(self.counts.borrow().get(table).copied().unwrap_or(0))
        }
        fn adopt_as_changelog(&self, table: &str) -> Result<()> {
            self.calls.borrow_mut().push(format!("adopt {table}"));
            let rows = self.row_count(table)?;
            let changes = format!("{table}__changes");
            self.kinds.borrow_mut().remove(table);
            self.kinds
                .borrow_mut()
                .insert(changes.clone(), ObjectKind::Table);
            self.counts.borrow_mut().insert(changes, rows);
            Ok(())
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
            self.calls.borrow_mut().push(format!("append {table}"));
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

    fn full_load_left(rows: u64) -> FakeLoader {
        let f = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        f.kinds.borrow_mut().insert("t".into(), ObjectKind::Table);
        f.counts.borrow_mut().insert("t".into(), rows);
        f
    }

    fn load_incremental(f: &FakeLoader) -> Result<CdcLoadReport> {
        load_incremental_as(f, Ownership::Own)
    }

    fn load_incremental_as(f: &FakeLoader, ownership: Ownership) -> Result<CdcLoadReport> {
        load_incremental_with(f, ownership, false)
    }

    fn load_incremental_with(
        f: &FakeLoader,
        ownership: Ownership,
        rebuild: bool,
    ) -> Result<CdcLoadReport> {
        run_load_incremental(
            f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".to_string()],
            "id",
            Some(3),
            None,
            ownership,
            rebuild,
        )
    }

    pub(crate) fn calls(f: &FakeLoader) -> Vec<String> {
        f.calls.borrow().clone()
    }

    /// A fake for a driver-level test in a SIBLING module (`orchestrate` drives the load
    /// envelope). The fields stay private — the adapter is reached through this seam, the
    /// way a test in this module reaches it through the literal.
    pub(crate) fn fake_loader(rows: u64) -> FakeLoader {
        FakeLoader {
            rows,
            ..Default::default()
        }
    }

    fn changelog_with(drift: ChangelogDrift) -> FakeLoader {
        let f = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        f.kinds
            .borrow_mut()
            .insert("t__changes".into(), ObjectKind::Table);
        *f.drift.borrow_mut() = Some(drift);
        f
    }

    #[test]
    fn a_changed_clustering_of_the_changelog_is_applied_before_the_append() {
        let f = changelog_with(ChangelogDrift::Cluster {
            existing: vec!["id".into()],
            declared: vec!["v".into()],
        });
        load_incremental(&f).unwrap();
        assert_eq!(calls(&f), ["recluster t", "append t"]);
    }

    #[test]
    fn a_changed_partition_of_the_changelog_is_refused_until_a_rebuild_is_asked_for() {
        let drift = || ChangelogDrift::Partition {
            existing: "`ts` by day".into(),
            declared: "`ts` by month".into(),
            bytes: Some(3 * 1024 * 1024 * 1024 + 512 * 1024 * 1024),
        };
        let f = changelog_with(drift());
        let err = format!("{:#}", load_incremental(&f).unwrap_err());
        assert!(
            err.contains(
                "`db.t__changes` is partitioned by `ts` by day, the load declares `ts` by month"
            ),
            "{err}"
        );
        assert!(
            err.contains("--rebuild-changelog") && err.contains("3.5 GB"),
            "{err}"
        );
        assert!(calls(&f).is_empty(), "nothing appended: {:?}", calls(&f));
        assert!(
            load_incremental(&changelog_with(drift()))
                .unwrap_err()
                .is::<Refused>(),
            "a refusal before the write is typed"
        );

        let f = changelog_with(drift());
        load_incremental_with(&f, Ownership::Own, true).unwrap();
        assert_eq!(calls(&f), ["rebuild t", "append t"]);
    }

    /// The ledger tells a stop before the write (never makes the table rivet's own) from a
    /// failure after it (may have) by the error's type: every pre-write check of the three
    /// drivers stops as `Refused`; the count gate after the write fails plainly.
    #[test]
    fn a_stop_before_the_write_is_typed_and_a_failure_after_it_is_not() {
        let stops: Vec<(&str, anyhow::Error)> = vec![
            (
                "foreign full table",
                full_load(&full_load_left(5), Ownership::Foreign).unwrap_err(),
            ),
            (
                "shape conflict",
                full_load(
                    &FakeLoader {
                        shape_conflict: Some("it is clustered on `v`".into()),
                        ..full_load_left(5)
                    },
                    Ownership::Own,
                )
                .unwrap_err(),
            ),
            (
                "foreign table on the append path",
                load_incremental_as(&full_load_left(5), Ownership::Foreign).unwrap_err(),
            ),
            (
                "rebuild leftovers",
                load_incremental(&FakeLoader {
                    rows: 3,
                    leftovers: vec!["db.t__changes__old".into()],
                    ..Default::default()
                })
                .unwrap_err(),
            ),
            (
                "no key",
                run_load_incremental(
                    &FakeLoader::default(),
                    "t",
                    &spec(TargetStatus::Ok),
                    &uris(),
                    &[],
                    "updated_at",
                    None,
                    None,
                    Ownership::Own,
                    false,
                )
                .unwrap_err(),
            ),
        ];
        for (what, err) in &stops {
            assert!(err.is::<Refused>(), "{what} must stop as Refused: {err:#}");
        }
        let foreign = format!("{:#}", stops[0].1);
        assert!(
            foreign.contains("no record of rivet loading it"),
            "{foreign}"
        );

        let wrote = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        let err = run_load(
            &wrote,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(99),
            None,
            Ownership::Own,
        )
        .unwrap_err();
        assert_eq!(*wrote.materialized.borrow(), ["t"]);
        assert!(
            !err.is::<Refused>(),
            "the count gate fails AFTER the write: {err:#}"
        );
        assert!(format!("{err:#}").contains("count validation failed"));
    }

    #[test]
    fn an_interrupted_rebuild_is_refused_before_anything_else() {
        let f = FakeLoader {
            rows: 3,
            leftovers: vec!["db.t__changes__old".into()],
            ..Default::default()
        };
        let err = format!("{:#}", load_incremental(&f).unwrap_err());
        assert!(
            err.contains("interrupted") && err.contains("`db.t__changes__old`"),
            "{err}"
        );
        assert!(calls(&f).is_empty());
    }

    #[test]
    fn a_warehouse_without_shape_control_proceeds_and_never_reshapes() {
        let f = FakeLoader {
            rows: 3,
            shapeless: true,
            shape_conflict: Some("would conflict".into()),
            leftovers: vec!["db.t__changes__old".into()],
            ..Default::default()
        };
        *f.drift.borrow_mut() = Some(ChangelogDrift::Partition {
            existing: "`ts` by day".into(),
            declared: "`ts` by month".into(),
            bytes: None,
        });
        f.kinds.borrow_mut().insert("t".into(), ObjectKind::Table);
        f.counts.borrow_mut().insert("t".into(), 3);
        full_load(&f, Ownership::Own).unwrap();
        assert_eq!(
            *f.materialized.borrow(),
            ["t"],
            "no shape to compare, the overwrite proceeds"
        );
        let f = FakeLoader {
            rows: 3,
            shapeless: true,
            ..Default::default()
        };
        *f.drift.borrow_mut() = Some(ChangelogDrift::Cluster {
            existing: vec!["id".into()],
            declared: vec!["v".into()],
        });
        f.kinds
            .borrow_mut()
            .insert("t__changes".into(), ObjectKind::Table);
        load_incremental(&f).unwrap();
        assert_eq!(
            calls(&f),
            ["append t"],
            "no recluster or rebuild without shape control"
        );
    }

    #[test]
    fn a_full_load_table_becomes_the_changelog_before_the_first_append() {
        let f = full_load_left(5);
        load_incremental(&f).unwrap();
        assert_eq!(calls(&f), ["adopt t", "append t"]);
        assert_eq!(*f.views.borrow(), ["t"]);
        assert_eq!(f.object_kind("t").unwrap(), ObjectKind::Absent);
        assert_eq!(f.object_kind("t__changes").unwrap(), ObjectKind::Table);
    }

    /// A whole-table load whose partition the append cannot re-shape is refused
    /// BEFORE the rename, so the table keeps its name and the operator's next
    /// command starts from an unchanged warehouse. Refusing after the rename left
    /// `t` gone and no view, under an error that said "nothing was changed".
    #[test]
    fn a_partition_drift_is_refused_before_the_full_load_table_is_adopted() {
        let f = full_load_left(5);
        *f.drift.borrow_mut() = Some(ChangelogDrift::Partition {
            existing: "`ts` by day".into(),
            declared: "`ts` by month".into(),
            bytes: None,
        });
        let err = format!("{:#}", load_incremental(&f).unwrap_err());
        assert!(
            err.contains("nothing was changed") && err.contains("--rebuild-changelog"),
            "{err}"
        );
        assert!(
            calls(&f).is_empty(),
            "no rename, no append: {:?}",
            calls(&f)
        );
        assert_eq!(
            f.object_kind("t").unwrap(),
            ObjectKind::Table,
            "the table keeps its name"
        );
        assert_eq!(f.object_kind("t__changes").unwrap(), ObjectKind::Absent);

        // Asked for, the rebuild adopts and then rebuilds — the same drift, resolved.
        let f = full_load_left(5);
        *f.drift.borrow_mut() = Some(ChangelogDrift::Partition {
            existing: "`ts` by day".into(),
            declared: "`ts` by month".into(),
            bytes: None,
        });
        load_incremental_with(&f, Ownership::Own, true).unwrap();
        assert_eq!(calls(&f), ["adopt t", "rebuild t", "append t"]);
    }

    #[test]
    fn a_table_rivet_did_not_load_is_not_taken_over() {
        let f = full_load_left(5);
        let err = format!(
            "{:#}",
            load_incremental_as(&f, Ownership::Foreign).unwrap_err()
        );
        assert!(err.contains("no record of rivet loading it"), "{err}");
        assert!(calls(&f).is_empty(), "nothing renamed or appended");
    }

    #[test]
    fn without_a_ledger_the_table_is_taken_over_on_its_shape() {
        let f = full_load_left(5);
        load_incremental_as(&f, Ownership::Unknown).unwrap();
        assert_eq!(calls(&f), ["adopt t", "append t"]);
    }

    fn full_load(f: &FakeLoader, ownership: Ownership) -> Result<LoadReport> {
        run_load(
            f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(3),
            None,
            ownership,
        )
    }

    #[test]
    fn a_full_load_overwrites_its_own_table_and_refuses_the_rest() {
        let fresh = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        full_load(&fresh, Ownership::Foreign).unwrap();
        assert_eq!(
            *fresh.materialized.borrow(),
            ["t"],
            "an absent table is created"
        );

        full_load(&full_load_left(5), Ownership::Own).unwrap();
        full_load(&full_load_left(5), Ownership::Unknown).unwrap();

        let reshaped = FakeLoader {
            shape_conflict: Some("it is clustered on `v`, the config clusters on `id`".into()),
            ..full_load_left(5)
        };
        let err = format!("{:#}", full_load(&reshaped, Ownership::Own).unwrap_err());
        assert!(err.contains("clustered on `v`"), "{err}");
        assert!(reshaped.materialized.borrow().is_empty());

        let foreign = full_load_left(5);
        let err = format!("{:#}", full_load(&foreign, Ownership::Foreign).unwrap_err());
        assert!(err.contains("no record of rivet loading it"), "{err}");
        assert!(foreign.materialized.borrow().is_empty());

        let viewed = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        viewed
            .kinds
            .borrow_mut()
            .insert("t".into(), ObjectKind::View);
        let err = format!("{:#}", full_load(&viewed, Ownership::Own).unwrap_err());
        assert!(err.contains("it is a view over `db.t__changes`"), "{err}");
        assert!(viewed.materialized.borrow().is_empty());
    }

    #[test]
    fn a_full_load_table_is_adopted_on_the_cdc_path_too() {
        let f = full_load_left(5);
        run_load_cdc(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".to_string()],
            cdc::SourceEngine::MySql,
            Some(3),
            None,
            Ownership::Own,
            false,
        )
        .unwrap();
        assert_eq!(calls(&f), ["adopt t", "append t"]);
    }

    #[test]
    fn a_full_load_table_with_other_columns_is_refused_untouched() {
        let f = FakeLoader {
            overlap: Some((3, 1)),
            ..full_load_left(5)
        };
        let err = format!("{:#}", load_incremental(&f).unwrap_err());
        assert!(
            err.contains("baseline") && err.contains("3 column"),
            "{err}"
        );
        assert!(calls(&f).is_empty(), "nothing renamed or appended");
    }

    #[test]
    fn a_table_beside_an_existing_changelog_is_refused_untouched() {
        for (changes_rows, prior) in [(5, false), (9, false), (5, true)] {
            let f = FakeLoader {
                prior_changes: prior,
                ..full_load_left(5)
            };
            f.kinds
                .borrow_mut()
                .insert("t__changes".into(), ObjectKind::Table);
            f.counts
                .borrow_mut()
                .insert("t__changes".into(), changes_rows);
            let err = format!("{:#}", load_incremental(&f).unwrap_err());
            assert!(err.contains("already exists"), "{err}");
            assert!(
                calls(&f).is_empty(),
                "{changes_rows}/{prior}: nothing touched"
            );
        }
    }

    #[test]
    fn a_rename_that_loses_rows_is_reported() {
        struct ShortCopy(FakeLoader);
        impl TargetLoader for ShortCopy {
            fn fqtn(&self, t: &str) -> String {
                self.0.fqtn(t)
            }
            fn changes_has_prior_changes(&self, t: &str) -> Result<bool> {
                self.0.changes_has_prior_changes(t)
            }
            fn object_kind(&self, t: &str) -> Result<ObjectKind> {
                self.0.object_kind(t)
            }
            fn column_overlap(&self, t: &str, n: &[&str]) -> Result<(u64, u64)> {
                self.0.column_overlap(t, n)
            }
            fn row_count(&self, t: &str) -> Result<u64> {
                Ok(self.0.row_count(t)? - u64::from(t.ends_with("__changes")))
            }
            fn adopt_as_changelog(&self, t: &str) -> Result<()> {
                self.0.adopt_as_changelog(t)
            }
            fn materialize(&self, t: &str, s: &[TargetColumnSpec], u: &[String]) -> Result<u64> {
                self.0.materialize(t, s, u)
            }
            fn append_changelog(
                &self,
                t: &str,
                s: &[TargetColumnSpec],
                u: &[String],
                p: &[String],
            ) -> Result<u64> {
                self.0.append_changelog(t, s, u, p)
            }
            fn warehouse(&self) -> cdc::Warehouse {
                self.0.warehouse()
            }
            fn create_view(&self, t: &str, v: &str) -> Result<()> {
                self.0.create_view(t, v)
            }
        }
        let f = ShortCopy(full_load_left(5));
        let err = format!(
            "{:#}",
            adopt_full_load_table(&f, "t", &spec(TargetStatus::Ok), Ownership::Own, false)
                .unwrap_err()
        );
        assert!(err.contains("before the rename"), "{err}");
        assert_eq!(
            calls(&f.0),
            ["adopt t"],
            "nothing is appended after a short rename"
        );
    }

    #[test]
    fn a_view_or_an_absent_name_is_not_adopted() {
        for kind in [ObjectKind::View, ObjectKind::Absent, ObjectKind::Other] {
            let f = FakeLoader {
                rows: 3,
                ..Default::default()
            };
            f.kinds.borrow_mut().insert("t".into(), kind);
            load_incremental(&f).unwrap();
            assert_eq!(calls(&f), ["append t"], "{kind:?}");
        }
    }

    /// A base replaced while its buffer still holds rows is refused by name; an absent
    /// or empty buffer (the first cycle, or right after a compaction) lets the pass
    /// through. RED against landing the whole-table pass regardless.
    #[test]
    fn a_whole_table_pass_over_a_buffered_base_is_refused_until_the_buffer_is_dealt_with() {
        let first_cycle = FakeLoader::default();
        assert_eq!(stale_buffer_refusal(&first_cycle, "t").unwrap(), None);

        let compacted = FakeLoader {
            kinds: RefCell::new([("t__changes".to_string(), ObjectKind::Table)].into()),
            ..Default::default()
        };
        assert_eq!(stale_buffer_refusal(&compacted, "t").unwrap(), None);

        let buffered = FakeLoader {
            kinds: RefCell::new([("t__changes".to_string(), ObjectKind::Table)].into()),
            counts: RefCell::new([("t__changes".to_string(), 7)].into()),
            ..Default::default()
        };
        let why = stale_buffer_refusal(&buffered, "t")
            .unwrap()
            .expect("a buffered base refuses the pass");
        assert!(
            why.contains("`db.t__changes` still holds 7 change row(s) from BEFORE it")
                && why.contains("run `rivet compact` first")
                && why.contains("drop `db.t__changes`")
                && why.contains("nothing was consumed"),
            "{why}"
        );
    }

    #[test]
    fn object_kind_probe_codes_decode_and_unknown_codes_fail() {
        assert_eq!(ObjectKind::from_probe(0).unwrap(), ObjectKind::Absent);
        assert_eq!(ObjectKind::from_probe(1).unwrap(), ObjectKind::Table);
        assert_eq!(ObjectKind::from_probe(2).unwrap(), ObjectKind::View);
        assert_eq!(ObjectKind::from_probe(4).unwrap(), ObjectKind::Other);
        assert!(ObjectKind::from_probe(3).is_err());
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

    #[test]
    fn delete_under_and_gc_orphans_refuse_the_bucket_root_and_spare_siblings() {
        // #8 e2e against the REAL opendal fs-backed store (the load layer's offline
        // e2e seam — `delete_under`/`gc_orphans` run their real recursive delete
        // here). A bucket-ROOT prefix (the empty resolved key a no-`prefix` or
        // `{partition}`-leading GCS export + cleanup_source produces) must be
        // REFUSED, never wiped — a real `remove_all("")` destroys UNRELATED exports.
        let dir = tempfile::tempdir().unwrap();
        for rel in ["exportA/part.parquet", "innocent-neighbour/keep.parquet"] {
            let obj = dir.path().join(rel);
            std::fs::create_dir_all(obj.parent().unwrap()).unwrap();
            std::fs::write(obj, b"x").unwrap();
        }
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();

        // The destructive paths refuse the root — BEFORE touching the store.
        assert!(
            delete_under(&store, "gs://bucket/").is_err(),
            "cleanup_source must REFUSE a bucket-root prefix, not remove_all(\"\")"
        );
        assert!(
            reconcile::gc_orphans(&store, "gs://bucket/", &[], false, &Default::default()).is_err(),
            "gc_orphans must REFUSE a bucket-root prefix, not list+delete the whole bucket"
        );
        // Nothing was deleted — both independent exports survive intact.
        assert!(prefix_populated(&store, "exportA"), "exportA must survive");
        assert!(
            prefix_populated(&store, "innocent-neighbour"),
            "an unrelated neighbour export must survive the refused root cleanup"
        );

        // Contrast — the guard does NOT over-block: a REAL per-export prefix still
        // drains its own subtree and spares the neighbour.
        delete_under(&store, "gs://bucket/exportA").unwrap();
        assert!(
            !prefix_populated(&store, "exportA"),
            "a real prefix cleanup still drains its own export"
        );
        assert!(
            prefix_populated(&store, "innocent-neighbour"),
            "a scoped cleanup spares the sibling"
        );
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
        assert!(
            run_load(
                &f,
                "t",
                &spec(TargetStatus::Ok),
                &[],
                Some(10),
                None,
                Ownership::Own
            )
            .is_err()
        );
        assert!(f.materialized.borrow().is_empty());
    }

    #[test]
    fn fail_spec_bails_before_materialize() {
        let f = FakeLoader::default();
        assert!(
            run_load(
                &f,
                "t",
                &spec(TargetStatus::Fail),
                &uris(),
                Some(10),
                None,
                Ownership::Own
            )
            .is_err()
        );
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
            Ownership::Own,
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
            Ownership::Own,
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
        let r = run_load(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            Some(10),
            None,
            Ownership::Own,
        )
        .unwrap();
        assert!(!r.source_cleaned);
    }

    #[test]
    fn none_expected_skips_the_gate() {
        let f = FakeLoader {
            rows: 999,
            ..Default::default()
        };
        // No expected count → any landed rows pass (an ad-hoc load).
        assert!(
            run_load(
                &f,
                "t",
                &spec(TargetStatus::Ok),
                &uris(),
                None,
                None,
                Ownership::Own
            )
            .is_ok()
        );
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
            Ownership::Own,
            false,
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

    /// The buffer append has the same count gate as the changelog append: a short
    /// buffer fails BEFORE cleanup and before `compact` can merge a partial cycle;
    /// an exact one reports what it buffered.
    #[test]
    fn buffer_delta_mismatch_bails_and_an_exact_delta_reports() {
        let f = FakeLoader {
            rows: 3,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store_with_prefix(&dir, REL);
        let err = run_load_buffer(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".into()],
            Some(5),
            Some((&store, PREFIX)),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("CDC count validation failed"), "{err}");
        assert!(
            prefix_populated(&store, REL),
            "cleanup must not run on a failed gate"
        );

        let ok = run_load_buffer(
            &f,
            "t",
            &spec(TargetStatus::Ok),
            &uris(),
            &["id".into()],
            Some(3),
            None,
        )
        .expect("an exact delta passes the gate");
        assert_eq!(ok.rows_appended, 3);
        assert!(
            f.views.borrow().is_empty(),
            "the buffer layout builds no view"
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
            Ownership::Own,
            false,
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
        // clustering the config asked for. Reading cluster_by/partition pins
        // the wiring that `Box<dyn TargetLoader>` hides.
        let daily = plan::TablePartition {
            key: plan::PartitionKey::Time {
                column: Some("ts".into()),
                granularity: plan::Granularity::Day,
            },
            expr: "TIMESTAMP_TRUNC(ts, DAY)".into(),
            expiration_days: None,
            require_filter: false,
        };
        let written = plan::Clustering::Written(vec!["customer_id".into(), "region".into()]);
        let l = build_bigquery_loader("proj", "ds", Some(&daily), &written, "run-1");
        assert_eq!(l.cluster_by(), ["customer_id", "region"]);
        assert!(l.clustering.is_written());
        assert_eq!(l.partition.as_ref(), Some(&daily));

        // No keys set → neither clause (the default), never a spurious one.
        let auto = plan::Clustering::Auto(vec![]);
        let bare = build_bigquery_loader("proj", "ds", None, &auto, "run-1");
        assert!(bare.cluster_by().is_empty());
        assert!(!bare.clustering.is_written());
        assert!(bare.partition.is_none());
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
        // The bucket-ROOT prefix must be REFUSED, never returned as an empty key:
        // an empty key sends delete_under → remove_all("") / gc_orphans across the
        // WHOLE bucket. A GCS export with no prefix, or a `{partition}`-leading
        // prefix (base collapses to ""), resolves to exactly these — a legal config
        // that would otherwise wipe unrelated data. (RED before the empty-key guard.)
        for root in ["gs://bucket/", "gs://bucket//", "gs://bucket///"] {
            assert!(
                split_gs_uri(root).is_err(),
                "bucket-root prefix {root:?} must be refused, not parsed to an empty (root) key"
            );
        }
        // A non-empty key with a trailing slash is still a real prefix.
        assert_eq!(split_gs_uri("gs://b/exports/").unwrap(), ("b", "exports/"));
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
                None,
                Ownership::Own,
                false,
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
                run_load(
                    &f,
                    "t",
                    &spec(TargetStatus::Ok),
                    &uris,
                    Some(1),
                    None,
                    Ownership::Own
                )
                .is_err(),
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
                    Ownership::Own,
                    false,
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
            Ownership::Own,
            false,
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

    // RED before the pk gate moved into append_and_view: the CDC path gated its
    // PK columns (round-6), but the INCREMENTAL path spliced pk into the dedup
    // view's PARTITION BY (via quote_ident — bare on Snowflake, unescaped
    // backticks on BigQuery) WITHOUT the same gate. A non-identifier PK name is
    // an injection vector, and it must be refused before the driver runs.
    #[test]
    fn incremental_hostile_pk_is_refused_before_the_view_splice() {
        let f = FakeLoader::default();
        let err = run_load_incremental(
            &f,
            "t",
            &spec(TargetStatus::Ok), // exported column: `id`
            &uris(),
            &["id) OR (1=1) --".to_string()], // hostile PK spliced into PARTITION BY
            "id",                             // valid cursor (in specs) so we reach the pk gate
            None,
            None,
            Ownership::Own,
            false,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("not a plain SQL identifier") && err.contains("PARTITION BY"),
            "incremental load must refuse a non-identifier PK before splicing it into the view; got: {err}"
        );
        assert!(
            f.appended.borrow().is_empty() && f.views.borrow().is_empty(),
            "must bail before appending or building the view"
        );
    }
}

#[cfg(test)]
mod compact_gate_tests {
    use super::*;

    /// Every shape the base can be in when `rivet compact` reaches it. The two
    /// silent ones are why this exists: an ABSENT base used to surface as
    /// BigQuery's `Not found: Table`, and a FOREIGN one was not checked at all —
    /// the MERGE would have rewritten rows rivet never loaded.
    #[test]
    fn the_compact_gate_refuses_every_base_that_is_not_rivets_own_table() {
        let go = compact_gate(ObjectKind::Table, Ownership::Own, "p.d.t", "p.d.t__changes");
        assert_eq!(go, CompactGate::Go);

        let unknown = compact_gate(
            ObjectKind::Table,
            Ownership::Unknown,
            "p.d.t",
            "p.d.t__changes",
        );
        let CompactGate::Note(note) = unknown else {
            panic!("a stateless compact proceeds with a note: {unknown:?}")
        };
        assert!(note.contains("no load ledger"), "{note}");

        for (kind, ownership, wanted) in [
            (
                ObjectKind::Absent,
                Ownership::Own,
                "the base table does not exist",
            ),
            (ObjectKind::View, Ownership::Own, "that name is a VIEW"),
            (
                ObjectKind::Other,
                Ownership::Own,
                "neither a table nor a view",
            ),
            (
                ObjectKind::Table,
                Ownership::Foreign,
                "no record of rivet loading it",
            ),
        ] {
            let gate = compact_gate(kind, ownership, "p.d.t", "p.d.t__changes");
            let CompactGate::Refuse(msg) = gate else {
                panic!("{kind:?}/{ownership:?} must refuse: {gate:?}")
            };
            assert!(msg.contains(wanted), "{kind:?}/{ownership:?}: {msg}");
            assert!(
                msg.contains("`p.d.t__changes`") && msg.contains("`p.d.t`"),
                "the refusal names both tables: {msg}"
            );
        }
    }
}
