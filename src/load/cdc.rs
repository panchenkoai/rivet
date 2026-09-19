//! CDC current-state dedup view.
//!
//! rivet CDC appends a change log to `<table>__changes` (free `LOAD DATA` /
//! billed `COPY`); a **view** collapses it to current state. The collapse is one
//! `ROW_NUMBER` window that keeps the latest change per PK:
//!
//! ```sql
//! ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <total change order> DESC) = 1
//! ```
//!
//! A deleted row is kept as a **tombstone**, not dropped: the winning change's
//! `__op` becomes a boolean `__is_deleted` column, so the row survives with its
//! last-known values and an auditable delete flag — a delete is never a silent
//! disappearance. Live current state is `WHERE NOT __is_deleted`.
//!
//! The **total change order** is `(__pos, __seq)`:
//! - `__pos` is the commit position — it orders changes *across* transactions,
//!   but every change in one transaction shares it (verified live on all three
//!   engines: 8000 updates of one PK in one transaction → a single `__pos`).
//! - `__seq` (OSS `TxnSeq`) is the intra-transaction ordinal — it breaks that
//!   tie. Without it the dedup picked an arbitrary row (live: `counter = 1` for
//!   a row whose committed value was `8000`).
//!
//! `__pos` is a JSON string whose shape is per source engine, so its parse (the
//! part before `__seq`) is engine-specific — see [`SourceEngine`]. The parse
//! functions are also *warehouse*-specific ([`Warehouse`]): BigQuery reads JSON
//! with `JSON_VALUE`, Snowflake with `PARSE_JSON(...):path`.

use crate::types::target::{TargetColumnSpec, TargetStatus};

/// The source engine a change log came from — selects how `__pos` is parsed
/// into a sortable key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceEngine {
    /// `{"file":"binlog.000047","pos":10840633}` — order by file, then numeric pos.
    MySql,
    /// `{"lsn":"3D/484A4908"}` — hex `hi/lo`; zero-pad each half to fixed width
    /// so a lexical compare equals a numeric one (raw `"9" > "10"` otherwise).
    Postgres,
    /// `{"lsn":"0000002d000000d80194"}` — fixed-width hex; lexical == numeric.
    SqlServer,
    /// `{"_data":"826A4E0001..."}` — the change-stream resume token. `_data` is
    /// an order-preserving hex keystring (compared lexically, like SQL Server's
    /// lsn); the primary key is the document's `_id` column. See
    /// `source::cdc::validate::parse_pos`, which keys Mongo `__pos` on `_data`.
    Mongo,
}

/// The warehouse the view is defined in — selects the JSON-parse dialect and
/// the `SELECT * EXCEPT/EXCLUDE` keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Warehouse {
    BigQuery,
    Snowflake,
}

impl Warehouse {
    /// The warehouse's name in operator-facing notes.
    pub fn label(self) -> &'static str {
        match self {
            Warehouse::BigQuery => "BigQuery",
            Warehouse::Snowflake => "Snowflake",
        }
    }

    /// The `SELECT *`-minus-columns keyword: BigQuery spells it `EXCEPT`,
    /// Snowflake `EXCLUDE`.
    fn except_keyword(self) -> &'static str {
        match self {
            Warehouse::BigQuery => "EXCEPT",
            Warehouse::Snowflake => "EXCLUDE",
        }
    }

    /// Quote a `project.dataset.table` / `db.schema.table` identifier for this
    /// warehouse. BigQuery back-ticks the whole path; Snowflake leaves it bare
    /// (matching the unquoted identifiers the Snowflake loader creates, so a
    /// lowercase name resolves to the same upper-cased object) — a back-tick
    /// there is a syntax error.
    fn quote_fqtn(self, fqtn: &str) -> String {
        match self {
            Warehouse::BigQuery => format!("`{fqtn}`"),
            Warehouse::Snowflake => fqtn.to_string(),
        }
    }

    /// Quote a single column identifier for the view's `PARTITION BY`/`ORDER BY`.
    /// BigQuery back-ticks (case-preserving, so a reserved-word column like
    /// `order`/`end` is safe). Snowflake is left BARE — the loader creates its
    /// columns unquoted (upper-cased), and a case-sensitive `"col"` there would
    /// miss them; a reserved-word column already fails at the Snowflake `__changes`
    /// DDL, a narrower pre-existing limitation.
    fn quote_ident(self, col: &str) -> String {
        match self {
            Warehouse::BigQuery => format!("`{col}`"),
            Warehouse::Snowflake => col.to_string(),
        }
    }
}

/// What decides the WINNER when a key appears more than once in a compaction
/// buffer: a CDC stream's log position, or an incremental export's cursor.
///
/// One type so the two cannot drift: the cursor arm renders the SAME order the
/// incremental current-state view ranks by (`inc_dedup_view_sql`), including the
/// NULL-baseline guard — a first pass has no cursor bound, so its rows carry a
/// NULL cursor and must lose to any later value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactOrder {
    Cdc(SourceEngine),
    Cursor(String),
}

impl From<SourceEngine> for CompactOrder {
    fn from(e: SourceEngine) -> Self {
        CompactOrder::Cdc(e)
    }
}

impl CompactOrder {
    /// The complete `ORDER BY` clause of the winner-picking `ROW_NUMBER`.
    pub(crate) fn order_by(&self, warehouse: Warehouse) -> String {
        match self {
            CompactOrder::Cdc(engine) => engine
                .order_exprs(warehouse)
                .into_iter()
                .map(|e| format!("{e} DESC"))
                .collect::<Vec<_>>()
                .join(", "),
            CompactOrder::Cursor(column) => cursor_order_by(warehouse, column),
        }
    }
}

impl SourceEngine {
    /// The `ORDER BY` expressions (most-significant first) that totally-order
    /// the change log for this engine in `warehouse`'s SQL dialect: the parsed
    /// commit position, then `__seq`.
    pub(crate) fn order_exprs(self, warehouse: Warehouse) -> Vec<String> {
        let pos: Vec<String> = match (warehouse, self) {
            // ── BigQuery: JSON_VALUE + SPLIT(...)[OFFSET(n)] + CAST(... AS INT64)
            (Warehouse::BigQuery, SourceEngine::MySql) => vec![
                // Order by the binlog file's NUMERIC ordinal, not the raw string:
                // '…999999' sorts AFTER '…1000000' lexically, so at a rollover the
                // current-state view picked a stale row (bug hunt 2026-08-09, the
                // load-view sibling of the validate PosKey ordinal fix #169).
                "CAST(REGEXP_EXTRACT(JSON_VALUE(__pos,'$.file'), r'[0-9]+$') AS INT64)".into(),
                "CAST(JSON_VALUE(__pos,'$.pos') AS INT64)".into(),
            ],
            (Warehouse::BigQuery, SourceEngine::Postgres) => vec![
                "LPAD(SPLIT(JSON_VALUE(__pos,'$.lsn'),'/')[OFFSET(0)],8,'0')".into(),
                "LPAD(SPLIT(JSON_VALUE(__pos,'$.lsn'),'/')[OFFSET(1)],8,'0')".into(),
            ],
            (Warehouse::BigQuery, SourceEngine::SqlServer) => {
                vec!["JSON_VALUE(__pos,'$.lsn')".into()]
            }
            (Warehouse::BigQuery, SourceEngine::Mongo) => {
                vec!["JSON_VALUE(__pos,'$._data')".into()]
            }
            // ── Snowflake: PARSE_JSON(__pos):path::type + SPLIT_PART(...,n)
            (Warehouse::Snowflake, SourceEngine::MySql) => vec![
                // Numeric binlog ordinal, not the raw string (rollover — see the
                // BigQuery arm; bug hunt 2026-08-09, sibling of #169).
                "TO_NUMBER(REGEXP_SUBSTR(PARSE_JSON(__pos):file::string, '[0-9]+$'))".into(),
                "PARSE_JSON(__pos):pos::integer".into(),
            ],
            (Warehouse::Snowflake, SourceEngine::Postgres) => vec![
                "LPAD(SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',1),8,'0')".into(),
                "LPAD(SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',2),8,'0')".into(),
            ],
            (Warehouse::Snowflake, SourceEngine::SqlServer) => {
                vec!["PARSE_JSON(__pos):lsn::string".into()]
            }
            (Warehouse::Snowflake, SourceEngine::Mongo) => {
                vec!["PARSE_JSON(__pos):_data::string".into()]
            }
        };
        // `__seq` is always the final, least-significant tiebreak: it orders
        // changes that share a commit position (same transaction).
        pos.into_iter()
            .chain(std::iter::once("__seq".to_string()))
            .collect()
    }
}

/// The three CDC meta columns rivet's change log carries, typed for
/// `warehouse`. rivet CDC writes `__op` (Utf8), `__pos` (Utf8), `__seq` (Int64)
/// ahead of the after-image columns (OSS `cdc::sink`); `rivet check` reports
/// only the data columns, so the loader must prepend these to build the
/// `<table>__changes` schema.
/// The reserved CDC meta-column vocabulary — the one home for `__op/__pos/__seq`
/// so a warehouse adapter can't drift from the view builder. Both `bigquery` and
/// `snowflake` delegate here.
pub(crate) fn is_meta_column(name: &str) -> bool {
    matches!(name, "__op" | "__pos" | "__seq")
}

pub fn meta_column_specs(warehouse: Warehouse) -> Vec<TargetColumnSpec> {
    let (str_ty, int_ty) = match warehouse {
        Warehouse::BigQuery => ("STRING", "INT64"),
        Warehouse::Snowflake => ("VARCHAR", "INTEGER"),
    };
    ["__op", "__pos"]
        .into_iter()
        .map(|name| meta_spec(name, str_ty))
        .chain(std::iter::once(meta_spec("__seq", int_ty)))
        .collect()
}

fn meta_spec(name: &str, ty: &str) -> TargetColumnSpec {
    TargetColumnSpec {
        column_name: name.into(),
        target_type: ty.into(),
        autoload_type: String::new(),
        status: TargetStatus::Ok,
        note: None,
        cast_sql: None,
    }
}

/// The soft-delete flag column the view exposes: `true` when the latest change
/// for a PK was a delete. In rivet's reserved `__` namespace so it can never
/// collide with a source column (a plain `is_deleted` might).
pub const DELETE_FLAG_COLUMN: &str = "__is_deleted";

/// The base table's delete flag as a column spec, appended to the source columns
/// of a base-and-buffer load (the baseline Parquet carries it as `false`).
pub fn flag_spec(warehouse: Warehouse) -> TargetColumnSpec {
    let ty = match warehouse {
        Warehouse::BigQuery => "BOOL",
        Warehouse::Snowflake => "BOOLEAN",
    };
    meta_spec(DELETE_FLAG_COLUMN, ty)
}

/// A half-open bound on the base's partition column, as typed SQL literals, that a
/// compaction MERGE applies to BOTH sides so BigQuery prunes the base's partitions
/// (only constants prune a MERGE target).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBound {
    pub column: String,
    pub lo: String,
    pub hi_exclusive: String,
}

/// `(rows, min, max, null_count)` of the buffer's partition column, as strings —
/// the MERGE's pruning bounds come from here. `DATE(col)` normalises DATE /
/// DATETIME / TIMESTAMP to one `YYYY-MM-DD` shape; an integer range column is cast.
pub fn compact_probe_sql(
    changes_fqtn: &str,
    partition_col: Option<&str>,
    time_key: bool,
) -> String {
    match partition_col {
        None => {
            format!(
                "SELECT COUNT(*) AS n, '' AS lo, '' AS hi, 0 AS null_keys FROM `{changes_fqtn}`"
            )
        }
        Some(c) if time_key => format!(
            "SELECT COUNT(*) AS n, IFNULL(CAST(MIN(DATE(`{c}`)) AS STRING), '') AS lo, \
             IFNULL(CAST(MAX(DATE(`{c}`)) AS STRING), '') AS hi, COUNTIF(`{c}` IS NULL) AS null_keys \
             FROM `{changes_fqtn}`"
        ),
        Some(c) => format!(
            "SELECT COUNT(*) AS n, IFNULL(CAST(MIN(`{c}`) AS STRING), '') AS lo, \
             IFNULL(CAST(MAX(`{c}`) AS STRING), '') AS hi, COUNTIF(`{c}` IS NULL) AS null_keys \
             FROM `{changes_fqtn}`"
        ),
    }
}

/// Split `[lo, hi]` (inclusive dates) into half-open day windows of at most
/// `step_days` — one MERGE per window keeps each job under BigQuery's cap on the
/// partitions one statement may modify.
pub fn day_windows(
    lo: chrono::NaiveDate,
    hi: chrono::NaiveDate,
    step_days: i64,
) -> Vec<(chrono::NaiveDate, chrono::NaiveDate)> {
    let mut out = Vec::new();
    let mut start = lo;
    let end = hi + chrono::Duration::days(1);
    while start < end {
        let next = (start + chrono::Duration::days(step_days)).min(end);
        out.push((start, next));
        start = next;
    }
    out
}

/// A `date` as a literal of the partition column's BigQuery type.
pub fn time_literal(target_type: &str, date: chrono::NaiveDate) -> String {
    let d = date.format("%Y-%m-%d");
    match target_type.to_ascii_uppercase().as_str() {
        "DATE" => format!("DATE '{d}'"),
        "DATETIME" => format!("DATETIME '{d}T00:00:00'"),
        _ => format!("TIMESTAMP '{d} 00:00:00+00'"),
    }
}

/// The compaction MERGE: the latest change per key in `changes` (the buffer) is
/// upserted into `base`; a tombstone flags the base row (`__is_deleted = TRUE`,
/// values kept — the warehouse deletes nothing), a later insert un-flags it.
/// `columns` are the source columns both tables share; `bound`, when given, is
/// applied to the buffer AND to the base in `ON`, so the base's partitions prune.
/// `nulls_only` selects the buffer rows whose partition column is NULL (a
/// tombstone with a minimal before-image) — merged unpruned, on their own.
///
/// The winner per key is ranked over the WHOLE buffer and only then filtered by
/// the bound, so a key with changes on both sides of a split (a window and the
/// NULL set, or two windows) lands in exactly ONE job — the one its latest change
/// belongs to. Ranking inside the filter picked a stale winner per subset and let
/// the job order decide the row (RED: `a_key_changed_across_the_partition_split…`).
// The arity IS the statement: two tables, the shared columns, the dedup key, what
// ranks the winner, the bound, the NULL-key pass, and whether the base carries the
// delete flag. Bundling them would only move the same seven facts elsewhere.
#[allow(clippy::too_many_arguments)]
pub fn compact_merge_sql(
    base_fqtn: &str,
    changes_fqtn: &str,
    columns: &[&str],
    pk: &[&str],
    order: impl Into<CompactOrder>,
    bound: Option<&RangeBound>,
    nulls_only: Option<&str>,
    deleted_flag: bool,
) -> String {
    let filter = match (bound, nulls_only) {
        (Some(b), _) => MergeFilter::Range(b.clone()),
        (None, Some(c)) => MergeFilter::NullKeys(c.to_string()),
        (None, None) => MergeFilter::All,
    };
    compact_merge_filtered_sql(
        base_fqtn,
        changes_fqtn,
        columns,
        pk,
        &order.into(),
        &filter,
        deleted_flag,
    )
}

/// Which of the buffer's WINNERS one MERGE takes, and the matching constant
/// predicate on the base so BigQuery prunes it. `Days` names a script variable
/// (`ARRAY<DATE>`) — measured: `DATE(T.col) IN UNNEST(var)` reads only the listed
/// partitions (172 bytes against 48 KB for the MIN..MAX range on the same buffer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeFilter {
    All,
    Range(RangeBound),
    /// `DATE(column) IN UNNEST(variable)` — a day-partitioned base.
    Days {
        column: String,
        variable: String,
    },
    NullKeys(String),
}

impl MergeFilter {
    /// The predicate on a row of `alias` (no alias for the buffer's own columns).
    fn predicate(&self, alias: &str) -> String {
        let q = |c: &str| {
            if alias.is_empty() {
                format!("`{c}`")
            } else {
                format!("{alias}.`{c}`")
            }
        };
        match self {
            MergeFilter::All => String::new(),
            MergeFilter::Range(b) => format!(
                " AND {c} >= {lo} AND {c} < {hi}",
                c = q(&b.column),
                lo = b.lo,
                hi = b.hi_exclusive
            ),
            MergeFilter::Days { column, variable } => {
                format!(" AND DATE({}) IN UNNEST({variable})", q(column))
            }
            MergeFilter::NullKeys(c) => format!(" AND {} IS NULL", q(c)),
        }
    }
}

/// [`compact_merge_sql`] over a [`MergeFilter`]: the filter is applied to the
/// buffer's winners AND, for a partition filter, to the base in `ON`.
pub fn compact_merge_filtered_sql(
    base_fqtn: &str,
    changes_fqtn: &str,
    columns: &[&str],
    pk: &[&str],
    order: &CompactOrder,
    filter: &MergeFilter,
    deleted_flag: bool,
) -> String {
    let wh = Warehouse::BigQuery;
    let partition = quote_partition(wh, pk);
    let order = order.order_by(wh);
    let source_filter = filter.predicate("");
    let on_keys = pk
        .iter()
        .map(|k| format!("T.`{k}` = S.`{k}`"))
        .collect::<Vec<_>>()
        .join(" AND ");
    // NULL-keyed winners match the base by key alone: their partition is unknown.
    let on_bound = match filter {
        MergeFilter::NullKeys(_) => String::new(),
        other => other.predicate("T"),
    };
    let flag = |rendered: String| deleted_flag.then_some(rendered);
    let set = columns
        .iter()
        .map(|c| format!("`{c}` = S.`{c}`"))
        .chain(flag(format!("`{DELETE_FLAG_COLUMN}` = FALSE")))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = columns
        .iter()
        .map(|c| format!("`{c}`"))
        .chain(flag(format!("`{DELETE_FLAG_COLUMN}`")))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|c| format!("S.`{c}`"))
        .chain(flag("FALSE".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    // Without the flag column there is no tombstone to write: a query-based
    // export cannot express a delete, and naming the column would break a MERGE
    // into a base that does not have it.
    let tombstone = if deleted_flag {
        format!(
            "WHEN MATCHED AND S.__op = 'delete' THEN UPDATE SET `{DELETE_FLAG_COLUMN}` = TRUE\n"
        )
    } else {
        String::new()
    };
    format!(
        "MERGE `{base_fqtn}` AS T\n\
         USING (\n\
         \x20 SELECT * EXCEPT (__rn) FROM (\n\
         \x20   SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS __rn\n\
         \x20   FROM `{changes_fqtn}`\n\
         \x20 ) WHERE __rn = 1{source_filter}\n\
         ) AS S\n\
         ON {on_keys}{on_bound}\n\
         {tombstone}WHEN MATCHED THEN UPDATE SET {set}\n\
         WHEN NOT MATCHED AND COALESCE(S.__op, '') != 'delete' THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )
}

/// Build the current-state dedup view over a `<table>__changes` log for
/// `warehouse`. `pk` is the change log's primary key column(s); `engine`
/// selects the `__pos` parse. The view is free to define; reading it scans
/// `__changes` (billed), kept cheap by clustering the log on `pk`.
///
/// **Soft delete.** The view keeps the latest change per PK unconditionally and
/// projects the winning row's `__op` into a boolean [`DELETE_FLAG_COLUMN`]
/// (`__op = 'delete'`), rather than dropping deleted rows. A tombstone therefore
/// survives with its last-known column values — an auditable delete instead of
/// a silent disappearance. Consumers read live state with
/// `WHERE NOT __is_deleted`.
///
/// **Backfill.** `cdc.initial: snapshot` preexisting rows load from a plain
/// full-snapshot parquet, so their `__op`/`__pos` are NULL in `__changes`. The
/// flag is `COALESCE(.. , FALSE)` (a NULL `__op` is a live snapshot insert, not a
/// delete — otherwise `WHERE NOT __is_deleted` drops the whole backfill), and the
/// order ranks NULL `__pos` last (see below) so a later change beats the FIRST
/// baseline.
///
/// KNOWN LIMIT (round-6, proven on the verbatim view SQL): "later change wins"
/// is TRUE only against the first snapshot. A RE-baseline (slot-loss/binlog-purge
/// recovery re-snapshot) appends NEW NULL-`__pos` rows beside the OLD change
/// rows — and loses to all of them, so the view silently serves pre-gap values
/// for exactly the PKs the re-snapshot exists to fix (two snapshots for one PK
/// even tie nondeterministically). The recovery bail and cdc-failure-modes.md
/// therefore prescribe truncating `__changes` before the post-recovery load,
/// and the CDC load driver REFUSES when it detects the shape on a ledgered
/// load (rebaseline_action in orchestrate.rs; stateless degrades to a note).
/// The anchor stamp SHIPPED (round-10: checkpointed MySQL/MSSQL/Mongo flows
/// stamp constant `__pos`/`__seq=-1`; PG-no-checkpoint and legacy legs stay
/// NULL) — it fixes the ORDERING half only. TRUNCATE remains the remedy
/// because no snapshot can express a PK DELETED during the gap (no row, no
/// tombstone: its pre-gap rows would win), so the refusal stands for stamped
/// legs too.
///
/// The subquery + `__rn` structure (rather than a `QUALIFY`) is deliberate: the
/// flag must reflect the *winning* row per PK, computed **after** `ROW_NUMBER`.
/// Note `__op` is both dropped from the `*` expansion and referenced by the flag
/// expression — both BigQuery `EXCEPT` and Snowflake `EXCLUDE` allow that (the
/// exclusion only affects `*`, not an explicit reference).
pub fn dedup_view_sql(
    warehouse: Warehouse,
    view_fqtn: &str,
    changes_fqtn: &str,
    pk: &[&str],
    engine: SourceEngine,
) -> String {
    let partition = quote_partition(warehouse, pk);
    // `initial: snapshot` backfill rows load as a plain full-snapshot parquet —
    // no `__op`/`__pos`/`__seq` — so they land in `__changes` with those NULL.
    // `__pos IS NOT NULL DESC` FIRST in the order ranks any real change above the
    // snapshot baseline deterministically across dialects: without it BigQuery
    // sorts a NULL `__pos` last (snapshot loses — correct by luck) but Snowflake
    // sorts it first (snapshot would WIN a later update → stale current state).
    let order = std::iter::once("__pos IS NOT NULL".to_string())
        .chain(engine.order_exprs(warehouse))
        .map(|e| format!("{e} DESC"))
        .collect::<Vec<_>>()
        .join(", ");
    build_dedup_view(
        warehouse,
        view_fqtn,
        changes_fqtn,
        &partition,
        &order,
        "COALESCE(__op = 'delete', FALSE)",
    )
}

/// Quote each PK column for `warehouse` and join for a `PARTITION BY`.
fn quote_partition(warehouse: Warehouse, pk: &[&str]) -> String {
    pk.iter()
        .map(|c| warehouse.quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The shared current-state view envelope: keep the winning row per PK
/// (`ROW_NUMBER … WHERE __rn = 1`), drop the meta columns from `*`, and project
/// `delete_flag` into [`DELETE_FLAG_COLUMN`]. The two public builders differ only
/// in `order_by` (how "winning" is decided) and `delete_flag` — everything else,
/// including the `EXCEPT`/`EXCLUDE` dialect keyword and identifier quoting, lives
/// here so CDC and incremental can never drift on the view shape.
fn build_dedup_view(
    warehouse: Warehouse,
    view_fqtn: &str,
    changes_fqtn: &str,
    partition: &str,
    order_by: &str,
    delete_flag: &str,
) -> String {
    format!(
        "CREATE OR REPLACE VIEW {view} AS\n\
         SELECT * {except} (__op, __pos, __seq, __rn),\n\
         \x20      {delete_flag} AS {flag}\n\
         FROM (\n\
         \x20 SELECT *, ROW_NUMBER() OVER (\n\
         \x20   PARTITION BY {partition}\n\
         \x20   ORDER BY {order_by}\n\
         \x20 ) AS __rn\n\
         \x20 FROM {changes}\n\
         )\n\
         WHERE __rn = 1;",
        view = warehouse.quote_fqtn(view_fqtn),
        changes = warehouse.quote_fqtn(changes_fqtn),
        except = warehouse.except_keyword(),
        flag = DELETE_FLAG_COLUMN,
    )
}

/// Build the current-state dedup view for an **incremental** load's change log.
/// Unlike CDC ([`dedup_view_sql`]), an incremental delta has no `__op`/`__pos`/
/// `__seq` (the change log reuses the CDC append so those columns exist but are
/// NULL): current state is simply the row with the greatest `cursor_column` per
/// PK. Incremental can't observe deletes, so [`DELETE_FLAG_COLUMN`] is a constant
/// `FALSE` — the view SHAPE matches CDC so downstream reads `WHERE NOT
/// __is_deleted` uniformly across both modes.
/// The order an incremental key's latest row wins by: real cursor values above
/// NULL — a first pass has no cursor bound, so its rows carry a NULL cursor and
/// must lose to any later value — then newest first. Shared by the current-state
/// view and the compaction merge so the two can never rank one key differently.
fn cursor_order_by(warehouse: Warehouse, cursor_column: &str) -> String {
    let cur = warehouse.quote_ident(cursor_column);
    format!("{cur} IS NOT NULL DESC, {cur} DESC")
}

pub fn inc_dedup_view_sql(
    warehouse: Warehouse,
    view_fqtn: &str,
    changes_fqtn: &str,
    pk: &[&str],
    cursor_column: &str,
) -> String {
    let partition = quote_partition(warehouse, pk);
    // `<cursor> IS NOT NULL DESC` FIRST — the same NULL-baseline guard the CDC
    // view uses for `__pos` (see dedup_view_sql). A nullable cursor lands NULL
    // rows in `<table>__changes` (the first incremental run has no cursor bound,
    // so it extracts every row, NULL cursor included). Without the guard the
    // order is a bare `<cursor> DESC`, and Snowflake sorts NULLs FIRST in DESC —
    // so a NULL-cursor baseline row would win ROW_NUMBER=1 and HIDE a later
    // non-NULL-cursor update (stale current state; BigQuery sorts NULLs last, so
    // it was correct only by luck). Ranking real cursor values above NULL makes
    // the dedup deterministic across both dialects.
    let order = cursor_order_by(warehouse, cursor_column);
    build_dedup_view(
        warehouse,
        view_fqtn,
        changes_fqtn,
        &partition,
        &order,
        "FALSE",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const WAREHOUSES: [Warehouse; 2] = [Warehouse::BigQuery, Warehouse::Snowflake];
    const ENGINES: [SourceEngine; 4] = [
        SourceEngine::MySql,
        SourceEngine::Postgres,
        SourceEngine::SqlServer,
        SourceEngine::Mongo,
    ];

    #[test]
    fn every_warehouse_and_engine_orders_by_seq_last_and_drops_meta_columns() {
        for wh in WAREHOUSES {
            for engine in ENGINES {
                let sql = dedup_view_sql(wh, "p.d.orders", "p.d.orders__changes", &["id"], engine);
                // The intra-transaction tiebreak is present and LAST in the order.
                assert!(sql.contains("__seq DESC"), "{wh:?}/{engine:?}: {sql}");
                let order = sql.split("ORDER BY").nth(1).unwrap();
                let seq_at = order.find("__seq DESC").unwrap();
                let pos_at = order.find("__pos").unwrap();
                assert!(
                    pos_at < seq_at,
                    "{wh:?}/{engine:?}: __pos must sort before __seq"
                );
                // Soft delete: latest row per PK is kept (no delete filter); the
                // winning `__op` becomes the boolean tombstone flag.
                assert!(sql.contains("WHERE __rn = 1;"), "{wh:?}/{engine:?}: {sql}");
                assert!(
                    !sql.contains("!= 'delete'"),
                    "{wh:?}/{engine:?}: deletes must NOT be dropped"
                );
                assert!(
                    sql.contains("COALESCE(__op = 'delete', FALSE) AS __is_deleted"),
                    "{wh:?}/{engine:?}"
                );
                assert!(
                    sql.contains(&format!("PARTITION BY {}", wh.quote_ident("id"))),
                    "{wh:?}/{engine:?}"
                );
            }
        }
    }

    #[test]
    fn bigquery_uses_json_value_and_except() {
        let sql = dedup_view_sql(Warehouse::BigQuery, "v", "c", &["id"], SourceEngine::MySql);
        // numeric binlog ordinal, not the raw string (rollover fix, sibling #169)
        assert!(sql.contains(
            "CAST(REGEXP_EXTRACT(JSON_VALUE(__pos,'$.file'), r'[0-9]+$') AS INT64) DESC"
        ));
        assert!(sql.contains("CAST(JSON_VALUE(__pos,'$.pos') AS INT64) DESC"));
        assert!(sql.contains("EXCEPT (__op, __pos, __seq, __rn)"));
    }

    #[test]
    fn snowflake_uses_parse_json_and_exclude() {
        let sql = dedup_view_sql(Warehouse::Snowflake, "v", "c", &["id"], SourceEngine::MySql);
        assert!(
            sql.contains(
                "TO_NUMBER(REGEXP_SUBSTR(PARSE_JSON(__pos):file::string, '[0-9]+$')) DESC"
            )
        );
        assert!(sql.contains("PARSE_JSON(__pos):pos::integer DESC"));
        assert!(sql.contains("EXCLUDE (__op, __pos, __seq, __rn)"));
    }

    #[test]
    fn postgres_zero_pads_each_lsn_half_per_dialect() {
        let bq = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["id"],
            SourceEngine::Postgres,
        );
        assert!(bq.contains("[OFFSET(0)],8,'0')"));
        assert!(bq.contains("[OFFSET(1)],8,'0')"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "v",
            "c",
            &["id"],
            SourceEngine::Postgres,
        );
        // Snowflake splits the LSN with SPLIT_PART (1-indexed), not OFFSET.
        assert!(sf.contains("SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',1)"));
        assert!(sf.contains("SPLIT_PART(PARSE_JSON(__pos):lsn::string,'/',2)"));
    }

    #[test]
    fn sqlserver_uses_fixed_width_lsn_directly() {
        let bq = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["id"],
            SourceEngine::SqlServer,
        );
        assert!(bq.contains("JSON_VALUE(__pos,'$.lsn') DESC, __seq DESC"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "v",
            "c",
            &["id"],
            SourceEngine::SqlServer,
        );
        assert!(sf.contains("PARSE_JSON(__pos):lsn::string DESC, __seq DESC"));
    }

    #[test]
    fn mongo_orders_by_resume_token_data_and_partitions_by_id() {
        // Mongo's `_id` is the dedup PK; `__pos` orders on the `_data` resume
        // token (single string key + `__seq` tiebreak, like SQL Server's lsn).
        let bq = dedup_view_sql(Warehouse::BigQuery, "v", "c", &["_id"], SourceEngine::Mongo);
        assert!(bq.contains("JSON_VALUE(__pos,'$._data') DESC, __seq DESC"));
        assert!(bq.contains("PARTITION BY `_id`"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "v",
            "c",
            &["_id"],
            SourceEngine::Mongo,
        );
        assert!(sf.contains("PARSE_JSON(__pos):_data::string DESC, __seq DESC"));
        // Soft-delete parity holds for Mongo too.
        assert!(sf.contains("COALESCE(__op = 'delete', FALSE) AS __is_deleted"));
    }

    #[test]
    fn snapshot_backfill_rows_are_live_and_rank_oldest_on_every_dialect() {
        // `cdc.initial: snapshot` rows carry NULL __op/__pos in `__changes`. The
        // view must (1) read a NULL __op as a live insert — not a NULL flag that
        // `WHERE NOT __is_deleted` silently drops — and (2) rank a NULL __pos below
        // any real change on BOTH dialects, not rely on the engine's NULL-order
        // default (BigQuery NULLS-last vs Snowflake NULLS-first would disagree).
        for wh in WAREHOUSES {
            for engine in ENGINES {
                let sql = dedup_view_sql(wh, "p.d.t", "p.d.t__changes", &["id"], engine);
                assert!(
                    sql.contains("COALESCE(__op = 'delete', FALSE) AS __is_deleted"),
                    "{wh:?}/{engine:?}: NULL __op must read as not-deleted (live): {sql}"
                );
                // The null-rank guard is the FIRST, most-significant order key.
                let order = sql.split("ORDER BY").nth(1).unwrap();
                assert!(
                    order.contains("__pos IS NOT NULL DESC"),
                    "{wh:?}/{engine:?}: NULL __pos must be ranked, not left to engine default: {sql}"
                );
                let guard_at = order.find("__pos IS NOT NULL DESC").unwrap();
                let parse_at = order.find(if wh == Warehouse::BigQuery {
                    "JSON_VALUE"
                } else {
                    "PARSE_JSON"
                });
                if let Some(parse_at) = parse_at {
                    assert!(
                        guard_at < parse_at,
                        "{wh:?}/{engine:?}: null-rank guard must precede the __pos parse: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn identifiers_are_backticked_for_bigquery_and_bare_for_snowflake() {
        let bq = dedup_view_sql(
            Warehouse::BigQuery,
            "p.d.orders",
            "p.d.orders__changes",
            &["id"],
            SourceEngine::MySql,
        );
        assert!(bq.contains("VIEW `p.d.orders` AS"));
        assert!(bq.contains("FROM `p.d.orders__changes`"));
        let sf = dedup_view_sql(
            Warehouse::Snowflake,
            "db.sc.orders",
            "db.sc.orders__changes",
            &["id"],
            SourceEngine::MySql,
        );
        // Back-ticks would be a Snowflake syntax error — identifiers stay bare.
        assert!(!sf.contains('`'), "snowflake view must not back-tick: {sf}");
        assert!(sf.contains("VIEW db.sc.orders AS"));
        assert!(sf.contains("FROM db.sc.orders__changes"));
    }

    #[test]
    fn inc_dedup_view_orders_by_cursor_and_never_tombstones_on_every_dialect() {
        for wh in WAREHOUSES {
            let sql = inc_dedup_view_sql(
                wh,
                "p.d.orders",
                "p.d.orders__changes",
                &["id"],
                "updated_at",
            );
            assert!(
                sql.contains(&format!("PARTITION BY {}", wh.quote_ident("id"))),
                "{wh:?}: {sql}"
            );
            // Latest-per-PK is the greatest cursor value — but a NULL-cursor
            // baseline row must NOT win the dedup. Rank real cursor values above
            // NULL (the same `IS NOT NULL DESC` guard the CDC view uses for
            // __pos), else Snowflake (NULLs sort FIRST in DESC) keeps the stale
            // baseline over a later non-NULL-cursor update.
            let cur = wh.quote_ident("updated_at");
            assert!(
                sql.contains(&format!("ORDER BY {cur} IS NOT NULL DESC, {cur} DESC")),
                "{wh:?}: cursor order must guard NULL-baseline first: {sql}"
            );
            // Incremental can't observe deletes → the flag is a constant FALSE,
            // with none of CDC's `__op = 'delete'` logic.
            assert!(sql.contains("FALSE AS __is_deleted"), "{wh:?}: {sql}");
            assert!(
                !sql.contains("'delete'"),
                "{wh:?}: no CDC delete logic: {sql}"
            );
            let kw = match wh {
                Warehouse::BigQuery => "EXCEPT",
                Warehouse::Snowflake => "EXCLUDE",
            };
            assert!(
                sql.contains(&format!("{kw} (__op, __pos, __seq, __rn)")),
                "{wh:?}: drops the (reused) CDC meta columns: {sql}"
            );
        }
    }

    #[test]
    fn inc_dedup_view_quotes_identifiers_per_dialect() {
        let bq = inc_dedup_view_sql(
            Warehouse::BigQuery,
            "p.d.o",
            "p.d.o__changes",
            &["id"],
            "ts",
        );
        assert!(bq.contains("VIEW `p.d.o` AS"));
        assert!(bq.contains("FROM `p.d.o__changes`"));
        let sf = inc_dedup_view_sql(
            Warehouse::Snowflake,
            "db.sc.o",
            "db.sc.o__changes",
            &["id"],
            "ts",
        );
        assert!(!sf.contains('`'), "snowflake bare identifiers: {sf}");
        assert!(sf.contains("VIEW db.sc.o AS"));
    }

    #[test]
    fn composite_primary_key_partitions_by_all_columns() {
        let sql = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["tenant", "id"],
            SourceEngine::MySql,
        );
        assert!(sql.contains("PARTITION BY `tenant`, `id`"));
    }

    #[test]
    fn identifiers_are_quoted_per_dialect_so_a_reserved_word_column_is_safe() {
        // BigQuery back-ticks pk + cursor (a column named `order`/`end` would be a
        // syntax error unquoted); Snowflake leaves them bare (matching its
        // unquoted/upper-cased loader columns).
        let bq = inc_dedup_view_sql(Warehouse::BigQuery, "v", "c", &["order"], "end");
        assert!(bq.contains("PARTITION BY `order`"), "{bq}");
        assert!(
            bq.contains("ORDER BY `end` IS NOT NULL DESC, `end` DESC"),
            "{bq}"
        );
        let sf = inc_dedup_view_sql(Warehouse::Snowflake, "v", "c", &["order"], "end");
        assert!(sf.contains("PARTITION BY order"), "{sf}");
        assert!(
            sf.contains("ORDER BY end IS NOT NULL DESC, end DESC"),
            "{sf}"
        );
        // CDC composite pk: each column quoted for BigQuery.
        let cdc = dedup_view_sql(
            Warehouse::BigQuery,
            "v",
            "c",
            &["a", "b"],
            SourceEngine::MySql,
        );
        assert!(cdc.contains("PARTITION BY `a`, `b`"), "{cdc}");
    }

    #[test]
    fn meta_column_specs_are_typed_per_warehouse_and_ordered() {
        let bq = meta_column_specs(Warehouse::BigQuery);
        let names: Vec<&str> = bq.iter().map(|s| s.column_name.as_str()).collect();
        assert_eq!(
            names,
            ["__op", "__pos", "__seq"],
            "meta columns lead the schema, in order"
        );
        assert_eq!(bq[0].target_type, "STRING");
        assert_eq!(bq[2].target_type, "INT64");
        let sf = meta_column_specs(Warehouse::Snowflake);
        assert_eq!(sf[1].target_type, "VARCHAR");
        assert_eq!(sf[2].target_type, "INTEGER");
    }
}

/// What the compaction probe read from the buffer: its rows, the partition
/// column's MIN/MAX as text (dates for a time key, integers for a range key),
/// and how many rows carry a NULL partition column.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactProbe {
    pub rows: u64,
    pub lo: String,
    pub hi: String,
    pub nulls: u64,
}

/// What every compaction MERGE is built from: the data columns (meta columns and
/// the delete flag excluded), the key columns, and whether the base carries the
/// delete flag at all — the one fact that turns the tombstone arm on. One derivation
/// for both builders, so the filter and the flag cannot drift between them.
fn merge_inputs<'a>(
    specs: &'a [TargetColumnSpec],
    pk: &'a [String],
) -> (Vec<&'a str>, Vec<&'a str>, bool) {
    let columns = specs
        .iter()
        .map(|s| s.column_name.as_str())
        .filter(|c| !is_meta_column(c) && *c != DELETE_FLAG_COLUMN)
        .collect();
    let pk_refs = pk.iter().map(String::as_str).collect();
    let deleted_flag = specs.iter().any(|s| s.column_name == DELETE_FLAG_COLUMN);
    (columns, pk_refs, deleted_flag)
}

/// The MERGE statements one compaction runs, decided from the probe alone: none
/// for an empty buffer; one unbounded MERGE when the base has no partition key
/// (or the key is the load time); otherwise one per window of the key's range —
/// at most 4,000 partitions each, the cap on what one statement may modify —
/// plus one for the rows whose key is NULL. Pure, so the loader is glue.
pub fn plan_compact_merges(
    base: &str,
    changes_fqtn: &str,
    specs: &[TargetColumnSpec],
    pk: &[String],
    order: impl Into<CompactOrder>,
    key: Option<&crate::load::plan::PartitionKey>,
    probe: &CompactProbe,
) -> anyhow::Result<Vec<String>> {
    use crate::load::plan::{Granularity, PartitionKey};
    let order = order.into();
    if probe.rows == 0 {
        return Ok(Vec::new());
    }
    let (columns, pk_refs, deleted_flag) = merge_inputs(specs, pk);
    let merge = |bound: Option<&RangeBound>, nulls_only: Option<&str>| {
        compact_merge_sql(
            base,
            changes_fqtn,
            &columns,
            &pk_refs,
            order.clone(),
            bound,
            nulls_only,
            deleted_flag,
        )
    };
    let part_col = key.and_then(PartitionKey::column);
    let mut merges = Vec::new();
    match (key, part_col) {
        (Some(PartitionKey::Time { granularity, .. }), Some(col)) if !probe.lo.is_empty() => {
            let ty = specs
                .iter()
                .find(|s| s.column_name == col)
                .map(|s| s.target_type.as_str())
                .unwrap_or("TIMESTAMP");
            // Days per window such that no window touches more than 4,000 partitions
            // of this granularity. Finite on every arm: `chrono` panics past
            // ~10^11 days, and a sentinel here did exactly that on a monthly table.
            let step = match granularity {
                Granularity::Hour => 166,
                Granularity::Day => 4000,
                Granularity::Month => 4000 * 28,
                Granularity::Year => 4000 * 365,
            };
            let parse = |d: &str| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d");
            let (Ok(lo_d), Ok(hi_d)) = (parse(&probe.lo), parse(&probe.hi)) else {
                anyhow::bail!(
                    "compact `{base}`: cannot read the buffer's `{col}` range ({:?}..{:?})",
                    probe.lo,
                    probe.hi
                );
            };
            for (from, to) in day_windows(lo_d, hi_d, step) {
                let bound = RangeBound {
                    column: col.to_string(),
                    lo: time_literal(ty, from),
                    hi_exclusive: time_literal(ty, to),
                };
                merges.push(merge(Some(&bound), None));
            }
            if probe.nulls > 0 {
                merges.push(merge(None, Some(col)));
            }
        }
        (Some(PartitionKey::Range { interval, .. }), Some(col)) if !probe.lo.is_empty() => {
            let (Ok(lo_i), Ok(hi_i)) = (probe.lo.parse::<i64>(), probe.hi.parse::<i64>()) else {
                anyhow::bail!(
                    "compact `{base}`: cannot read the buffer's `{col}` range ({:?}..{:?})",
                    probe.lo,
                    probe.hi
                );
            };
            let step = interval.saturating_mul(4000).max(1);
            let mut from = lo_i;
            while from <= hi_i {
                let to = from.saturating_add(step);
                let bound = RangeBound {
                    column: col.to_string(),
                    lo: from.to_string(),
                    hi_exclusive: to.to_string(),
                };
                merges.push(merge(Some(&bound), None));
                from = to;
            }
            if probe.nulls > 0 {
                merges.push(merge(None, Some(col)));
            }
        }
        _ => merges.push(merge(None, None)),
    }
    Ok(merges)
}

/// Partitions one MERGE statement may touch (BigQuery's cap per DML statement).
pub const MERGE_PARTITION_CAP: usize = 4000;

/// ONE multi-statement job that compacts a base whose partition key is a DAY
/// column — or no key at all — and drops the buffer: the probe, the MERGEs and
/// the DROP are statements of one script, so a cycle costs one round trip and one
/// labelled job (its children inherit the labels). The buffer's distinct days are
/// collected into a script variable and the MERGE prunes the base with
/// `DATE(col) IN UNNEST(<variable>)` — exactly the touched partitions, in chunks of
/// [`MERGE_PARTITION_CAP`]; NULL-keyed winners merge on their own, unpruned. The
/// last statement returns `(changes_rows, merge_jobs)` for the report.
pub fn compact_script_sql(
    base_fqtn: &str,
    changes_fqtn: &str,
    specs: &[TargetColumnSpec],
    pk: &[String],
    order: impl Into<CompactOrder>,
    day_column: Option<&str>,
) -> String {
    let order = order.into();
    let (columns, pk_refs, deleted_flag) = merge_inputs(specs, pk);
    let merge = |filter: &MergeFilter| {
        compact_merge_filtered_sql(
            base_fqtn,
            changes_fqtn,
            &columns,
            &pk_refs,
            &order,
            filter,
            deleted_flag,
        )
    };
    let Some(col) = day_column else {
        return format!(
            "DECLARE n INT64 DEFAULT 0;\n\
             SET n = (SELECT COUNT(*) FROM `{changes_fqtn}`);\n\
             IF n > 0 THEN\n{merge_all}\nEND IF;\n\
             DROP TABLE `{changes_fqtn}`;\n\
             SELECT n AS changes_rows, IF(n > 0, 1, 0) AS merge_jobs;",
            merge_all = merge(&MergeFilter::All)
        );
    };
    let by_days = merge(&MergeFilter::Days {
        column: col.to_string(),
        variable: "chunk".to_string(),
    });
    let null_keys = merge(&MergeFilter::NullKeys(col.to_string()));
    format!(
        "DECLARE n INT64 DEFAULT 0;\n\
         DECLARE null_keys INT64 DEFAULT 0;\n\
         DECLARE days ARRAY<DATE> DEFAULT [];\n\
         DECLARE chunk ARRAY<DATE>;\n\
         DECLARE i INT64 DEFAULT 0;\n\
         DECLARE jobs INT64 DEFAULT 0;\n\
         SET (n, null_keys, days) = (SELECT AS STRUCT COUNT(*), COUNTIF(`{col}` IS NULL), IFNULL(ARRAY_AGG(DISTINCT DATE(`{col}`) IGNORE NULLS), []) FROM `{changes_fqtn}`);\n\
         WHILE i < ARRAY_LENGTH(days) DO\n\
         \x20 SET chunk = ARRAY(SELECT d FROM UNNEST(days) AS d WITH OFFSET AS o WHERE o >= i AND o < i + {cap});\n\
         {by_days}\n\
         \x20 SET i = i + {cap};\n\
         \x20 SET jobs = jobs + 1;\n\
         END WHILE;\n\
         IF null_keys > 0 THEN\n{null_keys_merge}\n\x20 SET jobs = jobs + 1;\nEND IF;\n\
         DROP TABLE `{changes_fqtn}`;\n\
         SELECT n AS changes_rows, jobs AS merge_jobs;",
        cap = MERGE_PARTITION_CAP,
        null_keys_merge = null_keys,
    )
}

#[cfg(test)]
mod compact_tests {
    use super::*;
    use crate::load::plan::{Granularity, PartitionKey};

    /// The day-key script: one job — probe, chunked `IN UNNEST(chunk)` MERGEs (≤ 4,000
    /// days each, the base pruned to exactly those days), the NULL-keyed pass, the
    /// DROP, and the `(changes_rows, merge_jobs)` row last. No key: one unbounded
    /// MERGE guarded by the row count, then the DROP.
    #[test]
    fn the_compaction_script_probes_merges_by_day_list_and_drops_in_one_job() {
        let s = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            Some("created_at"),
        );
        assert!(
            s.contains(
                "SET (n, null_keys, days) = (SELECT AS STRUCT COUNT(*), COUNTIF(`created_at` IS NULL), \
                 IFNULL(ARRAY_AGG(DISTINCT DATE(`created_at`) IGNORE NULLS), []) FROM `p.d.t__changes`);"
            ),
            "ONE probe statement over the buffer — every statement that reads a table is \
             billed a 10 MB floor: {s}"
        );
        assert_eq!(
            s.matches("FROM `p.d.t__changes`").count(),
            3,
            "the buffer is read by the probe and the two MERGEs, nowhere else: {s}"
        );
        assert!(s.contains("WHILE i < ARRAY_LENGTH(days) DO"), "{s}");
        assert!(s.contains("WHERE o >= i AND o < i + 4000"), "{s}");
        assert!(
            s.contains(") WHERE __rn = 1 AND DATE(`created_at`) IN UNNEST(chunk)")
                && s.contains("ON T.`id` = S.`id` AND DATE(T.`created_at`) IN UNNEST(chunk)"),
            "winners filtered by the chunk, the base pruned by the same variable: {s}"
        );
        assert!(
            s.contains("IF null_keys > 0 THEN")
                && s.contains(") WHERE __rn = 1 AND `created_at` IS NULL")
                && s.contains("ON T.`id` = S.`id`\nWHEN MATCHED AND S.__op = 'delete'"),
            "NULL-keyed winners merge by key alone: {s}"
        );
        let drop = s.find("DROP TABLE `p.d.t__changes`;").expect("the drop");
        let last_merge = s.rfind("MERGE `p.d.t`").expect("a merge");
        assert!(last_merge < drop, "every MERGE precedes the DROP: {s}");
        assert!(
            s.trim_end()
                .ends_with("SELECT n AS changes_rows, jobs AS merge_jobs;"),
            "{s}"
        );

        let plain = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            None,
        );
        assert!(
            plain.contains("IF n > 0 THEN\nMERGE `p.d.t` AS T"),
            "{plain}"
        );
        assert!(plain.contains("ON T.`id` = S.`id`\n"), "unbounded: {plain}");
        assert!(!plain.contains("UNNEST"), "{plain}");
        assert!(
            plain.ends_with("SELECT n AS changes_rows, IF(n > 0, 1, 0) AS merge_jobs;"),
            "{plain}"
        );

        // Hostile names: a reserved-word key and a dashed table id are quoted
        // everywhere they appear — the ON clause, the window, the buffer filter.
        let hostile = compact_script_sql(
            "p.d.my-orders",
            "p.d.my-orders__changes",
            &[meta_spec("order", "INT64"), meta_spec("created_at", "DATE")],
            &["order".to_string()],
            SourceEngine::Postgres,
            Some("created_at"),
        );
        assert!(hostile.contains("MERGE `p.d.my-orders` AS T"), "{hostile}");
        assert!(
            hostile.contains("PARTITION BY `order` ORDER BY"),
            "{hostile}"
        );
        assert!(
            hostile.contains("ON T.`order` = S.`order` AND DATE(T.`created_at`)"),
            "{hostile}"
        );
        assert!(
            hostile.contains("DROP TABLE `p.d.my-orders__changes`;"),
            "{hostile}"
        );
        assert!(
            !hostile.contains(" order "),
            "never a bare reserved word: {hostile}"
        );
    }

    fn probe(rows: u64, lo: &str, hi: &str, nulls: u64) -> CompactProbe {
        CompactProbe {
            rows,
            lo: lo.into(),
            hi: hi.into(),
            nulls,
        }
    }

    fn specs() -> Vec<TargetColumnSpec> {
        vec![
            meta_spec("id", "INT64"),
            meta_spec("v", "INT64"),
            meta_spec("created_at", "DATETIME"),
            meta_spec("__is_deleted", "BOOL"),
        ]
    }

    fn plan(key: Option<&PartitionKey>, p: &CompactProbe) -> Vec<String> {
        plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            key,
            p,
        )
        .expect("a plan")
    }

    /// An empty buffer plans nothing; no key plans one unbounded MERGE; a day
    /// key over 5,000 days plans two windows (+ one for the NULL rows when there
    /// are any); a range key steps by interval × 4,000; the flag column never
    /// appears among the merged columns.
    #[test]
    fn the_compaction_plan_follows_the_probe_and_the_key() {
        let day = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Day,
        };
        assert!(
            plan(Some(&day), &probe(0, "", "", 0)).is_empty(),
            "nothing to merge"
        );
        let all_null_keys = plan(Some(&day), &probe(3, "", "", 3));
        assert_eq!(
            all_null_keys.len(),
            1,
            "a key with no range (every row's key NULL) merges once, unbounded"
        );
        assert!(
            all_null_keys[0].contains("ON T.`id` = S.`id`\n"),
            "{}",
            all_null_keys[0]
        );
        let hour = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Hour,
        };
        assert_eq!(
            plan(Some(&hour), &probe(4, "2024-01-01", "2024-07-18", 0)).len(),
            2,
            "hourly partitions: 4,000 of them are 166 days, so 200 days take two windows"
        );
        let month = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Month,
        };
        assert_eq!(
            plan(Some(&month), &probe(4, "1990-01-01", "2026-09-17", 0)).len(),
            1,
            "monthly partitions never reach the cap: one window"
        );

        let none = plan(None, &probe(7, "", "", 0));
        assert_eq!(none.len(), 1);
        assert!(
            none[0].contains("ON T.`id` = S.`id`\n"),
            "unbounded: {}",
            none[0]
        );
        assert!(
            none[0].contains("SET `id` = S.`id`, `v` = S.`v`, `created_at` = S.`created_at`, `__is_deleted` = FALSE"),
            "the flag is set, never copied from the buffer: {}",
            none[0]
        );

        let two = plan(Some(&day), &probe(9, "2000-01-01", "2013-09-08", 0));
        assert_eq!(two.len(), 2, "5,000 days → two windows of ≤ 4,000");
        assert!(
            two[0].contains("T.`created_at` >= DATETIME '2000-01-01T00:00:00' AND T.`created_at` < DATETIME '2010-12-14T00:00:00'"),
            "{}",
            two[0]
        );
        assert!(
            two[1].contains(">= DATETIME '2010-12-14T00:00:00'"),
            "{}",
            two[1]
        );

        let with_nulls = plan(Some(&day), &probe(9, "2024-01-01", "2024-01-02", 3));
        assert_eq!(with_nulls.len(), 2, "one window + the NULL set");
        assert!(
            with_nulls[1].contains("WHERE __rn = 1 AND `created_at` IS NULL"),
            "{}",
            with_nulls[1]
        );

        let range = PartitionKey::Range {
            column: "bucket".into(),
            start: 0,
            end: 1_000_000,
            interval: 10,
        };
        let stepped = plan(Some(&range), &probe(5, "0", "45000", 1));
        assert_eq!(
            stepped.len(),
            3,
            "45,001 keys at 10 × 4,000 per job → two windows + NULLs"
        );
        assert!(
            stepped[0].contains("`bucket` >= 0 AND `bucket` < 40000"),
            "{}",
            stepped[0]
        );
        assert!(
            stepped[1].contains("`bucket` >= 40000 AND `bucket` < 80000"),
            "{}",
            stepped[1]
        );

        // A monthly key steps 4,000 × 28 days: 400 years span two windows, not one
        // and not hundreds.
        let long_month = plan(Some(&month), &probe(4, "1600-01-01", "2000-01-01", 0));
        assert_eq!(long_month.len(), 2, "146,000 days at 112,000 per window");
        // A range key without NULL keys plans no NULL pass; one whose every key is
        // NULL (no range to read) merges once, unbounded.
        assert_eq!(plan(Some(&range), &probe(5, "0", "45000", 0)).len(), 2);
        let range_nulls = plan(Some(&range), &probe(2, "", "", 2));
        assert_eq!(range_nulls.len(), 1);
        assert!(
            range_nulls[0].contains("ON T.`id` = S.`id`\n"),
            "{}",
            range_nulls[0]
        );

        let err = plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs(),
            &["id".to_string()],
            SourceEngine::MySql,
            Some(&day),
            &probe(1, "not-a-date", "x", 0),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("cannot read the buffer's `created_at` range"),
            "{err}"
        );
    }

    fn bound(lo: &str, hi: &str) -> RangeBound {
        RangeBound {
            column: "created_at".into(),
            lo: lo.into(),
            hi_exclusive: hi.into(),
        }
    }

    /// The MERGE upserts the latest change per key, flags a tombstone instead of
    /// deleting, un-flags a re-insert, never inserts a delete — and prunes BOTH sides
    /// by the same constant bound, since only constants prune a MERGE target.
    #[test]
    fn compact_merge_flags_deletes_and_prunes_both_sides_by_constants() {
        let sql = compact_merge_sql(
            "p.d.orders",
            "p.d.orders__changes",
            &["id", "v", "created_at"],
            &["id"],
            SourceEngine::MySql,
            Some(&bound("DATE '2000-01-01'", "DATE '2010-12-14'")),
            None,
            true,
        );
        assert!(sql.starts_with("MERGE `p.d.orders` AS T"), "{sql}");
        assert!(sql.contains("PARTITION BY `id` ORDER BY"), "{sql}");
        assert!(
            sql.contains("FROM `p.d.orders__changes`\n  ) WHERE __rn = 1 AND `created_at` >= DATE '2000-01-01' AND `created_at` < DATE '2010-12-14'"),
            "the buffer side is bounded AFTER ranking, on the winner: {sql}"
        );
        assert!(
            sql.contains("ON T.`id` = S.`id` AND T.`created_at` >= DATE '2000-01-01' AND T.`created_at` < DATE '2010-12-14'"),
            "the base side is bounded by the same constants: {sql}"
        );
        assert!(
            sql.contains(
                "WHEN MATCHED AND S.__op = 'delete' THEN UPDATE SET `__is_deleted` = TRUE"
            ),
            "a tombstone flags, never deletes: {sql}"
        );
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET `id` = S.`id`, `v` = S.`v`, `created_at` = S.`created_at`, `__is_deleted` = FALSE"),
            "an update refreshes the values and un-flags: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED AND COALESCE(S.__op, '') != 'delete' THEN INSERT (`id`, `v`, `created_at`, `__is_deleted`) VALUES (S.`id`, S.`v`, S.`created_at`, FALSE)"),
            "an insert lands live; a delete of an unknown key inserts nothing: {sql}"
        );
        assert!(
            !sql.contains("THEN DELETE"),
            "the warehouse deletes nothing: {sql}"
        );
        // MySQL orders by the binlog file's ordinal then position, `__seq` last.
        assert!(
            sql.contains("JSON_VALUE(__pos,'$.file')") && sql.contains("__seq DESC"),
            "{sql}"
        );
    }

    /// Rows whose partition column is NULL (a tombstone with a minimal before-image)
    /// merge on their own, unpruned, so a bounded pass cannot skip them.
    #[test]
    fn compact_merge_of_null_partition_rows_is_unbounded() {
        let sql = compact_merge_sql(
            "p.d.t",
            "p.d.t__changes",
            &["id"],
            &["id"],
            SourceEngine::Postgres,
            None,
            Some("created_at"),
            true,
        );
        assert!(
            sql.contains("WHERE __rn = 1 AND `created_at` IS NULL"),
            "the NULL set is chosen among the WINNERS, so a key whose latest change is \
             dated never merges here as well: {sql}"
        );
        assert!(
            sql.contains("ON T.`id` = S.`id`\n"),
            "no bound on the base: {sql}"
        );
    }

    /// Windows of at most `step` days cover `[lo, hi]` exactly once each — 5,000
    /// days at BigQuery's 4,000-partition cap is two MERGEs, not one refusal.
    #[test]
    fn day_windows_cover_the_span_once_in_cap_sized_steps() {
        let lo = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let hi = lo + chrono::Duration::days(4999);
        let w = day_windows(lo, hi, 4000);
        assert_eq!(w.len(), 2);
        assert_eq!(w[0].0, lo);
        assert_eq!(w[0].1, lo + chrono::Duration::days(4000));
        assert_eq!(w[1].0, w[0].1, "windows abut");
        assert_eq!(
            w[1].1,
            hi + chrono::Duration::days(1),
            "the last is half-open past hi"
        );
        assert_eq!(
            day_windows(lo, lo, 4000),
            vec![(lo, lo + chrono::Duration::days(1))]
        );
    }

    #[test]
    fn time_literals_follow_the_columns_type() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 9, 17).unwrap();
        assert_eq!(time_literal("DATE", d), "DATE '2026-09-17'");
        assert_eq!(
            time_literal("DATETIME", d),
            "DATETIME '2026-09-17T00:00:00'"
        );
        assert_eq!(
            time_literal("TIMESTAMP", d),
            "TIMESTAMP '2026-09-17 00:00:00+00'"
        );
    }

    #[test]
    fn the_probe_normalises_time_keys_to_dates_and_reads_ranges_as_is() {
        let t = compact_probe_sql("p.d.t__changes", Some("created_at"), true);
        assert!(
            t.contains("MIN(DATE(`created_at`))") && t.contains("COUNTIF(`created_at` IS NULL)"),
            "{t}"
        );
        let r = compact_probe_sql("p.d.t__changes", Some("bucket"), false);
        assert!(r.contains("MIN(`bucket`)") && !r.contains("DATE("), "{r}");
        let n = compact_probe_sql("p.d.t__changes", None, false);
        assert!(
            n.contains("'' AS lo") && n.contains("0 AS null_keys"),
            "{n}"
        );
    }
}

#[cfg(test)]
mod compact_column_tests {
    use super::*;
    use crate::load::plan::{Granularity, PartitionKey};

    fn probe(rows: u64, lo: &str, hi: &str, nulls: u64) -> CompactProbe {
        CompactProbe {
            rows,
            lo: lo.into(),
            hi: hi.into(),
            nulls,
        }
    }

    /// A YEAR key steps 4,000 x 365 days, so four centuries are ONE window. RED
    /// against `4000 + 365` (34 windows) and `4000 / 365` (13,000 of them) — the
    /// arm the day/hour/month cases never reach.
    #[test]
    fn a_yearly_key_plans_one_window_for_four_centuries() {
        let year = PartitionKey::Time {
            column: Some("created_at".into()),
            granularity: Granularity::Year,
        };
        let specs = vec![
            meta_spec("id", "INT64"),
            meta_spec("created_at", "DATETIME"),
            flag_spec(Warehouse::BigQuery),
        ];
        let plans = plan_compact_merges(
            "p.d.t",
            "p.d.t__changes",
            &specs,
            &["id".to_string()],
            SourceEngine::MySql,
            Some(&year),
            &probe(4, "1600-01-01", "2000-01-01", 0),
        )
        .expect("a plan");
        assert_eq!(plans.len(), 1, "146,098 days at 1,460,000 per window");
        assert!(
            plans[0].contains("T.`created_at` >= DATETIME '1600-01-01T00:00:00'")
                && plans[0].contains("T.`created_at` < DATETIME '2000-01-02T00:00:00'"),
            "the one window covers the whole span: {}",
            plans[0]
        );
    }

    /// The scripted MERGE carries the DATA columns only. The buffer's `__op` /
    /// `__pos` / `__seq` are its own bookkeeping, and `__is_deleted` is DECIDED by
    /// the merge, never copied from a buffer row that has no such column.
    #[test]
    fn the_compaction_script_merges_data_columns_only() {
        let mut specs = meta_column_specs(Warehouse::BigQuery);
        specs.push(meta_spec("id", "INT64"));
        specs.push(meta_spec("v", "INT64"));
        specs.push(flag_spec(Warehouse::BigQuery));
        let s = compact_script_sql(
            "p.d.t",
            "p.d.t__changes",
            &specs,
            &["id".to_string()],
            SourceEngine::MySql,
            None,
        );
        assert!(
            s.contains("SET `id` = S.`id`, `v` = S.`v`, `__is_deleted` = FALSE"),
            "data columns, then the flag the merge sets: {s}"
        );
        assert!(
            s.contains("INSERT (`id`, `v`, `__is_deleted`) VALUES (S.`id`, S.`v`, FALSE)"),
            "{s}"
        );
        for meta in ["__op", "__pos", "__seq"] {
            assert!(
                !s.contains(&format!("`{meta}` = S.`{meta}`")),
                "the buffer's {meta} must not land in the base: {s}"
            );
        }
        assert!(
            !s.contains("`__is_deleted` = S.`__is_deleted`"),
            "the flag is the merge's decision, not a copied column: {s}"
        );
    }
}

#[cfg(test)]
mod compact_order_tests {
    use super::*;

    /// The compaction and the current-state view must rank ONE key the same way,
    /// or a compacted base and a view over the same rows disagree about which is
    /// latest. The cursor arm renders the view's own order, NULL guard included.
    #[test]
    fn the_cursor_order_is_the_one_the_incremental_view_ranks_by() {
        let order = CompactOrder::Cursor("updated_at".into()).order_by(Warehouse::BigQuery);
        assert_eq!(order, "`updated_at` IS NOT NULL DESC, `updated_at` DESC");
        let view = inc_dedup_view_sql(
            Warehouse::BigQuery,
            "p.d.t",
            "p.d.t__changes",
            &["id"],
            "updated_at",
        );
        assert!(
            view.contains(&order),
            "the view must rank by the same order: {view}"
        );
        let cdc = CompactOrder::Cdc(SourceEngine::MySql).order_by(Warehouse::BigQuery);
        assert!(
            cdc.contains("__pos") && cdc.ends_with("DESC"),
            "a stream ranks by its log position: {cdc}"
        );
        assert_eq!(
            CompactOrder::from(SourceEngine::MySql).order_by(Warehouse::BigQuery),
            cdc
        );
    }
}

#[cfg(test)]
mod compact_flag_tests {
    use super::*;

    /// A query-based export cannot express a DELETE, so its base need not carry
    /// `__is_deleted` — an extra column per row on the warehouse side. With the
    /// column absent the MERGE must not name it ANYWHERE: not in the SET list, not
    /// among the inserted columns, and not as a tombstone arm, or it would fail
    /// against a base that does not have it.
    #[test]
    fn a_base_without_the_delete_flag_merges_without_naming_it() {
        let data = vec![meta_spec("id", "INT64"), meta_spec("v", "INT64")];
        let mut flagged = data.clone();
        flagged.push(flag_spec(Warehouse::BigQuery));
        let build = |specs: &[TargetColumnSpec]| {
            compact_script_sql(
                "p.d.t",
                "p.d.t__changes",
                specs,
                &["id".to_string()],
                SourceEngine::MySql,
                None,
            )
        };

        let with = build(&flagged);
        assert!(
            with.contains("`__is_deleted` = FALSE") && with.contains("`__is_deleted` = TRUE"),
            "a stream's base keeps the flag and its tombstone arm: {with}"
        );

        let without = build(&data);
        assert!(
            !without.contains("__is_deleted"),
            "the column is named nowhere when the base has none: {without}"
        );
        assert!(
            !without.contains("WHEN MATCHED AND S.__op = 'delete'"),
            "no flag, no tombstone to write: {without}"
        );
        assert!(
            without.contains("WHEN MATCHED THEN UPDATE SET `id` = S.`id`, `v` = S.`v`\n")
                && without.contains("INSERT (`id`, `v`) VALUES (S.`id`, S.`v`)"),
            "the data columns still upsert: {without}"
        );
    }
}
