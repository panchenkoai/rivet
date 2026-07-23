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

impl SourceEngine {
    /// The `ORDER BY` expressions (most-significant first) that totally-order
    /// the change log for this engine in `warehouse`'s SQL dialect: the parsed
    /// commit position, then `__seq`.
    fn order_exprs(self, warehouse: Warehouse) -> Vec<String> {
        let pos: Vec<String> = match (warehouse, self) {
            // ── BigQuery: JSON_VALUE + SPLIT(...)[OFFSET(n)] + CAST(... AS INT64)
            (Warehouse::BigQuery, SourceEngine::MySql) => vec![
                "JSON_VALUE(__pos,'$.file')".into(),
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
                "PARSE_JSON(__pos):file::string".into(),
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
/// order ranks NULL `__pos` last (see below) so a later change always wins.
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
    let cur = warehouse.quote_ident(cursor_column);
    let order = format!("{cur} IS NOT NULL DESC, {cur} DESC");
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
        assert!(sql.contains("JSON_VALUE(__pos,'$.file') DESC"));
        assert!(sql.contains("CAST(JSON_VALUE(__pos,'$.pos') AS INT64) DESC"));
        assert!(sql.contains("EXCEPT (__op, __pos, __seq, __rn)"));
    }

    #[test]
    fn snowflake_uses_parse_json_and_exclude() {
        let sql = dedup_view_sql(Warehouse::Snowflake, "v", "c", &["id"], SourceEngine::MySql);
        assert!(sql.contains("PARSE_JSON(__pos):file::string DESC"));
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
