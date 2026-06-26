//! SQL Server preflight diagnostics for `rivet check` / `rivet plan` (parity
//! with [`super::postgres`] / [`super::mysql`]).
//!
//! The shape mirrors the MySQL module — connect once, build one
//! [`ExportDiagnostic`] per export, reuse every engine-neutral analysis helper
//! in [`super::analysis`] — but the probes the diagnostic *can* run are narrower
//! because the source seam ([`MssqlSource`]) exposes only
//! [`query_scalar`](crate::source::Source::query_scalar) (one scalar cell), and
//! SQL Server has no portable `EXPLAIN` that returns a parseable row estimate
//! over that seam.
//!
//! What is probed (all through `query_scalar`, the same seam `init` uses):
//! - **row estimate** — `SUM(row_count)` from `sys.dm_db_partition_stats`
//!   (heap/clustered index, `index_id IN (0,1)`), the exact SQL `rivet init`
//!   and `introspect_mssql_table_for_chunking` already run; `None` when the base
//!   query is not a simple single-table read (joins, subqueries, inline SQL).
//! - **cursor / range min-max** — `MIN`/`MAX` of the cursor/chunk column for
//!   incremental/chunked modes, surfaced as the `Cursor range` line.
//! - **`uses_index`** — an honest catalog probe: is the range/cursor column the
//!   single leading key of *some* index on the base table? No query-plan
//!   inspection is fabricated, so `scan_type` is left `None` (the printer simply
//!   omits the `Scan type` line) and the verdict rests on the catalog fact.
//!
//! Everything downstream (strategy, profile, parallelism, verdict, warnings,
//! suggestion) is the shared `analysis` surface PG/MySQL feed, so MSSQL gets the
//! same advice.

use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use super::schema_error::PreflightSchemaError;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::error::Result;
use crate::source::Source;
use crate::source::mssql::MssqlSource;

pub(super) fn check_mssql(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
    silent: bool,
) -> Result<()> {
    let mut conn = MssqlSource::connect_with_tls(url, tls)?;

    for export in exports {
        let diag = diagnose_mssql(&mut conn, export)?;
        if !silent {
            super::print_diagnostic(&diag);
        }
    }

    Ok(())
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_mssql(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<super::ExportDiagnostic> {
    let mut conn = MssqlSource::connect_with_tls(url, tls)?;
    diagnose_mssql(&mut conn, export)
}

fn diagnose_mssql(conn: &mut MssqlSource, export: &ExportConfig) -> Result<ExportDiagnostic> {
    let mode_str = diagnose_mode_str(export);

    let base_query = resolve_preflight_base_query(export);
    let base_query = base_query.as_str();

    // A missing table/column (or no SELECT grant) is permanent and
    // author-fixable — fail preflight loudly instead of letting it surface only
    // at run time. The catalog-based row-estimate below never touches the table,
    // so this zero-row probe is the only check that validates the query's actual
    // relations. Operational errors stay fail-soft (`None`).
    if let Some(fail) = schema_fail_mssql(conn, base_query) {
        return Err(fail);
    }

    let range_col = export
        .chunk_column
        .as_deref()
        .or(export.cursor_column.as_deref());

    // Recover the base relation (`[schema.]table`) the probes key on. `init`
    // emits `query: SELECT cols FROM <table>`, and the `table:` shortcut emits
    // `SELECT * FROM <table>` — both resolve to a single relation here. Anything
    // the parser can't reduce to one base table (joins, subqueries, hand-written
    // inline SQL) yields `None`, and the row-estimate / index probes degrade to
    // an honest "unknown" rather than guessing the wrong relation.
    let base_table =
        strip_select_star_from(base_query).or_else(|| table_from_simple_query(base_query));

    // Row estimate from `sys.dm_db_partition_stats` — the same fast,
    // no-`COUNT(*)` probe `rivet init` and `introspect_mssql_table_for_chunking`
    // run. `None` (printer omits the line) when the base relation is unknown or
    // the stats row is absent.
    let row_estimate = match base_table {
        Some(table) => row_estimate_mssql(conn, table),
        None => None,
    };

    // Average bytes/row from the same `dm_db_partition_stats` DMV — feeds the
    // oversized-chunk warning. `None` when the base relation is unknown.
    let avg_row_bytes = base_table.and_then(|table| avg_row_bytes_mssql(conn, table));

    // Cursor / chunk range. Incremental mode orders on the (possibly COALESCE'd)
    // key expression; chunked/cursor modes take MIN/MAX of the range column.
    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        match incremental_key_expr(export, SourceType::Mssql) {
            Some(expr) => range_min_max_mssql(conn, base_query, base_table, &expr),
            None => (None, None),
        }
    } else if let Some(col) = range_col {
        let expr = crate::sql::quote_ident(SourceType::Mssql, col);
        range_min_max_mssql(conn, base_query, base_table, &expr)
    } else {
        (None, None)
    };

    // Honest index signal: no query plan is parsed (SQL Server has no portable
    // `EXPLAIN` over the scalar seam), so `scan_type` stays `None`. For
    // chunked/incremental modes the catalog *can* answer the question that
    // actually matters for the run (`WHERE range_col > $last` / `BETWEEN`): is
    // the range column the single leading key of some index? That fact alone
    // drives the verdict, the same way the PG/MySQL catalog probe overrides
    // their EXPLAIN hint.
    let scan_type = None;
    let uses_index = if matches!(export.mode, ExportMode::Chunked | ExportMode::Incremental)
        && let Some(col) = range_col
        && let Some(table) = base_table
    {
        column_has_index_mssql(conn, table, col).unwrap_or(false)
    } else {
        false
    };

    let strategy = derive_strategy(export);
    let verdict = compute_verdict(
        row_estimate,
        uses_index,
        export.cursor_column.is_some(),
        avg_row_bytes,
        export.parallel,
    );
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    // SQL Server has no portable per-user `max_connections` server variable
    // readable over this seam (the cap is per-edition / Resource Governor), so
    // the connection-limit warning is skipped — `None` makes `collect_warnings`
    // emit the "check skipped" note only when parallel > 1, exactly like the
    // PG/MySQL path when that probe fails.
    let warnings = collect_warnings(
        export,
        row_estimate,
        avg_row_bytes,
        range_min.as_deref(),
        range_max.as_deref(),
        None,
    );
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        strategy,
        mode: mode_str,
        cursor_column: export.cursor_column.clone(),
        row_estimate,
        avg_row_bytes,
        cursor_min: range_min,
        cursor_max: range_max,
        scan_type,
        uses_index,
        verdict,
        recommended_profile,
        recommended_parallel,
        warnings,
        suggestion,
    })
}

/// Row estimate from `sys.dm_db_partition_stats` for `[schema.]table`. Mirrors
/// the SQL `rivet init` (`src/init/mssql.rs`) and
/// `introspect_mssql_table_for_chunking` already run — rows in the heap /
/// clustered index (`index_id IN (0,1)`), no `COUNT(*)` scan. `None` when the
/// stats row is absent (view, no stats) or the probe fails; preflight is
/// non-fatal, so a failure is logged at debug, never aborted.
/// Validate the query's relations with a zero-row wrap; map an author-fixable
/// failure (Invalid object name = 208, Invalid column name = 207) to a loud
/// preflight error. `None` (fail-soft) for success and operational errors.
fn schema_fail_mssql(conn: &mut MssqlSource, base_query: &str) -> Option<anyhow::Error> {
    // `TOP 0` over a derived table executes the relations without scanning rows.
    let probe = format!("SELECT TOP 0 1 AS _ok FROM ({base_query}) AS _rivet_probe");
    let Err(e) = conn.query_scalar(&probe) else {
        return None;
    };
    let m = format!("{e:#}");
    let detail = if m.contains("Invalid object name") {
        "a table/view in the export's query does not exist"
    } else if m.contains("Invalid column name") {
        "a column in the export's query does not exist"
    } else {
        return None; // operational → fail-soft
    };
    // The `query_scalar` seam erases the *typed* tiberius error, but the SQL
    // Server number survives in its Display string ("… (code: 208, …)"), so we
    // parse it out for an "error 208" label matching PG's SQLSTATE / MySQL's
    // code. A *typed* code would mean probing off the `query_scalar` seam — the
    // ADR-0011 boundary noted in `schema_error.rs`.
    let code_label = mssql_error_code(&m)
        .map(|c| format!("error {c}"))
        .unwrap_or_else(|| "SQL Server schema error".into());
    Some(PreflightSchemaError::new(detail, code_label).into_error())
}

/// Pull the SQL Server error number from a tiberius error's Display string
/// (`"… (code: 208, state: 1, class: 16)"`). `None` if tiberius ever changes the
/// format — `schema_fail_mssql` then falls back to a generic label, never panics.
fn mssql_error_code(msg: &str) -> Option<u16> {
    msg.split("code: ")
        .nth(1)?
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

fn row_estimate_mssql(conn: &mut MssqlSource, qualified_table: &str) -> Option<i64> {
    let (schema, table) = split_qualified(qualified_table);
    let sql = format!(
        "SELECT SUM(p.row_count) FROM sys.dm_db_partition_stats p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' AND p.index_id IN (0,1)",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
    );
    match conn.query_scalar(&sql) {
        Ok(opt) => opt.and_then(|s| s.parse::<i64>().ok()).map(|n| n.max(0)),
        Err(e) => {
            log::debug!("preflight: row-estimate probe failed for {qualified_table}: {e}");
            None
        }
    }
}

/// Average bytes/row from `dm_db_partition_stats` for `[schema.]table`:
/// `SUM(used_page_count) * 8192 / SUM(row_count)` over the heap / clustered
/// index (`index_id IN (0,1)`) — a maintained stat, no scan (8 KB is SQL
/// Server's fixed page size). `None` when the table is empty, the stats row is
/// absent, or the probe fails (preflight is non-fatal).
fn avg_row_bytes_mssql(conn: &mut MssqlSource, qualified_table: &str) -> Option<i64> {
    let (schema, table) = split_qualified(qualified_table);
    let sql = format!(
        "SELECT CASE WHEN SUM(p.row_count) > 0 \
                THEN SUM(p.used_page_count) * 8192 / SUM(p.row_count) ELSE NULL END \
         FROM sys.dm_db_partition_stats p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' AND p.index_id IN (0,1)",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
    );
    match conn.query_scalar(&sql) {
        Ok(opt) => opt.and_then(|s| s.parse::<i64>().ok()).filter(|n| *n > 0),
        Err(e) => {
            log::debug!("preflight: row-width probe failed for {qualified_table}: {e}");
            None
        }
    }
}

/// `MIN`/`MAX` of `expr` over the export's base relation, as display strings.
///
/// When the base query is a simple `SELECT … FROM <table>` we run
/// `MIN/MAX … FROM <table>` directly (no subquery wrap); otherwise we wrap the
/// user's query as a derived table so the bounds still come from exactly the
/// rows the export reads. `CONVERT(varchar(64), …)` renders any orderable type
/// (int, date, datetime2, uniqueidentifier) to the text form the printer shows.
fn range_min_max_mssql(
    conn: &mut MssqlSource,
    base_query: &str,
    base_table: Option<&str>,
    expr: &str,
) -> (Option<String>, Option<String>) {
    // One scalar cell only (the seam returns the first column), so MIN and MAX
    // are folded into a single `US`-delimited value and split back apart.
    const US: char = '\u{1f}';
    let select_list = format!(
        "CONCAT(CONVERT(varchar(64), MIN({expr})), CHAR(31), CONVERT(varchar(64), MAX({expr})))"
    );
    let sql = match base_table {
        Some(table) => format!("SELECT {select_list} FROM {table}"),
        None => format!("SELECT {select_list} FROM ({base_query}) AS _rivet"),
    };
    match conn.query_scalar(&sql) {
        Ok(Some(agg)) => {
            let mut parts = agg.splitn(2, US);
            let min_v = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
            let max_v = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
            (min_v, max_v)
        }
        Ok(None) => (None, None),
        Err(e) => {
            log::debug!("preflight: range probe on '{expr}' failed: {e}");
            (None, None)
        }
    }
}

/// `Some(true)` when `column` is the single leading key of *some* index on
/// `[schema.]table`, `Some(false)` when the catalog probe ran cleanly and found
/// none, `None` when the probe itself failed.
///
/// Range chunking (`WHERE col >= $lo AND col < $hi`) and incremental cursors
/// (`WHERE col > $last ORDER BY col`) only benefit when the column leads an
/// index (`ic.key_ordinal = 1`). This is the SQL Server analogue of the
/// PG/MySQL leading-key probe; it asks `sys.index_columns` directly rather than
/// inspect a query plan, so the signal is a catalog fact, not a heuristic.
fn column_has_index_mssql(
    conn: &mut MssqlSource,
    qualified_table: &str,
    column: &str,
) -> Option<bool> {
    let (schema, table) = split_qualified(qualified_table);
    let sql = format!(
        "SELECT COUNT(*) FROM sys.indexes i \
         JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         JOIN sys.objects o ON o.object_id = i.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE ic.key_ordinal = 1 AND i.index_id > 0 \
           AND s.name = N'{}' AND o.name = N'{}' AND c.name = N'{}'",
        schema.replace('\'', "''"),
        table.replace('\'', "''"),
        column.replace('\'', "''"),
    );
    match conn.query_scalar(&sql) {
        Ok(opt) => Some(opt.and_then(|s| s.parse::<i64>().ok()).unwrap_or(0) > 0),
        Err(e) => {
            log::debug!("preflight: index probe failed for {qualified_table}.{column}: {e}");
            None
        }
    }
}

/// Split a `[schema.]table` name into `(schema, table)`, defaulting the schema
/// to `dbo` when unqualified (SQL Server's default schema — matches `init` and
/// `introspect_mssql_table_for_chunking`).
fn split_qualified(qualified_table: &str) -> (&str, &str) {
    match qualified_table.split_once('.') {
        Some((s, t)) => (s, t),
        None => ("dbo", qualified_table),
    }
}

// Reuse the PG simple-FROM parser for the `query: SELECT cols FROM <table>`
// shape `init` emits — it recovers a single bare/dotted relation and rejects
// joins/comma-lists/subqueries, exactly the conservatism the catalog probes
// need. `strip_select_star_from` covers the `table:`-shortcut `SELECT * FROM …`
// form (and is what the chunk runner keys on), so we try it first.
use super::postgres::table_from_simple_query;
use crate::sql::strip_select_star_from;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_qualified_defaults_schema_to_dbo() {
        assert_eq!(split_qualified("orders"), ("dbo", "orders"));
    }

    #[test]
    fn split_qualified_keeps_explicit_schema() {
        assert_eq!(split_qualified("dbo.orders"), ("dbo", "orders"));
        assert_eq!(split_qualified("sales.line_items"), ("sales", "line_items"));
    }

    // ── base-relation recovery: the row-estimate / index probes must key on
    // the real table for both YAML shapes `rivet init` emits ──────────────────
    //
    // `init` renders `query: SELECT cols FROM <table>` to lock the column list;
    // the `table:` shortcut renders `SELECT * FROM <table>`. Both must reduce to
    // the single base relation so the partition-stats / index probes describe
    // the real table — never a 1-row stub or the wrong relation.

    fn base_table_of(q: &str) -> Option<&str> {
        strip_select_star_from(q).or_else(|| table_from_simple_query(q))
    }

    #[test]
    fn base_table_from_select_star_shortcut() {
        assert_eq!(
            base_table_of("SELECT * FROM dbo.orders"),
            Some("dbo.orders")
        );
        assert_eq!(base_table_of("SELECT * FROM orders"), Some("orders"));
    }

    #[test]
    fn base_table_from_init_column_list_query() {
        // The exact shape `rivet init` emits (explicit column list, schema-
        // qualified relation). `strip_select_star_from` rejects it (not `*`),
        // so the `table_from_simple_query` fallback must recover the relation.
        let q = "SELECT id, user_id, product FROM dbo.orders";
        assert_eq!(base_table_of(q), Some("dbo.orders"));
    }

    #[test]
    fn base_table_none_for_multi_relation_query() {
        // A genuinely multi-relation FROM (an immediate JOIN, or a comma list)
        // can't be reduced to one base relation — the probes must see `None`
        // and degrade to "unknown" rather than catalog-probe the wrong table
        // and falsely promote the verdict. A FROM-clause subquery is likewise
        // not a base table. (Note: `strip_select_star_from` is the first gate
        // and already rejects every trailing clause, so the `table:` shortcut
        // never reaches the looser `table_from_simple_query` fallback here.)
        assert_eq!(
            base_table_of("SELECT * FROM orders JOIN users ON orders.user_id = users.id"),
            None
        );
        assert_eq!(base_table_of("SELECT * FROM orders, users"), None);
        assert_eq!(base_table_of("SELECT * FROM (SELECT 1 AS x) AS s"), None);
    }

    #[test]
    fn mssql_error_code_parsed_from_tiberius_display() {
        // The exact shape tiberius renders for a missing relation (verified live).
        let m = "mssql: scalar query failed: Token error: 'Invalid object name \
                 'ordrs'.' on server x executing  on line 1 (code: 208, state: 1, class: 16)";
        assert_eq!(mssql_error_code(m), Some(208));
        assert_eq!(
            mssql_error_code("Invalid column name 'totl'. (code: 207)"),
            Some(207)
        );
        assert_eq!(mssql_error_code("connection reset; no code here"), None);
    }
}
