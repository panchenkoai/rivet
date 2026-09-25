//! Oracle preflight diagnostics for `rivet check` / `rivet plan` (the MSSQL
//! module's shape: catalog probes over the `query_scalar` seam feed the shared
//! [`assemble_diagnostic`]; no query plan is parsed, so `scan_type` stays `None`).

use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use super::postgres::table_from_simple_query;
use super::schema_error::PreflightSchemaError;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::error::Result;
use crate::source::Source;
use crate::source::oracle::OracleSource;
use crate::sql::strip_select_star_from;

/// Connect once and build one [`ExportDiagnostic`] per export.
pub(super) fn check_oracle(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
) -> Result<Vec<ExportDiagnostic>> {
    let mut conn = OracleSource::connect_with_tls(url, tls)?;
    super::collect_diagnostics(exports, |export| diagnose_oracle(&mut conn, export))
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_oracle(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<ExportDiagnostic> {
    let mut conn = OracleSource::connect_with_tls(url, tls)?;
    diagnose_oracle(&mut conn, export)
}

fn scalar_i64(conn: &mut OracleSource, sql: &str, what: &str) -> Option<i64> {
    match conn.query_scalar(sql) {
        Ok(v) => v.and_then(|s| s.trim().parse::<i64>().ok()),
        Err(e) => {
            log::debug!("preflight: oracle {what} probe failed: {e:#}");
            None
        }
    }
}

fn diagnose_oracle(conn: &mut OracleSource, export: &ExportConfig) -> Result<ExportDiagnostic> {
    let base_query = resolve_preflight_base_query(export);
    let base_query = base_query.as_str();
    if let Some(fail) = schema_fail_oracle(conn, base_query) {
        return Err(fail);
    }
    for col in key_columns(export) {
        if let Some(fail) = key_column_fail_oracle(conn, base_query, col) {
            return Err(fail);
        }
    }
    let base_table_owned = strip_select_star_from(base_query)
        .map(std::borrow::Cow::Borrowed)
        .or_else(|| table_from_simple_query(base_query));
    let base_table = base_table_owned.as_deref();

    let auto_pk: Option<String> =
        auto_pk_probe_target(export, base_table).and_then(|t| single_int_pk_oracle(conn, t));
    let range_col = preflight_range_col_resolved(export, auto_pk.as_deref());

    let row_estimate = base_table.and_then(|t| {
        let (owner, table) = crate::sql::oracle_catalog_preds(t);
        scalar_i64(
            conn,
            &format!(
                "SELECT num_rows FROM all_tables WHERE owner = {owner} AND table_name = {table}"
            ),
            "row-estimate",
        )
        .map(|n| n.max(0))
    });
    let avg_row_bytes = base_table.and_then(|t| {
        let (owner, table) = crate::sql::oracle_catalog_preds(t);
        scalar_i64(
            conn,
            &format!(
                "SELECT avg_row_len FROM all_tables WHERE owner = {owner} AND table_name = {table}"
            ),
            "row-width",
        )
        .filter(|n| *n > 0)
    });

    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        match incremental_key_expr(export, SourceType::Oracle) {
            Some(expr) => range_min_max_oracle(conn, base_query, base_table, &expr),
            None => (None, None),
        }
    } else if let Some(col) = range_col {
        let expr = crate::sql::quote_ident(SourceType::Oracle, col);
        range_min_max_oracle(conn, base_query, base_table, &expr)
    } else {
        (None, None)
    };

    let catalog_index = index_probe_target(export, auto_pk.as_deref(), base_table)
        .and_then(|(table, col)| column_has_index_oracle(conn, table, col));
    let db_max_connections = conn
        .query_scalar("SELECT value FROM v$parameter WHERE name = 'processes'")
        .ok()
        .flatten()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|&n| n > 0);

    Ok(assemble_diagnostic(
        export,
        ProbeFacts {
            auto_pk,
            row_estimate,
            avg_row_bytes,
            range_min,
            range_max,
            scan_type: None,
            plan_uses_index: false,
            catalog_index,
            db_max_connections,
        },
    ))
}

/// Validate the query's relations with a zero-row wrap; a missing table/column
/// (ORA-00942 / ORA-00904) is a loud preflight error, anything else fail-soft.
fn schema_fail_oracle(conn: &mut OracleSource, base_query: &str) -> Option<anyhow::Error> {
    let probe = format!("SELECT 1 FROM ({base_query}) \"_rivet_probe\" WHERE 1 = 0");
    let Err(e) = conn.query_scalar(&probe) else {
        return None;
    };
    let m = format!("{e:#}");
    let (detail, code) = if m.contains("ORA-00942") {
        (
            "a table/view in the export's query does not exist",
            "ORA-00942",
        )
    } else if m.contains("ORA-00904") {
        ("a column in the export's query does not exist", "ORA-00904")
    } else {
        return None;
    };
    Some(PreflightSchemaError::new(detail, code.to_string()).into_error())
}

/// The columns the export's strategy pages or tracks by, as written in the config.
fn key_columns(export: &ExportConfig) -> Vec<&str> {
    [
        &export.chunk_column,
        &export.chunk_by_key,
        &export.cursor_column,
        &export.cursor_fallback_column,
    ]
    .into_iter()
    .flatten()
    .map(String::as_str)
    .collect()
}

/// A strategy column the result does not have is a loud error naming Oracle's case rule.
fn key_column_fail_oracle(
    conn: &mut OracleSource,
    base_query: &str,
    col: &str,
) -> Option<anyhow::Error> {
    let quoted = crate::sql::quote_ident(SourceType::Oracle, col);
    let probe = format!("SELECT {quoted} FROM ({base_query}) \"_rivet_probe\" WHERE 1 = 0");
    let e = conn.query_scalar(&probe).err()?;
    format!("{e:#}").contains("ORA-00904").then(|| {
        PreflightSchemaError::new(unknown_key_column_detail(col), "ORA-00904".to_string())
            .into_error()
    })
}

/// Why a strategy column was not found: Oracle matches names exactly, and unquoted DDL is upper-case.
fn unknown_key_column_detail(col: &str) -> String {
    format!(
        "column '{col}' is not in the export's result; Oracle names match exactly and a table \
         created without quotes stores them upper-case — write '{}'",
        col.to_uppercase()
    )
}

/// The single integer `NUMBER(p<=18,0)` primary-key column of `table`, if any.
fn single_int_pk_oracle(conn: &mut OracleSource, qualified: &str) -> Option<String> {
    let (owner, table) = crate::sql::oracle_catalog_preds(qualified);
    conn.query_scalar(&format!(
        "SELECT MIN(cc.column_name) FROM all_constraints c \
         JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name \
         JOIN all_tab_columns tc ON tc.owner = cc.owner AND tc.table_name = cc.table_name \
           AND tc.column_name = cc.column_name \
         WHERE c.constraint_type = 'P' AND c.owner = {owner} AND c.table_name = {table} \
         HAVING COUNT(*) = 1 AND MIN(CASE WHEN tc.data_type = 'NUMBER' AND tc.data_scale = 0 \
           AND tc.data_precision <= 18 THEN 1 ELSE 0 END) = 1"
    ))
    .ok()
    .flatten()
}

/// `MIN`/`MAX` of `expr` over the export's rows, as display text.
fn range_min_max_oracle(
    conn: &mut OracleSource,
    base_query: &str,
    base_table: Option<&str>,
    expr: &str,
) -> (Option<String>, Option<String>) {
    let from = match base_table {
        Some(t) => t.to_string(),
        None => format!("({base_query}) \"_rivet\""),
    };
    let sql = format!("SELECT TO_CHAR(MIN({expr})) || CHR(31) || TO_CHAR(MAX({expr})) FROM {from}");
    match conn.query_scalar(&sql) {
        Ok(Some(agg)) => {
            let mut parts = agg.splitn(2, '\u{1f}');
            let lo = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
            let hi = parts.next().filter(|s| !s.is_empty()).map(str::to_string);
            (lo, hi)
        }
        Ok(None) => (None, None),
        Err(e) => {
            log::debug!("preflight: oracle range probe on '{expr}' failed: {e:#}");
            (None, None)
        }
    }
}

/// `Some(true)` when `column` leads some index on `table`, `Some(false)` when the
/// probe ran and found none, `None` when it could not run.
fn column_has_index_oracle(conn: &mut OracleSource, qualified: &str, column: &str) -> Option<bool> {
    let (owner, table) = crate::sql::oracle_catalog_preds(qualified);
    let col = column.trim_matches('"').replace('\'', "''");
    scalar_i64(
        conn,
        &format!(
            "SELECT COUNT(*) FROM all_ind_columns WHERE table_owner = {owner} \
             AND table_name = {table} AND column_position = 1 AND column_name = '{col}'"
        ),
        "index",
    )
    .map(|n| n > 0)
}
