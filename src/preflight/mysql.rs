use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::error::Result;

pub(super) fn check_mysql(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
    silent: bool,
) -> Result<()> {
    let pool = crate::source::mysql::connect_pool(url, tls)?;
    let mut conn = pool.get_conn()?;
    let db_max_connections = fetch_max_connections_mysql(&mut conn);

    for export in exports {
        let diag = diagnose_mysql(&mut conn, export, db_max_connections)?;
        if !silent {
            super::print_diagnostic(&diag);
        }
    }

    Ok(())
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_mysql(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<super::ExportDiagnostic> {
    let pool = crate::source::mysql::connect_pool(url, tls)?;
    let mut conn = pool.get_conn()?;
    let db_max_connections = fetch_max_connections_mysql(&mut conn);
    diagnose_mysql(&mut conn, export, db_max_connections)
}

fn fetch_max_connections_mysql(conn: &mut mysql::PooledConn) -> Option<u32> {
    use mysql::prelude::Queryable;
    let val: u64 = conn.query_first("SELECT @@max_connections").ok()??;
    val.try_into().ok()
}

fn diagnose_mysql(
    conn: &mut mysql::PooledConn,
    export: &ExportConfig,
    db_max_connections: Option<u32>,
) -> Result<ExportDiagnostic> {
    use mysql::prelude::Queryable;

    let mode_str = match export.mode {
        ExportMode::Full => "full".to_string(),
        ExportMode::Incremental => format!(
            "incremental (cursor: {})",
            export.cursor_column.as_deref().unwrap_or("?")
        ),
        ExportMode::Chunked => format!(
            "chunked (column: {}, size: {})",
            export.chunk_column.as_deref().unwrap_or("?"),
            export.chunk_size
        ),
        ExportMode::TimeWindow => format!(
            "time_window (column: {}, days: {})",
            export.time_column.as_deref().unwrap_or("?"),
            export.days_window.unwrap_or(0)
        ),
    };

    let base_query = export.query.as_deref().unwrap_or("SELECT 1");
    let range_col = export
        .chunk_column
        .as_deref()
        .or(export.cursor_column.as_deref());
    let effective_query = if let Some(order) = incremental_key_expr(export, SourceType::Mysql) {
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, order
        )
    } else {
        base_query.to_string()
    };

    let row_estimate = {
        let explain_query = format!("EXPLAIN {}", effective_query);
        let rows: Vec<mysql::Row> = conn.query(&explain_query).unwrap_or_default();
        rows.first().and_then(|r| {
            let val: Option<mysql::Value> = r.get("rows");
            match val {
                Some(mysql::Value::Int(v)) => Some(v),
                Some(mysql::Value::UInt(v)) => Some(v as i64),
                Some(mysql::Value::Bytes(ref b)) => {
                    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                }
                _ => None,
            }
        })
    };

    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        if let Some(expr) = incremental_key_expr(export, SourceType::Mysql) {
            let range_query = format!(
                "SELECT CAST(min({expr}) AS CHAR), CAST(max({expr}) AS CHAR) FROM ({base}) AS _rivet",
                expr = expr,
                base = base_query,
            );
            match conn.query_first::<(Option<String>, Option<String>), _>(&range_query) {
                Ok(Some((min_v, max_v))) => (min_v, max_v),
                _ => (None, None),
            }
        } else {
            (None, None)
        }
    } else if let Some(col) = range_col {
        let range_query = format!(
            "SELECT CAST(min({col}) AS CHAR), CAST(max({col}) AS CHAR) FROM ({base}) AS _rivet",
            col = col,
            base = base_query,
        );
        match conn.query_first::<(Option<String>, Option<String>), _>(&range_query) {
            Ok(Some((min_v, max_v))) => (min_v, max_v),
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    let (scan_type, plan_uses_index) = {
        let explain_query = format!("EXPLAIN {}", effective_query);
        let rows: Vec<mysql::Row> = conn.query(&explain_query).unwrap_or_default();
        if let Some(row) = rows.first() {
            let access_type = mysql_row_get_string(row, "type");
            let key = mysql_row_get_string(row, "key");
            let has_index = matches!(
                access_type.as_deref(),
                Some("ref") | Some("range") | Some("index") | Some("eq_ref") | Some("const")
            );
            let desc = match (&access_type, &key) {
                (Some(t), Some(k)) => format!("{} using {}", t, k),
                (Some(t), None) => t.clone(),
                _ => "unknown".to_string(),
            };
            (Some(desc), has_index)
        } else {
            (None, false)
        }
    };

    // Same logic as the PG side: EXPLAIN of the base query reports `ALL`
    // (full table scan) for a no-WHERE read, even on tables where the
    // chunk_column is a perfect PK with a btree. The chunk runner actually
    // issues `WHERE chunk_col >= $lo AND chunk_col < $hi`, which would use
    // the index. Override `uses_index` from the catalog when the column is
    // the leading key of some index on the table.
    let uses_index = if matches!(export.mode, ExportMode::Chunked | ExportMode::Incremental)
        && let Some(col) = range_col
        && let Some(table) = export
            .table
            .as_deref()
            .or_else(|| super::postgres::table_from_simple_query(base_query))
    {
        match column_has_index_mysql(conn, table, col) {
            Some(true) => true,
            Some(false) => plan_uses_index,
            None => plan_uses_index,
        }
    } else {
        plan_uses_index
    };

    let strategy = derive_strategy(export);
    let verdict = compute_verdict(row_estimate, uses_index, export.cursor_column.is_some());
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let warnings = collect_warnings(
        export,
        row_estimate,
        range_min.as_deref(),
        range_max.as_deref(),
        db_max_connections,
    );
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        strategy,
        mode: mode_str,
        cursor_column: export.cursor_column.clone(),
        row_estimate,
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

fn mysql_row_get_string(row: &mysql::Row, col: &str) -> Option<String> {
    let val: Option<mysql::Value> = row.get(col);
    match val {
        Some(mysql::Value::Bytes(b)) => String::from_utf8(b).ok(),
        Some(mysql::Value::Int(v)) => Some(v.to_string()),
        Some(mysql::Value::UInt(v)) => Some(v.to_string()),
        _ => None,
    }
}

/// True when `column` is the leading key of *any* index on `table`.
///
/// MySQL exposes index metadata via `information_schema.statistics`. We
/// match rows where `COLUMN_NAME = $col` and `SEQ_IN_INDEX = 1` — the
/// leading-column condition for range and prefix scans. Index type
/// (`BTREE` / `HASH` / `FULLTEXT`) is filtered to BTREE only because
/// range chunking (`WHERE col >= $lo AND col < $hi`) and incremental
/// cursors (`WHERE col > $last`) only benefit from B-tree access paths.
///
/// `table` is either bare (`orders`) or schema-qualified (`rivet.orders`).
/// Unqualified names resolve against the connection's current database.
///
/// Returns `Some(true)` when an index is found, `Some(false)` when the
/// catalog probe ran cleanly and found none, `None` when the probe
/// itself failed. Callers fall back to the EXPLAIN-based heuristic on
/// `None`.
pub(crate) fn column_has_index_mysql(
    conn: &mut mysql::PooledConn,
    qualified_table: &str,
    column: &str,
) -> Option<bool> {
    use mysql::prelude::Queryable;
    let (schema_opt, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (Some(s.to_string()), t.to_string()),
        None => (None, qualified_table.to_string()),
    };
    let sql = match &schema_opt {
        Some(_) => {
            "SELECT 1 FROM information_schema.statistics \
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? \
                      AND SEQ_IN_INDEX = 1 AND INDEX_TYPE = 'BTREE' LIMIT 1"
        }
        None => {
            "SELECT 1 FROM information_schema.statistics \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? \
                   AND SEQ_IN_INDEX = 1 AND INDEX_TYPE = 'BTREE' LIMIT 1"
        }
    };
    let res: mysql::Result<Option<u8>> = match &schema_opt {
        Some(schema) => conn.exec_first(sql, (schema, &table, column)),
        None => conn.exec_first(sql, (&table, column)),
    };
    match res {
        Ok(row) => Some(row.is_some()),
        Err(e) => {
            log::debug!("preflight: btree index probe failed for {qualified_table}.{column}: {e}");
            None
        }
    }
}
