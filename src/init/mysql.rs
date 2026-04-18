use mysql::prelude::Queryable;

use crate::error::Result;

use super::{ColumnInfo, TableInfo};

/// Database to list tables from: `--schema` if set, else non-empty path segment in the URL.
pub(super) fn resolve_database_for_listing(url: &str, schema_cli: Option<&str>) -> Result<String> {
    if let Some(s) = schema_cli {
        let s = s.trim();
        if !s.is_empty() {
            return Ok(s.to_string());
        }
    }
    let opts = mysql::Opts::from_url(url)?;
    opts.get_db_name()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL: put the database in the URL (mysql://user:pass@host:3306/dbname) or pass --schema <database>"
            )
        })
}

/// Tables and views in a MySQL database (`information_schema`; `TABLE_SCHEMA` is the database name).
pub(super) fn list_tables(url: &str, database: &str) -> Result<Vec<String>> {
    let pool = mysql::Pool::new(mysql::Opts::from_url(url)?)?;
    let mut conn = pool.get_conn()?;
    let sql = format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
         ORDER BY TABLE_NAME",
        escape(database)
    );
    let rows: Vec<String> = conn.query(sql)?;
    Ok(rows)
}

pub(super) fn introspect(url: &str, table: &str) -> Result<TableInfo> {
    let pool = mysql::Pool::new(mysql::Opts::from_url(url)?)?;
    let mut conn = pool.get_conn()?;

    // Row estimate + on-disk size from information_schema (fast, no COUNT(*)).
    let row_and_size: Option<(Option<u64>, Option<u64>, Option<u64>)> =
        conn.query_first(format!(
            "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
            escape(table)
        ))?;
    let (row_estimate, total_bytes) = match row_and_size {
        Some((rows, data, idx)) => {
            let r = rows.map(|n| n as i64).unwrap_or(0);
            let bytes = match (data, idx) {
                (Some(d), Some(i)) => Some((d + i) as i64),
                (Some(d), None) => Some(d as i64),
                (None, Some(i)) => Some(i as i64),
                (None, None) => None,
            };
            (r, bytes.filter(|v| *v > 0))
        }
        None => (0, None),
    };

    // Column metadata (includes KEY + IS_NULLABLE for Epic B).
    let rows: Vec<(String, String, String, String)> = conn.query(format!(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}' \
         ORDER BY ORDINAL_POSITION",
        escape(table)
    ))?;

    if rows.is_empty() {
        anyhow::bail!(
            "Table '{table}' not found or has no columns. \
             Check the table name and that the user has SELECT privilege."
        );
    }

    let columns = rows
        .into_iter()
        .map(|(name, data_type, column_key, nullable)| ColumnInfo {
            is_primary_key: column_key == "PRI",
            is_nullable: nullable.eq_ignore_ascii_case("YES"),
            name,
            data_type,
        })
        .collect();

    Ok(TableInfo {
        schema: String::new(),
        table: table.to_string(),
        row_estimate,
        total_bytes,
        columns,
    })
}

/// Minimal escaping: replace single quotes in identifiers.
fn escape(s: &str) -> String {
    s.replace('\'', "\\'")
}
