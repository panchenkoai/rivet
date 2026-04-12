use crate::error::Result;

use super::{ColumnInfo, TableInfo};

/// Tables and views in a PostgreSQL schema (`information_schema`).
pub(super) fn list_tables(url: &str, schema: &str) -> Result<Vec<String>> {
    let mut client = postgres::Client::connect(url, postgres::NoTls)?;
    let rows = client.query(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type IN ('BASE TABLE', 'VIEW')
         ORDER BY table_name",
        &[&schema],
    )?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

pub(super) fn introspect(url: &str, schema: &str, table: &str) -> Result<TableInfo> {
    let mut client = postgres::Client::connect(url, postgres::NoTls)?;

    // Row estimate from pg_class (fast, no COUNT(*))
    let row_estimate: i64 = client
        .query_opt(
            "SELECT reltuples::bigint FROM pg_class
             WHERE relname = $1 AND relnamespace = (
                 SELECT oid FROM pg_namespace WHERE nspname = $2
             )",
            &[&table, &schema],
        )?
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0)
        .max(0);

    // Primary key columns
    let pk_rows = client.query(
        "SELECT a.attname
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = ($1 || '.' || $2)::regclass
           AND i.indisprimary",
        &[&schema, &table],
    )?;
    let pk_cols: std::collections::HashSet<String> =
        pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    // Column metadata
    let col_rows = client.query(
        "SELECT column_name, data_type
         FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
         ORDER BY ordinal_position",
        &[&schema, &table],
    )?;

    if col_rows.is_empty() {
        anyhow::bail!(
            "Table '{schema}.{table}' not found or has no columns. \
             Check the table name and that the user has SELECT privilege."
        );
    }

    let columns = col_rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let is_primary_key = pk_cols.contains(&name);
            ColumnInfo {
                name,
                data_type,
                is_primary_key,
            }
        })
        .collect();

    Ok(TableInfo {
        schema: schema.to_string(),
        table: table.to_string(),
        row_estimate,
        columns,
    })
}
