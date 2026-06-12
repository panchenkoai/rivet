use mysql::prelude::Queryable;

use crate::error::Result;

use super::{ColumnInfo, TableInfo};

/// Introspection SQL, parameterized with `?` binds (prepared statements) so
/// identifiers never enter the SQL text — no interpolation, no hand-rolled
/// quote escaping.
const LIST_TABLES_SQL: &str = "SELECT TABLE_NAME FROM information_schema.TABLES \
     WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
     ORDER BY TABLE_NAME";

const TABLE_STATS_SQL: &str = "SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES \
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";

const COLUMNS_SQL: &str = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, IS_NULLABLE, \
            NUMERIC_PRECISION, NUMERIC_SCALE \
     FROM information_schema.COLUMNS \
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
     ORDER BY ORDINAL_POSITION";

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

/// Open the one connection shared across the whole init run (`list_tables`
/// plus every per-table `introspect` — no per-table reconnect).
///
/// Goes through the shared [`crate::source::mysql::connect_pool`] path so the
/// pool gets the lean constraints (no eager pre-connections). `init` runs
/// before any YAML `tls:` block exists (it *generates* the config), so the
/// connection stays plaintext, relying on the driver's own URL parsing.
pub(super) fn connect(url: &str) -> Result<mysql::PooledConn> {
    let pool = crate::source::mysql::connect_pool(url, None)?;
    Ok(pool.get_conn()?)
}

/// Tables and views in a MySQL database (`information_schema`; `TABLE_SCHEMA` is the database name).
pub(super) fn list_tables(conn: &mut mysql::PooledConn, database: &str) -> Result<Vec<String>> {
    let rows: Vec<String> = conn.exec(LIST_TABLES_SQL, (database,))?;
    Ok(rows)
}

pub(super) fn introspect(conn: &mut mysql::PooledConn, table: &str) -> Result<TableInfo> {
    // Row estimate + on-disk size from information_schema (fast, no COUNT(*)).
    let row_and_size: Option<(Option<u64>, Option<u64>, Option<u64>)> =
        conn.exec_first(TABLE_STATS_SQL, (table,))?;
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

    // Column metadata (includes KEY + IS_NULLABLE + numeric precision/scale for decimal columns).
    #[allow(clippy::type_complexity)]
    let rows: Vec<(String, String, String, String, Option<u32>, Option<u32>)> =
        conn.exec(COLUMNS_SQL, (table,))?;

    if rows.is_empty() {
        anyhow::bail!(
            "Table '{table}' not found or has no columns. \
             Check the table name and that the user has SELECT privilege."
        );
    }

    let columns = rows
        .into_iter()
        .map(
            |(name, data_type, column_key, nullable, numeric_precision, numeric_scale)| {
                ColumnInfo {
                    is_primary_key: column_key == "PRI",
                    is_nullable: nullable.eq_ignore_ascii_case("YES"),
                    name,
                    data_type,
                    numeric_precision,
                    numeric_scale,
                }
            },
        )
        .collect();

    Ok(TableInfo {
        schema: String::new(),
        table: table.to_string(),
        row_estimate,
        total_bytes,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn introspection_sql_binds_identifiers_instead_of_interpolating() {
        for sql in [LIST_TABLES_SQL, TABLE_STATS_SQL, COLUMNS_SQL] {
            assert_eq!(
                sql.matches('?').count(),
                1,
                "exactly one identifier bind expected in: {sql}"
            );
            // Regression: identifiers used to be format!-interpolated into the
            // SQL with a hand-rolled quote escape; the text must stay constant.
            assert!(
                !sql.contains('{') && !sql.contains('}'),
                "no format placeholder allowed in: {sql}"
            );
        }
    }

    #[test]
    fn resolve_database_prefers_cli_schema_over_url() {
        let db =
            resolve_database_for_listing("mysql://u:p@host:3306/from_url", Some("cli_db")).unwrap();
        assert_eq!(db, "cli_db");
    }

    #[test]
    fn resolve_database_falls_back_to_url_path_when_schema_blank_or_missing() {
        let db = resolve_database_for_listing("mysql://u:p@host:3306/from_url", None).unwrap();
        assert_eq!(db, "from_url");
        let db =
            resolve_database_for_listing("mysql://u:p@host:3306/from_url", Some("  ")).unwrap();
        assert_eq!(db, "from_url");
    }

    #[test]
    fn resolve_database_errors_when_no_database_anywhere() {
        let err = resolve_database_for_listing("mysql://u:p@host:3306", None)
            .expect_err("no database anywhere must error");
        assert!(format!("{err}").contains("--schema"), "got: {err}");
    }
}
