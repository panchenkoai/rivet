//! SQL Server introspection for `rivet init` (parity with [`super::postgres`] /
//! [`super::mysql`]).
//!
//! The shape mirrors the PG module — `connect()` opens one connection shared
//! across the whole init run, `list_tables()` enumerates a schema, and
//! `introspect()` builds a [`TableInfo`] — but the seam is narrower: the
//! [`MssqlSource`] the export path already owns exposes only
//! [`query_scalar`](crate::source::Source::query_scalar) publicly (one scalar
//! cell). So, exactly like
//! [`crate::source::mssql::introspect_mssql_table_for_chunking`], every
//! multi-row / multi-column read is folded server-side into a single delimited
//! scalar with `STRING_AGG`: `CHAR(31)` (unit separator) between fields and
//! `CHAR(30)` (record separator) between rows. Both bytes are illegal in a SQL
//! Server identifier and never appear in `information_schema` type names, so the
//! split back apart is unambiguous.

use crate::error::Result;
use crate::source::Source;
use crate::source::mssql::MssqlSource;

use super::{ColumnInfo, TableInfo};

/// Record (row) separator folded into the aggregated scalar — ASCII 30 (RS).
const RS: char = '\u{1e}';
/// Field separator within one aggregated record — ASCII 31 (US).
const US: char = '\u{1f}';

/// Open the one connection shared across the whole init run (`list_tables`
/// plus every per-table `introspect` — no per-table reconnect), through the
/// same [`MssqlSource::connect_with_tls`] path doctor/check/run use.
///
/// `init` runs before any YAML `tls:` block exists (it *generates* the config),
/// so no [`crate::config::TlsConfig`] is threaded here: SQL Server forces TLS on
/// the login handshake regardless, and with no `tls:` block the connector
/// trust-certs the server certificate (encrypted, unverified) — the documented
/// default for dev / self-signed setups. Add a `source.tls:` block to the
/// generated config to enable strict validation for production runs.
pub(super) fn connect(url: &str) -> Result<MssqlSource> {
    MssqlSource::connect_with_tls(url, None)
}

/// Base tables and views in a SQL Server schema (`information_schema.TABLES`).
///
/// `TABLE_CATALOG` is the connection's database (from the URL), so no catalog
/// filter is needed; `schema` filters `TABLE_SCHEMA` (default `dbo`). Names are
/// aggregated into one `RS`-delimited scalar because the source seam only hands
/// back a single cell.
pub(super) fn list_tables(conn: &mut MssqlSource, schema: &str) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT STRING_AGG(TABLE_NAME, CHAR(30)) WITHIN GROUP (ORDER BY TABLE_NAME) \
         FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = N'{}' AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')",
        schema.replace('\'', "''"),
    );
    let names = conn
        .query_scalar(&sql)?
        .map(|s| {
            s.split(RS)
                .filter(|n| !n.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(names)
}

pub(super) fn introspect(conn: &mut MssqlSource, schema: &str, table: &str) -> Result<TableInfo> {
    let schema_lit = schema.replace('\'', "''");
    let table_lit = table.replace('\'', "''");

    // Row estimate from `sys.dm_db_partition_stats` (rows in the heap/clustered
    // index, index_id 0/1) — fast, no `COUNT(*)`. `0` for a view or when stats
    // are unavailable; never fails the scaffold.
    let count_sql = format!(
        "SELECT SUM(p.row_count) FROM sys.dm_db_partition_stats p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{schema_lit}' AND o.name = N'{table_lit}' AND p.index_id IN (0,1)",
    );
    let row_estimate = conn
        .query_scalar(&count_sql)?
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);

    // Column metadata: one `RS`-delimited record per column, each holding six
    // `US`-delimited fields (name, data_type, is_pk, is_nullable, precision,
    // scale). `COLUMNPROPERTY(..., 'IsXmlIndexable')` is not needed; PK-ness
    // comes from the primary-key constraint join, NULL-ability and numeric
    // precision/scale from `information_schema.COLUMNS`. `ISNULL(...,'')` keeps
    // absent precision/scale as empty fields so the `US` split stays aligned.
    let columns_sql = format!(
        "SELECT STRING_AGG( \
             CONCAT( \
                 c.COLUMN_NAME, CHAR(31), \
                 c.DATA_TYPE, CHAR(31), \
                 CASE WHEN pk.COLUMN_NAME IS NULL THEN '0' ELSE '1' END, CHAR(31), \
                 c.IS_NULLABLE, CHAR(31), \
                 ISNULL(CONVERT(varchar(12), c.NUMERIC_PRECISION), ''), CHAR(31), \
                 ISNULL(CONVERT(varchar(12), c.NUMERIC_SCALE), '') \
             ), CHAR(30)) WITHIN GROUP (ORDER BY c.ORDINAL_POSITION) \
         FROM information_schema.COLUMNS c \
         LEFT JOIN ( \
             SELECT ku.COLUMN_NAME \
             FROM information_schema.TABLE_CONSTRAINTS tc \
             JOIN information_schema.KEY_COLUMN_USAGE ku \
                 ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
                 AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA \
                 AND ku.TABLE_NAME = tc.TABLE_NAME \
             WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
                 AND tc.TABLE_SCHEMA = N'{schema_lit}' AND tc.TABLE_NAME = N'{table_lit}' \
         ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME \
         WHERE c.TABLE_SCHEMA = N'{schema_lit}' AND c.TABLE_NAME = N'{table_lit}'",
    );

    let agg = conn.query_scalar(&columns_sql)?;
    let columns: Vec<ColumnInfo> = agg.as_deref().map(parse_columns_agg).unwrap_or_default();

    if columns.is_empty() {
        anyhow::bail!(
            "Table '{schema}.{table}' not found or has no columns. \
             Check the table name and that the user has SELECT privilege."
        );
    }

    Ok(TableInfo {
        schema: schema.to_string(),
        table: table.to_string(),
        row_estimate,
        total_bytes: None,
        columns,
    })
}

/// Split the `RS`/`US`-delimited column aggregate back into [`ColumnInfo`]s.
/// A malformed record (wrong field count) is skipped rather than panicking —
/// the worst case is a missing column in the scaffold, which `rivet check`
/// would then flag, never a crash.
fn parse_columns_agg(agg: &str) -> Vec<ColumnInfo> {
    agg.split(RS)
        .filter(|rec| !rec.is_empty())
        .filter_map(|rec| {
            let f: Vec<&str> = rec.split(US).collect();
            if f.len() != 6 {
                return None;
            }
            Some(ColumnInfo {
                name: f[0].to_string(),
                data_type: f[1].to_string(),
                is_primary_key: f[2] == "1",
                is_nullable: f[3].eq_ignore_ascii_case("YES"),
                numeric_precision: f[4].parse::<u32>().ok(),
                numeric_scale: f[5].parse::<u32>().ok(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build one `US`-joined column record (mirrors the server-side CONCAT).
    fn rec(fields: &[&str]) -> String {
        fields.join(&US.to_string())
    }

    /// Join column records with the `RS` separator (mirrors STRING_AGG).
    fn agg(records: &[String]) -> String {
        records.join(&RS.to_string())
    }

    #[test]
    fn parse_columns_agg_round_trips_fields() {
        // id BIGINT PK NOT NULL, amount DECIMAL(12,2) NOT NULL, note NVARCHAR NULL.
        let s = agg(&[
            rec(&["id", "bigint", "1", "NO", "", ""]),
            rec(&["amount", "decimal", "0", "NO", "12", "2"]),
            rec(&["note", "nvarchar", "0", "YES", "", ""]),
        ]);
        let cols = parse_columns_agg(&s);
        assert_eq!(cols.len(), 3);

        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "bigint");
        assert!(cols[0].is_primary_key);
        assert!(!cols[0].is_nullable);
        assert_eq!(cols[0].numeric_precision, None);
        assert_eq!(cols[0].numeric_scale, None);

        assert_eq!(cols[1].name, "amount");
        assert!(!cols[1].is_primary_key);
        assert_eq!(cols[1].numeric_precision, Some(12));
        assert_eq!(cols[1].numeric_scale, Some(2));

        assert_eq!(cols[2].name, "note");
        assert!(cols[2].is_nullable);
    }

    #[test]
    fn parse_columns_agg_skips_malformed_record() {
        // Second record has only 2 fields — dropped, the well-formed one survives.
        let s = agg(&[
            rec(&["id", "bigint", "1", "NO", "", ""]),
            rec(&["broken", "rec"]),
        ]);
        let cols = parse_columns_agg(&s);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn parse_columns_agg_empty_is_empty() {
        assert!(parse_columns_agg("").is_empty());
    }
}
