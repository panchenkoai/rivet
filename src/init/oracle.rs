//! `rivet init` catalog reader for Oracle: tables/views of one owner, their
//! columns, primary key and leading-index columns, and the optimizer row count.
//! Oracle's catalog types are renamed onto the vocabulary init's classifiers
//! speak (see [`catalog_type`]), the way the SQL Server reader renames
//! `timestamp` to `rowversion`.

use crate::error::Result;
use crate::source::Source;
use crate::source::oracle::OracleSource;

use super::{ColumnInfo, TableInfo};

pub(super) fn connect(url: &str, tls: Option<&crate::config::TlsConfig>) -> Result<OracleSource> {
    OracleSource::connect_with_tls(url, tls)
}

/// The owner init reads when none is given: the session's current schema.
pub(super) fn current_schema(conn: &mut OracleSource) -> Result<String> {
    conn.query_scalar("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual")?
        .ok_or_else(|| anyhow::anyhow!("oracle: could not read the current schema"))
}

/// `schema`, or the session's current schema for an unqualified / `public` placeholder.
pub(super) fn resolve_schema(conn: &mut OracleSource, schema: Option<&str>) -> Result<String> {
    match explicit_owner(schema) {
        Some(owner) => Ok(owner),
        None => current_schema(conn),
    }
}

/// The owner a `--schema` names, folded like Oracle; `None` for none, blank or the `public` placeholder.
fn explicit_owner(schema: Option<&str>) -> Option<String> {
    schema
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "public")
        .map(crate::sql::oracle_catalog_name)
}

fn lit(s: &str) -> String {
    s.replace('\'', "''")
}

pub(super) fn list_tables(conn: &mut OracleSource, schema: &str) -> Result<Vec<String>> {
    conn.query_list(&format!(
        "SELECT object_name FROM all_objects WHERE owner = '{}' \
           AND object_type IN ('TABLE', 'VIEW') AND object_name NOT LIKE 'BIN$%' \
         ORDER BY object_name",
        lit(schema)
    ))
}

/// Introspect `schema.table` (catalog-exact names); a private or PUBLIC synonym is followed to its local base table.
pub(super) fn introspect(conn: &mut OracleSource, schema: &str, table: &str) -> Result<TableInfo> {
    let mut columns = columns_of(conn, schema, table)?;
    let (mut schema, mut table) = (schema.to_string(), table.to_string());
    if columns.is_empty()
        && let Some((base_owner, base_table)) = synonym_target(conn, &schema, &table)?
    {
        columns = columns_of(conn, &base_owner, &base_table)?;
        if columns.is_empty() {
            anyhow::bail!(
                "Table '{schema}.{table}' not found or has no columns: it is a synonym for \
                 '{base_owner}.{base_table}', which the user cannot read (or which does not exist)."
            );
        }
        eprintln!(
            "rivet: note: '{schema}.{table}' is a synonym for '{base_owner}.{base_table}' — the \
             config reads the base table"
        );
        (schema, table) = (base_owner, base_table);
    }
    if columns.is_empty() {
        anyhow::bail!(
            "Table '{schema}.{table}' not found or has no columns. Oracle stores unquoted \
             names upper-case — check the spelling, and that the user can SELECT it."
        );
    }
    let (owner, name) = (lit(&schema), lit(&table));
    let stats = conn.query_scalar(&format!(
        "SELECT NVL(TO_CHAR(num_rows), 'NULL') FROM all_tables \
         WHERE owner = '{owner}' AND table_name = '{name}'"
    ))?;
    let mut density = None;
    let row_estimate = match stats.as_deref() {
        Some("NULL") => {
            let counted = conn
                .query_scalar(&capped_count_sql(&schema, &table))?
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let probe = unanalyzed_estimate(counted);
            let rows = probe.rows;
            density = Some(probe);
            rows
        }
        other => other
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0)
            .max(0),
    };
    Ok(TableInfo {
        density,
        schema,
        table,
        row_estimate,
        total_bytes: None,
        columns,
    })
}

/// Rows a never-analyzed table's estimate counts up to: past the 100K mode threshold, cheap to read.
const UNANALYZED_COUNT_CAP: i64 = 1_000_000;

/// A `COUNT(*)` that stops reading at [`UNANALYZED_COUNT_CAP`] rows.
fn capped_count_sql(schema: &str, table: &str) -> String {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    format!(
        "SELECT COUNT(*) FROM (SELECT 1 FROM {}.{} WHERE ROWNUM <= {UNANALYZED_COUNT_CAP})",
        q(schema),
        q(table)
    )
}

/// The estimate for a table with no optimizer stats: an exact count below the cap, a floor marked unverified at it.
fn unanalyzed_estimate(counted: i64) -> crate::init::density::DensityProbe {
    use crate::init::density::{DensityProbe, EstimateMethod};
    DensityProbe {
        rows: counted,
        density: 0.0,
        method: if counted < UNANALYZED_COUNT_CAP {
            EstimateMethod::Counted
        } else {
            EstimateMethod::Unverified
        },
        catalog_rows: 0,
        k: 0,
        w: 0,
    }
}

/// The local `(owner, table)` a synonym `schema.name` (private first, then PUBLIC) points at.
fn synonym_target(
    conn: &mut OracleSource,
    schema: &str,
    name: &str,
) -> Result<Option<(String, String)>> {
    let rows = conn.query_rows(&format!(
        "SELECT table_owner, table_name FROM all_synonyms \
         WHERE synonym_name = '{}' AND owner IN ('{}', 'PUBLIC') AND db_link IS NULL \
         ORDER BY CASE owner WHEN 'PUBLIC' THEN 1 ELSE 0 END",
        lit(name),
        lit(schema)
    ))?;
    Ok(rows.into_iter().next().and_then(|r| match r.as_slice() {
        [Some(o), Some(t)] => Some((o.clone(), t.clone())),
        _ => None,
    }))
}

fn columns_of(conn: &mut OracleSource, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
    let (owner, name) = (lit(schema), lit(table));
    let columns_sql = format!(
        "SELECT c.column_name, c.data_type, \
             CASE WHEN pk.column_name IS NULL THEN '0' ELSE '1' END, \
             CASE WHEN ix.column_name IS NULL THEN '0' ELSE '1' END, \
             c.nullable, c.data_precision, c.data_scale \
         FROM all_tab_columns c \
         LEFT JOIN (SELECT cc.column_name FROM all_constraints k \
                    JOIN all_cons_columns cc ON cc.owner = k.owner \
                      AND cc.constraint_name = k.constraint_name \
                    WHERE k.constraint_type = 'P' AND k.owner = '{owner}' \
                      AND k.table_name = '{name}') pk ON pk.column_name = c.column_name \
         LEFT JOIN (SELECT DISTINCT column_name FROM all_ind_columns \
                    WHERE table_owner = '{owner}' AND table_name = '{name}' \
                      AND column_position = 1) ix ON ix.column_name = c.column_name \
         WHERE c.owner = '{owner}' AND c.table_name = '{name}' \
         ORDER BY c.column_id"
    );
    Ok(conn
        .query_rows(&columns_sql)?
        .iter()
        .filter_map(|r| column_info(r))
        .collect())
}

/// One catalog row `(name, type, is_pk, is_indexed, nullable, precision, scale)`.
fn column_info(f: &[Option<String>]) -> Option<ColumnInfo> {
    let [name, data_type, pk, ix, nullable, precision, scale] = f else {
        return None;
    };
    let text = |c: &Option<String>| c.as_deref().unwrap_or_default().to_string();
    let precision = precision.as_deref().and_then(|p| p.parse::<u32>().ok());
    let scale = scale.as_deref().and_then(|s| s.parse::<i32>().ok());
    let is_pk = pk.as_deref() == Some("1");
    Some(ColumnInfo {
        name: name.clone()?,
        data_type: catalog_type(&text(data_type), precision, scale),
        is_primary_key: is_pk,
        is_indexed: is_pk || ix.as_deref() == Some("1"),
        is_nullable: nullable.as_deref() == Some("Y"),
        numeric_precision: precision,
        numeric_scale: scale.and_then(|s| u32::try_from(s).ok()),
    })
}

/// An Oracle catalog type in the vocabulary init's classifiers read: an integer
/// NUMBER(p<=18,0) is `bigint` (range-chunkable), a bare, INTEGER (precision NULL, scale 0) or wider integer NUMBER is
/// `number` (keysettable, never range-chunked), a scaled NUMBER is `numeric`, and DATE — which carries
/// the time to the second — is `datetime`, not the day-coarse `date`.
fn catalog_type(data_type: &str, precision: Option<u32>, scale: Option<i32>) -> String {
    match (data_type, precision, scale) {
        ("NUMBER", Some(p), Some(0)) if p <= 18 => "bigint".into(),
        ("NUMBER", None, None) | ("NUMBER", _, Some(0)) => "number".into(),
        ("NUMBER", _, _) => "numeric".into(),
        ("DATE", _, _) => "datetime".into(),
        ("BINARY_FLOAT", _, _) => "real".into(),
        ("BINARY_DOUBLE", _, _) => "double".into(),
        (other, _, _) => other.to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_types_map_onto_the_classifier_vocabulary() {
        assert_eq!(catalog_type("NUMBER", Some(10), Some(0)), "bigint");
        assert_eq!(catalog_type("NUMBER", Some(19), Some(0)), "number");
        assert_eq!(catalog_type("NUMBER", None, None), "number");
        assert_eq!(catalog_type("NUMBER", None, Some(0)), "number", "INTEGER");
        assert_eq!(catalog_type("NUMBER", Some(12), Some(2)), "numeric");
        assert_eq!(catalog_type("DATE", None, None), "datetime");
        assert_eq!(catalog_type("TIMESTAMP(6)", None, Some(6)), "timestamp(6)");
        assert_eq!(catalog_type("VARCHAR2", None, None), "varchar2");
        assert!(!super::super::is_coarse_stamp_type(&catalog_type(
            "DATE", None, None
        )));
    }

    #[test]
    fn a_never_analyzed_table_is_counted_up_to_a_cap_and_marked_unverified_there() {
        use crate::init::density::EstimateMethod;
        let below = unanalyzed_estimate(150_000);
        assert_eq!(
            (below.rows, below.method),
            (150_000, EstimateMethod::Counted)
        );
        let at = unanalyzed_estimate(UNANALYZED_COUNT_CAP);
        assert_eq!(
            (at.rows, at.method),
            (UNANALYZED_COUNT_CAP, EstimateMethod::Unverified)
        );
        assert_eq!(
            capped_count_sql("RIVET", "Big\"T"),
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"RIVET\".\"Big\"\"T\" WHERE ROWNUM <= 1000000)"
        );
    }

    #[test]
    fn a_catalog_row_parses_every_field() {
        let row = |v: [&str; 7]| -> Vec<Option<String>> {
            v.iter()
                .map(|c| (!c.is_empty()).then(|| c.to_string()))
                .collect()
        };
        let id = column_info(&row(["ID", "NUMBER", "1", "1", "N", "10", "0"])).unwrap();
        assert!(id.is_primary_key && id.is_indexed && !id.is_nullable);
        assert_eq!(id.data_type, "bigint");
        let note = column_info(&row(["NOTE", "VARCHAR2", "0", "0", "Y", "", ""])).unwrap();
        assert!(note.is_nullable && !note.is_indexed && !note.is_primary_key);
        let ts = column_info(&row(["UPDATED_AT", "TIMESTAMP(6)", "0", "1", "N", "", "6"])).unwrap();
        assert!(ts.is_indexed && !ts.is_primary_key);
        assert_eq!(ts.data_type, "timestamp(6)");
        assert!(column_info(&row(["X", "NUMBER", "0", "0", "Y", "", ""])[..6]).is_none());
    }

    #[test]
    fn an_explicit_schema_folds_and_placeholders_mean_the_current_one() {
        assert_eq!(explicit_owner(Some(" rivet ")).as_deref(), Some("RIVET"));
        assert_eq!(explicit_owner(Some("\"Mixed\"")).as_deref(), Some("Mixed"));
        assert_eq!(explicit_owner(Some("public")), None);
        assert_eq!(explicit_owner(Some("  ")), None);
        assert_eq!(explicit_owner(None), None);
    }

    #[test]
    fn a_catalog_literal_doubles_its_quotes() {
        assert_eq!(lit("O'NEIL"), "O''NEIL");
    }
}
