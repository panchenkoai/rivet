use postgres::Client;

use crate::config::TlsConfig;
use crate::error::Result;

use super::{ColumnInfo, TableInfo};

/// Open the one client shared across the whole init run (`list_tables` plus
/// every per-table `introspect` — no per-table reconnect).
///
/// `init` runs before any YAML `tls:` block exists (it *generates* the
/// config), so the transport-security policy comes from the URL's `sslmode`
/// parameter; the connection itself goes through the same
/// [`crate::source::postgres::connect_client`] path as doctor/check/run.
pub(super) fn connect(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
    // An explicit `--tls` flag WINS over the URL's `sslmode` — the flag is the
    // operator's direct statement, the URL parameter an inherited convention.
    let from_url;
    let tls = match tls {
        Some(t) => Some(t),
        None => {
            from_url = crate::source::url_tls(url).1;
            from_url.as_ref()
        }
    };
    crate::source::postgres::connect_client(url, tls)
}

/// Tables and views in a PostgreSQL schema (`information_schema`).
pub(super) fn list_tables(client: &mut Client, schema: &str) -> Result<Vec<String>> {
    let rows = client.query(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type IN ('BASE TABLE', 'VIEW')
         ORDER BY table_name",
        &[&schema],
    )?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Columns leading a valid, non-partial btree index — the only ones a range window can seek on.
const LEADING_BTREE_KEY_SQL: &str = "SELECT a.attname
     FROM pg_index i
     JOIN pg_class ic ON ic.oid = i.indexrelid
     JOIN pg_am am ON am.oid = ic.relam
     JOIN pg_attribute a ON a.attrelid = i.indrelid
         AND a.attnum = i.indkey[0]
     WHERE i.indrelid = to_regclass(quote_ident($1) || '.' || quote_ident($2))
       AND i.indrelid IS NOT NULL
       AND am.amname = 'btree'
       AND i.indisvalid AND i.indisready
       AND i.indpred IS NULL";

pub(super) fn introspect(client: &mut Client, schema: &str, table: &str) -> Result<TableInfo> {
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

    // Physical size (heap + indexes); None for views and when privileges are missing.
    let total_bytes: Option<i64> = client
        .query_opt(
            "SELECT pg_total_relation_size(c.oid)::bigint
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = $2 AND c.relkind IN ('r','p','m')",
            &[&table, &schema],
        )
        .ok()
        .flatten()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .filter(|v| *v > 0);

    // Primary key columns
    // to_regclass() returns NULL (no error) when the table disappears between
    // list_tables and introspect — possible under concurrent test table drops.
    let pk_rows = client.query(
        "SELECT a.attname
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = to_regclass(quote_ident($1) || '.' || quote_ident($2))
           AND i.indrelid IS NOT NULL
           AND i.indisprimary",
        &[&schema, &table],
    )?;
    let pk_cols: std::collections::HashSet<String> =
        pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    let indexed_rows = client.query(LEADING_BTREE_KEY_SQL, &[&schema, &table])?;
    let indexed_cols: std::collections::HashSet<String> =
        indexed_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    // Column metadata — including NULL-ability and numeric precision/scale for decimal columns.
    let col_rows = client.query(
        "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
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
            let is_nullable_str: String = row.get(2);
            let numeric_precision: Option<i32> = row.get(3);
            let numeric_scale: Option<i32> = row.get(4);
            let is_primary_key = pk_cols.contains(&name);
            let is_indexed = indexed_cols.contains(&name);
            ColumnInfo {
                is_indexed,
                name,
                data_type,
                is_primary_key,
                is_nullable: is_nullable_str.eq_ignore_ascii_case("YES"),
                numeric_precision: numeric_precision.map(|v| v as u32),
                numeric_scale: numeric_scale.map(|v| v as u32),
                ..Default::default()
            }
        })
        .collect();

    Ok(TableInfo {
        density: None,
        schema: schema.to_string(),
        table: table.to_string(),
        row_estimate,
        total_bytes,
        columns,
    })
}

/// First-run density probe (#148), PostgreSQL leg: `reltuples` after
/// autovacuum is usually within a few %, so the probe runs only on LARGE
/// tables (> 5M estimated), where a % error still moves absolute decisions
/// (parallel scaling, duration prediction). Same stratified method as MySQL;
/// best-effort (any error keeps the catalog figure, marked unverified only
/// when we attempted and failed — an un-probed small table keeps density=None,
/// i.e. "the catalog is trusted here by policy").
pub(super) fn density_probe(client: &mut Client, info: &mut super::TableInfo) {
    use super::density::*;

    const PG_PROBE_LINE: i64 = 5_000_000;
    let catalog = info.row_estimate;
    // #148 (roast 2026-08-09): a never-analyzed table has reltuples -1, clamped
    // to 0 at introspect — `0 < 5M` would skip the probe and trust 0 on a table
    // that may hold 100M rows (the just-restored/just-migrated moment users run
    // init). Probe when the figure is 0/unknown OR large; trust only a REAL
    // small figure (1..5M) where reltuples is a fresh ANALYZE.
    if (1..PG_PROBE_LINE).contains(&catalog) {
        return;
    }
    let q_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let rel = format!("{}.{}", q_ident(&info.schema), q_ident(&info.table));
    let Some(key) = info.best_chunk_column().map(str::to_string) else {
        // No integer-indexed chunk key to probe density with — a uuid/text PK is keysettable but
        // not density-probeable here. If the catalog figure is the clamped/unknown 0 (the
        // just-restored/just-migrated moment we probe FOR), trusting it scaffolds `mode: full` on
        // a possibly-huge table. Match MySQL's leg: an honest COUNT(*) when the catalog is
        // small/unknown (< 1M) — correct-but-slow beats fast-but-wrong; a genuinely large catalog
        // (>= 5M, the only other way to reach here) is trusted as-is (no scan of a huge table).
        let counted = (catalog < 1_000_000)
            .then(|| {
                client
                    .query_one(&format!("SELECT COUNT(*)::bigint FROM {rel}"), &[])
                    .ok()
                    .and_then(|r| r.try_get::<_, i64>(0).ok())
            })
            .flatten();
        info.density = Some(match counted {
            Some(n) => {
                info.row_estimate = n;
                DensityProbe {
                    rows: n,
                    density: 0.0,
                    method: EstimateMethod::Counted,
                    catalog_rows: catalog,
                    k: 0,
                    w: 0,
                }
            }
            None => DensityProbe {
                rows: catalog,
                density: 0.0,
                method: EstimateMethod::Unverified,
                catalog_rows: catalog,
                k: 0,
                w: 0,
            },
        });
        return;
    };
    let kq = q_ident(&key);
    let Ok(row) = client.query_one(
        &format!("SELECT MIN({kq})::bigint, MAX({kq})::bigint FROM {rel}"),
        &[],
    ) else {
        return;
    };
    let (Some(min), Some(max)): (Option<i64>, Option<i64>) = (row.get(0), row.get(1)) else {
        return;
    };
    let offsets = stratified_offsets(min, max, PROBE_K, PROBE_W);
    if offsets.is_empty() {
        return;
    }
    let mut counts = Vec::with_capacity(offsets.len());
    for off in &offsets {
        let hi = off.saturating_add(PROBE_W - 1);
        match client.query_one(
            &format!("SELECT COUNT(*) FROM {rel} WHERE {kq} BETWEEN {off} AND {hi}"),
            &[],
        ) {
            Ok(r) => counts.push(r.get::<_, i64>(0)),
            Err(_) => return,
        }
    }
    let sampled: i64 = counts.iter().sum();
    if !super::density::probe_trustworthy(sampled, offsets.len()) {
        // #148 sparse-key guard: too few sampled rows to trust the extrapolation.
        info.density = Some(DensityProbe {
            rows: catalog,
            density: 0.0,
            method: EstimateMethod::Unverified,
            catalog_rows: catalog,
            k: offsets.len(),
            w: PROBE_W,
        });
        return;
    }
    let (rows, density) = estimate_from_windows(min, max, &counts, PROBE_W);
    info.row_estimate = rows;
    info.density = Some(DensityProbe {
        rows,
        density,
        method: EstimateMethod::Probed,
        catalog_rows: catalog,
        k: offsets.len(),
        w: PROBE_W,
    });
}

#[cfg(test)]
mod tests {
    use super::LEADING_BTREE_KEY_SQL;

    /// H26: a non-leading, INCLUDE, hash/gin/brin, partial or invalid index position does not make a
    /// column range-seekable, so init must not call it indexed (preflight's leading-btree rule).
    #[test]
    fn indexed_means_leading_key_of_a_valid_full_btree() {
        let sql = LEADING_BTREE_KEY_SQL;
        assert!(sql.contains("a.attnum = i.indkey[0]"), "{sql}");
        assert!(!sql.contains("ANY(i.indkey)"), "{sql}");
        assert!(sql.contains("am.amname = 'btree'"), "{sql}");
        assert!(sql.contains("i.indisvalid AND i.indisready"), "{sql}");
        assert!(sql.contains("i.indpred IS NULL"), "{sql}");
    }
}
