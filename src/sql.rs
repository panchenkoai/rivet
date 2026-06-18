//! SQL identifier quoting helpers.
//!
//! Column and table names that come from user configuration must never be
//! interpolated raw into query strings — doing so opens a SQL injection vector
//! even when `start`/`end` bounds are typed integers.  Use `quote_ident` for
//! every identifier that is not a literal written by the developer.

use crate::config::SourceType;

/// Quote a SQL identifier (column or table name) for the target source dialect.
///
/// - **PostgreSQL** — double-quoted: `"column_name"`.  Internal `"` characters are
///   escaped by doubling (`"col""name"`).
/// - **MySQL** — backtick-quoted: `` `column_name` ``.  Internal backticks are
///   escaped by doubling (`` `col``name` ``).
///
/// # Example
/// ```
/// use rivet::config::SourceType;
/// // internal use only; not part of the public crate API
/// ```
pub(crate) fn quote_ident(source_type: SourceType, name: &str) -> String {
    match source_type {
        SourceType::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
        SourceType::Mysql => format!("`{}`", name.replace('`', "``")),
        // SQL Server bracket-quoting: `[col]`; internal `]` doubled (`[a]]b]`).
        SourceType::Mssql => format!("[{}]", name.replace(']', "]]")),
    }
}

/// Parse a `NaiveDate` out of a DB scalar (typically `min`/`max` of a
/// date/timestamp column returned by `query_scalar`).
///
/// Single source of truth for both the chunked date-range path
/// (`pipeline::chunked`) and value-based partitioning (`plan::partition`), which
/// live in different layers and so must share this through the `sql` leaf rather
/// than duplicate it.
///
/// Accepts, in order: `DATE` (`2023-01-01`), `DATETIME`
/// (`2023-01-01 14:32:00`), ISO-8601 with `T` and optional fractional seconds,
/// and finally a lenient leading-`YYYY-MM-DD` fallback that covers the
/// PostgreSQL `timestamptz` text form (`2023-01-01 14:32:00.123456+00`).
/// Returns `None` for an empty / unparseable value.
pub(crate) fn parse_date_flexible(s: &str) -> Option<chrono::NaiveDate> {
    use chrono::NaiveDate;
    let s = s.trim();
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
        .or_else(|| {
            // ISO 8601 with T, optional fractional seconds / offset.
            let base = s.split('.').next().unwrap_or(s);
            let base = base.split('+').next().unwrap_or(base);
            chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
        .or_else(|| {
            // Lenient fallback: any value whose first 10 chars are `YYYY-MM-DD`
            // (space-separated fractional/offset timestamptz, etc.).
            s.get(..10)
                .and_then(|head| NaiveDate::parse_from_str(head, "%Y-%m-%d").ok())
        })
}

/// If `base_query` is exactly `SELECT * FROM <ident>` (the `table:` YAML
/// shortcut form), return the table ident; otherwise `None`.
///
/// Lets query builders append a `WHERE`/aggregate directly to the table instead
/// of wrapping the base in a subquery — which keeps the PG numeric catalog-hint
/// parser working and the generated SQL clean. Acceptance is deliberately
/// strict: any trailing clause (WHERE, JOIN, comma, alias, semicolon) → `None`,
/// so a query the user thought was filtered is never rewritten.
///
/// Shared by `pipeline::chunked` (chunk/aggregate/count SQL), `preflight`, and
/// `plan::partition`, so it lives in this leaf rather than being duplicated.
pub(crate) fn strip_select_star_from(base_query: &str) -> Option<&str> {
    let trimmed = base_query.trim();
    let after = strip_prefix_ascii_ci(trimmed, "select")
        .map(str::trim_start)
        .and_then(|s| s.strip_prefix('*'))
        .map(str::trim_start)
        .and_then(|s| strip_prefix_ascii_ci(s, "from"))?;
    let rest = after.trim_start();
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
        .unwrap_or(rest.len());
    let ident = &rest[..end];
    let parts: Vec<&str> = ident.split('.').collect();
    if !(1..=2).contains(&parts.len()) {
        return None;
    }
    for p in &parts {
        let mut chars = p.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {
                if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    return None;
                }
            }
            _ => return None,
        }
    }
    // No trailing payload allowed (WHERE/JOIN/etc.).
    if !rest[end..].trim().is_empty() {
        return None;
    }
    Some(ident)
}

fn strip_prefix_ascii_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// `SELECT <agg>(<col>) FROM …` for `agg` in {`min`, `max`, `count`, …}, with
/// the `table:` fast path: a `SELECT * FROM <ident>` base aggregates the table
/// directly, anything else is wrapped as `… FROM (<base>) AS _rivet`.
///
/// Single source of truth for the chunked date/range detection
/// (`pipeline::chunked::detect`) and value-based partitioning
/// (`plan::partition`); `col` is quoted for the dialect internally.
pub(crate) fn aggregate_sql(
    source_type: SourceType,
    agg: &str,
    col: &str,
    base_query: &str,
) -> String {
    let q = quote_ident(source_type, col);
    match strip_select_star_from(base_query) {
        Some(table_ident) => format!("SELECT {agg}({q}) FROM {table_ident}"),
        None => format!("SELECT {agg}({q}) FROM ({base_query}) AS _rivet"),
    }
}

/// Scan-free row **estimate** for the chunk-sparsity diagnostic — reads catalog /
/// optimizer statistics, never a `COUNT(*)`. A full count on a large production
/// table is exactly the source-harm rivet exists to avoid (we measured ~12 min of
/// silent `COUNT(*)` before the first chunk on a 484M-row table), and the sparsity
/// heuristic only needs an order-of-magnitude figure.
///
/// `None` means "no trustworthy scan-free count for this dialect" — the caller
/// then logs boundaries without a density line rather than fall back to a scan.
/// Defined only for the `table:` shortcut: `table_ident` is the clean, validated
/// `[schema.]table` identifier from [`strip_select_star_from`] (ASCII
/// alphanumeric/underscore, ≤1 dot), so it is safe to interpolate. The caller also
/// treats a NULL / error / non-positive query result as "unknown" and skips.
pub(crate) fn row_estimate_sql(source_type: SourceType, table_ident: &str) -> Option<String> {
    match source_type {
        // `reltuples` is the planner's live estimate (kept fresh by ANALYZE /
        // autovacuum, usually within a few %); `-1` (never analysed) and any
        // negative are clamped to 0 so the caller's `> 0` guard skips it.
        SourceType::Postgres => Some(format!(
            "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = '{table_ident}'::regclass"
        )),
        // MySQL has NO reliable scan-free row count. `information_schema.TABLE_ROWS`
        // — and the optimizer's `EXPLAIN` estimate, which shares the same InnoDB
        // random-index-dive statistics — routinely err by 30-50%+ and read 0 on a
        // freshly-loaded table. Rather than drive a precise-looking density % off
        // an untrustworthy figure, skip the sparsity diagnostic on MySQL entirely;
        // the chunk boundaries come from min/max regardless. (An exact count would
        // need a full `COUNT(*)` scan — the very source-harm this function avoids.)
        SourceType::Mysql => None,
        // `sys.dm_db_partition_stats` is a maintained running row count (effectively
        // exact), not a sampled estimate — the same fast probe the SQL Server
        // preflight uses, over the heap/clustered index (index_id 0/1).
        SourceType::Mssql => Some(format!(
            "SELECT SUM(p.row_count) FROM sys.dm_db_partition_stats p \
             WHERE p.object_id = OBJECT_ID('{table_ident}') AND p.index_id IN (0,1)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_plain_identifier() {
        assert_eq!(quote_ident(SourceType::Postgres, "id"), "\"id\"");
        assert_eq!(
            quote_ident(SourceType::Postgres, "created_at"),
            "\"created_at\""
        );
    }

    #[test]
    fn postgres_escapes_internal_double_quotes() {
        assert_eq!(
            quote_ident(SourceType::Postgres, "col\"name"),
            "\"col\"\"name\""
        );
    }

    #[test]
    fn mysql_plain_identifier() {
        assert_eq!(quote_ident(SourceType::Mysql, "id"), "`id`");
        assert_eq!(quote_ident(SourceType::Mysql, "created_at"), "`created_at`");
    }

    #[test]
    fn mysql_escapes_internal_backticks() {
        assert_eq!(quote_ident(SourceType::Mysql, "col`name"), "`col``name`");
    }

    fn d(s: &str) -> chrono::NaiveDate {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn parse_date_flexible_handles_db_scalar_forms() {
        assert_eq!(parse_date_flexible("2023-06-15"), Some(d("2023-06-15")));
        assert_eq!(
            parse_date_flexible("2023-06-15 14:32:00"),
            Some(d("2023-06-15"))
        );
        assert_eq!(
            parse_date_flexible("2023-06-15T14:32:00"),
            Some(d("2023-06-15"))
        );
        assert_eq!(
            parse_date_flexible("2023-06-15T14:32:00.123456"),
            Some(d("2023-06-15"))
        );
        // PostgreSQL timestamptz text form — covered by the lenient fallback.
        assert_eq!(
            parse_date_flexible("2024-12-30 00:00:00.123456+00"),
            Some(d("2024-12-30"))
        );
    }

    #[test]
    fn parse_date_flexible_rejects_non_dates() {
        assert!(parse_date_flexible("").is_none());
        assert!(parse_date_flexible("not-a-date").is_none());
        assert!(parse_date_flexible("12345").is_none());
    }

    #[test]
    fn strip_select_star_from_accepts_simple_table_forms() {
        assert_eq!(
            strip_select_star_from("SELECT * FROM events"),
            Some("events")
        );
        assert_eq!(
            strip_select_star_from("select *  from  public.orders"),
            Some("public.orders")
        );
    }

    #[test]
    fn strip_select_star_from_rejects_anything_with_a_clause() {
        assert!(strip_select_star_from("SELECT * FROM events WHERE id > 1").is_none());
        assert!(strip_select_star_from("SELECT id FROM events").is_none());
        assert!(strip_select_star_from("SELECT * FROM a JOIN b").is_none());
        assert!(strip_select_star_from("SELECT * FROM events;").is_none());
        assert!(strip_select_star_from("SELECT * FROM a.b.c").is_none());
    }

    #[test]
    fn aggregate_sql_fast_path_on_table_shortcut() {
        assert_eq!(
            aggregate_sql(
                SourceType::Postgres,
                "min",
                "created_at",
                "SELECT * FROM events"
            ),
            "SELECT min(\"created_at\") FROM events"
        );
    }

    #[test]
    fn aggregate_sql_wraps_a_real_query() {
        assert_eq!(
            aggregate_sql(
                SourceType::Postgres,
                "max",
                "created_at",
                "SELECT id, created_at FROM events WHERE x"
            ),
            "SELECT max(\"created_at\") FROM (SELECT id, created_at FROM events WHERE x) AS _rivet"
        );
        // dialect quoting flows through.
        assert!(
            aggregate_sql(SourceType::Mysql, "min", "d", "SELECT d FROM t WHERE 1")
                .contains("min(`d`)")
        );
    }

    #[test]
    fn row_estimate_sql_is_scan_free_or_skipped_per_dialect() {
        // Postgres: planner stats (reltuples), never COUNT(*).
        let pg = row_estimate_sql(SourceType::Postgres, "warranty").expect("PG has an estimate");
        assert!(pg.contains("reltuples") && pg.contains("pg_class"), "{pg}");
        assert!(!pg.contains("COUNT"), "estimate must not scan: {pg}");
        // SQL Server: partition stats (maintained count), never COUNT(*).
        let ms =
            row_estimate_sql(SourceType::Mssql, "dbo.warranty").expect("MSSQL has an estimate");
        assert!(
            ms.contains("dm_db_partition_stats") && ms.contains("OBJECT_ID('dbo.warranty')"),
            "{ms}"
        );
        assert!(!ms.contains("COUNT"), "estimate must not scan: {ms}");
        // MySQL: no trustworthy scan-free count (TABLE_ROWS/EXPLAIN ±30-50%) →
        // skipped (None), never a misleading density figure.
        assert!(row_estimate_sql(SourceType::Mysql, "warranty").is_none());
        assert!(row_estimate_sql(SourceType::Mysql, "shop.warranty").is_none());
    }
}
