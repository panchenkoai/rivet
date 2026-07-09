//! Dialect SQL string building — the outbound half of source interaction.
//!
//! Builds and recognises the small SQL strings rivet sends to a source:
//! identifier quoting ([`quote_ident`]), the `SELECT * FROM <ident>` table-form
//! recogniser ([`strip_select_star_from`]), aggregate probes ([`aggregate_sql`]),
//! and scan-free row estimates ([`row_estimate_sql`]). Parsing the scalars read
//! back lives in the inbound mirror, [`crate::scalar`].
//!
//! Column and table names that come from user configuration must never be
//! interpolated raw into query strings — doing so opens a SQL injection vector
//! even when `start`/`end` bounds are typed integers.  Use [`quote_ident`] for
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
        // MongoDB has no SQL identifier dialect. Every SQL builder is guarded by
        // full-mode-only validation, so this arm is unreachable — panic loudly
        // if a future path ever routes a Mongo source through SQL.
        SourceType::Mongo => unreachable!(
            "quote_ident: MongoDB has no SQL dialect (guarded by full-mode-only validation)"
        ),
    }
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
    parse_bare_table_ident(after.trim_start())
}

/// Like [`strip_select_star_from`] but accepts an explicit **plain column-list**
/// projection (`SELECT id, title, … FROM <ident>`) — the shape `rivet init`
/// scaffolds — not just `*`.
///
/// For a chunk-boundary probe (`min`/`max`/null-count/row-count of the chunk
/// column) the projection is irrelevant: with **no `WHERE`** the row set is the
/// whole table, so the column's extremes — and the row count — are identical
/// whether read through the wide projection or straight off the table. Reading
/// off the table lets the planner use the index instead of materialising every
/// wide row into a temp-file spill (the wrap measured ~3.2 GB of temp_files on
/// an 8.6 GB table).
///
/// Acceptance stays strict so a filtered / derived / joined / de-duplicated
/// query is never rewritten to the bare table: the projection must be bare
/// column references only — any `(` (function / subquery), quote, `DISTINCT`,
/// or other token → `None`; any trailing clause after the table → `None`. On
/// `None` the caller wraps exactly as before.
pub(crate) fn strip_simple_projection_from(base_query: &str) -> Option<&str> {
    let trimmed = base_query.trim();
    let after_select = strip_prefix_ascii_ci(trimmed, "select").map(str::trim_start)?;
    // Split on the first whitespace-bounded `from`. The projection is validated
    // below to hold no `(`/quote, so that first `from` is necessarily the
    // table's FROM (a subquery's `from` would sit behind a `(`; a column named
    // `from` would need quoting) — never one inside a literal or derived table.
    let from_at = find_from_keyword(after_select)?;
    let projection = after_select[..from_at].trim();
    if !is_plain_column_list(projection) {
        return None;
    }
    parse_bare_table_ident(after_select[from_at + "from".len()..].trim_start())
}

/// Byte offset of the first whitespace/boundary-delimited `from` keyword
/// (ASCII case-insensitive), or `None`. Only meaningful once the preceding text
/// is known paren/quote-free (see [`strip_simple_projection_from`]).
fn find_from_keyword(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        if s[i..i + 4].eq_ignore_ascii_case("from")
            && (i == 0 || bytes[i - 1].is_ascii_whitespace())
            && (i + 4 == bytes.len() || bytes[i + 4].is_ascii_whitespace())
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// A non-empty list of bare column references: only ASCII alphanumerics, `_`,
/// `.`, `,`, `*`, and whitespace — and not a leading `DISTINCT` (which would
/// change the row count / set, breaking the dense-chunk count and the
/// whole-table equivalence the fast path relies on). Functions (`(`), quoted
/// idents / string literals, and any other punctuation → `false`.
fn is_plain_column_list(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    if let Some(rest) = strip_prefix_ascii_ci(s, "distinct")
        && rest.starts_with(|c: char| c.is_whitespace())
    {
        return false;
    }
    s.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ',' | '*' | ' ' | '\t' | '\n' | '\r')
    })
}

/// Validate `after_from` is a bare `[schema.]table` identifier with no trailing
/// clause (WHERE/JOIN/comma/alias/semicolon), returning it. Shared by the `*`
/// and column-list table-form recognisers so the strictness lives in one place.
fn parse_bare_table_ident(after_from: &str) -> Option<&str> {
    let rest = after_from.trim_start();
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
    match strip_simple_projection_from(base_query) {
        Some(table_ident) => format!("SELECT {agg}({q}) FROM {table_ident}"),
        None => format!("SELECT {agg}({q}) FROM ({base_query}) AS _rivet"),
    }
}

/// Presence probe for NULL chunk keys: one row iff `<col>` has any NULL. Replaces
/// the old `COUNT(*) - COUNT(col)` full count in `bail_if_null_keyed` — the DB
/// stops at the first NULL, and for a NOT NULL column the planner returns nothing
/// without scanning, so the cold full-index scan that count forced (the seconds
/// of upfront idle on a large chunked export) is gone. A nullable column with
/// zero NULLs still yields no row, preserving the "detect on live data" intent.
///
/// Same bare-table-vs-wrap shape as [`aggregate_sql`]; `col` is dialect-quoted
/// here. MSSQL uses `TOP 1` — the keyset `OFFSET/FETCH` limit form needs an
/// `ORDER BY` this presence probe has no use for.
pub(crate) fn null_key_probe_sql(source_type: SourceType, col: &str, base_query: &str) -> String {
    let from = match strip_simple_projection_from(base_query) {
        Some(table_ident) => table_ident.to_string(),
        None => format!("({base_query}) AS _rivet_nullprobe"),
    };
    let q = quote_ident(source_type, col);
    match source_type {
        SourceType::Mssql => format!("SELECT TOP 1 1 FROM {from} WHERE {q} IS NULL"),
        SourceType::Postgres | SourceType::Mysql => {
            format!("SELECT 1 FROM {from} WHERE {q} IS NULL LIMIT 1")
        }
        // Unreachable: the null-key probe is a chunked-mode concern, and chunked
        // mode is rejected for MongoDB at config validation.
        SourceType::Mongo => unreachable!(
            "null_key_probe_sql: MongoDB has no SQL (chunked mode is rejected for Mongo)"
        ),
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
        // No scan-free row estimate for MongoDB in this SQL helper — the
        // chunk-sparsity diagnostic is a SQL/chunked concern Mongo never reaches.
        // `None` = "unknown", which the caller already tolerates.
        SourceType::Mongo => None,
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
    fn strip_simple_projection_accepts_plain_column_lists() {
        // The shape `rivet init` scaffolds: an explicit column list off a bare
        // table — the chunk-column probes can read straight off the table.
        assert_eq!(
            strip_simple_projection_from("SELECT id, title, body FROM content_items"),
            Some("content_items")
        );
        // Star still works (a superset of strip_select_star_from).
        assert_eq!(
            strip_simple_projection_from("SELECT * FROM events"),
            Some("events")
        );
        // Schema-qualified table, dotted columns, mixed case / extra whitespace.
        assert_eq!(
            strip_simple_projection_from("select a.x, a.y  from  public.users"),
            Some("public.users")
        );
        // Folded multi-line (YAML `>` block) collapses to one line of columns.
        assert_eq!(
            strip_simple_projection_from("SELECT id, a, b, c, d FROM content_items\n"),
            Some("content_items")
        );
    }

    #[test]
    fn strip_simple_projection_rejects_anything_that_changes_the_row_set() {
        // WHERE / JOIN / GROUP / trailing clause / `;` — the bare table no
        // longer matches the query's rows, so the caller must wrap (None).
        assert!(strip_simple_projection_from("SELECT id FROM t WHERE id > 1").is_none());
        assert!(
            strip_simple_projection_from("SELECT a.id FROM t a JOIN u ON a.id = u.id").is_none()
        );
        assert!(strip_simple_projection_from("SELECT id FROM t GROUP BY id").is_none());
        assert!(strip_simple_projection_from("SELECT id FROM t;").is_none());
        // DISTINCT changes the row count/set (would break the dense-chunk COUNT).
        assert!(strip_simple_projection_from("SELECT DISTINCT id FROM t").is_none());
        // A function / expression in the projection — can't reason syntactically.
        assert!(strip_simple_projection_from("SELECT count(*) FROM t").is_none());
        assert!(strip_simple_projection_from("SELECT lower(name) FROM t").is_none());
        // A subquery / derived table: the first `from` is the inner one.
        assert!(strip_simple_projection_from("SELECT id FROM (SELECT id FROM y) z").is_none());
        // A string literal in the projection could hide a `from`.
        assert!(strip_simple_projection_from("SELECT 'from x' FROM t").is_none());
        // Three-part identifier is not `[schema.]table`.
        assert!(strip_simple_projection_from("SELECT id FROM a.b.c").is_none());
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
    fn null_key_probe_sql_is_a_presence_probe_not_a_count() {
        // Bare table / simple projection → probe the table directly (PG `LIMIT 1`).
        assert_eq!(
            null_key_probe_sql(SourceType::Postgres, "id", "SELECT id, title FROM orders"),
            "SELECT 1 FROM orders WHERE \"id\" IS NULL LIMIT 1"
        );
        // MSSQL has no LIMIT in this position → TOP 1.
        assert_eq!(
            null_key_probe_sql(SourceType::Mssql, "id", "SELECT * FROM orders"),
            "SELECT TOP 1 1 FROM orders WHERE [id] IS NULL"
        );
        // A filtered query can't be reasoned about → wrap, exactly as before.
        assert_eq!(
            null_key_probe_sql(
                SourceType::Postgres,
                "id",
                "SELECT id FROM orders WHERE x > 1"
            ),
            "SELECT 1 FROM (SELECT id FROM orders WHERE x > 1) AS _rivet_nullprobe \
             WHERE \"id\" IS NULL LIMIT 1"
        );
        // Never a COUNT — that full scan was the whole problem.
        assert!(!null_key_probe_sql(SourceType::Mysql, "k", "SELECT * FROM t").contains("COUNT"));
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
