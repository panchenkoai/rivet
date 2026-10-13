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
        // Oracle: double-quoted and therefore case-SENSITIVE — the name is used as the
        // catalog stores it (unquoted DDL names fold to upper case).
        SourceType::Oracle => format!("\"{}\"", name.replace('"', "\"\"")),
        // MongoDB has no SQL identifier dialect. Every SQL builder is guarded by
        // full-mode-only validation, so this arm is unreachable — panic loudly
        // if a future path ever routes a Mongo source through SQL.
        SourceType::Mongo => unreachable!(
            "quote_ident: MongoDB has no SQL dialect (guarded by full-mode-only validation)"
        ),
    }
}

/// A derived table's alias as referenced (`_rivet.col`); Oracle needs it quoted,
/// since an unquoted identifier may not start with `_`.
pub(crate) fn alias(source_type: SourceType, name: &str) -> String {
    match source_type {
        SourceType::Oracle => format!("\"{name}\""),
        _ => name.to_string(),
    }
}

/// A derived table's alias as declared after `FROM (...)`: `AS name`, or the bare
/// quoted name on Oracle, which rejects `AS` before a table alias.
pub(crate) fn derived(source_type: SourceType, name: &str) -> String {
    match source_type {
        SourceType::Oracle => alias(source_type, name),
        _ => format!("AS {name}"),
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
/// Acceptance stays strict so a filtered / derived / joined / de-duplicated /
/// renamed query is never rewritten to the bare table: the projection must be
/// bare column references only — any `(` (function / subquery), quote, alias,
/// `DISTINCT`, or other token → `None`; any trailing clause after the table →
/// `None`. On `None` the caller wraps exactly as before.
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
/// is known paren/quote-free (see [`strip_simple_projection_from`]). The keyword
/// is matched on bytes, so the offset and the byte after the keyword are char boundaries.
fn find_from_keyword(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        if bytes[i..i + 4].eq_ignore_ascii_case(b"from")
            && (i == 0 || bytes[i - 1].is_ascii_whitespace())
            && (i + 4 == bytes.len() || bytes[i + 4].is_ascii_whitespace())
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// A comma-separated list whose every item is ONE bare column reference (`col`, `t.col`, `*`); a second token in an item is an alias or a row limiter (`a AS b`, `a b`, `DISTINCT a`, `TOP 5 a`), and the query is then not the table.
fn is_plain_column_list(s: &str) -> bool {
    s.split(',').all(|item| {
        let item = item.trim();
        !item.is_empty()
            && item
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '*'))
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

/// `s` without its leading `prefix` (ASCII case-insensitive); `None` when `s` does not start with it.
fn strip_prefix_ascii_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let (head, rest) = s.split_at_checked(prefix.len())?;
    head.eq_ignore_ascii_case(prefix).then_some(rest)
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
        Some(table_ident) => format!("SELECT {agg}({q}) AS rivet_agg FROM {table_ident}"),
        None => format!(
            "SELECT {agg}({q}) AS rivet_agg FROM ({base_query}) {}",
            derived(source_type, "_rivet")
        ),
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
        None => format!(
            "({base_query}) {}",
            derived(source_type, "_rivet_nullprobe")
        ),
    };
    let q = quote_ident(source_type, col);
    match source_type {
        SourceType::Mssql => format!("SELECT TOP 1 1 FROM {from} WHERE {q} IS NULL"),
        SourceType::Postgres | SourceType::Mysql => {
            format!("SELECT 1 FROM {from} WHERE {q} IS NULL LIMIT 1")
        }
        SourceType::Oracle => {
            format!("SELECT 1 FROM {from} WHERE {q} IS NULL FETCH FIRST 1 ROWS ONLY")
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
        // `sys.partitions.rows` is a maintained running row count (effectively
        // exact), not a sampled estimate, readable without VIEW DATABASE STATE — the same probe the SQL Server
        // preflight uses, over the heap/clustered index (index_id 0/1).
        SourceType::Mssql => Some(format!(
            "SELECT SUM(p.rows) FROM sys.partitions p \
             WHERE p.object_id = OBJECT_ID('{table_ident}') AND p.index_id IN (0,1)"
        )),
        // No scan-free row estimate for MongoDB in this SQL helper — the
        // chunk-sparsity diagnostic is a SQL/chunked concern Mongo never reaches.
        // `None` = "unknown", which the caller already tolerates.
        // `ALL_TABLES.NUM_ROWS` is the optimizer statistic (NULL until DBMS_STATS
        // has run — the caller's `> 0` guard then skips the density line). The
        // `table:` shortcut ident is unquoted, so Oracle resolved it upper-cased.
        SourceType::Oracle => {
            let (owner, table) = oracle_catalog_preds(table_ident);
            Some(format!(
                "SELECT num_rows FROM all_tables WHERE owner = {owner} AND table_name = {table}"
            ))
        }
        SourceType::Mongo => None,
    }
}

/// The catalog name Oracle resolves one identifier part to: unquoted folds upper-case, double-quoted stays verbatim.
pub(crate) fn oracle_catalog_name(part: &str) -> String {
    match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None => part.to_uppercase(),
    }
}

/// `(owner, table)` catalog literals for an Oracle `[owner.]table`: an unquoted
/// part folds upper-case as Oracle resolves it, a double-quoted part stays verbatim.
pub(crate) fn oracle_catalog_preds(qualified: &str) -> (String, String) {
    let lit = |part: &str| format!("'{}'", oracle_catalog_name(part).replace('\'', "''"));
    match qualified.rsplit_once('.') {
        Some((owner, table)) => (lit(owner), lit(table)),
        None => (
            "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')".to_string(),
            lit(qualified),
        ),
    }
}

/// Whether `c` may be part of an unquoted identifier.
fn ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'_' | b'$' | b'#') || c >= 0x80
}

/// The byte that closes the quote `c` opens, if it opens one.
fn quote_closer(c: u8) -> Option<u8> {
    match c {
        b'\'' | b'"' | b'`' => Some(c),
        b'[' => Some(b']'),
        _ => None,
    }
}

/// Byte length of the comment that starts `rest`: a `--` line up to its line break, or a `/* */` block with the blocks nested in it (an unclosed one runs to the end); 0 when `rest` starts no comment.
fn comment_len(rest: &[u8]) -> usize {
    if rest.starts_with(b"--") {
        return rest.iter().position(|&c| c == b'\n').unwrap_or(rest.len());
    }
    if !rest.starts_with(b"/*") {
        return 0;
    }
    let (mut i, mut open) = (2, 1usize);
    while i < rest.len() && open > 0 {
        if rest[i..].starts_with(b"*/") {
            open -= 1;
            i += 2;
        } else if rest[i..].starts_with(b"/*") {
            open += 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// `rest` past its leading whitespace and comments.
fn skip_blank(rest: &str) -> &str {
    let mut rest = rest.trim_start();
    loop {
        match comment_len(rest.as_bytes()) {
            0 => return rest,
            n => rest = rest.get(n..).unwrap_or("").trim_start(),
        }
    }
}

/// Byte offset of the first `keyword` of `query` outside quotes, comments and parentheses.
fn outer_keyword(query: &str, keyword: &str) -> Option<usize> {
    let (b, kw) = (query.as_bytes(), keyword.as_bytes());
    let (mut depth, mut quote, mut i) = (0usize, None, 0);
    while i < b.len() {
        let c = b[i];
        if let Some(end) = quote {
            quote = Some(end).filter(|&e| e != c);
            i += 1;
            continue;
        }
        let comment = comment_len(&b[i..]);
        if comment > 0 {
            i += comment;
            continue;
        }
        quote = quote_closer(c);
        match c {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && keyword_at(&b[i..], kw) && (i == 0 || !ident_byte(b[i - 1])) {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Whether `rest` starts with `keyword`, in any letter case, as a whole word.
fn keyword_at(rest: &[u8], keyword: &[u8]) -> bool {
    rest.len() >= keyword.len()
        && rest[..keyword.len()].eq_ignore_ascii_case(keyword)
        && rest.get(keyword.len()).is_none_or(|&n| !ident_byte(n))
}

/// One dot-separated part of a relation name, and whether the query quoted it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamePart {
    pub(crate) name: String,
    pub(crate) quoted: bool,
}

/// The name part `rest` starts with and what follows it: a quoted one (`"…"`, `[…]`, `` `…` ``, a doubled closer standing for itself) or a bare run of identifier bytes that does not start with a digit.
fn name_part(rest: &str) -> Option<(NamePart, &str)> {
    let first = *rest.as_bytes().first()?;
    let Some(close) = quote_closer(first).filter(|_| first != b'\'') else {
        let len = rest.bytes().take_while(|&c| ident_byte(c)).count();
        let (name, tail) = rest.split_at_checked(len)?;
        let part = NamePart {
            name: name.to_string(),
            quoted: false,
        };
        return (len > 0 && !first.is_ascii_digit()).then_some((part, tail));
    };
    let close = char::from(close);
    let mut name = String::new();
    let mut chars = rest.get(1..)?.chars();
    while let Some(ch) = chars.next() {
        if ch != close {
            name.push(ch);
        } else if chars.as_str().starts_with(close) {
            chars.next();
            name.push(close);
        } else {
            let part = NamePart { name, quoted: true };
            return (!part.name.is_empty()).then_some((part, chars.as_str()));
        }
    }
    None
}

/// Whether a join, a comma list or a stray dotted name follows a relation.
fn joins_another(rest: &str) -> bool {
    const JOINS: [&str; 8] = [
        "join", "inner", "left", "right", "full", "outer", "cross", "natural",
    ];
    rest.starts_with([',', '.'])
        || JOINS
            .iter()
            .any(|kw| keyword_at(rest.as_bytes(), kw.as_bytes()))
}

/// Whether the clause after a relation starts here, so no alias stands between them.
fn ends_relation(rest: &str) -> bool {
    const CLAUSES: [&str; 13] = [
        "where",
        "group",
        "having",
        "order",
        "limit",
        "offset",
        "fetch",
        "union",
        "intersect",
        "except",
        "window",
        "for",
        "option",
    ];
    rest.is_empty()
        || rest.starts_with(';')
        || CLAUSES
            .iter()
            .any(|kw| keyword_at(rest.as_bytes(), kw.as_bytes()))
}

/// The relation the outermost `FROM` of `query` names first, part by part, and the text after that name; comments count as whitespace, and `only` skips PostgreSQL's `ONLY` before the name.
fn outer_name(query: &str, only: bool) -> Option<(Vec<NamePart>, &str)> {
    let from = outer_keyword(query, "from")?;
    let mut rest = skip_blank(query.get(from + 4..)?);
    if only && keyword_at(rest.as_bytes(), b"only") {
        rest = skip_blank(rest.get(4..)?);
    }
    let mut parts = Vec::new();
    loop {
        let (part, tail) = name_part(rest)?;
        parts.push(part);
        rest = skip_blank(tail);
        match rest.strip_prefix('.') {
            Some(after) => rest = skip_blank(after),
            None => return Some((parts, rest)),
        }
    }
}

/// The one relation the outermost `FROM` of `query` reads, part by part (`schema`, `table`); `None` when there is no such `FROM`, or it reads a subquery, a function call, a join or a comma list.
pub(crate) fn single_relation(query: &str, only: bool) -> Option<Vec<NamePart>> {
    let (parts, mut rest) = outer_name(query, only)?;
    if !ends_relation(rest) && !joins_another(rest) {
        if keyword_at(rest.as_bytes(), b"as") {
            rest = skip_blank(rest.get(2..)?);
        }
        rest = skip_blank(name_part(rest)?.1);
    }
    (!joins_another(rest)).then_some(parts)
}

/// The relation the outermost `FROM` of `query` names first, for a catalog probe of that table: `None` when a join or a comma list follows the name itself. What follows an alias is not read, so the driving table of `FROM t x CROSS APPLY f(x.id)` is still named.
pub(crate) fn first_relation(query: &str) -> Option<Vec<NamePart>> {
    let (parts, rest) = outer_name(query, false)?;
    (!joins_another(rest)).then_some(parts)
}

/// The rows `query` selects, as text to compare: the query without its outermost projection and with `?` for the relation its outermost `FROM` names (the stream holds the name), lower-cased and with whitespace folded outside quotes; the whole query when it has no outermost `SELECT ... FROM`.
pub(crate) fn row_set(query: &str) -> String {
    let q = query.trim_matches(|c: char| c.is_whitespace() || c == ';');
    let rows = match (outer_keyword(q, "select"), outer_keyword(q, "from")) {
        (Some(select), Some(from)) if select < from => match outer_relation(q) {
            Some((_, at)) => format!("{}{}?{}", &q[..select], &q[from..at.start], &q[at.end..]),
            None => format!("{}{}", &q[..select], &q[from..]),
        },
        _ => q.to_string(),
    };
    let (mut out, mut quote, mut gap) = (String::with_capacity(rows.len()), None, false);
    for c in rows.chars() {
        match quote {
            Some(end) => {
                out.push(c);
                quote = Some(end).filter(|&e| e != c);
            }
            None if c.is_whitespace() => gap = true,
            None => {
                if std::mem::take(&mut gap) && !out.is_empty() {
                    out.push(' ');
                }
                out.push(c.to_ascii_lowercase());
                quote = u8::try_from(c).ok().and_then(quote_closer).map(char::from);
            }
        }
    }
    out
}

/// The first relation the outermost `FROM` of `query` names, quotes stripped (`schema.table`); `None` for a subquery or no `FROM`.
pub(crate) fn outer_from_relation(query: &str) -> Option<String> {
    outer_relation(query).map(|(name, _)| name)
}

/// [`outer_from_relation`] with the bytes of `query` that spell the relation.
fn outer_relation(query: &str) -> Option<(String, std::ops::Range<usize>)> {
    let (ident, closer) = (ident_byte, quote_closer);
    let after_from = outer_keyword(query, "from")? + 4;
    let mut rest = skip_blank(query.get(after_from..)?);
    let start = query.len() - rest.len();
    let mut parts: Vec<&str> = Vec::new();
    loop {
        let first = *rest.as_bytes().first()?;
        let (part, tail) = match closer(first).filter(|_| first != b'\'') {
            Some(end) => {
                let close = rest[1..].find(end as char)? + 1;
                (&rest[1..close], &rest[close + 1..])
            }
            None => {
                let end = rest.bytes().position(|c| !ident(c)).unwrap_or(rest.len());
                (&rest[..end], &rest[end..])
            }
        };
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match tail.strip_prefix('.') {
            Some(next) => rest = next,
            None => return Some((parts.join("."), start..query.len() - tail.len())),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_row_set_changes_with_the_filter_and_the_source_and_not_with_the_projection_or_layout() {
        let rows = row_set;
        assert_eq!(rows("SELECT * FROM orders"), "from ?");
        assert_eq!(
            rows("select id, added_later\n  from   \"Sales\".Orders ;\n"),
            "from ?",
            "the relation is the stream's to compare"
        );
        assert_eq!(
            rows("SELECT id FROM orders WHERE spent = 1"),
            "from ? where spent = 1"
        );
        assert_ne!(
            rows("SELECT id FROM orders WHERE spent = 1"),
            rows("SELECT id FROM orders WHERE spent = 0")
        );
        assert_ne!(
            rows("SELECT id FROM orders o JOIN a ON a.id = o.id"),
            rows("SELECT id FROM orders o JOIN b ON b.id = o.id")
        );
        assert_ne!(
            rows("SELECT id FROM orders WHERE note = 'a  B'"),
            rows("SELECT id FROM orders WHERE note = 'a b'"),
            "text inside quotes is kept as written"
        );
        assert_eq!(
            rows("SELECT id FROM orders WHERE note = 'a  B'"),
            "from ? where note = 'a  B'"
        );
        assert_eq!(
            rows("SELECT id, extract(year FROM created) y, 'x from y' s FROM [Order Lines] L"),
            "from ? l"
        );
        assert_eq!(
            rows("WITH recent AS (SELECT * FROM archive WHERE d > 1) SELECT id FROM recent"),
            "with recent as (select * from archive where d > 1) from ?"
        );
        assert_eq!(
            rows("SELECT a FROM (SELECT 1 a) x WHERE a > 0"),
            "from (select 1 a) x where a > 0"
        );
        assert_ne!(
            rows("SELECT id FROM a UNION ALL SELECT id FROM b"),
            rows("SELECT id FROM a UNION ALL SELECT id FROM c")
        );
        assert_eq!(rows("SELECT 1"), "select 1");
        assert_eq!(rows("VALUES (1)"), "values (1)");
        assert_eq!(rows("FROM t SELECT x"), "from t select x");
        assert_eq!(rows("selection FROM t"), "selection from t");
        assert_eq!(rows(""), "");
    }

    #[test]
    fn outer_from_relation_names_the_object_a_query_reads_and_nothing_else() {
        let rel = |q: &str| outer_from_relation(q);
        let t = |s: &str| Some(s.to_string());
        assert_eq!(rel("SELECT * FROM orders"), t("orders"));
        assert_eq!(
            rel("SELECT \"id\", \"updated_at\"\nFROM \"orders\"\n"),
            t("orders")
        );
        assert_eq!(
            rel("select `id` from `shop`.`orders` o where o.id > 1"),
            t("shop.orders")
        );
        assert_eq!(
            rel("SELECT id FROM [dbo].[order lines] AS l"),
            t("dbo.order lines")
        );
        assert_eq!(rel("SELECT id FROM RIVET.ORDERS"), t("RIVET.ORDERS"));
        assert_eq!(
            rel(
                "SELECT id, extract(year FROM created) y, 'x from y' s FROM orders JOIN users u ON 1=1"
            ),
            t("orders")
        );
        assert_eq!(
            rel("WITH recent AS (SELECT * FROM archive) SELECT * FROM recent"),
            t("recent")
        );
        assert_eq!(
            rel("SELECT id FROM orders"),
            rel("SELECT id, added_later FROM orders WHERE id > 0"),
            "a changed projection or filter reads the same object"
        );
        assert_ne!(rel("SELECT id FROM orders"), rel("SELECT id FROM orders_b"));
        assert_ne!(
            rel("SELECT id FROM a.orders"),
            rel("SELECT id FROM b.orders")
        );
        assert_eq!(rel("SELECT * FROM (SELECT 1) x"), None);
        assert_eq!(rel("SELECT 1"), None);
        assert_eq!(rel("FROM t"), t("t"));
        assert_eq!(rel("SELECT xfrom FROM t"), t("t"));
        assert_eq!(rel("SELECT fromx FROM t"), t("t"));
        assert_eq!(rel("SELECT 1 FROM\tt"), t("t"));
        assert_eq!(rel("SELECT (SELECT max(id) FROM inner_t) m"), None);
        assert_eq!(rel("SELECT ((1)) FROM t"), t("t"));
        assert_eq!(rel("SELECT ')' FROM t"), t("t"));
        assert_eq!(rel("SELECT \"a(\" FROM t"), t("t"));
        assert_eq!(rel("SELECT [from] FROM t"), t("t"));
        assert_eq!(rel("SELECT 1 FROM t.\"u v\".w x"), t("t.u v.w"));
        assert_eq!(rel("SELECT 1 FROM t."), None);
        assert_eq!(rel("SELECT 1 FROM \"\""), None);
        assert_eq!(rel("SELECT 1 fro"), None);
        assert_eq!(rel("SELECT 1 FROM 'literal'"), None);
        assert_eq!(rel("SELECT fromage FROM"), None);
        assert_eq!(rel("SELECT 'unterminated FROM t"), None);
    }

    #[test]
    fn oracle_derived_aliases_are_quoted_and_carry_no_as() {
        assert_eq!(alias(SourceType::Oracle, "_rivet"), "\"_rivet\"");
        assert_eq!(alias(SourceType::Postgres, "_rivet"), "_rivet");
        assert_eq!(derived(SourceType::Oracle, "_rivet"), "\"_rivet\"");
        assert_eq!(derived(SourceType::Mysql, "_rivet"), "AS _rivet");
    }

    #[test]
    fn oracle_catalog_names_fold_unquoted_parts_only() {
        let (o, t) = oracle_catalog_preds("rivet.orders");
        assert_eq!((o.as_str(), t.as_str()), ("'RIVET'", "'ORDERS'"));
        let (o, t) = oracle_catalog_preds("orders");
        assert_eq!(o, "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')");
        assert_eq!(t, "'ORDERS'");
        assert_eq!(oracle_catalog_preds("\"Mixed\"").1, "'Mixed'");
        assert_eq!(oracle_catalog_preds("o'x").1, "'O''X'");
    }
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
        // DISTINCT changes the row count/set (would break the wrapped row COUNT).
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
    fn strip_simple_projection_rejects_a_projection_that_renames_or_limits() {
        for query in [
            "SELECT legacy_id AS id, name FROM customers",
            "SELECT legacy_id id, name FROM customers",
            "SELECT id, name AS label FROM customers",
            "SELECT TOP 10 id FROM customers",
            "SELECT ALL id FROM customers",
            "SELECT id,, name FROM customers",
        ] {
            assert_eq!(strip_simple_projection_from(query), None, "{query}");
        }
    }

    /// A table name starts with a letter or `_` in each of its parts; a leading digit is not a table.
    #[test]
    fn a_bare_table_ident_starts_with_a_letter_or_an_underscore_in_every_part() {
        for (query, table) in [
            ("SELECT * FROM _staging", Some("_staging")),
            ("SELECT * FROM _raw._orders", Some("_raw._orders")),
            ("SELECT * FROM t1", Some("t1")),
            ("SELECT * FROM 1t", None),
            ("SELECT * FROM public.1t", None),
            ("SELECT * FROM 1s.orders", None),
            ("SELECT * FROM 2024", None),
            ("SELECT * FROM public.", None),
            ("SELECT * FROM .orders", None),
            ("SELECT * FROM", None),
        ] {
            assert_eq!(strip_select_star_from(query), table, "{query}");
            assert_eq!(strip_simple_projection_from(query), table, "{query}");
        }
    }

    /// `FROM` is the keyword only as a whole word: a column that merely contains it does not end the projection.
    #[test]
    fn the_from_keyword_is_a_whole_word_wherever_it_stands() {
        for (text, at) in [
            ("id from t", Some(3)),
            ("id, valid_from from prices", Some(15)),
            ("id, from_date from prices", Some(14)),
            ("from t", Some(0)),
            ("id from", Some(3)),
            ("id fromage", None),
            ("valid_from", None),
            ("fro", None),
            ("", None),
        ] {
            assert_eq!(find_from_keyword(text), at, "{text:?}");
        }
        for (query, table) in [
            ("SELECT id, valid_from FROM prices", Some("prices")),
            ("SELECT id, from_date FROM prices", Some("prices")),
            (
                "SELECT valid_from, from_date, id FROM prices",
                Some("prices"),
            ),
            ("SELECT id FROM", None),
            ("SELECT FROM prices", None),
        ] {
            assert_eq!(strip_simple_projection_from(query), table, "{query}");
        }
    }

    /// Only a statement that starts with SELECT, and reaches FROM right after its `*`, is the table shortcut.
    #[test]
    fn only_a_statement_that_opens_with_select_is_the_table_shortcut() {
        assert_eq!(strip_prefix_ascii_ci("SeLeCt 1", "select"), Some(" 1"));
        assert_eq!(strip_prefix_ascii_ci("select", "select"), Some(""));
        assert_eq!(strip_prefix_ascii_ci("selec", "select"), None);
        assert_eq!(strip_prefix_ascii_ci("", "select"), None);
        assert_eq!(strip_prefix_ascii_ci("delete 1", "select"), None);
        for query in [
            "DELETE * FROM orders",
            "TABLE  * FROM orders",
            "SELECT * JOIN orders",
            "SELECT * frm",
            "SEL",
            "",
        ] {
            assert_eq!(strip_select_star_from(query), None, "{query:?}");
        }
        for query in ["DELETE id FROM orders", "UPDATE id FROM orders", "SEL"] {
            assert_eq!(strip_simple_projection_from(query), None, "{query:?}");
        }
    }

    #[test]
    fn a_bound_over_an_aliased_projection_is_read_from_the_query_not_the_table() {
        let query = "SELECT legacy_id AS id, name FROM customers";
        assert_eq!(
            aggregate_sql(SourceType::Postgres, "min", "id", query),
            "SELECT min(\"id\") AS rivet_agg FROM (SELECT legacy_id AS id, name FROM customers) AS _rivet"
        );
        assert_eq!(
            null_key_probe_sql(SourceType::Postgres, "id", query),
            "SELECT 1 FROM (SELECT legacy_id AS id, name FROM customers) AS _rivet_nullprobe \
             WHERE \"id\" IS NULL LIMIT 1"
        );
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
            "SELECT min(\"created_at\") AS rivet_agg FROM events"
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
            "SELECT max(\"created_at\") AS rivet_agg FROM (SELECT id, created_at FROM events WHERE x) AS _rivet"
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
            ms.contains("sys.partitions") && ms.contains("OBJECT_ID('dbo.warranty')"),
            "{ms}"
        );
        assert!(!ms.contains("COUNT"), "estimate must not scan: {ms}");
        // MySQL: no trustworthy scan-free count (TABLE_ROWS/EXPLAIN ±30-50%) →
        // skipped (None), never a misleading density figure.
        assert!(row_estimate_sql(SourceType::Mysql, "warranty").is_none());
        assert!(row_estimate_sql(SourceType::Mysql, "shop.warranty").is_none());
    }

    /// [`find_from_keyword`] as 0.31.0 spelled it: defined for ASCII text only.
    fn find_from_keyword_before(s: &str) -> Option<usize> {
        let bytes = s.as_bytes();
        (0..bytes.len().saturating_sub(3)).find(|&i| {
            s[i..i + 4].eq_ignore_ascii_case("from")
                && (i == 0 || bytes[i - 1].is_ascii_whitespace())
                && (i + 4 == bytes.len() || bytes[i + 4].is_ascii_whitespace())
        })
    }

    /// [`strip_prefix_ascii_ci`] as 0.31.0 spelled it: defined for ASCII text only.
    fn strip_prefix_ascii_ci_before<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
        if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
            Some(&s[prefix.len()..])
        } else {
            None
        }
    }

    /// Query-shaped text: SQL words and punctuation in any order, with multi-byte characters among them when `non_ascii`.
    fn query_text(non_ascii: bool) -> impl proptest::strategy::Strategy<Value = String> {
        use proptest::strategy::Strategy as _;
        let mut pieces = vec![
            "select", "SELECT", "from", "FROM", "From", "with", "as", "where", " ", "  ", "\t",
            "\n", "*", ",", ".", ";", "(", ")", "'", "\"", "`", "[", "]", "/*", "*/", "--", "t",
            "id", "a.b", "_x", "1", "$", "#", "fro", "sel",
        ];
        if non_ascii {
            pieces.extend([
                "\u{e9}",
                "\u{446}",
                "\u{2014}",
                "\u{1f600}",
                "\u{a0}",
                "\u{3000}",
            ]);
        }
        proptest::collection::vec(proptest::sample::select(pieces), 0..14).prop_map(|p| p.concat())
    }

    /// The one relation a simple query reads, whatever stands around it; a comment is whitespace wherever it stands.
    #[test]
    fn single_relation_reads_one_table_and_declines_the_rest() {
        let rel = |q: &str| {
            single_relation(q, true).map(|parts| {
                let shown = |p: &NamePart| match p.quoted {
                    true => format!("<{}>", p.name),
                    false => p.name.clone(),
                };
                parts.iter().map(shown).collect::<Vec<_>>().join(".")
            })
        };
        for (q, want) in [
            ("SELECT a FROM t", Some("t")),
            ("select a from s.t x where a = 1", Some("s.t")),
            (
                "SELECT a FROM ONLY inventory.items",
                Some("inventory.items"),
            ),
            (
                "SELECT a FROM \"T x\".\"a\"\"b\" AS \"X\" ORDER BY a",
                Some("<T x>.<a\"b>"),
            ),
            ("SELECT a FROM [s].[c]]d] WITH (NOLOCK)", Some("<s>.<c]d>")),
            ("SELECT a FROM `m`;", Some("<m>")),
            ("SELECT a FROM t;", Some("t")),
            ("SELECT a FROM s . t LIMIT 5", Some("s.t")),
            ("SELECT a FROM db.s.t", Some("db.s.t")),
            ("SELECT a FROM caf\u{e9} c", Some("caf\u{e9}")),
            (
                "SELECT 'it''s from x' AS y, a_from, fromage FROM t",
                Some("t"),
            ),
            ("SELECT (SELECT 1 FROM i) AS s FROM t", Some("t")),
            ("SELECT a FROM t TABLESAMPLE SYSTEM (1)", Some("t")),
            ("SELECT a -- rows from the last week\nFROM t", Some("t")),
            ("SELECT a\n-- taken from legacy\nFROM t", Some("t")),
            ("SELECT a /* from b */ FROM t", Some("t")),
            (
                "SELECT a /* outer /* from b */ still a comment */ FROM t",
                Some("t"),
            ),
            (
                "SELECT a FROM /* the table */ s /* . */ . t -- from b",
                Some("s.t"),
            ),
            ("SELECT a FROM t -- from b", Some("t")),
            ("SELECT a FROM t /* from b */ WHERE a = 1", Some("t")),
            ("SELECT a FROM t/* from b */x", Some("t")),
            ("SELECT '-- not a comment' AS c FROM t", Some("t")),
            ("SELECT \"a--b\", [c/*d] FROM t", Some("t")),
            ("SELECT a FROM t JOIN u ON 1 = 1", None),
            ("SELECT a FROM t x LEFT JOIN u USING (a)", None),
            ("SELECT a FROM t NATURAL JOIN u", None),
            ("SELECT a FROM t, u", None),
            ("SELECT a FROM t x, u", None),
            ("SELECT a FROM t x.y", None),
            ("SELECT a FROM (SELECT * FROM foo) s", None),
            ("SELECT a FROM generate_series(1, 3)", None),
            ("SELECT a FROM 1", None),
            ("SELECT a FROM \"\"", None),
            ("SELECT a FROM \"never closed", None),
            ("SELECT a FROM s.", None),
            ("SELECT a FROM t AS", None),
            ("SELECT a -- FROM t", None),
            ("SELECT a /* FROM t", None),
            ("SELECT 1", None),
        ] {
            assert_eq!(rel(q).as_deref(), want, "{q:?}");
        }
        assert_eq!(
            rel("SELECT a FROM only"),
            None,
            "ONLY is skipped, and no name follows it"
        );
        let plain = |q: &str| single_relation(q, false).map(|p| p[0].name.clone());
        assert_eq!(plain("SELECT a FROM only").as_deref(), Some("only"));
        assert_eq!(plain("SELECT a FROM ONLY t").as_deref(), Some("ONLY"));
    }

    /// The first relation is named when an alias stands before a join or an APPLY, and not when the join follows the name itself.
    #[test]
    fn first_relation_names_the_driving_table_past_an_alias() {
        let first = |q: &str| {
            first_relation(q).map(|p| {
                p.iter()
                    .map(|p| p.name.as_str())
                    .collect::<Vec<_>>()
                    .join(".")
            })
        };
        for (q, want) in [
            (
                "SELECT o.* FROM dbo.orders o CROSS APPLY dbo.f(o.id) x",
                Some("dbo.orders"),
            ),
            ("SELECT a FROM t x JOIN u ON 1 = 1", Some("t")),
            ("SELECT a FROM t x, u", Some("t")),
            ("SELECT a -- from b\nFROM s . t WHERE a = 1", Some("s.t")),
            ("SELECT 'it''s from x' AS y FROM t", Some("t")),
            ("SELECT a FROM t JOIN u ON 1 = 1", None),
            ("SELECT a FROM t, u", None),
            ("SELECT a FROM (SELECT 1) s", None),
        ] {
            assert_eq!(first(q).as_deref(), want, "{q:?}");
        }
    }

    /// A `FROM` inside a comment is not the outermost one: the stream of a commented query is its table.
    #[test]
    fn a_comment_is_never_read_as_the_outer_from() {
        for q in [
            "SELECT amount -- rows from the last week\nFROM t",
            "SELECT amount\n-- taken from legacy\nFROM t",
            "SELECT amount /* from b */ FROM t",
            "SELECT amount /* from b\n*/ FROM /* from c */ t",
            "SELECT amount FROM t -- from b",
        ] {
            assert_eq!(outer_from_relation(q).as_deref(), Some("t"), "{q:?}");
        }
    }

    /// Golden byte offsets of the outermost `FROM`: nesting, a quoted `from`, a doubled quote, identifier boundaries, a stray `)`.
    #[test]
    fn the_outer_from_is_found_at_its_byte_offset() {
        let f = |q: &str| outer_keyword(q, "from");
        assert_eq!(f("SELECT a FROM t"), Some(9));
        assert_eq!(f("select a frOm t"), Some(9));
        for q in [
            "SELECT (SELECT x FROM i) FROM t",
            "SELECT 'from' FROM t",
            "SELECT 'it''s from x' FROM t",
            "SELECT aaaaaaaaaaaaaaaaaaaaaaaaaaaa, 'it''s' FROM t",
            "SELECT a_from, fromage FROM t",
            "SELECT a -- from x\nFROM t",
            "SELECT a /* from x */ FROM t",
        ] {
            assert_eq!(f(q), q.rfind("FROM"), "{q:?}");
        }
        assert_eq!(
            f(") FROM t"),
            Some(2),
            "a stray `)` does not hide what follows"
        );
        assert_eq!(f("SELECT 1"), None);
        assert_eq!(f("SELECT (x FROM t)"), None);
        assert_eq!(f("xfrom t"), None, "preceded by an identifier byte");
        assert_eq!(f("from_x"), None, "followed by an identifier byte");
        assert_eq!(f("fro"), None);
        assert_eq!(f("x from"), Some(2), "the end of the text is a boundary");
    }

    /// A comment's length: a line up to its break, a block with the blocks nested in it, the rest of the text when it never closes.
    #[test]
    fn a_comment_ends_at_its_line_break_or_its_matching_close() {
        for (text, len) in [
            ("-- x\ny", 4),
            ("--", 2),
            ("-- to the end", 13),
            ("/* x */y", 7),
            ("/**/y", 4),
            ("/* a /* b */ c */y", 17),
            ("/*  /**/ x */y", 13),
            ("/* never closed", 15),
            ("/* a /* b */ still open", 23),
            ("- not one", 0),
            ("/ * not one", 0),
            ("", 0),
        ] {
            assert_eq!(comment_len(text.as_bytes()), len, "{text:?}");
        }
        assert_eq!(skip_blank("  -- a\n /* b */\n\tx -- c"), "x -- c");
        assert_eq!(skip_blank("-- only a comment"), "");
    }

    /// One megabyte of each hostile shape is read in one pass, inside the budget.
    #[test]
    fn the_relation_readers_are_linear_on_a_hostile_megabyte() {
        const MB: usize = 1 << 20;
        let shapes = [
            ("dashes", "-".repeat(MB), false),
            ("open blocks", "/*".repeat(MB / 2), false),
            ("closed blocks", "/**/".repeat(MB / 4), false),
            ("open parentheses", "(".repeat(MB), false),
            ("double quotes", "\"".repeat(MB), false),
            ("`from` repeated", "from ".repeat(MB / 5), true),
            (
                "a name of a megabyte of parts",
                format!("SELECT a FROM {}", "a.".repeat(MB / 2)),
                false,
            ),
            (
                "a quoted name never closed",
                format!("SELECT a FROM \"{}", "a".repeat(MB)),
                false,
            ),
            (
                "blanks after FROM",
                format!("SELECT a FROM{}t", " ".repeat(MB)),
                true,
            ),
            (
                "comments after FROM",
                format!("SELECT a FROM {}t", "--\n".repeat(MB / 3)),
                true,
            ),
        ];
        for (label, input, named) in shapes {
            let (done, result) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let read = single_relation(&input, true).is_some();
                let _ = done.send((read, outer_from_relation(&input), row_set(&input).len()));
            });
            let (read, _, _) = result
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap_or_else(|why| panic!("RELATION READER NOT LINEAR: {label}: {why}"));
            assert_eq!(read, named, "{label}");
        }
    }

    const SQL_ENGINES: [SourceType; 4] = [
        SourceType::Postgres,
        SourceType::Mysql,
        SourceType::Mssql,
        SourceType::Oracle,
    ];

    /// Every recogniser and SQL builder of this module over `query`; a panic is the failure.
    fn recognise_everything(query: &str) {
        let _ = strip_select_star_from(query);
        let _ = strip_simple_projection_from(query);
        let _ = row_set(query);
        let _ = outer_from_relation(query);
        let _ = single_relation(query, true);
        let _ = single_relation(query, false);
        let _ = first_relation(query);
        for engine in SQL_ENGINES {
            let _ = aggregate_sql(engine, "min", "id", query);
            let _ = null_key_probe_sql(engine, "id", query);
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 2048, ..Default::default()
        })]

        #[test]
        fn no_recogniser_panics_on_any_query_text(
            query in proptest::prop_oneof![
                query_text(true),
                proptest::prelude::any::<String>(),
            ],
        ) {
            recognise_everything(&query);
            if let Some(at) = find_from_keyword(&query) {
                proptest::prop_assert!(
                    query.is_char_boundary(at) && query.is_char_boundary(at + 4),
                    "{query:?}: {at}"
                );
            }
        }

        #[test]
        fn ascii_query_text_is_recognised_as_before(
            query in proptest::prop_oneof![query_text(false), "[ -~\\t\\n]{0,40}"],
        ) {
            proptest::prop_assert_eq!(
                find_from_keyword(&query),
                find_from_keyword_before(&query),
                "{:?}", query
            );
            for prefix in ["select", "from"] {
                proptest::prop_assert_eq!(
                    strip_prefix_ascii_ci(&query, prefix),
                    strip_prefix_ascii_ci_before(&query, prefix),
                    "{:?} / {}", query, prefix
                );
            }
        }
    }

    #[test]
    fn the_from_keyword_is_found_at_the_same_offset_around_multi_byte_text() {
        for (text, at) in [
            ("a from t", Some(2)),
            ("from t", Some(0)),
            ("a FROM", Some(2)),
            ("a\tFrOm\nt", Some(2)),
            ("afrom t", None),
            ("a fromt", None),
            ("fro", None),
            ("", None),
            ("caf\u{e9} from t", Some(6)),
            ("'\u{43a}\u{43e}\u{444}\u{435}' AS x FROM t", Some(16)),
            ("\u{1f600} from\tt", Some(5)),
            ("\u{e9}", None),
            ("\u{e9}from t", None),
            ("a from\u{e9}", None),
            ("a\u{a0}from t", None),
            ("a from\u{a0}t", None),
            ("\u{2014}\u{2014}\u{2014}", None),
        ] {
            assert_eq!(find_from_keyword(text), at, "{text:?}");
        }
    }

    #[test]
    fn a_keyword_prefix_is_stripped_only_from_text_that_starts_with_it() {
        for (text, prefix, rest) in [
            ("select x", "select", Some(" x")),
            ("SeLeCt", "select", Some("")),
            ("FROM\u{e9}", "from", Some("\u{e9}")),
            ("selec", "select", None),
            ("", "select", None),
            ("selec\u{e9}", "select", None),
            ("\u{e9}select", "select", None),
            ("WITH \u{e9} AS", "select", None),
            ("/* \u{446}\u{435}\u{43d}\u{430} */", "select", None),
            (", '\u{e9}t\u{e9}'", "from", None),
            ("fro\u{2014}", "from", None),
        ] {
            assert_eq!(
                strip_prefix_ascii_ci(text, prefix),
                rest,
                "{text:?} / {prefix}"
            );
        }
    }

    #[test]
    fn a_query_holding_multi_byte_text_is_never_read_as_the_bare_table() {
        for query in [
            "SELECT g AS id, 'caf\u{e9}' AS label FROM generate_series(1, 100) AS g",
            "WITH \u{e9} AS (SELECT 1 AS id) SELECT id FROM \u{e9}",
            "/* \u{446}\u{435}\u{43d}\u{430} */ SELECT 1 AS id",
            "/* ab\u{e9} */ SELECT * FROM orders",
            "SELECT *, '\u{e9}t\u{e9}' AS label FROM orders",
            "SELECT id, '\u{e9}t\u{e9}' AS label FROM orders",
            "SELECT id, /* \u{446}\u{435}\u{43d}\u{430} */ name FROM orders",
            "SELECT id, caf\u{e9} FROM orders",
            "SELECT * FROM caf\u{e9}",
            "SELECT id FROM caf\u{e9}",
            "SELECT * FROM orders -- \u{2014}",
        ] {
            assert_eq!(strip_select_star_from(query), None, "{query}");
            assert_eq!(strip_simple_projection_from(query), None, "{query}");
            assert_eq!(
                aggregate_sql(SourceType::Postgres, "min", "id", query),
                format!("SELECT min(\"id\") AS rivet_agg FROM ({query}) AS _rivet"),
            );
            assert_eq!(
                null_key_probe_sql(SourceType::Mysql, "id", query),
                format!("SELECT 1 FROM ({query}) AS _rivet_nullprobe WHERE `id` IS NULL LIMIT 1"),
            );
        }
    }

    #[test]
    fn multi_byte_text_after_the_table_form_leaves_the_ascii_decision_alone() {
        assert_eq!(
            strip_select_star_from("SELECT * FROM orders\u{3000}"),
            strip_select_star_from("SELECT * FROM orders ")
        );
        assert_eq!(
            strip_simple_projection_from("SELECT id FROM orders WHERE note = '\u{e9}'"),
            strip_simple_projection_from("SELECT id FROM orders WHERE note = 'e'")
        );
        assert_eq!(
            outer_from_relation("SELECT '\u{e9}' x FROM caf\u{e9}.\"t \u{2014}\" o"),
            Some("caf\u{e9}.t \u{2014}".to_string())
        );
        assert_eq!(
            row_set("SELECT '\u{e9}' FROM \u{446} WHERE a = '\u{2014}  B'"),
            "from ? where a = '\u{2014}  B'"
        );
    }
}
