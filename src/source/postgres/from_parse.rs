//! FROM-clause parsing for Postgres `SELECT … FROM <table>` queries.
//!
//! Used by the catalog-hints path ([`super::pg_fetch_numeric_catalog_hints`])
//! to recover the relation name from a user-supplied SQL query so we can
//! ask `pg_catalog` for the numeric `(precision, scale)` of each column.
//!
//! Scope is intentionally narrow: a *simple* `FROM <maybe_schema>.<table>
//! [AS] [alias]` with no joins, no comma-list, no sub-queries.  Anything more
//! complex returns `None` — the caller falls back to type inference from the
//! result set instead of guessing.
//!
//! This module deliberately has zero dependencies on the `postgres` crate —
//! it's pure `&str`/`&[u8]` manipulation — so it can (and does) live behind
//! cheap unit tests with no live database required.

/// Trim ASCII whitespace from both ends.  Avoids `char::is_whitespace`'s
/// unicode-aware behaviour, which is overkill for SQL keyword boundaries.
fn trim_sql_ascii_ws(s: &str) -> &str {
    s.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r'))
}

/// Match a case-insensitive ASCII keyword at byte index `idx`, with
/// identifier-boundary checks on both sides (so `from_table` doesn't match
/// `from`).
fn sql_keyword_at(haystack: &[u8], idx: usize, kw_lower: &[u8]) -> bool {
    let n = kw_lower.len();
    if idx + n > haystack.len() {
        return false;
    }
    if !haystack[idx..idx + n].eq_ignore_ascii_case(kw_lower) {
        return false;
    }
    let before_ok = idx == 0 || !is_sql_ident_byte(haystack[idx - 1]);
    let after_idx = idx + n;
    let after_ok = after_idx >= haystack.len() || !is_sql_ident_byte(haystack[after_idx]);
    before_ok && after_ok
}

fn is_sql_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Find the byte offset of the top-level `FROM` keyword, skipping nested
/// parentheses (sub-queries, derived tables) and single-quoted string
/// literals (with `''` escapes).
fn pg_find_outer_from_keyword(sql: &str) -> Option<usize> {
    let b = sql.as_bytes();
    let mut i = 0usize;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    while i < b.len() {
        if in_single_quote {
            if b[i] == b'\'' {
                if i + 1 < b.len() && b[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
                continue;
            }
            i += 1;
            continue;
        }
        if b[i] == b'\'' {
            in_single_quote = true;
            i += 1;
            continue;
        }
        if b[i] == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b[i] == b')' {
            depth = depth.saturating_sub(1);
            i += 1;
            continue;
        }
        if depth == 0 && sql_keyword_at(b, i, b"from") {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_pg_double_quoted_ident(rest: &str) -> Option<(String, &str)> {
    let mut chars = rest.chars();
    if chars.next()? != '"' {
        return None;
    }
    let mut out = String::new();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            if chars.as_str().starts_with('"') {
                chars.next();
                out.push('"');
                continue;
            }
            return Some((out, chars.as_str()));
        }
        out.push(ch);
    }
    None
}

fn parse_pg_ident_piece(rest: &str) -> Option<(String, bool, &str)> {
    let rest = trim_sql_ascii_ws(rest);
    if rest.is_empty() {
        return None;
    }
    if rest.starts_with('"') {
        let (v, tail) = parse_pg_double_quoted_ident(rest)?;
        return Some((v, true, tail));
    }
    let bytes = rest.as_bytes();
    if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
        return None;
    }
    let mut i = 1usize;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    let ident = rest.get(0..i)?.to_string();
    Some((ident, false, rest.get(i..)?))
}

fn regclass_segment(ident: &str, quoted: bool) -> String {
    if quoted {
        format!("\"{}\"", ident.replace('"', "\"\""))
    } else {
        ident.to_string()
    }
}

fn parse_pg_qualified_table_for_regclass(mut rest: &str) -> Option<(String, &str)> {
    rest = trim_sql_ascii_ws(rest);
    let (p1, q1, tail) = parse_pg_ident_piece(rest)?;
    let tail = trim_sql_ascii_ws(tail);
    if tail.starts_with('.') {
        let after = trim_sql_ascii_ws(tail.get(1..)?);
        let (p2, q2, tail2) = parse_pg_ident_piece(after)?;
        return Some((
            format!(
                "{}.{}",
                regclass_segment(&p1, q1),
                regclass_segment(&p2, q2),
            ),
            tail2,
        ));
    }
    Some((regclass_segment(&p1, q1), tail))
}

fn starts_clause_boundary(rest: &str) -> bool {
    let r = trim_sql_ascii_ws(rest);
    if r.is_empty() {
        return true;
    }
    const KWS: &[&[u8]] = &[
        b"where",
        b"group",
        b"having",
        b"order",
        b"limit",
        b"offset",
        b"fetch",
        b"union",
        b"intersect",
        b"except",
        b"window",
        b"for",
    ];
    let bytes = r.as_bytes();
    KWS.iter().any(|kw| sql_keyword_at(bytes, 0, kw))
}

fn joins_or_comma_after_from(rest: &str) -> bool {
    let r = trim_sql_ascii_ws(rest);
    if r.starts_with(',') {
        return true;
    }
    let b = r.as_bytes();
    sql_keyword_at(b, 0, b"inner")
        || sql_keyword_at(b, 0, b"left")
        || sql_keyword_at(b, 0, b"right")
        || sql_keyword_at(b, 0, b"full")
        || sql_keyword_at(b, 0, b"cross")
        || sql_keyword_at(b, 0, b"natural")
        || sql_keyword_at(b, 0, b"join")
}

fn skip_optional_table_alias(rest: &str) -> Option<&str> {
    let rest = trim_sql_ascii_ws(rest);
    if rest.is_empty() || starts_clause_boundary(rest) || joins_or_comma_after_from(rest) {
        return Some(rest);
    }
    let mut rest = rest;
    if sql_keyword_at(rest.as_bytes(), 0, b"as") {
        rest = rest.get(2..)?;
        rest = trim_sql_ascii_ws(rest);
    }
    let (_, _, tail) = parse_pg_ident_piece(rest)?;
    let tail = trim_sql_ascii_ws(tail);
    if joins_or_comma_after_from(tail) {
        return None;
    }
    Some(tail)
}

/// Public entry point.  Try to extract the `<schema>.<table>` (or just
/// `<table>`) literal suitable for casting to `regclass` from a simple
/// `SELECT … FROM …` query.  Returns `None` for any query the parser does
/// not handle (joins, comma lists, sub-queries, multi-relation FROM).
pub(super) fn try_parse_pg_simple_from_regclass_literal(query: &str) -> Option<String> {
    let from_idx = pg_find_outer_from_keyword(query)?;
    let mut tail = query.get(from_idx + 4..)?;
    tail = trim_sql_ascii_ws(tail);
    if sql_keyword_at(tail.as_bytes(), 0, b"only") {
        tail = tail.get(4..)?;
        tail = trim_sql_ascii_ws(tail);
    }
    let (regclass_lit, after_rel) = parse_pg_qualified_table_for_regclass(tail)?;
    let after_rel = trim_sql_ascii_ws(after_rel);
    let after_rel = skip_optional_table_alias(after_rel)?;
    let after_rel = trim_sql_ascii_ws(after_rel);
    if joins_or_comma_after_from(after_rel) {
        return None;
    }
    Some(regclass_lit)
}

#[cfg(test)]
mod tests {
    use super::try_parse_pg_simple_from_regclass_literal;

    #[test]
    fn parse_simple_from_unqualified_table_alias_where() {
        let q = "SELECT id, amount\nFROM transactions t\nWHERE x = 1";
        assert_eq!(
            try_parse_pg_simple_from_regclass_literal(q).as_deref(),
            Some("transactions")
        );
    }

    #[test]
    fn parse_simple_from_qualified() {
        let q = "SELECT id FROM public.orders WHERE 1=1";
        assert_eq!(
            try_parse_pg_simple_from_regclass_literal(q).as_deref(),
            Some("public.orders")
        );
    }

    #[test]
    fn parse_only_prefix() {
        let q = "SELECT * FROM ONLY inventory.items";
        assert_eq!(
            try_parse_pg_simple_from_regclass_literal(q).as_deref(),
            Some("inventory.items")
        );
    }

    #[test]
    fn join_rejected() {
        assert!(
            try_parse_pg_simple_from_regclass_literal("SELECT * FROM a INNER JOIN b USING (id)")
                .is_none()
        );
    }

    #[test]
    fn subquery_from_rejected() {
        assert!(
            try_parse_pg_simple_from_regclass_literal("SELECT * FROM (SELECT * FROM foo) s")
                .is_none()
        );
    }
}
