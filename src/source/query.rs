//! Shared SQL builder for incremental extraction (ADR-0007 / Epic D).
//!
//! Both Postgres and MySQL use the same query shape; only identifier quoting
//! differs, and that is delegated to [`crate::sql::quote_ident`].
//!
//! # Cursor value handling (SecOps)
//!
//! Cursor values originate from source data — an attacker with write access to
//! the cursor column could plant SQL fragments (`' OR 1=1 --`, backslash escapes
//! on pre-9.1 Postgres, `NO_BACKSLASH_ESCAPES`-sensitive strings on MySQL).
//!
//! * **MySQL** — the cursor value is always passed as a positional bind
//!   parameter (`?`). The driver sends it out-of-band, so no escaping is needed
//!   and SQL injection is impossible.
//!
//! * **Postgres** — binding is awkward because the `postgres` crate requires
//!   the caller to know the column's type at bind time (the cursor column may
//!   be timestamp, bigint, text, uuid, …). Instead we embed the value as a
//!   quoted Postgres string literal using the `E'…'` syntax, which escapes both
//!   single quotes and backslashes regardless of the server's
//!   `standard_conforming_strings` setting. The server then implicitly casts
//!   the string to the column's type, matching the original pre-1.x behavior.
//!   [`escape_pg_literal`] is the single source of truth for that escaping and
//!   is covered by dedicated injection-attempt tests below.
//!
//! The builder returns a [`BuiltQuery`]; for MySQL `cursor_param` is `Some` and
//! the SQL contains `?`; for Postgres `cursor_param` is always `None` and the
//! literal is embedded.

use crate::config::{IncrementalCursorMode, SourceType};
use crate::plan::IncrementalCursorPlan;
use crate::sql::quote_ident;
use crate::types::CursorState;

/// Output of [`build_incremental_query`]: SQL text with an optional cursor bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuiltQuery {
    /// SQL text. Contains `$1` (Postgres) or `?` (MySQL) when [`Self::cursor_param`] is `Some`.
    pub sql: String,
    /// Cursor value to bind at position 1, if any.
    pub cursor_param: Option<String>,
}

impl BuiltQuery {
    fn without_param(sql: String) -> Self {
        Self {
            sql,
            cursor_param: None,
        }
    }
}

/// Build the effective extraction query for a base query + optional incremental cursor.
///
/// * `None` incremental → `base_query` unchanged (snapshot / chunked chunk body / time-window).
/// * `SingleColumn` → `WHERE <p> > $1 ORDER BY <p>`.
/// * `Coalesce` → single-level wrapper that appends a synthetic
///   [`IncrementalCursorPlan::RIVET_COALESCE_CURSOR_COL`] column, filters and orders
///   by `COALESCE(primary, fallback)`. The outer `ORDER BY` ensures the final Arrow
///   batch carries the maximum coalesced value so cursor advance stays monotonic.
pub(crate) fn build_incremental_query(
    base_query: &str,
    incremental: Option<&IncrementalCursorPlan>,
    cursor: Option<&CursorState>,
    source_type: SourceType,
) -> BuiltQuery {
    let Some(plan) = incremental else {
        return BuiltQuery::without_param(base_query.to_string());
    };

    let cursor_value = cursor.and_then(|c| c.last_cursor_value.as_deref());
    let primary = quote_ident(source_type, &plan.primary_column);

    match plan.mode {
        IncrementalCursorMode::SingleColumn => {
            if let Some(val) = cursor_value {
                let (predicate_rhs, cursor_param) = cursor_rhs(source_type, val);
                BuiltQuery {
                    sql: format!(
                        "SELECT * FROM ({base}) AS _rivet WHERE {p} > {rhs} ORDER BY {p}",
                        base = base_query,
                        p = primary,
                        rhs = predicate_rhs,
                    ),
                    cursor_param,
                }
            } else {
                BuiltQuery::without_param(format!(
                    "SELECT * FROM ({base}) AS _rivet ORDER BY {p}",
                    base = base_query,
                    p = primary,
                ))
            }
        }
        IncrementalCursorMode::Coalesce => {
            let fallback_name = plan
                .fallback_column
                .as_deref()
                .expect("coalesce requires fallback_column (enforced by Config::validate)");
            let fallback = quote_ident(source_type, fallback_name);
            let coalesce = format!("COALESCE(_rivet.{primary}, _rivet.{fallback})");
            let synthetic = quote_ident(
                source_type,
                IncrementalCursorPlan::RIVET_COALESCE_CURSOR_COL,
            );

            let (where_clause, cursor_param) = match cursor_value {
                Some(val) => {
                    let (rhs, param) = cursor_rhs(source_type, val);
                    (format!(" WHERE {coalesce} > {rhs}"), param)
                }
                None => (String::new(), None),
            };

            BuiltQuery {
                sql: format!(
                    "SELECT _rivet.*, {coalesce} AS {synthetic} FROM ({base}) AS _rivet{where_clause} \
                     ORDER BY {coalesce}, _rivet.{primary}, _rivet.{fallback}",
                    base = base_query,
                    coalesce = coalesce,
                    synthetic = synthetic,
                    where_clause = where_clause,
                    primary = primary,
                    fallback = fallback,
                ),
                cursor_param,
            }
        }
    }
}

/// Produce the `>` right-hand side for the cursor predicate plus any bind value.
///
/// * MySQL → `("?", Some(val))`: bind parameter, no escaping needed.
/// * Postgres → `("E'…escaped…'", None)`: in-SQL literal, always single-argument
///   `E'…'` form so both `'` and `\` are handled safely regardless of
///   `standard_conforming_strings`. No parameter binding (the `postgres` crate
///   needs a concrete type for binds; the column may be timestamp, bigint,
///   uuid, etc. — a text literal lets the server implicitly cast).
fn cursor_rhs(source_type: SourceType, value: &str) -> (String, Option<String>) {
    match source_type {
        SourceType::Mysql => ("?".to_string(), Some(value.to_string())),
        SourceType::Postgres => (escape_pg_literal(value), None),
    }
}

/// Quote `s` as a Postgres `E'…'` string literal, escaping both `'` and `\`.
/// The `E` prefix forces the server to interpret `\\` as a single backslash
/// regardless of `standard_conforming_strings`, which closes the historical
/// backslash-injection path.
pub(crate) fn escape_pg_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 3);
    out.push_str("E'");
    for c in s.chars() {
        match c {
            '\\' => out.push_str(r"\\"),
            '\'' => out.push_str(r"\'"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor_with(val: Option<&str>) -> CursorState {
        CursorState {
            export_name: "t".into(),
            last_cursor_value: val.map(str::to_string),
            last_run_at: None,
        }
    }

    fn single(col: &str) -> IncrementalCursorPlan {
        IncrementalCursorPlan {
            primary_column: col.into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
        }
    }

    fn coalesce(p: &str, f: &str) -> IncrementalCursorPlan {
        IncrementalCursorPlan {
            primary_column: p.into(),
            fallback_column: Some(f.into()),
            mode: IncrementalCursorMode::Coalesce,
        }
    }

    #[test]
    fn none_returns_base_query() {
        let q = build_incremental_query("SELECT 1", None, None, SourceType::Postgres);
        assert_eq!(q.sql, "SELECT 1");
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn single_column_first_run_has_no_param() {
        let p = single("updated_at");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(None)),
            SourceType::Postgres,
        );
        assert!(q.sql.contains("ORDER BY \"updated_at\""), "{}", q.sql);
        assert!(!q.sql.contains("WHERE"), "{}", q.sql);
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn single_column_mysql_uses_question_mark_placeholder() {
        let p = single("id");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("42"))),
            SourceType::Mysql,
        );
        assert!(q.sql.contains("WHERE `id` > ?"), "{}", q.sql);
        assert!(
            !q.sql.contains("'42'"),
            "cursor must not appear inline: {}",
            q.sql
        );
        assert_eq!(q.cursor_param.as_deref(), Some("42"));
    }

    #[test]
    fn escape_pg_literal_basic() {
        assert_eq!(escape_pg_literal("hello"), r"E'hello'");
        assert_eq!(escape_pg_literal("O'Brien"), r"E'O\'Brien'");
        assert_eq!(escape_pg_literal(r"C:\tmp"), r"E'C:\\tmp'");
        assert_eq!(escape_pg_literal(r"'; DROP --"), r"E'\'; DROP --'");
    }

    #[test]
    fn single_column_postgres_embeds_escaped_literal() {
        let p = single("id");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("42"))),
            SourceType::Postgres,
        );
        assert!(q.sql.contains("WHERE \"id\" > E'42'"), "{}", q.sql);
        assert_eq!(
            q.cursor_param, None,
            "Postgres path never binds — values are embedded as E'…' literals"
        );
    }

    #[test]
    fn cursor_value_with_quote_is_escaped_for_postgres() {
        let p = single("name");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("O'Brien"))),
            SourceType::Postgres,
        );
        assert!(q.sql.contains(r"E'O\'Brien'"), "{}", q.sql);
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn cursor_value_with_quote_is_bound_for_mysql() {
        // MySQL keeps bind-parameter semantics — the driver sends the value
        // out-of-band, so no escaping is needed.
        let p = single("name");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("O'Brien"))),
            SourceType::Mysql,
        );
        assert!(!q.sql.contains("O'Brien"), "{}", q.sql);
        assert!(q.sql.contains("> ?"), "{}", q.sql);
        assert_eq!(q.cursor_param.as_deref(), Some("O'Brien"));
    }

    #[test]
    fn postgres_injection_attempt_is_fully_escaped() {
        // Attack payload: `'; DROP TABLE users; --`.
        // Must be rendered as E'\'; DROP TABLE users; --' — single-quoted, with
        // the payload's `'` escaped as `\'`, so nothing breaks out of the literal.
        let p = single("tenant_id");
        let malicious = "'; DROP TABLE users; --";
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some(malicious))),
            SourceType::Postgres,
        );
        // The keyword `DROP TABLE` survives inside the literal — that is the
        // whole point of quoting — but it must be enclosed in `E'…'` so it's a
        // string value, not SQL tokens.
        let rhs_start = q.sql.find("> E'").expect("E'-quoted literal");
        let tail = &q.sql[rhs_start..];
        assert!(tail.starts_with("> E'\\';"), "payload not escaped: {tail}");
        assert!(tail.ends_with("--' ORDER BY \"tenant_id\""), "tail: {tail}");
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn postgres_backslash_in_cursor_is_doubled() {
        let p = single("path");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("C:\\data"))),
            SourceType::Postgres,
        );
        assert!(q.sql.contains(r"E'C:\\data'"), "{}", q.sql);
    }

    #[test]
    fn mysql_injection_attempt_goes_into_bind() {
        let p = single("tenant_id");
        let malicious = "'; DROP TABLE users; --";
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some(malicious))),
            SourceType::Mysql,
        );
        assert!(!q.sql.contains("DROP TABLE"), "{}", q.sql);
        assert_eq!(q.cursor_param.as_deref(), Some(malicious));
    }

    #[test]
    fn coalesce_is_single_level_with_outer_order_by() {
        let p = coalesce("updated_at", "created_at");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("2024-01-01"))),
            SourceType::Postgres,
        );
        assert!(
            q.sql.contains("AS \"_rivet_coalesced_cursor\""),
            "synthetic alias missing: {}",
            q.sql
        );
        let order_idx = q.sql.rfind("ORDER BY").expect("ORDER BY present");
        let last_select = q.sql.rfind("SELECT").expect("SELECT present");
        assert!(
            order_idx > last_select,
            "ORDER BY not at outer level: {}",
            q.sql
        );
        assert!(q.sql.contains("WHERE COALESCE"), "{}", q.sql);
        assert!(q.sql.contains("> E'2024-01-01'"), "{}", q.sql);
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn coalesce_first_run_has_no_where() {
        let p = coalesce("updated_at", "created_at");
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(None)),
            SourceType::Postgres,
        );
        assert!(!q.sql.contains("WHERE"), "{}", q.sql);
        assert!(q.sql.contains("ORDER BY COALESCE"), "{}", q.sql);
        assert_eq!(q.cursor_param, None);
    }
}
