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
use crate::sql::{alias, derived, quote_ident};
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
///
/// # Boundary semantics — strict `>` and the tie hazard
///
/// Both modes resume with **strict `>`** (`WHERE <cursor> > <last>`), where
/// `<last>` is the maximum cursor value of the previous run. Unlike
/// [`build_keyset_query`] — whose key the planner *enforces* to be unique and
/// NOT NULL, making `>` provably lossless — the incremental cursor column has
/// **no uniqueness guarantee**. If two rows share the high-watermark value and
/// the second one becomes visible only *after* the run that advanced the
/// watermark past it (a low-resolution cursor like a second-granularity
/// `updated_at`, or rows committed at the same timestamp after the read
/// snapshot), the next run's `> <last>` **silently skips** them — those rows
/// are never exported.
///
/// `>` is the deliberate choice: `>=` would instead re-export the boundary row
/// on *every* run (guaranteed duplicates). The safe configuration is a
/// **strictly-monotonic, per-row-distinct** cursor (a sequence/identity id, or
/// a timestamp with sub-value uniqueness). When the cursor can tie, prefer a
/// chunked/full re-snapshot for the affected window. See `docs/semantics.md`.
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

    let rivet = alias(source_type, "_rivet");
    let from_rivet = derived(source_type, "_rivet");
    let prefix = match plan.mode {
        IncrementalCursorMode::SingleColumn => String::new(),
        IncrementalCursorMode::Coalesce => format!("{rivet}."),
    };
    let prefix = prefix.as_str();
    let cursor = cursor_expr(plan, prefix, source_type);

    let mut preds: Vec<String> = Vec::new();
    let mut cursor_param = None;
    if let Some(val) = cursor_value {
        let (rhs, param) = cursor_rhs(source_type, val);
        preds.push(format!("{cursor} > {rhs}"));
        cursor_param = param;
    }
    if let Some(settle) = &plan.settle {
        preds.extend(settle_predicates(
            plan,
            settle.column.as_deref(),
            settle.after_secs,
            prefix,
            base_query,
            cursor_value,
            source_type,
        ));
    }
    let where_clause = if preds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", preds.join(" AND "))
    };

    let sql = match plan.mode {
        IncrementalCursorMode::SingleColumn => format!(
            "SELECT * FROM ({base}) {from_rivet}{where_clause} ORDER BY {primary}",
            base = base_query,
        ),
        IncrementalCursorMode::Coalesce => {
            let fallback = quote_ident(source_type, coalesce_fallback(plan));
            let synthetic = quote_ident(
                source_type,
                IncrementalCursorPlan::RIVET_COALESCE_CURSOR_COL,
            );
            format!(
                "SELECT {rivet}.*, {cursor} AS {synthetic} FROM ({base}) {from_rivet}{where_clause} \
                 ORDER BY {cursor}, {rivet}.{primary}, {rivet}.{fallback}",
                base = base_query,
            )
        }
    };
    BuiltQuery { sql, cursor_param }
}

fn coalesce_fallback(plan: &IncrementalCursorPlan) -> &str {
    plan.fallback_column
        .as_deref()
        .expect("coalesce requires fallback_column (enforced by Config::validate)")
}

/// The cursor expression (primary column or its COALESCE) over a table prefix.
fn cursor_expr(plan: &IncrementalCursorPlan, prefix: &str, source_type: SourceType) -> String {
    let primary = quote_ident(source_type, &plan.primary_column);
    match plan.mode {
        IncrementalCursorMode::SingleColumn => format!("{prefix}{primary}"),
        IncrementalCursorMode::Coalesce => {
            let fallback = quote_ident(source_type, coalesce_fallback(plan));
            format!("COALESCE({prefix}{primary}, {prefix}{fallback})")
        }
    }
}

/// Settle predicates: the row has aged past `after_secs`, and when the settle column is
/// not the cursor, the cursor stays below the first still-settling row.
fn settle_predicates(
    plan: &IncrementalCursorPlan,
    settle_column: Option<&str>,
    after_secs: u64,
    prefix: &str,
    base_query: &str,
    last: Option<&str>,
    source_type: SourceType,
) -> Vec<String> {
    let threshold = settle_threshold(source_type, after_secs);
    let target = |pre: &str| match settle_column {
        Some(c) => format!("{pre}{}", quote_ident(source_type, c)),
        None => cursor_expr(plan, pre, source_type),
    };
    let t = target(prefix);
    let mut preds = vec![format!("({t} IS NULL OR {t} < {threshold})")];

    let settle_is_cursor = match (settle_column, plan.mode) {
        (None, _) => true,
        (Some(c), IncrementalCursorMode::SingleColumn) => c == plan.primary_column,
        (Some(_), IncrementalCursorMode::Coalesce) => false,
    };
    if !settle_is_cursor {
        let young_ref = format!("{}.", alias(source_type, "_rivet_young"));
        let young = cursor_expr(plan, &young_ref, source_type);
        let mut young_where = vec![format!("{young} IS NOT NULL")];
        if let Some(v) = last {
            young_where.push(format!("{young} > {}", inline_literal(source_type, v)));
        }
        young_where.push(format!("{} >= {threshold}", target(&young_ref)));
        preds.push(format!(
            "{cursor} < ALL (SELECT {young} FROM ({base_query}) {from_young} WHERE {cond})",
            from_young = derived(source_type, "_rivet_young"),
            cursor = cursor_expr(plan, prefix, source_type),
            cond = young_where.join(" AND "),
        ));
    }
    preds
}

/// `now − after_secs` on the source clock (every engine reads in a UTC session).
fn settle_threshold(source_type: SourceType, after_secs: u64) -> String {
    match source_type {
        SourceType::Mysql => format!("(NOW() - INTERVAL {after_secs} SECOND)"),
        SourceType::Postgres => format!("(now() - INTERVAL '{after_secs} seconds')"),
        SourceType::Mssql => format!("DATEADD(SECOND, -{after_secs}, SYSUTCDATETIME())"),
        SourceType::Oracle => {
            format!("(SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL({after_secs}, 'SECOND'))")
        }
        SourceType::Mongo => unreachable!(
            "settle_threshold: MongoDB incremental cursor is not a SQL path (guarded by full-mode-only validation)"
        ),
    }
}

/// Pick the right query builder for an export request: a keyset page when
/// `page_limit` is set with a key plan (OPT-4), otherwise the
/// incremental/snapshot shape. Centralizes the choice so both source drivers
/// stay identical.
pub(crate) fn build_export_query(
    request: &crate::source::ExportRequest<'_>,
    source_type: SourceType,
) -> BuiltQuery {
    match (request.page_limit, request.incremental) {
        (Some(limit), Some(plan)) => {
            let cursor = request.cursor.and_then(|c| c.last_cursor_value.as_deref());
            match request.upper_bound {
                // Sequential single-worker page (the common path).
                None => build_keyset_query(
                    request.query,
                    &plan.primary_column,
                    cursor,
                    limit,
                    source_type,
                ),
                // A parallel keyset worker's `(cursor, upper]` range.
                Some(_) => build_keyset_query_bounded(
                    request.query,
                    &plan.primary_column,
                    cursor,
                    request.upper_bound,
                    limit,
                    source_type,
                ),
            }
        }
        _ => build_incremental_query(
            request.query,
            request.incremental,
            request.cursor,
            source_type,
        ),
    }
}

/// Build one keyset (seek) pagination page (OPT-4).
///
/// * First page (`last = None`): `SELECT * FROM (base) AS _rivet ORDER BY <key> LIMIT n`.
/// * Subsequent pages: `… WHERE <key> > <rhs> ORDER BY <key> LIMIT n`.
///
/// `<key>` MUST be an index-backed, unique, NOT NULL column — enforced by the
/// planner ([`crate::plan::build`]) — so the `ORDER BY` is an index range scan
/// (never a filesort) and `> <last>` never skips rows that share the last key.
/// `<rhs>` reuses the same injection-safe handling as
/// [`build_incremental_query`] (MySQL `?` bind, Postgres escaped `E'…'`).
/// `limit` is a `usize`, inlined as a plain integer literal.
pub(crate) fn build_keyset_query(
    base_query: &str,
    key_column: &str,
    last: Option<&str>,
    limit: usize,
    source_type: SourceType,
) -> BuiltQuery {
    build_keyset_query_bounded(base_query, key_column, last, None, limit, source_type)
}

/// Keyset page with an optional INCLUSIVE upper bound `<= upper` — the parallel
/// keyset runner gives each worker a disjoint `(lower, upper]` range so N workers
/// page concurrently (feat/parallel-keyset). `upper` is INLINED (never a bind
/// param) so the single `cursor_param` slot stays free for the `>` cursor value:
/// `WHERE key > ?bind AND key <= '<inline>'`. Row-count parity is structural —
/// the half-open intervals partition the key, so the union reads every row once.
pub(crate) fn build_keyset_query_bounded(
    base_query: &str,
    key_column: &str,
    last: Option<&str>,
    upper: Option<&str>,
    limit: usize,
    source_type: SourceType,
) -> BuiltQuery {
    let key = quote_ident(source_type, key_column);
    let page = page_limit_clause(source_type, limit);
    // Upper bound is always an in-SQL literal (implicit-cast to the key type, same
    // as the cursor RHS on PG/MSSQL), so it never consumes the bind slot.
    let upper_pred = |joiner: &str| match upper {
        Some(hi) => format!(
            "{joiner}{k} <= {lit}",
            k = key,
            lit = inline_literal(source_type, hi)
        ),
        None => String::new(),
    };
    match last {
        Some(val) => {
            let (rhs, cursor_param) = cursor_rhs(source_type, val);
            BuiltQuery {
                sql: format!(
                    "SELECT * FROM ({base}) {from_rivet} WHERE {k} > {rhs}{up} ORDER BY {k} {page}",
                    from_rivet = derived(source_type, "_rivet"),
                    base = base_query,
                    k = key,
                    up = upper_pred(" AND "),
                ),
                cursor_param,
            }
        }
        None => BuiltQuery::without_param(format!(
            "SELECT * FROM ({base}) {from_rivet}{where_up} ORDER BY {k} {page}",
            from_rivet = derived(source_type, "_rivet"),
            base = base_query,
            k = key,
            where_up = upper_pred(" WHERE "),
        )),
    }
}

/// Restrict a base query to a half-open key window `(lo, hi]` — the whole-export
/// bound a `--split` range sub-export carries (#167). Wraps the base as
/// `SELECT * FROM (<base>) AS _rivet_split WHERE <key> > <lo> AND <key> <= <hi>`,
/// with either side omitted when its bound is `None` (the first window has no
/// floor, the last no ceil). No `ORDER BY`/`LIMIT`: the sub-export's own runner
/// (full/chunked/keyset) wraps THIS query for its paging — the window is a pure
/// row-set restriction. Both bounds use [`inline_literal`], the same
/// injection-safe form the keyset upper bound uses, so a probed boundary value
/// can never plant SQL. Returns the base unchanged when both bounds are `None`.
pub(crate) fn wrap_key_range(
    base_query: &str,
    key_column: &str,
    lo: Option<&str>,
    hi: Option<&str>,
    source_type: SourceType,
) -> String {
    if lo.is_none() && hi.is_none() {
        return base_query.to_string();
    }
    let key = quote_ident(source_type, key_column);
    let mut preds: Vec<String> = Vec::with_capacity(2);
    if let Some(lo) = lo {
        preds.push(format!("{key} > {}", inline_literal(source_type, lo)));
    }
    if let Some(hi) = hi {
        preds.push(format!("{key} <= {}", inline_literal(source_type, hi)));
    }
    format!(
        "SELECT * FROM ({base}) {from_split} WHERE {preds}",
        from_split = derived(source_type, "_rivet_split"),
        base = base_query,
        preds = preds.join(" AND "),
    )
}

/// In-SQL literal for a key value, per dialect — the injection-safe inline form
/// (never a bind param). MySQL/PG/MSSQL all implicit-cast a quoted literal to the
/// key's column type, so a numeric key compares correctly against `'250001'`.
pub(crate) fn inline_literal(source_type: SourceType, value: &str) -> String {
    match source_type {
        SourceType::Mysql => escape_mysql_literal(value),
        SourceType::Postgres => escape_pg_literal(value),
        SourceType::Mssql => escape_mssql_literal(value),
        SourceType::Oracle => escape_oracle_literal(value),
        SourceType::Mongo => unreachable!(
            "inline_literal: MongoDB keyset paging is not a SQL path (guarded by full-mode-only validation)"
        ),
    }
}

/// Dialect-appropriate "first N rows after the ORDER BY" clause for keyset
/// pages. PostgreSQL / MySQL spell it `LIMIT n`; T-SQL (SQL Server) has no
/// `LIMIT` — it uses `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` (which requires the
/// `ORDER BY` the keyset query already carries).
fn page_limit_clause(source_type: SourceType, limit: usize) -> String {
    match source_type {
        SourceType::Postgres | SourceType::Mysql => format!("LIMIT {limit}"),
        SourceType::Mssql => format!("OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"),
        SourceType::Oracle => format!("FETCH FIRST {limit} ROWS ONLY"),
        SourceType::Mongo => unreachable!(
            "page_limit_clause: MongoDB keyset paging is not a SQL path (guarded by full-mode-only validation)"
        ),
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
        // SQL Server: in-SQL `N'…'` unicode literal, server implicit-casts to the
        // column type (same rationale as Postgres — the keyset/cursor column may
        // be int, datetime2, uniqueidentifier, …). No backslash escaping in
        // T-SQL; only `'` is doubled.
        SourceType::Mssql => (escape_mssql_literal(value), None),
        // Oracle: a bind, converted to the column type through the session's pinned
        // NLS masks (the cursor's text form is rivet's own ISO rendering).
        SourceType::Oracle => (":1".to_string(), Some(value.to_string())),
        SourceType::Mongo => unreachable!(
            "cursor_rhs: MongoDB incremental cursor is not a SQL path (guarded by full-mode-only validation)"
        ),
    }
}

/// Quote `s` as an Oracle string literal: only `'` is escaped (doubled), and `N'…'`
/// keeps non-ASCII key values intact.
pub(crate) fn escape_oracle_literal(s: &str) -> String {
    // A VARCHAR2 literal, not N'…': Oracle cannot convert NVARCHAR text through the
    // pinned NLS_DATE_FORMAT's quoted parts (ORA-01830 on a DATE keyset bound).
    format!("'{}'", s.replace('\'', "''"))
}

/// Quote `s` as a T-SQL `N'…'` unicode string literal. SQL Server escapes only
/// the single quote (by doubling); backslash is a literal character (unlike
/// Postgres `E'…'`). The `N` prefix keeps non-ASCII cursor values intact.
pub(crate) fn escape_mssql_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    out.push_str("N'");
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Quote `s` as a MySQL `'…'` string literal, escaping `\` and `'` the MySQL
/// default way (`\\`, `\'`). MySQL implicit-casts the literal to the column type,
/// so a numeric keyset key compares correctly against `'250001'`. Used for the
/// INLINE upper bound of a parallel keyset range (the cursor still binds via `?`).
pub(crate) fn escape_mysql_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
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

    #[test]
    fn an_oracle_literal_is_varchar2_with_doubled_quotes() {
        assert_eq!(escape_oracle_literal("it's"), "'it''s'");
        assert_eq!(
            escape_oracle_literal("2024-01-01T00:00:00.000000"),
            "'2024-01-01T00:00:00.000000'"
        );
    }

    fn cursor_with(val: Option<&str>) -> CursorState {
        CursorState {
            export_name: "t".into(),
            last_cursor_value: val.map(str::to_string),
            last_run_at: None,
            cursor_column: None,
        }
    }

    fn single(col: &str) -> IncrementalCursorPlan {
        IncrementalCursorPlan {
            primary_column: col.into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
            settle: None,
        }
    }

    fn coalesce(p: &str, f: &str) -> IncrementalCursorPlan {
        IncrementalCursorPlan {
            primary_column: p.into(),
            fallback_column: Some(f.into()),
            mode: IncrementalCursorMode::Coalesce,
            settle: None,
        }
    }

    fn settled(mut plan: IncrementalCursorPlan, column: Option<&str>) -> IncrementalCursorPlan {
        plan.settle = Some(crate::plan::SettlePlan {
            column: column.map(str::to_string),
            after_secs: 3600,
        });
        plan
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

    // ── build_keyset_query (OPT-4) ────────────────────────────────────────────

    #[test]
    fn keyset_first_page_orders_and_limits_no_where_no_param() {
        let q = build_keyset_query("SELECT * FROM t", "id", None, 1000, SourceType::Postgres);
        assert!(
            !q.sql.contains("WHERE"),
            "first page has no WHERE: {}",
            q.sql
        );
        assert!(q.sql.contains("ORDER BY \"id\""), "{}", q.sql);
        assert!(q.sql.contains("LIMIT 1000"), "{}", q.sql);
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn keyset_bounded_upper_is_inline_cursor_stays_the_only_bind() {
        // Parallel keyset worker: `(cursor, upper]`. On MySQL the cursor binds via
        // `?` (the single param slot); the upper bound is an INLINE literal so it
        // does not consume a second bind. MUTANT: bind the upper too → cursor_param
        // would need to be a pair; this asserts it stays the single cursor value.
        let q = build_keyset_query_bounded(
            "SELECT * FROM t",
            "id",
            Some("100"),
            Some("250001"),
            500,
            SourceType::Mysql,
        );
        assert!(
            q.sql.contains("WHERE `id` > ? AND `id` <= '250001'"),
            "cursor bound `?`, upper inline: {}",
            q.sql
        );
        assert_eq!(
            q.cursor_param.as_deref(),
            Some("100"),
            "only the cursor binds; the upper is inline"
        );
        // First page of a bounded range (no cursor): WHERE is the upper only.
        let first = build_keyset_query_bounded(
            "SELECT * FROM t",
            "id",
            None,
            Some("250001"),
            500,
            SourceType::Postgres,
        );
        assert!(
            first.sql.contains("WHERE \"id\" <= E'250001'"),
            "bounded first page carries only the upper: {}",
            first.sql
        );
        assert_eq!(first.cursor_param, None);
        // No upper bound → identical to the sequential page (the default path).
        let seq = build_keyset_query_bounded(
            "SELECT * FROM t",
            "id",
            Some("9"),
            None,
            10,
            SourceType::Postgres,
        );
        assert!(
            !seq.sql.contains("<="),
            "no upper → no <= clause: {}",
            seq.sql
        );
    }

    #[test]
    fn keyset_mssql_uses_offset_fetch_with_bracket_quoting() {
        // First page (no cursor): bracket-quoted key + T-SQL paging, no LIMIT.
        let first = build_keyset_query("SELECT * FROM t", "id", None, 1000, SourceType::Mssql);
        assert!(!first.sql.contains("WHERE"), "{}", first.sql);
        assert!(first.sql.contains("ORDER BY [id]"), "{}", first.sql);
        assert!(
            first
                .sql
                .contains("OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY"),
            "T-SQL has no LIMIT: {}",
            first.sql
        );
        assert!(!first.sql.contains("LIMIT"), "{}", first.sql);

        // Subsequent page: cursor as an N'…' literal (server implicit-casts),
        // FETCH after the ORDER BY.
        let next = build_keyset_query(
            "SELECT * FROM t",
            "id",
            Some("00000000-0000-0000-0000-000000000001"),
            500,
            SourceType::Mssql,
        );
        assert!(next.sql.contains("WHERE [id] > N'"), "{}", next.sql);
        assert!(
            next.sql.contains("OFFSET 0 ROWS FETCH NEXT 500 ROWS ONLY"),
            "{}",
            next.sql
        );
        assert_eq!(next.cursor_param, None);
    }

    #[test]
    fn keyset_subsequent_page_mysql_binds_value() {
        let q = build_keyset_query("SELECT * FROM t", "id", Some("42"), 500, SourceType::Mysql);
        assert!(q.sql.contains("WHERE `id` > ?"), "{}", q.sql);
        assert!(q.sql.contains("ORDER BY `id`"), "{}", q.sql);
        assert!(q.sql.contains("LIMIT 500"), "{}", q.sql);
        assert!(
            !q.sql.contains("'42'"),
            "value must not be inlined: {}",
            q.sql
        );
        assert_eq!(q.cursor_param.as_deref(), Some("42"));
    }

    #[test]
    fn keyset_subsequent_page_postgres_embeds_escaped_literal() {
        let q = build_keyset_query(
            "SELECT * FROM t",
            "uuid",
            Some("a-b-c"),
            500,
            SourceType::Postgres,
        );
        assert!(q.sql.contains("WHERE \"uuid\" > E'a-b-c'"), "{}", q.sql);
        assert!(q.sql.contains("ORDER BY \"uuid\""), "{}", q.sql);
        assert!(q.sql.contains("LIMIT 500"), "{}", q.sql);
        assert_eq!(q.cursor_param, None);
    }

    #[test]
    fn keyset_value_with_quote_is_escaped_pg_and_bound_mysql() {
        let evil = "O'Brien";
        let pg = build_keyset_query("SELECT * FROM t", "k", Some(evil), 10, SourceType::Postgres);
        assert!(pg.sql.contains(r"E'O\'Brien'"), "{}", pg.sql);
        assert_eq!(pg.cursor_param, None);

        let my = build_keyset_query("SELECT * FROM t", "k", Some(evil), 10, SourceType::Mysql);
        assert!(
            !my.sql.contains("O'Brien"),
            "value must be bound: {}",
            my.sql
        );
        assert_eq!(my.cursor_param.as_deref(), Some(evil));
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

    #[test]
    fn settle_on_the_cursor_itself_is_a_plain_age_filter_on_first_and_later_runs() {
        let p = settled(single("updated_at"), None);
        let first = build_incremental_query("SELECT * FROM t", Some(&p), None, SourceType::Mysql);
        assert_eq!(
            first.sql,
            "SELECT * FROM (SELECT * FROM t) AS _rivet WHERE (`updated_at` IS NULL OR \
             `updated_at` < (NOW() - INTERVAL 3600 SECOND)) ORDER BY `updated_at`"
        );
        assert_eq!(first.cursor_param, None);

        let next = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("2026-09-11 10:00:00"))),
            SourceType::Mysql,
        );
        assert_eq!(
            next.sql,
            "SELECT * FROM (SELECT * FROM t) AS _rivet WHERE `updated_at` > ? AND \
             (`updated_at` IS NULL OR `updated_at` < (NOW() - INTERVAL 3600 SECOND)) \
             ORDER BY `updated_at`"
        );
        assert_eq!(next.cursor_param.as_deref(), Some("2026-09-11 10:00:00"));
        assert!(!next.sql.contains("ALL ("), "{}", next.sql);

        let explicit = settled(single("updated_at"), Some("updated_at"));
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&explicit),
            Some(&cursor_with(Some("x"))),
            SourceType::Mysql,
        );
        assert_eq!(q.sql, next.sql);
    }

    #[test]
    fn settle_threshold_is_the_source_clock_in_every_dialect() {
        let p = settled(single("ts"), None);
        let pg = build_incremental_query("SELECT * FROM t", Some(&p), None, SourceType::Postgres);
        assert!(
            pg.sql
                .contains("(\"ts\" IS NULL OR \"ts\" < (now() - INTERVAL '3600 seconds'))"),
            "{}",
            pg.sql
        );
        let ms = build_incremental_query("SELECT * FROM t", Some(&p), None, SourceType::Mssql);
        assert!(
            ms.sql
                .contains("([ts] IS NULL OR [ts] < DATEADD(SECOND, -3600, SYSUTCDATETIME()))"),
            "{}",
            ms.sql
        );
    }

    #[test]
    fn settle_on_another_column_bounds_the_cursor_below_the_first_settling_row() {
        let p = settled(single("idlink_va"), Some("server_time"));
        let q = build_incremental_query(
            "SELECT * FROM lva",
            Some(&p),
            Some(&cursor_with(Some("42"))),
            SourceType::Mysql,
        );
        assert_eq!(
            q.sql,
            "SELECT * FROM (SELECT * FROM lva) AS _rivet WHERE `idlink_va` > ? AND \
             (`server_time` IS NULL OR `server_time` < (NOW() - INTERVAL 3600 SECOND)) AND \
             `idlink_va` < ALL (SELECT _rivet_young.`idlink_va` FROM (SELECT * FROM lva) AS \
             _rivet_young WHERE _rivet_young.`idlink_va` IS NOT NULL AND \
             _rivet_young.`idlink_va` > '42' AND \
             _rivet_young.`server_time` >= (NOW() - INTERVAL 3600 SECOND)) ORDER BY `idlink_va`"
        );
        assert_eq!(q.cursor_param.as_deref(), Some("42"));

        let first = build_incremental_query("SELECT * FROM lva", Some(&p), None, SourceType::Mysql);
        assert!(
            first.sql.contains(
                "WHERE _rivet_young.`idlink_va` IS NOT NULL AND \
                 _rivet_young.`server_time` >= (NOW()"
            ),
            "{}",
            first.sql
        );
    }

    #[test]
    fn settle_tail_bound_value_is_escaped_not_spliced() {
        let p = settled(single("k"), Some("ts"));
        let evil = "1' OR '1'='1";
        let pg = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some(evil))),
            SourceType::Postgres,
        );
        assert!(pg.sql.contains(r"> E'1\' OR \'1\'=\'1'"), "{}", pg.sql);
        let ms = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some(evil))),
            SourceType::Mssql,
        );
        assert!(ms.sql.contains("> N'1'' OR ''1''=''1'"), "{}", ms.sql);
    }

    #[test]
    fn settle_in_coalesce_mode_defaults_to_the_coalesced_cursor() {
        let p = settled(coalesce("updated_at", "created_at"), None);
        let q = build_incremental_query(
            "SELECT * FROM t",
            Some(&p),
            Some(&cursor_with(Some("2024-01-01"))),
            SourceType::Postgres,
        );
        let c = "COALESCE(_rivet.\"updated_at\", _rivet.\"created_at\")";
        assert!(
            q.sql.contains(&format!(
                "WHERE {c} > E'2024-01-01' AND ({c} IS NULL OR {c} < (now() - INTERVAL '3600 seconds'))"
            )),
            "{}",
            q.sql
        );
        assert!(!q.sql.contains("ALL ("), "{}", q.sql);

        let other = settled(coalesce("updated_at", "created_at"), Some("created_at"));
        let q =
            build_incremental_query("SELECT * FROM t", Some(&other), None, SourceType::Postgres);
        assert!(
            q.sql
                .contains(&format!("{c} < ALL (SELECT COALESCE(_rivet_young.")),
            "{}",
            q.sql
        );
    }
}
