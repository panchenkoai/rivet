use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use super::schema_error::PreflightSchemaError;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::error::Result;

/// Connect once and build one [`ExportDiagnostic`] per export. Rendering
/// (TEXT table vs `--json`) is the caller's job in [`super::check`], so this
/// returns the diagnostics rather than printing inline.
pub(super) fn check_postgres(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
) -> Result<Vec<ExportDiagnostic>> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let db_max_connections = fetch_max_connections_pg(&mut client);

    let mut diags = Vec::with_capacity(exports.len());
    for export in exports {
        diags.push(diagnose_pg(&mut client, export, db_max_connections)?);
    }

    Ok(diags)
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_pg(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<super::ExportDiagnostic> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let db_max_connections = fetch_max_connections_pg(&mut client);
    diagnose_pg(&mut client, export, db_max_connections)
}

fn fetch_max_connections_pg(client: &mut postgres::Client) -> Option<u32> {
    let rows = match client.query("SELECT current_setting('max_connections')::int", &[]) {
        Ok(r) => r,
        Err(e) => {
            log::debug!("preflight: max_connections probe failed: {e}");
            return None;
        }
    };
    rows.first()?.get::<_, i32>(0).try_into().ok()
}

fn diagnose_pg(
    client: &mut postgres::Client,
    export: &ExportConfig,
    db_max_connections: Option<u32>,
) -> Result<ExportDiagnostic> {
    let mode_str = diagnose_mode_str(export);

    let base_query = resolve_preflight_base_query(export);
    let base_query = base_query.as_str();

    let range_col = export
        .chunk_column
        .as_deref()
        .or(export.cursor_column.as_deref());

    let effective_query = if let Some(order) = incremental_key_expr(export, SourceType::Postgres) {
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, order
        )
    } else {
        base_query.to_string()
    };

    let (row_estimate, avg_row_bytes) = estimate_rows_pg(client, &effective_query)?;

    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        if let Some(expr) = incremental_key_expr(export, SourceType::Postgres) {
            // Direct min/max on the relation when the user used the `table:`
            // shortcut — avoids the subquery wrap that PG would materialise
            // (3.2 GB of temp_files on a 8.6 GB table in our content_items bench).
            let range_query = match crate::pipeline::chunked::strip_select_star_from(base_query) {
                Some(tbl) => format!("SELECT min({expr})::text, max({expr})::text FROM {tbl}"),
                None => format!(
                    "SELECT min({expr})::text, max({expr})::text FROM ({base_query}) AS _rivet"
                ),
            };
            match client.query(&range_query, &[]) {
                Ok(rows) if !rows.is_empty() => {
                    let min_val: Option<String> = rows[0].get(0);
                    let max_val: Option<String> = rows[0].get(1);
                    (min_val, max_val)
                }
                Ok(_) => (None, None),
                Err(e) => {
                    log::debug!(
                        "preflight: incremental key range probe failed for export '{}': {e}",
                        export.name
                    );
                    (None, None)
                }
            }
        } else {
            (None, None)
        }
    } else if let Some(col) = range_col {
        get_cursor_range_pg(client, base_query, col)
    } else {
        (None, None)
    };

    let (scan_type, plan_uses_index) = analyze_plan_pg(client, &effective_query);

    // The EXPLAIN above runs against the *base* query (the whole table,
    // typically). PostgreSQL picks `Seq Scan` for a full-table read even
    // when there's a perfect btree on the chunk column — the index path is
    // genuinely slower for a full read. But the chunked/incremental run
    // does NOT issue the base query; it issues `WHERE chunk_col >= $lo
    // AND chunk_col < $hi`, which *will* use the btree.
    //
    // So for those modes, ask the catalog directly: is there a btree index
    // whose leading column is the range column? If yes, treat it as
    // indexed regardless of what EXPLAIN said for the base query. This
    // collapses the most common false-DEGRADED case (chunked on a PK).
    let uses_index = if matches!(export.mode, ExportMode::Chunked | ExportMode::Incremental)
        && let Some(col) = range_col
        && let Some(table) = export
            .table
            .as_deref()
            .or_else(|| table_from_simple_query(base_query))
    {
        match column_has_btree_pg(client, table, col) {
            Some(true) => true,
            Some(false) => plan_uses_index,
            None => plan_uses_index,
        }
    } else {
        plan_uses_index
    };

    let strategy = derive_strategy(export);
    let verdict = compute_verdict(
        row_estimate,
        uses_index,
        export.cursor_column.is_some(),
        avg_row_bytes,
        export.parallel,
    );
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let warnings = collect_warnings(
        export,
        row_estimate,
        avg_row_bytes,
        range_min.as_deref(),
        range_max.as_deref(),
        db_max_connections,
    );
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        strategy,
        mode: mode_str,
        cursor_column: export.cursor_column.clone(),
        row_estimate,
        avg_row_bytes,
        cursor_min: range_min,
        cursor_max: range_max,
        scan_type,
        uses_index,
        verdict,
        recommended_profile,
        recommended_parallel,
        warnings,
        suggestion,
    })
}

/// Returns `(row_estimate, avg_row_bytes)` parsed from one EXPLAIN — the top
/// node's `rows=` and `width=`, which describe the same projected output. One
/// plan, both numbers, so no second round-trip for the row-width estimate.
fn estimate_rows_pg(
    client: &mut postgres::Client,
    query: &str,
) -> Result<(Option<i64>, Option<i64>)> {
    let explain = format!("EXPLAIN {}", query);
    let rows = match client.query(&explain, &[]) {
        Ok(r) => r,
        Err(e) => {
            // A SQLSTATE *class 42* failure (undefined table/column, no
            // privilege, syntax) is permanent and author-fixable — let it fail
            // preflight loudly instead of sailing through to run time.
            if let Some(fail) = schema_fail_pg(&e) {
                return Err(fail);
            }
            // Everything else (08 connection, 53 resources, 55 lock, 57 cancel,
            // …) is operational and stays FAIL-SOFT: preflight is non-fatal, so
            // surface at debug (visible under `RUST_LOG=debug`) and continue.
            log::debug!("preflight: EXPLAIN for row-estimate failed: {e}");
            return Ok((None, None));
        }
    };
    let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let plan_text = lines.join("\n");
    Ok((
        parse_pg_row_estimate(&plan_text),
        parse_pg_row_width(&plan_text),
    ))
}

/// Map a Postgres SQLSTATE *class 42* (syntax/access-rule violation) connect-time
/// EXPLAIN failure to a loud, author-facing preflight error. Returns `None` for
/// every other class so operational/transient failures stay fail-soft.
fn schema_fail_pg(e: &postgres::Error) -> Option<anyhow::Error> {
    let code = e.code()?;
    // Class 42 = undefined_table (42P01), undefined_column (42703),
    // insufficient_privilege (42501), syntax_error (42601), … — all permanent
    // and fixable in the config/grants, not by retrying.
    code.code().starts_with("42").then(|| {
        // `e` (Display) is the bare "db error"; the real reason ("relation … does
        // not exist") lives on the DbError. Surface it + the SQLSTATE.
        let detail = e
            .as_db_error()
            .map(|db| db.message())
            .unwrap_or("schema/query error");
        PreflightSchemaError::new(detail, format!("SQLSTATE {}", code.code())).into_error()
    })
}

pub(crate) fn parse_pg_row_estimate(plan: &str) -> Option<i64> {
    for line in plan.lines() {
        if let Some(idx) = line.find("rows=") {
            let after = &line[idx + 5..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = num_str.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

/// Parse the estimated row `width=` (bytes) from an EXPLAIN plan — the first
/// (top-node) occurrence, so it pairs with the first `rows=` that
/// [`parse_pg_row_estimate`] reads. Both describe the same projected output
/// row, so `rows × width` estimates the bytes a chunk query transfers.
pub(crate) fn parse_pg_row_width(plan: &str) -> Option<i64> {
    for line in plan.lines() {
        if let Some(idx) = line.find("width=") {
            let after = &line[idx + 6..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = num_str.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

fn get_cursor_range_pg(
    client: &mut postgres::Client,
    base_query: &str,
    cursor_col: &str,
) -> (Option<String>, Option<String>) {
    // Quote the config-author-controlled column so a hostile value collapses to
    // a single (nonexistent) identifier instead of injecting SQL into the
    // min()/max() aggregates (CWE-89) — same defense the MSSQL sibling applies.
    let expr = crate::sql::quote_ident(SourceType::Postgres, cursor_col);
    let range_query = match crate::pipeline::chunked::strip_select_star_from(base_query) {
        Some(tbl) => format!("SELECT min({expr})::text, max({expr})::text FROM {tbl}"),
        None => {
            format!("SELECT min({expr})::text, max({expr})::text FROM ({base_query}) AS _rivet")
        }
    };
    match client.query(&range_query, &[]) {
        Ok(rows) if !rows.is_empty() => {
            let min_val: Option<String> = rows[0].get(0);
            let max_val: Option<String> = rows[0].get(1);
            (min_val, max_val)
        }
        Ok(_) => (None, None),
        Err(e) => {
            log::debug!("preflight: cursor range probe on '{cursor_col}' failed: {e}");
            (None, None)
        }
    }
}

/// Extract a `schema.table` (or bare `table`) from a single-relation
/// `SELECT … FROM <name>` query. Conservative on purpose — anything with
/// JOIN, a subquery, an alias, a CTE, or a non-identifier table name
/// returns `None` and callers fall back to the EXPLAIN-based heuristic.
///
/// Exists because `rivet init` generates `query: SELECT cols FROM tbl`
/// (not `table: tbl`) to lock the column list, so the index probe needs
/// to recover the table name from the rendered query.
pub(crate) fn table_from_simple_query(query: &str) -> Option<&str> {
    // Walk tokens after the first FROM that is *not* inside parens.
    let mut depth = 0u32;
    let mut chars = query.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                // Match `from` (case-insensitive) at this position when
                // bounded by whitespace.
                let rest = &query[idx..];
                let head_ok = idx == 0
                    || matches!(
                        query.as_bytes()[idx - 1],
                        b' ' | b'\t' | b'\n' | b'\r' | b')'
                    );
                if head_ok && rest.len() >= 5 && rest[..4].eq_ignore_ascii_case("from") {
                    let after = rest[4..].chars().next();
                    if matches!(after, Some(c) if c.is_whitespace() || c == '(') {
                        // skip 'from' + whitespace
                        let mut j = idx + 4;
                        let bytes = query.as_bytes();
                        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                            j += 1;
                        }
                        // Read one identifier (with optional `schema.` prefix);
                        // reject anything with quotes / subquery / comma.
                        let id_start = j;
                        while j < bytes.len() {
                            let b = bytes[j];
                            let id_char = b.is_ascii_alphanumeric() || b == b'_' || b == b'.';
                            if id_char {
                                j += 1;
                            } else {
                                break;
                            }
                        }
                        if j == id_start {
                            return None;
                        }
                        let token = &query[id_start..j];
                        // Reject only when this is genuinely multi-relation:
                        //   `FROM users JOIN orders …`  ← any JOIN flavor
                        //   `FROM users, orders`         ← comma list
                        // Plain aliases (`FROM users u`, `FROM users AS u`)
                        // and trailing clauses (`WHERE`, `ORDER BY`, `LIMIT`)
                        // do not change the relation — `users` is still the
                        // table the catalog probe should index-check.
                        let mut k = j;
                        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                            k += 1;
                        }
                        if k < bytes.len() {
                            let rest = &query[k..];
                            if rest.starts_with(',') {
                                return None;
                            }
                            let next_word: String = rest
                                .chars()
                                .take_while(|c| c.is_ascii_alphabetic())
                                .collect::<String>()
                                .to_ascii_lowercase();
                            // Standard SQL JOIN keywords (any prefix that
                            // precedes the join itself counts).
                            if matches!(
                                next_word.as_str(),
                                "join"
                                    | "inner"
                                    | "left"
                                    | "right"
                                    | "outer"
                                    | "full"
                                    | "cross"
                                    | "natural"
                            ) {
                                return None;
                            }
                        }
                        return Some(token);
                    }
                }
            }
            _ => {}
        }
        let _ = chars.peek();
    }
    None
}

/// True when `column` is the leading key of a `btree` index on `table`.
///
/// Range chunking (`WHERE col >= $lo AND col < $hi`) and incremental cursor
/// reads (`WHERE col > $last ORDER BY col`) only benefit from an index when
/// the column is the leading key of a btree. `gist`/`gin`/`hash`/`brin`
/// indexes do not help here. Composite btrees where our column is leading
/// (`(col, x, y)`) still do — the planner can use the leading prefix.
///
/// Returns `Some(true)` when an index is found, `Some(false)` when the
/// catalog probe ran and found none, `None` when the probe failed (e.g.
/// the `table:` shortcut wasn't used so we don't have a qualified name to
/// look up, or the user lacks SELECT on `pg_index`). Callers fall back to
/// the EXPLAIN-based heuristic in the `None` case.
pub(crate) fn column_has_btree_pg(
    client: &mut postgres::Client,
    qualified_table: &str,
    column: &str,
) -> Option<bool> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s, t),
        None => ("public", qualified_table),
    };
    // Find every btree index on (schema.table) whose attnum-0 (first key)
    // is our column. `i.indkey[0]` is the leading key's attnum;
    // `a.attnum = i.indkey[0]` joins it to the column name.
    let sql = "SELECT 1 \
               FROM pg_index i \
               JOIN pg_class c ON c.oid = i.indrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               JOIN pg_class ic ON ic.oid = i.indexrelid \
               JOIN pg_am am ON am.oid = ic.relam \
               JOIN pg_attribute a ON a.attrelid = i.indrelid \
                                  AND a.attnum  = i.indkey[0] \
               WHERE n.nspname = $1::text \
                 AND c.relname = $2::text \
                 AND a.attname = $3::text \
                 AND am.amname = 'btree' \
                 AND i.indisvalid AND i.indisready \
               LIMIT 1";
    match client.query(sql, &[&schema, &table, &column]) {
        Ok(rows) => Some(!rows.is_empty()),
        Err(e) => {
            log::debug!("preflight: btree index probe failed for {schema}.{table}.{column}: {e}");
            None
        }
    }
}

fn analyze_plan_pg(client: &mut postgres::Client, query: &str) -> (Option<String>, bool) {
    let explain = format!("EXPLAIN {}", query);
    match client.query(&explain, &[]) {
        Ok(rows) => {
            let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            let plan_text = lines.join("\n");
            let uses_index = plan_text.contains("Index Scan")
                || plan_text.contains("Index Only Scan")
                || plan_text.contains("Bitmap Index Scan");
            let scan_type = extract_scan_type(&plan_text);
            (Some(scan_type), uses_index)
        }
        Err(e) => {
            log::debug!("preflight: EXPLAIN for plan analysis failed: {e}");
            (None, false)
        }
    }
}

pub(crate) fn extract_scan_type(plan: &str) -> String {
    for line in plan.lines() {
        let trimmed = line.trim().trim_start_matches("-> ");
        if trimmed.contains("Scan") || trimmed.contains("scan") {
            return trimmed.trim_start_matches("-> ").to_string();
        }
    }
    plan.lines().next().unwrap_or("unknown").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_pg_row_estimate ────────────────────────────────────────────────

    #[test]
    fn parse_pg_row_estimate_typical_seq_scan() {
        let plan = "Seq Scan on orders  (cost=0.00..1250.00 rows=5000 width=120)";
        assert_eq!(parse_pg_row_estimate(plan), Some(5000));
    }

    #[test]
    fn parse_pg_row_estimate_nested_plan_first_rows_wins() {
        let plan = "Aggregate  (cost=0.00..50.00 rows=1 width=8)\n  ->  Seq Scan on t  (cost=0.00..100.00 rows=10000 width=4)";
        // First "rows=" is rows=1 from Aggregate
        assert_eq!(parse_pg_row_estimate(plan), Some(1));
    }

    #[test]
    fn parse_pg_row_estimate_large_row_count() {
        let plan =
            "Index Scan using idx on orders  (cost=0.00..1234567.00 rows=123456789 width=50)";
        assert_eq!(parse_pg_row_estimate(plan), Some(123_456_789));
    }

    #[test]
    fn parse_pg_row_estimate_no_rows_keyword_returns_none() {
        assert!(parse_pg_row_estimate("Seq Scan on t (cost=0.00..100.00 width=8)").is_none());
    }

    #[test]
    fn parse_pg_row_estimate_empty_plan_returns_none() {
        assert!(parse_pg_row_estimate("").is_none());
    }

    // ── parse_pg_row_width ───────────────────────────────────────────────────

    #[test]
    fn parse_pg_row_width_typical_seq_scan() {
        let plan = "Seq Scan on orders  (cost=0.00..1250.00 rows=5000 width=120)";
        assert_eq!(parse_pg_row_width(plan), Some(120));
    }

    #[test]
    fn parse_pg_row_width_top_node_wins() {
        // Top node's width pairs with the top node's rows (parse_pg_row_estimate
        // takes the first rows=, this takes the first width= — same node).
        let plan = "Sort  (cost=0.00..50.00 rows=1000 width=64)\n  ->  Seq Scan on t  (cost=0.00..100.00 rows=1000 width=200)";
        assert_eq!(parse_pg_row_width(plan), Some(64));
    }

    #[test]
    fn parse_pg_row_width_absent_returns_none() {
        assert!(parse_pg_row_width("Result  (cost=0.00..0.01 rows=1)").is_none());
        assert!(parse_pg_row_width("").is_none());
    }

    // ── extract_scan_type ────────────────────────────────────────────────────

    #[test]
    fn extract_scan_type_seq_scan() {
        let plan = "Seq Scan on orders  (cost=0.00..1000.00 rows=50000 width=8)";
        let result = extract_scan_type(plan);
        assert!(result.contains("Seq Scan"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_index_scan() {
        let plan = "  ->  Index Scan using orders_pkey on orders  (cost=0.43..8.45 rows=1 width=8)";
        let result = extract_scan_type(plan);
        assert!(result.contains("Index Scan"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_bitmap_heap_scan() {
        let plan = "  ->  Bitmap Heap Scan on orders  (cost=4.35..16.20 rows=4 width=8)";
        let result = extract_scan_type(plan);
        assert!(result.contains("Bitmap Heap Scan"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_no_scan_returns_first_line() {
        let plan = "Aggregate  (cost=10.00..10.01 rows=1 width=8)\n  ->  Sort  (...)";
        let result = extract_scan_type(plan);
        // No "Scan" in any line → returns first line trimmed
        assert!(result.starts_with("Aggregate"), "got: {result}");
    }

    #[test]
    fn extract_scan_type_empty_plan_returns_unknown() {
        assert_eq!(extract_scan_type(""), "unknown");
    }

    // ── table_from_simple_query ──────────────────────────────────────────────
    //
    // Drives the index-probe fallback that recovers the table name from
    // `query: SELECT … FROM <name>` configs (rivet init emits this shape
    // instead of `table:` to lock the column list — see preflight P1 #1).
    // The parser is intentionally conservative: anything that isn't a single
    // `SELECT cols FROM ident` must return None so we don't catalog-probe
    // the wrong relation and falsely promote a verdict.

    #[test]
    fn table_from_simple_query_bare_select() {
        assert_eq!(
            table_from_simple_query("SELECT id, name FROM users"),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_schema_qualified() {
        assert_eq!(
            table_from_simple_query("SELECT * FROM public.orders"),
            Some("public.orders")
        );
    }

    #[test]
    fn table_from_simple_query_multiline_rivet_init_shape() {
        // `rivet init` renders YAML with a `query: >` folded block that
        // becomes a single line with surrounding whitespace + newlines;
        // make sure the parser tolerates that exact shape.
        let q =
            "\nSELECT id, name, email, age, balance,\n  is_active, bio, created_at\nFROM users\n";
        assert_eq!(table_from_simple_query(q), Some("users"));
    }

    #[test]
    fn table_from_simple_query_case_insensitive_keyword() {
        assert_eq!(
            table_from_simple_query("select * from Users"),
            Some("Users")
        );
        assert_eq!(
            table_from_simple_query("Select * From users"),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_rejects_join() {
        // Multi-relation queries must fall back to the EXPLAIN heuristic —
        // catalog probing one of the tables would mislead the verdict.
        assert_eq!(
            table_from_simple_query("SELECT * FROM users JOIN orders USING (id)"),
            None
        );
        assert_eq!(table_from_simple_query("SELECT * FROM users, orders"), None);
    }

    #[test]
    fn table_from_simple_query_accepts_aliased_table() {
        // `FROM users u` and `FROM users AS u` are single-relation queries
        // with a local alias — the table is still `users`, which is what
        // the catalog probe needs to index-check. Aliases are harmless.
        assert_eq!(
            table_from_simple_query("SELECT * FROM users u"),
            Some("users")
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users AS u"),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_accepts_trailing_clauses() {
        // WHERE / ORDER BY / LIMIT don't change the relation; the table
        // before them is still the one to probe.
        assert_eq!(
            table_from_simple_query("SELECT * FROM users WHERE id > 0"),
            Some("users")
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users ORDER BY id"),
            Some("users")
        );
        assert_eq!(
            table_from_simple_query("SELECT * FROM users LIMIT 100"),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_rejects_all_join_flavors() {
        for kw in [
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
            "FULL OUTER JOIN",
            "CROSS JOIN",
            "NATURAL JOIN",
        ] {
            let q = format!("SELECT * FROM users {kw} orders ON users.id = orders.user_id");
            assert_eq!(
                table_from_simple_query(&q),
                None,
                "{kw}: should reject multi-relation"
            );
        }
    }

    #[test]
    fn table_from_simple_query_skips_subquery_from() {
        // `SELECT (SELECT max(x) FROM events) FROM users` — the `FROM events`
        // is inside parens (depth>0), so the parser should reach the outer
        // `FROM users`.
        assert_eq!(
            table_from_simple_query("SELECT (SELECT max(x) FROM events) FROM users"),
            Some("users")
        );
    }

    #[test]
    fn table_from_simple_query_subquery_only_returns_none() {
        // No top-level FROM at all → None.
        assert_eq!(
            table_from_simple_query("SELECT (SELECT max(x) FROM events)"),
            None
        );
    }

    #[test]
    fn table_from_simple_query_handles_no_from_clause() {
        // `SELECT 1` — preflight uses this as the fallback when the user
        // hasn't supplied a query yet. Must not crash, must return None.
        assert_eq!(table_from_simple_query("SELECT 1"), None);
        assert_eq!(table_from_simple_query(""), None);
    }

    // ── regression: `table:` shortcut must NOT preflight the "SELECT 1" stub ──
    //
    // Guards the root cause of audit_preflight_table (findings #3, #15): the
    // base query `diagnose_pg` probes for a `table:`-shortcut export (no
    // `query:`) is the canonical `SELECT * FROM <table>`, not the 1-row
    // placeholder. If this contract regresses, every downstream probe (row
    // estimate, scan type, cursor range) silently describes a dummy relation.

    #[test]
    fn table_shortcut_resolves_to_real_table_not_select_one() {
        let mut export = crate::config::sample_export("orders");
        export.query = None;
        export.table = Some("orders".into());
        // Same call diagnose_pg makes — config_dir/params are unused on this
        // branch. The base query the probes see must be the real table.
        let base = export
            .resolve_query(std::path::Path::new(""), None)
            .expect("table shortcut resolves");
        assert_eq!(base, "SELECT * FROM orders");
        assert_ne!(base, "SELECT 1");
        // …and it must be recognised as a single-table read so the min/max
        // range probe rewrites in place (recovering the dropped Cursor range).
        assert_eq!(
            crate::pipeline::chunked::strip_select_star_from(&base),
            Some("orders")
        );
        assert_eq!(table_from_simple_query(&base), Some("orders"));
    }

    #[test]
    fn schema_qualified_table_shortcut_resolves_to_real_table() {
        let mut export = crate::config::sample_export("orders");
        export.query = None;
        export.table = Some("public.orders".into());
        let base = export
            .resolve_query(std::path::Path::new(""), None)
            .expect("schema-qualified table shortcut resolves");
        assert_eq!(base, "SELECT * FROM public.orders");
        assert_eq!(
            crate::pipeline::chunked::strip_select_star_from(&base),
            Some("public.orders")
        );
    }

    #[test]
    fn inline_query_form_is_left_untouched() {
        // The fix must not change behavior when `query:` is provided — the
        // user's SQL flows through verbatim.
        let mut export = crate::config::sample_export("custom");
        export.table = None;
        export.query = Some("SELECT id, total FROM orders WHERE total > 0".into());
        let base = export
            .resolve_query(std::path::Path::new(""), None)
            .expect("inline query resolves");
        assert_eq!(base, "SELECT id, total FROM orders WHERE total > 0");
    }

    #[test]
    fn table_from_simple_query_rejects_quoted_identifier() {
        // `FROM "User Table"` — the parser only accepts bare identifier
        // chars (alnum / _ / .), so a double-quoted name returns None and
        // the catalog probe falls back to the EXPLAIN hint. Conservative
        // — quoted identifiers are uncommon in `rivet init` output.
        assert_eq!(
            table_from_simple_query("SELECT * FROM \"User Table\""),
            None
        );
    }
}
