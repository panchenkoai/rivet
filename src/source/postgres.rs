use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chrono::Timelike as _;
use postgres::types::{FromSql as PgFromSql, Json, Kind, Type};
use postgres::{Client, NoTls, Row};
use serde_json::Value as JsonValue;

use crate::config::{SourceType, TlsConfig};
use crate::error::Result;
use crate::source::pg_numeric_wire::{PgNumericWire, numeric_wire_normalized_plain};
use crate::source::query::build_incremental_query;
use crate::source::tls::build_native_tls;
use crate::tuning::{ADAPTIVE_SAMPLE_INTERVAL, SourceTuning, next_adaptive_batch_size};
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field,
};

/// Canonical hyphenated lowercase UUID strings from PostgreSQL `uuid` rows.
///
/// Servers usually send UUIDs as 16 raw bytes under the binary protocol, but defensive
/// text parsing avoids silent null exports if a client ever exposes the text form.
#[derive(Clone)]
struct PgUuidDisplayed(String);

impl<'a> PgFromSql<'a> for PgUuidDisplayed {
    fn accepts(ty: &Type) -> bool {
        ty == &Type::UUID
    }

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() == 16 {
            let uuid = uuid::Uuid::from_slice(raw)?;
            return Ok(Self(uuid.to_hyphenated().to_string()));
        }
        let text = simdutf8::basic::from_utf8(raw)?.trim();
        Ok(Self(
            uuid::Uuid::parse_str(text)?.to_hyphenated().to_string(),
        ))
    }
}

fn pg_numeric_optional_utf8_string(row: &Row, col_idx: usize) -> Result<Option<String>> {
    match row.try_get::<_, Option<PgNumericWire<'_>>>(col_idx)? {
        None => Ok(None),
        Some(w) => Ok(numeric_raw_to_optional_decimal_text(w.0)),
    }
}

fn numeric_raw_to_optional_decimal_text(raw: &[u8]) -> Option<String> {
    numeric_wire_normalized_plain(raw).or_else(|| {
        let text = simdutf8::basic::from_utf8(raw).ok()?.trim();
        (!text.is_empty()).then(|| text.to_owned())
    })
}

pub struct PostgresSource {
    client: Client,
    /// True when two consecutive pg_backend_pid() calls returned different values,
    /// indicating a transaction-mode connection pooler (pgBouncer, Odyssey, etc.).
    transaction_pooler: bool,
}

/// Detect whether the connection is going through a transaction-mode pooler
/// (pgBouncer, Odyssey, etc.) by comparing backend PIDs across two implicit
/// transactions. Returns true when PIDs differ — impossible on a direct
/// connection or session-mode pooler where the same physical backend is kept.
///
/// False negatives are possible when pool_size = 1 (the same backend is always
/// reused), so this is a best-effort warning rather than a hard guarantee.
fn detect_pg_transaction_pooler(client: &mut Client) -> bool {
    let pid1: Option<i32> = client
        .query_one("SELECT pg_backend_pid()", &[])
        .ok()
        .and_then(|r| r.try_get(0).ok());
    let pid2: Option<i32> = client
        .query_one("SELECT pg_backend_pid()", &[])
        .ok()
        .and_then(|r| r.try_get(0).ok());
    matches!((pid1, pid2), (Some(a), Some(b)) if a != b)
}

impl PostgresSource {
    /// Connect with no transport security (legacy path). Prefer [`Self::connect_with_tls`]
    /// for production workloads so credentials and result sets are not visible on the wire.
    pub fn connect(url: &str) -> Result<Self> {
        let mut client = Client::connect(url, NoTls)?;
        let transaction_pooler = detect_pg_transaction_pooler(&mut client);
        if transaction_pooler {
            log::warn!(
                "transaction-mode connection pooler detected (pgBouncer/Odyssey) — \
                 SET LOCAL tuning is transaction-scoped; \
                 LISTEN/NOTIFY and advisory locks are unavailable"
            );
        }
        Ok(Self {
            client,
            transaction_pooler,
        })
    }

    /// Connect honoring the user's [`TlsConfig`]. When `tls.mode` is
    /// [`TlsMode::Disable`] this falls back to [`Self::connect`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let connector = build_native_tls(cfg)?;
                let make_tls = postgres_native_tls::MakeTlsConnector::new(connector);
                let mut client = Client::connect(url, make_tls)?;
                let transaction_pooler = detect_pg_transaction_pooler(&mut client);
                if transaction_pooler {
                    log::warn!(
                        "transaction-mode connection pooler detected (pgBouncer/Odyssey) — \
                         SET LOCAL tuning is transaction-scoped; \
                         LISTEN/NOTIFY and advisory locks are unavailable"
                    );
                }
                Ok(Self {
                    client,
                    transaction_pooler,
                })
            }
            _ => Self::connect(url),
        }
    }
}

/// RAII guard for an open `BEGIN ... COMMIT` block.
///
/// `commit()` runs `COMMIT` and marks the txn done; if the guard is dropped
/// before `commit()` (early return, `?`-bubbled error, or panic-driven unwind),
/// `Drop` issues a best-effort `ROLLBACK`. Postgres releases any open cursors
/// as part of ROLLBACK, so the cursor declared inside the txn is also cleaned
/// up. Closes the **G1** gap from the DBA audit (cursor leak on panic).
struct PgTxnGuard<'a> {
    client: &'a mut Client,
    committed: bool,
}

impl<'a> PgTxnGuard<'a> {
    fn begin(client: &'a mut Client) -> Result<Self> {
        client.batch_execute("BEGIN")?;
        Ok(Self {
            client,
            committed: false,
        })
    }

    fn client_mut(&mut self) -> &mut Client {
        self.client
    }

    fn commit(mut self) -> Result<()> {
        self.client.batch_execute("COMMIT")?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PgTxnGuard<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Err(e) = self.client.batch_execute("ROLLBACK")
        {
            // Drop must not panic. Worst case the connection is poisoned and
            // the pool recycles it; log so operators see it.
            log::warn!("PgTxnGuard: ROLLBACK during drop failed: {e:#}");
        }
    }
}

/// Sample `checkpoints_req` from `pg_stat_bgwriter`.
///
/// PostgreSQL caches the statistics snapshot at the start of each transaction.
/// We call `pg_stat_clear_snapshot()` first to discard that cache so every
/// adaptive sample sees fresh counters rather than the frozen value from BEGIN.
fn pg_sample_checkpoints_req(client: &mut Client) -> Option<i64> {
    let _ = client.execute("SELECT pg_stat_clear_snapshot()", &[]);
    client
        .query_one("SELECT checkpoints_req FROM pg_stat_bgwriter", &[])
        .ok()
        .and_then(|r| r.try_get::<_, i64>(0).ok())
}

/// Open a bare `postgres::Client` honoring the configured TLS policy.
///
/// Shared by preflight, doctor, and init so every code path that connects to
/// Postgres applies the same transport-security rules. `tls = None` or
/// `mode: disable` falls back to the insecure `NoTls` transport — a warning is
/// logged from `create_source` so operators know TLS is off.
pub(crate) fn connect_client(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => {
            let connector = build_native_tls(cfg)?;
            let make_tls = postgres_native_tls::MakeTlsConnector::new(connector);
            Ok(Client::connect(url, make_tls)?)
        }
        _ => Ok(Client::connect(url, NoTls)?),
    }
}

/// Run the full export transaction against an open Postgres client.
///
/// All session-mutating SET commands use SET LOCAL so they are scoped to
/// the transaction and reset automatically on COMMIT or ROLLBACK. The caller
/// is responsible for issuing ROLLBACK if this function returns Err.
///
/// Returns (total_rows, had_schema). had_schema is false only when the query
/// returned zero rows; the caller must emit an empty schema in that case.
fn pg_run_export(
    client: &mut Client,
    built_sql: &str,
    tuning: &SourceTuning,
    column_overrides: &ColumnOverrides,
    sink: &mut dyn super::BatchSink,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> Result<(usize, bool)> {
    // Open the txn under guard *first* — if SET LOCAL or DECLARE fails below,
    // Drop will roll back. Without the guard, a failure between BEGIN and the
    // explicit ROLLBACK in the caller would leak a half-set-up txn into the pool.
    let mut guard = PgTxnGuard::begin(client)?;
    if tuning.statement_timeout_s > 0 {
        guard.client_mut().batch_execute(&format!(
            "SET LOCAL statement_timeout = '{}s'",
            tuning.statement_timeout_s
        ))?;
    }
    if tuning.lock_timeout_s > 0 {
        guard.client_mut().batch_execute(&format!(
            "SET LOCAL lock_timeout = '{}s'",
            tuning.lock_timeout_s
        ))?;
    }
    guard
        .client_mut()
        .batch_execute(&format!("DECLARE _rivet NO SCROLL CURSOR FOR {built_sql}"))?;

    let mut fetch_size = tuning.batch_size;
    let mut fetch_sql = format!("FETCH {} FROM _rivet", fetch_size);
    let mut schema: Option<SchemaRef> = None;
    let mut columns_cache: Option<Vec<(String, Type)>> = None;
    let mut total_rows: usize = 0;
    let mut base_fetch_size = fetch_size;
    let mut adaptive_last_ckpt: Option<i64> = if tuning.adaptive {
        pg_sample_checkpoints_req(guard.client_mut())
    } else {
        None
    };
    let mut batch_count: usize = 0;

    loop {
        let rows = guard.client_mut().query(&fetch_sql, &[])?;
        if rows.is_empty() {
            break;
        }

        if schema.is_none() {
            let stmt_cols: Vec<(String, Type)> = rows[0]
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_().clone()))
                .collect();
            let s = Arc::new(pg_columns_to_schema(
                rows[0].columns(),
                column_overrides,
                numeric_hints,
            )?);
            sink.on_schema(s.clone())?;
            schema = Some(s.clone());
            columns_cache = Some(stmt_cols);

            let effective = tuning.effective_batch_size(Some(&s));
            if effective != fetch_size {
                fetch_size = effective;
                fetch_sql = format!("FETCH {} FROM _rivet", fetch_size);
            }
            base_fetch_size = fetch_size;
        }

        let row_count = rows.len();
        total_rows += row_count;

        let s = schema.as_ref().expect("schema set on first iteration");
        let cols = columns_cache
            .as_ref()
            .expect("columns set on first iteration");
        let batch = rows_to_record_batch_typed(s, cols, &rows)?;
        drop(rows);
        sink.on_batch(&batch)?;

        batch_count += 1;
        if tuning.adaptive
            && batch_count.is_multiple_of(ADAPTIVE_SAMPLE_INTERVAL)
            && let Some(cur) = pg_sample_checkpoints_req(guard.client_mut())
        {
            let under_pressure = adaptive_last_ckpt.is_some_and(|prev| cur > prev);
            adaptive_last_ckpt = Some(cur);
            let next = next_adaptive_batch_size(fetch_size, base_fetch_size, under_pressure);
            if next != fetch_size {
                fetch_size = next;
                fetch_sql = format!("FETCH {} FROM _rivet", fetch_size);
                log::info!(
                    "adaptive batch size → {} ({})",
                    fetch_size,
                    if under_pressure {
                        "pressure"
                    } else {
                        "recovery"
                    }
                );
            }
        }

        log::info!("fetched {} rows so far...", total_rows);

        if row_count < fetch_size {
            break;
        }

        if tuning.throttle_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(tuning.throttle_ms));
        }
    }

    // Explicit CLOSE is technically redundant — COMMIT releases the cursor —
    // but it documents intent and surfaces any close errors before COMMIT.
    guard.client_mut().batch_execute("CLOSE _rivet")?;
    guard.commit()?;
    Ok((total_rows, schema.is_some()))
}

impl super::Source for PostgresSource {
    fn export(
        &mut self,
        request: &super::ExportRequest<'_>,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_incremental_query(
            request.query,
            request.incremental,
            request.cursor,
            SourceType::Postgres,
        );
        debug_assert!(
            built.cursor_param.is_none(),
            "Postgres path inlines cursor values as E'…' literals — binding is unused"
        );
        log::debug!(
            "executing query (connection={}): {}",
            if self.transaction_pooler {
                "transaction-pooler"
            } else {
                "direct"
            },
            built.sql
        );

        let numeric_hints = pg_numeric_catalog_hints_opt(&mut self.client, request.query);

        // PgTxnGuard inside pg_run_export rolls the txn back automatically on
        // any error or panic, so no explicit ROLLBACK is needed here.
        let (total_rows, had_schema) = pg_run_export(
            &mut self.client,
            &built.sql,
            request.tuning,
            request.column_overrides,
            sink,
            numeric_hints.as_ref(),
        )?;

        if !had_schema {
            sink.on_schema(Arc::new(Schema::empty()))?;
        }

        log::info!("total: {} rows", total_rows);
        Ok(())
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let rows = self.client.query(sql, &[])?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];
        if let Ok(Some(v)) = row.try_get::<_, Option<i64>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<i32>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(0) {
            return Ok(Some(v.to_string()));
        }
        // TIMESTAMP / DATE / TIMESTAMPTZ — required for MIN/MAX on time columns (e.g. chunk_by_days)
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(0) {
            return Ok(Some(v.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDate>>(0) {
            return Ok(Some(v.format("%Y-%m-%d").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(0) {
            return Ok(Some(v.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<String>>(0) {
            return Ok(Some(v));
        }
        Ok(None)
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
        let stmt = self.client.prepare(&wrapped)?;
        let hints = pg_numeric_catalog_hints_opt(&mut self.client, query);
        let mappings = stmt
            .columns()
            .iter()
            .map(|col| {
                let rivet = rivet_type_for_pg_column(col, column_overrides, hints.as_ref());
                let source = SourceColumn::simple(col.name(), col.type_().name(), true);
                TypeMapping::from_source(&source, rivet)
            })
            .collect();
        Ok(mappings)
    }
}

/// When the query is a single-table `SELECT … FROM rel` (no joins, no subquery
/// in `FROM`), PostgreSQL result metadata does not carry `NUMERIC` typmod, but
/// `information_schema` / the table DDL does. We resolve the base relation with
/// a small parser and fetch declared precision/scale so `rivet init`-style
/// exports work without hand-written `columns:` overrides.
fn pg_numeric_catalog_hints_opt(
    client: &mut Client,
    query: &str,
) -> Option<HashMap<String, (u8, i8)>> {
    match pg_fetch_numeric_catalog_hints(client, query) {
        Ok(m) => m,
        Err(e) => {
            log::debug!("PG numeric catalog lookup skipped: {e}");
            None
        }
    }
}

fn pg_fetch_numeric_catalog_hints(
    client: &mut Client,
    query: &str,
) -> crate::error::Result<Option<HashMap<String, (u8, i8)>>> {
    let Some(regclass_lit) = try_parse_pg_simple_from_regclass_literal(query) else {
        return Ok(None);
    };
    let locate_sql = "SELECT n.nspname::text, c.relname::text \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = $1::regclass";
    let row_opt = match client.query_opt(locate_sql, &[&regclass_lit]) {
        Ok(r) => r,
        Err(e) => {
            log::debug!("PG numeric catalog: '{regclass_lit}' regclass lookup: {e}");
            return Ok(None);
        }
    };
    let Some(row) = row_opt else {
        return Ok(None);
    };
    let schema: String = row.get(0);
    let table: String = row.get(1);
    let rows = client.query(
        "SELECT column_name::text, data_type::text, numeric_precision, numeric_scale \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        &[&schema, &table],
    )?;

    let mut map = HashMap::new();
    for row in rows {
        let col: String = row.get(0);
        let dt: String = row.get(1);
        if !is_pg_numeric_information_type(&dt) {
            continue;
        }
        let p: Option<i32> = row.get(2);
        let s: Option<i32> = row.get(3);
        if let (Some(p), Some(s)) = (p, s)
            && let Some(pair) = catalog_numeric_to_decimal_params(p, s)
        {
            map.insert(col, pair);
        }
    }

    if map.is_empty() {
        Ok(None)
    } else {
        log::debug!(
            "PG numeric catalog: resolved {} DECIMAL/NUMERIC column(s) for relation {regclass_lit}",
            map.len(),
        );
        Ok(Some(map))
    }
}

fn is_pg_numeric_information_type(dt: &str) -> bool {
    let d = dt.trim().to_ascii_lowercase();
    matches!(d.as_str(), "numeric" | "decimal")
        || d.starts_with("numeric(")
        || d.starts_with("decimal(")
}

/// Match Rivet YAML `decimal(p,s)` / Arrow limits (same bound as overrides).
fn catalog_numeric_to_decimal_params(precision: i32, scale: i32) -> Option<(u8, i8)> {
    if precision <= 0 || precision > 76 {
        return None;
    }
    let precision_u = precision as u8;
    if scale < i32::from(i8::MIN) || scale > i32::from(i8::MAX) {
        return None;
    }
    let scale_i = scale as i8;
    if scale_i > precision as i8 {
        return None;
    }
    Some((precision_u, scale_i))
}

fn trim_sql_ascii_ws(s: &str) -> &str {
    s.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r'))
}

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

fn try_parse_pg_simple_from_regclass_literal(query: &str) -> Option<String> {
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

/// Map a PostgreSQL wire-protocol type to Rivet's canonical type.
///
/// This is the authoritative PostgreSQL → RivetType function. All other code
/// must go through here rather than constructing Arrow types directly.
///
/// Key decisions vs. the old `pg_type_to_arrow`:
/// - Unbounded server `NUMERIC` (OID only in row metadata) yields `Unsupported`
///   unless overwritten by YAML `columns:` or by [`pg_fetch_numeric_catalog_hints`]
///   for simple single-table selects (precision/scale from `information_schema`).
/// - `TIMESTAMPTZ` → `Timestamp { timezone: Some("UTC") }` instead of `None`
///   (roadmap §13: TIMESTAMPTZ must carry UTC semantics into Arrow/Parquet).
/// - `UUID` / `JSON` / `JSONB` → `Uuid` / `Json` variants, so `build_arrow_field`
///   attaches the `rivet.logical_type` metadata for downstream consumers.
fn pg_type_to_rivet(t: &Type) -> RivetType {
    match *t {
        Type::BOOL => RivetType::Bool,
        Type::INT2 => RivetType::Int16,
        Type::INT4 => RivetType::Int32,
        Type::INT8 => RivetType::Int64,
        // OID is u32; Int64 is a safe widening and avoids introducing a UInt32
        // variant that has no natural downstream type in most warehouses.
        Type::OID => RivetType::Int64,
        Type::FLOAT4 => RivetType::Float32,
        Type::FLOAT8 => RivetType::Float64,

        // The postgres wire protocol does NOT carry atttypmod (precision/scale)
        // in RowDescription for arbitrary queries — only the OID is available.
        // For unbounded server `NUMERIC`, see [`rivet_type_for_pg_column`] + catalog
        // hints; this arm is the final fallback when no declared precision exists.
        Type::NUMERIC => RivetType::Unsupported {
            native_type: "numeric".into(),
            reason: "precision/scale unavailable from query metadata and catalog lookup; \
                     use a column override (e.g. columns: amount: decimal(18,2)), \
                     or a single-table SELECT ... FROM schema.table \
                     when the DDL declares numeric precision."
                .into(),
        },

        Type::DATE => RivetType::Date,
        Type::TIME => RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        },
        Type::TIMESTAMP => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: None,
        },
        // Roadmap §13: TIMESTAMPTZ is always normalized to UTC.
        Type::TIMESTAMPTZ => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        },

        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => RivetType::String,
        Type::BYTEA => RivetType::Binary,

        // Roadmap §14: JSON/JSONB → Utf8 + rivet.logical_type=json metadata.
        Type::JSON | Type::JSONB => RivetType::Json,
        // Roadmap §14: UUID → Utf8 + rivet.logical_type=uuid metadata.
        Type::UUID => RivetType::Uuid,

        // Roadmap §13: interval → IntervalMonthDayNano.
        Type::INTERVAL => RivetType::Interval,

        _ => match t.kind() {
            // M6: PG enum → Utf8 + metadata logical=enum.
            Kind::Enum(_) => RivetType::Enum,
            // M6: 1-D arrays → List(inner). Nested arrays fall through to Unsupported.
            Kind::Array(elem_type) => RivetType::List {
                inner: Box::new(pg_type_to_rivet(elem_type)),
            },
            _ => RivetType::Unsupported {
                native_type: t.name().to_string(),
                reason: "no Rivet mapping for this PostgreSQL type".into(),
            },
        },
    }
}

fn rivet_type_for_pg_column(
    col: &postgres::Column,
    column_overrides: &ColumnOverrides,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> RivetType {
    if let Some(t) = column_overrides.get(col.name()) {
        return t.clone();
    }
    if *col.type_() == Type::NUMERIC
        && let Some(hints) = numeric_hints
        && let Some(&(p, s)) = hints.get(col.name())
    {
        return RivetType::Decimal {
            precision: p,
            scale: s,
        };
    }
    pg_type_to_rivet(col.type_())
}

/// Build an Arrow `Schema` from PostgreSQL `Column` descriptors by routing
/// each column through the `SourceColumn → RivetType → TypeMapping → Field`
/// pipeline.
///
/// `column_overrides` takes priority over autodetection: if the user declared
/// e.g. `amount: decimal(18,2)` in `rivet.yaml`, that `RivetType` replaces
/// the autodetected `Unsupported` for `NUMERIC` columns.
///
/// Returns `Err` for any column that has no safe Rivet mapping and no override,
/// rather than silently exporting wrong data as Utf8. When `numeric_catalog_hints`
/// is populated (simple single-table `SELECT … FROM`), declared `numeric(p,s)` from
/// the catalog is merged before falling back to `Unsupported`.
fn pg_columns_to_schema(
    columns: &[postgres::Column],
    column_overrides: &ColumnOverrides,
    numeric_catalog_hints: Option<&HashMap<String, (u8, i8)>>,
) -> crate::error::Result<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();
    for col in columns {
        let rivet = rivet_type_for_pg_column(col, column_overrides, numeric_catalog_hints);
        let source = SourceColumn::simple(col.name(), col.type_().name(), true);
        let mapping = TypeMapping::from_source(&source, rivet);
        match build_arrow_field(&mapping) {
            Some(field) => fields.push(field),
            None => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this PostgreSQL type",
                };
                errors.push(format!(
                    "  • {} (PG type '{}'): {reason}",
                    col.name(),
                    col.type_().name()
                ));
            }
        }
    }
    if !errors.is_empty() {
        anyhow::bail!(
            "{} column(s) have no safe Rivet mapping — add column overrides in rivet.yaml:\n\
             columns:\n{}",
            errors.len(),
            errors.join("\n")
        );
    }
    Ok(Schema::new(fields))
}

/// Reads a PostgreSQL `INTERVAL` value from its 16-byte binary wire format:
///   bytes 0–7  (i64 big-endian): microseconds within day
///   bytes 8–11 (i32 big-endian): days
///   bytes 12–15 (i32 big-endian): months
struct PgInterval {
    microseconds: i64,
    days: i32,
    months: i32,
}

impl<'a> postgres_types::FromSql<'a> for PgInterval {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err(format!("expected 16-byte interval, got {}", raw.len()).into());
        }
        let microseconds = i64::from_be_bytes(raw[0..8].try_into()?);
        let days = i32::from_be_bytes(raw[8..12].try_into()?);
        let months = i32::from_be_bytes(raw[12..16].try_into()?);
        Ok(Self {
            microseconds,
            days,
            months,
        })
    }
    fn accepts(ty: &postgres_types::Type) -> bool {
        *ty == postgres_types::Type::INTERVAL
    }
}

/// Serialise a PostgreSQL INTERVAL to an ISO 8601 duration string.
///
/// Arrow `Interval(MonthDayNano)` cannot be written to Parquet, so we emit
/// Utf8 instead.  The three components map as:
///   months → years + months  (e.g. 14 → "P1Y2M")
///   days   → days            (e.g. 3  → "3D")
///   µs     → T…H…M…S        (e.g. 90_061_000_000 → "T25H1M1S")
fn pg_interval_to_iso8601(months: i32, days: i32, microseconds: i64) -> String {
    use std::fmt::Write as _;
    let years = months / 12;
    let m = months % 12;
    let mut s = String::from("P");
    if years != 0 {
        write!(s, "{years}Y").ok();
    }
    if m != 0 {
        write!(s, "{m}M").ok();
    }
    if days != 0 {
        write!(s, "{days}D").ok();
    }
    if microseconds != 0 {
        let neg = microseconds < 0;
        let abs = microseconds.unsigned_abs();
        let h = abs / 3_600_000_000;
        let r = abs % 3_600_000_000;
        let mi = r / 60_000_000;
        let r2 = r % 60_000_000;
        let sec = r2 / 1_000_000;
        let us = r2 % 1_000_000;
        let sign = if neg { "-" } else { "" };
        s.push('T');
        if h != 0 {
            write!(s, "{sign}{h}H").ok();
        }
        if mi != 0 {
            write!(s, "{sign}{mi}M").ok();
        }
        if us != 0 {
            write!(s, "{sign}{sec}.{us:06}S").ok();
        } else if sec != 0 || (h == 0 && mi == 0) {
            write!(s, "{sign}{sec}S").ok();
        }
    }
    if s == "P" {
        s.push_str("T0S");
    }
    s
}

/// Generic wrapper that reads any Postgres binary value as a UTF-8 string.
/// Used for enum types whose OID is not a standard text OID.
struct AnyAsString(String);

impl<'a> postgres_types::FromSql<'a> for AnyAsString {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(AnyAsString(simdutf8::basic::from_utf8(raw)?.to_string()))
    }
    fn accepts(_ty: &postgres_types::Type) -> bool {
        true
    }
}

fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    columns: &[(String, Type)],
    rows: &[Row],
) -> Result<RecordBatch> {
    use arrow::record_batch::RecordBatch;
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());
    for (col_idx, (_name, pg_type)) in columns.iter().enumerate() {
        let target_type = schema.field(col_idx).data_type();
        arrays.push(build_array(pg_type, target_type, col_idx, rows)?);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
}

fn build_array(
    pg_type: &Type,
    target_type: &DataType,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    match *pg_type {
        Type::BOOL => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::INT2 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::INT4 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::INT8 | Type::OID => {
            let mut b = Int64Builder::with_capacity(rows.len());
            if *pg_type == Type::OID {
                for row in rows {
                    b.append_option(row.get::<_, Option<u32>>(col_idx).map(|v| v as i64));
                }
            } else {
                for row in rows {
                    b.append_option(row.get(col_idx));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::FLOAT4 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::FLOAT8 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.get(col_idx);
                b.append_option(val.as_deref());
            }
            Ok(Arc::new(b.finish()))
        }
        Type::BYTEA => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.get::<_, Option<Vec<u8>>>(col_idx) {
                    Some(v) => b.append_value(&v),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::DATE => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::NaiveDate>>(col_idx) {
                    Some(d) => {
                        let epoch =
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                        b.append_value((d - epoch).num_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::TIME => {
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::NaiveTime>>(col_idx) {
                    Some(t) => {
                        let micros = t.num_seconds_from_midnight() as i64 * 1_000_000
                            + t.nanosecond() as i64 / 1_000;
                        b.append_value(micros);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::TIMESTAMP => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::NaiveDateTime>>(col_idx) {
                    Some(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Roadmap §13: TIMESTAMPTZ is normalized to UTC. The Arrow array carries
        // the UTC timezone tag so Parquet writes TIMESTAMP_MICROS(isAdjustedToUTC=true).
        Type::TIMESTAMPTZ => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(col_idx) {
                    Some(ts) => b.append_value(ts.timestamp_micros()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish().with_timezone("UTC")))
        }
        // UUID: hyphenated Utf8 (`uuid::Uuid` only covers the 16‑byte encoding).
        Type::UUID => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 48);
            for row in rows {
                match row.try_get::<_, Option<PgUuidDisplayed>>(col_idx)? {
                    None => b.append_null(),
                    Some(display) => b.append_value(&display.0),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // JSON / JSONB: Utf8 preserving JSON semantics — `postgres` rejects `String` for these OIDs,
        // so deserialize via [`Json`] rather than emitting silent null arrays.
        Type::JSON | Type::JSONB => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.try_get::<_, Option<Json<JsonValue>>>(col_idx)? {
                    None => b.append_null(),
                    Some(Json(v)) => {
                        let s = serde_json::to_string(&v)?;
                        b.append_value(&s);
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // NUMERIC: exact Decimal128/256 path when the user declared precision/scale
        // via a column override; otherwise fall back to Utf8 (schema already carries
        // rivet.fidelity=unsupported metadata so downstream tooling can detect it).
        Type::NUMERIC => match target_type {
            DataType::Decimal128(p, s) => pg_numeric_to_decimal128(*p, *s, col_idx, rows),
            DataType::Decimal256(p, s) => pg_numeric_to_decimal256(*p, *s, col_idx, rows),
            _ => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                for row in rows {
                    let val = pg_numeric_optional_utf8_string(row, col_idx)?;
                    b.append_option(val.as_deref());
                }
                Ok(Arc::new(b.finish()))
            }
        },
        _ => {
            // INTERVAL → Utf8 ISO 8601 (Interval(MonthDayNano) is not Parquet-writable).
            if *pg_type == Type::INTERVAL {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 12);
                for row in rows {
                    match row.try_get::<_, Option<PgInterval>>(col_idx).ok().flatten() {
                        Some(iv) => b.append_value(pg_interval_to_iso8601(
                            iv.months,
                            iv.days,
                            iv.microseconds,
                        )),
                        None => b.append_null(),
                    }
                }
                return Ok(Arc::new(b.finish()));
            }

            let kind = pg_type.kind();
            // M6: 1-D arrays → Arrow List via Vec<T> deserialization
            if matches!(kind, Kind::Array(_)) {
                return build_pg_list_array(target_type, col_idx, rows);
            }

            // M6: Enum types → Utf8 (read binary enum label as UTF-8)
            if matches!(kind, Kind::Enum(_)) {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match row
                        .try_get::<_, Option<AnyAsString>>(col_idx)
                        .ok()
                        .flatten()
                    {
                        Some(s) => b.append_value(&s.0),
                        None => b.append_null(),
                    }
                }
                return Ok(Arc::new(b.finish()));
            }

            log::warn!("unmapped PG type {:?}, extracting as text", pg_type);
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.try_get(col_idx).ok().flatten();
                b.append_option(val.as_deref());
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

/// Build an Arrow `ListArray` from a PostgreSQL array column.
///
/// Dispatches to `Vec<T>` deserialization based on the Arrow element type.
/// Supports: bool, int16/32/64, float32/64, text. Other element types fall
/// back to a null list.
fn build_pg_list_array(
    target_type: &DataType,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    let inner_dt = if let DataType::List(field_ref) = target_type {
        field_ref.data_type()
    } else {
        anyhow::bail!("build_pg_list_array called with non-List target type");
    };

    macro_rules! list_of {
        ($T:ty, $Builder:ty) => {{
            let mut lb = ListBuilder::new(<$Builder>::new());
            for row in rows {
                match row.try_get::<_, Option<Vec<$T>>>(col_idx).ok().flatten() {
                    Some(v) => {
                        for x in &v {
                            lb.values().append_value(*x);
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                }
            }
            Ok(Arc::new(lb.finish()))
        }};
    }

    match inner_dt {
        DataType::Boolean => list_of!(bool, BooleanBuilder),
        DataType::Int16 => list_of!(i16, Int16Builder),
        DataType::Int32 => list_of!(i32, Int32Builder),
        DataType::Int64 => list_of!(i64, Int64Builder),
        DataType::Float32 => list_of!(f32, Float32Builder),
        DataType::Float64 => list_of!(f64, Float64Builder),
        DataType::Utf8 => {
            let mut lb = ListBuilder::new(StringBuilder::new());
            for row in rows {
                match row
                    .try_get::<_, Option<Vec<String>>>(col_idx)
                    .ok()
                    .flatten()
                {
                    Some(v) => {
                        for s in &v {
                            lb.values().append_value(s);
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                }
            }
            Ok(Arc::new(lb.finish()))
        }
        other => {
            log::warn!(
                "PG array: unsupported element type {:?}, writing null list",
                other
            );
            let mut lb = ListBuilder::new(StringBuilder::new());
            for _ in rows {
                lb.append(false);
            }
            Ok(Arc::new(lb.finish()))
        }
    }
}

/// Decode a single `NUMERIC` cell to a scaled `i128` for Arrow `Decimal128`.
///
/// The driver transmits `numeric` columns in Postgres wire binary (`numeric_recv`).
/// We decode exactly (via [`crate::source::pg_numeric_wire`]), then stringify for
/// [`crate::types::decimal::decimal_str_to_scaled_i128`] — never through `f64`.
/// A trivial `Utf8` fallback remains for unconventional cast-to-text callers.
fn pg_numeric_optional_scaled_i128(row: &Row, col_idx: usize, scale: i8) -> Result<Option<i128>> {
    use crate::types::decimal::decimal_str_to_scaled_i128;

    match row.try_get::<_, Option<PgNumericWire<'_>>>(col_idx) {
        Ok(Some(wire)) => match numeric_wire_normalized_plain(wire.0) {
            Some(plain) => {
                let t = plain.trim();
                if t.is_empty() {
                    return Ok(None);
                }
                decimal_str_to_scaled_i128(t, scale)
                    .map(Some)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "cannot parse DECIMAL {:?} as decimal(scale={scale}) after binary decode",
                            t
                        )
                    })
            }
            None => Err(anyhow::anyhow!(
                "PostgreSQL NUMERIC: unsupported NaN/infinity payload (column idx {col_idx})",
            )),
        },
        Ok(None) => Ok(None),
        Err(_) => {
            if let Ok(Some(s)) = row.try_get::<_, Option<String>>(col_idx) {
                let t = s.trim();
                if t.is_empty() {
                    return Ok(None);
                }
                return decimal_str_to_scaled_i128(t, scale)
                    .map(Some)
                    .ok_or_else(|| {
                        anyhow::anyhow!("cannot parse {:?} as decimal(scale={scale})", t)
                    });
            }
            Ok(None)
        }
    }
}

/// Build a `Decimal128Array` from a PostgreSQL `NUMERIC` column.
fn pg_numeric_to_decimal128(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    let mut b = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match pg_numeric_optional_scaled_i128(row, col_idx, scale)? {
            Some(v) => b.append_value(v),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

/// Build a `Decimal256Array` for precision > 38 (roadmap §12).
fn pg_numeric_to_decimal256(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    use arrow::datatypes::i256;
    let mut b = Decimal256Builder::with_capacity(rows.len());
    for row in rows {
        match pg_numeric_optional_scaled_i128(row, col_idx, scale)? {
            Some(v) => b.append_value(i256::from_i128(v)),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

// RecordBatch is only used inside this file; import it locally to avoid
// polluting the module-level use block.
use arrow::record_batch::RecordBatch;

#[cfg(test)]
mod pg_from_parse_tests {
    use super::{catalog_numeric_to_decimal_params, try_parse_pg_simple_from_regclass_literal};

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

    #[test]
    fn catalog_decimal_bounds() {
        assert_eq!(catalog_numeric_to_decimal_params(18, 2), Some((18, 2)));
        assert!(catalog_numeric_to_decimal_params(0, 2).is_none());
        assert!(catalog_numeric_to_decimal_params(77, 0).is_none());
        assert!(catalog_numeric_to_decimal_params(18, 19).is_none());
    }
}
