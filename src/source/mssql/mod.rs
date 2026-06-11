//! **Layer: Execution** — MSSQL / SQL Server source engine.
//!
//! Third SQL engine after PostgreSQL and MySQL. The `tiberius` driver is
//! async (tokio); the `Source` trait is sync `&mut self` (ADR-0011), so each
//! `MssqlSource` owns a current-thread `tokio` runtime and `block_on`s every
//! driver call — no async leaks into the runner.
//!
//! Dialect deltas vs PG/MySQL (routed through the shared seams):
//! - identifier quoting `[col]` (`sql::quote_ident`)
//! - cursor literal `N'…'` with `''` escaping (`query::cursor_rhs`)
//! - introspection via `sys.*` catalog views
//!
//! Supported today: snapshot / incremental / chunked (range + dense) and keyset
//! (seek) export, `check --type-report`, `doctor`, chunked-mode planning. The
//! keyset page builder emits a dialect-correct
//! `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` clause (T-SQL has no `LIMIT`).

mod arrow_convert;
mod proxy;

pub use proxy::MssqlProxyKind;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use proxy::{detect_mssql_proxy_kind, warn_proxy_kind};

use crate::config::{TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::batch_controller::{
    AdaptiveBatchController, DEFAULT_BATCH_TARGET_MB, PROBE_BATCH_SIZE,
};
use crate::source::query::build_export_query;
use crate::source::{BatchSink, ExportRequest, Source, TableIntrospection};
use crate::types::{ColumnOverrides, TypeMapping};

type MssqlClient = Client<Compat<TcpStream>>;

/// SQL Server source. Owns the async driver + the runtime that drives it.
///
/// `pub` (not `pub(crate)`) so integration tests can reach `proxy_kind()` the
/// same way they reach `MysqlSource::proxy_kind()`; the rest of the type
/// carries the same "no external API contract" disclaimer as `MysqlSource`.
pub struct MssqlSource {
    rt: Runtime,
    client: MssqlClient,
    /// Pooler/gateway classification, sampled once at connect time.
    proxy_kind: MssqlProxyKind,
    /// Whether the export issued `SET LOCK_TIMEOUT` on this connection, so the
    /// `Drop` teardown knows to reset it (Epic 18 B2 — pooler-safe session).
    lock_timeout_applied: bool,
}

impl Drop for MssqlSource {
    /// Pooler-safe session teardown (Epic 18 B2). rivet never opens a
    /// transaction on this connection — every read is an autocommit `SELECT`,
    /// so there is no transaction to leave dangling across the `block_on`
    /// bridge (ADR-0011). The only session state the export mutates is
    /// `SET LOCK_TIMEOUT`; reset it to the SQL Server default (`-1`, wait
    /// indefinitely) before the connection closes so a *multiplexed* pooler
    /// that keeps the backend connection alive cannot hand our non-default
    /// `LOCK_TIMEOUT` to the next session that reuses it.
    ///
    /// Best-effort and time-boxed: after a failed read the stream is
    /// half-drained and the connection is dying anyway, so the reset (and the
    /// physical connection) just goes away; the 2 s cap guarantees `Drop`
    /// can never hang on a wedged connection.
    fn drop(&mut self) {
        if !self.lock_timeout_applied {
            return;
        }
        let Self { rt, client, .. } = self;
        let _ = rt.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.execute("SET LOCK_TIMEOUT -1", &[]),
            )
            .await
        });
    }
}

/// Parsed `sqlserver://user[:password]@host[:port]/db` connection parts.
struct MssqlUrl {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

fn parse_mssql_url(url: &str) -> Result<MssqlUrl> {
    let rest = url
        .strip_prefix("sqlserver://")
        .or_else(|| url.strip_prefix("mssql://"))
        .ok_or_else(|| anyhow::anyhow!("mssql url must start with sqlserver:// — got {url}"))?;
    // userinfo @ host:port / db   (rsplit the last '@' so a '@' in a password
    // is tolerated; '/' splits host from db).
    let (userinfo, hostpart) = rest
        .rsplit_once('@')
        .ok_or_else(|| anyhow::anyhow!("mssql url missing user@host: {url}"))?;
    let (user, password) = match userinfo.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (userinfo.to_string(), String::new()),
    };
    let (hostport, database) = hostpart
        .split_once('/')
        .map(|(h, d)| (h, d.to_string()))
        .unwrap_or((hostpart, String::new()));
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| anyhow::anyhow!("mssql url port not a number: {p}"))?,
        ),
        None => (hostport.to_string(), 1433),
    };
    if database.is_empty() {
        anyhow::bail!("mssql url must include a database: sqlserver://user:pass@host:port/<db>");
    }
    Ok(MssqlUrl {
        host,
        port,
        user,
        password,
        database,
    })
}

impl MssqlSource {
    /// Connect to SQL Server, honouring the shared `TlsConfig`. `url` is the
    /// resolved `sqlserver://user:pass@host:port/db` form. A successful return
    /// has completed a TLS login handshake and a `SELECT 1` round-trip.
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        // Refuse trust-any-cert to a remote host with no `tls:` block before any
        // dial (CWE-295): SQL Server always encrypts the login handshake, but
        // with `trust_cert` that handshake is unauthenticated, so a MITM is not
        // detected. Loopback keeps trust-cert (dev); a remote host must opt in
        // explicitly via `tls: { mode: ... }`.
        crate::source::require_tls_or_loopback(url, tls)?;
        let parts = parse_mssql_url(url)?;
        let mut config = Config::new();
        config.host(&parts.host);
        config.port(parts.port);
        config.database(&parts.database);
        config.authentication(AuthMethod::sql_server(&parts.user, &parts.password));

        // SQL Server forces TLS on the login handshake regardless; map the
        // shared TlsConfig onto tiberius' cert-trust knobs. A private CA goes
        // through `trust_cert_ca`; otherwise dev self-signed certs need
        // `trust_cert` (accept-invalid). Default keeps full verification.
        config.encryption(EncryptionLevel::Required);
        match tls {
            // `mode: disable` is the operator's explicit opt-in to an
            // unauthenticated (trust-any-cert) connection — the SQL Server
            // analogue of PG/MySQL remote plaintext. It is the documented way
            // to keep trust-cert against a remote host the gate above would
            // otherwise have refused.
            Some(cfg) if cfg.mode == TlsMode::Disable || cfg.accept_invalid_certs => {
                config.trust_cert()
            }
            Some(cfg) => {
                if let Some(ca) = cfg.ca_file.as_deref() {
                    config.trust_cert_ca(ca);
                }
            }
            None => {
                // Reached only for a LOOPBACK host (the gate above refuses a
                // remote host with no `tls:` block). On loopback, tiberius
                // trusts the server certificate without verifying issuer or
                // hostname: the handshake is encrypted but unauthenticated. That
                // is safe here because the bytes never leave the box, and it
                // keeps dev / self-signed docker setups working without opt-in.
                // Warn once, naming the config key that turns on strict
                // validation.
                static WARNED: std::sync::Once = std::sync::Once::new();
                WARNED.call_once(|| {
                    log::warn!(
                        "mssql: connecting with TLS certificate validation disabled \
                         (no `source.tls:` block) — the connection is encrypted but the \
                         server certificate is not verified (MITM not detected). Add \
                         `source.tls: {{ mode: verify-full, ca_file: <ca.pem> }}` to enable \
                         strict validation (or `mode: verify-ca` to skip only hostname checks)."
                    );
                });
                config.trust_cert();
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("mssql: tokio runtime build failed: {e}"))?;

        let client = rt.block_on(async {
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .map_err(|e| anyhow::anyhow!("mssql: TCP connect failed: {e}"))?;
            tcp.set_nodelay(true).ok();
            Client::connect(config, tcp.compat_write())
                .await
                .map_err(|e| anyhow::anyhow!("mssql: login failed: {e}"))
        })?;

        let mut src = Self {
            rt,
            client,
            proxy_kind: MssqlProxyKind::Direct,
            lock_timeout_applied: false,
        };
        // Health round-trip — surfaces auth/permission errors at connect time
        // (doctor relies on this).
        src.query_scalar("SELECT 1")?;
        // Best-effort pooler/gateway detection (mirrors PG `pg_backend_pid`
        // drift and MySQL `CONNECTION_ID()` drift): one warning at connect
        // time, never breaks the export. Disjoint borrows of `rt` (&) and
        // `client` (&mut).
        let kind = detect_mssql_proxy_kind(&src.rt, &mut src.client);
        warn_proxy_kind(kind);
        src.proxy_kind = kind;
        Ok(src)
    }

    /// Expose the proxy classification for diagnostics (preflight, integration
    /// tests). Not part of the `Source` trait — same internal-may-change
    /// contract as the rest of `rivet::source::mssql::*`.
    #[allow(dead_code)]
    pub fn proxy_kind(&self) -> MssqlProxyKind {
        self.proxy_kind
    }

    /// Declared `(precision, scale)` per decimal/numeric column, read from
    /// `sys.columns`, for a simple single-table `SELECT … FROM [schema.]table`.
    /// `None` for any query the FROM parser does not handle (joins, comma lists,
    /// subqueries) or when the lookup fails — the schema builder then falls back
    /// to data-inference, today's behaviour. Never fails the export: a lookup
    /// error is logged (mirrors `pg_numeric_catalog_hints_opt`) and downgraded
    /// to `None`.
    fn mssql_decimal_catalog_hints_opt(
        &mut self,
        query: &str,
    ) -> Option<HashMap<String, (u8, i8)>> {
        let (schema, table) = parse_mssql_simple_from_table(query)?;
        match self.fetch_mssql_decimal_catalog_hints(&schema, &table) {
            Ok(m) => m,
            Err(e) => {
                // The parser identified a single-table query but the catalog
                // lookup itself failed (permissions, gateway). Surface it —
                // otherwise a downstream decimal scale-0 freeze on an all-NULL
                // first batch looks like a config problem when the real cause is
                // a missing `sys.columns` read here.
                log::warn!(
                    "mssql decimal catalog lookup failed for {schema}.{table} — decimal scale \
                     will fall back to first-batch inference (declare it with a `columns:` \
                     override if an all-NULL first batch truncates it): {e}"
                );
                None
            }
        }
    }

    /// Probe `sys.columns` for each `decimal`/`numeric` column's declared
    /// `(precision, scale)`. Joined through `sys.schemas`/`sys.objects` so the
    /// `(schema, table)` pair resolves the exact base table the export reads.
    fn fetch_mssql_decimal_catalog_hints(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<Option<HashMap<String, (u8, i8)>>> {
        // `decimal` and `numeric` are synonyms in SQL Server and share one
        // `sys.types` entry per scale; filter on the base type name so only
        // fixed-point columns (not money / int / float) carry a hint.
        let sql = format!(
            "SELECT c.name, c.precision, c.scale \
             FROM sys.columns c \
             JOIN sys.types t ON t.user_type_id = c.user_type_id \
             JOIN sys.objects o ON o.object_id = c.object_id \
             JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE s.name = N'{}' AND o.name = N'{}' \
             AND t.name IN ('decimal', 'numeric')",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );
        let Self { rt, client, .. } = self;
        let rows = rt.block_on(async {
            client
                .query(sql.as_str(), &[])
                .await
                .map_err(|e| anyhow::anyhow!("mssql: sys.columns probe failed: {e}"))?
                .into_first_result()
                .await
                .map_err(|e| anyhow::anyhow!("mssql: reading sys.columns rows failed: {e}"))
        })?;

        let mut map = HashMap::new();
        for row in &rows {
            // sys.columns: name = sysname (nvarchar), precision/scale = tinyint.
            // `try_get` (not `get`) so an unexpected cell type downgrades to a
            // skipped hint rather than panicking the export.
            let name: Option<&str> = row.try_get(0).ok().flatten();
            let precision: Option<u8> = row.try_get(1).ok().flatten();
            let scale: Option<u8> = row.try_get(2).ok().flatten();
            if let (Some(name), Some(p), Some(s)) = (name, precision, scale)
                && let Some(pair) = catalog_decimal_to_params(p, s)
            {
                map.insert(name.to_string(), pair);
            }
        }

        if map.is_empty() {
            Ok(None)
        } else {
            log::debug!(
                "mssql decimal catalog: resolved {} DECIMAL/NUMERIC column(s) for {schema}.{table}",
                map.len(),
            );
            Ok(Some(map))
        }
    }
}

/// Convert `sys.columns` `(precision, scale)` into Rivet `decimal(p, s)`
/// parameters, rejecting anything outside the bounds the YAML overrides accept.
/// SQL Server caps precision at 38 and scale ≤ precision, so a well-formed
/// catalog row always passes; the guard defends against a degenerate row.
fn catalog_decimal_to_params(precision: u8, scale: u8) -> Option<(u8, i8)> {
    if precision == 0 || precision > 38 {
        return None;
    }
    if scale > precision || scale > i8::MAX as u8 {
        return None;
    }
    Some((precision, scale as i8))
}

/// Extract the `(schema, table)` of a simple single-table T-SQL
/// `SELECT … FROM [schema.]table` (no joins, no comma list, no subquery in
/// `FROM`). Returns `None` for anything more complex — the caller falls back to
/// data-inference rather than guessing. Schema defaults to `dbo` when the table
/// is unqualified. Handles `[bracketed]` and bare identifiers; pure `&str`
/// work, so it is unit-testable without a live server.
fn parse_mssql_simple_from_table(query: &str) -> Option<(String, String)> {
    let from_idx = mssql_find_outer_from_keyword(query)?;
    let tail = trim_sql_ws(query.get(from_idx + 4..)?);
    let (first, after1) = parse_mssql_ident_piece(tail)?;
    let after1 = trim_sql_ws(after1);
    // `schema.table` (optionally `db.schema.table` → take the last two parts).
    let (schema, table, after) = if after1.starts_with('.') {
        let (second, after2) = parse_mssql_ident_piece(trim_sql_ws(after1.get(1..)?))?;
        let after2 = trim_sql_ws(after2);
        if after2.starts_with('.') {
            // db.schema.table — `first` is the database, drop it.
            let (third, after3) = parse_mssql_ident_piece(trim_sql_ws(after2.get(1..)?))?;
            (second, third, trim_sql_ws(after3))
        } else {
            (first, second, after2)
        }
    } else {
        ("dbo".to_string(), first, after1)
    };
    // Reject joins / comma-lists / a trailing dotted continuation we didn't
    // consume; only a clause boundary (WHERE/ORDER/…/end) or an alias may follow.
    let after = skip_mssql_optional_alias(after)?;
    if mssql_joins_or_comma(after) {
        return None;
    }
    Some((schema, table))
}

fn trim_sql_ws(s: &str) -> &str {
    s.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r'))
}

fn is_sql_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Case-insensitive keyword match at byte `idx` with identifier-boundary checks
/// on both sides (so `from_x` does not match `from`).
fn sql_keyword_at(haystack: &[u8], idx: usize, kw_lower: &[u8]) -> bool {
    let n = kw_lower.len();
    if idx + n > haystack.len() || !haystack[idx..idx + n].eq_ignore_ascii_case(kw_lower) {
        return false;
    }
    let before_ok = idx == 0 || !is_sql_ident_byte(haystack[idx - 1]);
    let after_ok = idx + n >= haystack.len() || !is_sql_ident_byte(haystack[idx + n]);
    before_ok && after_ok
}

/// Byte offset of the top-level `FROM`, skipping nested parentheses
/// (subqueries) and `'…'` string literals (with `''` escapes).
fn mssql_find_outer_from_keyword(sql: &str) -> Option<usize> {
    let b = sql.as_bytes();
    let mut i = 0usize;
    let mut depth = 0usize;
    let mut in_quote = false;
    while i < b.len() {
        if in_quote {
            if b[i] == b'\'' {
                if i + 1 < b.len() && b[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_quote = false;
                    i += 1;
                }
                continue;
            }
            i += 1;
            continue;
        }
        match b[i] {
            b'\'' => in_quote = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && sql_keyword_at(b, i, b"from") => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Parse one T-SQL identifier piece: `[bracketed name]` (with `]]` escapes) or
/// a bare `ident`. Returns the unquoted name and the remaining tail.
fn parse_mssql_ident_piece(rest: &str) -> Option<(String, &str)> {
    let rest = trim_sql_ws(rest);
    if let Some(after_open) = rest.strip_prefix('[') {
        let mut out = String::new();
        let mut chars = after_open.chars();
        while let Some(ch) = chars.next() {
            if ch == ']' {
                if chars.as_str().starts_with(']') {
                    chars.next();
                    out.push(']');
                    continue;
                }
                return Some((out, chars.as_str()));
            }
            out.push(ch);
        }
        return None; // unterminated bracket
    }
    let bytes = rest.as_bytes();
    if bytes.is_empty() || (!bytes[0].is_ascii_alphabetic() && bytes[0] != b'_') {
        return None;
    }
    let mut i = 1usize;
    while i < bytes.len() && is_sql_ident_byte(bytes[i]) {
        i += 1;
    }
    Some((rest.get(0..i)?.to_string(), rest.get(i..)?))
}

/// `true` when a join / comma-list follows the relation — the parser rejects
/// these (catalog hints only resolve for a single base table).
fn mssql_joins_or_comma(rest: &str) -> bool {
    let r = trim_sql_ws(rest);
    if r.starts_with(',') || r.starts_with('.') {
        return true;
    }
    let b = r.as_bytes();
    ["inner", "left", "right", "full", "cross", "join"]
        .iter()
        .any(|kw| sql_keyword_at(b, 0, kw.as_bytes()))
}

/// Consume an optional table alias (`[AS] alias`) after the relation, stopping
/// at a clause boundary. Returns the tail after the alias, or `None` if what
/// follows is a join/comma (so the caller rejects the query).
fn skip_mssql_optional_alias(rest: &str) -> Option<&str> {
    let rest = trim_sql_ws(rest);
    if rest.is_empty() || mssql_starts_clause_boundary(rest) || mssql_joins_or_comma(rest) {
        return Some(rest);
    }
    let mut rest = rest;
    if sql_keyword_at(rest.as_bytes(), 0, b"as") {
        rest = trim_sql_ws(rest.get(2..)?);
    }
    let (_, tail) = parse_mssql_ident_piece(rest)?;
    Some(trim_sql_ws(tail))
}

fn mssql_starts_clause_boundary(rest: &str) -> bool {
    let r = trim_sql_ws(rest);
    if r.is_empty() {
        return true;
    }
    const KWS: &[&[u8]] = &[
        b"where",
        b"group",
        b"having",
        b"order",
        b"union",
        b"except",
        b"intersect",
        b"for",
        b"option",
        b"offset",
    ];
    let b = r.as_bytes();
    KWS.iter().any(|kw| sql_keyword_at(b, 0, kw))
}

impl Source for MssqlSource {
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()> {
        // Keyset (seek) pages build a dialect-correct
        // `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` clause (T-SQL has no `LIMIT`).
        let built = build_export_query(request, crate::config::SourceType::Mssql);
        let sql = built.sql.clone();
        let overrides = request.column_overrides.clone();
        // Stream the result one Arrow batch at a time (peak RSS ≈ one batch,
        // independent of `chunk_size`) through the shared `AdaptiveBatchController`
        // — it starts at a probe size and caps the batch to a memory target once
        // the real row width is known (the cap is computed in the loop). The SQL
        // Server analogue of the PostgreSQL cursor's `FETCH N`. (`adaptive` resize
        // is a no-op here: a single streaming connection can't sample DB pressure
        // mid-stream; the OPT-2 concurrency governor handles that at the chunk
        // layer instead.)
        let mut ctl =
            AdaptiveBatchController::new(request.tuning, request.tuning.batch_size.max(1));
        let mut cap_applied = false;
        // Source-safety knobs (parity with the PG/MySQL export loops):
        //  - lock_timeout → server-side `SET LOCK_TIMEOUT` so a blocked read
        //    fails fast instead of waiting on a writer's lock indefinitely.
        //  - statement_timeout → enforced client-side: SQL Server has no
        //    statement-duration `SET` (unlike PG's `statement_timeout` / MySQL's
        //    `max_execution_time`), so we stop pulling and error out once the
        //    wall-clock budget is spent. The half-drained stream is dropped with
        //    the (errored) source, so nothing leaks.
        //  - throttle_ms → applied by the controller between batches.
        let lock_timeout_ms = request.tuning.lock_timeout_s.saturating_mul(1000);
        let stmt_timeout = (request.tuning.statement_timeout_s > 0)
            .then(|| std::time::Duration::from_secs(request.tuning.statement_timeout_s));

        // Resolve declared decimal precision/scale from `sys.columns` for the
        // *unwrapped* base query (the chunk/keyset wrapper hides the source
        // table from the FROM parser, so resolve from the base — same restriction
        // as PG's catalog hints). `None` ⇒ not a simple single-table SELECT, so
        // the schema builder falls back to data-inference, today's behaviour.
        let hint_query = request.catalog_hint_query.unwrap_or(request.query);
        let decimal_hints = self.mssql_decimal_catalog_hints_opt(hint_query);

        // Record that we are about to mutate session state so `Drop` resets it
        // (Epic 18 B2). Set before the disjoint-borrow destructure below.
        if lock_timeout_ms > 0 {
            self.lock_timeout_applied = true;
        }

        let Self { rt, client, .. } = self;
        rt.block_on(async {
            use futures_util::stream::TryStreamExt;
            use tiberius::QueryItem;

            if lock_timeout_ms > 0 {
                client
                    .execute(format!("SET LOCK_TIMEOUT {lock_timeout_ms}"), &[])
                    .await
                    .map_err(|e| anyhow::anyhow!("mssql: SET LOCK_TIMEOUT failed: {e}"))?;
            }

            let started = std::time::Instant::now();
            let mut stream = client
                .query(sql.as_str(), &[])
                .await
                .map_err(|e| anyhow::anyhow!("mssql: query failed: {e}"))?;

            let mut columns: Vec<tiberius::Column> = Vec::new();
            let mut buf: Vec<tiberius::Row> = Vec::with_capacity(ctl.target());
            let mut schema: Option<SchemaRef> = None;

            while let Some(item) = stream
                .try_next()
                .await
                .map_err(|e| anyhow::anyhow!("mssql: streaming rows failed: {e}"))?
            {
                if let Some(budget) = stmt_timeout
                    && started.elapsed() > budget
                {
                    // Typed marker (not a bare string): the retry classifier
                    // downcasts the TYPE → permanent, so a reworded message can
                    // never silently make this deterministic timeout retryable.
                    // Its Display carries the same actionable hint for the user.
                    return Err(crate::source::StatementDurationTimeout::mssql(
                        budget.as_secs(),
                    )
                    .into());
                }
                match item {
                    // A single SELECT yields one metadata token (the column
                    // shape) ahead of its rows.
                    QueryItem::Metadata(meta) if columns.is_empty() => {
                        columns = meta.columns().to_vec();
                    }
                    QueryItem::Metadata(_) => {}
                    QueryItem::Row(row) => {
                        buf.push(row);
                        if buf.len() >= ctl.target() {
                            let arrow_bytes = emit_mssql_batch(
                                &columns,
                                &overrides,
                                decimal_hints.as_ref(),
                                &mut schema,
                                &buf,
                                sink,
                            )?;
                            let n = buf.len();
                            buf.clear();
                            // First batch: cap to a memory target now that the
                            // real Arrow width is known (same probe→cap the
                            // PG/MySQL loops do, clamped to the configured
                            // batch_size by the controller).
                            if !cap_applied && n > 0 {
                                let arrow_per_row = (arrow_bytes / n).max(64);
                                let target_mb = request
                                    .tuning
                                    .batch_size_memory_mb
                                    .unwrap_or(DEFAULT_BATCH_TARGET_MB);
                                let safe = ((target_mb * 1024 * 1024) / arrow_per_row)
                                    .max(PROBE_BATCH_SIZE);
                                if let Some(new) = ctl.apply_memory_cap(safe) {
                                    log::info!(
                                        "MSSQL batch cap: arrow≈{} B/row, target={} MB → batch_size → {}",
                                        arrow_per_row,
                                        target_mb,
                                        new
                                    );
                                    buf.reserve(new.saturating_sub(buf.capacity()));
                                }
                                cap_applied = true;
                            }
                            // adaptive no-op mid-stream (sample → None); throttle.
                            ctl.after_batch(|| None);
                            ctl.throttle();
                        }
                    }
                }
            }
            // Final partial batch — or, for an empty result set, a single call
            // that still emits the (empty) schema so the sink writes a
            // correctly-typed empty output. Rows arrive in the query's
            // `ORDER BY` order, so the last batch's last row carries the max
            // cursor the sink extracts.
            if !buf.is_empty() || schema.is_none() {
                emit_mssql_batch(
                    &columns,
                    &overrides,
                    decimal_hints.as_ref(),
                    &mut schema,
                    &buf,
                    sink,
                )?;
            }
            Ok::<_, anyhow::Error>(())
        })?;
        Ok(())
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            let row = client
                .query(sql, &[])
                .await
                .map_err(|e| anyhow::anyhow!("mssql: scalar query failed: {e}"))?
                .into_row()
                .await
                .map_err(|e| anyhow::anyhow!("mssql: reading scalar row failed: {e}"))?;
            Ok(row.and_then(|r| scalar_to_string(&r)))
        })
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        // Zero-row wrapper so the server returns column metadata without a scan.
        let wrapped = format!("SELECT * FROM ({query}) AS _rivet_q WHERE 1 = 0");
        let overrides = column_overrides.clone();
        let Self { rt, client, .. } = self;
        rt.block_on(async {
            let mut stream = client
                .query(wrapped.as_str(), &[])
                .await
                .map_err(|e| anyhow::anyhow!("mssql: type-probe query failed: {e}"))?;
            let columns = stream
                .columns()
                .await
                .map_err(|e| anyhow::anyhow!("mssql: type-probe metadata failed: {e}"))?
                .map(<[_]>::to_vec)
                .unwrap_or_default();
            // Drain so the connection is reusable.
            let _ = stream.into_first_result().await;
            Ok(arrow_convert::mssql_type_mappings(&columns, &overrides))
        })
    }

    fn sample_pressure(&mut self) -> Option<u64> {
        let Self { rt, client, .. } = self;
        // Extraction-pressure proxy (Epic 18 C2): cumulative `Workfiles Created`
        // + `Worktables Created` (SQLServer:Access Methods). A workfile /
        // worktable is created when a sort or hash spills to tempdb — the SQL
        // Server analogue of PG `temp_bytes` / MySQL `Created_tmp_disk_tables`.
        // The `cntr_value` of these `*/sec`-named perfmon counters is the raw
        // cumulative count, so their sum is monotonic — exactly what the governor
        // compares deltas of. Replaces `Log Flush Waits`, which is redo-**write**
        // pressure and barely moves during a read-only export. Instance-level
        // (no per-database `instance_name`), so no parameter is bound.
        let sql = "SELECT SUM(cntr_value) FROM sys.dm_os_performance_counters \
                   WHERE counter_name IN ('Workfiles Created/sec', 'Worktables Created/sec')";
        rt.block_on(async {
            let row = client.query(sql, &[]).await.ok()?.into_row().await.ok()??;
            row.get::<i64, _>(0).map(|v| v.max(0) as u64)
        })
    }
}

/// Emit one Arrow batch from `rows`, building (and emitting) the schema on the
/// first call and reusing it thereafter. tiberius drops a decimal column's
/// declared precision/scale, so the scale is recovered from the `decimal_hints`
/// catalog lookup (the upstream, lossless source that survives an all-NULL
/// first batch); only an expression/computed column with no catalog entry falls
/// back to inferring the scale from the first batch's data.
///
/// Returns the emitted batch's Arrow memory footprint (bytes), so the export
/// loop can size the memory cap from the real row width; `0` for an empty batch.
fn emit_mssql_batch(
    columns: &[tiberius::Column],
    overrides: &ColumnOverrides,
    decimal_hints: Option<&HashMap<String, (u8, i8)>>,
    schema: &mut Option<SchemaRef>,
    rows: &[tiberius::Row],
    sink: &mut dyn BatchSink,
) -> Result<usize> {
    let schema_ref = match schema {
        Some(s) => s.clone(),
        None => {
            let (built, _decoders) =
                arrow_convert::mssql_columns_to_schema(columns, overrides, rows, decimal_hints)?;
            let s: SchemaRef = Arc::new(built);
            sink.on_schema(s.clone())?;
            *schema = Some(s.clone());
            s
        }
    };
    if !rows.is_empty() {
        let batch = arrow_convert::mssql_rows_to_record_batch(&schema_ref, rows)?;
        let bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
        sink.on_batch(&batch)?;
        return Ok(bytes);
    }
    Ok(0)
}

/// Render a row's first column as a display string for `query_scalar`
/// (min/max bounds, COUNT(*), SELECT 1). Covers the scalar shapes the planner
/// asks for; richer typing flows through the export path, not here.
fn scalar_to_string(row: &tiberius::Row) -> Option<String> {
    use tiberius::ColumnData;
    let cell = row.cells().next().map(|(_, d)| d)?;
    match cell {
        ColumnData::U8(v) => v.map(|x| x.to_string()),
        ColumnData::I16(v) => v.map(|x| x.to_string()),
        ColumnData::I32(v) => v.map(|x| x.to_string()),
        ColumnData::I64(v) => v.map(|x| x.to_string()),
        ColumnData::F32(v) => v.map(|x| x.to_string()),
        ColumnData::F64(v) => v.map(|x| x.to_string()),
        ColumnData::Bit(v) => v.map(|x| x.to_string()),
        ColumnData::String(v) => v.as_ref().map(|s| s.to_string()),
        ColumnData::Numeric(v) => v.map(|n| {
            // unscaled value with an inserted decimal point at `scale`.
            let raw = n.value();
            let scale = n.scale() as usize;
            if scale == 0 {
                raw.to_string()
            } else {
                let neg = raw < 0;
                let digits = raw.unsigned_abs().to_string();
                let digits = format!("{digits:0>width$}", width = scale + 1);
                let (int, frac) = digits.split_at(digits.len() - scale);
                format!("{}{int}.{frac}", if neg { "-" } else { "" })
            }
        }),
        ColumnData::Guid(v) => v.map(|g| g.to_string()),
        other => Some(format!("{other:?}")),
    }
}

/// Probe `sys.*` for the stats chunked-mode planning needs (ADR-0015 seam).
/// Mirrors `introspect_pg_table_for_chunking` / `introspect_mysql_table_for_chunking`.
pub(crate) fn introspect_mssql_table_for_chunking(
    url: &str,
    tls: Option<&TlsConfig>,
    qualified_table: &str,
) -> Result<TableIntrospection> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("dbo".to_string(), qualified_table.to_string()),
    };
    let mut src = MssqlSource::connect_with_tls(url, tls)?;

    // Row estimate from `sys.dm_db_partition_stats` (rows in the heap/clustered
    // index, index_id 0/1).
    let count_sql = format!(
        "SELECT SUM(p.row_count) FROM sys.dm_db_partition_stats p \
         JOIN sys.objects o ON o.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{}' AND o.name = N'{}' AND p.index_id IN (0,1)",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    );
    let row_estimate = src
        .query_scalar(&count_sql)?
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    // Single-column integer PK → range chunking. `sys.indexes (is_primary_key)`
    // + one `index_columns` row + an integer base type.
    let pk_sql = format!(
        "SELECT TOP 1 c.name, t.name FROM sys.indexes i \
         JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         JOIN sys.types t ON t.user_type_id = c.user_type_id \
         JOIN sys.objects o ON o.object_id = i.object_id \
         JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE i.is_primary_key = 1 AND s.name = N'{}' AND o.name = N'{}' \
         GROUP BY c.name, t.name HAVING COUNT(*) = 1",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    );
    // Keyset keys (OPT-4) — parity with `postgres/mod.rs:314-340`: every
    // single-column, NOT NULL, UNIQUE index (the PK *plus* any unique
    // constraint/index), PK-first and de-duplicated, not just the PK. SQL
    // Server: `sys.indexes.is_unique = 1`, exactly one key column
    // (`ic.key_ordinal > 0` + `HAVING COUNT(*) = 1`), and the column is NOT NULL
    // — so `ORDER BY key LIMIT n` is an index range scan and `WHERE key > last`
    // never skips dup keys. Aggregated with a `CHAR(31)` (unit-separator)
    // delimiter because the introspection seam only exposes `query_scalar`; that
    // byte cannot appear in a real identifier, so the split is unambiguous.
    let keyset_sql = format!(
        "SELECT STRING_AGG(col, CHAR(31)) WITHIN GROUP (ORDER BY is_pk DESC, col) FROM ( \
           SELECT col, MAX(is_pk) AS is_pk FROM ( \
             SELECT MIN(c.name) AS col, MAX(CONVERT(int, i.is_primary_key)) AS is_pk \
             FROM sys.indexes i \
             JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0 \
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
             JOIN sys.objects o ON o.object_id = i.object_id \
             JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE i.is_unique = 1 AND c.is_nullable = 0 AND s.name = N'{}' AND o.name = N'{}' \
             GROUP BY i.object_id, i.index_id HAVING COUNT(*) = 1 \
           ) per_index GROUP BY col \
         ) deduped",
        schema.replace('\'', "''"),
        table.replace('\'', "''")
    );
    let keyset_keys: Vec<String> = src
        .query_scalar(&keyset_sql)?
        .map(|s| {
            s.split('\u{1f}')
                .filter(|c| !c.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    // Single-column integer PK → range chunking. Its own probe (the keyset list
    // above doesn't carry the type, and range-chunk eligibility needs it).
    let mut single_int_pk = None;
    if let Some(pk_col) = src.query_scalar(&pk_sql)? {
        // The scalar query returns only the column name; re-probe the type to
        // decide range-chunk eligibility.
        let type_sql = format!(
            "SELECT t.name FROM sys.columns c \
             JOIN sys.types t ON t.user_type_id = c.user_type_id \
             JOIN sys.objects o ON o.object_id = c.object_id \
             JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE s.name = N'{}' AND o.name = N'{}' AND c.name = N'{}'",
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
            pk_col.replace('\'', "''")
        );
        if let Some(ty) = src.query_scalar(&type_sql)?
            && matches!(ty.as_str(), "tinyint" | "smallint" | "int" | "bigint")
        {
            single_int_pk = Some(pk_col);
        }
    }

    Ok(TableIntrospection {
        single_int_pk,
        keyset_keys,
        row_estimate,
        avg_row_bytes: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{catalog_decimal_to_params, parse_mssql_simple_from_table};

    fn parse(q: &str) -> Option<(String, String)> {
        parse_mssql_simple_from_table(q)
    }

    #[test]
    fn parse_unqualified_table_defaults_to_dbo() {
        assert_eq!(
            parse("SELECT id, amount FROM transactions ORDER BY id"),
            Some(("dbo".into(), "transactions".into()))
        );
    }

    #[test]
    fn parse_schema_qualified() {
        assert_eq!(
            parse("SELECT id FROM sales.orders WHERE id > 1"),
            Some(("sales".into(), "orders".into()))
        );
    }

    #[test]
    fn parse_db_schema_table_takes_last_two() {
        assert_eq!(
            parse("SELECT * FROM mydb.sales.orders"),
            Some(("sales".into(), "orders".into()))
        );
    }

    #[test]
    fn parse_bracketed_identifiers() {
        assert_eq!(
            parse("SELECT * FROM [my schema].[order items]"),
            Some(("my schema".into(), "order items".into()))
        );
    }

    #[test]
    fn parse_table_with_alias() {
        assert_eq!(
            parse("SELECT t.id FROM transactions AS t WHERE t.x = 1"),
            Some(("dbo".into(), "transactions".into()))
        );
        assert_eq!(
            parse("SELECT t.id FROM transactions t ORDER BY t.id"),
            Some(("dbo".into(), "transactions".into()))
        );
    }

    #[test]
    fn parse_rejects_join() {
        assert_eq!(parse("SELECT * FROM a INNER JOIN b ON a.id = b.id"), None);
        assert_eq!(parse("SELECT * FROM a JOIN b ON a.id = b.id"), None);
    }

    #[test]
    fn parse_rejects_comma_list() {
        assert_eq!(parse("SELECT * FROM a, b WHERE a.id = b.id"), None);
    }

    #[test]
    fn parse_rejects_subquery_from() {
        assert_eq!(parse("SELECT * FROM (SELECT * FROM t) AS s"), None);
    }

    #[test]
    fn parse_ignores_from_inside_string_literal() {
        // The first top-level FROM is the real one, not the literal's bytes.
        assert_eq!(
            parse("SELECT 'from x', amount FROM ledger WHERE note = 'paid from cash'"),
            Some(("dbo".into(), "ledger".into()))
        );
    }

    #[test]
    fn catalog_bounds_accept_well_formed_and_reject_degenerate() {
        // DECIMAL(10,2) — the bug's column — rides through losslessly.
        assert_eq!(catalog_decimal_to_params(10, 2), Some((10, 2)));
        // SQL Server max precision.
        assert_eq!(catalog_decimal_to_params(38, 0), Some((38, 0)));
        assert_eq!(catalog_decimal_to_params(38, 38), Some((38, 38)));
        // Degenerate rows are rejected (defends against a corrupt catalog row),
        // so the builder falls back to data-inference rather than emitting a
        // nonsensical decimal type.
        assert_eq!(catalog_decimal_to_params(0, 0), None);
        assert_eq!(catalog_decimal_to_params(39, 0), None);
        assert_eq!(catalog_decimal_to_params(10, 11), None);
    }
}
