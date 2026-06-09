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

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use proxy::{detect_mssql_proxy_kind, warn_proxy_kind};

use crate::config::TlsConfig;
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
            Some(cfg) if cfg.accept_invalid_certs => config.trust_cert(),
            Some(cfg) => {
                if let Some(ca) = cfg.ca_file.as_deref() {
                    config.trust_cert_ca(ca);
                }
            }
            None => config.trust_cert(),
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
                    anyhow::bail!(
                        "mssql: statement timeout after {}s (tuning.statement_timeout_s) — \
                         this query cannot finish within the budget; split it with \
                         `mode: chunked` (per-chunk statements stay under the limit) or \
                         raise `tuning.statement_timeout_s`",
                        budget.as_secs()
                    );
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
                            let arrow_bytes =
                                emit_mssql_batch(&columns, &overrides, &mut schema, &buf, sink)?;
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
                emit_mssql_batch(&columns, &overrides, &mut schema, &buf, sink)?;
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
/// first call and reusing it thereafter. Decimal scales are recovered from the
/// data — tiberius drops a column's declared precision/scale — so the first
/// batch must carry each decimal column's first non-null value (true for every
/// table in practice; a decimal column NULL for the whole first batch falls back
/// to scale 0, same as the pre-streaming behaviour on an all-null column).
///
/// Returns the emitted batch's Arrow memory footprint (bytes), so the export
/// loop can size the memory cap from the real row width; `0` for an empty batch.
fn emit_mssql_batch(
    columns: &[tiberius::Column],
    overrides: &ColumnOverrides,
    schema: &mut Option<SchemaRef>,
    rows: &[tiberius::Row],
    sink: &mut dyn BatchSink,
) -> Result<usize> {
    let schema_ref = match schema {
        Some(s) => s.clone(),
        None => {
            let (built, _decoders) =
                arrow_convert::mssql_columns_to_schema(columns, overrides, rows)?;
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
