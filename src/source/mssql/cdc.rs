//! SQL Server CDC adapter — `cdc.*` change-table poll → canonical
//! [`cdc::ChangeEvent`]. The structural outlier: no client-streamable log over
//! TDS, so this polls the change tables the server's capture Agent extracted, by
//! LSN window — plain T-SQL over `tiberius` (no CDC-specific crate exists or is
//! needed).
//!
//! `next_change` polls the change function in bounded LSN batches into a buffer
//! and drains it (memory is O(batch), not O(total window)); a
//! continuous daemon wraps [`crate::source::cdc::run`] in an outer poll loop. The
//! runtime + connection are held by the stream (paid once, not per poll).
//!
//! Captured source columns are read generically from each change row
//! (`Row::cells()`) into typed `RivetValue`s — ints/bool/float/string/binary,
//! numeric → exact decimal text, temporal via tiberius+chrono structural
//! `try_get` (no manual DateTime2-increment math). Mirrors `mssql::arrow_convert`.
//!
//! Prereqs (heaviest of the three): CDC enabled, **SQL Server Agent running**,
//! supported edition (not Express). A stalled Agent freezes the change tables AND
//! pins log truncation — a real reader must detect a non-advancing max LSN.
//!
//! `#![allow(dead_code)]`: consumed by `cli::dispatch` (binary crate); the lib
//! crate compiles `source` for tests but has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::VecDeque;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::json;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::{TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};
use crate::source::require_tls_or_loopback;

/// Build one poll's T-SQL. `bound` pins `@max` to the open-time ceiling
/// (`0x{hex}`, a bounded `until_current` run) instead of re-reading
/// `sys.fn_cdc_get_max_lsn()` per poll (the daemon's chase-the-head mode) — the
/// termination contract of [`MssqlChangeStream::bound`], pure so the bounded
/// shape is asserted without a server.
fn fill_sql(ci: &str, from_expr: &str, batch: i64, bound: Option<&str>) -> String {
    let max_expr = match bound {
        Some(hex) => format!("0x{hex}"),
        None => "sys.fn_cdc_get_max_lsn()".to_string(),
    };
    format!(
        "DECLARE @from binary(10) = {from_expr}; \
         DECLARE @min binary(10) = sys.fn_cdc_get_min_lsn('{ci}'); \
         DECLARE @max binary(10) = {max_expr}; \
         IF @from IS NOT NULL AND @min IS NOT NULL AND @from < @min \
            THROW 51000, 'rivet cdc: the resume position is older than the SQL Server \
CDC change-table retention (the cleanup job removed it). Resuming would silently skip changes \
— re-snapshot the table (mode: full) and restart CDC from a fresh checkpoint.', 1; \
         DECLARE @to binary(10) = NULL; \
         IF @from IS NOT NULL AND @max IS NOT NULL AND @from <= @max \
            SELECT @to = MAX(s) FROM (SELECT TOP ({batch}) __$start_lsn AS s \
                FROM cdc.fn_cdc_get_all_changes_{ci}(@from, @max, N'all') \
                ORDER BY __$start_lsn) q; \
         IF @to IS NOT NULL \
            SELECT * FROM cdc.fn_cdc_get_all_changes_{ci}(@from, @to, N'all') \
            ORDER BY __$start_lsn, __$seqval;"
    )
}

/// Connection parameters for a SQL Server CDC poll stream.
pub(crate) struct MssqlCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// CDC capture instance, e.g. `dbo_orders`. Validated as an identifier
    /// because it is interpolated into the change-function name (which can't be a
    /// bind parameter).
    pub capture_instance: String,
    /// Resume LSN — the hex of the last durably-written `__$start_lsn`. The poll
    /// reads changes *after* it (`fn_cdc_increment_lsn`); `None` ⇒ from the change
    /// table's min LSN (first run). This is what makes SQL Server CDC at-least-once
    /// rather than re-reading the whole retained change table every run.
    pub from_lsn: Option<String>,
}

/// Polls a CDC change table and yields canonical changes.
pub(crate) struct MssqlChangeStream {
    rt: tokio::runtime::Runtime,
    client: Client<Compat<TcpStream>>,
    capture_instance: String,
    schema: String,
    table: String,
    /// Internal read cursor — advanced per bounded batch so the next poll reads
    /// the following window. NOT the durable resume position: that is the sink's
    /// checkpoint (advanced only after a durable part), so a crash re-reads from
    /// there (at-least-once) regardless of how far this cursor has moved.
    from_lsn: Option<String>,
    pending: VecDeque<ChangeEvent>,
    /// Max changes to pull per poll — bounds drain memory to O(batch) instead of
    /// O(total change-table window). See [`crate::source::cdc::PeekBound`].
    batch_limit: i64,
    /// A poll that returns no rows has drained the window up to the current max
    /// LSN — the stream ends (the next scheduler run resumes from the checkpoint).
    exhausted: bool,
    /// Open-time max-LSN ceiling (bare hex) for a bounded run: every poll's
    /// `@max` pins here instead of re-reading `fn_cdc_get_max_lsn()`, so the
    /// window cannot recede under sustained writes; `None` (daemon) keeps the
    /// chase-the-head behaviour. The contract lives on
    /// [`crate::source::cdc::CdcConfig::until_current`].
    bound: Option<String>,
}

impl MssqlChangeStream {
    /// Connect and bind to a capture instance. Holds the runtime + connection for
    /// the life of the stream (folds the per-poll runtime/connect smell away).
    ///
    /// `bound_at_open` = a bounded (`until_current`) run: snapshot
    /// `fn_cdc_get_max_lsn()` once and pin every poll's `@max` to it — see
    /// [`Self::bound`].
    pub(crate) fn open(
        cfg: &MssqlCdcConfig,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        bound_at_open: bool,
    ) -> Result<Self> {
        if !cfg
            .capture_instance
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            anyhow::bail!(
                "invalid CDC capture instance name: {:?}",
                cfg.capture_instance
            );
        }
        // The resume LSN is inlined into `0x{hex}` (binary(10) can't be bound), so
        // validate it to even-length hex — no SQL can break out.
        if let Some(lsn) = &cfg.from_lsn
            && (lsn.is_empty() || lsn.len() % 2 != 0 || !lsn.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            anyhow::bail!("mssql cdc: malformed resume LSN {lsn:?}");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let mut client = rt.block_on(connect(cfg, tls))?;

        // Resolve the REAL schema/table from cdc.change_tables metadata. The
        // previous `<schema>_<table>` name heuristic silently mis-tagged every
        // event when the capture instance was named after an underscored table
        // (`product_catalog` → schema "product", table "catalog"), so the
        // sink's table routing dropped 100% of its changes while the run still
        // reported success. The name is a label; the metadata is the truth.
        // Fall back to the heuristic only if the metadata row is unreadable.
        let meta: Option<(String, String)> = rt.block_on(async {
            let row = client
                .query(
                    "SELECT OBJECT_SCHEMA_NAME(source_object_id), \
                            OBJECT_NAME(source_object_id) \
                     FROM cdc.change_tables WHERE capture_instance = @P1",
                    &[&cfg.capture_instance.as_str()],
                )
                .await
                .ok()?
                .into_row()
                .await
                .ok()??;
            let s: Option<&str> = row.get(0);
            let t: Option<&str> = row.get(1);
            Some((s?.to_string(), t?.to_string()))
        });
        let (schema, table) = meta.unwrap_or_else(|| {
            cfg.capture_instance
                .split_once('_')
                .map(|(s, t)| (s.to_string(), t.to_string()))
                .unwrap_or_else(|| (String::new(), cfg.capture_instance.clone()))
        });

        // Bounded run: snapshot the ceiling once, at open. A NULL max LSN (CDC
        // not enabled yet) keeps `bound = None` so the first poll surfaces the
        // same loud setup error the daemon path does — never a silent empty run.
        let bound = if bound_at_open {
            let max: Option<String> = rt.block_on(async {
                Ok::<_, anyhow::Error>(
                    client
                        .query(
                            "SELECT CONVERT(varchar(24), sys.fn_cdc_get_max_lsn(), 1)",
                            &[],
                        )
                        .await?
                        .into_row()
                        .await?
                        .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string())),
                )
            })?;
            let max = max.map(|s| s.trim_start_matches("0x").to_string());
            // The value is inlined into `0x{hex}` in every poll — hold it to the
            // same charset gate as the resume LSN, even though the server made it.
            if let Some(hex) = &max
                && (hex.is_empty()
                    || hex.len() % 2 != 0
                    || !hex.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                anyhow::bail!("mssql cdc: malformed open-time max LSN {hex:?}");
            }
            max
        } else {
            None
        };
        Ok(Self {
            rt,
            client,
            capture_instance: cfg.capture_instance.clone(),
            schema,
            table,
            from_lsn: cfg.from_lsn.clone(),
            pending: VecDeque::new(),
            batch_limit: peek.rows_capped() as i64,
            exhausted: false,
            bound,
        })
    }

    /// Open from a `sqlserver://user:pass@host:port/db` URL + a capture instance
    /// (the factory path).
    pub(crate) fn from_url(
        url: &str,
        capture_instance: &str,
        from_lsn: Option<String>,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        bound_at_open: bool,
    ) -> Result<Self> {
        // Refuse remote plaintext / unauthenticated TLS before any dial (the gate
        // the batch MssqlSource uses).
        require_tls_or_loopback(url, tls)?;
        let p = crate::source::mssql::parse_mssql_url(url)?;
        Self::open(
            &MssqlCdcConfig {
                host: p.host,
                port: p.port,
                database: p.database,
                user: p.user,
                password: p.password,
                capture_instance: capture_instance.to_string(),
                from_lsn,
            },
            tls,
            peek,
            bound_at_open,
        )
    }

    /// Poll ONE **bounded batch** of the change table into `pending`. `@to` is the
    /// `__$start_lsn` of the `batch_limit`-th change, so the window `[@from, @to]`
    /// returns only **whole transactions** (`fn_cdc_get_all_changes` never splits a
    /// `__$start_lsn` group) — memory is O(batch), never O(total window). The
    /// internal cursor then advances to `@to`; the next poll continues past it. A
    /// poll that returns no rows has drained the window up to the current max LSN.
    fn fill(&mut self) -> Result<()> {
        // Only ever called from `next_change` under `!self.exhausted`, so no
        // early-return guard here (matches the PostgreSQL adapter).
        let ci = self.capture_instance.clone();
        // Resume window: read changes *after* the last cursor LSN
        // (`fn_cdc_increment_lsn`); on the first poll (no cursor) start at the
        // change table's min LSN. If the cursor has fallen BELOW the min LSN the
        // cleanup job removed the changes — THROW (forcing a re-snapshot) rather
        // than silently skip. `@to` bounds the batch at a real transaction
        // boundary (the batch_limit-th change's start LSN); NULL LSNs / nothing
        // new leave `@to` NULL and the final SELECT returns zero rows.
        let from_expr = match &self.from_lsn {
            Some(hex) => format!("sys.fn_cdc_increment_lsn(0x{hex})"),
            None => format!("sys.fn_cdc_get_min_lsn('{ci}')"),
        };
        let sql = fill_sql(&ci, &from_expr, self.batch_limit, self.bound.as_deref());
        // The most common SQL Server gotcha — "Invalid object name
        // cdc.fn_cdc_get_all_changes_…" — surfaces here, at the first poll, not at
        // connect. Append the setup hint so the missing CDC enable is obvious.
        let rows = {
            let Self { rt, client, .. } = self;
            rt.block_on(async { client.simple_query(sql).await?.into_first_result().await })
                .map_err(|e| anyhow::Error::new(e).context(crate::source::cdc::MSSQL_CDC_HINT))?
        };
        // Rows are ordered ascending by start LSN, so the last one's `__$start_lsn`
        // is `@to` — the cursor advances there regardless of each row's op.
        let mut max_lsn: Option<String> = None;
        for r in &rows {
            let mut op_code = 0i32;
            let mut lsn = String::new();
            let mut values: Vec<RivetValue> = Vec::new();
            // Captured column NAMES ride along — the sink then maps this
            // image by name, making the positional-corruption class
            // (findings #37/#41) unrepresentable on SQL Server too.
            let mut names: Vec<String> = Vec::new();
            for (idx, (col, data)) in r.cells().enumerate() {
                match col.name() {
                    "__$operation" => {
                        if let ColumnData::I32(Some(v)) = data {
                            op_code = *v;
                        }
                    }
                    "__$start_lsn" => {
                        if let ColumnData::Binary(Some(b)) = data {
                            lsn = hex(b);
                        }
                    }
                    n if n.starts_with("__$") => {} // skip other metadata
                    n => {
                        names.push(n.to_string());
                        values.push(cell_to_rivet(r, idx, data));
                    }
                }
            }
            if !lsn.is_empty() {
                max_lsn = Some(lsn.clone());
            }
            let Some(op) = map_op(op_code) else { continue };
            // after-image for insert/update; the key (before-image) for delete
            let (before, after) = match op {
                ChangeOp::Delete => (Some(values), None),
                _ => (None, Some(values)),
            };
            self.pending.push_back(ChangeEvent {
                op,
                schema: self.schema.clone(),
                table: self.table.clone(),
                before,
                after,
                position: Position(json!({ "lsn": lsn })),
                // The change table only ever holds already-committed changes.
                committed: true,
                image_names: Some(std::sync::Arc::from(names)),
                seq: 0, // stamped by TxnSeq as the stream is consumed
            });
        }
        match max_lsn {
            // Advance the internal cursor to @to; the next poll reads past it.
            Some(l) => self.from_lsn = Some(l),
            // No rows ⇒ the window is drained up to the current max LSN.
            None => self.exhausted = true,
        }
        Ok(())
    }
}

impl ChangeStream for MssqlChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        // Refill a bounded batch whenever the buffer drains, advancing the cursor
        // each time, until a poll returns nothing (window drained to the max LSN).
        while self.pending.is_empty() && !self.exhausted {
            if let Err(e) = self.fill() {
                return Some(Err(e));
            }
        }
        self.pending.pop_front().map(Ok)
    }
}

/// `__$operation` → canonical op. 1=delete, 2=insert, 4=update-after; 3 (update
/// before-image) is skipped — under `N'all'` an update yields only op 4.
fn map_op(code: i32) -> Option<ChangeOp> {
    match code {
        1 => Some(ChangeOp::Delete),
        2 => Some(ChangeOp::Insert),
        4 => Some(ChangeOp::Update),
        _ => None,
    }
}

/// Map a captured source cell to a typed [`RivetValue`]. Temporals use
/// tiberius+chrono's structural `try_get` (no manual DateTime2-increment math);
/// numeric carries its exact unscaled value → decimal text → `Decimal128` at the
/// sink. Mirrors `mssql::arrow_convert`'s per-`ColumnData` handling.
fn cell_to_rivet(row: &Row, idx: usize, data: &ColumnData<'_>) -> RivetValue {
    match data {
        ColumnData::Bit(Some(b)) => RivetValue::Bool(*b),
        ColumnData::U8(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I16(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I32(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I64(Some(v)) => RivetValue::Int(*v),
        ColumnData::F32(Some(v)) => RivetValue::Float(*v as f64),
        ColumnData::F64(Some(v)) => RivetValue::Float(*v),
        ColumnData::String(Some(s)) => RivetValue::Bytes(s.as_bytes().to_vec()),
        // uniqueidentifier resolves to a UUID column (FixedSizeBinary(16)), so carry
        // the 16 canonical bytes — NOT the 36-char string, which won't fit the
        // fixed-size builder and silently becomes NULL. Mirrors mssql::arrow_convert.
        ColumnData::Guid(Some(g)) => RivetValue::Bytes(g.as_bytes().to_vec()),
        ColumnData::Binary(Some(b)) => RivetValue::Bytes(b.to_vec()),
        ColumnData::Numeric(Some(n)) => {
            RivetValue::Bytes(numeric_to_decimal_string(n.value(), n.scale()).into_bytes())
        }
        // datetimeoffset is tz-aware — `try_get::<NaiveDateTime>` is the *wrong* type
        // and returns None (silent data loss). Read it as FixedOffset and carry its UTC
        // instant; the resolved column is a tz-aware Timestamp, so the sink writes it
        // identically to the batch export (parity) with the zone preserved.
        ColumnData::DateTimeOffset(_) => row
            .try_get::<chrono::DateTime<chrono::FixedOffset>, _>(idx)
            .ok()
            .flatten()
            .map_or(RivetValue::Null, |dt| RivetValue::DateTime(dt.naive_utc())),
        ColumnData::DateTime(_) | ColumnData::DateTime2(_) | ColumnData::SmallDateTime(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .ok()
            .flatten()
            .map_or(RivetValue::Null, RivetValue::DateTime),
        ColumnData::Date(_) => row
            .try_get::<NaiveDate, _>(idx)
            .ok()
            .flatten()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map_or(RivetValue::Null, RivetValue::DateTime),
        ColumnData::Time(_) => {
            row.try_get::<NaiveTime, _>(idx)
                .ok()
                .flatten()
                .map_or(RivetValue::Null, |t| {
                    RivetValue::TimeMicros(
                        t.num_seconds_from_midnight() as i64 * 1_000_000
                            + t.nanosecond() as i64 / 1000,
                    )
                })
        }
        // every None (NULL) variant + anything unhandled
        _ => RivetValue::Null,
    }
}

/// Render a tiberius `Numeric` (unscaled `value` + `scale`) to exact decimal text.
fn numeric_to_decimal_string(value: i128, scale: u8) -> String {
    let scale = scale as usize;
    if scale == 0 {
        return value.to_string();
    }
    let neg = value < 0;
    let digits = value.unsigned_abs().to_string();
    let digits = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let (int_part, frac) = digits.split_at(digits.len() - scale);
    format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn connect(
    cfg: &MssqlCdcConfig,
    tls: Option<&TlsConfig>,
) -> Result<Client<Compat<TcpStream>>> {
    let mut config = Config::new();
    config.host(&cfg.host);
    config.port(cfg.port);
    config.database(&cfg.database);
    config.authentication(AuthMethod::sql_server(&cfg.user, &cfg.password));
    config.encryption(EncryptionLevel::Required);
    // Gate trust_cert exactly as the batch MssqlSource does: verify the chain by
    // default (no trust_cert); trust the named CA when given; accept-any only for
    // an explicit disable / accept-invalid, or for loopback (None — the
    // require_tls_or_loopback gate already ensured a remote host carries a tls block).
    match tls {
        Some(c) if c.mode == TlsMode::Disable || c.accept_invalid_certs => config.trust_cert(),
        Some(c) => {
            if let Some(ca) = &c.ca_file {
                config.trust_cert_ca(ca);
            }
        }
        None => config.trust_cert(),
    }
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(config, tcp.compat_write()).await?)
}

/// Persist the database's CURRENT max LSN to `ckpt` — the anchor for
/// `cdc.initial: snapshot`, taken BEFORE the snapshot read so the change
/// stream overlaps the snapshot instead of gapping it. Fails loudly when CDC
/// is not enabled on the database (no max LSN exists to anchor at).
pub(crate) fn pin_checkpoint_at_max_lsn(
    url: &str,
    ckpt: &std::path::Path,
    tls: Option<&TlsConfig>,
) -> Result<()> {
    let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
    let probe = src.cdc_health(None)?;
    let Some(max) = probe_max_lsn(&probe) else {
        anyhow::bail!(
            "mssql cdc initial snapshot: sys.fn_cdc_get_max_lsn() is NULL — enable CDC first \
             (EXEC sys.sp_cdc_enable_db) so the anchor exists before the snapshot"
        );
    };
    Position(serde_json::json!({ "lsn": max })).save(ckpt)
}

/// The probe's max LSN as the bare hex the checkpoint stores (strip `0x`).
fn probe_max_lsn(probe: &crate::source::mssql::MssqlCdcProbe) -> Option<String> {
    if !probe.cdc_enabled {
        return None;
    }
    probe.max_lsn_hex.as_deref().map(|s| {
        s.trim_start_matches("0x")
            .trim_start_matches("0X")
            .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_renders_exact_decimal() {
        assert_eq!(numeric_to_decimal_string(15005, 2), "150.05");
        assert_eq!(numeric_to_decimal_string(-7500, 3), "-7.500");
        assert_eq!(numeric_to_decimal_string(42, 0), "42");
        assert_eq!(numeric_to_decimal_string(5, 2), "0.05");
    }

    // The until_current termination contract: a bounded poll pins `@max` to the
    // open-time ceiling — it must never re-read `fn_cdc_get_max_lsn()` (the
    // moving target that keeps a hot table's drain from ever terminating), and
    // the daemon poll must keep doing exactly that.
    #[test]
    fn fill_sql_bounded_pins_max_and_daemon_chases_it() {
        let bounded = fill_sql(
            "dbo_orders",
            "sys.fn_cdc_get_min_lsn('dbo_orders')",
            500,
            Some("0000002f000004d80005"),
        );
        assert!(
            bounded.contains("DECLARE @max binary(10) = 0x0000002f000004d80005;"),
            "bounded poll must pin @max to the open-time LSN: {bounded}"
        );
        assert!(
            !bounded.contains("fn_cdc_get_max_lsn"),
            "bounded poll must not consult the moving max LSN: {bounded}"
        );
        // The min-LSN retention guard must survive the pinning.
        assert!(bounded.contains("sys.fn_cdc_get_min_lsn('dbo_orders')"));
        assert!(bounded.contains("TOP (500)"));

        let daemon = fill_sql("dbo_orders", "sys.fn_cdc_increment_lsn(0xabcd)", 500, None);
        assert!(
            daemon.contains("DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();"),
            "daemon poll keeps chasing the head: {daemon}"
        );
    }

    fn cfg(capture_instance: &str) -> MssqlCdcConfig {
        MssqlCdcConfig {
            host: "127.0.0.1".into(),
            // The `mssql-cdc` instance (cdc profile, :1434) — SQL Server Agent on.
            port: 1434,
            database: "rivet".into(),
            user: "sa".into(),
            password: "Rivet_Passw0rd!".into(),
            capture_instance: capture_instance.into(),
            from_lsn: None,
        }
    }

    /// Run arbitrary T-SQL on a throwaway connection (test setup helper).
    fn exec(sql: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = connect(&cfg("dbo_cdc_unit"), None).await.unwrap();
            for batch in sql.split(";\n") {
                if !batch.trim().is_empty() {
                    c.simple_query(batch)
                        .await
                        .unwrap()
                        .into_results()
                        .await
                        .unwrap();
                }
            }
        });
    }

    #[test]
    #[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC enabled"]
    fn streams_change_operations() {
        exec(
            "IF OBJECT_ID('cdc_unit','U') IS NOT NULL DROP TABLE cdc_unit;\n\
             CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT);\n\
             IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;\n\
             IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_cdc_unit') \
               EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit';\n\
             EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='cdc_unit',@role_name=NULL,@capture_instance='dbo_cdc_unit',@supports_net_changes=0",
        );
        exec(
            "INSERT INTO cdc_unit VALUES (1,10);\n\
             UPDATE cdc_unit SET v=20 WHERE id=1;\n\
             DELETE FROM cdc_unit WHERE id=1",
        );
        // let the capture Agent job scan the log (~5 s cycle)
        std::thread::sleep(std::time::Duration::from_secs(8));

        let mut s = MssqlChangeStream::open(
            &cfg("dbo_cdc_unit"),
            None,
            crate::source::cdc::PeekBound::Sized(10_000),
            false,
        )
        .unwrap();
        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            ops.push(ev.unwrap().op);
        }
        exec(
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit'",
        );
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "CDC change table must yield insert(2), update-after(4), delete(1)"
        );
    }
}
