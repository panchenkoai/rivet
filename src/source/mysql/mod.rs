//! MySQL `Source` implementation.
//!
//! Module layout (mirrors `postgres/`):
//!
//! - `mod.rs` (this file) — `MysqlSource` struct + connect/TLS path, the
//!   `MysqlProxyKind` classifier (`classify_mysql_proxy` + the I/O wrapper
//!   `detect_mysql_proxy_kind`), the `InnoDB_log_waits` sampler, the
//!   `lean_pool_opts` / `connect_pool` / `build_mysql_ssl_opts` helpers,
//!   `introspect_mysql_table_for_chunking` + the InnoDB `AVG_ROW_LENGTH`
//!   correction, the cursor-bound `exec_iter` export loop
//!   (`mysql_run_export`), and the `Source` trait impl.
//! - [`arrow_convert`] — the entire row → Arrow `RecordBatch` pipeline:
//!   `mysql_type_to_rivet` + `mysql_native_type_name`,
//!   `mysql_schema_and_arrow_types`, BIT / TIME / DECIMAL decoders, and the
//!   array builders. Kept in a sibling because it is the largest
//!   single-purpose cluster in this driver (~510 LoC) and has zero reverse
//!   dependency back into the connection / pool / cursor layer.

mod arrow_convert;

use std::sync::Arc;

use arrow::datatypes::Schema;
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

use crate::config::{SourceType, TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::query::build_incremental_query;
use crate::tuning::{ADAPTIVE_SAMPLE_INTERVAL, SourceTuning, next_adaptive_batch_size};
use crate::types::ColumnOverrides;

use arrow_convert::{
    mysql_native_type_name, mysql_schema_and_arrow_types, mysql_type_to_rivet,
    rows_to_record_batch_typed,
};
// `bit_bytes_to_u64` is only referenced by the `tests` module below — gate the
// re-import on `cfg(test)` so non-test builds don't see an unused-import warning.
#[cfg(test)]
use arrow_convert::bit_bytes_to_u64;

pub struct MysqlSource {
    pool: Pool,
    proxy_kind: MysqlProxyKind,
}

/// What the MySQL connection is actually talking to.
///
/// Used to:
/// - decide which warning (if any) to print at connect time,
/// - tag the `executing query (connection=...)` debug log so operators can
///   distinguish direct vs proxied traffic when reading logs after the fact.
///
/// Detection happens once at connect time via [`detect_mysql_proxy_kind`].
///
/// `pub` for integration-test reachability via `MysqlSource::proxy_kind()`;
/// same "no external API contract" disclaimer applies as for the rest of
/// `rivet::source::mysql::*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlProxyKind {
    /// Direct connection to a MySQL server — no proxy detected.
    Direct,
    /// ProxySQL: detected via either the `@@version_comment` signature or
    /// `@@proxy_version` (a ProxySQL-only system variable).
    ProxySql,
    /// MariaDB MaxScale: detected via `@@version_comment` containing
    /// "maxscale".  MaxScale's `readwritesplit` and `readconnroute` routers
    /// do *not* multiplex by default but they can still rewrite or block
    /// queries — worth surfacing to the operator.
    MaxScale,
    /// An unknown transaction-mode multiplexer: detected because
    /// `CONNECTION_ID()` returned different values across two consecutive
    /// queries on the same `mysql::Conn`.  This catches in-house balancers,
    /// HAProxy-with-MySQL-mode setups, and ProxySQL/MaxScale instances that
    /// hide their banner.
    ///
    /// False negatives are possible when the proxy's backend pool_size is 1
    /// (the same physical backend is always reused).
    Multiplexed,
}

impl MysqlProxyKind {
    /// True for any non-direct connection (`is_proxy() == false` only for
    /// [`MysqlProxyKind::Direct`]).
    ///
    /// `#[allow(dead_code)]` because the binary compilation unit (which
    /// re-declares `mod source`) does not reference this; the lib + tests do.
    /// Same pattern as `MysqlSource::from_pool`.
    #[allow(dead_code)]
    pub fn is_proxy(self) -> bool {
        !matches!(self, MysqlProxyKind::Direct)
    }

    /// Stable label for the `executing query (connection=...)` debug log.
    /// Keep this terse and stable: external log parsers grep on these strings.
    pub fn log_label(self) -> &'static str {
        match self {
            MysqlProxyKind::Direct => "direct",
            MysqlProxyKind::ProxySql => "proxysql",
            MysqlProxyKind::MaxScale => "maxscale",
            MysqlProxyKind::Multiplexed => "proxy-multiplexed",
        }
    }

    /// One-time warning emitted at connect time.  Returns `None` for
    /// [`MysqlProxyKind::Direct`] (the common case, no warning needed).
    fn warn_message(self) -> Option<&'static str> {
        match self {
            MysqlProxyKind::Direct => None,
            MysqlProxyKind::ProxySql => Some(
                "MySQL proxy multiplexer detected (ProxySQL) — session variables \
                 set per-connection may not survive multiplexing; use direct connections \
                 for production exports",
            ),
            MysqlProxyKind::MaxScale => Some(
                "MySQL proxy detected (MaxScale) — queries may be rewritten or routed; \
                 verify SQL is accepted by the active MaxScale router (readwritesplit, \
                 readconnroute) and that session timeouts apply on the backend",
            ),
            MysqlProxyKind::Multiplexed => Some(
                "MySQL connection multiplexing detected (CONNECTION_ID() differs across \
                 queries) — session variables and temporary state may not persist across \
                 statements; use direct connections for production exports",
            ),
        }
    }
}

/// Pool options that prevent eager pre-connection. The default mysql::Pool
/// opens `min=10` connections immediately, which overflows MySQL's
/// max_connections when many parallel exports run simultaneously.
fn lean_pool_opts() -> PoolOpts {
    PoolOpts::default()
        .with_constraints(PoolConstraints::new(1, 100).expect("valid pool constraints"))
}

/// Pure classifier for proxy detection signals.  Kept separate from
/// [`detect_mysql_proxy_kind`] so it can be exhaustively unit-tested without a
/// live MySQL.  See [`MysqlProxyKind`] for the meaning of each variant.
///
/// Precedence is intentional:
///
/// 1. `PROXYSQL INTERNAL SESSION` accepted as a query (ProxySQL intercepts
///    this command on its client port; vanilla MySQL returns a syntax
///    error). This is the strongest signal because ProxySQL by default
///    forwards `@@version_comment`, `VERSION()`, and `@@version` straight
///    through to the backend, so a configured-as-default ProxySQL is
///    invisible to banner checks.
/// 2. Explicit banner match in `@@version_comment` (ProxySQL > MaxScale).
///    Catches ProxySQL builds with `server_version` overridden to advertise
///    "ProxySQL" in `VERSION()`, and MaxScale which puts "MaxScale" in the
///    banner.
/// 3. `@@proxy_version` presence (ProxySQL-only system variable when
///    `mysql_query_rules` is configured to expose it).
/// 4. `CONNECTION_ID()` differing across two queries (generic multiplexing
///    fallback — catches HAProxy MySQL mode, custom balancers, etc.).
///
/// Banner-before-CONNECTION_ID order matters: a ProxySQL behind a
/// `transaction_persistent` user (which keeps the same backend conn) would
/// fool the CONNECTION_ID check, so the specific signal wins.
pub(crate) fn classify_mysql_proxy(
    proxysql_internal_accepted: bool,
    version_comment: Option<&str>,
    proxy_version: Option<&str>,
    connection_id_pair: Option<(u64, u64)>,
) -> MysqlProxyKind {
    if proxysql_internal_accepted {
        return MysqlProxyKind::ProxySql;
    }
    if let Some(v) = version_comment {
        let l = v.to_ascii_lowercase();
        if l.contains("proxysql") {
            return MysqlProxyKind::ProxySql;
        }
        if l.contains("maxscale") {
            return MysqlProxyKind::MaxScale;
        }
    }
    if proxy_version.is_some() {
        return MysqlProxyKind::ProxySql;
    }
    if let Some((a, b)) = connection_id_pair
        && a != b
    {
        return MysqlProxyKind::Multiplexed;
    }
    MysqlProxyKind::Direct
}

/// I/O wrapper around [`classify_mysql_proxy`]: collects the detection
/// signals from a live connection and returns the classification.  On any
/// connection failure returns [`MysqlProxyKind::Direct`] — detection is
/// best-effort and must never break a real export.
fn detect_mysql_proxy_kind(pool: &Pool) -> MysqlProxyKind {
    let mut conn = match pool.get_conn() {
        Ok(c) => c,
        Err(_) => return MysqlProxyKind::Direct,
    };
    // `PROXYSQL INTERNAL SESSION` is intercepted by ProxySQL on its client
    // port (6033) and returns a single-column JSON row describing the
    // proxy session; vanilla MySQL and MariaDB return SQL syntax error
    // 1064.  We use `query_drop` so we don't have to model the response
    // shape — any `Ok` indicates ProxySQL accepted the command.
    //
    // (`PROXYSQL VERSION` exists too but is only accepted on the admin
    // port 6032, not on the client port we connect through.)
    let proxysql_internal_accepted: bool = conn.query_drop("PROXYSQL INTERNAL SESSION").is_ok();
    let version_comment: Option<String> =
        conn.query_first("SELECT @@version_comment").unwrap_or(None);
    let proxy_version: Option<String> = conn.query_first("SELECT @@proxy_version").unwrap_or(None);
    // CONNECTION_ID() is server-side: comparing two consecutive calls on the
    // same `Conn` detects transaction-mode multiplexers that hand each
    // statement to a different backend connection.
    let cid1: Option<u64> = conn.query_first("SELECT CONNECTION_ID()").unwrap_or(None);
    let cid2: Option<u64> = conn.query_first("SELECT CONNECTION_ID()").unwrap_or(None);
    let pair = match (cid1, cid2) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    };
    classify_mysql_proxy(
        proxysql_internal_accepted,
        version_comment.as_deref(),
        proxy_version.as_deref(),
        pair,
    )
}

/// Emit the one-time connect-time warning for a non-direct proxy kind.
/// Centralized so the wording stays consistent across the three connect entry
/// points (`from_pool`, `connect`, `connect_with_tls`).
fn warn_proxy_kind(kind: MysqlProxyKind) {
    if let Some(msg) = kind.warn_message() {
        log::warn!("{msg}");
    }
}

/// Sample the global `Innodb_log_waits` counter — increments when InnoDB has to
/// wait for redo-log buffer space, indicating write pressure.
fn mysql_sample_innodb_log_waits(pool: &Pool) -> Option<u64> {
    let mut conn = pool.get_conn().ok()?;
    conn.query_first::<(String, u64), _>("SHOW GLOBAL STATUS LIKE 'Innodb_log_waits'")
        .ok()
        .flatten()
        .map(|(_, v)| v)
}

impl MysqlSource {
    /// Build a source from an existing pool. Useful in tests that need to
    /// share the pool with post-export state inspection.
    #[allow(dead_code)]
    pub fn from_pool(pool: Pool) -> Self {
        let proxy_kind = detect_mysql_proxy_kind(&pool);
        warn_proxy_kind(proxy_kind);
        Self { pool, proxy_kind }
    }

    /// Connect with no transport security (legacy path).
    pub fn connect(url: &str) -> Result<Self> {
        let opts =
            Opts::from(OptsBuilder::from_opts(Opts::from_url(url)?).pool_opts(lean_pool_opts()));
        let pool = Pool::new(opts)?;
        let proxy_kind = detect_mysql_proxy_kind(&pool);
        warn_proxy_kind(proxy_kind);
        Ok(Self { pool, proxy_kind })
    }

    /// Connect honoring the user's [`TlsConfig`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let base = Opts::from_url(url)?;
                let ssl = build_mysql_ssl_opts(cfg);
                let opts = Opts::from(
                    OptsBuilder::from_opts(base)
                        .ssl_opts(Some(ssl))
                        .pool_opts(lean_pool_opts()),
                );
                let pool = Pool::new(opts)?;
                let proxy_kind = detect_mysql_proxy_kind(&pool);
                warn_proxy_kind(proxy_kind);
                Ok(Self { pool, proxy_kind })
            }
            _ => Self::connect(url),
        }
    }

    /// Expose the proxy classification for diagnostic tools (preflight,
    /// integration tests). Not part of the public Source trait — same
    /// internal-may-change contract as the rest of `rivet::source::mysql::*`.
    ///
    /// `#[allow(dead_code)]` covers the binary compilation unit; the lib +
    /// integration tests reference this through the `rivet::source::mysql`
    /// public surface.
    #[allow(dead_code)]
    pub fn proxy_kind(&self) -> MysqlProxyKind {
        self.proxy_kind
    }
}

/// Build a MySQL connection pool honoring the configured TLS policy.
///
/// Shared by preflight, doctor, init, and anywhere else we need a pool outside
/// the `Source` trait. `tls = None` falls back to plaintext (legacy behavior).
pub(crate) fn connect_pool(url: &str, tls: Option<&TlsConfig>) -> Result<Pool> {
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => {
            let base = Opts::from_url(url)?;
            let ssl = build_mysql_ssl_opts(cfg);
            let opts = Opts::from(
                OptsBuilder::from_opts(base)
                    .ssl_opts(Some(ssl))
                    .pool_opts(lean_pool_opts()),
            );
            Ok(Pool::new(opts)?)
        }
        _ => {
            let opts = Opts::from(
                OptsBuilder::from_opts(Opts::from_url(url)?).pool_opts(lean_pool_opts()),
            );
            Ok(Pool::new(opts)?)
        }
    }
}

/// Threshold above which `AVG_ROW_LENGTH` is treated as inflated by InnoDB BLOB
/// overflow pages and divided down. Rows under 8 KB fit inline (no overflow),
/// so the raw figure is accurate; above it the divisor compensates.
const INNODB_BLOB_OVERFLOW_THRESHOLD_BYTES: i64 = 8 * 1024;

/// Empirical divisor for InnoDB BLOB-page inflation. A wide-text row that
/// allocates eight 16 KB overflow pages reports ~128 KB in `AVG_ROW_LENGTH`
/// while the actual wire content is ~40 KB → factor of ~3.
const INNODB_BLOB_OVERFLOW_DIVISOR: i64 = 3;

/// Apply the InnoDB BLOB-overflow correction to a raw `AVG_ROW_LENGTH` value.
/// Pure function for unit testability — the live introspection helper calls
/// this on the figure returned by `information_schema.TABLES`.
///
/// - Below the 8 KB threshold: raw value is accurate (no overflow).
/// - Above: divide by 3, floored at threshold/2 so we never undershoot too far.
fn correct_innodb_avg_row_length(raw_bytes: i64) -> i64 {
    if raw_bytes > INNODB_BLOB_OVERFLOW_THRESHOLD_BYTES {
        (raw_bytes / INNODB_BLOB_OVERFLOW_DIVISOR).max(INNODB_BLOB_OVERFLOW_THRESHOLD_BYTES / 2)
    } else {
        raw_bytes
    }
}

/// Probe `information_schema` for stats chunked-mode planning needs.
///
/// MySQL analogue of [`crate::source::postgres::introspect_pg_table_for_chunking`]:
/// returns the same source-neutral [`crate::source::TableIntrospection`] so
/// `plan/build.rs` can dispatch on `source_type` and reuse the same downstream
/// logic for chunk-column / chunk_size derivation.
///
/// Two queries per call, both against `information_schema` (no extra grants
/// required for a normal app user):
/// - `TABLES.AVG_ROW_LENGTH` + `TABLE_ROWS` for the row-size and row-count estimate.
///   These come from `mysql.innodb_table_stats` and are only as fresh as the
///   last `ANALYZE TABLE` / autostat run. Empty / unanalysed → zero.
/// - `STATISTICS` filtered to `INDEX_NAME='PRIMARY'` with `SEQ_IN_INDEX=1` and a
///   second probe ensuring no `SEQ_IN_INDEX=2` row exists — single-column PK only.
///
/// `qualified_table` is `<schema>.<table>` or bare `<table>` (resolved under the
/// current database for the connection). Same strict ident rules as the YAML
/// `table:` shortcut so the SQL stays trivially safe.
pub(crate) fn introspect_mysql_table_for_chunking(
    url: &str,
    tls: Option<&TlsConfig>,
    qualified_table: &str,
) -> Result<crate::source::TableIntrospection> {
    let pool = connect_pool(url, tls)?;
    let mut conn = pool.get_conn()?;
    let default_db: Option<String> = conn.query_first("SELECT DATABASE()")?;
    let default_db = default_db.unwrap_or_default();

    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => (default_db, qualified_table.to_string()),
    };

    // (1) Row count + avg row bytes. AVG_ROW_LENGTH already accounts for
    // overflow pages on InnoDB, so we use it directly rather than dividing
    // DATA_LENGTH by TABLE_ROWS (which under-counts for tables with TOAST-like
    // overflow). Fall back to division when AVG_ROW_LENGTH is 0.
    let row_stats: Option<(i64, i64, i64)> = conn.exec_first(
        "SELECT CAST(IFNULL(TABLE_ROWS, 0) AS SIGNED), \
                CAST(IFNULL(AVG_ROW_LENGTH, 0) AS SIGNED), \
                CAST(IFNULL(DATA_LENGTH, 0) AS SIGNED) \
         FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (&schema, &table),
    )?;
    let (row_estimate, avg_row_bytes) = match row_stats {
        Some((rows, avg, data_len)) => {
            let row_count = rows.max(0);
            let raw_per_row = if avg > 0 {
                Some(avg)
            } else if row_count > 0 {
                Some(data_len / row_count)
            } else {
                None
            };
            // InnoDB stores TEXT/BLOB > ~768 B off-page in 16 KB BLOB pages,
            // and `AVG_ROW_LENGTH` counts the allocated page bytes — not the
            // actual content. On wide-text workloads (CMS bodies, JSON logs,
            // audit trails) this inflates the per-row estimate 3-5× compared
            // to what the client driver actually buffers over the wire.
            //
            // We empirically divide by 3 above an 8 KB threshold. Below 8 KB
            // a row fits inline with no overflow, so the raw figure is
            // accurate. Above it, dividing by 3 brings content_items' 41 KB
            // estimate down to ~14 KB — still conservative vs the ~10 KB the
            // PG side reports for the same payload via `pg_total_relation_size`.
            //
            // Pilots who want exact control can set `chunk_size:` explicitly
            // (it always wins over the budget-derived size).
            let per_row = raw_per_row.map(correct_innodb_avg_row_length);
            (row_count, per_row.filter(|b| *b > 0))
        }
        None => (0, None),
    };

    // (2) Single-column int PK probe. STATISTICS has one row per (column,
    // index) so we filter to PRIMARY + SEQ_IN_INDEX=1 and then check that
    // the PRIMARY index has no SEQ_IN_INDEX=2 row (composite).
    let pk_first: Option<(String,)> = conn.exec_first(
        "SELECT COLUMN_NAME \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' AND SEQ_IN_INDEX = 1",
        (&schema, &table),
    )?;
    let single_int_pk = if let Some((col,)) = pk_first {
        let composite: Option<(String,)> = conn.exec_first(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' AND SEQ_IN_INDEX = 2 \
             LIMIT 1",
            (&schema, &table),
        )?;
        if composite.is_some() {
            log::debug!(
                "introspect_mysql_table: composite PK on {schema}.{table} — skipping auto-resolve"
            );
            None
        } else {
            // Column type must be integer-family for safe range chunking.
            let type_row: Option<(String,)> = conn.exec_first(
                "SELECT DATA_TYPE FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (&schema, &table, &col),
            )?;
            match type_row.map(|(t,)| t.to_ascii_lowercase()) {
                Some(t)
                    if matches!(
                        t.as_str(),
                        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
                    ) =>
                {
                    Some(col)
                }
                Some(t) => {
                    log::debug!(
                        "introspect_mysql_table: PK '{col}' on {schema}.{table} has non-int type '{t}' — skipping auto-resolve"
                    );
                    None
                }
                None => None,
            }
        }
    } else {
        None
    };

    Ok(crate::source::TableIntrospection {
        single_int_pk,
        row_estimate,
        avg_row_bytes,
    })
}

fn build_mysql_ssl_opts(cfg: &TlsConfig) -> SslOpts {
    let mut ssl = SslOpts::default();
    if let Some(path) = &cfg.ca_file {
        ssl = ssl.with_root_cert_path(Some(std::path::PathBuf::from(path)));
    }
    match cfg.mode {
        TlsMode::Require => {
            ssl = ssl
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }
        TlsMode::VerifyCa => {
            ssl = ssl.with_danger_skip_domain_validation(true);
        }
        TlsMode::VerifyFull => {
            // Strict: verify chain + hostname.
        }
        TlsMode::Disable => {
            // Never invoked: gated in connect_with_tls.
        }
    }
    if cfg.accept_invalid_certs {
        ssl = ssl.with_danger_accept_invalid_certs(true);
    }
    if cfg.accept_invalid_hostnames {
        ssl = ssl.with_danger_skip_domain_validation(true);
    }
    ssl
}

/// Execute the MySQL query and stream results to sink.
///
/// Separated from export() so session-state cleanup (time_zone, max_execution_time)
/// can run unconditionally in the caller regardless of success or failure.
///
/// `sample_pool`: when `tuning.adaptive` is true, a clone of the source pool used
/// to obtain a second connection for `Innodb_log_waits` sampling without interfering
/// with the streaming result set on `conn`.
fn mysql_run_export(
    conn: &mut mysql::PooledConn,
    sample_pool: Option<Pool>,
    sql: &str,
    cursor_param: Option<&str>,
    tuning: &SourceTuning,
    column_overrides: &ColumnOverrides,
    sink: &mut dyn super::BatchSink,
) -> Result<usize> {
    // SecOps: cursor value is bound via exec_iter rather than string-interpolated.
    // Using exec_iter uniformly (even with empty params) keeps match arms
    // type-compatible — query_iter returns a Text-protocol result, exec_iter Binary.
    let mut result = match cursor_param {
        Some(val) => conn.exec_iter(sql, (val,))?,
        None => conn.exec_iter(sql, ())?,
    };
    let columns = result.columns().as_ref().to_vec();

    // Compute TypeMappings once; derive both the Arrow schema and the
    // per-column DataType vec from the same source so they can never diverge.
    let (schema, arrow_types) = mysql_schema_and_arrow_types(&columns, column_overrides)?;
    let schema = Arc::new(schema);

    sink.on_schema(schema.clone())?;

    // PG path uses `work_mem × 0.7 / row_bytes` for FETCH N — the analogous
    // bottleneck on MySQL is *our* `row_buf` accumulator. The mysql crate
    // streams rows from the wire one-at-a-time, but we pile up `effective_bs`
    // of them in a `Vec<Row>` before flushing to Arrow → for `batch_size: 50000`
    // (fast profile) on content_items that's ~650 MB just for the row_buf,
    // plus another ~650 MB for the Arrow batch it feeds — RSS scales with
    // `batch_size`, not chunk size.
    //
    // Fix: start with a small probe (`PROBE_BATCH_SIZE`), measure the actual
    // Arrow bytes per row after the first batch, then cap `effective_bs` so
    // each flush fits in roughly `MYSQL_BATCH_TARGET_MB` of Arrow memory.
    // Caller's `batch_size_memory_mb` wins when set; the default is 64 MB —
    // chosen to keep peak RSS well under 200 MB on wide-row tables while
    // keeping batches large enough to be efficient for the parquet writer.
    const PROBE_BATCH_SIZE: usize = 500;
    const MYSQL_BATCH_TARGET_MB_DEFAULT: usize = 64;

    let configured_batch_size = tuning.effective_batch_size(Some(&schema));
    let mut effective_bs = configured_batch_size.min(PROBE_BATCH_SIZE);
    let mut base_fetch_size = effective_bs;
    let mut adaptive_last_waits: Option<u64> = if tuning.adaptive {
        sample_pool.as_ref().and_then(mysql_sample_innodb_log_waits)
    } else {
        None
    };
    let mut batch_count: usize = 0;
    let row_set = result
        .iter()
        .ok_or_else(|| anyhow::anyhow!("no result set"))?;
    let mut row_buf: Vec<mysql::Row> = Vec::with_capacity(effective_bs);
    let mut total_rows: usize = 0;
    let mut memory_cap_applied = false;

    for row_result in row_set {
        let row = row_result?;
        row_buf.push(row);

        if row_buf.len() >= effective_bs {
            total_rows += row_buf.len();
            batch_count += 1;
            let batch = rows_to_record_batch_typed(&schema, &arrow_types, &row_buf)?;
            let batch_rows = row_buf.len();
            row_buf.clear();

            // After the first (probe-sized) batch we know how many bytes per
            // row Arrow actually uses. Cap subsequent flushes to a memory
            // target. Never exceed the user's configured `batch_size`.
            if !memory_cap_applied && batch_rows > 0 {
                let arrow_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
                let arrow_per_row = (arrow_bytes / batch_rows).max(64);
                let target_mb = tuning
                    .batch_size_memory_mb
                    .unwrap_or(MYSQL_BATCH_TARGET_MB_DEFAULT);
                let safe = ((target_mb * 1024 * 1024) / arrow_per_row).max(PROBE_BATCH_SIZE);
                let target = safe.min(configured_batch_size);
                if target != effective_bs {
                    log::info!(
                        "MySQL row_buf cap: arrow≈{} B/row, target={} MB → batch_size {} → {} (configured={})",
                        arrow_per_row,
                        target_mb,
                        effective_bs,
                        target,
                        configured_batch_size
                    );
                    effective_bs = target;
                    base_fetch_size = effective_bs;
                    row_buf.reserve(effective_bs.saturating_sub(row_buf.capacity()));
                }
                memory_cap_applied = true;
            }

            sink.on_batch(&batch)?;

            if tuning.adaptive
                && batch_count.is_multiple_of(ADAPTIVE_SAMPLE_INTERVAL)
                && let Some(ref pool) = sample_pool
                && let Some(cur) = mysql_sample_innodb_log_waits(pool)
            {
                let under_pressure = adaptive_last_waits.is_some_and(|prev| cur > prev);
                adaptive_last_waits = Some(cur);
                let next = next_adaptive_batch_size(effective_bs, base_fetch_size, under_pressure);
                if next != effective_bs {
                    effective_bs = next;
                    log::info!(
                        "adaptive batch size → {} ({})",
                        effective_bs,
                        if under_pressure {
                            "pressure"
                        } else {
                            "recovery"
                        }
                    );
                }
            }

            log::info!("fetched {} rows so far...", total_rows);

            if tuning.throttle_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(tuning.throttle_ms));
            }
        }
    }

    if !row_buf.is_empty() {
        total_rows += row_buf.len();
        let batch = rows_to_record_batch_typed(&schema, &arrow_types, &row_buf)?;
        sink.on_batch(&batch)?;
    }

    drop(result);
    Ok(total_rows)
}

impl super::Source for MysqlSource {
    fn export(
        &mut self,
        request: &super::ExportRequest<'_>,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_incremental_query(
            request.query,
            request.incremental,
            request.cursor,
            SourceType::Mysql,
        );
        log::debug!(
            "executing query (connection={}): {}",
            self.proxy_kind.log_label(),
            built.sql
        );

        let mut conn = self.pool.get_conn()?;

        // Roadmap §13: normalize TIMESTAMP columns to UTC so Parquet writes
        // isAdjustedToUTC=true. SET per-connection (not global) to avoid side-effects.
        conn.query_drop("SET time_zone = '+00:00'")?;

        if request.tuning.statement_timeout_s > 0 {
            conn.query_drop(format!(
                "SET SESSION max_execution_time = {}",
                request.tuning.statement_timeout_s * 1000
            ))?;
        }

        let sample_pool = if request.tuning.adaptive {
            Some(self.pool.clone())
        } else {
            None
        };
        let result = mysql_run_export(
            &mut conn,
            sample_pool,
            &built.sql,
            built.cursor_param.as_deref(),
            request.tuning,
            request.column_overrides,
            sink,
        );

        // Always reset session state before connection returns to pool,
        // regardless of whether the export succeeded or failed.
        let _ = conn.query_drop("SET time_zone = @@global.time_zone");
        if request.tuning.statement_timeout_s > 0 {
            let _ = conn.query_drop("SET SESSION max_execution_time = 0");
        }

        // The empty-result fallback to `Schema::empty()` lives here for
        // parity with the PG implementation, even though `exec_iter` always
        // returns the column metadata before yielding any rows so
        // mysql_run_export's `on_schema` already fired.
        let total_rows = result?;
        if total_rows == 0 {
            sink.on_schema(Arc::new(Schema::empty()))?;
        }
        log::info!("total: {} rows", total_rows);
        Ok(())
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let mut conn = self.pool.get_conn()?;
        let row: Option<mysql::Row> = conn.query_first(sql)?;
        match row {
            Some(r) => {
                let val: Option<mysql::Value> = r.get(0);
                match val {
                    Some(mysql::Value::Bytes(b)) => {
                        Ok(Some(String::from_utf8_lossy(&b).into_owned()))
                    }
                    Some(mysql::Value::Int(v)) => Ok(Some(v.to_string())),
                    Some(mysql::Value::UInt(v)) => Ok(Some(v.to_string())),
                    Some(mysql::Value::Float(v)) => Ok(Some(v.to_string())),
                    Some(mysql::Value::Double(v)) => Ok(Some(v.to_string())),
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<crate::types::TypeMapping>> {
        let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
        let mut conn = self.pool.get_conn()?;
        let result = conn.exec_iter(&wrapped, ())?;
        let columns = result.columns().as_ref().to_vec();
        drop(result);
        let mappings = columns
            .iter()
            .map(|col| {
                let rivet = column_overrides
                    .get(col.name_str().as_ref())
                    .cloned()
                    .unwrap_or_else(|| mysql_type_to_rivet(col));
                let source = crate::types::SourceColumn::simple(
                    col.name_str().as_ref(),
                    mysql_native_type_name(col),
                    true,
                );
                crate::types::TypeMapping::from_source(&source, rivet)
            })
            .collect();
        Ok(mappings)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MysqlProxyKind, bit_bytes_to_u64, classify_mysql_proxy, correct_innodb_avg_row_length,
    };

    // ── Proxy detection classifier ──────────────────────────────────────

    #[test]
    fn proxy_classify_direct_when_no_signals() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server - GPL"),
            None,
            Some((42, 42)),
        );
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_direct_when_all_signals_missing() {
        let kind = classify_mysql_proxy(false, None, None, None);
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_proxysql_via_internal_command() {
        let kind = classify_mysql_proxy(
            true,
            Some("MySQL Community Server - GPL"),
            None,
            Some((7, 7)),
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("(ProxySQL) High Performance MySQL Proxy"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_banner_lowercase() {
        let kind = classify_mysql_proxy(false, Some("(proxysql) hpmp"), None, None);
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_proxy_version_only() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server - GPL"),
            Some("2.5.5-percona-1.1"),
            Some((1, 1)),
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_maxscale_via_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("MariaDB MaxScale 22.08.4-ge6a8d35ec source distribution"),
            None,
            Some((1, 1)),
        );
        assert_eq!(kind, MysqlProxyKind::MaxScale);
    }

    #[test]
    fn proxy_classify_proxysql_internal_takes_precedence_over_banner_maxscale() {
        let kind = classify_mysql_proxy(
            true,
            Some("MariaDB MaxScale 22.08.4 source distribution"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_takes_precedence_over_maxscale_in_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("ProxySQL bridging MaxScale upstream"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_multiplexed_via_connection_id_drift() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server"),
            None,
            Some((100, 200)),
        );
        assert_eq!(kind, MysqlProxyKind::Multiplexed);
    }

    #[test]
    fn proxy_classify_direct_when_connection_id_pair_missing() {
        let kind = classify_mysql_proxy(false, Some("MySQL Community Server"), None, None);
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_direct_when_connection_ids_match() {
        let kind = classify_mysql_proxy(false, Some("MySQL Community Server"), None, Some((7, 7)));
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_proxysql_pool_size_one_still_caught_by_banner() {
        let kind = classify_mysql_proxy(false, Some("(ProxySQL) HPMP"), None, Some((5, 5)));
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_default_config_still_detected_via_internal() {
        let kind = classify_mysql_proxy(
            true,
            Some("MySQL Community Server - GPL"),
            None,
            Some((42, 42)),
        );
        assert_eq!(
            kind,
            MysqlProxyKind::ProxySql,
            "default-config ProxySQL must be detectable via PROXYSQL INTERNAL signal alone"
        );
    }

    // ── Warning / label contract ────────────────────────────────────────

    #[test]
    fn proxy_kind_is_proxy_helper_matches_variants() {
        assert!(!MysqlProxyKind::Direct.is_proxy());
        assert!(MysqlProxyKind::ProxySql.is_proxy());
        assert!(MysqlProxyKind::MaxScale.is_proxy());
        assert!(MysqlProxyKind::Multiplexed.is_proxy());
    }

    #[test]
    fn proxy_kind_direct_has_no_warning() {
        assert!(MysqlProxyKind::Direct.warn_message().is_none());
    }

    #[test]
    fn proxy_kind_non_direct_variants_have_warnings() {
        for k in [
            MysqlProxyKind::ProxySql,
            MysqlProxyKind::MaxScale,
            MysqlProxyKind::Multiplexed,
        ] {
            assert!(
                k.warn_message().is_some(),
                "{k:?} must emit a warning at connect time"
            );
        }
    }

    #[test]
    fn proxy_kind_log_labels_are_stable_and_distinct() {
        let labels = [
            MysqlProxyKind::Direct.log_label(),
            MysqlProxyKind::ProxySql.log_label(),
            MysqlProxyKind::MaxScale.log_label(),
            MysqlProxyKind::Multiplexed.log_label(),
        ];
        assert_eq!(
            labels,
            ["direct", "proxysql", "maxscale", "proxy-multiplexed"]
        );
    }

    // ── bit_bytes_to_u64 (lives in arrow_convert.rs, exported pub(super)) ──

    #[test]
    fn bit_bytes_single_byte() {
        assert_eq!(bit_bytes_to_u64(&[0x00]), 0);
        assert_eq!(bit_bytes_to_u64(&[0x01]), 1);
        assert_eq!(bit_bytes_to_u64(&[0xFF]), 255);
    }

    #[test]
    fn bit_bytes_multi_byte() {
        assert_eq!(bit_bytes_to_u64(&[0x01, 0x02]), 258);
        assert_eq!(bit_bytes_to_u64(&[0xFF; 8]), u64::MAX);
    }

    #[test]
    fn bit_bytes_empty() {
        assert_eq!(bit_bytes_to_u64(&[]), 0);
    }

    // ── InnoDB AVG_ROW_LENGTH correction ────────────────────────────────

    #[test]
    fn innodb_correction_below_threshold_is_identity() {
        assert_eq!(correct_innodb_avg_row_length(82), 82);
        assert_eq!(correct_innodb_avg_row_length(314), 314);
        assert_eq!(correct_innodb_avg_row_length(2_048), 2_048);
        assert_eq!(correct_innodb_avg_row_length(8 * 1024), 8 * 1024);
    }

    #[test]
    fn innodb_correction_above_threshold_divides_by_three() {
        assert_eq!(correct_innodb_avg_row_length(40_978), 40_978 / 3);
        assert_eq!(correct_innodb_avg_row_length(120_000), 40_000);
    }

    #[test]
    fn innodb_correction_does_not_undershoot_floor() {
        let just_above = 8 * 1024 + 1;
        let divided = correct_innodb_avg_row_length(just_above);
        assert!(divided >= 4 * 1024, "got {divided}");
    }
}
