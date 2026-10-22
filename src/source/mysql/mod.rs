//! MySQL `Source` implementation.
//!
//! Module layout (mirrors `postgres/`):
//!
//! - `mod.rs` (this file) — `MysqlSource` struct + connect/TLS path, the
//!   extraction-pressure sampler, the `lean_pool_opts` / `connect_pool` /
//!   `build_mysql_ssl_opts` helpers, `introspect_mysql_on`
//!   together with the InnoDB `AVG_ROW_LENGTH` correction, the cursor-bound
//!   `exec_iter` export loop (`mysql_run_export`), and the `Source` trait impl.
//! - [`arrow_convert`] — the entire row → Arrow `RecordBatch` pipeline:
//!   `mysql_type_to_rivet` + `mysql_native_type_name`,
//!   `mysql_schema_and_arrow_types`, BIT / TIME / DECIMAL decoders, and the
//!   array builders. Kept in a sibling because it is the largest
//!   single-purpose cluster in this driver (~510 LoC) and has zero reverse
//!   dependency back into the connection / pool / cursor layer.
//! - [`proxy`] — `MysqlProxyKind` enum, the pure `classify_mysql_proxy`
//!   classifier, the I/O wrapper `detect_mysql_proxy_kind`, and
//!   `warn_proxy_kind`. Detection runs once at connect time; the classifier
//!   is exhaustively unit-tested in isolation (no live MySQL needed).

mod arrow_convert;
pub(crate) use arrow_convert::time_outside_day;
pub(crate) mod cdc;
mod proxy;

use std::sync::Arc;

use arrow::datatypes::Schema;
use mysql::prelude::*;
use mysql::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

use crate::config::{SourceType, TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::batch_controller::{
    AdaptiveBatchController, DEFAULT_BATCH_TARGET_MB, PROBE_BATCH_SIZE,
};
use crate::source::query::build_export_query;
use crate::tuning::SourceTuning;
use crate::types::ColumnOverrides;

use arrow_convert::{
    mysql_native_type_name, mysql_schema_and_arrow_types, mysql_type_to_rivet,
    rows_to_record_batch_typed,
};
// `bit_bytes_to_u64` is only referenced by the `tests` module below — gate the
// re-import on `cfg(test)` so non-test builds don't see an unused-import warning.
#[cfg(test)]
use arrow_convert::bit_bytes_to_u64;
use proxy::{detect_mysql_proxy_kind, warn_proxy_kind};

// Re-exported so external code (`tests/live_pool_safety.rs`) can still write
// `use rivet::source::mysql::MysqlProxyKind` after the proxy block moved to
// the `proxy` submodule.
pub use proxy::MysqlProxyKind;

pub struct MysqlSource {
    pool: Pool,
    proxy_kind: MysqlProxyKind,
    session: std::sync::Arc<SessionPlan>,
}

/// Pool options that prevent eager pre-connection. The default mysql::Pool
/// opens `min=10` connections immediately, which overflows MySQL's
/// max_connections when many parallel exports run simultaneously.
fn lean_pool_opts() -> PoolOpts {
    PoolOpts::default()
        .with_constraints(PoolConstraints::new(1, 100).expect("valid pool constraints"))
        // The session settings stay on a connection rivet's own pool hands back, so a reused
        // connection needs no SET (`MysqlSessionGuard`); a reset would drop them unseen.
        .with_reset_connection(false)
}

/// Sample the FOREIGN-pressure proxy for the concurrency governor:
/// `Innodb_log_waits` — redo-log-buffer waits, pure WRITE pressure. A
/// read-only export barely moves it (the exact reason it is unfit as the batch
/// loop's own-extraction proxy — see [`mysql_sample_extraction_pressure`]),
/// which is precisely what the governor needs: it rises only when someone ELSE
/// is writing hard, so the export's own keyset pages cannot talk the governor
/// into shedding its own workers (field find, 2026-08-13 — see
/// `Source::sample_governor_pressure`).
fn mysql_sample_foreign_pressure(pool: &Pool) -> Option<u64> {
    let mut conn = pool.get_conn().ok()?;
    let rows: Vec<(String, u64)> = conn
        .query("SHOW GLOBAL STATUS LIKE 'Innodb_log_waits'")
        .ok()?;
    if rows.is_empty() {
        return None;
    }
    Some(rows.iter().map(|(_, v)| *v).sum())
}

/// Sample an **extraction-pressure** proxy (Epic 18 C1) — the MySQL analogue of
/// PG's `temp_bytes`, for the ADAPTIVE BATCH loop only (never the governor).
/// Sums two monotonic global counters:
///
/// - `Created_tmp_disk_tables` — a query spilled an internal temp table to disk
///   (a `GROUP BY` / `DISTINCT` / `ORDER BY` that exceeded `tmp_table_size`).
/// - `Innodb_buffer_pool_wait_free` — InnoDB had to wait for a free buffer-pool
///   page, i.e. the read is evicting pages under memory pressure.
///
/// Either moving means "MY extraction is stressing the source"; their sum is
/// monotonic, so a `cur > prev` comparison works unchanged. The sum is robust
/// to MySQL 8.0's `TempTable` engine, where a spill may not bump
/// `Created_tmp_disk_tables` — `Innodb_buffer_pool_wait_free` carries the
/// signal then (and `Created_tmp_disk_tables` adds it on 5.7 / MariaDB).
///
/// Because the export's OWN reads move it, this counter must never reach the
/// concurrency governor: feeding it there is the 2026-08-13 field regression
/// (self-exhaust spiral 4→3→2→1). The governor samples
/// [`mysql_sample_foreign_pressure`] instead.
fn mysql_sample_extraction_pressure(pool: &Pool) -> Option<u64> {
    let mut conn = pool.get_conn().ok()?;
    let rows: Vec<(String, u64)> = conn
        .query(
            "SHOW GLOBAL STATUS WHERE Variable_name IN \
             ('Created_tmp_disk_tables', 'Innodb_buffer_pool_wait_free')",
        )
        .ok()?;
    if rows.is_empty() {
        return None;
    }
    Some(rows.iter().map(|(_, v)| *v).sum())
}

impl MysqlSource {
    /// Build a source from an existing pool: the single place that detects the
    /// proxy kind, warns once, and wraps the pool. The `connect*` entry points
    /// all funnel through here (also handy in tests that share the pool for
    /// post-export state inspection).
    pub fn from_pool(pool: Pool) -> Self {
        Self::over(pool, false)
    }

    /// A source over `pool`; `keeps_session` when the pool hands connections back unreset (rivet's own).
    fn over(pool: Pool, keeps_session: bool) -> Self {
        let proxy_kind = detect_mysql_proxy_kind(&pool);
        Self::with_kind(pool, keeps_session, proxy_kind)
    }

    /// Connect rivet's own pool to `url`. The proxy is probed once per process on the pool's own
    /// first connection, by statements that leave a ProxySQL session multiplexed: no extra dial.
    fn connect_own(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        let pool = connect_pool(url, tls)?;
        let proxy_kind = proxy::proxy_kind_of(url, &pool);
        Ok(Self::with_kind(pool, true, proxy_kind))
    }

    /// A source over `pool`, whose proxy kind is known.
    fn with_kind(pool: Pool, keeps_session: bool, proxy_kind: MysqlProxyKind) -> Self {
        warn_proxy_kind(proxy_kind);
        Self {
            pool,
            proxy_kind,
            session: std::sync::Arc::new(SessionPlan {
                keeps_session,
                ..Default::default()
            }),
        }
    }

    /// Connect with no transport security (legacy path); the live tests reach it
    /// through the library surface, so the binary unit sees it as unused.
    #[allow(dead_code)]
    pub fn connect(url: &str) -> Result<Self> {
        Self::connect_own(url, None)
    }

    /// Connect honoring the user's [`TlsConfig`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        Self::connect_own(url, tls)
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
    // Refuse remote plaintext (no `tls:` block) before any dial (CWE-319).
    crate::source::require_tls_or_loopback(url, tls)?;
    let opts = with_tls(
        session_opts(OptsBuilder::from_opts(Opts::from_url(url)?)).pool_opts(lean_pool_opts()),
        tls,
    );
    let pool = Pool::new(opts).map_err(|e| crate::source::describe_connect_error(url, e.into()))?;
    // Dial now, so a wrong host or port fails here with its name and not at the first query.
    pool.get_conn()
        .map_err(|e| crate::source::describe_connect_error(url, e.into()))?;
    Ok(pool)
}

/// Dial one MySQL `Conn` under the same TLS gate and host:port-naming error as [`connect_pool`].
pub(crate) fn dial_conn(url: &str, tls: Option<&TlsConfig>) -> Result<Conn> {
    crate::source::require_tls_or_loopback(url, tls)?;
    let opts = with_tls(
        session_opts(OptsBuilder::from_opts(Opts::from_url(url)?)),
        tls,
    );
    Conn::new(opts).map_err(|e| crate::source::describe_connect_error(url, e.into()))
}

/// The largest packet the driver splits its own writes at, given so it does not ask the server.
const CLIENT_MAX_ALLOWED_PACKET: usize = 64 * 1024 * 1024;

/// Options under which the driver's handshake reads no `@@` variable (`@@socket`,
/// `@@max_allowed_packet`): ProxySQL stops multiplexing a session that reads one.
fn session_opts(builder: OptsBuilder) -> OptsBuilder {
    builder
        .prefer_socket(false)
        .max_allowed_packet(Some(CLIENT_MAX_ALLOWED_PACKET))
}

/// Apply an enforced [`TlsConfig`] to the driver's SSL options; anything else stays plaintext.
fn with_tls(builder: OptsBuilder, tls: Option<&TlsConfig>) -> Opts {
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => {
            Opts::from(builder.ssl_opts(Some(build_mysql_ssl_opts(cfg))))
        }
        _ => Opts::from(builder),
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
/// MySQL analogue of the Postgres chunk-planner probe:
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
fn introspect_mysql_on(
    conn: &mut mysql::PooledConn,
    qualified_table: &str,
) -> Result<crate::source::TableIntrospection> {
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
            mysql_row_stats(rows, avg, data_len)
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
                Some(t) if is_mysql_integer_type(&t) => Some(col),
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

    // (3) Keyset keys (OPT-4): single-column, NOT NULL, UNIQUE index columns —
    // usable as a seek-pagination key. NON_UNIQUE=0 filters to unique indexes
    // (PRIMARY included); SEQ_IN_INDEX=1 with no SEQ_IN_INDEX=2 row keeps only
    // single-column indexes; IS_NULLABLE='NO' guarantees `> last` never has to
    // reason about NULL ordering. Index-backed by definition, so keyset's
    // `ORDER BY key LIMIT n` is a range scan, not a filesort.
    let keyset_rows: Vec<(String, String, String)> = conn.exec(
        "SELECT s.COLUMN_NAME, s.INDEX_NAME, c.IS_NULLABLE \
         FROM information_schema.STATISTICS s \
         JOIN information_schema.COLUMNS c \
           ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME \
              AND c.COLUMN_NAME = s.COLUMN_NAME \
         WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 0 \
           AND s.SEQ_IN_INDEX = 1 \
           AND c.DATA_TYPE NOT IN ('decimal', 'numeric') \
           AND NOT EXISTS ( \
             SELECT 1 FROM information_schema.STATISTICS s2 \
             WHERE s2.TABLE_SCHEMA = s.TABLE_SCHEMA AND s2.TABLE_NAME = s.TABLE_NAME \
               AND s2.INDEX_NAME = s.INDEX_NAME AND s2.SEQ_IN_INDEX = 2)",
        (&schema, &table),
    )?;
    // PRIMARY first (most efficient — clustered), then other unique indexes.
    let keyset_keys = super::keyset_keys_primary_first(&mysql_keyset_columns(keyset_rows));

    // Integer-family columns — the safety set for an explicit `chunk_column`
    // (see TableIntrospection::int_columns). `DATA_TYPE` is unsigned-agnostic
    // (`int unsigned` still has DATA_TYPE `int`).
    let int_columns: Vec<String> = conn
        .exec::<(String,), _, _>(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               AND DATA_TYPE IN ('tinyint', 'smallint', 'mediumint', 'int', 'bigint')",
            (&schema, &table),
        )?
        .into_iter()
        .map(|(c,)| c)
        .collect();

    Ok(crate::source::TableIntrospection {
        single_int_pk,
        keyset_keys,
        row_estimate,
        avg_row_bytes,
        int_columns,
    })
}

/// The row count and the bytes one row takes, from a table's `information_schema.TABLES` row: the reported average row length, else the data length spread over the rows, corrected for InnoDB's off-page pages; a figure that is not positive is no estimate.
fn mysql_row_stats(rows: i64, avg: i64, data_len: i64) -> (i64, Option<i64>) {
    let row_count = rows.max(0);
    let raw_per_row = if avg > 0 {
        Some(avg)
    } else if row_count > 0 {
        Some(data_len / row_count)
    } else {
        None
    };
    let per_row = raw_per_row.map(correct_innodb_avg_row_length);
    (row_count, per_row.filter(|b| *b > 0))
}

/// Whether a lower-cased MySQL `DATA_TYPE` is of the integer family, the only one a range chunk may slice.
fn is_mysql_integer_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
    )
}

/// The NOT NULL columns among (column, index name, IS_NULLABLE) rows, each with whether its index is the primary key.
fn mysql_keyset_columns(rows: Vec<(String, String, String)>) -> Vec<(String, bool)> {
    rows.into_iter()
        .filter(|(_, _, is_nullable)| is_nullable.eq_ignore_ascii_case("NO"))
        .map(|(col, index_name, _)| (col, index_name == "PRIMARY"))
        .collect()
}

/// `max_execution_time` in milliseconds for a statement timeout in seconds; none when the timeout is off.
fn max_execution_ms(statement_timeout_s: u64) -> Option<u64> {
    (statement_timeout_s > 0).then(|| statement_timeout_s * 1000)
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

/// Refuse a session whose clock is not UTC after rivet set `time_zone = '+00:00'`.
///
/// Every TIMESTAMP is read in the session time zone and written as UTC, so a SET that did not
/// reach the backend that reads (a proxy that answered it without forwarding, or routed the read
/// elsewhere) shifts every value by the server's offset with exit 0. Only the offset decides: a
/// server already in UTC reads right whatever the session says.
fn refuse_a_session_off_utc(zone: &str, offset_s: i64) -> Result<()> {
    if offset_s == 0 {
        return Ok(());
    }
    let hours = offset_s as f64 / 3600.0;
    crate::rivet_bail!(
        crate::error::codes::SOURCE_SESSION_TIME_ZONE_NOT_APPLIED,
        "mysql: after `SET time_zone = '+00:00'` the session that reads is at `{zone}`, \
         {hours:+} h from UTC: the SET did not reach the backend connection (a proxy such as \
         ProxySQL answered it without forwarding it, or sent the read to another backend), so \
         every TIMESTAMP would be written {hours:+} h off as UTC. Nothing was read. Set \
         `default_time_zone = '+00:00'` on the server, let the proxy keep rivet's session \
         variables on one backend (ProxySQL: session-variable tracking, no multiplexing for \
         rivet's user), or connect rivet to MySQL directly."
    )
}

#[cfg(test)]
mod session_time_zone_tests {
    #[test]
    fn a_session_off_utc_is_refused_by_code_and_a_utc_one_reads() {
        super::refuse_a_session_off_utc("+00:00", 0).unwrap();
        super::refuse_a_session_off_utc("SYSTEM", 0).unwrap();
        for (zone, offset, hours) in [
            ("SYSTEM", 10_800, "+3 h"),
            ("+03:00", 10_800, "+3 h"),
            ("-05:30", -19_800, "-5.5 h"),
        ] {
            let err = super::refuse_a_session_off_utc(zone, offset).unwrap_err();
            assert_eq!(
                crate::error::error_code(&err),
                Some("RIVET_SOURCE_SESSION_TIME_ZONE_NOT_APPLIED")
            );
            assert_eq!(crate::error::classify_exit(&err), 5);
            let text = format!("{err:#}");
            assert!(text.contains(hours), "{text}");
            assert!(
                text.contains(zone) && text.contains("default_time_zone"),
                "{text}"
            );
        }
    }
}

/// The server defaults a session is derived from, read once per pool.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerDefaults {
    sql_mode: String,
    max_exec_ms: u64,
}

/// Reads the server's `sql_mode` and `max_execution_time` without a `@@` variable, whose read
/// makes ProxySQL stop multiplexing the session.
const SERVER_DEFAULTS: &str =
    "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('sql_mode', 'max_execution_time')";

/// The session clock's offset from UTC, read without a session variable so a multiplexing proxy keeps multiplexing.
const SESSION_OFFSET: &str = "SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), NOW(6))";

/// The session settings of one pool's connections: the server defaults, read once, and the
/// `max_execution_time` each connection was last given, by connection id.
#[derive(Debug, Default)]
struct SessionPlan {
    /// Whether the pool keeps a connection's session when it is handed back (rivet's own pool).
    keeps_session: bool,
    defaults: std::sync::OnceLock<ServerDefaults>,
    given: std::sync::Mutex<std::collections::HashMap<u32, u64>>,
}

impl SessionPlan {
    /// The `max_execution_time` connection `id` still holds, or `None` when it must be set from
    /// scratch: always on a pool that resets a session it is handed back.
    fn given(&self, id: u32) -> Option<u64> {
        if !self.keeps_session {
            return None;
        }
        self.given
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .copied()
    }

    /// Record that connection `id` was given `max_exec_ms`.
    fn record(&self, id: u32, max_exec_ms: u64) {
        self.given
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, max_exec_ms);
    }

    /// The server defaults, read on `conn` the first time they are asked for.
    fn defaults(&self, conn: &mut mysql::PooledConn) -> Result<ServerDefaults> {
        if let Some(d) = self.defaults.get() {
            return Ok(d.clone());
        }
        let rows: Vec<(String, String)> = conn.query(SERVER_DEFAULTS)?;
        let value = |name: &str| {
            rows.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        let read = ServerDefaults {
            sql_mode: value("sql_mode").unwrap_or_default(),
            max_exec_ms: value("max_execution_time")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        };
        Ok(self.defaults.get_or_init(|| read).clone())
    }
}

/// The server's `sql_mode` without `NO_BACKSLASH_ESCAPES`, which the inline keyset boundary literals need off.
fn wanted_sql_mode(global: &str) -> String {
    global
        .split(',')
        .map(str::trim)
        .filter(|m| !m.is_empty() && !m.eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The literal SETs a connection needs: all three the first time (`given` is `None`), else only
/// `max_execution_time` when this read wants another one than the connection was given. The
/// time zone is UTC (Parquet writes `isAdjustedToUTC=true`), `sql_mode` the server's without
/// `NO_BACKSLASH_ESCAPES`, and `max_execution_time` the configured one, else the server's.
fn session_sets(
    defaults: &ServerDefaults,
    given: Option<u64>,
    max_exec_ms: Option<u64>,
) -> Vec<String> {
    let max = max_exec_ms.unwrap_or(defaults.max_exec_ms);
    let mut sets = Vec::new();
    if given.is_none() {
        sets.push("SET time_zone = '+00:00'".to_string());
        sets.push(format!(
            "SET SESSION sql_mode = '{}'",
            wanted_sql_mode(&defaults.sql_mode)
                .replace('\\', "\\\\")
                .replace('\'', "''")
        ));
    }
    if given != Some(max) {
        sets.push(format!("SET SESSION max_execution_time = {max}"));
    }
    sets
}

/// A pooled connection in the session a read needs.
///
/// The settings are literal SETs, sent once per connection and kept on it: the pool is rivet's
/// own, and a later read that wants another `max_execution_time` sends that one literal. A literal
/// SET is what ProxySQL tracks and replays on whichever backend connection it multiplexes the
/// session onto; an expression (`REPLACE(@@sql_mode, ..)`, `@@global.time_zone`) it cannot parse
/// and locks the session to one hostgroup, and a read of a session variable stops multiplexing.
/// The session clock is checked on every checkout, so a proxy that dropped the SET is refused
/// before a row is read.
struct MysqlSessionGuard<'a> {
    conn: &'a mut mysql::PooledConn,
}

impl<'a> MysqlSessionGuard<'a> {
    /// Bring the session to what a read needs and refuse one whose clock is not UTC.
    fn apply(
        conn: &'a mut mysql::PooledConn,
        plan: &SessionPlan,
        max_exec_ms: Option<u64>,
    ) -> Result<Self> {
        let defaults = plan.defaults(conn)?;
        let id = conn.connection_id();
        let sets = session_sets(&defaults, plan.given(id), max_exec_ms);
        for set in &sets {
            conn.query_drop(set)?;
        }
        if !sets.is_empty() {
            plan.record(id, max_exec_ms.unwrap_or(defaults.max_exec_ms));
        }
        let offset_s: i64 = conn.query_first(SESSION_OFFSET)?.unwrap_or(0);
        if offset_s != 0 {
            let zone: Option<String> = conn.query_first("SELECT @@session.time_zone")?;
            refuse_a_session_off_utc(zone.as_deref().unwrap_or(""), offset_s)?;
        }
        Ok(Self { conn })
    }

    fn conn(&mut self) -> &mut mysql::PooledConn {
        self.conn
    }
}

#[cfg(test)]
mod session_set_tests {
    use super::*;

    fn defaults() -> ServerDefaults {
        ServerDefaults {
            sql_mode: "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES".into(),
            max_exec_ms: 0,
        }
    }

    /// A new connection gets three literal SETs; a reused one none, or one when its timeout changes.
    #[test]
    fn a_connection_is_set_once_by_literals_and_again_only_for_another_timeout() {
        assert_eq!(
            session_sets(&defaults(), None, Some(300_000)),
            [
                "SET time_zone = '+00:00'",
                "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'",
                "SET SESSION max_execution_time = 300000",
            ]
        );
        assert!(session_sets(&defaults(), Some(300_000), Some(300_000)).is_empty());
        assert_eq!(
            session_sets(&defaults(), Some(300_000), None),
            ["SET SESSION max_execution_time = 0"],
            "an export without a timeout gets the server's back, as a literal"
        );
        assert!(session_sets(&defaults(), Some(0), None).is_empty());
        for sql in session_sets(&defaults(), None, Some(1)) {
            assert!(!sql.contains('@'), "a literal, never an expression: {sql}");
        }
    }

    /// The driver's options read no `@@` at the handshake: no socket lookup, and a packet size
    /// given rather than asked for, exactly 64 MiB.
    #[test]
    fn the_session_options_ask_the_server_for_no_variable() {
        let opts = Opts::from(session_opts(OptsBuilder::new()));
        assert!(!opts.get_prefer_socket());
        assert_eq!(opts.get_max_allowed_packet(), Some(64 * 1024 * 1024));
        assert_eq!(CLIENT_MAX_ALLOWED_PACKET, 67_108_864);
    }

    /// A source over rivet's own pool keeps each connection's settings, so a reused connection
    /// is not set again; over a pool that resets sessions every checkout is set from scratch.
    #[test]
    fn only_a_pool_that_keeps_sessions_skips_the_sets_on_a_reused_connection() {
        let pool = || {
            let lazy = PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 1).expect("valid pool constraints"));
            Pool::new(
                OptsBuilder::new()
                    .ip_or_hostname(Some("127.0.0.1"))
                    .tcp_port(1)
                    .pool_opts(lazy),
            )
            .expect("a pool with no minimum dials nothing")
        };
        for keeps in [true, false] {
            let source = MysqlSource::with_kind(pool(), keeps, MysqlProxyKind::Direct);
            let plan = &source.session;
            assert_eq!(plan.keeps_session, keeps);
            assert_eq!(plan.given(7), None, "a new connection is set");
            plan.record(7, 300_000);
            let again = session_sets(&defaults(), plan.given(7), Some(300_000));
            assert_eq!(again.is_empty(), keeps, "keeps_session={keeps}: {again:?}");
        }
    }

    #[test]
    fn the_wanted_sql_mode_is_the_servers_without_no_backslash_escapes() {
        assert_eq!(wanted_sql_mode("NO_BACKSLASH_ESCAPES"), "");
        assert_eq!(
            wanted_sql_mode("ANSI_QUOTES,no_backslash_escapes,STRICT_ALL_TABLES"),
            "ANSI_QUOTES,STRICT_ALL_TABLES"
        );
        assert_eq!(wanted_sql_mode(""), "");
    }
}

/// Execute the MySQL query and stream results to sink.
///
/// The session (`time_zone`, `sql_mode`, `max_execution_time`) is set by the caller's
/// [`MysqlSessionGuard`] before the read.
///
/// `sample_pool`: when `tuning.adaptive` is true, a clone of the source pool used
/// to obtain a second connection for extraction-pressure sampling without interfering
/// with the streaming result set on `conn`.
/// Whether a MySQL driver error is the server-side `max_execution_time` /
/// ER_QUERY_TIMEOUT (3024) statement-duration timeout — so the export path can
/// re-wrap it as an actionable `StatementDurationTimeout` instead of leaking the
/// terse driver prose. Matched by code AND message so it survives a MariaDB
/// spelling (`max_statement_time` / 1969) or a reworded server string.
fn is_statement_timeout(e: &mysql::Error) -> bool {
    if let mysql::Error::MySqlError(db) = e
        && (db.code == 3024 || db.code == 1969)
    {
        return true;
    }
    let msg = e.to_string();
    msg.contains("max_execution_time")
        || msg.contains("max_statement_time")
        || msg.contains("maximum statement execution time exceeded")
}

fn mysql_run_export(
    conn: &mut mysql::PooledConn,
    sample_pool: Option<Pool>,
    sql: &str,
    cursor_param: Option<&str>,
    tuning: &SourceTuning,
    column_overrides: &ColumnOverrides,
    sink: &mut dyn super::BatchSink,
) -> Result<usize> {
    // Re-wrap a server-side `max_execution_time` timeout (ERROR 3024) as an
    // actionable StatementDurationTimeout (the classifier downcasts the TYPE →
    // permanent; the Display carries the fix). The raw driver error is terse and
    // gave the field's `*_version` timeouts no guidance (#field-3024).
    let wrap_timeout = |e: mysql::Error| -> anyhow::Error {
        if tuning.statement_timeout_s > 0 && is_statement_timeout(&e) {
            super::StatementDurationTimeout::mysql(tuning.statement_timeout_s).into()
        } else {
            anyhow::Error::new(e)
        }
    };

    // SecOps: cursor value is bound via exec_iter rather than string-interpolated.
    // Using exec_iter uniformly (even with empty params) keeps match arms
    // type-compatible — query_iter returns a Text-protocol result, exec_iter Binary.
    let mut result = match cursor_param {
        Some(val) => conn.exec_iter(sql, (val,)).map_err(&wrap_timeout)?,
        None => conn.exec_iter(sql, ()).map_err(&wrap_timeout)?,
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
    let configured_batch_size = tuning.effective_batch_size(Some(&schema));
    // Shared batch-size state machine (probe → memory-cap → adaptive → throttle);
    // MySQL provides only the row source + the target-MB cap formula below.
    let mut ctl = AdaptiveBatchController::new(tuning, configured_batch_size);
    ctl.seed_pressure(if tuning.adaptive {
        sample_pool
            .as_ref()
            .and_then(mysql_sample_extraction_pressure)
    } else {
        None
    });
    let row_set = result.iter().ok_or_else(|| {
        anyhow::Error::new(crate::error::CodedError::new(
            crate::error::codes::INTERNAL_SOURCE_INVARIANT,
            "no result set".to_string(),
        ))
    })?;
    let mut row_buf: Vec<mysql::Row> = Vec::with_capacity(ctl.target());
    let mut total_rows: usize = 0;
    let mut memory_cap_applied = false;
    // Per-value ceiling (MB→bytes; `0`/None disables), enforced pre-allocation
    // inside the batch builder so an oversized cell bails before Arrow reserves
    // the buffer. Same source of truth as the sink's backstop guard.
    let max_value_bytes = tuning.max_value_bytes();

    for row_result in row_set {
        // The timeout usually fires mid-stream (the query runs while rows are
        // pulled), so wrap here too, not only at exec_iter above.
        let row = row_result.map_err(&wrap_timeout)?;
        row_buf.push(row);

        if row_buf.len() >= ctl.target() {
            total_rows += row_buf.len();
            let batch =
                rows_to_record_batch_typed(&schema, &arrow_types, &row_buf, max_value_bytes)?;
            let batch_rows = row_buf.len();
            row_buf.clear();

            // After the first (probe-sized) batch we know how many bytes per
            // row Arrow actually uses. Cap subsequent flushes to a memory
            // target. The controller clamps it to the configured `batch_size`.
            if !memory_cap_applied && batch_rows > 0 {
                let arrow_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
                let arrow_per_row = (arrow_bytes / batch_rows).max(64);
                let target_mb = tuning
                    .batch_size_memory_mb
                    .unwrap_or(DEFAULT_BATCH_TARGET_MB);
                let safe = ((target_mb * 1024 * 1024) / arrow_per_row).max(PROBE_BATCH_SIZE);
                if let Some(new) = ctl.apply_memory_cap(safe) {
                    log::info!(
                        "MySQL row_buf cap: arrow≈{} B/row, target={} MB → batch_size → {} (configured={})",
                        arrow_per_row,
                        target_mb,
                        new,
                        configured_batch_size
                    );
                    row_buf.reserve(new.saturating_sub(row_buf.capacity()));
                }
                memory_cap_applied = true;
            }

            sink.on_batch(&batch)?;

            if let Some((new, under_pressure)) = ctl.after_batch(|| {
                sample_pool
                    .as_ref()
                    .and_then(mysql_sample_extraction_pressure)
            }) {
                log::info!(
                    "adaptive batch size → {} ({})",
                    new,
                    if under_pressure {
                        "pressure"
                    } else {
                        "recovery"
                    }
                );
            }

            log::info!("fetched {} rows so far...", total_rows);
            ctl.throttle(batch.num_rows());
        }
    }

    if !row_buf.is_empty() {
        total_rows += row_buf.len();
        let batch = rows_to_record_batch_typed(&schema, &arrow_types, &row_buf, max_value_bytes)?;
        sink.on_batch(&batch)?;
    }

    drop(result);
    Ok(total_rows)
}

impl super::Source for MysqlSource {
    fn introspect_for_chunking(
        &mut self,
        qualified_table: &str,
    ) -> Result<crate::source::TableIntrospection> {
        introspect_mysql_on(&mut self.pool.get_conn()?, qualified_table)
    }

    fn export(
        &mut self,
        request: &super::ExportRequest<'_>,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_export_query(request, SourceType::Mysql);
        log::debug!(
            "executing query (connection={}): {}",
            self.proxy_kind.log_label(),
            built.sql
        );

        let mut conn = self.pool.get_conn()?;

        // The session a read needs, set once per pooled connection by literal SETs (ProxySQL
        // tracks `time_zone`, `sql_mode` and `max_execution_time` and replays them on the backend
        // it multiplexes the session onto).
        let max_exec_ms = max_execution_ms(request.tuning.statement_timeout_s);
        let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, max_exec_ms)?;

        let sample_pool = if request.tuning.adaptive {
            Some(self.pool.clone())
        } else {
            None
        };
        let result = mysql_run_export(
            guard.conn(),
            sample_pool,
            &built.sql,
            built.cursor_param.as_deref(),
            request.tuning,
            request.column_overrides,
            sink,
        );

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

    fn primary_key(&mut self, table: &str) -> Result<Option<Vec<String>>> {
        let bare = table.replace('`', "");
        let (schema, name) = match bare.split_once('.') {
            Some((s, t)) => (format!("'{}'", s.replace('\'', "''")), t.to_string()),
            None => ("DATABASE()".to_string(), bare.clone()),
        };
        // One row per column, assembled here — not `GROUP_CONCAT`, which silently
        // TRUNCATES at `group_concat_max_len` (1024 bytes by default) and handed the
        // load a partial merge key that collapses distinct rows.
        let sql = format!(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = {schema} \
             AND TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
            name.replace('\'', "''")
        );
        let mut conn = self.pool.get_conn()?;
        let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, None)?;
        let cols: Vec<String> = guard.conn().query(sql)?;
        Ok(crate::source::split_key_list(Some(cols.join("\u{1f}"))))
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let mut conn = self.pool.get_conn()?;
        // Pin the SAME session state the export/worker connections get (sql_mode with
        // NO_BACKSLASH_ESCAPES stripped + UTC time_zone) via the shared guard.
        // sample_key_boundaries runs INLINE escaped boundary literals through here; a
        // sampler whose sql_mode disagrees with the workers' mis-parses a backslash-
        // bearing key and drifts the percentile split points (the M1 hole this closes).
        let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, None)?;
        let row: Option<mysql::Row> = guard.conn().query_first(sql)?;
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
                let rivet =
                    crate::types::resolve_or(column_overrides, col.name_str().as_ref(), || {
                        mysql_type_to_rivet(col)
                    });
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

    /// Governor FOREIGN-pressure proxy: `Innodb_log_waits` (redo-write
    /// pressure a read-only export cannot move) — deliberately NOT the batch
    /// loop's extraction-pressure sum, which the export's own pages inflate
    /// (see [`mysql_sample_foreign_pressure`] and
    /// `Source::sample_governor_pressure`).
    fn sample_governor_pressure(&mut self) -> Option<u64> {
        mysql_sample_foreign_pressure(&self.pool)
    }

    /// Snapshot the broader source-harm counters from `SHOW GLOBAL STATUS` — a
    /// superset of the batch loop's [`mysql_sample_extraction_pressure`].
    /// Returns `(metric, cumulative_value)` pairs the pipeline deltas around
    /// the export and stores in `export_harm`. `SHOW GLOBAL STATUS` needs **no
    /// special privilege**. These are global counters, so concurrent load
    /// inflates the delta (accurate on a quiet pilot box). `Innodb_rows_read`
    /// is the read-amplification signal the 0.12 harm A/B keyed on. `None` on
    /// connect/query failure — never blocks the export.
    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        let pool = &self.pool;
        let mut conn = pool.get_conn().ok()?;
        let rows: Vec<(String, i64)> = conn
            .query(
                "SHOW GLOBAL STATUS WHERE Variable_name IN \
             ('Innodb_rows_read', 'Innodb_buffer_pool_reads', 'Created_tmp_disk_tables', \
              'Handler_read_rnd_next', 'Innodb_row_lock_waits', 'Innodb_row_lock_time')",
            )
            .ok()?;
        if rows.is_empty() {
            return None;
        }
        Some(
            rows.into_iter()
                .map(|(k, v)| (format!("mysql_{}", k.to_lowercase()), v))
                .collect(),
        )
    }

    fn native_column_types(&mut self, table: &str) -> Result<Vec<(String, String)>> {
        // A qualified `db.table` carries its OWN schema, which may differ from the
        // connection's DATABASE() (a cross-database capture), and a db-less URL has no
        // DATABASE() at all — so the qualifier wins and only a bare name asks.
        let mut conn = self.pool.get_conn()?;
        let default_db: Option<String> = if table.contains('.') {
            None
        } else {
            conn.query_first("SELECT DATABASE()")?
        };
        let (schema, bare) =
            crate::source::cdc::enrich_schema_and_table(table, default_db.as_deref());
        Ok(conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (&schema, &bare),
        )?)
    }

    fn server_context(&mut self) -> Option<String> {
        // One `SHOW GLOBAL VARIABLES`, never a `@@` read: ProxySQL stops multiplexing a session
        // that reads one. Global values: the server truth this forensic snapshot exists for, not
        // the session the guard set (+00:00, NO_BACKSLASH_ESCAPES stripped). Best-effort per
        // variable (a proxy or older server may omit one). `max_execution_time` (ms) is the limit
        // that surfaces as ERROR 3024.
        let rows: Vec<(String, String)> = (|| -> Result<Vec<(String, String)>> {
            let mut conn = self.pool.get_conn()?;
            let mut guard = MysqlSessionGuard::apply(&mut conn, &self.session, None)?;
            Ok(guard.conn().query(
                "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('version', 'max_execution_time', \
                 'sql_mode', 'time_zone', 'wait_timeout', 'max_connections')",
            )?)
        })()
        .unwrap_or_default();
        let var = |name: &str| {
            rows.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        };
        Some(
            serde_json::json!({
                "engine": "mysql",
                "version": var("version"),
                "max_execution_time_ms": var("max_execution_time"),
                "sql_mode": var("sql_mode"),
                "time_zone": var("time_zone"),
                "wait_timeout_s": var("wait_timeout"),
                "max_connections": var("max_connections"),
            })
            .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{bit_bytes_to_u64, correct_innodb_avg_row_length};

    /// The row size is the reported average, else the data length over the rows, and nothing when neither is positive.
    #[test]
    fn row_stats_prefer_the_reported_average_and_fall_back_to_data_over_rows() {
        use super::mysql_row_stats;
        assert_eq!(mysql_row_stats(100, 50, 9_000), (100, Some(50)));
        assert_eq!(mysql_row_stats(100, 0, 9_000), (100, Some(90)));
        assert_eq!(mysql_row_stats(1, 0, 7), (1, Some(7)));
        assert_eq!(mysql_row_stats(0, 0, 9_000), (0, None));
        assert_eq!(mysql_row_stats(-5, 0, 9_000), (0, None));
        assert_eq!(
            mysql_row_stats(100, 0, 0),
            (100, None),
            "zero bytes is no estimate"
        );
        assert_eq!(
            mysql_row_stats(100, 0, 50),
            (100, None),
            "under a byte a row is no estimate"
        );
        assert_eq!(mysql_row_stats(0, 12, 0), (0, Some(12)));
        assert_eq!(
            mysql_row_stats(10, 41_000, 0),
            (10, Some(correct_innodb_avg_row_length(41_000))),
            "an off-page row length is corrected"
        );
    }

    #[test]
    fn only_the_integer_family_is_an_integer_type() {
        use super::is_mysql_integer_type;
        for t in ["tinyint", "smallint", "mediumint", "int", "bigint"] {
            assert!(is_mysql_integer_type(t), "{t}");
        }
        for t in ["decimal", "varchar", "bit", "double", "integer", ""] {
            assert!(!is_mysql_integer_type(t), "{t}");
        }
    }

    /// A nullable column is no keyset key, and only the index named PRIMARY is the primary key.
    #[test]
    fn keyset_columns_are_the_not_null_ones_marked_by_the_primary_index() {
        use super::mysql_keyset_columns;
        let row = |c: &str, i: &str, n: &str| (c.to_string(), i.to_string(), n.to_string());
        assert_eq!(
            mysql_keyset_columns(vec![
                row("email", "uq_email", "NO"),
                row("nick", "uq_nick", "YES"),
                row("id", "PRIMARY", "no"),
                row("code", "primary_like", "NO"),
            ]),
            [
                ("email".to_string(), false),
                ("id".to_string(), true),
                ("code".to_string(), false)
            ]
        );
    }

    #[test]
    fn a_statement_timeout_becomes_milliseconds_and_zero_is_no_timeout() {
        use super::max_execution_ms;
        assert_eq!(max_execution_ms(0), None);
        assert_eq!(max_execution_ms(1), Some(1_000));
        assert_eq!(max_execution_ms(300), Some(300_000));
    }

    /// The server's statement timeout is known by either error code or by any one of its three message spellings.
    #[test]
    fn a_statement_timeout_is_known_by_its_code_or_by_its_message() {
        use super::is_statement_timeout;
        let server = |code: u16, message: &str| {
            mysql::Error::MySqlError(mysql::MySqlError {
                state: "HY000".into(),
                message: message.into(),
                code,
            })
        };
        assert!(is_statement_timeout(&server(3024, "interrupted")));
        assert!(is_statement_timeout(&server(1969, "interrupted")));
        for message in [
            "Query execution was interrupted, max_execution_time exceeded",
            "max_statement_time exceeded",
            "Query execution was interrupted, maximum statement execution time exceeded",
        ] {
            assert!(is_statement_timeout(&server(1, message)), "{message}");
        }
        assert!(!is_statement_timeout(&server(
            1205,
            "Lock wait timeout exceeded"
        )));
        assert!(!is_statement_timeout(&server(
            1064,
            "You have an error in your SQL syntax"
        )));
    }

    #[test]
    fn the_pool_opens_one_connection_until_more_are_asked_for() {
        let constraints = super::lean_pool_opts().constraints();
        assert_eq!((constraints.min(), constraints.max()), (1, 100));
    }

    /// Each TLS mode and each override sets exactly the driver options it names.
    #[test]
    fn tls_options_follow_the_mode_the_ca_file_and_each_override() {
        use crate::config::{TlsConfig, TlsMode};
        let opts = |cfg: TlsConfig| {
            let ssl = super::build_mysql_ssl_opts(&cfg);
            (
                ssl.accept_invalid_certs(),
                ssl.skip_domain_validation(),
                ssl.root_cert_path()
                    .map(|p| p.to_string_lossy().into_owned()),
            )
        };
        let mode = |mode| TlsConfig {
            mode,
            ..Default::default()
        };
        assert_eq!(opts(mode(TlsMode::Require)), (true, true, None));
        assert_eq!(opts(mode(TlsMode::VerifyCa)), (false, true, None));
        assert_eq!(opts(mode(TlsMode::VerifyFull)), (false, false, None));
        assert_eq!(
            opts(TlsConfig {
                ca_file: Some("/etc/ca.pem".into()),
                ..mode(TlsMode::VerifyFull)
            }),
            (false, false, Some("/etc/ca.pem".to_string()))
        );
        assert_eq!(
            opts(TlsConfig {
                accept_invalid_certs: true,
                ..mode(TlsMode::VerifyFull)
            }),
            (true, false, None)
        );
        assert_eq!(
            opts(TlsConfig {
                accept_invalid_hostnames: true,
                ..mode(TlsMode::VerifyFull)
            }),
            (false, true, None)
        );
    }

    #[test]
    fn only_an_enforced_tls_mode_turns_on_the_driver_ssl_options() {
        use crate::config::{TlsConfig, TlsMode};
        let ssl = |mode: Option<TlsMode>| {
            let cfg = mode.map(|mode| TlsConfig {
                mode,
                ..Default::default()
            });
            let builder =
                mysql::OptsBuilder::from_opts(mysql::Opts::from_url("mysql://h/db").unwrap());
            super::with_tls(builder, cfg.as_ref())
                .get_ssl_opts()
                .is_some()
        };
        assert!(ssl(Some(TlsMode::Require)));
        assert!(!ssl(Some(TlsMode::Disable)));
        assert!(!ssl(None));
    }

    // Proxy classifier tests live in `proxy.rs` alongside the classifier.

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

    #[test]
    fn bit_bytes_ascii_digit_bytes_are_bits_not_text() {
        // Regression (mysql-bit): BIT bytes that happen to be ASCII digits are
        // still big-endian bits — never decimal text.
        assert_eq!(bit_bytes_to_u64(&[0x39]), 57); // "9" as text, BIT(8) 57
        assert_eq!(bit_bytes_to_u64(&[0x31, 0x32]), 0x3132); // b"12" → 12594
        assert_eq!(bit_bytes_to_u64(&[0x31, 0xFF]), 12799); // digit head, non-digit tail
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
