//! MySQL `Source` implementation.
//!
//! Module layout (mirrors `postgres/`):
//!
//! - `mod.rs` (this file) — `MysqlSource` struct + connect/TLS path, the
//!   `InnoDB_log_waits` sampler, the `lean_pool_opts` / `connect_pool` /
//!   `build_mysql_ssl_opts` helpers, `introspect_mysql_table_for_chunking`
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
mod proxy;

use std::sync::Arc;

use arrow::datatypes::Schema;
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

use crate::config::{SourceType, TlsConfig, TlsMode};
use crate::error::Result;
use crate::source::batch_controller::{AdaptiveBatchController, PROBE_BATCH_SIZE};
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
}

/// Pool options that prevent eager pre-connection. The default mysql::Pool
/// opens `min=10` connections immediately, which overflows MySQL's
/// max_connections when many parallel exports run simultaneously.
fn lean_pool_opts() -> PoolOpts {
    PoolOpts::default()
        .with_constraints(PoolConstraints::new(1, 100).expect("valid pool constraints"))
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
           AND NOT EXISTS ( \
             SELECT 1 FROM information_schema.STATISTICS s2 \
             WHERE s2.TABLE_SCHEMA = s.TABLE_SCHEMA AND s2.TABLE_NAME = s.TABLE_NAME \
               AND s2.INDEX_NAME = s.INDEX_NAME AND s2.SEQ_IN_INDEX = 2)",
        (&schema, &table),
    )?;
    let mut keyset_keys: Vec<String> = Vec::new();
    // PRIMARY first (most efficient — clustered), then other unique indexes.
    for primary in [true, false] {
        for (col, index_name, is_nullable) in &keyset_rows {
            let is_primary = index_name == "PRIMARY";
            if is_primary == primary
                && is_nullable.eq_ignore_ascii_case("NO")
                && !keyset_keys.contains(col)
            {
                keyset_keys.push(col.clone());
            }
        }
    }

    Ok(crate::source::TableIntrospection {
        single_int_pk,
        keyset_keys,
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
    const MYSQL_BATCH_TARGET_MB_DEFAULT: usize = 64;

    let configured_batch_size = tuning.effective_batch_size(Some(&schema));
    // Shared batch-size state machine (probe → memory-cap → adaptive → throttle);
    // MySQL provides only the row source + the target-MB cap formula below.
    let mut ctl = AdaptiveBatchController::new(tuning, configured_batch_size);
    ctl.seed_pressure(if tuning.adaptive {
        sample_pool.as_ref().and_then(mysql_sample_innodb_log_waits)
    } else {
        None
    });
    let row_set = result
        .iter()
        .ok_or_else(|| anyhow::anyhow!("no result set"))?;
    let mut row_buf: Vec<mysql::Row> = Vec::with_capacity(ctl.target());
    let mut total_rows: usize = 0;
    let mut memory_cap_applied = false;

    for row_result in row_set {
        let row = row_result?;
        row_buf.push(row);

        if row_buf.len() >= ctl.target() {
            total_rows += row_buf.len();
            let batch = rows_to_record_batch_typed(&schema, &arrow_types, &row_buf)?;
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
                    .unwrap_or(MYSQL_BATCH_TARGET_MB_DEFAULT);
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

            if let Some((new, under_pressure)) =
                ctl.after_batch(|| sample_pool.as_ref().and_then(mysql_sample_innodb_log_waits))
            {
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
            ctl.throttle();
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
        let built = build_export_query(request, SourceType::Mysql);
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

    /// Governor pressure proxy: global `Innodb_log_waits` — the same monotonic
    /// counter the adaptive batch loop samples. Rising between samples means
    /// InnoDB is stalling on redo-log buffer space under write pressure.
    fn sample_pressure(&mut self) -> Option<u64> {
        mysql_sample_innodb_log_waits(&self.pool)
    }
}

#[cfg(test)]
mod tests {
    use super::{bit_bytes_to_u64, correct_innodb_avg_row_length};

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
