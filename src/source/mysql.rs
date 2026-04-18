use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, SslOpts, Value};

use crate::config::{SourceType, TlsConfig, TlsMode};
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::source::query::build_incremental_query;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

pub struct MysqlSource {
    pool: Pool,
}

impl MysqlSource {
    /// Connect with no transport security (legacy path).
    pub fn connect(url: &str) -> Result<Self> {
        let opts = Opts::from_url(url)?;
        let pool = Pool::new(opts)?;
        Ok(Self { pool })
    }

    /// Connect honoring the user's [`TlsConfig`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let base = Opts::from_url(url)?;
                let ssl = build_mysql_ssl_opts(cfg);
                let opts = Opts::from(OptsBuilder::from_opts(base).ssl_opts(Some(ssl)));
                let pool = Pool::new(opts)?;
                Ok(Self { pool })
            }
            _ => Self::connect(url),
        }
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
            let opts = Opts::from(OptsBuilder::from_opts(base).ssl_opts(Some(ssl)));
            Ok(Pool::new(opts)?)
        }
        _ => Ok(Pool::new(Opts::from_url(url)?)?),
    }
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

impl super::Source for MysqlSource {
    fn export(
        &mut self,
        query: &str,
        incremental: Option<&IncrementalCursorPlan>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_incremental_query(query, incremental, cursor, SourceType::Mysql);
        log::debug!("executing query: {}", built.sql);

        let mut conn = self.pool.get_conn()?;

        if tuning.statement_timeout_s > 0 {
            conn.query_drop(format!(
                "SET SESSION max_execution_time = {}",
                tuning.statement_timeout_s * 1000
            ))?;
        }

        // SecOps: cursor value is bound via `exec_iter` rather than string-interpolated.
        // Using `exec_iter` uniformly (even with empty params) keeps the match arms
        // type-compatible — `query_iter` returns a Text-protocol result, `exec_iter`
        // returns a Binary-protocol result.
        let mut result = match built.cursor_param.as_deref() {
            Some(val) => conn.exec_iter(&built.sql, (val,))?,
            None => conn.exec_iter(&built.sql, ())?,
        };
        let columns = result.columns().as_ref().to_vec();
        let schema = Arc::new(mysql_columns_to_schema(&columns));
        let arrow_types: Vec<DataType> = columns.iter().map(mysql_type_to_arrow).collect();

        sink.on_schema(schema.clone())?;

        let effective_bs = tuning.effective_batch_size(Some(&schema));
        let row_set = result
            .iter()
            .ok_or_else(|| anyhow::anyhow!("no result set"))?;
        let mut row_buf: Vec<mysql::Row> = Vec::with_capacity(effective_bs);
        let mut total_rows: usize = 0;

        for row_result in row_set {
            let row = row_result?;
            row_buf.push(row);

            if row_buf.len() >= effective_bs {
                total_rows += row_buf.len();
                let batch = rows_to_record_batch_typed(&schema, &arrow_types, &row_buf)?;
                sink.on_batch(&batch)?;
                row_buf.clear();

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

        if tuning.statement_timeout_s > 0 {
            conn.query_drop("SET SESSION max_execution_time = 0")?;
        }

        log::info!("total: {} rows", total_rows);
        Ok(())
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        use mysql::prelude::*;
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
}

fn mysql_type_to_arrow(col: &mysql::Column) -> DataType {
    use mysql::consts::ColumnType::*;
    match col.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT => DataType::Int16,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => DataType::Int32,
        MYSQL_TYPE_LONGLONG => DataType::Int64,
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE => DataType::Float64,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => DataType::Utf8,
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET => DataType::Utf8,
        MYSQL_TYPE_JSON => DataType::Utf8,
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            if col.character_set() == 63 {
                DataType::Binary
            } else {
                DataType::Utf8
            }
        }
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => DataType::Date32,
        MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2 => DataType::Timestamp(TimeUnit::Microsecond, None),
        MYSQL_TYPE_BIT => DataType::Boolean,
        MYSQL_TYPE_YEAR => DataType::Int16,
        _ => {
            log::warn!(
                "unmapped MySQL type {:?}, falling back to Utf8",
                col.column_type()
            );
            DataType::Utf8
        }
    }
}

fn mysql_columns_to_schema(columns: &[mysql::Column]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(col.name_str().to_string(), mysql_type_to_arrow(col), true))
        .collect();
    Schema::new(fields)
}

fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    arrow_types: &[DataType],
    rows: &[mysql::Row],
) -> Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(arrow_types.len());
    for (col_idx, arrow_type) in arrow_types.iter().enumerate() {
        arrays.push(build_array(arrow_type, col_idx, rows)?);
    }
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

fn bytes_to_str(b: &[u8]) -> Option<&str> {
    std::str::from_utf8(b).ok()
}

fn build_array(
    arrow_type: &DataType,
    col_idx: usize,
    rows: &[mysql::Row],
) -> Result<Arc<dyn Array>> {
    match arrow_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v != 0),
                    Some(Value::UInt(v)) => b.append_value(*v != 0),
                    Some(Value::Bytes(bv)) => {
                        let v = bytes_to_str(bv)
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(0);
                        b.append_value(v != 0);
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v as i16),
                    Some(Value::UInt(v)) => b.append_value(*v as i16),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v as i32),
                    Some(Value::UInt(v)) => b.append_value(*v as i32),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v),
                    Some(Value::UInt(v)) => b.append_value(*v as i64),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => b.append_value(*v),
                    Some(Value::Double(v)) => b.append_value(*v as f32),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => b.append_value(*v as f64),
                    Some(Value::Double(v)) => b.append_value(*v),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) => b.append_value(String::from_utf8_lossy(bv).as_ref()),
                    Some(Value::Int(v)) => b.append_value(v.to_string()),
                    Some(Value::UInt(v)) => b.append_value(v.to_string()),
                    Some(Value::Float(v)) => b.append_value(v.to_string()),
                    Some(Value::Double(v)) => b.append_value(v.to_string()),
                    Some(Value::Date(y, m, d, h, mi, s, us)) => {
                        b.append_value(format!(
                            "{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
                        ));
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) => b.append_value(bv),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let d = match row.as_ref(col_idx) {
                    Some(Value::Date(y, m, d, _, _, _, _)) => {
                        chrono::NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                    }
                    Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                        chrono::NaiveDate::parse_from_str(
                            s.split(' ').next().unwrap_or(s),
                            "%Y-%m-%d",
                        )
                        .ok()
                    }),
                    _ => None,
                };
                match d {
                    Some(date) => {
                        let epoch =
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                        b.append_value((date - epoch).num_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let dt = match row.as_ref(col_idx) {
                    Some(Value::Date(y, mo, d, h, mi, s, us)) => chrono::NaiveDate::from_ymd_opt(
                        *y as i32, *mo as u32, *d as u32,
                    )
                    .and_then(|d| d.and_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us)),
                    Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
                    }),
                    _ => None,
                };
                match dt {
                    Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            log::warn!(
                "unhandled Arrow type {:?} for MySQL, writing nulls",
                arrow_type
            );
            let mut b = StringBuilder::with_capacity(rows.len(), 0);
            for _ in rows {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
    }
}
