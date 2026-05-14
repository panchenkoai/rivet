use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
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
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field,
};

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
        column_overrides: &ColumnOverrides,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_incremental_query(query, incremental, cursor, SourceType::Mysql);
        log::debug!("executing query: {}", built.sql);

        let mut conn = self.pool.get_conn()?;

        // Roadmap §13: normalize TIMESTAMP columns to UTC so Parquet writes
        // isAdjustedToUTC=true. SET per-connection (not global) to avoid side-effects.
        conn.query_drop("SET time_zone = '+00:00'")?;

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

        // Compute TypeMappings once; derive both the Arrow schema and the
        // per-column DataType vec from the same source so they can never diverge.
        let (schema, arrow_types) = mysql_schema_and_arrow_types(&columns, column_overrides)?;
        let schema = Arc::new(schema);

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

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<crate::types::TypeMapping>> {
        use mysql::prelude::*;
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

/// Human-readable MySQL type name for error messages and Arrow metadata.
fn mysql_native_type_name(col: &mysql::Column) -> &'static str {
    use mysql::consts::ColumnType::*;
    match col.column_type() {
        MYSQL_TYPE_TINY => "tinyint",
        MYSQL_TYPE_SHORT => "smallint",
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => "int",
        MYSQL_TYPE_LONGLONG => "bigint",
        MYSQL_TYPE_FLOAT => "float",
        MYSQL_TYPE_DOUBLE => "double",
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => "decimal",
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING => "varchar",
        MYSQL_TYPE_ENUM => "enum",
        MYSQL_TYPE_SET => "set",
        MYSQL_TYPE_JSON => "json",
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            "blob"
        }
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => "date",
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "time",
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "datetime",
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "timestamp",
        MYSQL_TYPE_BIT => "bit",
        MYSQL_TYPE_YEAR => "year",
        _ => "unknown",
    }
}

/// Map a MySQL column descriptor to Rivet's canonical type.
///
/// Key decisions vs. the old `mysql_type_to_arrow`:
/// - `DECIMAL/NEWDECIMAL` → `Unsupported` (roadmap §12: no silent float fallback;
///   requires column override or `type_policy.decimal.unbounded`).
/// - `TIMESTAMP/TIMESTAMP2` → `Timestamp { timezone: Some("UTC") }` (roadmap §13:
///   MySQL TIMESTAMP is stored as UTC and session tz must be set to +00:00).
/// - `JSON` → `RivetType::Json` so `build_arrow_field` attaches logical-type metadata.
/// - `ENUM`/`SET` → `RivetType::String` (Utf8, matching prior behavior).
/// - `TINYINT(1)` / `BOOL` / `BOOLEAN` → `RivetType::Bool` (display-width 1 = MySQL boolean convention).
/// - `TINYINT` (other widths) → `RivetType::Int16`.
/// - `BIT(1)` → `RivetType::Bool`; `BIT(n>1)` → `RivetType::Int64` (avoids silent bit-truncation).
fn mysql_type_to_rivet(col: &mysql::Column) -> RivetType {
    use mysql::consts::ColumnType::*;
    match col.column_type() {
        // BOOL / BOOLEAN in MySQL is TINYINT(1); display width == 1 is the canonical signal.
        // TINYINT(1) UNSIGNED is also treated as bool (same display-width convention).
        MYSQL_TYPE_TINY if col.column_length() == 1 => RivetType::Bool,
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT => RivetType::Int16,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => RivetType::Int32,
        MYSQL_TYPE_LONGLONG => RivetType::Int64,
        MYSQL_TYPE_FLOAT => RivetType::Float32,
        MYSQL_TYPE_DOUBLE => RivetType::Float64,

        // MySQL DECIMAL carries precision/scale but the mysql crate does not
        // expose them on `Column` — only the OID-equivalent `column_type()` is
        // available at this layer. Roadmap §12 forbids silent float conversion,
        // so we mark this Unsupported until a column override supplies p/s.
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => RivetType::Unsupported {
            native_type: "decimal".into(),
            reason: "precision/scale unavailable from MySQL column metadata; \
                     add a column override (columns: amount: decimal(18,2)) \
                     or configure type_policy.decimal.unbounded"
                .into(),
        },

        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING => {
            // Charset 63 = "binary"; `BINARY(n)` / `VARBINARY(n)` use STRING/VAR_STRING
            // metadata in the MySQL protocol, unlike `BLOB` OIDs — still binary bytes.
            if col.character_set() == 63 {
                RivetType::Binary
            } else {
                RivetType::String
            }
        }
        // M6: ENUM/SET → Utf8 + metadata logical=enum (roadmap §15).
        MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => RivetType::Enum,
        MYSQL_TYPE_JSON => RivetType::Json,

        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            // charset 63 = binary; everything else is a text blob.
            if col.character_set() == 63 {
                RivetType::Binary
            } else {
                RivetType::Text
            }
        }

        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => RivetType::Date,

        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        },

        // MySQL DATETIME has no timezone; stored as local/wall-clock time.
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: None,
        },
        // Roadmap §13: MySQL TIMESTAMP is always stored as UTC.
        // The driver must issue `SET time_zone = '+00:00'` before the query
        // (Chunk 4 / TypePolicy will enforce this; for now callers are responsible).
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        },

        // BIT(1) is a single-bit boolean; BIT(n>1) is a multi-bit integer that must not be
        // truncated to a single boolean — map to Int64 (fits BIT(1)..BIT(63) losslessly).
        // BIT(64) technically needs u64 but Int64 is the widest signed Arrow integer type;
        // values using bit 63 as data rather than sign are rare and can use a column override.
        MYSQL_TYPE_BIT if col.column_length() == 1 => RivetType::Bool,
        MYSQL_TYPE_BIT => RivetType::Int64,
        MYSQL_TYPE_YEAR => RivetType::Int16,

        _ => RivetType::Unsupported {
            native_type: mysql_native_type_name(col).to_string(),
            reason: "no Rivet mapping for this MySQL type".into(),
        },
    }
}

/// Build an Arrow `Schema` and a parallel `Vec<DataType>` from MySQL column
/// descriptors. Both are derived from the same `TypeMapping` slice so the
/// schema field type and the array type used in `build_array` are always
/// identical — mismatches would cause `RecordBatch::try_new` to panic.
///
/// `column_overrides` takes priority over autodetection.
fn mysql_schema_and_arrow_types(
    columns: &[mysql::Column],
    column_overrides: &ColumnOverrides,
) -> crate::error::Result<(Schema, Vec<DataType>)> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrow_types: Vec<DataType> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();

    for col in columns {
        let native = mysql_native_type_name(col);
        let rivet = column_overrides
            .get(col.name_str().as_ref())
            .cloned()
            .unwrap_or_else(|| mysql_type_to_rivet(col));
        let source = SourceColumn::simple(col.name_str().to_string(), native, true);
        let mapping = TypeMapping::from_source(&source, rivet);

        match (build_arrow_field(&mapping), mapping.arrow_type) {
            (Some(field), Some(dt)) => {
                fields.push(field);
                arrow_types.push(dt);
            }
            _ => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this MySQL type",
                };
                errors.push(format!(
                    "  • {} (MySQL type '{native}'): {reason}",
                    col.name_str()
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
    Ok((Schema::new(fields), arrow_types))
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
    simdutf8::basic::from_utf8(b).ok()
}

/// Interpret raw big-endian bytes from a MySQL BIT column as an unsigned integer.
/// MySQL sends BIT(n) values as ceil(n/8) big-endian bytes in the binary protocol.
fn bit_bytes_to_u64(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |acc, &byte| acc << 8 | u64::from(byte))
}

/// Parse MySQL text-protocol TIME string ("HH:MM:SS", "-HHH:MM:SS", "HH:MM:SS.uuuuuu")
/// into microseconds since midnight. Negative values are allowed.
fn parse_time_str_to_micros(s: &str) -> Option<i64> {
    let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else {
        (false, s)
    };
    let (hms, us_part) = if let Some(pos) = rest.find('.') {
        let us_str = &rest[pos + 1..];
        let us_digits = us_str.len().min(6);
        let us = us_str[..us_digits].parse::<i64>().ok()?;
        let scale = 10i64.pow((6 - us_digits) as u32);
        (&rest[..pos], us * scale)
    } else {
        (rest, 0i64)
    };
    let mut parts = hms.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    let total = (h * 3_600 + m * 60 + s) * 1_000_000 + us_part;
    Some(if neg { -total } else { total })
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
                    // BIT(1) columns arrive as raw big-endian bytes, not decimal strings.
                    Some(Value::Bytes(bv)) => b.append_value(bit_bytes_to_u64(bv) != 0),
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
                    Some(Value::Bytes(bv)) => match atoi::atoi::<i16>(bv) {
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
                    Some(Value::Bytes(bv)) => match atoi::atoi::<i32>(bv) {
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
                    Some(Value::Bytes(bv)) => {
                        // BIT(n>1) columns arrive as raw big-endian bytes; TEXT columns as UTF-8.
                        // Try decimal parse first; fall back to big-endian uint interpretation.
                        let v =
                            atoi::atoi::<i64>(bv).unwrap_or_else(|| bit_bytes_to_u64(bv) as i64);
                        b.append_value(v);
                    }
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
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    // MySQL wire protocol delivers TIME as Value::Time(neg, days, h, m, s, us)
                    Some(Value::Time(neg, days, h, m, s, us)) => {
                        let total_us = (*days as i64 * 86_400
                            + *h as i64 * 3_600
                            + *m as i64 * 60
                            + *s as i64)
                            * 1_000_000
                            + *us as i64;
                        b.append_value(if *neg { -total_us } else { total_us });
                    }
                    Some(Value::Bytes(bv)) => {
                        // text-protocol fallback: "HH:MM:SS" or "HHH:MM:SS.uuuuuu"
                        if let Some(us) = bytes_to_str(bv).and_then(parse_time_str_to_micros) {
                            b.append_value(us);
                        } else {
                            b.append_null();
                        }
                    }
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
        // Both DATETIME (tz=None) and TIMESTAMP (tz=Some("UTC")) share the
        // same physical i64 microsecond values. The timezone tag on the array
        // type is what distinguishes them in the Arrow / Parquet schema.
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let tz_tag = tz.clone();
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
            let arr = b.finish();
            // Roadmap §13: attach the UTC timezone tag so Parquet writes
            // TIMESTAMP_MICROS(isAdjustedToUTC=true) for TIMESTAMP columns.
            match tz_tag {
                Some(tz_str) => Ok(Arc::new(arr.with_timezone(tz_str.as_ref()))),
                None => Ok(Arc::new(arr)),
            }
        }
        // Exact decimal path: column override declared decimal(p,s) for a MySQL DECIMAL column.
        DataType::Decimal128(p, s) => mysql_decimal_to_decimal128(*p, *s, col_idx, rows),
        DataType::Decimal256(p, s) => mysql_decimal_to_decimal256(*p, *s, col_idx, rows),
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

/// Build a `Decimal128Array` from MySQL DECIMAL column bytes (text protocol).
fn mysql_decimal_to_decimal128(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[mysql::Row],
) -> Result<Arc<dyn Array>> {
    use crate::types::decimal::decimal_str_to_scaled_i128;
    let mut b = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(col_idx) {
            Some(Value::Bytes(bv)) => {
                let s = bytes_to_str(bv).unwrap_or("");
                match decimal_str_to_scaled_i128(s, scale) {
                    Some(v) => b.append_value(v),
                    None => {
                        return Err(anyhow::anyhow!(
                            "cannot parse '{}' as decimal({},{})",
                            s,
                            precision,
                            scale
                        ));
                    }
                }
            }
            _ => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

/// Build a `Decimal256Array` for precision > 38.
fn mysql_decimal_to_decimal256(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[mysql::Row],
) -> Result<Arc<dyn Array>> {
    use crate::types::decimal::decimal_str_to_scaled_i128;
    use arrow::datatypes::i256;
    let mut b = Decimal256Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(col_idx) {
            Some(Value::Bytes(bv)) => {
                let s = bytes_to_str(bv).unwrap_or("");
                match decimal_str_to_scaled_i128(s, scale) {
                    Some(v) => b.append_value(i256::from_i128(v)),
                    None => {
                        return Err(anyhow::anyhow!(
                            "cannot parse '{}' as decimal({},{})",
                            s,
                            precision,
                            scale
                        ));
                    }
                }
            }
            _ => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::bit_bytes_to_u64;

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
}
