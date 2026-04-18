use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use postgres::types::Type;
use postgres::{Client, NoTls, Row};

use crate::config::{SourceType, TlsConfig};
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::source::query::build_incremental_query;
use crate::source::tls::build_native_tls;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

pub struct PostgresSource {
    client: Client,
}

impl PostgresSource {
    /// Connect with no transport security (legacy path). Prefer [`Self::connect_with_tls`]
    /// for production workloads so credentials and result sets are not visible on the wire.
    pub fn connect(url: &str) -> Result<Self> {
        let client = Client::connect(url, NoTls)?;
        Ok(Self { client })
    }

    /// Connect honoring the user's [`TlsConfig`]. When `tls.mode` is
    /// [`TlsMode::Disable`] this falls back to [`Self::connect`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let connector = build_native_tls(cfg)?;
                let make_tls = postgres_native_tls::MakeTlsConnector::new(connector);
                let client = Client::connect(url, make_tls)?;
                Ok(Self { client })
            }
            _ => Self::connect(url),
        }
    }
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

impl super::Source for PostgresSource {
    fn export(
        &mut self,
        query: &str,
        incremental: Option<&IncrementalCursorPlan>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_incremental_query(query, incremental, cursor, SourceType::Postgres);
        debug_assert!(
            built.cursor_param.is_none(),
            "Postgres path inlines cursor values as E'…' literals — binding is unused"
        );
        log::debug!("executing query: {}", built.sql);

        if tuning.statement_timeout_s > 0 {
            self.client.batch_execute(&format!(
                "SET statement_timeout = '{}s'",
                tuning.statement_timeout_s
            ))?;
        }
        if tuning.lock_timeout_s > 0 {
            self.client
                .batch_execute(&format!("SET lock_timeout = '{}s'", tuning.lock_timeout_s))?;
        }

        self.client.batch_execute("BEGIN")?;
        self.client.batch_execute(&format!(
            "DECLARE _rivet NO SCROLL CURSOR FOR {}",
            built.sql
        ))?;

        let mut fetch_size = tuning.batch_size;
        let mut fetch_sql = format!("FETCH {} FROM _rivet", fetch_size);
        let mut schema: Option<SchemaRef> = None;
        let mut columns_cache: Option<Vec<(String, Type)>> = None;
        let mut total_rows: usize = 0;

        loop {
            let rows = self.client.query(&fetch_sql, &[])?;
            if rows.is_empty() {
                break;
            }

            if schema.is_none() {
                let stmt_cols: Vec<(String, Type)> = rows[0]
                    .columns()
                    .iter()
                    .map(|c| (c.name().to_string(), c.type_().clone()))
                    .collect();
                let s = Arc::new(pg_columns_to_schema(rows[0].columns()));
                sink.on_schema(s.clone())?;
                schema = Some(s.clone());
                columns_cache = Some(stmt_cols);

                let effective = tuning.effective_batch_size(Some(&s));
                if effective != fetch_size {
                    fetch_size = effective;
                    fetch_sql = format!("FETCH {} FROM _rivet", fetch_size);
                }
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

            log::info!("fetched {} rows so far...", total_rows);

            if row_count < fetch_size {
                break;
            }

            if tuning.throttle_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(tuning.throttle_ms));
            }
        }

        self.client.batch_execute("CLOSE _rivet")?;
        self.client.batch_execute("COMMIT")?;
        self.client.batch_execute("RESET statement_timeout")?;
        self.client.batch_execute("RESET lock_timeout")?;

        if schema.is_none() {
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
}

fn pg_type_to_arrow(pg_type: &Type) -> DataType {
    match *pg_type {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::NUMERIC => DataType::Utf8,
        Type::JSON | Type::JSONB => DataType::Utf8,
        Type::UUID => DataType::Utf8,
        Type::OID => DataType::Int64,
        _ => {
            log::warn!("unmapped PG type {:?}, falling back to Utf8", pg_type);
            DataType::Utf8
        }
    }
}

fn pg_columns_to_schema(columns: &[postgres::Column]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = pg_type_to_arrow(col.type_());
            Field::new(col.name(), dt, true)
        })
        .collect();
    Schema::new(fields)
}

fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    columns: &[(String, Type)],
    rows: &[Row],
) -> Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());
    for (col_idx, (_name, pg_type)) in columns.iter().enumerate() {
        let array = build_array(pg_type, col_idx, rows)?;
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
}

fn build_array(pg_type: &Type, col_idx: usize, rows: &[Row]) -> Result<Arc<dyn Array>> {
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
        Type::INT8 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
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
        Type::TIMESTAMPTZ => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(col_idx) {
                    Some(ts) => b.append_value(ts.timestamp_micros()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::NUMERIC | Type::JSON | Type::JSONB | Type::UUID => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.try_get(col_idx).ok().flatten();
                b.append_option(val.as_deref());
            }
            Ok(Arc::new(b.finish()))
        }
        Type::OID => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get::<_, Option<u32>>(col_idx).map(|v| v as i64));
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
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
