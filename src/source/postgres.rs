use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use postgres::types::Type;
use postgres::{Client, NoTls, Row};

use crate::error::Result;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

pub struct PostgresSource {
    client: Client,
    schema: Option<SchemaRef>,
    columns_cache: Option<Vec<(String, Type)>>,
    fetch_sql: Option<String>,
    throttle_ms: u64,
    batch_size: usize,
    done: bool,
    total_rows: usize,
    pending_batch: Option<RecordBatch>,
}

impl PostgresSource {
    pub fn connect(url: &str) -> Result<Self> {
        let client = Client::connect(url, NoTls)?;
        Ok(Self {
            client,
            schema: None,
            columns_cache: None,
            fetch_sql: None,
            throttle_ms: 0,
            batch_size: 0,
            done: true,
            total_rows: 0,
            pending_batch: None,
        })
    }

    fn fetch_rows_and_convert(&mut self) -> Result<Option<RecordBatch>> {
        let fetch_sql = self.fetch_sql.as_ref().unwrap();
        let rows = self.client.query(fetch_sql, &[])?;

        if rows.is_empty() {
            self.done = true;
            return Ok(None);
        }

        if self.schema.is_none() {
            let stmt_cols: Vec<(String, Type)> = rows[0]
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_().clone()))
                .collect();
            let s = Arc::new(pg_columns_to_schema(rows[0].columns()));
            self.schema = Some(s);
            self.columns_cache = Some(stmt_cols);
        }

        let row_count = rows.len();
        self.total_rows += row_count;

        let schema = self.schema.as_ref().unwrap();
        let cols = self.columns_cache.as_ref().unwrap();
        let batch = rows_to_record_batch_typed(schema, cols, &rows)?;

        if row_count < self.batch_size {
            self.done = true;
        }

        Ok(Some(batch))
    }
}

impl super::Source for PostgresSource {
    fn begin_query(
        &mut self,
        query: &str,
        cursor_column: Option<&str>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
    ) -> Result<SchemaRef> {
        let effective_query = build_query(query, cursor_column, cursor);
        log::info!("executing query: {}", effective_query);

        if tuning.statement_timeout_s > 0 {
            self.client.batch_execute(&format!(
                "SET statement_timeout = '{}s'",
                tuning.statement_timeout_s
            ))?;
        }
        if tuning.lock_timeout_s > 0 {
            self.client.batch_execute(&format!(
                "SET lock_timeout = '{}s'",
                tuning.lock_timeout_s
            ))?;
        }

        self.client.batch_execute("BEGIN")?;
        self.client.batch_execute(&format!(
            "DECLARE _rivet NO SCROLL CURSOR FOR {}",
            effective_query
        ))?;

        self.fetch_sql = Some(format!("FETCH {} FROM _rivet", tuning.batch_size));
        self.throttle_ms = tuning.throttle_ms;
        self.batch_size = tuning.batch_size;
        self.done = false;
        self.total_rows = 0;
        self.schema = None;
        self.columns_cache = None;
        self.pending_batch = None;

        // First fetch to discover schema
        let first_batch = self.fetch_rows_and_convert()?;
        let schema = self.schema.clone().unwrap_or_else(|| Arc::new(Schema::empty()));
        self.pending_batch = first_batch;

        Ok(schema)
    }

    fn fetch_next(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.pending_batch.take() {
            log::info!("fetched {} rows so far...", self.total_rows);
            return Ok(Some(batch));
        }

        if self.done {
            return Ok(None);
        }

        if self.throttle_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.throttle_ms));
        }

        let batch = self.fetch_rows_and_convert()?;
        if batch.is_some() {
            log::info!("fetched {} rows so far...", self.total_rows);
        } else {
            log::info!("total: {} rows", self.total_rows);
        }
        Ok(batch)
    }

    fn close_query(&mut self) -> Result<()> {
        if self.fetch_sql.is_some() {
            self.client.batch_execute("CLOSE _rivet")?;
            self.client.batch_execute("COMMIT")?;
            self.client.batch_execute("RESET statement_timeout")?;
            self.client.batch_execute("RESET lock_timeout")?;
            self.fetch_sql = None;
        }
        self.done = true;
        Ok(())
    }
}

pub(crate) fn build_query(
    base_query: &str,
    cursor_column: Option<&str>,
    cursor: Option<&CursorState>,
) -> String {
    let has_cursor_value = cursor
        .and_then(|c| c.last_cursor_value.as_deref())
        .is_some();

    if let (Some(col), true) = (cursor_column, has_cursor_value) {
        let cursor_val = cursor.unwrap().last_cursor_value.as_deref().unwrap();
        format!(
            "SELECT * FROM ({base}) AS _rivet WHERE {col} > '{val}' ORDER BY {col}",
            base = base_query,
            col = col,
            val = cursor_val,
        )
    } else if let Some(col) = cursor_column {
        format!(
            "SELECT * FROM ({base}) AS _rivet ORDER BY {col}",
            base = base_query,
            col = col,
        )
    } else {
        base_query.to_string()
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
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
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
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<bool> = row.get(col_idx);
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::INT2 => {
            let mut builder = Int16Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i16> = row.get(col_idx);
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::INT4 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i32> = row.get(col_idx);
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::INT8 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i64> = row.get(col_idx);
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::FLOAT4 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<f32> = row.get(col_idx);
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::FLOAT8 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<f64> = row.get(col_idx);
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.get(col_idx);
                builder.append_option(val.as_deref());
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::BYTEA => {
            let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                let val: Option<Vec<u8>> = row.get(col_idx);
                match val {
                    Some(v) => builder.append_value(&v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::DATE => {
            let mut builder = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<chrono::NaiveDate> = row.get(col_idx);
                match val {
                    Some(d) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let days = (d - epoch).num_days() as i32;
                        builder.append_value(days);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::TIMESTAMP => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<chrono::NaiveDateTime> = row.get(col_idx);
                match val {
                    Some(ts) => {
                        let micros = ts.and_utc().timestamp_micros();
                        builder.append_value(micros);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::TIMESTAMPTZ => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<chrono::DateTime<chrono::Utc>> = row.get(col_idx);
                match val {
                    Some(ts) => {
                        builder.append_value(ts.timestamp_micros());
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::NUMERIC | Type::JSON | Type::JSONB | Type::UUID => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val.as_deref());
            }
            Ok(Arc::new(builder.finish()))
        }
        Type::OID => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<u32> = row.get(col_idx);
                builder.append_option(val.map(|v| v as i64));
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            log::warn!("unmapped PG type {:?}, extracting as text", pg_type);
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val.as_deref());
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CursorState;

    #[test]
    fn test_build_query_full() {
        let q = build_query("SELECT * FROM users", None, None);
        assert_eq!(q, "SELECT * FROM users");
    }

    #[test]
    fn test_build_query_incremental_first_run() {
        let cursor = CursorState {
            export_name: "test".to_string(),
            last_cursor_value: None,
            last_run_at: None,
        };
        let q = build_query("SELECT * FROM users", Some("updated_at"), Some(&cursor));
        assert!(q.contains("ORDER BY updated_at"));
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn test_build_query_incremental_with_cursor() {
        let cursor = CursorState {
            export_name: "test".to_string(),
            last_cursor_value: Some("2024-01-01T00:00:00".to_string()),
            last_run_at: Some("2024-06-01".to_string()),
        };
        let q = build_query("SELECT * FROM orders", Some("updated_at"), Some(&cursor));
        assert!(q.contains("WHERE updated_at > '2024-01-01T00:00:00'"), "got: {}", q);
        assert!(q.contains("ORDER BY updated_at"));
    }
}
