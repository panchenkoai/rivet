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
}

impl PostgresSource {
    pub fn connect(url: &str) -> Result<Self> {
        let client = Client::connect(url, NoTls)?;
        Ok(Self { client })
    }
}

impl super::Source for PostgresSource {
    fn export(
        &mut self,
        query: &str,
        cursor_column: Option<&str>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let effective_query = build_query(query, cursor_column, cursor);
        log::info!("executing query: {}", effective_query);

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
            effective_query
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
        if let Ok(Some(v)) = row.try_get::<_, Option<String>>(0) {
            return Ok(Some(v));
        }
        Ok(None)
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
        let cursor_val = cursor
            .expect("cursor checked above")
            .last_cursor_value
            .as_deref()
            .expect("cursor value checked above");
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
            export_name: "t".into(),
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
            export_name: "t".into(),
            last_cursor_value: Some("2024-01-01T00:00:00".into()),
            last_run_at: Some("2024-06-01".into()),
        };
        let q = build_query("SELECT * FROM orders", Some("updated_at"), Some(&cursor));
        assert!(
            q.contains("WHERE updated_at > '2024-01-01T00:00:00'"),
            "got: {}",
            q
        );
        assert!(q.contains("ORDER BY updated_at"));
    }
}
