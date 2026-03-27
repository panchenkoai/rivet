use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use mysql::prelude::*;
use mysql::{Opts, Pool, Value};

use crate::error::Result;
use crate::tuning::SourceTuning;
use crate::types::CursorState;

pub struct MysqlSource {
    pool: Pool,
    // query state
    schema: Option<SchemaRef>,
    arrow_types: Vec<DataType>,
    rows: Vec<mysql::Row>,
    cursor: usize,
    batch_size: usize,
    throttle_ms: u64,
    total_rows: usize,
}

impl MysqlSource {
    pub fn connect(url: &str) -> Result<Self> {
        let opts = Opts::from_url(url)?;
        let pool = Pool::new(opts)?;
        Ok(Self {
            pool,
            schema: None,
            arrow_types: Vec::new(),
            rows: Vec::new(),
            cursor: 0,
            batch_size: 0,
            throttle_ms: 0,
            total_rows: 0,
        })
    }
}

impl super::Source for MysqlSource {
    fn begin_query(
        &mut self,
        query: &str,
        cursor_column: Option<&str>,
        cursor: Option<&CursorState>,
        tuning: &SourceTuning,
    ) -> Result<SchemaRef> {
        let effective_query = build_query(query, cursor_column, cursor);
        log::info!("executing query: {}", effective_query);

        let mut conn = self.pool.get_conn()?;

        if tuning.statement_timeout_s > 0 {
            conn.query_drop(format!(
                "SET SESSION max_execution_time = {}",
                tuning.statement_timeout_s * 1000
            ))?;
        }

        let mut result = conn.query_iter(&effective_query)?;
        let columns = result.columns().as_ref().to_vec();
        let schema = Arc::new(mysql_columns_to_schema(&columns));
        let arrow_types: Vec<DataType> = columns.iter().map(|c| mysql_type_to_arrow(c)).collect();

        let mut rows = Vec::new();
        let row_set = result.iter().ok_or_else(|| anyhow::anyhow!("no result set"))?;
        for row_result in row_set {
            rows.push(row_result?);
        }
        drop(result);

        log::info!("loaded {} rows into memory", rows.len());

        if tuning.statement_timeout_s > 0 {
            conn.query_drop("SET SESSION max_execution_time = 0")?;
        }

        self.schema = Some(schema.clone());
        self.arrow_types = arrow_types;
        self.rows = rows;
        self.cursor = 0;
        self.batch_size = tuning.batch_size;
        self.throttle_ms = tuning.throttle_ms;
        self.total_rows = 0;

        Ok(schema)
    }

    fn fetch_next(&mut self) -> Result<Option<RecordBatch>> {
        if self.cursor >= self.rows.len() {
            return Ok(None);
        }

        if self.total_rows > 0 && self.throttle_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.throttle_ms));
        }

        let end = (self.cursor + self.batch_size).min(self.rows.len());
        let chunk = &self.rows[self.cursor..end];
        let schema = self.schema.as_ref().unwrap();
        let batch = rows_to_record_batch_typed(schema, &self.arrow_types, chunk)?;

        self.cursor = end;
        self.total_rows += chunk.len();
        log::info!("fetched {} rows so far...", self.total_rows);

        Ok(Some(batch))
    }

    fn close_query(&mut self) -> Result<()> {
        log::info!("total: {} rows", self.total_rows);
        self.rows.clear();
        self.schema = None;
        self.arrow_types.clear();
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

fn mysql_type_to_arrow(col: &mysql::Column) -> DataType {
    use mysql::consts::ColumnType::*;
    match col.column_type() {
        MYSQL_TYPE_TINY => DataType::Int16,
        MYSQL_TYPE_SHORT => DataType::Int16,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => DataType::Int32,
        MYSQL_TYPE_LONGLONG => DataType::Int64,
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE => DataType::Float64,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => DataType::Utf8,
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING | MYSQL_TYPE_ENUM
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
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIMESTAMP
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
        .map(|col| {
            let dt = mysql_type_to_arrow(col);
            Field::new(col.name_str().to_string(), dt, true)
        })
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
        let array = build_array(arrow_type, col_idx, rows)?;
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
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
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => builder.append_value(*v != 0),
                    Some(Value::UInt(v)) => builder.append_value(*v != 0),
                    Some(Value::Bytes(b)) => {
                        let v = bytes_to_str(b).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                        builder.append_value(v != 0);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => builder.append_value(*v as i16),
                    Some(Value::UInt(v)) => builder.append_value(*v as i16),
                    Some(Value::Bytes(b)) => match bytes_to_str(b).and_then(|s| s.parse().ok()) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => builder.append_value(*v as i32),
                    Some(Value::UInt(v)) => builder.append_value(*v as i32),
                    Some(Value::Bytes(b)) => match bytes_to_str(b).and_then(|s| s.parse().ok()) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => builder.append_value(*v),
                    Some(Value::UInt(v)) => builder.append_value(*v as i64),
                    Some(Value::Bytes(b)) => match bytes_to_str(b).and_then(|s| s.parse().ok()) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => builder.append_value(*v),
                    Some(Value::Double(v)) => builder.append_value(*v as f32),
                    Some(Value::Bytes(b)) => match bytes_to_str(b).and_then(|s| s.parse().ok()) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => builder.append_value(*v as f64),
                    Some(Value::Double(v)) => builder.append_value(*v),
                    Some(Value::Bytes(b)) => match bytes_to_str(b).and_then(|s| s.parse().ok()) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(b)) => {
                        let s = String::from_utf8_lossy(b);
                        builder.append_value(&*s);
                    }
                    Some(Value::Int(v)) => builder.append_value(v.to_string()),
                    Some(Value::UInt(v)) => builder.append_value(v.to_string()),
                    Some(Value::Float(v)) => builder.append_value(v.to_string()),
                    Some(Value::Double(v)) => builder.append_value(v.to_string()),
                    Some(Value::Date(y, m, d, h, mi, s, us)) => {
                        builder.append_value(format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"));
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(b)) => builder.append_value(b),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let date_opt = match row.as_ref(col_idx) {
                    Some(Value::Date(year, month, day, _, _, _, _)) => {
                        chrono::NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                    }
                    Some(Value::Bytes(b)) => bytes_to_str(b)
                        .and_then(|s| chrono::NaiveDate::parse_from_str(s.split(' ').next().unwrap_or(s), "%Y-%m-%d").ok()),
                    _ => None,
                };
                match date_opt {
                    Some(date) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        builder.append_value((date - epoch).num_days() as i32);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let dt_opt = match row.as_ref(col_idx) {
                    Some(Value::Date(year, month, day, hour, min, sec, micro)) => {
                        chrono::NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                            .and_then(|d| d.and_hms_micro_opt(*hour as u32, *min as u32, *sec as u32, *micro))
                    }
                    Some(Value::Bytes(b)) => bytes_to_str(b)
                        .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()),
                    _ => None,
                };
                match dt_opt {
                    Some(dt) => builder.append_value(dt.and_utc().timestamp_micros()),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => {
            log::warn!("unhandled Arrow type {:?} for MySQL, writing nulls", other);
            let mut builder = StringBuilder::with_capacity(rows.len(), 0);
            for _ in rows {
                builder.append_null();
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
        let q = build_query("SELECT * FROM users", Some("id"), Some(&cursor));
        assert!(q.contains("ORDER BY id"));
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn test_build_query_incremental_with_cursor() {
        let cursor = CursorState {
            export_name: "test".to_string(),
            last_cursor_value: Some("42".to_string()),
            last_run_at: Some("2024-06-01".to_string()),
        };
        let q = build_query("SELECT * FROM events", Some("id"), Some(&cursor));
        assert!(q.contains("WHERE id > '42'"), "got: {}", q);
        assert!(q.contains("ORDER BY id"));
    }
}
