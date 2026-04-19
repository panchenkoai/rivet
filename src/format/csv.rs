use std::io::Write;

use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::error::Result;

pub struct CsvFormat;

pub struct CsvFormatWriter {
    writer: Box<dyn Write + Send>,
    bytes_written: u64,
}

impl super::Format for CsvFormat {
    fn create_writer(
        &self,
        schema: &SchemaRef,
        mut writer: Box<dyn Write + Send>,
    ) -> Result<Box<dyn super::FormatWriter>> {
        let header = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(",");
        let header_bytes = header.len() as u64 + 1; // +1 for newline
        writeln!(writer, "{}", header)?;
        Ok(Box::new(CsvFormatWriter {
            writer,
            bytes_written: header_bytes,
        }))
    }

    fn file_extension(&self) -> &str {
        "csv"
    }
}

impl super::FormatWriter for CsvFormatWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let before = self.bytes_written;
        let _ = before; // suppress unused warning, we count after

        let mut buf = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let mut first = true;
            for col_idx in 0..batch.num_columns() {
                if !first {
                    write!(buf, ",")?;
                }
                first = false;
                write_csv_value(&mut buf, batch.column(col_idx), row_idx)?;
            }
            writeln!(buf)?;
        }
        self.bytes_written += buf.len() as u64;
        self.writer.write_all(&buf)?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

fn write_csv_value(writer: &mut dyn Write, array: &dyn Array, idx: usize) -> Result<()> {
    if array.is_null(idx) {
        return Ok(());
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("DataType/Array mismatch");
            let val = arr.value(idx);
            if val.contains(',') || val.contains('"') || val.contains('\n') {
                write!(writer, "\"{}\"", val.replace('"', "\"\""))?;
            } else {
                write!(writer, "{}", val)?;
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("DataType/Array mismatch");
            let val = arr.value(idx);
            for byte in val {
                write!(writer, "{:02x}", byte)?;
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("DataType/Array mismatch");
            let days = arr.value(idx);
            // `Date32` is "days since 1970-01-01"; a pathological value near
            // i32::MAX overflows `NaiveDate + Duration` and panics in chrono.
            // Fall back to checked arithmetic and emit an empty cell on
            // overflow — matches the null-cell convention for unserialisable
            // values elsewhere in this writer.
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
            let date =
                chrono::Duration::try_days(days as i64).and_then(|d| epoch.checked_add_signed(d));
            if let Some(date) = date {
                write!(writer, "{}", date)?;
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("DataType/Array mismatch");
            let micros = arr.value(idx);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                write!(writer, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f"))?;
            }
        }
        other => {
            log::warn!("CSV: unhandled Arrow type {:?}, skipping value", other);
        }
    }

    Ok(())
}
