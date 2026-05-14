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
        let mut buf = Vec::with_capacity(batch.num_rows() * batch.num_columns() * 8);
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                if col_idx > 0 {
                    buf.push(b',');
                }
                write_csv_value(&mut buf, batch.column(col_idx), row_idx)?;
            }
            buf.push(b'\n');
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
                writer.write_all(b"\"")?;
                let mut rest = val;
                while let Some(pos) = rest.find('"') {
                    writer.write_all(&rest.as_bytes()[..pos])?;
                    writer.write_all(b"\"\"")?;
                    rest = &rest[pos + 1..];
                }
                writer.write_all(rest.as_bytes())?;
                writer.write_all(b"\"")?;
            } else {
                writer.write_all(val.as_bytes())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    // Helper: render one cell to a String using write_csv_value.
    fn cell<A: Array + 'static>(array: A, idx: usize) -> String {
        let mut buf = Vec::new();
        write_csv_value(&mut buf, &array, idx).unwrap();
        String::from_utf8(buf).unwrap()
    }

    // Helper: render a null cell from any typed array.
    fn null_cell(dt: DataType) -> String {
        use arrow::array::new_null_array;
        let arr = new_null_array(&dt, 1);
        let mut buf = Vec::new();
        write_csv_value(&mut buf, arr.as_ref(), 0).unwrap();
        String::from_utf8(buf).unwrap()
    }

    // ── null handling ────────────────────────────────────────────────────────

    #[test]
    fn null_value_writes_empty_string() {
        assert_eq!(null_cell(DataType::Int64), "");
        assert_eq!(null_cell(DataType::Utf8), "");
        assert_eq!(null_cell(DataType::Boolean), "");
    }

    // ── scalars ─────────────────────────────────────────────────────────────

    #[test]
    fn bool_true_writes_true() {
        assert_eq!(cell(BooleanArray::from(vec![true]), 0), "true");
    }

    #[test]
    fn bool_false_writes_false() {
        assert_eq!(cell(BooleanArray::from(vec![false]), 0), "false");
    }

    #[test]
    fn int16_value() {
        assert_eq!(cell(Int16Array::from(vec![42i16]), 0), "42");
    }

    #[test]
    fn int32_negative() {
        assert_eq!(cell(Int32Array::from(vec![-7i32]), 0), "-7");
    }

    #[test]
    fn int64_large() {
        assert_eq!(
            cell(Int64Array::from(vec![9_999_999_999i64]), 0),
            "9999999999"
        );
    }

    #[test]
    fn float32_value() {
        let result = cell(Float32Array::from(vec![1.5f32]), 0);
        assert!(result.starts_with("1.5"), "got: {result}");
    }

    #[test]
    fn float64_value() {
        let result = cell(Float64Array::from(vec![std::f64::consts::PI]), 0);
        assert!(result.starts_with("3.14"), "got: {result}");
    }

    // ── string escaping ──────────────────────────────────────────────────────

    #[test]
    fn plain_string_no_quoting() {
        assert_eq!(cell(StringArray::from(vec!["hello"]), 0), "hello");
    }

    #[test]
    fn string_with_comma_is_quoted() {
        assert_eq!(cell(StringArray::from(vec!["a,b"]), 0), "\"a,b\"");
    }

    #[test]
    fn string_with_double_quote_is_escaped() {
        // say "hi" → opening " + say  + "" + hi + "" + closing " = "say ""hi"""
        let result = cell(StringArray::from(vec![r#"say "hi""#]), 0);
        assert_eq!(result, r#""say ""hi""""#);
    }

    #[test]
    fn string_with_newline_is_quoted() {
        let result = cell(StringArray::from(vec!["line1\nline2"]), 0);
        assert!(
            result.starts_with('"') && result.ends_with('"'),
            "got: {result}"
        );
        assert!(result.contains("line1\nline2"), "got: {result}");
    }

    // ── binary ───────────────────────────────────────────────────────────────

    #[test]
    fn binary_is_written_as_hex() {
        let arr = BinaryArray::from_vec(vec![&[0xDE, 0xAD, 0xBE, 0xEF][..]]);
        assert_eq!(cell(arr, 0), "deadbeef");
    }

    #[test]
    fn binary_empty_writes_empty() {
        let arr = BinaryArray::from_vec(vec![&[][..]]);
        assert_eq!(cell(arr, 0), "");
    }

    // ── Date32 ───────────────────────────────────────────────────────────────

    #[test]
    fn date32_epoch_is_1970_01_01() {
        assert_eq!(cell(Date32Array::from(vec![0i32]), 0), "1970-01-01");
    }

    #[test]
    fn date32_positive_offset() {
        // 365 days after epoch = 1971-01-01
        assert_eq!(cell(Date32Array::from(vec![365i32]), 0), "1971-01-01");
    }

    // ── Timestamp(Microsecond) ───────────────────────────────────────────────

    #[test]
    fn timestamp_micros_formats_as_iso() {
        // 2023-01-01T00:00:00.000000 = 1672531200_000000 micros since epoch
        let micros: i64 = 1_672_531_200 * 1_000_000;
        let _schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let arr = TimestampMicrosecondArray::from(vec![micros]);
        let result = cell(arr, 0);
        assert!(result.starts_with("2023-01-01T"), "got: {result}");
        assert!(result.contains("00:00:00"), "got: {result}");
    }

    // ── write_batch via CsvFormat ────────────────────────────────────────────

    #[test]
    fn csv_format_write_batch_tracks_bytes_and_succeeds() {
        use crate::format::Format;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), None])),
            ],
        )
        .unwrap();

        // Pass Vec by value — avoids the &mut T 'static lifetime requirement.
        let fmt = CsvFormat;
        let mut writer = fmt
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        writer.write_batch(&batch).unwrap();
        // Header "id,name\n" + rows "1,alice\n" + "2,\n" = at least 18 bytes
        assert!(
            writer.bytes_written() > 10,
            "expected >10 bytes, got {}",
            writer.bytes_written()
        );
        writer.finish().unwrap();
    }
}
