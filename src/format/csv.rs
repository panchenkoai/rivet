use std::io::Write;

use arrow::array::Time64MicrosecondArray;
use arrow::array::types::Decimal128Type;
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::types::decimal::scaled_i128_to_decimal_str;

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
    ) -> Result<Box<dyn super::FormatWriter + Send>> {
        // Fail loud: arrays and other nested/wide Arrow types have no CSV cell
        // representation. Reject them up front, naming the column, instead of
        // silently writing empty values for every row — `format: parquet` or
        // excluding the column from the query is the fix.
        if let Some(field) = schema
            .fields()
            .iter()
            .find(|f| !csv_serializable(f.data_type()))
        {
            anyhow::bail!(
                "CSV cannot serialize column '{}' (Arrow type {:?}); use `format: parquet` \
                 or drop the column from the query",
                field.name(),
                field.data_type()
            );
        }
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

/// Arrow types `write_csv_value` can serialize. Everything else — lists,
/// structs, maps, `Decimal256`, non-UUID fixed binary, … — has no CSV cell
/// representation and is rejected at writer creation rather than silently
/// emitted as an empty value.
pub(crate) fn csv_serializable(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(16)
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
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
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("DataType/Array mismatch");
            write!(writer, "{}", arr.value(idx))?;
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_primitive::<Decimal128Type>();
            let text = scaled_i128_to_decimal_str(arr.value(idx), *scale);
            writer.write_all(text.as_bytes())?;
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
        // FixedSizeBinary today only carries 16-byte UUIDs (see
        // `RivetType::Uuid` → `DataType::FixedSizeBinary(16)` in
        // `src/types/mapping.rs`). CSV has no native binary cell; emit the
        // canonical hyphenated lowercase form so downstream readers can
        // recognise it as a UUID rather than 16 bytes of mojibake. Any
        // future FixedSizeBinary use that is not a UUID should branch on
        // the size argument before reaching this arm.
        DataType::FixedSizeBinary(16) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("DataType/Array mismatch");
            let val = arr.value(idx);
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(val);
            write!(writer, "{}", uuid::Uuid::from_bytes(bytes).to_hyphenated())?;
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
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("DataType/Array mismatch");
            let micros = arr.value(idx);
            let secs = micros / 1_000_000;
            let frac_us = micros % 1_000_000;
            write!(
                writer,
                "{:02}:{:02}:{:02}.{:06}",
                secs / 3600,
                (secs % 3600) / 60,
                secs % 60,
                frac_us
            )?;
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
            // Defensive: `create_writer` rejects unsupported types up front, so
            // this should be unreachable. Bail rather than silently skip if a
            // new type slips through.
            anyhow::bail!(
                "CSV: no serializer for Arrow type {other:?} (column should have been rejected at writer creation)"
            );
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
    fn decimal128_writes_exact_text() {
        let arr = Decimal128Array::from(vec![10i128])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(cell(arr, 0), "0.10");
        let scaled =
            crate::types::decimal::decimal_str_to_scaled_i128("999999999999.99", 2).unwrap();
        let arr = Decimal128Array::from(vec![scaled])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(cell(arr, 0), "999999999999.99");
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

    // Characterization: float NaN/±Infinity are IEEE-754 values a float column
    // can legitimately hold (unlike `decimal`, whose Arrow `Decimal128` has no
    // NaN bit pattern — see the NUMERIC NaN/infinity reject in
    // `postgres::arrow_convert::build_array`). They are preserved natively in
    // Parquet; in CSV we emit the Rust float literal (`NaN` / `inf` / `-inf`)
    // rather than an empty cell, because writing empty would silently conflate
    // a real NaN/Inf with NULL — corruption — whereas a recognizable literal
    // round-trips into every major loader's float parser. This pins that
    // contract so a future arrow/std change can't silently alter it. The CSV
    // literal is documented in docs/type-mapping.md.
    #[test]
    fn float_special_values_emit_literals_not_empty() {
        assert_eq!(cell(Float64Array::from(vec![f64::NAN]), 0), "NaN");
        assert_eq!(cell(Float64Array::from(vec![f64::INFINITY]), 0), "inf");
        assert_eq!(cell(Float64Array::from(vec![f64::NEG_INFINITY]), 0), "-inf");
        assert_eq!(cell(Float32Array::from(vec![f32::NAN]), 0), "NaN");
        assert_eq!(cell(Float32Array::from(vec![f32::INFINITY]), 0), "inf");
        // -0.0 keeps its sign (a real IEEE-754 distinction), never becomes "0".
        assert_eq!(cell(Float64Array::from(vec![-0.0f64]), 0), "-0");
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

    // ── fail loud on types CSV can't represent ───────────────────────────────

    #[test]
    fn csv_rejects_array_columns_loudly() {
        use crate::format::Format;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let Err(err) = CsvFormat.create_writer(&schema, Box::new(Vec::<u8>::new())) else {
            panic!("CSV must reject array columns, not silently drop them");
        };
        let msg = format!("{err:#}");
        assert!(msg.contains("tags"), "error must name the column: {msg}");
        assert!(msg.to_lowercase().contains("csv"), "{msg}");
    }

    /// Consistency guard: every type `csv_serializable` admits must actually be
    /// handled by `write_csv_value` (not hit its `other => bail` fallthrough).
    /// Keeps the whitelist and the writer in lock-step so one can't drift.
    #[test]
    fn every_serializable_type_is_actually_written() {
        use crate::format::Format;
        let cols: Vec<(&str, ArrayRef)> = vec![
            ("b", Arc::new(BooleanArray::from(vec![true]))),
            ("i16", Arc::new(Int16Array::from(vec![1i16]))),
            ("i32", Arc::new(Int32Array::from(vec![1i32]))),
            ("i64", Arc::new(Int64Array::from(vec![1i64]))),
            ("u64", Arc::new(UInt64Array::from(vec![1u64]))),
            (
                "dec",
                Arc::new(
                    Decimal128Array::from(vec![100i128])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
            ),
            ("f32", Arc::new(Float32Array::from(vec![1.0f32]))),
            ("f64", Arc::new(Float64Array::from(vec![1.0f64]))),
            ("s", Arc::new(StringArray::from(vec!["x"]))),
            ("bin", Arc::new(BinaryArray::from_vec(vec![&[1u8][..]]))),
            (
                "uuid",
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![0u8; 16])).unwrap(),
                ),
            ),
            ("d", Arc::new(Date32Array::from(vec![0i32]))),
            ("t", Arc::new(Time64MicrosecondArray::from(vec![0i64]))),
            ("ts", Arc::new(TimestampMicrosecondArray::from(vec![0i64]))),
        ];
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        // Sanity: each column's type is on the whitelist.
        for f in &fields {
            assert!(
                csv_serializable(f.data_type()),
                "test type {:?} not in csv_serializable",
                f.data_type()
            );
        }
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = cols.into_iter().map(|(_, a)| a).collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let mut w = CsvFormat
            .create_writer(&schema, Box::new(Vec::<u8>::new()))
            .unwrap();
        w.write_batch(&batch)
            .expect("every serializable type must write without hitting the fallthrough");
    }
}
