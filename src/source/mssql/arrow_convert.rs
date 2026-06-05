//! SQL Server `Row` → Arrow `RecordBatch` pipeline.
//!
//! Mirrors `postgres::arrow_convert` / `mysql::arrow_convert`: map each
//! `tiberius::ColumnType` to a `RivetType` (the one semantic layer — ADR-0014),
//! build the Arrow schema via the shared `build_arrow_field`, then fill arrays
//! cell-by-cell from `tiberius::ColumnData`.
//!
//! Extraction goes through `ColumnData` (not typed `Row::get::<T>`) so the
//! `uuid` version tiberius vendors (1.x) does not have to match the crate's own
//! (`uuid` 0.8) — a `Guid` is read as its 16 canonical bytes directly. Temporal
//! values use tiberius' `chrono` `FromSql` impls via `try_get`, which already
//! normalise the raw TDS day/second counts.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, Timelike};
use tiberius::{Column, ColumnData, ColumnType, Row};

use crate::error::Result;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field, resolve_or,
};

/// Days from the Arrow/Unix epoch (1970-01-01) used to anchor `Date32`.
const UNIX_EPOCH_DAY: i32 = 0;

/// Map a SQL Server column type to its `RivetType`. An explicit
/// `exports[].columns:` override wins (lets a `decimal` without resolvable
/// precision still ride as a declared type, same as PG/MySQL).
pub(super) fn mssql_type_to_rivet(col: &Column, overrides: &ColumnOverrides) -> RivetType {
    resolve_or(overrides, col.name(), || match col.column_type() {
        ColumnType::Bit | ColumnType::Bitn => RivetType::Bool,
        // tinyint (0-255, unsigned) widens to i16 losslessly.
        ColumnType::Int1 | ColumnType::Int2 => RivetType::Int16,
        ColumnType::Int4 => RivetType::Int32,
        ColumnType::Int8 | ColumnType::Intn => RivetType::Int64,
        ColumnType::Float4 => RivetType::Float32,
        ColumnType::Float8 | ColumnType::Floatn => RivetType::Float64,
        // money / smallmoney are fixed decimal(19,4) / (10,4).
        ColumnType::Money => RivetType::Decimal {
            precision: 19,
            scale: 4,
        },
        ColumnType::Money4 => RivetType::Decimal {
            precision: 10,
            scale: 4,
        },
        // Precision/scale beyond this default land via wire metadata in a
        // follow-up; default keeps integer-valued numerics lossless.
        ColumnType::Decimaln | ColumnType::Numericn => RivetType::Decimal {
            precision: 38,
            scale: 0,
        },
        ColumnType::Guid => RivetType::Uuid,
        ColumnType::NVarchar
        | ColumnType::NChar
        | ColumnType::BigVarChar
        | ColumnType::BigChar
        | ColumnType::Text
        | ColumnType::NText => RivetType::String,
        ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => RivetType::Binary,
        ColumnType::Daten => RivetType::Date,
        ColumnType::Timen => RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        },
        ColumnType::Datetime
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::Datetime2 => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: None,
        },
        ColumnType::DatetimeOffsetn => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        },
        other => RivetType::Unsupported {
            native_type: format!("{other:?}").to_lowercase(),
            reason: format!("SQL Server column type {other:?} has no Rivet mapping yet"),
        },
    })
}

/// Build the Arrow schema for a result set's columns + the per-column
/// `(name, ColumnType)` the row decoder dispatches on.
pub(super) fn mssql_columns_to_schema(
    columns: &[Column],
    overrides: &ColumnOverrides,
    rows: &[Row],
) -> Result<(Schema, Vec<(String, ColumnType)>)> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut decoders: Vec<(String, ColumnType)> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();

    for (idx, col) in columns.iter().enumerate() {
        let mut rivet = mssql_type_to_rivet(col, overrides);
        // tiberius' `Column` drops a decimal's declared precision/scale (only the
        // type tag survives), so an autodetected `Decimal` carries a placeholder
        // scale. Recover the real scale from the data — every `Numeric` value in
        // a column shares it — so a `decimal(12,2)` does not silently truncate to
        // scale 0. A user `columns:` override wins and is left untouched.
        if !overrides.contains_key(col.name())
            && let RivetType::Decimal { precision, .. } = rivet
            && let Some(s) = decimal_scale_from_rows(idx, rows)
        {
            rivet = RivetType::Decimal {
                precision: precision.max(s),
                scale: s as i8,
            };
        }
        let native = format!("{:?}", col.column_type()).to_lowercase();
        let source = SourceColumn::simple(col.name(), native, true);
        let mapping = TypeMapping::from_source(&source, rivet);
        match build_arrow_field(&mapping) {
            Some(field) => {
                fields.push(field);
                decoders.push((col.name().to_string(), col.column_type()));
            }
            None => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this SQL Server type",
                };
                errors.push(format!(
                    "  • {} ({:?}): {reason}",
                    col.name(),
                    col.column_type()
                ));
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!(
            "SQL Server export: {} column(s) have no safe type mapping:\n{}",
            errors.len(),
            errors.join("\n")
        );
    }
    Ok((Schema::new(fields), decoders))
}

/// `TypeMapping` for every column — drives `rivet check --type-report`.
pub(super) fn mssql_type_mappings(
    columns: &[Column],
    overrides: &ColumnOverrides,
) -> Vec<TypeMapping> {
    columns
        .iter()
        .map(|col| {
            let rivet = mssql_type_to_rivet(col, overrides);
            let native = format!("{:?}", col.column_type()).to_lowercase();
            let source = SourceColumn::simple(col.name(), native, true);
            TypeMapping::from_source(&source, rivet)
        })
        .collect()
}

/// Fetch the `ColumnData` of column `idx` from a row without consuming it.
fn cell(row: &Row, idx: usize) -> Option<&ColumnData<'static>> {
    row.cells().nth(idx).map(|(_, d)| d)
}

/// The decimal scale of column `idx`, read from the first non-null `Numeric`
/// value (all values in a column share it). `None` when the column is all-null
/// or empty — the caller keeps its placeholder scale (no values to misalign).
fn decimal_scale_from_rows(idx: usize, rows: &[Row]) -> Option<u8> {
    rows.iter().find_map(|row| match cell(row, idx) {
        Some(ColumnData::Numeric(Some(n))) => Some(n.scale()),
        _ => None,
    })
}

/// Build one Arrow array for column `idx`, dispatching on the Arrow target type
/// (so the array always matches the schema field by construction).
fn build_array(target: &DataType, idx: usize, rows: &[Row]) -> Result<ArrayRef> {
    macro_rules! simple {
        ($builder:ty, $pat:pat => $val:expr) => {{
            let mut b = <$builder>::with_capacity(rows.len());
            for row in rows {
                match cell(row, idx) {
                    $pat => b.append_value($val),
                    Some(ColumnData::U8(None))
                    | Some(ColumnData::I16(None))
                    | Some(ColumnData::I32(None))
                    | Some(ColumnData::I64(None))
                    | Some(ColumnData::F32(None))
                    | Some(ColumnData::F64(None))
                    | Some(ColumnData::Bit(None))
                    | Some(ColumnData::String(None))
                    | Some(ColumnData::Binary(None))
                    | Some(ColumnData::Guid(None))
                    | Some(ColumnData::Numeric(None))
                    | None => b.append_null(),
                    Some(other) => anyhow::bail!(
                        "mssql column {idx}: expected {} but row carried {other:?}",
                        stringify!($builder)
                    ),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let arr: ArrayRef = match target {
        DataType::Boolean => simple!(BooleanBuilder, Some(ColumnData::Bit(Some(v))) => *v),
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::I16(Some(v))) => b.append_value(*v),
                    Some(ColumnData::U8(Some(v))) => b.append_value(*v as i16),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => simple!(Int32Builder, Some(ColumnData::I32(Some(v))) => *v),
        DataType::Int64 => simple!(Int64Builder, Some(ColumnData::I64(Some(v))) => *v),
        DataType::Float32 => simple!(Float32Builder, Some(ColumnData::F32(Some(v))) => *v),
        DataType::Float64 => simple!(Float64Builder, Some(ColumnData::F64(Some(v))) => *v),
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::String(Some(s))) => b.append_value(s.as_ref()),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::Binary(Some(bytes))) => b.append_value(bytes.as_ref()),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::Guid(Some(g))) => b.append_value(g.as_bytes())?,
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(_p, s) => {
            let scale = (*s).max(0) as u8;
            let mut b = Decimal128Builder::with_capacity(rows.len()).with_data_type(target.clone());
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::Numeric(Some(n))) => {
                        b.append_value(rescale_i128(n.value(), n.scale(), scale)?)
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for (r, row) in rows.iter().enumerate() {
                match row.try_get::<NaiveDate, _>(idx) {
                    Ok(Some(d)) => b.append_value(
                        (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
                            + UNIX_EPOCH_DAY,
                    ),
                    Ok(None) => b.append_null(),
                    Err(e) => anyhow::bail!("mssql date column {idx} row {r}: {e}"),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for (r, row) in rows.iter().enumerate() {
                match row.try_get::<NaiveDateTime, _>(idx) {
                    Ok(Some(dt)) => b.append_value(dt.and_utc().timestamp_micros()),
                    Ok(None) => b.append_null(),
                    Err(e) => anyhow::bail!("mssql datetime column {idx} row {r}: {e}"),
                }
            }
            let arr = b.finish();
            return Ok(match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            });
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            use arrow::array::Time64MicrosecondBuilder;
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for (r, row) in rows.iter().enumerate() {
                match row.try_get::<chrono::NaiveTime, _>(idx) {
                    Ok(Some(t)) => b.append_value(
                        t.num_seconds_from_midnight() as i64 * 1_000_000
                            + (t.nanosecond() as i64 / 1000),
                    ),
                    Ok(None) => b.append_null(),
                    Err(e) => anyhow::bail!("mssql time column {idx} row {r}: {e}"),
                }
            }
            Arc::new(b.finish())
        }
        other => anyhow::bail!("mssql: no array builder for Arrow type {other:?} (column {idx})"),
    };
    Ok(arr)
}

/// Rescale an unscaled decimal from `from_scale` to `to_scale` (Arrow's fixed
/// column scale). Loses no information when `to_scale >= from_scale`.
fn rescale_i128(value: i128, from_scale: u8, to_scale: u8) -> Result<i128> {
    use std::cmp::Ordering;
    match to_scale.cmp(&from_scale) {
        Ordering::Equal => Ok(value),
        Ordering::Greater => {
            let factor = 10i128.pow((to_scale - from_scale) as u32);
            value
                .checked_mul(factor)
                .ok_or_else(|| anyhow::anyhow!("mssql decimal overflow rescaling to {to_scale}"))
        }
        Ordering::Less => {
            let factor = 10i128.pow((from_scale - to_scale) as u32);
            Ok(value / factor)
        }
    }
}

/// Build a full `RecordBatch` from collected rows.
pub(super) fn mssql_rows_to_record_batch(
    schema: &SchemaRef,
    rows: &[Row],
) -> Result<arrow::record_batch::RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        arrays.push(build_array(field.data_type(), idx, rows)?);
    }
    Ok(arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        arrays,
    )?)
}
