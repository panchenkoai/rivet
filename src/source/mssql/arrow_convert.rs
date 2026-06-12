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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
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

use crate::source::value_within_ceiling;

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
///
/// `decimal_hints` carries each decimal/numeric column's *declared*
/// `(precision, scale)` read from `sys.columns` for a simple single-table
/// `SELECT` (the MSSQL twin of the PG catalog-hint path). tiberius drops the
/// declared precision/scale off `Column` (only the type tag survives), so
/// without the hint the scale would be inferred from the data — which freezes
/// at the placeholder `0` when the first emitted batch is all NULL. The
/// catalog hint is the upstream, lossless recovery; the data-inference
/// (`decimal_scale_from_rows`) stays ONLY as the fallback for expression /
/// computed columns that have no `sys.columns` entry. `None` ⇒ no catalog
/// lookup was possible (joins, subqueries, multi-table FROM), so keep today's
/// data-inference behaviour.
pub(super) fn mssql_columns_to_schema(
    columns: &[Column],
    overrides: &ColumnOverrides,
    rows: &[Row],
    decimal_hints: Option<&HashMap<String, (u8, i8)>>,
) -> Result<(Schema, Vec<(String, ColumnType)>)> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut decoders: Vec<(String, ColumnType)> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();

    for (idx, col) in columns.iter().enumerate() {
        let mut rivet = mssql_type_to_rivet(col, overrides);
        // tiberius' `Column` drops a decimal's declared precision/scale (only the
        // type tag survives), so an autodetected `Decimal` carries a placeholder
        // scale. Recover it — a user `columns:` override always wins and is left
        // untouched. Order of precedence for an autodetected `Decimal`:
        //   1. a `sys.columns` catalog hint (declared precision+scale) — the
        //      upstream, lossless source that survives an all-NULL first batch;
        //   2. otherwise the scale inferred from the first non-null value in the
        //      data (`decimal_scale_from_rows`), for expression/computed columns
        //      with no catalog entry.
        if !overrides.contains_key(col.name())
            && let RivetType::Decimal { precision, .. } = rivet
        {
            if let Some(&(p, s)) = decimal_hints.and_then(|h| h.get(col.name())) {
                rivet = RivetType::Decimal {
                    precision: p,
                    scale: s,
                };
            } else if let Some(s) = decimal_scale_from_rows(idx, rows) {
                rivet = RivetType::Decimal {
                    precision: precision.max(s),
                    scale: s as i8,
                };
            }
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
fn build_array(
    target: &DataType,
    idx: usize,
    rows: &[Row],
    column: &str,
    max_value_bytes: Option<usize>,
) -> Result<ArrayRef> {
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
            // Hand-rolled (not `simple!`) because it accepts two value variants
            // — I16 and a U8 widened to i16. Mismatch policy must still match
            // its numeric siblings: genuine NULLs append null, an unexpected
            // *valued* variant bails loud rather than silently nulling (this
            // arm was the one MSSQL type that broke that rule).
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::I16(Some(v))) => b.append_value(*v),
                    Some(ColumnData::U8(Some(v))) => b.append_value(*v as i16),
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
                        "mssql column {idx}: expected Int16Builder but row carried {other:?}"
                    ),
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
                    Some(ColumnData::String(Some(s))) => {
                        // Pre-allocation ceiling: the driver copy is unavoidable,
                        // but bail before the append so the Arrow buffer never
                        // grows to hold the oversized cell.
                        value_within_ceiling(column, s.len(), max_value_bytes)?;
                        b.append_value(s.as_ref());
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                match cell(row, idx) {
                    Some(ColumnData::Binary(Some(bytes))) => {
                        value_within_ceiling(column, bytes.len(), max_value_bytes)?;
                        b.append_value(bytes.as_ref());
                    }
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
            for (r, row) in rows.iter().enumerate() {
                match cell(row, idx) {
                    Some(ColumnData::Numeric(Some(n))) => b.append_value(
                        rescale_i128(n.value(), n.scale(), scale)
                            .with_context(|| format!("mssql decimal column {idx} row {r}"))?,
                    ),
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
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            // Opt-in via a `timestamp_ns` / `timestamp_tz_ns` column override —
            // preserves `datetime2(7)`'s 100 ns tick that the default microsecond
            // mapping truncates. Arrow nanosecond timestamps are i64 ns, so the
            // representable range is 1677-09-21 .. 2262-04-11; a value outside
            // that cannot be encoded and is exported as NULL (the documented
            // range caveat of the override — the default `timestamp` keeps full
            // range at microsecond precision).
            let mut b = TimestampNanosecondBuilder::with_capacity(rows.len());
            for (r, row) in rows.iter().enumerate() {
                match row.try_get::<NaiveDateTime, _>(idx) {
                    Ok(Some(dt)) => match dt.and_utc().timestamp_nanos_opt() {
                        Some(ns) => b.append_value(ns),
                        None => b.append_null(),
                    },
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
/// column scale). Loses no information when `to_scale >= from_scale`; a
/// down-scale is accepted only when the dropped digits are all zero — anything
/// else is lossy degradation and must surface as an `Err`, never as silently
/// truncated data.
fn rescale_i128(value: i128, from_scale: u8, to_scale: u8) -> Result<i128> {
    use std::cmp::Ordering;
    match to_scale.cmp(&from_scale) {
        Ordering::Equal => Ok(value),
        Ordering::Greater => {
            let factor = 10i128.pow((to_scale - from_scale) as u32);
            value.checked_mul(factor).ok_or_else(|| {
                anyhow::anyhow!(
                    "mssql decimal overflow: unscaled value {value} at scale {from_scale} \
                     does not fit i128 when rescaled to {to_scale}"
                )
            })
        }
        Ordering::Less => {
            let factor = 10i128.pow((from_scale - to_scale) as u32);
            if value % factor != 0 {
                anyhow::bail!(
                    "mssql decimal rescale is lossy: unscaled value {value} at scale \
                     {from_scale} does not fit column scale {to_scale} (non-zero digits \
                     would be dropped). The column's declared scale was likely lost — \
                     tiberius drops decimal precision/scale, and a first batch whose \
                     decimal values are all NULL falls back to scale 0. Declare the \
                     scale with a column override, e.g. \
                     columns: <name>: decimal(38,{from_scale})"
                );
            }
            Ok(value / factor)
        }
    }
}

/// Build a full `RecordBatch` from collected rows.
pub(super) fn mssql_rows_to_record_batch(
    schema: &SchemaRef,
    rows: &[Row],
    max_value_bytes: Option<usize>,
) -> Result<arrow::record_batch::RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        arrays.push(
            build_array(field.data_type(), idx, rows, field.name(), max_value_bytes)
                .with_context(|| format!("mssql column '{}'", field.name()))?,
        );
    }
    Ok(arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        arrays,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ROAST-RED mssql-rescale-loud: rescale_i128's down-scale arm does plain
    // integer division (`value / factor`), silently truncating non-zero
    // fractional remainders (123.45 at scale 2 -> scale 0 becomes 123, cents
    // gone). Lossy degradation must be LOUD: a down-scale that drops digits
    // must return Err, never an Ok with corrupted data.
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_rescale_i128_lossy_downscale_must_err() {
        // 12345 at scale 2 is 123.45; rescaling to scale 0 cannot represent
        // the .45 — the only honest answer is Err.
        let got = rescale_i128(12345, 2, 0);
        assert!(
            got.is_err(),
            "lossy down-scale of 12345 (scale 2 -> 0) must return Err, \
             got Ok({:?}) — the .45 cents were silently truncated",
            got.ok()
        );
    }

    // ROAST-RED mssql-rescale-loud: lossless guards — these already pass and
    // must keep passing once the lossy-down-scale fix lands (the fix must only
    // reject down-scales with a non-zero remainder, not all down-scales).
    #[test]
    fn roast_rescale_i128_lossless_paths_stay_ok() {
        // Exact down-scale: 12300 at scale 2 is 123.00 — scale 0 holds it.
        let down = rescale_i128(12300, 2, 0)
            .expect("lossless down-scale of 12300 (scale 2 -> 0) must stay Ok");
        assert_eq!(
            down, 123,
            "lossless down-scale of 12300 (scale 2 -> 0): expected 123, got {down}"
        );
        // Up-scale never loses information.
        let up = rescale_i128(123, 0, 2).expect("up-scale of 123 (scale 0 -> 2) must stay Ok");
        assert_eq!(
            up, 12300,
            "up-scale of 123 (scale 0 -> 2): expected 12300, got {up}"
        );
    }

    #[test]
    fn rescale_i128_equal_scale_is_identity() {
        assert_eq!(rescale_i128(12345, 2, 2).unwrap(), 12345);
        assert_eq!(rescale_i128(0, 0, 0).unwrap(), 0);
    }

    // Greater-arm regression: an up-scale that exceeds i128 must Err via
    // checked_mul, never wrap.
    #[test]
    fn rescale_i128_upscale_overflow_must_err() {
        let got = rescale_i128(i128::MAX, 0, 2);
        assert!(
            got.is_err(),
            "up-scaling i128::MAX (scale 0 -> 2) must overflow loudly, got Ok({:?})",
            got.ok()
        );
        let msg = format!("{:#}", got.unwrap_err());
        assert!(
            msg.contains("overflow"),
            "overflow Err must say so, got: {msg}"
        );
    }

    // Rust's `%` keeps the dividend's sign; the lossy check must catch negative
    // remainders too, and a lossless negative down-scale must stay exact.
    #[test]
    fn rescale_i128_negative_values() {
        assert!(
            rescale_i128(-12345, 2, 0).is_err(),
            "-123.45 (scale 2 -> 0) drops -.45 and must Err"
        );
        assert_eq!(rescale_i128(-12300, 2, 0).unwrap(), -123);
    }

    // Zero has no non-zero digits to drop at any scale.
    #[test]
    fn rescale_i128_zero_downscales_losslessly() {
        assert_eq!(rescale_i128(0, 38, 0).unwrap(), 0);
    }

    // SQL Server's max scale is 38; 10^38 still fits i128 (~1.7e38), so the
    // factor computation must not panic at the boundary in either direction.
    #[test]
    fn rescale_i128_max_scale_boundary_no_panic() {
        assert!(
            rescale_i128(1, 38, 0).is_err(),
            "1e-38 (scale 38 -> 0) is lossy and must Err, not panic"
        );
        assert_eq!(rescale_i128(1, 0, 38).unwrap(), 10i128.pow(38));
    }

    // The lossy Err must be actionable: name the value, both scales, and point
    // at the column-override remediation (the upstream fix — per the repo rule,
    // no post-hoc recovery exists once digits are dropped).
    #[test]
    fn rescale_i128_lossy_err_is_actionable() {
        let msg = format!("{:#}", rescale_i128(12345, 2, 0).unwrap_err());
        for needle in ["12345", "scale", "2", "0", "columns:", "override"] {
            assert!(
                msg.contains(needle),
                "lossy-rescale Err must mention {needle:?}, got: {msg}"
            );
        }
    }
}
