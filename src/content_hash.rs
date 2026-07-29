//! Extraction-time canonical content hash — `__content_hash`.
//!
//! Computes, per row, `sha256(pk ‖ '|' ‖ col ‖ '|' ‖ …)` (lowercase hex,
//! first 15 chars) over a CANONICAL text rendering of the named columns,
//! and appends it to the record batch as a `Utf8` column. The rendering is
//! deliberately reproducible in SQL on every supported engine:
//!
//! - integers render as decimal digits (`CAST(col AS <text>)`),
//! - text passes through raw (byte comparison),
//! - timestamps render at ONE fixed second-precision format,
//!   `YYYY-MM-DD HH:MM:SS` (what `FORMAT_TIMESTAMP`/`DATE_FORMAT`/
//!   `to_char` produce with an explicit format string),
//! - NULL renders as the explicit sentinel `<NULL>` — a NULL must never
//!   silently collapse a row's text onto another row's.
//!
//! Downstream (e.g. a warehouse↔source auditor) can then recompute the
//! same value with a plain SQL expression and compare hashes without ever
//! re-reading the content columns from the warehouse: the hash column IS
//! the content, 15 hex chars wide. This is NOT `_rivet_row_hash`: that one
//! is xxh3 over Arrow's display formatting — free and fine for
//! warehouse↔warehouse drift, but unreproducible in SQL, so it cannot
//! anchor a source comparison.
//!
//! Types outside the proven-parity set (decimals, floats, bytes, dates,
//! booleans) are REFUSED loudly rather than rendered ad hoc: every type
//! class added here must first earn its cross-engine rendering parity
//! (trailing-zero scale padding on decimals is the classic silent trap).
//!
//! Session-state caveat (the CDC datestyle lesson): the SQL counterpart of
//! this rendering must use explicit format functions and a pinned session
//! (UTC, fixed datestyle) — engine default renderings are session-shaped.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};

use crate::config::ContentHashConfig;
use crate::error::Result;

/// The appended column. Double-underscore namespace, like the CDC meta
/// columns (`__op`/`__pos`/`__seq`): it travels INTO the warehouse table.
pub const COL_CONTENT_HASH: &str = "__content_hash";

/// Hex chars kept from the sha256 — 60 bits, fits a signed 64-bit integer
/// on every engine (the auditor's XOR-aggregation constraint).
pub const HASH_HEX_CHARS: usize = 15;

/// NULL sentinel in the canonical text.
pub const NULL_SENTINEL: &str = "<NULL>";

/// The schema `Field` for the hash column. Non-nullable: every row of a
/// batch gets a hash (NULL cells render as the sentinel).
pub fn content_hash_field() -> Field {
    Field::new(COL_CONTENT_HASH, DataType::Utf8, false)
}

/// `schema` + the hash field appended last.
pub fn append_schema(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(content_hash_field()));
    Arc::new(Schema::new(fields))
}

/// Compute the hash column from named arrays. `cols` are looked up by name
/// in `named` — a missing name or an unsupported Arrow type fails LOUD
/// (never a silent skip: a skipped column would produce hashes that
/// "match" while attesting content that was never hashed).
pub fn hash_array(named: &[(&str, &ArrayRef)], pk: &str, cols: &[String]) -> Result<ArrayRef> {
    let find = |name: &str| -> Result<&ArrayRef> {
        named
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, a)| *a)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "content_hash: column '{name}' not found in the export's result set \
                     (available: {})",
                    named.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
                )
            })
    };
    let pk_arr = find(pk)?;
    let col_arrs: Vec<&ArrayRef> = cols.iter().map(|c| find(c)).collect::<Result<Vec<_>>>()?;
    // Type refusal lives in `render_cell`'s catch-all arm (it names the
    // column); the SEAM-level check is `validate_against_schema`, which runs
    // before any row is read.
    let rows = pk_arr.len();
    let mut out = Vec::with_capacity(rows);
    let mut text = String::new();
    for row in 0..rows {
        text.clear();
        if pk_arr.is_null(row) {
            anyhow::bail!(
                "content_hash: pk column '{pk}' is NULL at row {row} — a NULL key \
                 cannot anchor a per-row content hash"
            );
        }
        render_cell(&mut text, pk, pk_arr, row)?;
        for (name, arr) in cols.iter().zip(&col_arrs) {
            text.push('|');
            render_cell(&mut text, name, arr, row)?;
        }
        let digest = Sha256::digest(text.as_bytes());
        let mut hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
        hex.truncate(HASH_HEX_CHARS);
        out.push(hex);
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

/// Append the hash column to a batch (batch-export seam). The CDC sink
/// uses [`hash_array`] directly — its arrays exist before the batch does.
pub fn append_content_hash(batch: &RecordBatch, cfg: &ContentHashConfig) -> Result<RecordBatch> {
    let schema = batch.schema();
    let named: Vec<(&str, &ArrayRef)> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, a)| (f.name().as_str(), a))
        .collect();
    let hash = hash_array(&named, &cfg.pk, &cfg.cols)?;
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays.push(hash);
    Ok(RecordBatch::try_new(append_schema(&schema), arrays)?)
}

/// The proven-parity type set. Everything else refuses with the reason.
///
/// Tz-AWARE timestamps are refused too: the Rust side would render the UTC
/// instant, but the SQL counterparts (`to_char` on PG `timestamptz`,
/// `DATE_FORMAT` on a MySQL `TIMESTAMP`) render in SESSION time zone — the
/// exact session-state class the CDC datestyle lesson flags. They join the
/// set only with per-engine live parity cells under a FLIPPED session zone
/// ("parity at default state is not evidence").
fn ensure_renderable(name: &str, dt: &DataType) -> Result<()> {
    use DataType::*;
    match dt {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Ok(()),
        Utf8 | LargeUtf8 => Ok(()),
        Timestamp(_, None) => Ok(()),
        // A DATE earns its place under exactly the rule the catch-all states:
        // the rendering must be provably session-independent, and it is.
        // `to_char(d,'YYYY-MM-DD HH24:MI:SS')`, BigQuery's `FORMAT_DATETIME`
        // over a CAST, and `CONVERT(VARCHAR(19), d, 120)` all render a
        // zoneless calendar day identically, with a midnight time part. That
        // is precisely what separates it from the tz-aware timestamp refused
        // below: there is no session zone for the engines to disagree about.
        Date32 | Date64 => Ok(()),
        Timestamp(_, Some(_)) => anyhow::bail!(
            "content_hash: column '{name}' is a TZ-AWARE timestamp — its SQL \
             re-rendering is session-zone-dependent, and cross-engine parity \
             under a non-UTC session is unproven. Hash a naive (wall-clock) \
             column instead, or wait for the tz parity cells."
        ),
        other => anyhow::bail!(
            "content_hash: column '{name}' has type {other} — outside the proven \
             cross-engine rendering set (int, text, date, naive timestamp). \
             Decimals/floats/bytes are refused until their canonical rendering \
             parity is proven per engine (trailing-zero scale padding differs \
             silently)."
        ),
    }
}

/// Validate a `content_hash` config against a resolved schema — run this at
/// the SEAM (batch `on_schema`, CDC capture setup), before any row is read:
/// an empty run must fail a broken config loudly, not write a green
/// `_SUCCESS` that hides the typo until the first non-empty run.
pub fn validate_against_schema(schema: &SchemaRef, cfg: &ContentHashConfig) -> Result<()> {
    let named: Vec<(&str, &DataType)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    validate_named(&named, cfg)
}

/// [`validate_against_schema`] over bare `(name, type)` pairs — for the CDC
/// seam, where the resolved `TypeMapping`s exist before any Arrow schema.
/// A `None`/unbuildable arrow type degrades to `Utf8` in the sink schema, so
/// callers map that case to `&DataType::Utf8` to mirror what will be built.
pub fn validate_named(named: &[(&str, &DataType)], cfg: &ContentHashConfig) -> Result<()> {
    for name in std::iter::once(&cfg.pk).chain(cfg.cols.iter()) {
        let dt = named
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, dt)| *dt)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "content_hash: column '{name}' not found in the export's result set \
                     (available: {})",
                    named.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
                )
            })?;
        ensure_renderable(name, dt)?;
    }
    Ok(())
}

/// Render one cell into `text` in the canonical form. The catch-all arm is
/// the per-batch type refusal (the seam check in [`validate_against_schema`]
/// normally fires first; this one guards direct `hash_array` callers).
fn render_cell(text: &mut String, name: &str, arr: &ArrayRef, row: usize) -> Result<()> {
    use arrow::array::*;
    use std::fmt::Write;
    if arr.is_null(row) {
        text.push_str(NULL_SENTINEL);
        return Ok(());
    }
    macro_rules! int_arm {
        ($ty:ty) => {{
            let a = arr.as_any().downcast_ref::<$ty>().unwrap();
            let _ = write!(text, "{}", a.value(row));
        }};
    }
    match arr.data_type() {
        DataType::Int8 => int_arm!(Int8Array),
        DataType::Int16 => int_arm!(Int16Array),
        DataType::Int32 => int_arm!(Int32Array),
        DataType::Int64 => int_arm!(Int64Array),
        DataType::UInt8 => int_arm!(UInt8Array),
        DataType::UInt16 => int_arm!(UInt16Array),
        DataType::UInt32 => int_arm!(UInt32Array),
        DataType::UInt64 => int_arm!(UInt64Array),
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            text.push_str(a.value(row));
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            text.push_str(a.value(row));
        }
        // Date32 counts DAYS, Date64 milliseconds — both widened to seconds
        // here. The direction matters: written as a divide, every DATE renders
        // as 1970-01-01, which looks like a perfectly ordinary hash and
        // disagrees with the source on every dated row.
        DataType::Date32 | DataType::Date64 => {
            let secs = match arr.data_type() {
                DataType::Date32 => {
                    arr.as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(row) as i64
                        * 86_400
                }
                _ => arr
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .unwrap()
                    .value(row)
                    .div_euclid(1_000),
            };
            let dt = chrono::DateTime::from_timestamp(secs, 0)
                .ok_or_else(|| anyhow::anyhow!("content_hash: date {secs}s out of chrono range"))?;
            let _ = write!(text, "{}", dt.format("%Y-%m-%d %H:%M:%S"));
        }
        // One fixed rendering per naive-timestamp unit: the stored wall-clock
        // value truncated to seconds. Tz-aware timestamps are refused by
        // `ensure_renderable` (session-zone parity unproven) and fall to the
        // catch-all arm below.
        DataType::Timestamp(unit, None) => {
            let raw = match unit {
                TimeUnit::Second => arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Millisecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row)
                    .div_euclid(1_000),
                TimeUnit::Microsecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row)
                    .div_euclid(1_000_000),
                TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(row)
                    .div_euclid(1_000_000_000),
            };
            let dt = chrono::DateTime::from_timestamp(raw, 0).ok_or_else(|| {
                anyhow::anyhow!("content_hash: timestamp {raw}s out of chrono range")
            })?;
            let _ = write!(text, "{}", dt.format("%Y-%m-%d %H:%M:%S"));
        }
        other => {
            // Same refusal text as the seam check, so a direct caller gets
            // the identical, actionable message.
            ensure_renderable(name, other)?;
            anyhow::bail!("content_hash: unreachable — '{name}' passed ensure_renderable")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};

    fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            cols.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }

    fn cfg(pk: &str, cols: &[&str]) -> ContentHashConfig {
        ContentHashConfig {
            pk: pk.into(),
            cols: cols.iter().map(|c| c.to_string()).collect(),
        }
    }

    /// Golden values: sha256 of the canonical text, first 15 hex chars —
    /// computed independently (python hashlib). If this test moves, the
    /// SQL counterpart expression diverges silently: NEVER regenerate the
    /// goldens from this code's own output.
    #[test]
    fn golden_hash_int_text_timestamp() {
        // 2024-01-01 03:22:00 UTC = 1704079320 s
        let b = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            (
                "status",
                Arc::new(StringArray::from(vec!["approved"])) as ArrayRef,
            ),
            (
                "updated_at",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_704_079_320_000_000i64,
                ])) as ArrayRef,
            ),
        ]);
        let out = append_content_hash(&b, &cfg("id", &["status", "updated_at"])).unwrap();
        let hashes = out
            .column(out.num_columns() - 1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // text: "1|approved|2024-01-01 03:22:00"
        assert_eq!(hashes.value(0), "81035f62dfbeab2");
        assert_eq!(
            out.schema().field(out.num_columns() - 1).name(),
            COL_CONTENT_HASH
        );
    }
    #[test]
    fn missing_column_refuses_loud() {
        let b = batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )]);
        let err = append_content_hash(&b, &cfg("id", &["nope"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'nope'"), "{err}");
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn unsupported_type_refuses_loud_naming_column() {
        use arrow::array::Decimal128Array;
        let dec: ArrayRef = Arc::new(
            Decimal128Array::from(vec![144286i128])
                .with_precision_and_scale(11, 4)
                .unwrap(),
        );
        let b = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("amount", dec),
        ]);
        let err = append_content_hash(&b, &cfg("id", &["amount"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'amount'"), "{err}");
        assert!(err.contains("proven"), "{err}");
    }

    #[test]
    fn null_pk_refuses_loud() {
        let b = batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
        )]);
        let err = append_content_hash(&b, &cfg("id", &[]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("NULL"), "{err}");
    }

    /// Tz-AWARE timestamps refuse: their SQL re-rendering is session-zone-
    /// dependent and no flipped-session parity cell exists yet. Accepting
    /// them would make every hash silently wrong on a non-UTC source session.
    #[test]
    fn tz_aware_timestamp_refuses_loud() {
        let ts: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![1_704_079_320_000_000i64]).with_timezone("UTC"),
        );
        let b = batch(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("ts", ts),
        ]);
        let err = append_content_hash(&b, &cfg("id", &["ts"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("TZ-AWARE"), "{err}");
        assert!(err.contains("'ts'"), "{err}");
    }

    /// The seam check must catch the same config errors BEFORE any batch —
    /// an empty run fails loud instead of writing a green `_SUCCESS` over a
    /// typo'd column (the config error would otherwise hide until the first
    /// non-empty run).
    #[test]
    fn validate_against_schema_catches_typo_and_type_before_any_row() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(11, 4), true),
            Field::new("status", DataType::Utf8, true),
        ]));
        let err = validate_against_schema(&schema, &cfg("id", &["nope"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'nope'") && err.contains("not found"), "{err}");
        let err = validate_against_schema(&schema, &cfg("id", &["amount"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'amount'") && err.contains("proven"), "{err}");
        validate_against_schema(&schema, &cfg("id", &["status"])).unwrap();
    }
}
