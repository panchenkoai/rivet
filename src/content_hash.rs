//! **Layer: Extraction**
//!
//! The canonical per-row **content hash** — computed where the rows are
//! read, not where they land.
//!
//! ## Why extraction and not the warehouse
//!
//! A columnar warehouse bills by column read, so auditing content by
//! re-reading N content columns costs N column-scans on every run. Persist
//! one hash column and the warehouse side reads exactly two columns (`pk`,
//! [`COL_CONTENT_HASH`]) — as cheap as a presence check — while the source
//! recomputes the same value inline, which a row store pays for anyway.
//!
//! The obvious way to produce that column is to project a SQL expression in
//! the export query. That works for batch and **cannot work for CDC**: a
//! binlog / WAL / oplog event never passes through a SQL projection, yet the
//! drain and the snapshot leg write the *same warehouse table* and must
//! therefore land the *same columns*. A projection-only hash yields a table
//! whose backfilled rows carry a hash and whose streamed rows do not.
//!
//! So the canonical render lives here, in Rust, and every leg applies it:
//! batch sink, CDC sink, snapshot leg. The column becomes a property of
//! *extraction* rather than of whichever path produced the row.
//!
//! ## The parity obligation
//!
//! The audit's source side recomputes this value **in SQL**. That makes the
//! rendering a cross-engine contract, not an implementation detail: the text
//! below must be reproducible character-for-character by PostgreSQL,
//! MySQL, SQL Server, BigQuery and Snowflake. Concretely
//!
//! ```text
//! text = pk ‖ '|' ‖ col₁ ‖ '|' ‖ col₂ ‖ …          (NULL → "<NULL>")
//! hash = lowercase_hex(sha256(utf8(text)))[..15]
//! ```
//!
//! with timestamps at ONE fixed second-precision format —
//! `YYYY-MM-DD HH:MM:SS`, UTC — on every engine, never the engine's default
//! rendering (defaults are session-state dependent, and that class of bug
//! ships silently).
//!
//! Two consequences that look like omissions and are not:
//!
//! * **Unreproducible types are refused, never coerced.** Floats and
//!   decimals have no text form every engine agrees on; booleans render
//!   `true`/`false` on one engine and `1`/`0` on another. Hashing them
//!   anyway would produce a column that disagrees with the source on every
//!   row — a silent 100% false divergence. [`ContentHashConfig::validate`]
//!   and [`append_content_hash`] refuse loudly instead.
//! * **The `|` delimiter is not escaped.** Escaping would be safer in
//!   isolation, but no engine's SQL expression escapes it, and parity with
//!   that expression is the entire point. A `|` inside a value therefore
//!   aliases with the field separator — identically on both sides, so the
//!   comparison stays sound. The golden fixture pins this behaviour so it
//!   cannot drift silently.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::Result;

/// The materialized hash column, as it appears in the warehouse.
pub const COL_CONTENT_HASH: &str = "__content_hash";

/// Hex characters kept from the sha256 digest. 15 is inherited from the
/// audit's digest width: it is a *string* prefix (`SUBSTR(…, 1, 15)`), so it
/// deliberately splits the 8th byte.
pub const HASH_HEX_CHARS: usize = 15;

/// What a NULL renders as. An explicit sentinel, never an empty string: a
/// NULL must not be able to masquerade as `''`, or NULL-vs-empty drift would
/// hash identically and pass the audit.
pub const NULL_SENTINEL: &str = "<NULL>";

/// The field separator, matching every engine's SQL expression.
pub const FIELD_SEP: char = '|';

/// Which columns the hash covers.
///
/// This is a **contract**, not a preference: the value only means anything
/// next to the column set that produced it, so the set travels with the data
/// (see the run manifest) and an auditor comparing against a different set
/// must fail rather than silently compare different texts.
///
/// ```yaml
/// exports:
///   - name: cashback_versions
///     content_hash:
///       pk: id
///       cols: [status, updated_at]
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ContentHashConfig {
    /// The primary key column. Rendered first and never NULL-coalesced — a
    /// NULL primary key is refused (see [`append_content_hash`]).
    pub pk: String,
    /// The content columns, in the order they are hashed. Order is part of
    /// the contract: reordering changes every hash.
    pub cols: Vec<String>,
}

impl ContentHashConfig {
    /// The column set this hash covers, `pk` first — the exact string an
    /// auditor compares against its own `--sample-cols`.
    pub fn covered(&self) -> Vec<String> {
        let mut v = Vec::with_capacity(self.cols.len() + 1);
        v.push(self.pk.clone());
        v.extend(self.cols.iter().cloned());
        v
    }

    /// Config-time validation: everything checkable without a batch.
    ///
    /// Type refusals need the Arrow schema and so happen in
    /// [`hashed_schema`]; this catches the shape errors a `rivet check` can
    /// report before a single row is read.
    pub fn validate(&self, export_name: &str) -> Result<()> {
        if self.pk.trim().is_empty() {
            anyhow::bail!("export '{export_name}': content_hash.pk must name a column");
        }
        if self.cols.is_empty() {
            anyhow::bail!(
                "export '{export_name}': content_hash.cols is empty — a hash over the primary \
                 key alone carries no content and would report every content edit as CLEAN. \
                 List the columns the audit must cover."
            );
        }
        if let Some(dup) = first_duplicate(&self.covered()) {
            anyhow::bail!(
                "export '{export_name}': content_hash lists column '{dup}' twice (pk is always \
                 hashed first) — the covered set must be a set"
            );
        }
        Ok(())
    }
}

fn first_duplicate(names: &[String]) -> Option<&str> {
    let mut seen = std::collections::HashSet::new();
    names
        .iter()
        .find(|n| !seen.insert(n.as_str()))
        .map(|s| s.as_str())
}

/// Hash a rendered row text. Split out so the golden fixture can pin the
/// digest independently of any Arrow plumbing.
pub fn hash_of(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    // Encode only the bytes the prefix can reach: 8 bytes → 16 hex chars,
    // one more than HASH_HEX_CHARS needs.
    let mut hex = hex::encode(&digest[..(HASH_HEX_CHARS / 2 + 1)]);
    hex.truncate(HASH_HEX_CHARS);
    hex
}

/// Extend an Arrow schema with [`COL_CONTENT_HASH`], refusing anything the
/// renderer cannot reproduce in SQL.
///
/// This is where type refusal happens, and it happens **before** the first
/// row is read: a float in the covered set is a config error, not a runtime
/// surprise on batch 400.
pub fn hashed_schema(schema: &SchemaRef, cfg: &ContentHashConfig) -> Result<SchemaRef> {
    if schema.column_with_name(COL_CONTENT_HASH).is_some() {
        anyhow::bail!(
            "the source already projects a column named '{COL_CONTENT_HASH}' — rivet would \
             overwrite it. Rename it, or drop content_hash from the export."
        );
    }
    // Resolving every column here means a typo fails the run rather than
    // silently hashing a shorter text.
    let pk_field = field(schema, &cfg.pk)?;
    Role::Pk.accepts(pk_field.data_type()).map_err(|why| {
        anyhow::anyhow!(
            "content_hash.pk '{}' is {} — {why}",
            cfg.pk,
            pk_field.data_type()
        )
    })?;
    for name in &cfg.cols {
        let f = field(schema, name)?;
        Role::Col.accepts(f.data_type()).map_err(|why| {
            anyhow::anyhow!("content_hash column '{name}' is {} — {why}", f.data_type())
        })?;
    }

    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        COL_CONTENT_HASH,
        DataType::Utf8,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

fn field<'a>(schema: &'a SchemaRef, name: &str) -> Result<&'a Field> {
    schema
        .column_with_name(name)
        .map(|(_, f)| f)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "content_hash names column '{name}', which this export does not project. \
                 Available: {}",
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

/// Where a column sits in the rendered text. The primary key is rendered
/// bare (no NULL sentinel) and so tolerates fewer types than a content
/// column: a timestamp key would render as `2024-01-01T03:22:00` on BigQuery
/// and `2024-01-01 03:22:00` on PostgreSQL, and nothing downstream could
/// tell the two apart.
#[derive(Clone, Copy)]
enum Role {
    Pk,
    Col,
}

impl Role {
    fn accepts(self, dt: &DataType) -> std::result::Result<(), &'static str> {
        let ok = match dt {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => true,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => {
                matches!(self, Role::Col)
            }
            _ => false,
        };
        if ok {
            return Ok(());
        }
        Err(match dt {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                "a float has no text form every engine renders identically (digits, exponent \
                 and rounding all differ), so the source's SQL recomputation would disagree on \
                 every row. Exclude it, or carry an exact column alongside it"
            }
            DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => {
                "a decimal renders with engine-specific trailing zeros and sign placement, so \
                 the source's SQL recomputation would disagree. Exclude it from the hash"
            }
            DataType::Boolean => {
                "a boolean renders 'true'/'false' on some engines and '1'/'0' on others. Exclude \
                 it, or project it as text in the query"
            }
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => {
                "a temporal primary key renders differently per engine (BigQuery interposes a \
                 'T'); use an integer or text key"
            }
            _ => {
                "this type has no canonical cross-engine text form. Project it as text in the \
                 export query if it must be covered"
            }
        })
    }
}

/// The [`COL_CONTENT_HASH`] column for a batch, on its own.
///
/// The two sinks assemble their output columns differently — the batch path
/// appends to an enriched schema, the CDC path builds `__op`/`__pos`/`__seq`
/// first — so both take the array and place it themselves.
pub fn hash_array(batch: &RecordBatch, cfg: &ContentHashConfig) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let mut renderers = Vec::with_capacity(cfg.cols.len() + 1);
    renderers.push(Renderer::resolve(batch, &cfg.pk, Role::Pk)?);
    for name in &cfg.cols {
        renderers.push(Renderer::resolve(batch, name, Role::Col)?);
    }

    let mut text = String::with_capacity(64);
    let mut hashes = Vec::with_capacity(n);
    for row in 0..n {
        text.clear();
        for (i, r) in renderers.iter().enumerate() {
            if i > 0 {
                text.push(FIELD_SEP);
            }
            r.write(row, &mut text)?;
        }
        hashes.push(hash_of(&text));
    }
    Ok(Arc::new(StringArray::from(hashes)))
}

/// Append [`COL_CONTENT_HASH`] to a batch.
///
/// `hashed` must be the schema returned by [`hashed_schema`] for the same
/// config — passing it in keeps the (validated) schema built once per export
/// rather than once per batch.
pub fn append_content_hash(
    batch: &RecordBatch,
    cfg: &ContentHashConfig,
    hashed: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(hash_array(batch, cfg)?);
    Ok(RecordBatch::try_new(hashed.clone(), columns)?)
}

/// A column resolved to its canonical rendering, once per batch.
///
/// The type dispatch happens here rather than per cell; casting the integer
/// and string families to one representative type each keeps the per-row
/// path to four arms without giving up `u64::MAX` (which does not survive a
/// trip through `i64`).
struct Renderer {
    name: String,
    role: Role,
    array: ArrayRef,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Int,
    UInt,
    Str,
    /// Seconds-per-unit for the stored integer, floor-divided so pre-epoch
    /// values round the same direction as SQL's date formatting.
    Epoch(i64),
}

impl Renderer {
    fn resolve(batch: &RecordBatch, name: &str, role: Role) -> Result<Self> {
        let (idx, f) = batch
            .schema()
            .column_with_name(name)
            .map(|(i, f)| (i, f.clone()))
            .ok_or_else(|| {
                anyhow::anyhow!("content_hash column '{name}' vanished from the batch schema")
            })?;
        let dt = f.data_type().clone();
        role.accepts(&dt)
            .map_err(|why| anyhow::anyhow!("content_hash column '{name}' is {dt} — {why}"))?;
        let src = batch.column(idx);
        let (kind, array) = match &dt {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                (Kind::Int, arrow::compute::cast(src, &DataType::Int64)?)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                (Kind::UInt, arrow::compute::cast(src, &DataType::UInt64)?)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                (Kind::Str, arrow::compute::cast(src, &DataType::Utf8)?)
            }
            // Both render with the time-of-day part, matching
            // `to_char(date, '… HH24:MI:SS')`. Date32 counts DAYS, so it is
            // widened to Date64's milliseconds first rather than given its
            // own scale — one less unit for the row loop to get backwards.
            DataType::Date32 | DataType::Date64 => {
                let ms = arrow::compute::cast(src, &DataType::Date64)?;
                (
                    Kind::Epoch(1_000),
                    arrow::compute::cast(&ms, &DataType::Int64)?,
                )
            }
            DataType::Timestamp(unit, _tz) => {
                // A tz-aware Arrow timestamp already stores a UTC instant, and
                // a naive one stores the wall clock the source held. Rendering
                // the stored value as UTC is correct for both — and is what
                // every engine's expression does (BigQuery casts DATETIME to
                // TIMESTAMP at UTC; MySQL formats the stored DATETIME).
                let per_second = match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 1_000,
                    TimeUnit::Microsecond => 1_000_000,
                    TimeUnit::Nanosecond => 1_000_000_000,
                };
                (
                    Kind::Epoch(per_second),
                    arrow::compute::cast(src, &DataType::Int64)?,
                )
            }
            other => anyhow::bail!("content_hash column '{name}': unsupported type {other}"),
        };
        Ok(Self {
            name: name.to_string(),
            role,
            array,
            kind,
        })
    }

    fn write(&self, row: usize, out: &mut String) -> Result<()> {
        use std::fmt::Write as _;
        if self.array.is_null(row) {
            return match self.role {
                // Refused, not sentinelled: the key is what the two sides
                // JOIN on, so a NULL key does not identify a row to compare.
                // MySQL's CONCAT_WS would additionally *skip* the field
                // rather than render it, so the two sides would not even
                // agree on the shape of the text.
                Role::Pk => anyhow::bail!(
                    "content_hash.pk '{}' is NULL on row {row}. A NULL key cannot be audited — \
                     it identifies no row to compare against the source",
                    self.name
                ),
                Role::Col => {
                    out.push_str(NULL_SENTINEL);
                    Ok(())
                }
            };
        }
        match self.kind {
            Kind::Int => {
                let a = self
                    .array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("cast to Int64");
                let _ = write!(out, "{}", a.value(row));
            }
            Kind::UInt => {
                let a = self
                    .array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .expect("cast to UInt64");
                let _ = write!(out, "{}", a.value(row));
            }
            Kind::Str => {
                let a = self
                    .array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("cast to Utf8");
                out.push_str(a.value(row));
            }
            Kind::Epoch(per_second) => {
                let a = self
                    .array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("cast to Int64");
                out.push_str(&render_epoch(a.value(row), per_second, &self.name, row)?);
            }
        }
        Ok(())
    }
}

/// `YYYY-MM-DD HH:MM:SS`, UTC, sub-second truncated.
///
/// `div_euclid` rather than `/`: for a pre-epoch instant the two disagree,
/// and SQL's formatters floor. `-1ns` is `1969-12-31 23:59:59`, not
/// `1970-01-01 00:00:00`.
fn render_epoch(raw: i64, per_second: i64, name: &str, row: usize) -> Result<String> {
    let secs = raw.div_euclid(per_second);
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "content_hash column '{name}' holds an out-of-range instant on row {row} \
                 ({secs} s since epoch) — no engine can render it, so no audit could match it"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampMicrosecondArray, UInt64Array};

    fn cfg(pk: &str, cols: &[&str]) -> ContentHashConfig {
        ContentHashConfig {
            pk: pk.into(),
            cols: cols.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// The anchor recovered from the live pilot warehouse: this exact triple
    /// is stored as `e17181cd2cf0526` in BigQuery, computed by BigQuery's own
    /// SQL. If this test fails, Rust and SQL have diverged and every audit
    /// built on the hash is comparing different texts.
    #[test]
    fn matches_the_live_bigquery_anchor() {
        assert_eq!(
            hash_of("202|approved|2024-01-01 03:22:00"),
            "e17181cd2cf0526"
        );
        assert_eq!(hash_of("1|approved|2024-01-01 00:01:00"), "b048d9cc169adda");
        assert_eq!(hash_of("2|paid|2024-01-01 00:02:00"), "052d8b663c22dfe");
    }

    #[test]
    fn renders_pk_cols_and_null_sentinel() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("status", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("approved"), None])),
            ],
        );
        let c = cfg("id", &["status"]);
        let schema = hashed_schema(&b.schema(), &c).unwrap();
        let out = append_content_hash(&b, &c, &schema).unwrap();
        let h = out
            .column_by_name(COL_CONTENT_HASH)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(h.value(0), hash_of("1|approved"));
        assert_eq!(h.value(1), hash_of(&format!("2|{NULL_SENTINEL}")));
    }

    #[test]
    fn empty_string_is_not_null() {
        assert_ne!(hash_of("1|"), hash_of(&format!("1|{NULL_SENTINEL}")));
    }

    #[test]
    fn u64_max_survives() {
        let b = batch(
            vec![Field::new("id", DataType::UInt64, false)],
            vec![Arc::new(UInt64Array::from(vec![u64::MAX]))],
        );
        let c = ContentHashConfig {
            pk: "id".into(),
            cols: vec!["id".into()],
        };
        // (duplicate covered set is rejected at validate() time, but the
        // renderer itself must still carry the value losslessly)
        let schema = hashed_schema(&b.schema(), &c).unwrap();
        let out = append_content_hash(&b, &c, &schema).unwrap();
        let h = out
            .column_by_name(COL_CONTENT_HASH)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            h.value(0),
            hash_of("18446744073709551615|18446744073709551615")
        );
    }

    #[test]
    fn pre_epoch_floors_like_sql() {
        assert_eq!(
            render_epoch(-1, 1_000_000_000, "t", 0).unwrap(),
            "1969-12-31 23:59:59"
        );
        assert_eq!(
            render_epoch(-1_000_000_000, 1_000_000_000, "t", 0).unwrap(),
            "1969-12-31 23:59:59"
        );
        assert_eq!(
            render_epoch(0, 1_000_000_000, "t", 0).unwrap(),
            "1970-01-01 00:00:00"
        );
    }

    #[test]
    fn sub_second_is_truncated_not_rounded() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                // 00:00:00.999999 must render as 00:00:00, never 00:00:01.
                Arc::new(TimestampMicrosecondArray::from(vec![999_999])),
            ],
        );
        let c = cfg("id", &["t"]);
        let schema = hashed_schema(&b.schema(), &c).unwrap();
        let out = append_content_hash(&b, &c, &schema).unwrap();
        let h = out
            .column_by_name(COL_CONTENT_HASH)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(h.value(0), hash_of("1|1970-01-01 00:00:00"));
    }

    #[test]
    fn float_is_refused_loudly() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("amount", DataType::Float64, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(arrow::array::Float64Array::from(vec![1.5])),
            ],
        );
        let err = hashed_schema(&b.schema(), &cfg("id", &["amount"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("amount") && err.contains("float"), "{err}");
    }

    #[test]
    fn boolean_is_refused_loudly() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("live", DataType::Boolean, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(arrow::array::BooleanArray::from(vec![true])),
            ],
        );
        let err = hashed_schema(&b.schema(), &cfg("id", &["live"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("live") && err.contains("'1'/'0'"), "{err}");
    }

    #[test]
    fn temporal_pk_is_refused() {
        let b = batch(
            vec![Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )],
            vec![Arc::new(TimestampMicrosecondArray::from(vec![0]))],
        );
        let err = hashed_schema(&b.schema(), &cfg("t", &["t"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("temporal primary key"), "{err}");
    }

    #[test]
    fn missing_column_is_refused_with_the_available_list() {
        let b = batch(
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );
        let err = hashed_schema(&b.schema(), &cfg("id", &["nope"]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("nope") && err.contains("Available: id"),
            "{err}"
        );
    }

    #[test]
    fn null_pk_is_refused_at_the_row() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("status", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![None, Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        );
        let c = cfg("id", &["status"]);
        let schema = hashed_schema(&b.schema(), &c).unwrap();
        let err = append_content_hash(&b, &c, &schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains("NULL") && err.contains("row 0"), "{err}");
    }

    #[test]
    fn colliding_hash_column_is_refused() {
        let b = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new(COL_CONTENT_HASH, DataType::Utf8, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        );
        let err = hashed_schema(&b.schema(), &cfg("id", &["id"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("already projects"), "{err}");
    }

    #[test]
    fn validate_rejects_empty_and_duplicate_sets() {
        let err = cfg("id", &[]).validate("e").unwrap_err().to_string();
        assert!(err.contains("carries no content"), "{err}");
        let err = cfg("id", &["status", "status"])
            .validate("e")
            .unwrap_err()
            .to_string();
        assert!(err.contains("twice"), "{err}");
        let err = cfg("id", &["id"]).validate("e").unwrap_err().to_string();
        assert!(err.contains("twice"), "{err}");
        cfg("id", &["status"]).validate("e").unwrap();
    }

    #[test]
    fn covered_puts_pk_first() {
        assert_eq!(cfg("id", &["b", "a"]).covered(), vec!["id", "b", "a"]);
    }
}
