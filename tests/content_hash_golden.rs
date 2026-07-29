//! Golden fixture for the extraction-time content hash.
//!
//! Every expected value here was computed by an **independent**
//! implementation (a Python one-liner over `hashlib.sha256`), not by calling
//! rivet and recording what it said. That is the whole point of a golden: a
//! bug in the renderer must show up as a diff, and a fixture regenerated from
//! the code under test cannot do that.
//!
//! These tests go through the real Arrow path —
//! [`hashed_schema`] + [`append_content_hash`] — so they pin the *rendering*
//! (types, NULLs, timestamp truncation, time units), not merely that SHA-256
//! works.
//!
//! The contract they defend is cross-engine byte parity: the audit's source
//! leg recomputes these same strings in SQL. `matches_the_live_bigquery_anchor`
//! in `src/content_hash.rs` pins one end of that against a value BigQuery
//! actually produced; this file pins the shape of everything around it.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rivet::content_hash::{
    COL_CONTENT_HASH, ContentHashConfig, append_content_hash, hashed_schema,
};

/// Build a one-row batch, hash it, return the hash.
fn hash_row(pk: &str, cols: &[&str], fields: Vec<Field>, columns: Vec<ArrayRef>) -> String {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let cfg = ContentHashConfig {
        pk: pk.into(),
        cols: cols.iter().map(|s| s.to_string()).collect(),
    };
    let schema = hashed_schema(&batch.schema(), &cfg).unwrap();
    let out = append_content_hash(&batch, &cfg, &schema).unwrap();
    out.column_by_name(COL_CONTENT_HASH)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

fn i64_col(name: &str, v: i64) -> (Field, ArrayRef) {
    (
        Field::new(name, DataType::Int64, false),
        Arc::new(Int64Array::from(vec![v])),
    )
}

fn str_col(name: &str, v: Option<&str>) -> (Field, ArrayRef) {
    (
        Field::new(name, DataType::Utf8, true),
        Arc::new(StringArray::from(vec![v])),
    )
}

fn ts_us_col(name: &str, v: i64) -> (Field, ArrayRef) {
    (
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Arc::new(TimestampMicrosecondArray::from(vec![v])),
    )
}

/// `1|approved|2024-01-01 00:01:00`
#[test]
fn golden_plain_row() {
    let (f1, c1) = i64_col("id", 1);
    let (f2, c2) = str_col("status", Some("approved"));
    let (f3, c3) = ts_us_col("updated_at", 1_704_067_260_000_000);
    assert_eq!(
        hash_row(
            "id",
            &["status", "updated_at"],
            vec![f1, f2, f3],
            vec![c1, c2, c3]
        ),
        "b048d9cc169adda"
    );
}

/// `2|<NULL>` — a NULL renders as the explicit sentinel.
#[test]
fn golden_null_renders_as_sentinel() {
    let (f1, c1) = i64_col("id", 2);
    let (f2, c2) = str_col("status", None);
    assert_eq!(
        hash_row("id", &["status"], vec![f1, f2], vec![c1, c2]),
        "47cbba2725c1d1c"
    );
}

/// `3|` — and it must NOT equal the NULL rendering. NULL-vs-empty is a real
/// divergence in the source; if both hashed alike the audit would call it
/// CLEAN.
#[test]
fn golden_empty_string_is_not_null() {
    let (f1, c1) = i64_col("id", 3);
    let (f2, c2) = str_col("status", Some(""));
    let empty = hash_row("id", &["status"], vec![f1, f2], vec![c1, c2]);
    assert_eq!(empty, "626938c3fd46f41");
    assert_ne!(empty, "47cbba2725c1d1c", "empty string collided with NULL");
}

/// `4|a|b` — the delimiter is deliberately NOT escaped, so a `|` inside a
/// value aliases with the field separator.
///
/// This is pinned rather than fixed: no engine's SQL expression escapes the
/// separator either, so both sides alias identically and the comparison stays
/// sound. Escaping in Rust alone would BREAK parity. If a future change adds
/// escaping, it must land on both legs at once — and this test is what will
/// notice.
#[test]
fn golden_pipe_inside_a_value_aliases_by_design() {
    let (f1, c1) = i64_col("id", 4);
    let (f2, c2) = str_col("status", Some("a|b"));
    let one_col = hash_row("id", &["status"], vec![f1, f2], vec![c1, c2]);
    assert_eq!(one_col, "331619b874dc2e7");

    let (g1, d1) = i64_col("id", 4);
    let (g2, d2) = str_col("a", Some("a"));
    let (g3, d3) = str_col("b", Some("b"));
    let two_cols = hash_row("id", &["a", "b"], vec![g1, g2, g3], vec![d1, d2, d3]);
    assert_eq!(
        one_col, two_cols,
        "documented aliasing: 'a|b' in one column renders like 'a','b' in two"
    );
}

/// `5|Ünïcødé ✓ 日本` — hashed over UTF-8 bytes, never over a lossy
/// transcoding.
#[test]
fn golden_unicode_is_hashed_as_utf8() {
    let (f1, c1) = i64_col("id", 5);
    let (f2, c2) = str_col("status", Some("Ünïcødé ✓ 日本"));
    assert_eq!(
        hash_row("id", &["status"], vec![f1, f2], vec![c1, c2]),
        "3edf2aca5dc8719"
    );
}

/// `6|1969-12-31 23:59:59` — one microsecond BEFORE the epoch floors down,
/// the direction SQL's formatters take. A truncating division would give
/// `1970-01-01 00:00:00` and silently disagree with every source.
#[test]
fn golden_pre_epoch_floors() {
    let (f1, c1) = i64_col("id", 6);
    let (f2, c2) = ts_us_col("t", -1);
    assert_eq!(
        hash_row("id", &["t"], vec![f1, f2], vec![c1, c2]),
        "2cf1b1c3bc80ec7"
    );

    let (g1, d1) = i64_col("id", 6);
    let (g2, d2) = ts_us_col("t", 0);
    assert_eq!(
        hash_row("id", &["t"], vec![g1, g2], vec![d1, d2]),
        "d8ffa7e2a01919c",
        "the epoch itself must not be dragged backwards"
    );
}

/// The same instant in seconds, milliseconds, microseconds and nanoseconds
/// must hash identically — the source's column precision is an encoding
/// detail, not content.
#[test]
fn golden_time_units_agree() {
    const EXPECTED: &str = "7c59c508cf96fc2"; // 10|2024-06-15 12:34:56
    let secs = 1_718_454_896_i64;

    let variants: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new("t", DataType::Timestamp(TimeUnit::Second, None), false),
            Arc::new(TimestampSecondArray::from(vec![secs])) as ArrayRef,
        ),
        (
            Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Arc::new(TimestampMillisecondArray::from(vec![secs * 1_000])),
        ),
        (
            Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Arc::new(TimestampMicrosecondArray::from(vec![secs * 1_000_000])),
        ),
        (
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Arc::new(TimestampNanosecondArray::from(vec![secs * 1_000_000_000])),
        ),
    ];

    for (field, array) in variants {
        let unit = field.data_type().clone();
        let (f1, c1) = i64_col("id", 10);
        assert_eq!(
            hash_row("id", &["t"], vec![f1, field], vec![c1, array]),
            EXPECTED,
            "unit {unit} disagreed"
        );
    }
}

/// Sub-second precision is TRUNCATED, never rounded: `.999999` stays on the
/// same second. Rounding would push a row into the next second and diverge
/// from every engine's `HH:MM:SS` formatter.
#[test]
fn golden_sub_second_truncates() {
    let (f1, c1) = i64_col("id", 10);
    let (f2, c2) = ts_us_col("t", 1_718_454_896_999_999);
    assert_eq!(
        hash_row("id", &["t"], vec![f1, f2], vec![c1, c2]),
        "7c59c508cf96fc2"
    );
}

/// `18446744073709551615|x` — a `u64` key survives intact. Normalising the
/// integer family through `i64` would wrap this to `-1`.
#[test]
fn golden_u64_max_is_not_wrapped() {
    let fields = vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, true),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![u64::MAX])),
        Arc::new(StringArray::from(vec![Some("x")])),
    ];
    assert_eq!(
        hash_row("id", &["status"], fields, columns),
        "90d811ef8e1460c"
    );
}

/// `-7|x` — a negative key renders with its sign, as `CAST(pk AS STRING)`
/// does on every engine.
#[test]
fn golden_negative_key() {
    let (f1, c1) = i64_col("id", -7);
    let (f2, c2) = str_col("status", Some("x"));
    assert_eq!(
        hash_row("id", &["status"], vec![f1, f2], vec![c1, c2]),
        "4958eb4bb4bc63e"
    );
}

/// `8|2024-01-01 00:00:00` — a DATE carries the time-of-day part, matching
/// `to_char(date, 'YYYY-MM-DD HH24:MI:SS')`.
#[test]
fn golden_date_carries_midnight() {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d", DataType::Date32, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![8])),
        Arc::new(Date32Array::from(vec![19_723])),
    ];
    assert_eq!(hash_row("id", &["d"], fields, columns), "291bb751ad05724");
}

/// `9|9999-12-31 23:59:59` — the far end of the range every engine accepts.
#[test]
fn golden_far_future() {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("t", DataType::Timestamp(TimeUnit::Second, None), false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![9])),
        Arc::new(TimestampSecondArray::from(vec![253_402_300_799])),
    ];
    assert_eq!(hash_row("id", &["t"], fields, columns), "506c3748a8126c2");
}

/// Column ORDER is part of the contract — swapping two columns must change
/// the hash, or a mis-ordered `--sample-cols` would silently pass.
#[test]
fn golden_column_order_matters() {
    let mk = |cols: &[&str]| {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("x")])),
            Arc::new(StringArray::from(vec![Some("y")])),
        ];
        hash_row("id", cols, fields, columns)
    };
    assert_ne!(mk(&["a", "b"]), mk(&["b", "a"]));
}
