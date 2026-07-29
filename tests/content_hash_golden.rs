//! Golden fixture for the extraction-time canonical content hash
//! (`__content_hash`, `src/content_hash.rs`).
//!
//! Every expected hash here was computed INDEPENDENTLY (python hashlib over
//! the canonical text) — never regenerated from the code's own output. If a
//! change moves one of these values, the SQL counterpart expression in every
//! warehouse/auditor stops matching the extracted column SILENTLY: that is a
//! cross-engine contract break, not a fixture refresh.
//!
//! Live parity anchor (2026-07-29, MySQL 8 → parquet): the same four rows
//! hashed byte-identically via (a) this Rust renderer, (b) MySQL
//! `SUBSTRING(SHA2(CONCAT_WS('|', id, COALESCE(CAST(status AS CHAR),'<NULL>'),
//! COALESCE(CAST(DATE_FORMAT(updated_at,'%Y-%m-%d %H:%i:%s') AS CHAR),'<NULL>')),256),1,15)`,
//! and (c) BigQuery `SUBSTR(TO_HEX(SHA256(...)),1,15)` — e.g. pk 202 →
//! `e17181cd2cf0526` on all three.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rivet::config::ContentHashConfig;
use rivet::content_hash::{COL_CONTENT_HASH, append_content_hash};

fn cfg(pk: &str, cols: &[&str]) -> ContentHashConfig {
    ContentHashConfig {
        pk: pk.into(),
        cols: cols.iter().map(|c| c.to_string()).collect(),
    }
}

fn hashes(batch: &RecordBatch) -> Vec<String> {
    let col = batch
        .column_by_name(COL_CONTENT_HASH)
        .expect("hash column present");
    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
    (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
}

/// The core matrix: int pk, nullable text, nullable timestamp — including a
/// pipe INSIDE a text value, unicode, a pre-epoch second, and an empty (not
/// NULL) string. One batch, five golden rows.
#[test]
fn golden_matrix_int_text_timestamp() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    // 2024-01-01 00:01:00 = 1704067260, 2026-07-29 12:00:00 = 1785326400,
    // 1969-12-31 23:59:59 = -1 (pre-epoch).
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec![
                Some("alice"),
                None,
                Some("py|pe"),
                Some("мир"),
                Some(""),
                Some("x"),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_704_067_260_000_000),
                Some(1_785_326_400_000_000),
                None,
                Some(-1_000_000),
                Some(1_704_067_260_000_000),
                // -0.5s: fractional PRE-EPOCH must floor along the timeline
                // (div_euclid) to -1s — a plain `/` would round toward zero
                // and render 00:00:00, silently diverging from SQL.
                Some(-500_000),
            ])),
        ],
    )
    .unwrap();
    let out = append_content_hash(&batch, &cfg("id", &["status", "updated_at"])).unwrap();
    assert_eq!(
        hashes(&out),
        vec![
            // "1|alice|2024-01-01 00:01:00"
            "ec245aefe160132",
            // "2|<NULL>|2026-07-29 12:00:00"
            "664cf22e67c6fb1",
            // "3|py|pe|<NULL>"  (a pipe inside a value joins flat — the SQL
            // CONCAT_WS counterpart does the same, so parity holds; the
            // canonical text is deliberately NOT injection-proof, it is
            // deliberately IDENTICAL to what SQL renders)
            "18131bb524fd684",
            // "4|мир|1969-12-31 23:59:59" (UTF-8 bytes, pre-epoch second)
            "a5fd53af05a6fd6",
            // "5||2024-01-01 00:01:00" (empty string ≠ NULL sentinel)
            "76a4a9e6a12d5ae",
            // "6|x|1969-12-31 23:59:59" (fractional pre-epoch floors to -1s)
            "3db864887253f10",
        ]
    );
}

/// Unsigned 64-bit pk at its maximum and a negative signed int must render as
/// plain decimal digits (what `CAST(col AS CHAR)` produces everywhere).
#[test]
fn golden_int_extremes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::UInt64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    let out = append_content_hash(&batch, &cfg("pk", &["v"])).unwrap();
    // "18446744073709551615|x"
    assert_eq!(hashes(&out), vec!["90d811ef8e1460c"]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int32, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![-42])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    let out = append_content_hash(&batch, &cfg("pk", &["v"])).unwrap();
    // "-42|x"
    assert_eq!(hashes(&out), vec!["b501f6fa785f5ad"]);
}

/// Timestamp UNIT must not change the hash: the same instant at second /
/// milli / micro / nano precision renders to the same canonical second.
#[test]
fn golden_timestamp_units_agree() {
    let expected = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampSecondArray::from(vec![1_704_067_260])),
            ],
        )
        .unwrap();
        hashes(&append_content_hash(&batch, &cfg("id", &["ts"])).unwrap())
    };
    let milli: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_704_067_260_500]));
    let micro: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_704_067_260_999_999]));
    let nano: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        1_704_067_260_000_000_001,
    ]));
    for (unit, arr) in [
        (TimeUnit::Millisecond, milli),
        (TimeUnit::Microsecond, micro),
        (TimeUnit::Nanosecond, nano),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Timestamp(unit, None), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef, arr],
        )
        .unwrap();
        assert_eq!(
            hashes(&append_content_hash(&batch, &cfg("id", &["ts"])).unwrap()),
            expected,
            "unit {unit:?} drifted from the canonical second rendering"
        );
    }
}

/// A DATE renders with a midnight time part, like every engine's zoneless
/// calendar-day formatter (`to_char(d,'… HH24:MI:SS')`, `CONVERT(…,120)`).
///
/// The pre-epoch case is the one that matters. Date32 counts DAYS, and the
/// natural way to write the widening is a divide — which renders every date as
/// 1970-01-01: a hash that looks entirely ordinary and disagrees with the
/// source on every dated row.
#[test]
fn golden_dates_render_at_midnight_including_pre_epoch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("d", DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![8, 9])),
            // 19723 = 2024-01-01; -1 = 1969-12-31.
            Arc::new(Date32Array::from(vec![19_723, -1])),
        ],
    )
    .unwrap();
    let out = append_content_hash(&batch, &cfg("id", &["d"])).unwrap();
    assert_eq!(
        hashes(&out),
        vec!["291bb751ad05724", "47d634e74ececf1"],
        "a DATE must carry its calendar day, not collapse to the epoch"
    );
}

/// Date64 (milliseconds) must land on the same text as the equivalent Date32 —
/// the source's date WIDTH is an encoding detail, not content.
#[test]
fn golden_date64_agrees_with_date32() {
    let mk = |field: Field, arr: ArrayRef| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            field,
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![8])), arr]).unwrap();
        hashes(&append_content_hash(&batch, &cfg("id", &["d"])).unwrap())[0].clone()
    };
    let d32 = mk(
        Field::new("d", DataType::Date32, false),
        Arc::new(Date32Array::from(vec![19_723])),
    );
    let d64 = mk(
        Field::new("d", DataType::Date64, false),
        Arc::new(Date64Array::from(vec![19_723i64 * 86_400 * 1_000])),
    );
    assert_eq!(d32, d64);
    assert_eq!(d32, "291bb751ad05724");
}
