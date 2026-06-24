//! Opt-in value checksum (`RIVET_VALUE_CHECKSUM=1`) — a cheap, order-independent
//! additive checksum per column, cross-checked source-vs-Arrow in process.
//!
//! **Form A.** An independent source-side pass (per engine — e.g. Postgres'
//! `pg_rows_checksums`) computes a per-column checksum from the raw driver values;
//! [`arrow_batch_checksums`] ("side B") computes the same over the *built* Arrow
//! [`RecordBatch`]; [`verify`] compares them on the spot. A mismatch means the
//! value converter (`build_array`) changed a value between read and Arrow build —
//! caught in process, no re-read. Opt-in + benchmark-gated.
//!
//! A SUM collides and overflows at scale (`wrapping_add` keeps it panic-free), so
//! it is a cheap cross-check — it complements the content-MD5 (byte integrity)
//! and the differential fingerprint (test-time), it does not replace them.
//!
//! Coverage (both sides MUST agree, or the check false-mismatches): int / uint /
//! float / bool / date / timestamp(µs) / utf8 / binary. Decimal128, Time64,
//! FixedSizeBinary (UUID), List, and nanosecond timestamps contribute 0 on BOTH
//! sides for this first cut (not yet matched between the two passes).

use std::sync::OnceLock;

use arrow::array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, UInt64Type,
};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};

use crate::error::Result;

/// `true` when `RIVET_VALUE_CHECKSUM` is set to `1`/`true` (read once).
pub fn enabled() -> bool {
    static CELL: OnceLock<bool> = OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("RIVET_VALUE_CHECKSUM")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    })
}

/// Side B — order-independent additive checksum per column over a built batch.
/// int/uint → value; float → IEEE bits; bool → 0/1; date → days; timestamp(µs) →
/// epoch µs; utf8/binary → byte length. Uncovered types contribute 0 (see the
/// module-level coverage note — the source-side pass must skip exactly the same).
pub fn arrow_batch_checksums(batch: &RecordBatch) -> Vec<i128> {
    batch.columns().iter().map(|c| column_checksum(c)).collect()
}

fn column_checksum(arr: &dyn Array) -> i128 {
    let mut s: i128 = 0;
    macro_rules! sum_prim {
        ($t:ty, $f:expr) => {{
            let a = arr.as_primitive::<$t>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    s = s.wrapping_add($f(a.value(i)));
                }
            }
        }};
    }
    match arr.data_type() {
        DataType::Int16 => sum_prim!(Int16Type, |v| v as i128),
        DataType::Int32 => sum_prim!(Int32Type, |v| v as i128),
        DataType::Int64 => sum_prim!(Int64Type, |v| v as i128),
        DataType::UInt64 => sum_prim!(UInt64Type, |v| v as i128),
        DataType::Float32 => sum_prim!(Float32Type, |v: f32| v.to_bits() as i128),
        DataType::Float64 => sum_prim!(Float64Type, |v: f64| v.to_bits() as i128),
        DataType::Date32 => sum_prim!(Date32Type, |v| v as i128),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            sum_prim!(TimestampMicrosecondType, |v| v as i128)
        }
        DataType::Boolean => {
            let a = arr.as_boolean();
            for i in 0..a.len() {
                if !a.is_null(i) && a.value(i) {
                    s = s.wrapping_add(1);
                }
            }
        }
        DataType::Utf8 => {
            let a = arr.as_string::<i32>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    s = s.wrapping_add(a.value(i).len() as i128);
                }
            }
        }
        DataType::Binary => {
            let a = arr.as_binary::<i32>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    s = s.wrapping_add(a.value(i).len() as i128);
                }
            }
        }
        // Not yet covered (must match the source-side skip) — contribute 0.
        _ => {}
    }
    s
}

/// Compare the source-side ("A") and Arrow-side ("B") checksums column-by-column.
/// A mismatch means the value converter produced a value the source did not — fail
/// loud, naming the column, rather than write the corrupted batch.
pub fn verify(source: &[i128], arrow: &[i128], schema: &SchemaRef) -> Result<()> {
    for (i, (a, b)) in source.iter().zip(arrow.iter()).enumerate() {
        if a != b {
            let col = schema.field(i).name();
            anyhow::bail!(
                "value checksum mismatch in column '{col}': source={a} arrow={b} \
                 — the value converter changed a value between read and Arrow build"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn schema3() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    }

    #[test]
    fn verify_passes_on_equal() {
        assert!(verify(&[1, 2, 3], &[1, 2, 3], &schema3()).is_ok());
    }

    #[test]
    fn verify_fails_on_mismatch_naming_the_column() {
        let err = verify(&[1, 2, 3], &[1, 9, 3], &schema3()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("column 'b'"),
            "must name the diverged column: {msg}"
        );
        assert!(msg.contains("source=2") && msg.contains("arrow=9"), "{msg}");
    }

    #[test]
    fn checksum_distinguishes_a_changed_value_so_verify_fires() {
        // A single corrupted cell must move that column's checksum — proves the
        // cross-check is sensitive to value corruption, not a constant/no-op that
        // would trivially agree. (Guards against the "self-oracle" blind spot:
        // the sanity run only shows it PASSES on correct data; this shows it FIRES
        // when the value converter diverges.)
        let s = schema3();
        let good = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        // simulate build_array corrupting column `b`'s middle row (2 -> 999).
        let bad = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 999, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let cg = arrow_batch_checksums(&good);
        let cb = arrow_batch_checksums(&bad);
        assert_eq!(cg[0], cb[0], "the unchanged column must still match");
        assert_ne!(cg[1], cb[1], "the changed column's checksum must diverge");
        // The "source" (good) vs the corrupted "arrow" (bad) → verify must catch it.
        assert!(
            verify(&cg, &cb, &s).is_err(),
            "verify must fire when a column diverges"
        );
    }
}
