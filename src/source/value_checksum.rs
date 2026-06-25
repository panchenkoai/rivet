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
//! float / bool / date / timestamp(µs) / utf8 / binary / decimal128. Time64,
//! FixedSizeBinary (UUID), List, and nanosecond timestamps contribute 0 on BOTH
//! sides for now (not yet matched between the two passes).

use std::sync::OnceLock;

use arrow::array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
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
        // The scaled i128 the array already holds; the source pass rescales the pg
        // numeric the same way (truncate toward zero) so a build-time scaling bug
        // shows as a mismatch.
        DataType::Decimal128(..) => sum_prim!(Decimal128Type, |v| v),
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

/// Form B: re-read the exported Parquet parts and recompute the per-column
/// checksum ("side C"), comparing it to the value recorded in the manifest's
/// `column_checksums`. Catches an `Arrow→Parquet` encode fault or post-write
/// corruption that the in-process Form A check (source↔Arrow) cannot see. The
/// recorded sums are i128 decimal strings (JSON-stable in the manifest).
///
/// The manifest field + this re-read entry are in place and unit-tested (it
/// catches a corrupted re-read); the call site in the validate command is the
/// remaining Form B recording/validate wiring.
#[allow(dead_code)]
pub fn validate_recorded_checksums(
    recorded: &[String],
    part_paths: &[std::path::PathBuf],
) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut actual: Vec<i128> = Vec::new();
    let mut schema: Option<SchemaRef> = None;
    for path in part_paths {
        let file = std::fs::File::open(path)
            .map_err(|e| anyhow::anyhow!("value checksum: open {}: {e}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            let batch = batch?;
            if actual.is_empty() {
                schema = Some(batch.schema());
                actual = vec![0i128; batch.num_columns()];
            }
            for (i, c) in arrow_batch_checksums(&batch).into_iter().enumerate() {
                if i < actual.len() {
                    actual[i] = actual[i].wrapping_add(c);
                }
            }
        }
    }

    if recorded.len() != actual.len() {
        anyhow::bail!(
            "value checksum: manifest recorded {} columns, re-read found {}",
            recorded.len(),
            actual.len()
        );
    }
    for (i, (rec, act)) in recorded.iter().zip(actual.iter()).enumerate() {
        let rec_i: i128 = rec.parse().map_err(|_| {
            anyhow::anyhow!("value checksum: manifest column {i} sum '{rec}' is not an integer")
        })?;
        if rec_i != *act {
            let col = schema
                .as_ref()
                .and_then(|s| s.fields().get(i))
                .map(|f| f.name().clone())
                .unwrap_or_else(|| format!("#{i}"));
            anyhow::bail!(
                "value checksum mismatch in column '{col}': manifest={rec_i} re-read={act} \
                 — the Parquet differs from what was checksummed at write (encode fault or corruption)"
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

    #[test]
    fn decimal128_checksum_distinguishes_a_scaling_bug() {
        // The decimal arm must be sensitive too: a build-time scaling bug (12.34
        // scaled to 1234, but built as 123) must move the column checksum and fire
        // verify — not pass silently.
        use arrow::array::Decimal128Array;
        let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 2),
            true,
        )]));
        let mk = |mid: i128| {
            RecordBatch::try_new(
                s.clone(),
                vec![Arc::new(
                    Decimal128Array::from(vec![1234_i128, mid, -99])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                )],
            )
            .unwrap()
        };
        let good = mk(5000); // 50.00
        let bad = mk(500); //  5.00 — a 10x scaling slip
        let cg = arrow_batch_checksums(&good);
        let cb = arrow_batch_checksums(&bad);
        assert_ne!(cg[0], cb[0], "a decimal scaling bug must move the checksum");
        assert!(
            verify(&cg, &cb, &s).is_err(),
            "verify must fire on the decimal divergence"
        );
    }

    fn write_parquet(batch: &RecordBatch, path: &std::path::Path) {
        let file = std::fs::File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        w.write(batch).unwrap();
        w.close().unwrap();
    }

    fn batch3(b_mid: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema3(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, b_mid, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap()
    }

    fn recorded_of(batch: &RecordBatch) -> Vec<String> {
        arrow_batch_checksums(batch)
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    #[test]
    fn form_b_validate_passes_on_matching_parts() {
        let batch = batch3(2);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch, &p);
        validate_recorded_checksums(&recorded_of(&batch), &[p])
            .expect("matching parts must validate");
    }

    #[test]
    fn form_b_validate_fires_on_corrupted_part() {
        // The manifest recorded the checksum of the correct batch (col `b` mid = 2)...
        let recorded = recorded_of(&batch3(2));
        // ...but the Parquet on disk holds a corrupted `b` (2 -> 999).
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch3(999), &p);
        let err = validate_recorded_checksums(&recorded, &[p])
            .expect_err("a corrupted re-read must fire");
        assert!(
            err.to_string().contains("column 'b'"),
            "must name the diverged column: {err}"
        );
    }
}
