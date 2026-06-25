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
//! The per-column value is `xxh3` of each cell's value **bytes**, XOR-combined
//! (order-independent, so chunk/parallel order doesn't matter). Hashing the bytes
//! — not summing values or lengths — makes it sensitive to content corruption
//! that preserves length or sum (a byte flip, two compensating changes), which the
//! earlier value-sum silently missed. It complements the content-MD5 (file-byte
//! integrity) and the differential fingerprint (test-time); it does not replace
//! them. (A follow-on keys each cell's hash to the row PK — `xxh3(pk ‖ value)` —
//! to also catch row/value swaps, which an order-independent combine still misses.)
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
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

use crate::error::Result;

/// `true` when `RIVET_VALUE_CHECKSUM` is set to `1`/`true` (read once).
pub fn enabled() -> bool {
    static CELL: OnceLock<bool> = OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("RIVET_VALUE_CHECKSUM")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    })
}

/// Side B — order-independent per-column checksum over a built batch: `xxh3` of
/// each cell's **value bytes**, XOR-combined. int/uint/float/decimal128/date/
/// timestamp(µs) → little-endian value bytes; bool → a 0/1 byte; utf8/binary → the
/// raw bytes. Uncovered types (Time64, FixedSizeBinary/UUID, List, ns-timestamps)
/// contribute 0 on BOTH sides. Hashing every cell (vs the old value-sum) makes the
/// check sensitive to **content** corruption that preserves length or sum — a byte
/// flip, or two compensating changes — which a sum/length silently misses.
pub fn arrow_batch_checksums(batch: &RecordBatch) -> Vec<u64> {
    batch.columns().iter().map(|c| column_checksum(c)).collect()
}

fn column_checksum(arr: &dyn Array) -> u64 {
    let mut acc: u64 = 0;
    macro_rules! hash_prim {
        ($t:ty) => {{
            let a = arr.as_primitive::<$t>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    acc ^= xxh3_64(&a.value(i).to_le_bytes());
                }
            }
        }};
    }
    match arr.data_type() {
        DataType::Int16 => hash_prim!(Int16Type),
        DataType::Int32 => hash_prim!(Int32Type),
        DataType::Int64 => hash_prim!(Int64Type),
        DataType::UInt64 => hash_prim!(UInt64Type),
        DataType::Float32 => hash_prim!(Float32Type),
        DataType::Float64 => hash_prim!(Float64Type),
        // The scaled i128 the array holds; the source pass rescales the pg numeric
        // to the same i128 (truncate toward zero) so the bytes — and the hash —
        // match on a correct build and diverge on a scaling bug.
        DataType::Decimal128(..) => hash_prim!(Decimal128Type),
        DataType::Date32 => hash_prim!(Date32Type),
        DataType::Timestamp(TimeUnit::Microsecond, _) => hash_prim!(TimestampMicrosecondType),
        DataType::Boolean => {
            let a = arr.as_boolean();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    acc ^= xxh3_64(&[a.value(i) as u8]);
                }
            }
        }
        DataType::Utf8 => {
            let a = arr.as_string::<i32>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    acc ^= xxh3_64(a.value(i).as_bytes());
                }
            }
        }
        DataType::Binary => {
            let a = arr.as_binary::<i32>();
            for i in 0..a.len() {
                if !a.is_null(i) {
                    acc ^= xxh3_64(a.value(i));
                }
            }
        }
        // Not yet covered (must match the source-side skip) — contribute 0.
        _ => {}
    }
    acc
}

/// Keyed side B — the **swap-resistant** variant. Each cell's contribution is
/// `xxh3(key_value_bytes ‖ cell_value_bytes)` instead of `xxh3(cell_value_bytes)`,
/// so the per-column XOR stays order-independent (chunk/parallel safe) yet a value
/// **swap between two rows** — which preserves the column's multiset and is
/// therefore invisible to the un-keyed XOR — now changes each row's prepended key
/// and diverges. `key_col` is the keyset / incremental cursor column resolved to a
/// schema index. The source pass prepends the SAME key-column bytes (read from the
/// batch it just built), so a correct build keeps A==B and a swap breaks it on both.
///
/// Not yet called from the export hooks — that is the live-pk wiring step (switch
/// `arrow_batch_checksums(&batch)` → `arrow_batch_checksums_keyed(&batch, k)` once
/// the cursor key is threaded through `rows_to_record_batch_typed`, and have each
/// engine's source pass prepend the same key column). Exercised now by
/// `keyed_checksum_catches_a_value_swap_that_unkeyed_misses`.
#[allow(dead_code)]
pub fn arrow_batch_checksums_keyed(batch: &RecordBatch, key_col: usize) -> Vec<u64> {
    let key = batch.column(key_col).as_ref();
    batch
        .columns()
        .iter()
        .map(|c| column_checksum_keyed(c.as_ref(), key))
        .collect()
}

/// Whether a type contributes to the checksum — must mirror the `match` arms in
/// `column_checksum` / `feed_cell` exactly, so an uncovered column reads 0 on BOTH
/// the keyed and un-keyed paths (and so A==B for it).
#[allow(dead_code)]
fn covered(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Date32
            | DataType::Boolean
            | DataType::Utf8
            | DataType::Binary
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

#[allow(dead_code)]
fn column_checksum_keyed(arr: &dyn Array, key: &dyn Array) -> u64 {
    if !covered(arr.data_type()) {
        return 0;
    }
    let mut acc: u64 = 0;
    for r in 0..arr.len() {
        if arr.is_null(r) {
            continue;
        }
        let mut h = Xxh3::new();
        feed_cell(&mut h, key, r);
        feed_cell(&mut h, arr, r);
        acc ^= h.digest();
    }
    acc
}

/// Feed cell `r` of `arr` into `h` as its value bytes — byte-identical to the
/// one-shot `xxh3_64(value_bytes)` the un-keyed path uses (streaming and one-shot
/// XXH3 agree on the same input), so feeding nothing else reproduces the un-keyed
/// hash and feeding the key first produces the keyed one. Uncovered types feed
/// nothing — their column is short-circuited to 0 by `covered`.
#[allow(dead_code)]
fn feed_cell(h: &mut Xxh3, arr: &dyn Array, r: usize) {
    macro_rules! fp {
        ($t:ty) => {
            h.update(&arr.as_primitive::<$t>().value(r).to_le_bytes())
        };
    }
    match arr.data_type() {
        DataType::Int16 => fp!(Int16Type),
        DataType::Int32 => fp!(Int32Type),
        DataType::Int64 => fp!(Int64Type),
        DataType::UInt64 => fp!(UInt64Type),
        DataType::Float32 => fp!(Float32Type),
        DataType::Float64 => fp!(Float64Type),
        DataType::Decimal128(..) => fp!(Decimal128Type),
        DataType::Date32 => fp!(Date32Type),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fp!(TimestampMicrosecondType),
        DataType::Boolean => h.update(&[arr.as_boolean().value(r) as u8]),
        DataType::Utf8 => h.update(arr.as_string::<i32>().value(r).as_bytes()),
        DataType::Binary => h.update(arr.as_binary::<i32>().value(r)),
        _ => {}
    }
}

/// Compare the source-side ("A") and Arrow-side ("B") checksums column-by-column.
/// A mismatch means the value converter produced a value the source did not — fail
/// loud, naming the column, rather than write the corrupted batch.
pub fn verify(source: &[u64], arrow: &[u64], schema: &SchemaRef) -> Result<()> {
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
/// checksum ("side C") BY NAME — keyed to the same cursor column as the export
/// when `key_col_name` is set — comparing each to the value recorded in the
/// manifest's `column_checksums`. Catches an `Arrow→Parquet` encode fault or
/// post-write corruption that the in-process Form A check (source↔Arrow) cannot
/// see. The recorded values are the per-column u64 xxh3 hashes as decimal strings
/// (JSON-stable in the manifest). Wired via [`validate_manifest_checksums`] (the
/// destination entry point) into `rivet validate`.
pub fn validate_recorded_checksums(
    recorded: &[crate::manifest::ColumnChecksum],
    part_paths: &[std::path::PathBuf],
    key_col_name: Option<&str>,
) -> Result<()> {
    use std::collections::BTreeMap;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    // Re-read every part, accumulate side C per column BY NAME — keyed to the same
    // cursor column as the export when `key_col_name` is set, so the keyed hashes
    // match. Name-keyed so a column reorder can't silently misalign the compare.
    let mut actual: BTreeMap<String, u64> = BTreeMap::new();
    for path in part_paths {
        let file = std::fs::File::open(path)
            .map_err(|e| anyhow::anyhow!("value checksum: open {}: {e}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            let batch = batch?;
            let key_col = key_col_name.and_then(|n| batch.schema().index_of(n).ok());
            let sums = match key_col {
                Some(k) => arrow_batch_checksums_keyed(&batch, k),
                None => arrow_batch_checksums(&batch),
            };
            for (i, f) in batch.schema().fields().iter().enumerate() {
                *actual.entry(f.name().clone()).or_insert(0) ^= sums[i];
            }
        }
    }

    // Compare each recorded (data) column to the re-read value by name. Re-read
    // columns absent from `recorded` (enrichment / meta columns) are not compared.
    for rec in recorded {
        let want: u64 = rec.checksum.parse().map_err(|_| {
            anyhow::anyhow!(
                "value checksum: manifest column '{}' hash '{}' is not a u64",
                rec.name,
                rec.checksum
            )
        })?;
        match actual.get(&rec.name) {
            Some(&got) if got == want => {}
            Some(&got) => anyhow::bail!(
                "value checksum mismatch in column '{}': manifest={want} re-read={got} \
                 — the Parquet differs from what was checksummed at write (Arrow→Parquet \
                 encode fault or post-write corruption)",
                rec.name
            ),
            None => anyhow::bail!(
                "value checksum: manifest column '{}' was not found when re-reading the exported parts",
                rec.name
            ),
        }
    }
    Ok(())
}

/// Form B at the destination: read the manifest + every part via `dest`,
/// materialise each part to a temp file (a part may be remote), and verify the
/// re-read per-column checksums against the manifest's `column_checksums`. A
/// no-op (`Ok`) when the manifest records none (older run / non-Parquet). Mirrors
/// [`crate::source::cdc::validate::check_positions`]; called by `rivet validate`
/// for Parquet runs.
pub fn validate_manifest_checksums(
    dest: &dyn crate::destination::Destination,
    prefix: &str,
) -> Result<()> {
    use std::io::Write;

    use crate::manifest::{MANIFEST_FILENAME, RunManifest, join_key};

    let manifest_key = join_key(prefix, MANIFEST_FILENAME);
    let manifest: RunManifest = serde_json::from_slice(&dest.read(&manifest_key)?)?;
    let Some(recorded) = manifest.column_checksums.as_deref() else {
        return Ok(());
    };

    // Materialise each part to a temp file so the Parquet reader can seek (the
    // dest may be a cloud backend); keep the temp files alive for the re-read.
    let mut tmps: Vec<tempfile::NamedTempFile> = Vec::with_capacity(manifest.parts.len());
    for part in &manifest.parts {
        let body = dest.read(&join_key(prefix, &part.path))?;
        let mut tmp = tempfile::NamedTempFile::new()?;
        tmp.write_all(&body)?;
        tmp.flush()?;
        tmps.push(tmp);
    }
    let paths: Vec<std::path::PathBuf> = tmps.iter().map(|t| t.path().to_path_buf()).collect();

    validate_recorded_checksums(recorded, &paths, manifest.checksum_key_column.as_deref())
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
    fn keyed_checksum_catches_a_value_swap_that_unkeyed_misses() {
        // The order-independent XOR's known blind spot: swap two rows' values in a
        // column and the column's multiset is unchanged, so the un-keyed checksum is
        // identical — a corruption it cannot see. Keying each cell to its row's PK
        // (`xxh3(id ‖ value)`) makes the same swap diverge. This test is the proof
        // the keyed variant earns its cost.
        use arrow::array::StringArray;
        let s: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        // id=1 -> "alpha", id=2 -> "beta".
        let id = Int64Array::from(vec![1_i64, 2]);
        let original = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(id.clone()),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            ],
        )
        .unwrap();
        // Same ids, values swapped: id=1 -> "beta", id=2 -> "alpha".
        let swapped = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(id),
                Arc::new(StringArray::from(vec!["beta", "alpha"])),
            ],
        )
        .unwrap();

        // Un-keyed: the `val` column's multiset {"alpha","beta"} is unchanged, so the
        // XOR-combined checksum is blind to the swap.
        let uk = arrow_batch_checksums(&original);
        let uk_swapped = arrow_batch_checksums(&swapped);
        assert_eq!(
            uk[1], uk_swapped[1],
            "un-keyed XOR cannot distinguish a same-multiset row swap"
        );

        // Keyed on `id` (col 0): each value is hashed with its row's id, so the swap
        // diverges — exactly the corruption the un-keyed variant missed.
        let k = arrow_batch_checksums_keyed(&original, 0);
        let k_swapped = arrow_batch_checksums_keyed(&swapped, 0);
        assert_ne!(
            k[1], k_swapped[1],
            "keying to the PK must catch a value swap the un-keyed check missed"
        );
        // The key column itself is identical in both, so its own checksum must not move
        // (and feeding id‖id is deterministic).
        assert_eq!(
            k[0], k_swapped[0],
            "the id column is identical in both batches"
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

    fn recorded_of(batch: &RecordBatch) -> Vec<crate::manifest::ColumnChecksum> {
        arrow_batch_checksums(batch)
            .iter()
            .zip(batch.schema().fields())
            .map(|(v, f)| crate::manifest::ColumnChecksum {
                name: f.name().clone(),
                checksum: v.to_string(),
            })
            .collect()
    }

    #[test]
    fn form_b_validate_passes_on_matching_parts() {
        let batch = batch3(2);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        write_parquet(&batch, &p);
        validate_recorded_checksums(&recorded_of(&batch), &[p], None)
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
        let err = validate_recorded_checksums(&recorded, &[p], None)
            .expect_err("a corrupted re-read must fire");
        assert!(
            err.to_string().contains("column 'b'"),
            "must name the diverged column: {err}"
        );
    }
}
