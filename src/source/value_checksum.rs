//! Always-on value checksum — a cheap, order-independent per-column `xxh3`,
//! cross-checked at two stages.
//!
//! **Form A (in-process).** An independent source-side pass (per engine — e.g.
//! Postgres' `pg_rows_checksums`) computes a per-column checksum from the raw
//! driver values; [`arrow_batch_checksums`] ("side B") computes the same over the
//! *built* Arrow [`RecordBatch`]; [`verify`] compares them on the spot. A mismatch
//! means the value converter (`build_array`) changed a value between read and Arrow
//! build — caught in process, no re-read. Always-on (~+7% CPU, ~0% wall on the
//! I/O-bound export path).
//!
//! **Form B (validate-time).** The sink records the per-column checksum in the
//! manifest (name-keyed [`crate::manifest::ColumnChecksum`]); `rivet validate`
//! re-reads the Parquet and recompares ([`validate_manifest_checksums`]), catching
//! an `Arrow→Parquet` encode / post-write fault Form A cannot see.
//!
//! The per-column value is `xxh3` of each cell's value **bytes**, XOR-combined
//! (order-independent, so chunk/parallel order doesn't matter) — keyed to the row
//! cursor/key column when present (`xxh3(key ‖ value)`, swap-resistant). Hashing
//! the bytes — not summing values or lengths — makes it sensitive to content
//! corruption that preserves length or sum (a byte flip, two compensating changes)
//! the earlier value-sum silently missed. It complements the content-MD5 (file-byte
//! integrity) and the differential fingerprint (test-time).
//!
//! Coverage (both sides MUST agree, or the check false-mismatches): int / uint /
//! float / bool / date / timestamp(µs) / time64(µs) / utf8 / binary / fixed-size
//! binary (UUID, BINARY(n)) / decimal128 / decimal256 / one-dimensional lists
//! over {bool, i16..i64, f32, f64, utf8}. Nanosecond timestamps and exotic list
//! elements contribute 0 on BOTH sides (declared skips, surfaced per export).

use std::borrow::Cow;

use arrow::array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, TimestampMicrosecondType, UInt64Type,
};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

use crate::error::Result;

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
    column_xxh3(arr, None)
}

/// Side B for ONE column — the CDC sink verifies per column as it builds (the
/// batch path verifies whole batches via [`arrow_batch_checksums`]).
pub fn array_checksum(arr: &dyn Array) -> u64 {
    column_xxh3(arr, None)
}

/// What the value checksum does with a column of a given Arrow type — the single
/// declared source of truth for coverage, consulted by both the keyed and un-keyed
/// paths and by [`coverage_skips`]. Adding a covered type is one edit here plus the
/// matching extraction arm in [`feed_cell`] (a unit test pins the two together); an
/// uncovered type is a DECLARED skip whose reason is surfaced (logged once per
/// export) — never a silent zero that reads as "verified".
enum CheckRule {
    /// Covered — [`feed_cell`] knows how to hash this type's value bytes.
    Covered,
    /// Not yet matched between the source pass and the Arrow pass; contributes 0 on
    /// BOTH sides (so A==B holds for it). Reason surfaced via [`coverage_skips`].
    Skipped(&'static str),
}

fn check_rule(dt: &DataType) -> CheckRule {
    match dt {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::FixedSizeBinary(_)
        | DataType::Boolean
        | DataType::Utf8
        | DataType::Binary => CheckRule::Covered,
        // One-dimensional lists over the element set the exporters produce.
        DataType::List(f) if list_elem_covered(f.data_type()) => CheckRule::Covered,
        DataType::List(_) | DataType::LargeList(_) => {
            CheckRule::Skipped("List element type not matched")
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            CheckRule::Skipped("nanosecond timestamp not yet matched")
        }
        _ => CheckRule::Skipped("type not covered by the value checksum"),
    }
}

fn list_elem_covered(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
    )
}

/// One element of a list cell, as its source pass hands it over — the shared
/// vocabulary both sides encode through [`encode_list_cell`], so the two passes
/// cannot drift on list byte layout.
pub enum ListElem {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(Vec<u8>),
}

/// Canonical byte encoding of ONE list cell: per element a tag byte, then a
/// little-endian u32 length, then the value bytes (empty for `Null`). Order
/// matters INSIDE a cell (`[a,b] ≠ [b,a]`), unlike the per-column XOR across
/// cells. Both [`with_cell_bytes`] (Arrow side) and every engine's source pass
/// encode through this one function.
pub fn encode_list_cell(elems: &[ListElem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(elems.len() * 8);
    let mut put = |tag: u8, bytes: &[u8]| {
        out.push(tag);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    };
    for e in elems {
        match e {
            ListElem::Null => put(0, &[]),
            ListElem::Bool(v) => put(1, &[*v as u8]),
            ListElem::I16(v) => put(2, &v.to_le_bytes()),
            ListElem::I32(v) => put(3, &v.to_le_bytes()),
            ListElem::I64(v) => put(4, &v.to_le_bytes()),
            ListElem::F32(v) => put(5, &v.to_le_bytes()),
            ListElem::F64(v) => put(6, &v.to_le_bytes()),
            ListElem::Str(b) => put(7, b),
        }
    }
    out
}

/// Cell `r` of a `List` array as [`ListElem`]s — the Arrow-side twin of the
/// engines' list accessors, feeding the same [`encode_list_cell`] canon.
fn list_cell_elems(arr: &dyn Array, r: usize) -> Vec<ListElem> {
    use arrow::array::ListArray;
    let list = arr.as_any().downcast_ref::<ListArray>().expect("List");
    let v = list.value(r);
    let inner = v.as_ref();
    (0..inner.len())
        .map(|i| {
            if inner.is_null(i) {
                return ListElem::Null;
            }
            match inner.data_type() {
                DataType::Boolean => ListElem::Bool(inner.as_boolean().value(i)),
                DataType::Int16 => ListElem::I16(inner.as_primitive::<Int16Type>().value(i)),
                DataType::Int32 => ListElem::I32(inner.as_primitive::<Int32Type>().value(i)),
                DataType::Int64 => ListElem::I64(inner.as_primitive::<Int64Type>().value(i)),
                DataType::Float32 => ListElem::F32(inner.as_primitive::<Float32Type>().value(i)),
                DataType::Float64 => ListElem::F64(inner.as_primitive::<Float64Type>().value(i)),
                DataType::Utf8 => {
                    ListElem::Str(inner.as_string::<i32>().value(i).as_bytes().to_vec())
                }
                _ => ListElem::Null, // unreachable: gated by list_elem_covered
            }
        })
        .collect()
}

/// The single per-column hasher behind both [`arrow_batch_checksums`] (un-keyed,
/// `key = None`) and [`arrow_batch_checksums_keyed`] (swap-resistant, `key = Some`).
/// Each non-null cell hashes `xxh3(key_bytes ‖ value_bytes)` — or just the value
/// bytes when un-keyed — via [`feed_cell`], XOR-combined (order-independent, so
/// chunk/parallel order is irrelevant). A [`CheckRule::Skipped`] column
/// short-circuits to 0 on both paths.
fn column_xxh3(arr: &dyn Array, key: Option<&dyn Array>) -> u64 {
    if matches!(check_rule(arr.data_type()), CheckRule::Skipped(_)) {
        return 0;
    }
    let mut acc: u64 = 0;
    for r in 0..arr.len() {
        if arr.is_null(r) {
            continue;
        }
        match key {
            // Un-keyed: one-shot `xxh3_64` — ~2.5× faster than constructing a
            // streaming `Xxh3` per cell (which inits the full ~576-byte state).
            None => acc ^= with_cell_bytes(arr, r, xxh3_64),
            // Keyed: stream `key ‖ value` so the swap-resistant hash needs no
            // concat alloc; same hash value as one-shot over the concatenation.
            Some(k) => {
                let d = with_cell_bytes(k, r, |kb| {
                    with_cell_bytes(arr, r, |vb| {
                        let mut h = Xxh3::new();
                        h.update(kb);
                        h.update(vb);
                        h.digest()
                    })
                });
                acc ^= d;
            }
        }
    }
    acc
}

/// Whether this Arrow type participates in the value checksum (both sides hash
/// it) — the CDC sink's independent fold consults this so its skip set can
/// never drift from the batch pass's.
pub fn is_covered(dt: &DataType) -> bool {
    matches!(check_rule(dt), CheckRule::Covered)
}

/// The columns the value checksum does NOT cover, each with the reason — so a
/// `UUID` / `List` / `Decimal256` / ns-timestamp column yields a visible "not
/// checked" line (logged once per export by the sink) instead of a silent zero that
/// reads as "verified". Empty ⇒ full coverage.
pub fn coverage_skips(schema: &SchemaRef) -> Vec<(String, &'static str)> {
    schema
        .fields()
        .iter()
        .filter_map(|f| match check_rule(f.data_type()) {
            CheckRule::Skipped(reason) => Some((f.name().clone(), reason)),
            CheckRule::Covered => None,
        })
        .collect()
}

/// Keyed side B — the **swap-resistant** variant. Each cell's contribution is
/// `xxh3(key_value_bytes ‖ cell_value_bytes)` instead of `xxh3(cell_value_bytes)`,
/// so the per-column XOR stays order-independent (chunk/parallel safe) yet a value
/// **swap between two rows** — invisible to the un-keyed XOR because it preserves
/// the column's multiset — now changes each row's prepended key and diverges.
/// `key_col` is the keyset / incremental cursor column resolved to a schema index.
/// Used by the sink's Form B recording; the source pass prepends the SAME key-column
/// bytes, so a correct build keeps A==B and a swap breaks it on both.
pub fn arrow_batch_checksums_keyed(batch: &RecordBatch, key_col: usize) -> Vec<u64> {
    let key = batch.column(key_col).as_ref();
    batch
        .columns()
        .iter()
        .map(|c| column_xxh3(c.as_ref(), Some(key)))
        .collect()
}

/// Hand cell `r` of `arr` to `f` as its canonical value bytes — the single per-type
/// byte extraction behind [`column_xxh3`]. The un-keyed path one-shots
/// `xxh3_64(bytes)` (fast); the keyed path feeds two calls (`key ‖ value`) into a
/// streaming `Xxh3`. The `_` arm is unreachable for a (covered) cell —
/// [`column_xxh3`] short-circuits `Skipped` first — but a Skipped KEY column (e.g. a
/// UUID cursor) lands there and feeds an empty key; a unit test pins the covered set
/// to [`check_rule`].
fn with_cell_bytes<R>(arr: &dyn Array, r: usize, f: impl FnOnce(&[u8]) -> R) -> R {
    macro_rules! fp {
        ($t:ty) => {
            f(&arr.as_primitive::<$t>().value(r).to_le_bytes())
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
        DataType::Decimal256(..) => f(&arr.as_primitive::<Decimal256Type>().value(r).to_le_bytes()),
        DataType::Date32 => fp!(Date32Type),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fp!(TimestampMicrosecondType),
        DataType::Time64(TimeUnit::Microsecond) => fp!(Time64MicrosecondType),
        DataType::FixedSizeBinary(_) => f(arr.as_fixed_size_binary().value(r)),
        DataType::Boolean => f(&[arr.as_boolean().value(r) as u8]),
        DataType::Utf8 => f(arr.as_string::<i32>().value(r).as_bytes()),
        DataType::Binary => f(arr.as_binary::<i32>().value(r)),
        DataType::List(_) => f(&encode_list_cell(&list_cell_elems(arr, r))),
        _ => f(&[]),
    }
}

/// A per-engine, INDEPENDENT source of typed cell values — a second decode of the
/// same rows, the side-A twin of side B's Arrow read. Each accessor returns the
/// cell's canonical value (the generic [`source_checksums`] byte-encodes it
/// identically to [`with_cell_bytes`]) or `None` for a NULL / absent cell; the
/// variable-length `utf8`/`binary` return the bytes as a `Cow` — borrowed for plain
/// text / driver bytes, owned for a computed string (uuid, interval, enum label,
/// numeric-as-text). A new covered type is a new required method here — a compile
/// requirement on all three engines, the point of one dispatch instead of three.
pub trait CellSource {
    fn num_rows(&self) -> usize;
    fn int16(&self, col: usize, row: usize) -> Option<i16>;
    fn int32(&self, col: usize, row: usize) -> Option<i32>;
    fn int64(&self, col: usize, row: usize) -> Option<i64>;
    fn uint64(&self, col: usize, row: usize) -> Option<u64>;
    fn float32(&self, col: usize, row: usize) -> Option<f32>;
    fn float64(&self, col: usize, row: usize) -> Option<f64>;
    fn decimal128(&self, col: usize, row: usize, scale: i8) -> Option<i128>;
    fn date32(&self, col: usize, row: usize) -> Option<i32>;
    fn ts_micros(&self, col: usize, row: usize) -> Option<i64>;
    fn boolean(&self, col: usize, row: usize) -> Option<bool>;
    fn utf8(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>>;
    fn binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>>;
    /// Microseconds since midnight (`TIME` → `Time64(µs)`).
    fn time64_micros(&self, col: usize, row: usize) -> Option<i64>;
    /// The exact bytes of a `FixedSizeBinary` cell (UUID's 16, `BINARY(n)`'s n).
    fn fixed_binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>>;
    /// Unscaled `i256` of a `Decimal256(_, scale)` cell (precision > 38).
    fn decimal256(&self, col: usize, row: usize, scale: i8) -> Option<arrow::datatypes::i256>;
    /// A list cell's elements — encoded via [`encode_list_cell`] by the
    /// dispatcher, so engines only extract, never encode. Engines whose type
    /// system has no arrays return `None` unconditionally.
    fn list(&self, col: usize, row: usize, elem: &DataType) -> Option<Vec<ListElem>>;
}

/// Side A — the per-column checksum computed **independently** from a [`CellSource`]
/// (the raw driver values), the single dispatch behind every engine's source pass.
/// Dispatches on the target Arrow type, byte-encodes each non-null cell **identically
/// to [`feed_cell`]** (so a correct build keeps A==B and the matrix guard fires on
/// any drift), XOR-combined. Un-keyed: the in-process Form A check is un-keyed today.
pub fn source_checksums<S: CellSource>(schema: &SchemaRef, src: &S) -> Vec<u64> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col, field)| {
            let dt = field.data_type();
            if matches!(check_rule(dt), CheckRule::Skipped(_)) {
                return 0;
            }
            let mut acc: u64 = 0;
            for row in 0..src.num_rows() {
                // One-shot `xxh3_64` per non-null cell — un-keyed (Form A is
                // un-keyed), byte-encoded identically to `with_cell_bytes`.
                macro_rules! le {
                    ($opt:expr) => {
                        if let Some(v) = $opt {
                            acc ^= xxh3_64(&v.to_le_bytes());
                        }
                    };
                }
                match dt {
                    DataType::Int16 => le!(src.int16(col, row)),
                    DataType::Int32 => le!(src.int32(col, row)),
                    DataType::Int64 => le!(src.int64(col, row)),
                    DataType::UInt64 => le!(src.uint64(col, row)),
                    DataType::Float32 => le!(src.float32(col, row)),
                    DataType::Float64 => le!(src.float64(col, row)),
                    DataType::Decimal128(_, scale) => le!(src.decimal128(col, row, *scale)),
                    DataType::Date32 => le!(src.date32(col, row)),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => le!(src.ts_micros(col, row)),
                    DataType::Boolean => {
                        if let Some(b) = src.boolean(col, row) {
                            acc ^= xxh3_64(&[b as u8]);
                        }
                    }
                    DataType::Utf8 => {
                        if let Some(b) = src.utf8(col, row) {
                            acc ^= xxh3_64(b.as_ref());
                        }
                    }
                    DataType::Binary => {
                        if let Some(b) = src.binary(col, row) {
                            acc ^= xxh3_64(b.as_ref());
                        }
                    }
                    DataType::Time64(TimeUnit::Microsecond) => {
                        le!(src.time64_micros(col, row))
                    }
                    DataType::FixedSizeBinary(_) => {
                        if let Some(b) = src.fixed_binary(col, row) {
                            acc ^= xxh3_64(b.as_ref());
                        }
                    }
                    DataType::Decimal256(_, scale) => {
                        if let Some(v) = src.decimal256(col, row, *scale) {
                            acc ^= xxh3_64(&v.to_le_bytes());
                        }
                    }
                    DataType::List(f) => {
                        if let Some(elems) = src.list(col, row, f.data_type()) {
                            acc ^= xxh3_64(&encode_list_cell(&elems));
                        }
                    }
                    _ => {}
                }
            }
            acc
        })
        .collect()
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
    fn coverage_skips_names_uncovered_columns_and_they_contribute_zero() {
        use arrow::array::TimestampNanosecondArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        // The skip is declared + named, not silent.
        let skips = coverage_skips(&schema);
        assert_eq!(
            skips.len(),
            1,
            "exactly the ns-timestamp column is uncovered: {skips:?}"
        );
        assert_eq!(skips[0].0, "t");
        assert!(
            skips[0].1.contains("nanosecond"),
            "reason names the type: {}",
            skips[0].1
        );
        // ...and it short-circuits to 0 (feed_cell's skip set == check_rule's
        // Skipped), so an uncovered column can never false-mismatch (0 on both sides).
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(TimestampNanosecondArray::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let sums = arrow_batch_checksums(&batch);
        assert_ne!(sums[0], 0, "covered Int64 contributes");
        assert_eq!(sums[1], 0, "uncovered ns-timestamp contributes 0");
    }

    #[test]
    fn newly_covered_types_contribute_and_detect_corruption() {
        use arrow::array::{Decimal256Array, FixedSizeBinaryArray, Time64MicrosecondArray};
        use arrow::datatypes::i256;
        let schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("u", DataType::FixedSizeBinary(16), true),
            Field::new("d", DataType::Decimal256(50, 10), true),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        assert!(
            coverage_skips(&schema).is_empty(),
            "all four newly-covered types must be Covered"
        );
        let mk = |tval: i64, uuid_last: u8, dval: i128, s: &str| {
            use arrow::array::StringBuilder;
            let mut lb = arrow::array::ListBuilder::new(StringBuilder::new())
                .with_field(Arc::new(Field::new("item", DataType::Utf8, true)));
            lb.values().append_value(s);
            lb.values().append_null();
            lb.append(true);
            let mut ub = [7u8; 16];
            ub[15] = uuid_last;
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Time64MicrosecondArray::from(vec![tval])),
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(vec![ub.to_vec()].into_iter()).unwrap(),
                    ),
                    Arc::new(
                        Decimal256Array::from(vec![i256::from_i128(dval)])
                            .with_precision_and_scale(50, 10)
                            .unwrap(),
                    ),
                    Arc::new(lb.finish()) as Arc<dyn Array>,
                ],
            )
            .unwrap()
        };
        let good = arrow_batch_checksums(&mk(1000, 1, 42, "alpha"));
        for (i, bad) in [
            arrow_batch_checksums(&mk(1001, 1, 42, "alpha")),
            arrow_batch_checksums(&mk(1000, 2, 42, "alpha")),
            arrow_batch_checksums(&mk(1000, 1, 43, "alpha")),
            arrow_batch_checksums(&mk(1000, 1, 42, "alphb")),
        ]
        .into_iter()
        .enumerate()
        {
            assert_ne!(good[i], bad[i], "column {i}: corruption must move the sum");
            assert_ne!(good[i], 0, "column {i}: covered type must contribute");
        }
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
