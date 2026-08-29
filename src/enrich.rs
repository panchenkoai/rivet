use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::config::{MetaColumns, RowHash};
use crate::error::Result;

pub const COL_EXPORTED_AT: &str = "_rivet_exported_at";
pub const COL_ROW_HASH: &str = "_rivet_row_hash";

/// The identity of the row hash's rendering, recorded next to the data.
///
/// Any change to how the canonical bytes are built — the field framing, the NULL
/// marker, Arrow's display formatting, the digest, the truncation to `i64` —
/// produces different values for the same rows. A reader that recomputes the
/// hash must be able to tell "this column was written by a rendering I know"
/// from "…by one I do not", and refuse rather than compare different bytes.
/// Bump this whenever the rendering changes, and never reuse an old one.
///
/// `v1` was not injective, in two independent ways, both RED-proven:
///
/// * Fields were joined by a bare `\x1f` with no length, so a value could forge
///   a boundary — `("a\x1f", "b")` and `("a", "\x1fb")` built the same bytes —
///   and a bare `\x00` NULL marker let a value forge absence.
/// * Container cells were hashed by Arrow's DISPLAY text, which is lossy: it
///   joins list elements with `", "` and renders a NULL element as the empty
///   string, so `["a, b"]` and `["a","b"]` were one value, as were `[NULL]`,
///   `[""]` and `[]`.
///
/// `v2` length-prefixes every field AND builds a container's form from its
/// children (element count, then each element framed the same way) instead of
/// from its rendered text, so no value can imitate a boundary, absence, or a
/// different shape at any depth.
pub const ROW_HASH_RENDER_ID: &str = "xxh3-128-i64-arrow-display-us-v2";

/// What a run's [`COL_ROW_HASH`] column actually covers, travelling with the
/// data in the run manifest and the journal.
///
/// Without it a reader can only probe that the column EXISTS, and existence
/// says nothing about coverage: a hash over `(id, status)` compared against a
/// re-extraction of `(id, status, amount)` is a valid-looking column that
/// silently attests a different text.
///
/// The coverage is recorded as the SPEC, not as a resolved list. `row_hash:
/// true` means "every column this part projects", and the part's schema is in
/// the part — resolving it here would duplicate a fact the data already
/// carries, and would go stale the moment the projection changes.
#[derive(
    Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema, Clone, PartialEq, Eq,
)]
pub struct RowHashContract {
    /// The materialized column's name (always [`COL_ROW_HASH`] today; recorded
    /// rather than assumed so a reader never has to guess).
    pub column: String,
    /// The declared coverage, exactly as configured.
    pub covered: RowHash,
    /// The rendering's identity — see [`ROW_HASH_RENDER_ID`].
    pub render: String,
}

impl RowHashContract {
    /// The contract for a run's `meta_columns.row_hash`, or `None` when no hash
    /// column is written.
    pub fn of(spec: &RowHash) -> Option<Self> {
        spec.enabled().then(|| Self {
            column: COL_ROW_HASH.to_string(),
            covered: spec.clone(),
            render: ROW_HASH_RENDER_ID.to_string(),
        })
    }
}

/// Extend an Arrow schema with the columns rivet adds.
///
/// [`enrich_batch`] builds its arrays in this same order — the two must not be
/// able to disagree, which is why both live here rather than being appended by
/// each caller.
///
/// Fallible because `row_hash` may name a column this export does not project;
/// that refusal belongs at schema time, before a single row is read.
pub fn enrich_schema(schema: &SchemaRef, meta: &MetaColumns) -> Result<SchemaRef> {
    if !meta.exported_at && !meta.row_hash.enabled() && meta.cdc_snapshot_pos.is_none() {
        return Ok(schema.clone());
    }
    // Resolved for its refusal, not its result: a name outside the projection
    // must fail the run here rather than on the first batch.
    row_hash_columns(schema, &meta.row_hash)?;
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    if meta.exported_at {
        fields.push(Arc::new(Field::new(
            COL_EXPORTED_AT,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )));
    }
    if meta.cdc_snapshot_pos.is_some() {
        // The snapshot leg's anchor stamp (round-10): constant per run, typed
        // exactly as the drain's columns so union_by_name lines them up.
        fields.push(Arc::new(Field::new("__pos", DataType::Utf8, false)));
        fields.push(Arc::new(Field::new("__seq", DataType::Int64, false)));
    }
    if meta.row_hash.enabled() {
        fields.push(Arc::new(Field::new(COL_ROW_HASH, DataType::Int64, false)));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Add rivet's columns to a RecordBatch, in [`enrich_schema`]'s order.
/// `exported_at_us` is a single microsecond-precision UTC timestamp shared by all rows.
pub fn enrich_batch(
    batch: &RecordBatch,
    meta: &MetaColumns,
    enriched_schema: &SchemaRef,
    exported_at_us: i64,
) -> Result<RecordBatch> {
    if !meta.exported_at && !meta.row_hash.enabled() && meta.cdc_snapshot_pos.is_none() {
        return Ok(batch.clone());
    }

    let n = batch.num_rows();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    if meta.exported_at {
        let ts_array =
            TimestampMicrosecondArray::from(vec![Some(exported_at_us); n]).with_timezone("UTC");
        columns.push(Arc::new(ts_array));
    }

    if let Some(pos) = meta.cdc_snapshot_pos.as_deref() {
        // Order mirrors enrich_schema: __pos, __seq BEFORE row_hash.
        columns.push(Arc::new(arrow::array::StringArray::from(vec![
            Some(pos);
            n
        ])));
        columns.push(Arc::new(arrow::array::Int64Array::from(vec![-1i64; n])));
    }

    if meta.row_hash.enabled() {
        // Resolved per batch rather than threaded from `enrich_schema`: the
        // lookup is one name scan per covered column, and keeping the two
        // functions independent means neither can quietly drift from the
        // other's idea of coverage.
        let names = row_hash_columns(&batch.schema(), &meta.row_hash)?;
        columns.push(row_hash_array(batch, &names)?);
    }

    Ok(RecordBatch::try_new(enriched_schema.clone(), columns)?)
}

/// The columns `_rivet_row_hash` covers, resolved against a real schema.
///
/// `row_hash: true` resolves to every column in projection order — what the
/// column has always meant. A declared list resolves to itself, and a name
/// that is not in the projection FAILS here rather than being skipped: a
/// skipped column would produce hashes that agree while attesting content
/// that was never hashed, which is the quietest way this feature could lie.
pub fn row_hash_columns(schema: &SchemaRef, spec: &RowHash) -> Result<Vec<String>> {
    let available: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    row_hash_columns_of(&available, spec)
}

/// [`row_hash_columns`] over a bare name list — for the CDC seam, where the
/// DATA columns are known before any Arrow schema exists, and where "all
/// columns" must mean all *data* columns.
///
/// That distinction is the whole reason this overload exists. The CDC sink's
/// schema also carries `__op`/`__pos`/`__seq`, which the snapshot leg does not
/// have; folding them in would give the two legs different hashes for the same
/// row, and they write the same `__changes` log.
pub fn row_hash_columns_of(available: &[String], spec: &RowHash) -> Result<Vec<String>> {
    let Some(declared) = spec.declared() else {
        return Ok(available.to_vec());
    };
    for name in declared {
        if !available.iter().any(|a| a == name) {
            anyhow::bail!(
                "meta_columns.row_hash names column '{name}', which this export does not \
                 project. Available: {}",
                available.join(", ")
            );
        }
    }
    Ok(declared.to_vec())
}

/// The `_rivet_row_hash` column for a batch, over an already-resolved column
/// set. Both sinks compute it here so neither can drift from the other's
/// rendering — the value's only guarantee is that one function produces it.
pub fn row_hash_array(batch: &RecordBatch, cols: &[String]) -> Result<ArrayRef> {
    let idx: Vec<usize> = cols
        .iter()
        .map(|c| {
            batch.schema().index_of(c).map_err(|_| {
                anyhow::anyhow!("row_hash: column '{c}' vanished from the batch schema")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    hash_column(batch, batch.num_rows(), &idx).map(|a| Arc::new(a) as ArrayRef)
}

/// Absent value at any depth. Distinct from a PRESENT value of length zero,
/// which is [`TAG_PRESENT`] with a zero length — that difference is the whole
/// reason the tag is separate from the content.
const TAG_NULL: u8 = 0x00;
const TAG_PRESENT: u8 = 0x01;

/// Whether a type is a CONTAINER, whose canonical form must be built from its
/// children rather than from its own display text.
///
/// Arrow's display for a container is lossy: it joins list elements with `", "`
/// and renders a NULL element as the EMPTY string, so `["a, b"]` and
/// `["a","b"]` both render `[a, b]`, and `[NULL]`, `[""]` and `[]` all render
/// `[]`. Hashing that text would attest that different arrays are the same
/// array. Reachable, not theoretical: a PostgreSQL `text[]` becomes
/// `DataType::List` (`types/mapping.rs`, `RivetType::List`).
///
/// Listed explicitly rather than with a `_` fallthrough so a new Arrow container
/// type is a COMPILE error to classify, not a silent "scalar, safe to render" —
/// the failure mode here is a hash that quietly attests equality.
fn is_container(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

/// Append one LEAF cell: presence tag, then the rendered length, then the bytes.
///
/// The length is what makes the encoding injective — a bare separator lets a
/// value imitate a field boundary (`("a\x1f","b")` vs `("a","\x1fb")` built the
/// same bytes under render id v1), and a bare NULL marker lets a value imitate
/// absence (NULL vs a lone `\x00`). Both were RED-proven collisions.
fn write_leaf(
    buf: &mut Vec<u8>,
    array: &dyn arrow::array::Array,
    fmt: &arrow::util::display::ArrayFormatter,
    row: usize,
) {
    use std::io::Write as IoWrite;
    if array.is_null(row) {
        buf.push(TAG_NULL);
        return;
    }
    buf.push(TAG_PRESENT);
    // Placeholder, back-filled once written: the formatter renders straight into
    // `buf`, so the length is not known before the write.
    let len_at = buf.len();
    buf.extend_from_slice(&0u32.to_le_bytes());
    // Infallible: `buf` is a `Vec`, whose `io::Write` never errors.
    let _ = write!(buf, "{}", fmt.value(row));
    let len = (buf.len() - len_at - 4) as u32;
    buf[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
}

/// Append the canonical form of one cell, recursing into containers.
///
/// `name` is carried only for the error message, so a refusal names the column
/// the operator wrote rather than an anonymous child field.
fn write_canon(
    buf: &mut Vec<u8>,
    array: &dyn arrow::array::Array,
    row: usize,
    name: &str,
    options: &arrow::util::display::FormatOptions,
) -> Result<()> {
    use arrow::array::{FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray};

    if !is_container(array.data_type()) {
        let fmt = leaf_formatter(array, name, options)?;
        write_leaf(buf, array, &fmt, row);
        return Ok(());
    }
    if array.is_null(row) {
        // A NULL container is absence, distinct from an EMPTY one (which is
        // PRESENT with element count 0) — the `[NULL]`/`[""]`/`[]` collision
        // family is exactly this distinction being lost.
        buf.push(TAG_NULL);
        return Ok(());
    }
    buf.push(TAG_PRESENT);
    match array.data_type() {
        DataType::List(_) => {
            let a = downcast::<ListArray>(array, name)?;
            write_elements(buf, a.value(row).as_ref(), name, options)
        }
        DataType::LargeList(_) => {
            let a = downcast::<LargeListArray>(array, name)?;
            write_elements(buf, a.value(row).as_ref(), name, options)
        }
        DataType::FixedSizeList(_, _) => {
            let a = downcast::<FixedSizeListArray>(array, name)?;
            write_elements(buf, a.value(row).as_ref(), name, options)
        }
        DataType::Map(_, _) => {
            // One entry is a {key, value} struct; the entry COUNT is length-
            // prefixed like a list's, so `{}` and `{k: NULL}` cannot collide.
            let a = downcast::<MapArray>(array, name)?;
            let entries = a.value(row);
            write_elements(buf, &entries, name, options)
        }
        DataType::Struct(_) => {
            // No count needed — the arity is fixed by the schema — but every
            // field is written, so a NULL field is still one tag rather than
            // nothing at all.
            let a = downcast::<StructArray>(array, name)?;
            for child in a.columns() {
                write_canon(buf, child.as_ref(), row, name, options)?;
            }
            Ok(())
        }
        // Union / RunEndEncoded / the list VIEWS: rivet's type mapping produces
        // none of them (`RivetType` has no variant that resolves to one), so
        // rather than guess a canonical form for a shape we cannot generate and
        // cannot test against real data, refuse — loudly, naming the escape.
        other => anyhow::bail!(
            "row_hash: column '{name}' has type {other}, for which rivet has no canonical \
             form, so hashing it could attest that different rows are equal. Declare the \
             covered set without it — `meta_columns.row_hash: [col, ...]` — or leave \
             `row_hash` off for this export."
        ),
    }
}

/// Append a length-prefixed sequence of cells — one list cell's elements, or one
/// map cell's entries. `elements` is the row's SLICE of the child array, so its
/// length is this cell's element count.
///
/// The count is written even when zero: it is what separates `[]` (count 0) from
/// `[NULL]` (count 1 + [`TAG_NULL`]) from `[""]` (count 1 + [`TAG_PRESENT`] +
/// length 0) — three values Arrow's own display renders identically.
fn write_elements(
    buf: &mut Vec<u8>,
    elements: &dyn arrow::array::Array,
    name: &str,
    options: &arrow::util::display::FormatOptions,
) -> Result<()> {
    buf.extend_from_slice(&(elements.len() as u32).to_le_bytes());
    if is_container(elements.data_type()) {
        for i in 0..elements.len() {
            write_canon(buf, elements, i, name, options)?;
        }
        return Ok(());
    }
    // One formatter for the whole slice rather than one per element — the
    // hoisting the top level does, at every depth.
    let fmt = leaf_formatter(elements, name, options)?;
    for i in 0..elements.len() {
        write_leaf(buf, elements, &fmt, i);
    }
    Ok(())
}

fn leaf_formatter<'a>(
    array: &'a dyn arrow::array::Array,
    name: &str,
    options: &'a arrow::util::display::FormatOptions,
) -> Result<arrow::util::display::ArrayFormatter<'a>> {
    arrow::util::display::ArrayFormatter::try_new(array, options).map_err(|e| {
        anyhow::anyhow!(
            "row_hash: column '{name}' has type {} which Arrow cannot render, so it cannot \
             be hashed ({e}). Declare the covered set without it — `meta_columns.row_hash: \
             [col, ...]`.",
            array.data_type()
        )
    })
}

fn downcast<'a, T: 'static>(array: &'a dyn arrow::array::Array, name: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow::anyhow!(
            "row_hash: column '{name}' declares {} but its array is a different kind — \
             refusing rather than hashing a value read as the wrong shape",
            array.data_type()
        )
    })
}

/// Compute deterministic 64-bit hashes over `cols` for all rows in a batch.
/// Hoists one ArrayFormatter per covered scalar column (avoiding per-cell String
/// allocations) and reuses a single scratch buffer across all rows.
///
/// The rendering is xxh3 over Arrow's display formatting, framed injectively
/// (see [`write_leaf`] and [`write_elements`]). Deliberately NOT reproducible in
/// SQL, because the only other place this value is computed is rivet itself,
/// re-reading the sampled rows through the same path (repair-design.md §5h):
/// agreement is a property of running one piece of code twice.
///
/// Fallible for the same reason the column resolution is: a column rivet cannot
/// canonicalize must FAIL the run, never be silently omitted from the hash.
/// Omitting it would leave the manifest's `RowHashContract` claiming a coverage
/// the value does not have — the quietest way this feature could lie.
fn hash_column(batch: &RecordBatch, n: usize, cols: &[usize]) -> Result<Int64Array> {
    use xxhash_rust::xxh3::xxh3_128;

    /// A covered column's fast path. `Container` is NOT "skip" — it routes to
    /// `write_canon`; the enum exists so the scalar hoist stays a hoist without
    /// an `Option` whose `None` could be mistaken for "omit this column".
    enum Top<'a> {
        Scalar(arrow::util::display::ArrayFormatter<'a>),
        Container,
    }

    let options = arrow::util::display::FormatOptions::default();
    let names: Vec<String> = cols
        .iter()
        .map(|&i| batch.schema().field(i).name().to_string())
        .collect();
    let tops: Vec<Top> = cols
        .iter()
        .zip(&names)
        .map(|(&i, name)| {
            let array = batch.column(i);
            if is_container(array.data_type()) {
                Ok(Top::Container)
            } else {
                leaf_formatter(array.as_ref(), name, &options).map(Top::Scalar)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut buf = Vec::with_capacity(256);
    let mut hashes = Vec::with_capacity(n);
    for row in 0..n {
        buf.clear();
        for ((&col_idx, top), name) in cols.iter().zip(&tops).zip(&names) {
            let array = batch.column(col_idx);
            match top {
                Top::Scalar(fmt) => write_leaf(&mut buf, array.as_ref(), fmt, row),
                Top::Container => write_canon(&mut buf, array.as_ref(), row, name, &options)?,
            }
        }
        let h = xxh3_128(&buf);
        hashes.push(h as i64);
    }
    Ok(Int64Array::from(hashes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;

    /// A timestamp whose timezone is a NAME must hash, not refuse.
    ///
    /// Arrow's `ArrayFormatter` rejects `Timestamp(_, Some("UTC"))` unless the
    /// `chrono-tz` feature is on — "only offset based timezones supported
    /// without chrono-tz feature". rivet maps EVERY timestamp to
    /// `Some("UTC")` (`types/mapping.rs`), so `meta_columns.row_hash` bailed on
    /// every temporal column with
    ///
    ///   row_hash: column 'action_date' has type Timestamp(µs, "UTC") which
    ///   Arrow cannot render
    ///
    /// Measured on a real MySQL config, 2026-08-05: 63 of 65 finished exports
    /// failed on exactly this, and the run recorded them as `failed` with zero
    /// rows. Nothing in the suite caught it because a NAIVE timestamp and an
    /// OFFSET timezone (`+03:00`) both render fine — only the NAMED form, which
    /// is the only form rivet produces, does not.
    ///
    /// This test is the reason the feature cannot be dropped again: without
    /// `chrono-tz` it fails at the `unwrap`, naming the column.
    #[test]
    fn row_hash_covers_a_named_timezone_timestamp() {
        use arrow::array::TimestampMicrosecondArray;
        let ts = TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000i64), Some(1i64)])
            .with_timezone("UTC".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "action_date",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(ts)],
        )
        .unwrap();

        let hashes = row_hash_array(&batch, &["id".to_string(), "action_date".to_string()]).expect(
            "a named-timezone timestamp must hash — if this is an Arrow render error, the \
                 `chrono-tz` feature has been dropped from the arrow dependency",
        );
        let h = hashes.as_any().downcast_ref::<Int64Array>().unwrap();
        // Two rows differing ONLY in the timestamp must not collide: proves the
        // value reached the hash rather than being rendered as a constant.
        assert_ne!(
            h.value(0),
            h.value(1),
            "rows differing only in the timestamp collided — the column did not reach the hash"
        );
    }

    fn sample_batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    None,
                    Some("charlie"),
                ])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    /// `enrich_schema` and `enrich_batch` are separate functions walking the
    /// same list, so a divergence in order would surface as an Arrow "column
    /// count/type mismatch" at write time — on a live run. Pin the order here
    /// instead.
    #[test]
    fn meta_column_order_is_pinned() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: RowHash::All(true),
            cdc_snapshot_pos: None,
        };
        let enriched = enrich_schema(&schema, &meta).unwrap();
        assert_eq!(
            enriched
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", COL_EXPORTED_AT, COL_ROW_HASH]
        );
        // try_new is what enforces the agreement — this unwrap IS the assertion.
        enrich_batch(&batch, &meta, &enriched, 0).unwrap();
    }

    /// The hash covers SOURCE columns only. If it were computed after
    /// enrichment, `_rivet_exported_at` would enter it under `row_hash: true`
    /// and the value would change every run, so no audit could ever match it —
    /// and the two legs, which stamp different instants, could never agree.
    #[test]
    fn the_row_hash_ignores_the_meta_columns_it_ships_beside() {
        let (_, batch) = sample_batch();
        let without = row_hashes(
            &MetaColumns {
                exported_at: false,
                row_hash: RowHash::All(true),
                cdc_snapshot_pos: None,
            },
            &batch,
        );
        let with = row_hashes(
            &MetaColumns {
                exported_at: true,
                row_hash: RowHash::All(true),
                cdc_snapshot_pos: None,
            },
            &batch,
        );
        assert_eq!(without, with);
    }

    /// The contract is what a reader consults to decide whether a persisted
    /// hash is comparable to what it is about to compute. It records the SPEC:
    /// `true` means "every column this part projects", and the part carries its
    /// own schema — resolving it here would duplicate a fact the data already
    /// holds and go stale when the projection changes.
    #[test]
    fn the_contract_records_the_spec_and_a_render_id() {
        assert_eq!(RowHashContract::of(&RowHash::All(false)), None);
        let c = RowHashContract::of(&RowHash::All(true)).unwrap();
        assert_eq!(c.column, COL_ROW_HASH);
        assert_eq!(c.covered, RowHash::All(true));
        assert_eq!(c.render, ROW_HASH_RENDER_ID);

        let declared = RowHash::Columns(vec!["status".into(), "amount".into()]);
        let c = RowHashContract::of(&declared).unwrap();
        assert_eq!(c.covered, declared);
        assert_ne!(
            RowHashContract::of(&RowHash::Columns(vec!["a".into(), "b".into()])),
            RowHashContract::of(&RowHash::Columns(vec!["b".into(), "a".into()])),
            "column order changes every hash, so it must survive into the contract"
        );
    }

    fn row_hashes(meta: &MetaColumns, batch: &RecordBatch) -> Vec<i64> {
        let schema = enrich_schema(&batch.schema(), meta).unwrap();
        let out = enrich_batch(batch, meta, &schema, 0).unwrap();
        let a = out
            .column_by_name(COL_ROW_HASH)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    fn meta_rh(spec: RowHash) -> MetaColumns {
        MetaColumns {
            exported_at: false,
            row_hash: spec,
            cdc_snapshot_pos: None,
        }
    }

    /// Round-10 STRUCT: the snapshot stamp appends a CONSTANT `__pos` (the
    /// anchor string) + `__seq = -1` to every row — the columns whose absence
    /// made a re-baseline lose the warehouse dedup to every pre-gap change.
    /// RED against dropping either column or the -1.
    #[test]
    fn cdc_snapshot_stamp_appends_constant_pos_and_seq() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(false),
            cdc_snapshot_pos: Some(r#"{"file":"binlog.000007","pos":42}"#.into()),
        };
        let enriched = enrich_schema(&schema, &meta).unwrap();
        assert_eq!(
            enriched.fields().len(),
            schema.fields().len() + 2,
            "__pos + __seq"
        );
        let out = enrich_batch(&batch, &meta, &enriched, 0).unwrap();
        let pos = out
            .column_by_name("__pos")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seq = out
            .column_by_name("__seq")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..out.num_rows() {
            assert_eq!(pos.value(i), r#"{"file":"binlog.000007","pos":42}"#);
            assert_eq!(
                seq.value(i),
                -1,
                "must LOSE an exact-anchor tie to any change"
            );
        }
    }

    /// `row_hash: true` keeps meaning every column, in projection order —
    /// existing configs must not silently change what they attest.
    #[test]
    fn row_hash_true_still_covers_every_column() {
        let (schema, batch) = sample_batch();
        assert_eq!(
            row_hash_columns(&schema, &RowHash::All(true)).unwrap(),
            vec!["id", "name"]
        );
        assert_eq!(
            row_hashes(&meta_rh(RowHash::All(true)), &batch),
            row_hashes(
                &meta_rh(RowHash::Columns(vec!["id".into(), "name".into()])),
                &batch
            ),
            "naming every column must equal asking for all of them"
        );
    }

    /// The point of a declared set: a column outside it does not move the
    /// hash. This is the behaviour an operator is buying when they exclude a
    /// load timestamp — and equally the reason the covered set has to be
    /// recorded rather than inferred.
    #[test]
    fn a_declared_set_narrows_what_the_hash_attests() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mk = |name: &str| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![Some(name)])),
                ],
            )
            .unwrap()
        };
        let id_only = meta_rh(RowHash::Columns(vec!["id".into()]));
        assert_eq!(
            row_hashes(&id_only, &mk("alice")),
            row_hashes(&id_only, &mk("bob")),
            "a column outside the declared set must not move the hash"
        );
        let both = meta_rh(RowHash::All(true));
        assert_ne!(
            row_hashes(&both, &mk("alice")),
            row_hashes(&both, &mk("bob")),
            "...and inside it, it must"
        );
    }

    /// Column ORDER is part of what is hashed, so a reorder must change the
    /// value rather than quietly comparing equal.
    #[test]
    fn declared_order_changes_the_hash() {
        let (_, batch) = sample_batch();
        assert_ne!(
            row_hashes(
                &meta_rh(RowHash::Columns(vec!["id".into(), "name".into()])),
                &batch
            ),
            row_hashes(
                &meta_rh(RowHash::Columns(vec!["name".into(), "id".into()])),
                &batch
            ),
        );
    }

    /// A name that is not projected FAILS. Skipping it would produce hashes
    /// that agree while attesting content that was never hashed.
    #[test]
    fn an_unprojected_column_is_refused_with_the_available_list() {
        let (schema, _) = sample_batch();
        let err = row_hash_columns(&schema, &RowHash::Columns(vec!["nope".into()]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope") && err.contains("id, name"), "{err}");
    }

    #[test]
    fn empty_and_duplicate_declared_sets_are_refused() {
        let err = RowHash::Columns(vec![])
            .validate("e")
            .unwrap_err()
            .to_string();
        assert!(err.contains("attests nothing"), "{err}");
        let err = RowHash::Columns(vec!["a".into(), "a".into()])
            .validate("e")
            .unwrap_err()
            .to_string();
        assert!(err.contains("twice"), "{err}");
        RowHash::Columns(vec!["a".into()]).validate("e").unwrap();
        RowHash::All(true).validate("e").unwrap();
    }

    /// THE invariant §5h rests on: the CDC drain and the snapshot leg must
    /// produce the same hash for the same row.
    ///
    /// They do not see the same batch. The drain's carries `__op`/`__pos`/
    /// `__seq` in front of the data; the snapshot's does not. So the hash must
    /// be computed over the resolved DATA columns and be blind to whatever else
    /// the batch happens to hold — otherwise every backfilled row and every
    /// streamed row would disagree in the one log they share, and the column
    /// would be useless for exactly the comparison it exists to serve.
    #[test]
    fn cdc_meta_columns_do_not_enter_the_row_hash() {
        let data_only = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let snapshot = RecordBatch::try_new(
            data_only,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("alice")])),
            ],
        )
        .unwrap();

        // What the CDC sink builds: meta columns first, then the same data.
        let with_meta = Arc::new(Schema::new(vec![
            Field::new("__op", DataType::Utf8, false),
            Field::new("__pos", DataType::Utf8, false),
            Field::new("__seq", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let drain = RecordBatch::try_new(
            with_meta,
            vec![
                Arc::new(StringArray::from(vec![Some("insert")])),
                Arc::new(StringArray::from(vec![Some("{\"pos\":7}")])),
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("alice")])),
            ],
        )
        .unwrap();

        let data: Vec<String> = vec!["id".into(), "name".into()];
        let read = |b: &RecordBatch| {
            let a = row_hash_array(b, &data).unwrap();
            a.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
        };
        assert_eq!(read(&snapshot), read(&drain));

        // And "all columns" on the CDC leg resolves to the DATA columns, not to
        // the sink's own — which is what makes the equality above reachable
        // from a plain `row_hash: true`.
        assert_eq!(
            row_hash_columns_of(&data, &RowHash::All(true)).unwrap(),
            data
        );
    }

    #[test]
    fn enrich_disabled_is_noop() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(false),
            cdc_snapshot_pos: None,
        };
        let enriched_schema = enrich_schema(&schema, &meta).unwrap();
        assert_eq!(enriched_schema.fields().len(), 2);
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn enrich_exported_at_only() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: RowHash::All(false),
            cdc_snapshot_pos: None,
        };
        let enriched_schema = enrich_schema(&schema, &meta).unwrap();
        assert_eq!(enriched_schema.fields().len(), 3);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);

        let ts = 1_711_612_800_000_000i64;
        let result = enrich_batch(&batch, &meta, &enriched_schema, ts).unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 3);

        let ts_col = result
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), ts);
        assert_eq!(ts_col.value(2), ts);
    }

    #[test]
    fn enrich_row_hash_only() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(true),
            cdc_snapshot_pos: None,
        };
        let enriched_schema = enrich_schema(&schema, &meta).unwrap();
        assert_eq!(enriched_schema.field(2).name(), COL_ROW_HASH);
        assert_eq!(*enriched_schema.field(2).data_type(), DataType::Int64);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
        let hash_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Different rows produce different hashes
        assert_ne!(hash_col.value(0), hash_col.value(1));
        assert_ne!(hash_col.value(1), hash_col.value(2));
    }

    #[test]
    fn enrich_both_columns() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: RowHash::All(true),
            cdc_snapshot_pos: None,
        };
        let enriched_schema = enrich_schema(&schema, &meta).unwrap();
        assert_eq!(enriched_schema.fields().len(), 4);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);
        assert_eq!(enriched_schema.field(3).name(), COL_ROW_HASH);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 123456).unwrap();
        assert_eq!(result.num_columns(), 4);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn hash_is_deterministic() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(true),
            cdc_snapshot_pos: None,
        };
        let enriched_schema = enrich_schema(&schema, &meta).unwrap();

        let r1 = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
        let r2 = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();

        let h1 = r1.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let h2 = r2.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..3 {
            assert_eq!(
                h1.value(i),
                h2.value(i),
                "hash should be deterministic for row {i}"
            );
        }
    }

    #[test]
    fn hash_distinguishes_null_from_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![None, Some("")]))],
        )
        .unwrap();

        let meta = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(true),
            cdc_snapshot_pos: None,
        };
        let enriched_schema = enrich_schema(&schema, &meta).unwrap();
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
        let hashes = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_ne!(
            hashes.value(0),
            hashes.value(1),
            "NULL and empty string should hash differently"
        );
    }

    /// Hash a batch of two string columns. TWO columns on purpose: a
    /// single-column fixture cannot express a field BOUNDARY, and the boundary
    /// is exactly what the framing has to get right — the same
    /// fixture-past-the-activation-threshold rule a one-element fold falls foul
    /// of.
    fn two_column_hashes(a: Vec<Option<&str>>, b: Vec<Option<&str>>) -> Vec<i64> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(a)) as ArrayRef,
                Arc::new(StringArray::from(b)) as ArrayRef,
            ],
        )
        .unwrap();
        let meta = meta_rh(RowHash::All(true));
        let es = enrich_schema(&schema, &meta).unwrap();
        let out = enrich_batch(&batch, &meta, &es, 0).unwrap();
        let h = out
            .column(out.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..h.len()).map(|i| h.value(i)).collect()
    }

    /// A value must not be able to forge a field boundary.
    ///
    /// RED against the `\x1f`-separator framing (render id v1), which built
    /// IDENTICAL bytes for these two rows and returned one hash for both:
    /// `("a\x1f","b")` → `a 1F 1F b 1F`, and `("a","\x1fb")` → `a 1F 1F b 1F`.
    /// Control characters are legal in every engine's text type, so this is
    /// reachable data rather than a synthetic edge.
    #[test]
    fn a_value_containing_the_old_separator_cannot_forge_a_field_boundary() {
        let h = two_column_hashes(
            vec![Some("a\u{1f}"), Some("a")],
            vec![Some("b"), Some("\u{1f}b")],
        );
        assert_ne!(
            h[0], h[1],
            "('a\\x1f','b') and ('a','\\x1fb') are DIFFERENT rows and must not share a hash"
        );
    }

    /// A NULL must not collide with a value whose rendering IS the null marker.
    /// RED against v1, where a NULL wrote a bare `\x00` and a one-NUL-byte
    /// string rendered to the same `\x00`.
    #[test]
    fn hash_distinguishes_null_from_a_value_rendering_as_the_null_marker() {
        let h = two_column_hashes(vec![None, Some("\u{0}")], vec![Some("x"), Some("x")]);
        assert_ne!(
            h[0], h[1],
            "NULL and the one-NUL-byte string must not share a hash"
        );
    }

    /// Where the boundary FALLS must matter when the concatenated content is
    /// identical — ("ab","c") vs ("a","bc"). Pins the half of injectivity that
    /// a separator alone gives and a length prefix must not lose.
    #[test]
    fn hash_distinguishes_where_the_field_boundary_falls() {
        let h = two_column_hashes(vec![Some("ab"), Some("a")], vec![Some("c"), Some("bc")]);
        assert_ne!(
            h[0], h[1],
            "('ab','c') and ('a','bc') are DIFFERENT rows and must not share a hash"
        );
    }

    /// An array column must hash INJECTIVELY, not by Arrow's display text.
    ///
    /// Every pair below renders identically under Arrow's own list display — it
    /// joins elements with ", " and renders a NULL element as the empty string —
    /// so hashing that text attested that different arrays were the same array.
    /// A PostgreSQL `text[]` reaches this path (`RivetType::List` →
    /// `DataType::List`, types/mapping.rs), so a row whose array changed kept its
    /// old hash and change detection skipped it.
    ///
    /// RED-proven before the recursive canonicalization: `[NULL]`, `[""]` and
    /// `[]` all hashed 3646912262511830962, and `["a, b"]`/`["a","b"]` both
    /// 579306972153428012. Framing the FIELD cannot fix an ambiguity inside the
    /// field's own text; the elements themselves have to be framed.
    ///
    /// Six cells on purpose — the collisions are between SIBLING shapes, so a
    /// fixture with one array per case is below the threshold that can catch
    /// them.
    #[test]
    fn an_array_column_hashes_injectively_not_by_its_display_text() {
        use arrow::array::{ListBuilder, StringBuilder};
        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_null(); // 0: [NULL]
        b.append(true);
        b.values().append_value(""); // 1: [""]
        b.append(true);
        b.append(true); // 2: []
        b.values().append_value("a, b"); // 3: ["a, b"] — one element with the join
        b.append(true);
        b.values().append_value("a"); // 4: ["a","b"] — two elements
        b.values().append_value("b");
        b.append(true);
        b.append(false); // 5: NULL cell (absent, not empty)
        let arr = Arc::new(b.finish()) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            arr.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let meta = meta_rh(RowHash::All(true));
        let es = enrich_schema(&schema, &meta).unwrap();
        let out = enrich_batch(&batch, &meta, &es, 0).expect("an array column is hashable");
        let h = out
            .column(out.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let got: Vec<i64> = (0..6).map(|i| h.value(i)).collect();

        let mut uniq = got.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(
            uniq.len(),
            6,
            "all six array shapes are DIFFERENT values and must hash differently — \
             [NULL], [\"\"], [], [\"a, b\"], [\"a\",\"b\"], NULL; got {got:?}"
        );
    }

    /// A struct column is framed per field, so moving content between fields
    /// changes the hash. `{a:\"x\", b:\"\"}` vs `{a:\"x\", b:NULL}` also differ —
    /// a NULL field is a tag, not nothing.
    #[test]
    fn a_struct_column_distinguishes_its_fields_and_their_nulls() {
        use arrow::array::StructArray;
        let f = |name: &str, v: Vec<Option<&str>>| {
            (
                Arc::new(Field::new(name, DataType::Utf8, true)),
                Arc::new(StringArray::from(v)) as ArrayRef,
            )
        };
        // rows: 0 = {a:"x", b:""}, 1 = {a:"x", b:NULL}, 2 = {a:"", b:"x"}
        let arr = Arc::new(StructArray::from(vec![
            f("a", vec![Some("x"), Some("x"), Some("")]),
            f("b", vec![Some(""), None, Some("x")]),
        ])) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "meta",
            arr.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let meta = meta_rh(RowHash::All(true));
        let es = enrich_schema(&schema, &meta).unwrap();
        let out = enrich_batch(&batch, &meta, &es, 0).expect("a struct column is hashable");
        let h = out
            .column(out.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let got: Vec<i64> = (0..3).map(|i| h.value(i)).collect();
        let mut uniq = got.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(
            uniq.len(),
            3,
            "an empty field, a NULL field and content moved between fields are three \
             different rows; got {got:?}"
        );
    }

    /// An INDEPENDENT oracle for the rendering: `expected` is a literal, not a
    /// second call to the code under test, so any change to the framing, the
    /// digest or the `i64` truncation fails HERE — beside the assertion that the
    /// render id must move with it. Recomputing `expected` via `row_hash_array`
    /// would pass for any rendering whatsoever, which is the self-oracle trap.
    ///
    /// If this fails, the rendering changed: bump [`ROW_HASH_RENDER_ID`] and
    /// re-pin the literals in the SAME commit. Never re-pin one alone.
    #[test]
    fn the_rendering_is_pinned_to_literals_and_to_its_render_id() {
        let h = two_column_hashes(vec![Some("a"), None], vec![Some("b"), Some("")]);
        assert_eq!(
            h,
            vec![
                -7_527_003_277_948_829_337_i64,
                -5_099_907_639_937_407_564_i64
            ],
            "the row-hash rendering changed — bump ROW_HASH_RENDER_ID and re-pin these \
             literals together, never one without the other"
        );
        assert_eq!(
            ROW_HASH_RENDER_ID, "xxh3-128-i64-arrow-display-us-v2",
            "the render id must change whenever the rendering above does"
        );
    }
}
