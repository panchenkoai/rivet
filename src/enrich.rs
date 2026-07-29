use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::config::{MetaColumns, RowHash};
use crate::content_hash::{self, ContentHashConfig};
use crate::error::Result;

pub const COL_EXPORTED_AT: &str = "_rivet_exported_at";
pub const COL_ROW_HASH: &str = "_rivet_row_hash";

/// Extend an Arrow schema with the columns rivet adds.
///
/// `hash` comes first and the `_rivet_*` meta columns after, because the two
/// are different kinds of thing: `__content_hash` is part of the warehouse
/// table's contract (the audit reads it), while the meta columns are optional
/// provenance. [`enrich_batch`] builds its arrays in this same order — the two
/// must not be able to disagree, which is why both live here rather than being
/// appended by each caller.
///
/// Fallible only because of `hash`: the content hash refuses column types it
/// cannot render identically in SQL, and that refusal belongs at schema time,
/// before a single row is read.
pub fn enrich_schema(
    schema: &SchemaRef,
    meta: &MetaColumns,
    hash: Option<&ContentHashConfig>,
) -> Result<SchemaRef> {
    if !meta.exported_at && !meta.row_hash.enabled() && hash.is_none() {
        return Ok(schema.clone());
    }
    let base = match hash {
        Some(h) => content_hash::hashed_schema(schema, h)?,
        None => schema.clone(),
    };
    if !meta.exported_at && !meta.row_hash.enabled() {
        return Ok(base);
    }
    let mut fields: Vec<Arc<Field>> = base.fields().iter().cloned().collect();
    if meta.exported_at {
        fields.push(Arc::new(Field::new(
            COL_EXPORTED_AT,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )));
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
    hash: Option<&ContentHashConfig>,
) -> Result<RecordBatch> {
    if !meta.exported_at && !meta.row_hash.enabled() && hash.is_none() {
        return Ok(batch.clone());
    }

    let n = batch.num_rows();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    if let Some(h) = hash {
        columns.push(content_hash::hash_array(batch, h)?);
    }

    if meta.exported_at {
        let ts_array =
            TimestampMicrosecondArray::from(vec![Some(exported_at_us); n]).with_timezone("UTC");
        columns.push(Arc::new(ts_array));
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
    Ok(Arc::new(hash_column(batch, batch.num_rows(), &idx)))
}

/// Compute deterministic 64-bit hashes over `cols` for all rows in a batch.
/// Creates one ArrayFormatter per covered column (avoids per-cell String
/// allocations), then reuses a single scratch buffer across all rows.
///
/// The rendering is xxh3 over Arrow's display formatting. That is fine — and
/// deliberately NOT reproducible in SQL — because the only other place this
/// value is ever computed is rivet itself, re-reading the sampled rows through
/// the same path (repair-design.md §5h). Agreement is a property of the code.
fn hash_column(batch: &RecordBatch, n: usize, cols: &[usize]) -> Int64Array {
    use std::io::Write as IoWrite;
    use xxhash_rust::xxh3::xxh3_128;

    let options = arrow::util::display::FormatOptions::default();
    let formatters: Vec<Option<arrow::util::display::ArrayFormatter>> = cols
        .iter()
        .map(|&i| {
            arrow::util::display::ArrayFormatter::try_new(batch.column(i).as_ref(), &options).ok()
        })
        .collect();

    let mut buf = Vec::with_capacity(256);
    let mut hashes = Vec::with_capacity(n);
    for row in 0..n {
        buf.clear();
        for (&col_idx, fmt_opt) in cols.iter().zip(&formatters) {
            let array = batch.column(col_idx);
            if array.is_null(row) {
                buf.extend_from_slice(b"\x00");
            } else if let Some(fmt) = fmt_opt {
                let _ = write!(buf, "{}", fmt.value(row));
            }
            buf.push(b'\x1f');
        }
        let h = xxh3_128(&buf);
        hashes.push(h as i64);
    }
    Int64Array::from(hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;

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

    fn hash_cfg() -> ContentHashConfig {
        ContentHashConfig {
            pk: "id".into(),
            cols: vec!["name".into()],
        }
    }

    // `__content_hash` precedes the `_rivet_*` meta columns, and — the part that
    // actually matters — enrich_schema and enrich_batch agree on that order.
    // They are separate functions over the same list, so a divergence would show
    // up as an Arrow "column count/type mismatch" at write time, on a live run.
    #[test]
    fn content_hash_column_precedes_meta_columns() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: RowHash::All(true),
        };
        let cfg = hash_cfg();
        let enriched = enrich_schema(&schema, &meta, Some(&cfg)).unwrap();
        assert_eq!(
            enriched
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "id",
                "name",
                content_hash::COL_CONTENT_HASH,
                COL_EXPORTED_AT,
                COL_ROW_HASH
            ]
        );
        // try_new is what enforces the agreement — this unwrap IS the assertion.
        let out = enrich_batch(&batch, &meta, &enriched, 0, Some(&cfg)).unwrap();
        let hashes = out
            .column_by_name(content_hash::COL_CONTENT_HASH)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(hashes.value(0), content_hash::hash_of("1|alice"));
        assert_eq!(
            hashes.value(1),
            content_hash::hash_of(&format!("2|{}", content_hash::NULL_SENTINEL))
        );
    }

    // The hash must be computed over SOURCE columns only. If it were computed
    // after enrichment, `_rivet_exported_at` would enter the text and the value
    // would change every run, so no audit could ever match it.
    #[test]
    fn content_hash_ignores_the_meta_columns_it_ships_beside() {
        let (schema, batch) = sample_batch();
        let cfg = hash_cfg();
        let bare = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(false),
        };
        let with_meta = MetaColumns {
            exported_at: true,
            row_hash: RowHash::All(true),
        };
        let read = |meta: &MetaColumns| {
            let sch = enrich_schema(&schema, meta, Some(&cfg)).unwrap();
            enrich_batch(&batch, meta, &sch, 999, Some(&cfg))
                .unwrap()
                .column_by_name(content_hash::COL_CONTENT_HASH)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(read(&bare), read(&with_meta));
    }

    fn row_hashes(meta: &MetaColumns, batch: &RecordBatch) -> Vec<i64> {
        let schema = enrich_schema(&batch.schema(), meta, None).unwrap();
        let out = enrich_batch(batch, meta, &schema, 0, None).unwrap();
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
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.fields().len(), 2);
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn enrich_exported_at_only() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: RowHash::All(false),
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.fields().len(), 3);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);

        let ts = 1_711_612_800_000_000i64;
        let result = enrich_batch(&batch, &meta, &enriched_schema, ts, None).unwrap();
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
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.field(2).name(), COL_ROW_HASH);
        assert_eq!(*enriched_schema.field(2).data_type(), DataType::Int64);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
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
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.fields().len(), 4);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);
        assert_eq!(enriched_schema.field(3).name(), COL_ROW_HASH);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 123456, None).unwrap();
        assert_eq!(result.num_columns(), 4);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn hash_is_deterministic() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: RowHash::All(true),
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();

        let r1 = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
        let r2 = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();

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
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
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
}
