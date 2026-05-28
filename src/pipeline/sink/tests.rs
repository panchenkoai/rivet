//! Unit tests for `ExportSink` — extracted from `mod.rs` to keep the
//! implementation file focused.  See [`super`] for the public surface.

use super::*;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

// ─── extract_last_cursor_value tests live in src/pipeline/sink/cursor.rs

// ─── schema_without_internal / record_batch_without_internal ──

#[test]
fn strip_internal_column_from_schema() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_rivet_chunk_rn", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let result = ExportSink::schema_without_internal(&schema, "_rivet_chunk_rn").unwrap();
    assert_eq!(result.fields().len(), 2);
    assert_eq!(result.field(0).name(), "id");
    assert_eq!(result.field(1).name(), "name");
}

#[test]
fn strip_internal_column_missing_errors() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    assert!(ExportSink::schema_without_internal(&schema, "nonexistent").is_err());
}

#[test]
fn strip_internal_column_from_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_rivet_chunk_rn", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    let stripped = ExportSink::record_batch_without_internal(&batch, "_rivet_chunk_rn").unwrap();
    assert_eq!(stripped.num_columns(), 1);
    assert_eq!(stripped.schema().field(0).name(), "id");
    let ids = stripped
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
}

// ─── I1: Finalize Before Write ───────────────────────────────────────────────

/// I1 (ADR-0001) — The temp-file writer must be finalized (`w.finish()`) before
/// the file is transferred to the destination.
///
/// This test verifies that after `writer.take()` + `w.finish()`, the temp file
/// is non-empty and complete — i.e. the destination would receive a valid,
/// non-truncated file.
///
/// Implementation: `pipeline/single.rs` calls `sink.writer.take()` and `w.finish()`
/// (lines 149–151) *before* entering the `dest.write()` loop.  This unit test
/// anchors the contract at the sink layer: a finished writer always yields a
/// readable, complete file.
#[test]
fn i1_writer_finish_produces_complete_file_before_destination_write() {
    use crate::source::BatchSink;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let mut sink = minimal_sink();
    // Switch to Parquet so we can verify the footer is present (CSV also works).
    // Use the default Csv format from minimal_sink() — simpler, no codec deps.

    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["alice", "bob", "carol"]))],
    )
    .unwrap();

    // Writer is absent until the first schema arrives.
    assert!(sink.writer.is_none(), "writer must start as None");

    sink.on_schema(schema).unwrap();
    assert!(
        sink.writer.is_some(),
        "writer must be present after on_schema"
    );

    sink.on_batch(&batch).unwrap();

    // I1: take and finish the writer before any destination write.
    // This mirrors single.rs:149-151: `if let Some(w) = sink.writer.take() { w.finish()?; }`
    if let Some(w) = sink.writer.take() {
        w.finish()
            .expect("I1: writer.finish() must succeed before destination write");
    }
    assert!(
        sink.writer.is_none(),
        "writer must be consumed after finish()"
    );

    // The temp file must be non-empty — a finished writer produces a complete file.
    // An unfinished Parquet file (no footer) or CSV (missing last line) would be
    // detected by the destination consumer; finish() prevents this.
    let file_len = std::fs::metadata(sink.tmp.path())
        .expect("temp file must be accessible after finish()")
        .len();
    assert!(
        file_len > 0,
        "I1: temp file must be non-empty after writer.finish() — \
         the destination must never receive a truncated file; got {} bytes",
        file_len
    );

    // Total rows must reflect the written batch.
    assert_eq!(sink.total_rows, 3, "total_rows must count written rows");
}

// ─── quality tracking ────────────────────────────────────────

#[test]
fn track_quality_counts_nulls() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            None,
            Some("d"),
        ]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec!["name".into()], vec![]);
    sink.on_schema(schema).unwrap();
    sink.track_quality(&batch);
    assert_eq!(sink.quality_null_counts.get("name"), Some(&2));
}

#[test]
fn track_quality_counts_uniques() {
    use crate::source::BatchSink;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3]))],
    )
    .unwrap();

    let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
    sink.on_schema(schema).unwrap();
    sink.track_quality(&batch);
    assert_eq!(sink.quality_unique_sets.get("id").unwrap().len(), 3);
}

#[test]
fn run_quality_checks_no_config_returns_empty() {
    let sink = ExportSink {
        quality_columns: None,
        total_rows: 100,
        ..minimal_sink()
    };
    assert!(sink.run_quality_checks().is_empty());
}

#[test]
fn run_quality_checks_detects_excess_nulls() {
    let mut sink = minimal_sink_with_quality(vec!["col".into()], vec![]);
    sink.quality_columns
        .as_mut()
        .unwrap()
        .null_ratio_max
        .insert("col".into(), 0.1);
    sink.total_rows = 100;
    sink.quality_null_counts.insert("col".into(), 50);
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1);
    assert!(issues[0].message.contains("null ratio"));
}

#[test]
fn run_quality_checks_detects_duplicates() {
    let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
    sink.total_rows = 5;
    let mut set = std::collections::HashSet::new();
    set.insert(0u64);
    set.insert(1u64);
    set.insert(2u64);
    sink.quality_unique_sets.insert("id".into(), set);
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1);
    assert!(issues[0].message.contains("duplicate"));
}

// ─── unique_max_entries cap ───────────────────────────────────

fn sink_with_unique_cap(unique_cols: Vec<String>, cap: usize) -> ExportSink {
    ExportSink {
        quality_columns: Some(crate::config::QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: std::collections::HashMap::new(),
            unique_columns: unique_cols,
            unique_max_entries: Some(cap),
        }),
        ..minimal_sink()
    }
}

#[test]
fn unique_cap_stops_inserting_at_limit() {
    // cap=3 with 5 distinct rows — set must stop growing at 3
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 3);
    sink.quality_unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch);
    let set_len = sink
        .quality_unique_sets
        .get("id")
        .map(|s| s.len())
        .unwrap_or(0);
    assert!(set_len <= 3, "set must not grow past cap; got {set_len}");
    assert!(
        sink.quality_unique_capped.contains("id"),
        "id must be flagged as capped"
    );
}

#[test]
fn unique_cap_emits_warn_issue_not_fail() {
    let mut sink = sink_with_unique_cap(vec!["id".into()], 2);
    sink.total_rows = 5;
    sink.quality_unique_capped.insert("id".into());
    let mut set = std::collections::HashSet::new();
    set.insert(0u64);
    set.insert(1u64);
    sink.quality_unique_sets.insert("id".into(), set);
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].severity, crate::quality::Severity::Warn);
    assert!(
        issues[0].message.contains("capped"),
        "message must say 'capped'; got: {}",
        issues[0].message
    );
}

#[test]
fn unique_no_cap_grows_unbounded() {
    use crate::source::BatchSink;
    // No unique_max_entries — all 10 rows must be tracked
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..10i64))],
    )
    .unwrap();
    let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
    sink.on_schema(schema).unwrap();
    sink.track_quality(&batch);
    assert_eq!(
        sink.quality_unique_sets.get("id").unwrap().len(),
        10,
        "without cap all distinct values must be tracked"
    );
    assert!(
        sink.quality_unique_capped.is_empty(),
        "no cap → no column should be flagged"
    );
}

#[test]
fn unique_cap_column_skipped_in_subsequent_batches() {
    // After cap is hit in batch 1, batch 2 must be skipped entirely for that column.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 2);
    sink.quality_unique_indices = vec![(0, "id".into())];
    sink.track_quality(&batch1); // cap hit here
    let len_after_first = sink
        .quality_unique_sets
        .get("id")
        .map(|s| s.len())
        .unwrap_or(0);
    sink.track_quality(&batch2); // must be a no-op
    let len_after_second = sink
        .quality_unique_sets
        .get("id")
        .map(|s| s.len())
        .unwrap_or(0);
    assert_eq!(
        len_after_first, len_after_second,
        "set must not grow after cap is hit; was {len_after_first}, then {len_after_second}"
    );
}

// ─── helpers ─────────────────────────────────────────────────

fn minimal_sink() -> ExportSink {
    ExportSink {
        writer: None,
        format_type: crate::config::FormatType::Csv,
        compression: crate::config::CompressionType::None,
        compression_level: None,
        tmp: tempfile::NamedTempFile::new().unwrap(),
        total_rows: 0,
        part_rows: 0,
        cursor_column: None,
        last_cursor_value: None,
        schema: None,
        dest_schema: None,
        meta: crate::config::MetaColumns::default(),
        enriched_schema: None,
        exported_at_us: 0,
        quality_null_counts: std::collections::HashMap::new(),
        quality_unique_sets: std::collections::HashMap::new(),
        quality_unique_capped: std::collections::HashSet::new(),
        quality_columns: None,
        quality_null_indices: Vec::new(),
        quality_unique_indices: Vec::new(),
        max_file_size: None,
        completed_parts: Vec::new(),
        strip_internal_column: None,
        column_max_bytes: std::collections::HashMap::new(),
        max_batch_memory_bytes: None,
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::Warn,
        oversized_batch_count: 0,
        parquet_config: None,
        parquet_row_group_rows: None,
    }
}

fn minimal_sink_with_quality(null_cols: Vec<String>, unique_cols: Vec<String>) -> ExportSink {
    let mut null_ratio_max = std::collections::HashMap::new();
    for col in &null_cols {
        null_ratio_max.insert(col.clone(), 0.5);
    }
    ExportSink {
        quality_columns: Some(crate::config::QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max,
            unique_columns: unique_cols,
            unique_max_entries: None,
        }),
        ..minimal_sink()
    }
}

// ── batch memory cap ─────────────────────────────────────────────────────

fn make_int_batch(n_rows: usize, n_cols: usize) -> RecordBatch {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let fields: Vec<Field> = (0..n_cols)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let cols: Vec<Arc<dyn arrow::array::Array>> = (0..n_cols)
        .map(|_| {
            Arc::new(Int64Array::from_iter_values(0..n_rows as i64)) as Arc<dyn arrow::array::Array>
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

#[test]
fn warn_policy_does_not_fail_on_oversized_batch() {
    let batch = make_int_batch(1_000, 10); // 10 × 1000 × 8 B = ~80 KB
    let batch_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    assert!(batch_bytes > 0);

    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1), // 1 byte — every batch will exceed
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::Warn,
        ..minimal_sink()
    };
    // Provide an enriched schema so on_batch_inner can write batches
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(
        (0..10)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    sink.enriched_schema = Some(schema);

    // Should not return an error even though limit is exceeded
    assert!(sink.on_batch_inner(&batch).is_ok());
    assert_eq!(sink.oversized_batch_count, 0); // counter is set in on_batch, not inner
}

#[test]
fn fail_policy_returns_error_on_oversized_batch() {
    let schema_fields: Vec<arrow::datatypes::Field> = (0..5)
        .map(|i| {
            arrow::datatypes::Field::new(format!("c{i}"), arrow::datatypes::DataType::Int64, false)
        })
        .collect();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(schema_fields.clone()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        schema_fields
            .iter()
            .map(|_| {
                std::sync::Arc::new(arrow::array::Int64Array::from_iter_values(0..100i64))
                    as std::sync::Arc<dyn arrow::array::Array>
            })
            .collect(),
    )
    .unwrap();

    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1), // always exceeded
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::Fail,
        enriched_schema: Some(schema),
        ..minimal_sink()
    };

    let result = sink.on_batch(&batch);
    assert!(result.is_err(), "Fail policy should return Err");
    assert!(
        result.unwrap_err().to_string().contains("exceeds"),
        "error should mention 'exceeds'"
    );
    assert_eq!(sink.oversized_batch_count, 1);
}

#[test]
fn batch_memory_bytes_returns_positive_for_non_empty_batch() {
    let batch = make_int_batch(1_000, 4);
    let bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    // 4 cols × 1000 rows × 8 B/elem = 32,000 B plus nullmap overhead
    assert!(bytes >= 32_000, "got {}", bytes);
}

// ── auto_shrink policy ───────────────────────────────────────────────────

fn sink_with_auto_shrink(limit_bytes: usize, n_cols: usize) -> ExportSink {
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(
        (0..n_cols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    ExportSink {
        max_batch_memory_bytes: Some(limit_bytes),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::AutoShrink,
        enriched_schema: Some(schema),
        ..minimal_sink()
    }
}

#[test]
fn auto_shrink_preserves_row_count_exact() {
    // 100 rows × 4 cols × 8 B = 3200 B — limit set to 1 B so every batch splits.
    let batch = make_int_batch(100, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    // Must succeed and accumulate all 100 rows.
    assert!(sink.on_batch(&batch).is_ok());
    assert_eq!(
        sink.total_rows, 100,
        "auto_shrink must not lose or duplicate rows; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_recursive_split_large_batch() {
    // 1000 rows — with a tiny limit the batch must be split many levels deep.
    // Row count is the only correctness invariant we can assert without a live writer.
    let batch = make_int_batch(1_000, 8); // 64 KB+
    let mut sink = sink_with_auto_shrink(1, 8);

    assert!(sink.on_batch(&batch).is_ok());
    assert_eq!(
        sink.total_rows, 1_000,
        "recursive auto_shrink must preserve all rows; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_odd_row_count_no_row_lost() {
    // Odd-sized batches: mid = n/2 (floor), so one half has one more row.
    let batch = make_int_batch(101, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    assert!(sink.on_batch(&batch).is_ok());
    assert_eq!(
        sink.total_rows, 101,
        "odd-row batch must not lose the extra row; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_single_row_over_limit_writes_and_does_not_panic() {
    // A single-row batch that exceeds the limit cannot be split further.
    // The policy must warn and write it as-is rather than loop infinitely.
    let batch = make_int_batch(1, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    let result = sink.on_batch(&batch);
    assert!(
        result.is_ok(),
        "single-row over-limit must not fail: {:?}",
        result
    );
    assert_eq!(
        sink.total_rows, 1,
        "single row must be written; got {}",
        sink.total_rows
    );
}

#[test]
fn auto_shrink_oversized_count_tracks_original_batches_only() {
    // oversized_batch_count must count original batches, not recursive sub-batches.
    let batch = make_int_batch(100, 4);
    let mut sink = sink_with_auto_shrink(1, 4);

    sink.on_batch(&batch).unwrap();
    assert_eq!(
        sink.oversized_batch_count, 1,
        "one original batch triggered the cap; got {}",
        sink.oversized_batch_count
    );
}

#[test]
fn auto_shrink_batch_within_limit_writes_directly() {
    // When the batch fits within the limit, no splitting should occur.
    let batch = make_int_batch(10, 2); // small — well under any reasonable limit
    let actual_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
    let mut sink = sink_with_auto_shrink(actual_bytes * 2, 2); // limit = 2× actual size

    sink.on_batch(&batch).unwrap();
    assert_eq!(sink.total_rows, 10, "in-limit batch: all rows written");
    assert_eq!(
        sink.oversized_batch_count, 0,
        "in-limit batch must not increment oversized counter"
    );
}

// ─── bug regression: row_count_min on empty export ────────────────────────

#[test]
fn row_count_min_fires_when_total_rows_is_zero() {
    // Regression for the bug where run_quality_checks() was called AFTER the
    // total_rows == 0 early return in single.rs, silently bypassing the gate.
    // The check itself is correct — the bug was in the call site.
    let mut sink = minimal_sink();
    sink.quality_columns = Some(crate::config::QualityConfig {
        row_count_min: Some(100),
        row_count_max: None,
        null_ratio_max: std::collections::HashMap::new(),
        unique_columns: vec![],
        unique_max_entries: None,
    });
    // total_rows is 0 (no data exported)
    let issues = sink.run_quality_checks();
    assert_eq!(issues.len(), 1, "must emit exactly one issue");
    assert_eq!(issues[0].severity, crate::quality::Severity::Fail);
    assert!(
        issues[0].message.contains("row_count") && issues[0].message.contains("minimum"),
        "message must identify row_count minimum; got: {}",
        issues[0].message
    );
}

#[test]
fn row_count_min_passes_when_row_count_meets_threshold() {
    let mut sink = minimal_sink();
    sink.quality_columns = Some(crate::config::QualityConfig {
        row_count_min: Some(5),
        row_count_max: None,
        null_ratio_max: std::collections::HashMap::new(),
        unique_columns: vec![],
        unique_max_entries: None,
    });
    sink.total_rows = 5;
    assert!(
        sink.run_quality_checks().is_empty(),
        "exact minimum must pass"
    );
}

// ─── gremlin tests ────────────────────────────────────────────────────────
//
// Fault-injection scenarios that verify sink invariants under adversarial
// conditions: writer failures mid-auto_shrink, file-split interaction with
// auto_shrink, and exact cap boundary for uniqueness tracking.

/// A writer that succeeds on the first N `write_batch` calls, then fails.
struct FailingWriter {
    calls: usize,
    fail_after: usize,
}
impl crate::format::FormatWriter for FailingWriter {
    fn write_batch(&mut self, _batch: &RecordBatch) -> crate::error::Result<()> {
        self.calls += 1;
        if self.calls > self.fail_after {
            anyhow::bail!("gremlin: injected write failure at call {}", self.calls);
        }
        Ok(())
    }
    fn finish(self: Box<Self>) -> crate::error::Result<()> {
        Ok(())
    }
    fn bytes_written(&self) -> u64 {
        0
    }
}

/// A writer that always reports enormous bytes_written, forcing `maybe_split`
/// to trigger after every batch, while never actually failing.
struct SplitTriggerWriter {
    write_count: usize,
}
impl crate::format::FormatWriter for SplitTriggerWriter {
    fn write_batch(&mut self, _batch: &RecordBatch) -> crate::error::Result<()> {
        self.write_count += 1;
        Ok(())
    }
    fn finish(self: Box<Self>) -> crate::error::Result<()> {
        Ok(())
    }
    fn bytes_written(&self) -> u64 {
        999_999_999 // always > any max_file_size → triggers split
    }
}

#[test]
fn gremlin_writer_fail_on_second_subbatch_propagates_error() {
    // auto_shrink splits a batch into 2 sub-batches.
    // The writer succeeds on sub-batch #1 but fails on #2.
    // on_batch must propagate the error — no silent data loss.
    let batch = make_int_batch(4, 2); // 4 rows → splits to 2+2
    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1), // forces splitting on every batch
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::AutoShrink,
        enriched_schema: Some(Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Int64, false),
            Field::new("c1", DataType::Int64, false),
        ]))),
        writer: Some(Box::new(FailingWriter {
            calls: 0,
            fail_after: 1,
        })),
        ..minimal_sink()
    };

    let result = sink.on_batch(&batch);
    assert!(
        result.is_err(),
        "writer failure inside auto_shrink must propagate as Err"
    );
}

#[test]
fn gremlin_writer_fail_before_any_write_propagates_error() {
    // Writer fails immediately on the first write.
    let batch = make_int_batch(2, 2);
    let mut sink = ExportSink {
        enriched_schema: Some(Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Int64, false),
            Field::new("c1", DataType::Int64, false),
        ]))),
        writer: Some(Box::new(FailingWriter {
            calls: 0,
            fail_after: 0,
        })),
        ..minimal_sink()
    };
    let result = sink.on_batch(&batch);
    assert!(
        result.is_err(),
        "immediate writer failure must propagate as Err"
    );
}

#[test]
fn gremlin_auto_shrink_plus_file_split_total_rows_consistent() {
    // auto_shrink (limit=1 byte) + max_file_size (1 byte) both active.
    // Every sub-batch triggers a file split. After N original batches:
    //   total_rows == sum(completed_parts[].rows) + part_rows
    // This ensures no rows are silently dropped or double-counted when
    // the two splitting mechanisms interleave.

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let mut sink = ExportSink {
        max_batch_memory_bytes: Some(1),
        batch_memory_policy: crate::tuning::BatchMemoryPolicy::AutoShrink,
        max_file_size: Some(1),
        enriched_schema: Some(schema.clone()),
        writer: Some(Box::new(SplitTriggerWriter { write_count: 0 })),
        ..minimal_sink()
    };
    // Provide real schema so on_schema-driven code paths work if needed
    sink.schema = Some(schema.clone());

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..20i64))],
    )
    .unwrap();

    // Feed 3 batches of 20 rows each
    for _ in 0..3 {
        sink.on_batch(&batch).unwrap();
    }

    let parts_rows: usize = sink.completed_parts.iter().map(|p| p.rows).sum();
    assert_eq!(
        parts_rows + sink.part_rows,
        sink.total_rows,
        "completed_parts + part_rows must equal total_rows; \
         parts_rows={parts_rows}, part_rows={}, total_rows={}",
        sink.part_rows,
        sink.total_rows
    );
    assert_eq!(sink.total_rows, 60, "all 60 rows must be counted");
}

#[test]
fn gremlin_unique_cap_exact_boundary_no_false_capped_flag() {
    // Exactly `unique_max_entries` distinct values → set reaches the cap but
    // the cap flag must NOT be set (we cap at `>= cap`, not `> cap`).
    // On the row that would push it to cap+1, the flag is set.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    // Batch of exactly 5 distinct values with cap=5 → no flag
    let exact_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();
    let mut sink = sink_with_unique_cap(vec!["id".into()], 5);
    sink.quality_unique_indices = vec![(0, "id".into())];
    sink.track_quality(&exact_batch);

    assert!(
        !sink.quality_unique_capped.contains("id"),
        "exactly cap distinct values must NOT set the capped flag"
    );
    assert_eq!(
        sink.quality_unique_sets.get("id").unwrap().len(),
        5,
        "all 5 distinct values must be tracked"
    );

    // Now add one more distinct value → cap flag must fire
    let overflow_batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![6]))]).unwrap();
    sink.track_quality(&overflow_batch);

    assert!(
        sink.quality_unique_capped.contains("id"),
        "cap+1 distinct values must set the capped flag"
    );
}

#[test]
fn gremlin_zero_row_batch_does_not_panic_or_increment_rows() {
    // A zero-row batch fed to on_batch must be a no-op: no crash, no row count change.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let empty =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]).unwrap();
    let mut sink = minimal_sink();
    sink.on_batch(&empty).unwrap();
    assert_eq!(
        sink.total_rows, 0,
        "zero-row batch must not increment total_rows"
    );
}

// ── pipelined sink equivalence ───────────────────────────────────────────

#[test]
fn pipelined_sink_output_is_byte_identical_to_synchronous() {
    use crate::pipeline::manifest_writer::compute_part_fingerprint;
    use crate::source::BatchSink;

    let batches: Vec<RecordBatch> = (0..3).map(|_| make_int_batch(100, 4)).collect();
    let schema_ref = batches[0].schema();

    // Synchronous reference.
    let mut sync_sink = minimal_sink();
    sync_sink.on_schema(schema_ref.clone()).unwrap();
    for b in &batches {
        sync_sink.on_batch(b).unwrap();
    }
    let sync_writer = sync_sink.writer.take().unwrap();
    sync_writer.finish().unwrap();
    let sync_fp = compute_part_fingerprint(sync_sink.tmp.path()).unwrap();
    let sync_rows = sync_sink.total_rows;

    // Pipelined path through the worker thread.
    let mut p = PipelinedSink::spawn_with_sink(minimal_sink());
    p.on_schema(schema_ref.clone()).unwrap();
    for b in &batches {
        p.on_batch(b).unwrap();
    }
    let mut rec = p.finish().unwrap();
    let p_writer = rec.writer.take().unwrap();
    p_writer.finish().unwrap();
    let p_fp = compute_part_fingerprint(rec.tmp.path()).unwrap();

    assert_eq!(sync_rows, 300);
    assert_eq!(rec.total_rows, sync_rows, "row count must match");
    assert_eq!(
        sync_fp, p_fp,
        "pipelined output must be byte-identical to synchronous"
    );
}
