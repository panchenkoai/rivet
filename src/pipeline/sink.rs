//! **Layer: Execution**
//!
//! `ExportSink` manages the local temp-file write path: buffers Arrow batches,
//! rotates files at `max_file_size_bytes`, and runs inline quality checks.
//! All decisions (format, compression, quality rules) come from `ResolvedRunPlan`.

use std::io::BufWriter;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::chunked::RIVET_CHUNK_RN_COL;
use crate::config::IncrementalCursorMode;
use crate::enrich;
use crate::error::Result;
use crate::format::{self, FormatWriter};
use crate::plan::{
    CompressionType, ExtractionStrategy, FormatType, IncrementalCursorPlan, MetaColumns,
    ResolvedRunPlan,
};
use crate::source::BatchSink;

pub(crate) struct CompletedPart {
    pub(in crate::pipeline) tmp: tempfile::NamedTempFile,
    pub(in crate::pipeline) rows: usize,
}

pub(crate) struct ExportSink {
    pub(in crate::pipeline) writer: Option<Box<dyn FormatWriter>>,
    pub(in crate::pipeline) format_type: FormatType,
    pub(in crate::pipeline) compression: CompressionType,
    pub(in crate::pipeline) compression_level: Option<u32>,
    pub(in crate::pipeline) tmp: tempfile::NamedTempFile,
    pub(in crate::pipeline) total_rows: usize,
    pub(in crate::pipeline) part_rows: usize,
    /// Last batch WITH internal columns still present — used for cursor extraction
    /// (e.g. `_rivet_coalesced_cursor`). Destination-facing batches (stripped of
    /// internal columns) are built on the fly in `on_batch`.
    pub(in crate::pipeline) last_batch: Option<RecordBatch>,
    /// Schema WITH internal columns — matches `last_batch`. `extract_last_cursor_value`
    /// indexes by column name, so this must include the synthetic coalesce column when present.
    pub(in crate::pipeline) schema: Option<SchemaRef>,
    /// Destination-facing schema (stripped of internal columns). Used for schema-change
    /// detection against the stored snapshot.
    pub(in crate::pipeline) dest_schema: Option<SchemaRef>,
    pub(in crate::pipeline) meta: MetaColumns,
    pub(in crate::pipeline) enriched_schema: Option<SchemaRef>,
    pub(in crate::pipeline) exported_at_us: i64,
    pub(in crate::pipeline) quality_null_counts: std::collections::HashMap<String, usize>,
    pub(in crate::pipeline) quality_unique_sets:
        std::collections::HashMap<String, std::collections::HashSet<String>>,
    pub(in crate::pipeline) quality_columns: Option<crate::config::QualityConfig>,
    pub(in crate::pipeline) max_file_size: Option<u64>,
    pub(in crate::pipeline) completed_parts: Vec<CompletedPart>,
    /// When set, this column is removed from Arrow batches before enrichment and write (see `chunk_dense`).
    pub(in crate::pipeline) strip_internal_column: Option<String>,
}

impl ExportSink {
    pub fn new(plan: &ResolvedRunPlan) -> Result<Self> {
        let tmp = tempfile::NamedTempFile::new()?;
        let exported_at_us = chrono::Utc::now().timestamp_micros();
        let strip_internal_column = match &plan.strategy {
            ExtractionStrategy::Chunked(cp) if cp.dense => Some(RIVET_CHUNK_RN_COL.to_string()),
            ExtractionStrategy::Incremental(p) if p.mode == IncrementalCursorMode::Coalesce => {
                Some(IncrementalCursorPlan::RIVET_COALESCE_CURSOR_COL.to_string())
            }
            _ => None,
        };
        Ok(Self {
            writer: None,
            format_type: plan.format,
            compression: plan.compression,
            compression_level: plan.compression_level,
            tmp,
            total_rows: 0,
            part_rows: 0,
            last_batch: None,
            schema: None,
            dest_schema: None,
            meta: plan.meta_columns.clone(),
            enriched_schema: None,
            exported_at_us,
            quality_null_counts: std::collections::HashMap::new(),
            quality_unique_sets: std::collections::HashMap::new(),
            quality_columns: plan.quality.clone(),
            max_file_size: plan.max_file_size_bytes,
            completed_parts: Vec::new(),
            strip_internal_column,
        })
    }

    fn schema_without_internal(schema: &Schema, name: &str) -> Result<SchemaRef> {
        let idx = schema.index_of(name)?;
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    }

    fn record_batch_without_internal(batch: &RecordBatch, name: &str) -> Result<RecordBatch> {
        let schema = batch.schema();
        let idx = schema.index_of(name)?;
        let indices: Vec<usize> = (0..schema.fields().len()).filter(|&i| i != idx).collect();
        batch
            .project(&indices)
            .map_err(|e| anyhow::anyhow!("project batch without {}: {}", name, e))
    }

    pub fn maybe_split(&mut self) -> Result<()> {
        let max = match self.max_file_size {
            Some(m) => m,
            None => return Ok(()),
        };
        let written = self.writer.as_ref().map(|w| w.bytes_written()).unwrap_or(0);
        if written < max || self.part_rows == 0 {
            return Ok(());
        }

        if let Some(w) = self.writer.take() {
            w.finish()?;
        }

        let old_tmp = std::mem::replace(&mut self.tmp, tempfile::NamedTempFile::new()?);
        self.completed_parts.push(CompletedPart {
            tmp: old_tmp,
            rows: self.part_rows,
        });
        self.part_rows = 0;

        if let Some(schema) = &self.enriched_schema {
            let fmt =
                format::create_format(self.format_type, self.compression, self.compression_level);
            let file = self.tmp.as_file().try_clone()?;
            let buf_writer = BufWriter::new(file);
            self.writer = Some(fmt.create_writer(schema, Box::new(buf_writer))?);
        }

        log::info!(
            "file split: started part {}",
            self.completed_parts.len() + 1
        );
        Ok(())
    }

    pub fn track_quality(&mut self, batch: &RecordBatch) {
        let qc = match &self.quality_columns {
            Some(q) => q,
            None => return,
        };
        let schema = batch.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            if qc.null_ratio_max.contains_key(name.as_str()) {
                *self.quality_null_counts.entry(name.clone()).or_default() +=
                    batch.column(i).null_count();
            }
            if qc.unique_columns.contains(name) {
                let col = batch.column(i);
                let set = self.quality_unique_sets.entry(name.clone()).or_default();
                if let Ok(formatter) = arrow::util::display::ArrayFormatter::try_new(
                    col.as_ref(),
                    &arrow::util::display::FormatOptions::default(),
                ) {
                    for row in 0..col.len() {
                        set.insert(formatter.value(row).to_string());
                    }
                }
            }
        }
    }

    pub fn run_quality_checks(&self) -> Vec<crate::quality::QualityIssue> {
        let qc = match &self.quality_columns {
            Some(q) => q,
            None => return Vec::new(),
        };
        let mut issues = Vec::new();
        issues.extend(crate::quality::check_row_count(self.total_rows, qc));

        if self.total_rows > 0 {
            for (col, max_ratio) in &qc.null_ratio_max {
                let nulls = self.quality_null_counts.get(col).copied().unwrap_or(0);
                let ratio = nulls as f64 / self.total_rows as f64;
                if ratio > *max_ratio {
                    issues.push(crate::quality::QualityIssue {
                        severity: crate::quality::Severity::Fail,
                        message: format!(
                            "column '{}': null ratio {:.4} exceeds threshold {:.4}",
                            col, ratio, max_ratio
                        ),
                    });
                }
            }

            for col in &qc.unique_columns {
                if let Some(set) = self.quality_unique_sets.get(col) {
                    let dupes = self.total_rows.saturating_sub(set.len());
                    if dupes > 0 {
                        issues.push(crate::quality::QualityIssue {
                            severity: crate::quality::Severity::Fail,
                            message: format!(
                                "column '{}': {} duplicate values out of {} rows",
                                col, dupes, self.total_rows
                            ),
                        });
                    }
                }
            }
        }
        issues
    }
}

impl BatchSink for ExportSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        // Strip the synthetic column only when it's actually present in the schema —
        // empty-schema fallbacks (zero-row runs) otherwise error on missing field.
        let dest_schema = match &self.strip_internal_column {
            Some(strip) if schema.index_of(strip).is_ok() => {
                Self::schema_without_internal(schema.as_ref(), strip)?
            }
            _ => schema.clone(),
        };
        let enriched = enrich::enrich_schema(&dest_schema, &self.meta);
        let fmt = format::create_format(self.format_type, self.compression, self.compression_level);
        let file = self.tmp.as_file().try_clone()?;
        let buf_writer = BufWriter::new(file);
        self.writer = Some(fmt.create_writer(&enriched, Box::new(buf_writer))?);
        // `schema` keeps internal columns so cursor extraction (e.g. synthetic
        // `_rivet_coalesced_cursor`) can index by name. `dest_schema` is what
        // downstream consumers see — used for schema-change detection.
        self.schema = Some(schema);
        self.dest_schema = Some(dest_schema);
        self.enriched_schema = Some(enriched);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let dest_batch = match &self.strip_internal_column {
            Some(strip) if batch.schema().index_of(strip).is_ok() => {
                Self::record_batch_without_internal(batch, strip)?
            }
            _ => batch.clone(),
        };

        self.total_rows += dest_batch.num_rows();
        self.part_rows += dest_batch.num_rows();
        self.track_quality(&dest_batch);

        let output = if let Some(es) = &self.enriched_schema {
            enrich::enrich_batch(&dest_batch, &self.meta, es, self.exported_at_us)?
        } else {
            dest_batch.clone()
        };

        if let Some(w) = self.writer.as_mut() {
            w.write_batch(&output)?;
        }
        // Keep the RAW batch (with internal columns) so cursor extraction can
        // read the synthetic coalesce column after the run finishes.
        self.last_batch = Some(batch.clone());
        self.maybe_split()?;
        Ok(())
    }
}

pub(crate) fn extract_last_cursor_value(
    batch: &RecordBatch,
    cursor_column: &str,
    schema: &SchemaRef,
) -> Option<String> {
    let col_idx = schema.index_of(cursor_column).ok()?;
    let array = batch.column(col_idx);
    let last_row = batch.num_rows().checked_sub(1)?;

    if array.is_null(last_row) {
        return None;
    }

    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    match array.data_type() {
        DataType::Int16 => Some(
            array
                .as_any()
                .downcast_ref::<Int16Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Int32 => Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Utf8 => Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(last_row);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        }
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()?
                .value(last_row);
            let date =
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1)? + chrono::Duration::days(days as i64);
            Some(date.to_string())
        }
        _ => {
            log::warn!("cannot extract cursor for type {:?}", array.data_type());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    // ─── extract_last_cursor_value: every Arrow type branch ──────

    #[test]
    fn cursor_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("30".into())
        );
    }

    #[test]
    fn cursor_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("200".into())
        );
    }

    #[test]
    fn cursor_int16() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int16, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int16Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("3".into())
        );
    }

    #[test]
    fn cursor_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![1.5, 2.7]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "score", &schema).unwrap();
        assert!(val.starts_with("2.7"), "got: {val}");
    }

    #[test]
    fn cursor_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["aaa", "zzz"]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "key", &schema),
            Some("zzz".into())
        );
    }

    #[test]
    fn cursor_timestamp_microsecond() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let micros = 1_700_000_000_000_000i64; // 2023-11-14T22:13:20
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![micros]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "ts", &schema).unwrap();
        assert!(
            val.starts_with("2023-11-14T22:13:20"),
            "unexpected ts: {val}"
        );
    }

    #[test]
    fn cursor_date32() {
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
        let days = 19723i32; // 2024-01-01
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![days]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "d", &schema),
            Some("2024-01-01".into())
        );
    }

    #[test]
    fn cursor_null_last_row_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn cursor_missing_column_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "nonexistent", &schema),
            None
        );
    }

    #[test]
    fn cursor_empty_batch_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn cursor_unsupported_type_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bin",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![b"hello".as_slice()]))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "bin", &schema), None);
    }

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
        let stripped =
            ExportSink::record_batch_without_internal(&batch, "_rivet_chunk_rn").unwrap();
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
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                None,
                Some("d"),
            ]))],
        )
        .unwrap();

        let mut sink = minimal_sink_with_quality(vec!["name".into()], vec![]);
        sink.track_quality(&batch);
        assert_eq!(sink.quality_null_counts.get("name"), Some(&2));
    }

    #[test]
    fn track_quality_counts_uniques() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3]))])
                .unwrap();

        let mut sink = minimal_sink_with_quality(vec![], vec!["id".into()]);
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
        set.insert("1".into());
        set.insert("2".into());
        set.insert("3".into());
        sink.quality_unique_sets.insert("id".into(), set);
        let issues = sink.run_quality_checks();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.contains("duplicate"));
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
            last_batch: None,
            schema: None,
            dest_schema: None,
            meta: crate::config::MetaColumns::default(),
            enriched_schema: None,
            exported_at_us: 0,
            quality_null_counts: std::collections::HashMap::new(),
            quality_unique_sets: std::collections::HashMap::new(),
            quality_columns: None,
            max_file_size: None,
            completed_parts: Vec::new(),
            strip_internal_column: None,
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
            }),
            ..minimal_sink()
        }
    }
}
