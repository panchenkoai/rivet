use std::io::BufWriter;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::chunked::RIVET_CHUNK_RN_COL;
use crate::config::{CompressionType, ExportConfig, FormatType, MetaColumns};
use crate::enrich;
use crate::error::Result;
use crate::format::{self, FormatWriter};
use crate::source::BatchSink;

pub(crate) struct CompletedPart {
    pub tmp: tempfile::NamedTempFile,
    pub rows: usize,
}

pub(crate) struct ExportSink {
    pub writer: Option<Box<dyn FormatWriter>>,
    pub format_type: FormatType,
    pub compression: CompressionType,
    pub compression_level: Option<u32>,
    pub tmp: tempfile::NamedTempFile,
    pub total_rows: usize,
    pub part_rows: usize,
    pub last_batch: Option<RecordBatch>,
    pub schema: Option<SchemaRef>,
    pub meta: MetaColumns,
    pub enriched_schema: Option<SchemaRef>,
    pub exported_at_us: i64,
    pub quality_null_counts: std::collections::HashMap<String, usize>,
    pub quality_unique_sets: std::collections::HashMap<String, std::collections::HashSet<String>>,
    pub quality_columns: Option<crate::config::QualityConfig>,
    pub max_file_size: Option<u64>,
    pub completed_parts: Vec<CompletedPart>,
    /// When set, this column is removed from Arrow batches before enrichment and write (see `chunk_dense`).
    pub strip_internal_column: Option<String>,
}

impl ExportSink {
    pub fn new(export: &ExportConfig) -> Result<Self> {
        let tmp = tempfile::NamedTempFile::new()?;
        let exported_at_us = chrono::Utc::now().timestamp_micros();
        Ok(Self {
            writer: None,
            format_type: export.format,
            compression: export.compression,
            compression_level: export.compression_level,
            tmp,
            total_rows: 0,
            part_rows: 0,
            last_batch: None,
            schema: None,
            meta: export.meta_columns.clone(),
            enriched_schema: None,
            exported_at_us,
            quality_null_counts: std::collections::HashMap::new(),
            quality_unique_sets: std::collections::HashMap::new(),
            quality_columns: export.quality.clone(),
            max_file_size: export.max_file_size_bytes(),
            completed_parts: Vec::new(),
            strip_internal_column: if export.chunk_dense {
                Some(RIVET_CHUNK_RN_COL.to_string())
            } else {
                None
            },
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
        let data_schema = if let Some(ref strip) = self.strip_internal_column {
            Self::schema_without_internal(schema.as_ref(), strip)?
        } else {
            schema.clone()
        };
        let enriched = enrich::enrich_schema(&data_schema, &self.meta);
        let fmt = format::create_format(self.format_type, self.compression, self.compression_level);
        let file = self.tmp.as_file().try_clone()?;
        let buf_writer = BufWriter::new(file);
        self.writer = Some(fmt.create_writer(&enriched, Box::new(buf_writer))?);
        self.schema = Some(data_schema);
        self.enriched_schema = Some(enriched);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let batch = if let Some(ref strip) = self.strip_internal_column {
            Self::record_batch_without_internal(batch, strip)?
        } else {
            batch.clone()
        };

        self.total_rows += batch.num_rows();
        self.part_rows += batch.num_rows();
        self.track_quality(&batch);

        let output = if let Some(es) = &self.enriched_schema {
            enrich::enrich_batch(&batch, &self.meta, es, self.exported_at_us)?
        } else {
            batch.clone()
        };

        if let Some(w) = self.writer.as_mut() {
            w.write_batch(&output)?;
        }
        self.last_batch = Some(batch);
        self.maybe_split()?;
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) fn build_file_name(export_name: &str, extension: &str) -> String {
    let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    format!("{}_{}.{}", export_name, now, extension)
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
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_build_file_name() {
        let name = build_file_name("users", "csv");
        assert!(name.starts_with("users_"));
        assert!(name.ends_with(".csv"));
    }

    #[test]
    fn test_extract_cursor_int64() {
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
    fn test_extract_cursor_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }
}
