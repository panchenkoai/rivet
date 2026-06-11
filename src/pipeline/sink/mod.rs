//! **Layer: Execution**
//!
//! `ExportSink` manages the local temp-file write path: buffers Arrow batches,
//! rotates files at `max_file_size_bytes`, and runs inline quality checks.
//! All decisions (format, compression, quality rules) come from `ResolvedRunPlan`.

mod cursor;
mod pipelined;
pub(crate) use cursor::extract_last_cursor_value;
pub(crate) use pipelined::PipelinedSink;

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
    pub(in crate::pipeline) writer: Option<Box<dyn FormatWriter + Send>>,
    pub(in crate::pipeline) format_type: FormatType,
    pub(in crate::pipeline) compression: CompressionType,
    pub(in crate::pipeline) compression_level: Option<u32>,
    pub(in crate::pipeline) tmp: tempfile::NamedTempFile,
    pub(in crate::pipeline) total_rows: usize,
    pub(in crate::pipeline) part_rows: usize,
    /// Cursor column name (with internal columns), set from plan at construction.
    /// When `Some`, `on_batch` extracts the last cursor value inline so we never
    /// hold a full batch in memory just for post-run cursor commit.
    pub(in crate::pipeline) cursor_column: Option<String>,
    /// Last extracted cursor value — set by `on_batch`, consumed by `run_single_export`.
    pub(in crate::pipeline) last_cursor_value: Option<String>,
    /// Schema WITH internal columns — used in `on_batch` for inline cursor extraction.
    pub(in crate::pipeline) schema: Option<SchemaRef>,
    /// Destination-facing schema (stripped of internal columns). Used for schema-change
    /// detection against the stored snapshot.
    pub(in crate::pipeline) dest_schema: Option<SchemaRef>,
    pub(in crate::pipeline) meta: MetaColumns,
    pub(in crate::pipeline) enriched_schema: Option<SchemaRef>,
    pub(in crate::pipeline) exported_at_us: i64,
    pub(in crate::pipeline) quality_null_counts: std::collections::HashMap<String, usize>,
    pub(in crate::pipeline) quality_unique_sets:
        std::collections::HashMap<String, std::collections::HashSet<u64>>,
    /// Per-column count of non-NULL values seen by uniqueness tracking. NULLs are
    /// never duplicates (SQL UNIQUE semantics) and are skipped from hashing, so
    /// duplicates must be computed against this count, not `total_rows`.
    pub(in crate::pipeline) quality_unique_non_null_counts:
        std::collections::HashMap<String, usize>,
    /// Columns whose unique-entry tracking was stopped because `unique_max_entries` was reached.
    pub(in crate::pipeline) quality_unique_capped: std::collections::HashSet<String>,
    pub(in crate::pipeline) quality_columns: Option<crate::config::QualityConfig>,
    /// Column index cache for quality tracking — built once in `on_schema`.
    pub(in crate::pipeline) quality_null_indices: Vec<(usize, String)>,
    pub(in crate::pipeline) quality_unique_indices: Vec<(usize, String)>,
    pub(in crate::pipeline) max_file_size: Option<u64>,
    pub(in crate::pipeline) completed_parts: Vec<CompletedPart>,
    /// When set, this column is removed from Arrow batches before enrichment and write (see `chunk_dense`).
    pub(in crate::pipeline) strip_internal_column: Option<String>,
    /// Running per-column max byte length for string/binary columns (Epic 8).
    pub(in crate::pipeline) column_max_bytes: std::collections::HashMap<String, u64>,
    /// Hard cap on a single Arrow batch in bytes (`max_batch_memory_mb * 1024²`). `None` = no cap.
    pub(in crate::pipeline) max_batch_memory_bytes: Option<usize>,
    /// Hard ceiling on a single cell/value in bytes (`max_value_mb * 1024²`).
    /// `None` = no guard. Unlike `max_batch_memory_bytes` (an average-based
    /// batch cap), this bounds one giant text/JSON/blob value that would
    /// otherwise OOM the process (OPT-1).
    pub(in crate::pipeline) max_value_bytes: Option<usize>,
    pub(in crate::pipeline) batch_memory_policy: crate::tuning::BatchMemoryPolicy,
    /// Count of batches that exceeded `max_batch_memory_bytes` (for run summary / logging).
    pub(in crate::pipeline) oversized_batch_count: u64,
    /// Parquet row group config from plan. `None` = CSV or no row group tuning.
    pub(in crate::pipeline) parquet_config: Option<crate::config::ParquetConfig>,
    /// Resolved rows-per-row-group, computed from schema in `on_schema`. `None` = library default.
    pub(in crate::pipeline) parquet_row_group_rows: Option<usize>,
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
            cursor_column: plan.strategy.cursor_extract_column().map(str::to_string),
            last_cursor_value: None,
            schema: None,
            dest_schema: None,
            meta: plan.meta_columns.clone(),
            enriched_schema: None,
            exported_at_us,
            quality_null_counts: std::collections::HashMap::new(),
            quality_unique_sets: std::collections::HashMap::new(),
            quality_unique_non_null_counts: std::collections::HashMap::new(),
            quality_unique_capped: std::collections::HashSet::new(),
            quality_columns: plan.quality.clone(),
            quality_null_indices: Vec::new(),
            quality_unique_indices: Vec::new(),
            max_file_size: plan.max_file_size_bytes,
            completed_parts: Vec::new(),
            strip_internal_column,
            column_max_bytes: std::collections::HashMap::new(),
            max_batch_memory_bytes: plan.tuning.max_batch_memory_mb.map(|mb| mb * 1024 * 1024),
            // `0` (or None) disables the per-value guard; otherwise convert MB→bytes.
            max_value_bytes: plan
                .tuning
                .max_value_mb
                .filter(|&mb| mb > 0)
                .map(|mb| mb * 1024 * 1024),
            batch_memory_policy: plan.tuning.on_batch_memory_exceeded,
            oversized_batch_count: 0,
            parquet_config: plan.parquet.clone(),
            parquet_row_group_rows: None,
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
            let fmt = format::create_format(
                self.format_type,
                self.compression,
                self.compression_level,
                self.parquet_row_group_rows,
            );
            let file = self.tmp.as_file().try_clone()?;
            let buf_writer = BufWriter::with_capacity(256 * 1024, file);
            self.writer = Some(fmt.create_writer(schema, Box::new(buf_writer))?);
        }

        log::info!(
            "file split: started part {}",
            self.completed_parts.len() + 1
        );
        Ok(())
    }

    pub fn track_quality(&mut self, batch: &RecordBatch) {
        if self.quality_columns.is_none() {
            return;
        }
        for (i, name) in &self.quality_null_indices {
            *self.quality_null_counts.entry(name.clone()).or_default() +=
                batch.column(*i).null_count();
        }
        if self.quality_unique_indices.is_empty() {
            return;
        }
        let cap = self
            .quality_columns
            .as_ref()
            .and_then(|q| q.unique_max_entries);
        use std::io::Write as _;
        use xxhash_rust::xxh3::xxh3_64;
        let fmt_options = arrow::util::display::FormatOptions::default();
        let mut scratch = Vec::with_capacity(64);
        for (i, name) in &self.quality_unique_indices {
            if self.quality_unique_capped.contains(name) {
                continue;
            }
            let col = batch.column(*i);
            let non_null_count = self
                .quality_unique_non_null_counts
                .entry(name.clone())
                .or_default();
            let set = self.quality_unique_sets.entry(name.clone()).or_default();
            if let Ok(formatter) =
                arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &fmt_options)
            {
                for row in 0..col.len() {
                    // NULLs are never duplicates (SQL UNIQUE semantics): skip
                    // before the cap check so trailing NULLs can't trip the cap.
                    if col.is_null(row) {
                        continue;
                    }
                    if let Some(limit) = cap
                        && set.len() >= limit
                    {
                        self.quality_unique_capped.insert(name.clone());
                        break;
                    }
                    scratch.clear();
                    let _ = write!(scratch, "{}", formatter.value(row));
                    set.insert(xxh3_64(&scratch));
                    *non_null_count += 1;
                }
            }
        }
    }

    /// Hard per-value guard (OPT-1): abort with `RIVET_VALUE_TOO_LARGE` when a
    /// single variable-length cell exceeds `max_value_bytes`. Only Utf8 /
    /// LargeUtf8 / Binary / LargeBinary values can be individually huge; fixed-
    /// width types (ints, floats, dates) cannot, so they need no check. Runs
    /// before the batch is split/encoded so the giant cell never reaches the
    /// row-group writer or the auto-shrink splitter (which can't divide one
    /// oversized value). `O(rows × var-length-cols)` length reads — no copies.
    fn check_value_ceiling(&self, batch: &RecordBatch) -> Result<()> {
        use arrow::array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
        use arrow::datatypes::DataType;

        let Some(limit) = self.max_value_bytes else {
            return Ok(());
        };
        let schema = batch.schema();
        for (idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx);
            let over: Option<usize> = match field.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len()).find(|&n| n > limit)),
                DataType::LargeUtf8 => col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len()).find(|&n| n > limit)),
                DataType::Binary => col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len()).find(|&n| n > limit)),
                DataType::LargeBinary => col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len()).find(|&n| n > limit)),
                _ => None,
            };
            if let Some(value_bytes) = over {
                anyhow::bail!(
                    "RIVET_VALUE_TOO_LARGE: column '{}' has a single value of {:.1} MB, exceeding the \
                     per-value ceiling of {} MB. One oversized cell can OOM the process regardless of \
                     batch size. Raise `tuning.max_value_mb` (or set it to 0 to disable the guard) if \
                     this value is expected.",
                    field.name(),
                    value_bytes as f64 / (1024.0 * 1024.0),
                    limit / (1024 * 1024),
                );
            }
        }
        Ok(())
    }

    /// Update the running per-column max byte length for string/binary columns.
    pub fn track_shape(&mut self, batch: &RecordBatch) {
        use arrow::array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
        use arrow::datatypes::DataType;

        let schema = batch.schema();
        for (idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx);
            let batch_max: Option<u64> = match field.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len() as u64).max()),
                DataType::LargeUtf8 => col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .and_then(|a| a.iter().flatten().map(|s| s.len() as u64).max()),
                DataType::Binary => col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len() as u64).max()),
                DataType::LargeBinary => col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .and_then(|a| a.iter().flatten().map(|b| b.len() as u64).max()),
                _ => None,
            };
            if let Some(m) = batch_max {
                let entry = self
                    .column_max_bytes
                    .entry(field.name().clone())
                    .or_insert(0);
                if m > *entry {
                    *entry = m;
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
                if self.quality_unique_capped.contains(col) {
                    let cap = qc.unique_max_entries.unwrap_or(0);
                    issues.push(crate::quality::QualityIssue {
                        severity: crate::quality::Severity::Warn,
                        message: format!(
                            "column '{}': uniqueness check capped at {} entries; \
                             result may be incomplete (set unique_max_entries higher to cover all rows)",
                            col, cap
                        ),
                    });
                } else if let Some(set) = self.quality_unique_sets.get(col) {
                    let non_null = self
                        .quality_unique_non_null_counts
                        .get(col)
                        .copied()
                        .unwrap_or(0);
                    let dupes = non_null.saturating_sub(set.len());
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

    /// Core batch processing: track quality/shape, enrich, write. Called after memory check.
    fn on_batch_inner(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        self.total_rows += dest_batch.num_rows();
        self.part_rows += dest_batch.num_rows();
        self.track_quality(dest_batch);
        self.track_shape(dest_batch);

        let output = if let Some(es) = &self.enriched_schema {
            enrich::enrich_batch(dest_batch, &self.meta, es, self.exported_at_us)?
        } else {
            dest_batch.clone()
        };

        if let Some(w) = self.writer.as_mut() {
            w.write_batch(&output)?;
        }
        self.maybe_split()?;
        Ok(())
    }
}

impl ExportSink {
    /// Apply memory-cap policy and write `dest_batch` (already stripped of internal columns).
    ///
    /// Separating this from `on_batch` lets `AutoShrink` recurse through the full memory
    /// check on every sub-batch, not just the first split level.
    fn process_dest_batch(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        if let Some(limit) = self.max_batch_memory_bytes {
            let batch_bytes = crate::tuning::SourceTuning::batch_memory_bytes(dest_batch);
            if batch_bytes > limit {
                let batch_mb = batch_bytes / (1024 * 1024);
                let limit_mb = limit / (1024 * 1024);
                let suggested = dest_batch
                    .num_rows()
                    .saturating_mul(limit)
                    .checked_div(batch_bytes)
                    .unwrap_or(1)
                    .max(1);
                match self.batch_memory_policy {
                    crate::tuning::BatchMemoryPolicy::Warn => {
                        log::warn!(
                            "batch memory {} MB exceeds max_batch_memory_mb={} MB \
                             ({} rows). Consider lowering batch_size to ~{}.",
                            batch_mb,
                            limit_mb,
                            dest_batch.num_rows(),
                            suggested
                        );
                    }
                    crate::tuning::BatchMemoryPolicy::Fail => {
                        anyhow::bail!(
                            "batch memory {} MB exceeds max_batch_memory_mb={} MB \
                             ({} rows). Lower batch_size to ~{} or set \
                             on_batch_memory_exceeded: auto_shrink.",
                            batch_mb,
                            limit_mb,
                            dest_batch.num_rows(),
                            suggested
                        );
                    }
                    crate::tuning::BatchMemoryPolicy::AutoShrink => {
                        let mid = dest_batch.num_rows() / 2;
                        if mid == 0 {
                            // Single-row batch already over limit — warn and write as-is.
                            log::warn!(
                                "single-row batch is {} MB — cannot shrink further, writing as-is.",
                                batch_mb
                            );
                        } else {
                            let lo = dest_batch.slice(0, mid);
                            let hi = dest_batch.slice(mid, dest_batch.num_rows() - mid);
                            self.process_dest_batch(&lo)?;
                            self.process_dest_batch(&hi)?;
                            return Ok(());
                        }
                    }
                }
            }
        }
        self.on_batch_inner(dest_batch)
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
        // Compute row group rows from the actual schema now that it's available.
        if let Some(pc) = &self.parquet_config {
            self.parquet_row_group_rows = pc.effective_row_group_rows(&dest_schema);
            if let Some(rows) = self.parquet_row_group_rows {
                log::debug!(
                    "parquet row_group_rows={} (strategy={:?})",
                    rows,
                    pc.row_group_strategy.unwrap_or_default()
                );
            }
        }
        // Warn loud (#6/#29, CLAUDE.md "never a silent no-op"): `max_file_size`
        // is enforced by `maybe_split` comparing the writer's FLUSHED bytes
        // against the cap. parquet-rs only flushes on row-group close, so with
        // the library-default (~1M-row) row group an export below one group
        // flushes ~nothing and the cap silently never fires. Tell the operator
        // their declared cap won't engage unless they constrain the row group
        // (or switch to CSV). Once per process so chunked runs don't spam.
        if self.format_type == FormatType::Parquet
            && self.max_file_size.is_some()
            && self.parquet_row_group_rows.is_none()
        {
            static WARN_ONCE: std::sync::Once = std::sync::Once::new();
            WARN_ONCE.call_once(|| {
                log::warn!(
                    "max_file_size is set but will NOT be enforced for this parquet export: \
                     parquet only flushes bytes when a row group closes, and no \
                     `parquet.row_group_rows` is configured (library default ~1M rows), so a \
                     file below one row group never reaches the cap. Set a small \
                     `parquet.row_group_rows` to make max_file_size effective, or use \
                     `format: csv`."
                );
            });
        }
        let fmt = format::create_format(
            self.format_type,
            self.compression,
            self.compression_level,
            self.parquet_row_group_rows,
        );
        let file = self.tmp.as_file().try_clone()?;
        let buf_writer = BufWriter::new(file);
        self.writer = Some(fmt.create_writer(&enriched, Box::new(buf_writer))?);
        // Build quality field index cache from dest_schema (after stripping internal cols).
        if let Some(qc) = &self.quality_columns {
            // Fail loud (#33, CLAUDE.md "never a silent no-op"): a quality rule
            // naming a column the export does not produce would otherwise be
            // dropped by the `contains`/`index_of` filters below and report
            // `quality: pass` over a gate that never ran. Validate names against
            // the real schema the moment it resolves, before any batch.
            let available: Vec<String> = dest_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            crate::quality::validate_quality_columns(qc, &available)?;
            self.quality_null_indices = dest_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| qc.null_ratio_max.contains_key(f.name().as_str()))
                .map(|(i, f)| (i, f.name().clone()))
                .collect();
            self.quality_unique_indices = dest_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| qc.unique_columns.contains(f.name()))
                .map(|(i, f)| (i, f.name().clone()))
                .collect();
        }
        // `schema` keeps internal columns so cursor extraction (e.g. synthetic
        // `_rivet_coalesced_cursor`) can index by name. `dest_schema` is what
        // downstream consumers see — used for schema-change detection.
        self.schema = Some(schema);
        self.dest_schema = Some(dest_schema);
        self.enriched_schema = Some(enriched);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Avoid cloning the batch when no internal column needs to be stripped.
        let stripped: Option<RecordBatch> = match &self.strip_internal_column {
            Some(strip) if batch.schema().index_of(strip).is_ok() => {
                Some(Self::record_batch_without_internal(batch, strip)?)
            }
            _ => None,
        };
        let dest_batch: &RecordBatch = stripped.as_ref().unwrap_or(batch);

        // OPT-1: fail fast on a single oversized cell, before the batch is split
        // or encoded (the auto-shrink splitter can't divide one giant value).
        self.check_value_ceiling(dest_batch)?;

        // Count original batches that exceed the memory cap (before any splitting).
        if let Some(limit) = self.max_batch_memory_bytes
            && crate::tuning::SourceTuning::batch_memory_bytes(dest_batch) > limit
        {
            self.oversized_batch_count += 1;
        }

        self.process_dest_batch(dest_batch)?;

        // Extract cursor value inline so the batch can be freed immediately after
        // on_batch returns — avoids holding one full batch in memory for the rest of the run.
        if let (Some(col), Some(schema)) = (&self.cursor_column, &self.schema) {
            self.last_cursor_value = extract_last_cursor_value(batch, col, schema);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
