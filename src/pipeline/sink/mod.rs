//! **Layer: Execution**
//!
//! `ExportSink` manages the local temp-file write path: buffers Arrow batches,
//! rotates files at `max_file_size_bytes`, and runs inline quality checks.
//! All decisions (format, compression, quality rules) come from `ResolvedRunPlan`.

mod cursor;
mod pipelined;
pub(crate) use cursor::{extract_first_cursor_value, extract_last_cursor_value};
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
use crate::plan::rollover::PartitionUnit;
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
    /// The RUN-wide bytes-read counter, shared from `plan.bytes_read` (every
    /// sink this run creates — per chunk, per worker — increments the same
    /// `Arc`, so accumulation is runner-agnostic by construction; see #175).
    pub(in crate::pipeline) bytes_read: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(in crate::pipeline) part_rows: usize,
    /// Cursor column name (with internal columns), set from plan at construction.
    /// When `Some`, `on_batch` extracts the last cursor value inline so we never
    /// hold a full batch in memory just for post-run cursor commit.
    pub(in crate::pipeline) cursor_column: Option<String>,
    /// Columns the settle window compares; must be date/timestamp.
    pub(in crate::pipeline) settle_columns: Vec<String>,
    /// Last extracted cursor value — set by `on_batch`, consumed by `run_single_export`.
    pub(in crate::pipeline) last_cursor_value: Option<String>,
    /// First observed cursor value of the RUN (first non-null of the first
    /// batch carrying one) — the floor for cursor_min metrics (#151).
    pub(in crate::pipeline) first_cursor_value: Option<String>,
    /// A lossless keyset token the SOURCE reported via `set_source_cursor`
    /// (MongoDB's BSON `_id`), overriding the type-ambiguous string extracted
    /// from the output column. `effective_cursor()` prefers it. `None` for SQL.
    pub(in crate::pipeline) source_cursor: Option<String>,
    /// Schema WITH internal columns — used in `on_batch` for inline cursor extraction.
    pub(in crate::pipeline) schema: Option<SchemaRef>,
    /// Destination-facing schema (stripped of internal columns). Used for schema-change
    /// detection against the stored snapshot.
    pub(in crate::pipeline) dest_schema: Option<SchemaRef>,
    pub(in crate::pipeline) meta: MetaColumns,
    pub(in crate::pipeline) enriched_schema: Option<SchemaRef>,
    pub(in crate::pipeline) exported_at_us: i64,
    /// The declared quality rules and everything accumulated against them.
    pub(in crate::pipeline) quality: QualityTracker,
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
    /// Form B value-checksum accumulator: per-column xxh3 (keyed to the cursor
    /// column when present — `xxh3(key ‖ value)`), XOR-combined over the export,
    /// keyed by column NAME. Surfaced into the manifest by `finalize`; `rivet
    /// validate` re-reads to catch an Arrow→Parquet encode / post-write fault the
    /// in-process Form A check cannot see. Always tracked (cheap, one pass over
    /// the batch the sink already holds).
    pub(in crate::pipeline) column_checksums: std::collections::BTreeMap<String, u64>,
    /// Index of the cursor/key column in the dest batch (the pk for the keyed
    /// checksum), resolved in `on_schema`. `None` = un-keyed (full export, or a
    /// stripped/synthetic cursor not present in the dest batch).
    pub(in crate::pipeline) checksum_key_col: Option<usize>,
    /// Per-batch row-progress feed (chunked exports). `None` for paths that
    /// don't drive a progress bar.
    pub(in crate::pipeline) row_progress: Option<RowProgress>,
    /// The warehouse partition budget the CURRENT part is kept inside, and what it has
    /// spent — see [`PartBudget`].
    pub(in crate::pipeline) partition: PartBudget,
}

/// Whether the byte cap closes the current part: a cap is declared, the part has
/// reached it, and it holds at least one row (an empty part is never shipped).
fn should_split(written: u64, max_file_size: Option<u64>, part_rows: usize) -> bool {
    max_file_size.is_some_and(|max| written >= max) && part_rows > 0
}

/// The unit an Arrow date/timestamp type stores, or `None` for a type no warehouse
/// partitions by — the signal `on_schema` turns into a warning rather than silence.
fn partition_unit_of(data_type: &arrow::datatypes::DataType) -> Option<PartitionUnit> {
    use arrow::datatypes::{DataType, TimeUnit};
    match data_type {
        DataType::Date32 => Some(PartitionUnit::Days),
        DataType::Date64 => Some(PartitionUnit::Millis),
        DataType::Timestamp(TimeUnit::Second, _) => Some(PartitionUnit::Seconds),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(PartitionUnit::Millis),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(PartitionUnit::Micros),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(PartitionUnit::Nanos),
        _ => None,
    }
}

/// The partition column's raw stored values, widened to `i64`. `None` when the array is
/// not one of the date/timestamp arrays `partition_unit_of` admits.
fn partition_values(col: &dyn arrow::array::Array) -> Option<Vec<i64>> {
    use arrow::array::{
        Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<Date32Array>() {
        return Some(a.values().iter().map(|v| i64::from(*v)).collect());
    }
    any.downcast_ref::<Date64Array>()
        .map(|a| a.values().to_vec())
        .or_else(|| {
            any.downcast_ref::<TimestampSecondArray>()
                .map(|a| a.values().to_vec())
        })
        .or_else(|| {
            any.downcast_ref::<TimestampMillisecondArray>()
                .map(|a| a.values().to_vec())
        })
        .or_else(|| {
            any.downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| a.values().to_vec())
        })
        .or_else(|| {
            any.downcast_ref::<TimestampNanosecondArray>()
                .map(|a| a.values().to_vec())
        })
}

/// The warehouse partition budget the CURRENT part is kept inside, and what it has
/// spent. The writer is the only layer that can keep a part within the load job's
/// partition cap, so the count lives here — per part, hence reset on every rotation.
#[derive(Default)]
pub(in crate::pipeline) struct PartBudget {
    /// The declared budget; `None` leaves part sizing to `max_file_size` alone.
    pub(in crate::pipeline) rollover: Option<crate::plan::rollover::PartitionRollover>,
    /// The partition column's index in the DEST batch and the unit its Arrow type stores,
    /// resolved by [`Self::resolve`]. `None` when the export does not carry that column.
    pub(in crate::pipeline) col: Option<(usize, PartitionUnit)>,
    /// Distinct partitions the current part already holds — the budget spent so far.
    pub(in crate::pipeline) buckets: std::collections::HashSet<i64>,
}

impl PartBudget {
    pub(in crate::pipeline) fn new(
        rollover: Option<crate::plan::rollover::PartitionRollover>,
    ) -> Self {
        Self {
            rollover,
            ..Self::default()
        }
    }

    /// Resolve the partition column against the DEST schema. Both failures WARN rather
    /// than disable quietly (#6/#29): an uncounted part is one the warehouse may refuse
    /// to load outright, and that must not first be discovered at load time.
    fn resolve(&mut self, dest_schema: &arrow::datatypes::Schema) {
        self.col = None;
        // A zero-row run declares an empty schema: there is no part to budget, and nothing is missing.
        if dest_schema.fields().is_empty() {
            return;
        }
        if let Some(r) = self.rollover.clone() {
            match dest_schema.field_with_name(&r.column) {
                Ok(field) => match partition_unit_of(field.data_type()) {
                    Some(unit) => {
                        self.col = dest_schema.index_of(&r.column).ok().map(|i| (i, unit));
                    }
                    None => log::warn!(
                        "the load partitions by `{}`, which this export writes as {} — not a \
                         date or timestamp, so parts cannot be kept within the {}-partition \
                         load budget and a wide history will be refused at load time",
                        r.column,
                        field.data_type(),
                        r.cap
                    ),
                },
                Err(_) => log::warn!(
                    "the load partitions by `{}`, which this export does not produce — parts \
                     cannot be kept within the {}-partition load budget",
                    r.column,
                    r.cap
                ),
            }
        }
    }

    /// The partition bucket of every row of `batch`, with the budget they are counted
    /// against. `None` when this export is not budgeted (no column partition, or the
    /// column is absent / not a date) — the caller then writes the batch unchanged.
    fn buckets_for(&self, batch: &RecordBatch) -> Option<(Vec<i64>, usize)> {
        use crate::plan::rollover::{NULL_BUCKET, bucket_of, to_epoch_seconds};
        let (idx, unit) = self.col?;
        let granularity = self.rollover.as_ref()?.granularity;
        let cap = self.rollover.as_ref()?.cap;
        let col = batch.column(idx);
        let raw = partition_values(col.as_ref())?;
        let buckets = (0..batch.num_rows())
            .map(|row| {
                if col.is_null(row) {
                    NULL_BUCKET
                } else {
                    bucket_of(to_epoch_seconds(raw[row], unit), granularity)
                }
            })
            .collect();
        Some((buckets, cap))
    }

    /// What the closing part records in its footer — the column, granularity and the
    /// distinct partitions it holds; `None` when the part is not budgeted.
    fn footer_note(&self) -> Option<String> {
        self.col?;
        let r = self.rollover.as_ref()?;
        Some(crate::plan::rollover::partition_buckets_note(
            &r.column,
            r.granularity,
            self.buckets.len(),
        ))
    }

    /// The budget the current part has already spent.
    fn held(&self) -> &std::collections::HashSet<i64> {
        &self.buckets
    }

    /// Count `buckets` against the current part.
    fn spend(&mut self, buckets: &[i64]) {
        self.buckets.extend(buckets.iter().copied());
    }

    /// A new part starts with its whole budget.
    fn reset(&mut self) {
        self.buckets.clear();
    }
}

/// The export's declared quality rules and what has been measured against them.
///
/// Seven of `ExportSink`'s fields were this one concern, and nothing in the write path
/// reads them: the tracker needs the batch, the resolved dest schema, and the run's row
/// count, and it answers with issues. Keeping it whole means the sink's interface no
/// longer carries the accumulators, and the rules are testable without a writer.
#[derive(Default)]
pub(in crate::pipeline) struct QualityTracker {
    pub(in crate::pipeline) columns: Option<crate::config::QualityConfig>,
    pub(in crate::pipeline) null_counts: std::collections::HashMap<String, usize>,
    pub(in crate::pipeline) unique_sets:
        std::collections::HashMap<String, std::collections::HashSet<u64>>,
    /// Per-column count of non-NULL values seen by uniqueness tracking. NULLs are never
    /// duplicates (SQL UNIQUE semantics) and are skipped from hashing, so duplicates must
    /// be computed against this count, not the run's `total_rows`.
    pub(in crate::pipeline) unique_non_null_counts: std::collections::HashMap<String, usize>,
    /// Columns whose unique-entry tracking stopped because `unique_max_entries` was reached.
    pub(in crate::pipeline) unique_capped: std::collections::HashSet<String>,
    /// Column index caches, built once when the dest schema resolves.
    pub(in crate::pipeline) null_indices: Vec<(usize, String)>,
    pub(in crate::pipeline) unique_indices: Vec<(usize, String)>,
}

impl QualityTracker {
    pub(in crate::pipeline) fn new(columns: Option<crate::config::QualityConfig>) -> Self {
        Self {
            columns,
            ..Default::default()
        }
    }

    /// Bind the declared rules to the resolved dest schema, caching each rule's column
    /// index.
    ///
    /// Fails loud (#33, "never a silent no-op"): a rule naming a column the export does not
    /// produce would otherwise be dropped by the filters below and report `quality: pass`
    /// over a gate that never ran. Validated the moment the schema resolves, before any
    /// batch.
    pub(in crate::pipeline) fn resolve_columns(&mut self, dest_schema: &Schema) -> Result<()> {
        let Some(qc) = &self.columns else {
            return Ok(());
        };
        let available: Vec<String> = dest_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        crate::quality::validate_quality_columns(qc, &available)?;
        self.null_indices = dest_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| qc.null_ratio_max.contains_key(f.name().as_str()))
            .map(|(i, f)| (i, f.name().clone()))
            .collect();
        self.unique_indices = dest_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| qc.unique_columns.contains(f.name()))
            .map(|(i, f)| (i, f.name().clone()))
            .collect();
        Ok(())
    }

    /// Accumulate one batch against the declared rules.
    pub(in crate::pipeline) fn track(&mut self, batch: &RecordBatch) {
        if self.columns.is_none() {
            return;
        }
        for (i, name) in &self.null_indices {
            *self.null_counts.entry(name.clone()).or_default() += batch.column(*i).null_count();
        }
        if self.unique_indices.is_empty() {
            return;
        }
        let cap = self.columns.as_ref().and_then(|q| q.unique_max_entries);
        use std::io::Write as _;
        use xxhash_rust::xxh3::xxh3_64;
        let fmt_options = arrow::util::display::FormatOptions::default();
        let mut scratch = Vec::with_capacity(64);
        for (i, name) in &self.unique_indices {
            if self.unique_capped.contains(name) {
                continue;
            }
            let col = batch.column(*i);
            let non_null_count = self.unique_non_null_counts.entry(name.clone()).or_default();
            let set = self.unique_sets.entry(name.clone()).or_default();
            if let Ok(formatter) =
                arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &fmt_options)
            {
                for row in 0..col.len() {
                    // NULLs are never duplicates (SQL UNIQUE semantics): skip before the
                    // cap check so trailing NULLs can't trip the cap.
                    if col.is_null(row) {
                        continue;
                    }
                    if let Some(limit) = cap
                        && set.len() >= limit
                    {
                        self.unique_capped.insert(name.clone());
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

    /// The verdict, given the run's row count — the one number the rules need that the
    /// tracker does not own.
    pub(in crate::pipeline) fn issues(
        &self,
        total_rows: usize,
    ) -> Vec<crate::quality::QualityIssue> {
        let Some(qc) = &self.columns else {
            return Vec::new();
        };
        let mut issues = Vec::new();
        issues.extend(crate::quality::check_row_count(total_rows, qc));
        if total_rows == 0 {
            return issues;
        }
        for (col, max_ratio) in &qc.null_ratio_max {
            let nulls = self.null_counts.get(col).copied().unwrap_or(0);
            let ratio = nulls as f64 / total_rows as f64;
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
            if self.unique_capped.contains(col) {
                let cap = qc.unique_max_entries.unwrap_or(0);
                issues.push(crate::quality::QualityIssue {
                    severity: crate::quality::Severity::Warn,
                    message: format!(
                        "column '{}': uniqueness check capped at {} entries; result may be \
                         incomplete (set unique_max_entries higher to cover all rows)",
                        col, cap
                    ),
                });
            } else if let Some(set) = self.unique_sets.get(col) {
                let non_null = self.unique_non_null_counts.get(col).copied().unwrap_or(0);
                let dupes = non_null.saturating_sub(set.len());
                if dupes > 0 {
                    issues.push(crate::quality::QualityIssue {
                        severity: crate::quality::Severity::Fail,
                        message: format!(
                            "column '{}': {} duplicate values out of {} rows",
                            col, dupes, total_rows
                        ),
                    });
                }
            }
        }
        issues
    }
}

/// Per-batch progress feed for chunked exports: ticks the export's shared
/// progress bar with the running row count *during* a chunk's read, so a wide
/// first chunk (e.g. 250k rows / 90 MB) doesn't sit at "0 rows" for seconds and
/// read as idle. `streamed` is shared across the export's chunk workers (each
/// batch adds to it); `last_tick` throttles the bar/IPC refresh to ~8/s.
pub(in crate::pipeline) struct RowProgress {
    pub(in crate::pipeline) handle: crate::pipeline::progress::ChunkProgressHandle,
    pub(in crate::pipeline) streamed: Arc<std::sync::atomic::AtomicI64>,
    pub(in crate::pipeline) last_tick: std::time::Instant,
}

impl ExportSink {
    /// ADR-0029 (splitting ADR-0028's `drain_tail_into`) — the OBSERVATION half
    /// of this sink's tail: the dest schema it resolved and the per-column shape
    /// bytes it saw. Neither carries a coverage obligation, so drain them as
    /// soon as the read is done and BEFORE any fallible write — a run that then
    /// fails still records the fingerprint it OBSERVED instead of the stale
    /// open-time baseline. Applied by `finalize::finalize_export{,_records}`.
    pub(in crate::pipeline) fn drain_observations_into(
        &mut self,
        ledger: &mut crate::pipeline::commit::CommitLedger,
    ) {
        if let Some(schema) = self.dest_schema.as_deref() {
            ledger.note_schema(schema);
        }
        ledger.merge_shape(&std::mem::take(&mut self.column_max_bytes));
    }

    /// ADR-0029 — the INTEGRITY half: the Form-B checksums this sink
    /// accumulated (keyed to the cursor/key column when the key survived into
    /// the dest batch), contributed under `unit`, the commit unit whose parts
    /// they cover. Drain it only once that unit's parts are committed; the seam
    /// compares `unit` against the units `record_part` registered and suppresses
    /// Form B itself if the two sets disagree.
    pub(in crate::pipeline) fn drain_integrity_into(
        &mut self,
        unit: crate::pipeline::commit::UnitId,
        ledger: &mut crate::pipeline::commit::CommitLedger,
    ) {
        let key = self.checksum_key();
        ledger.contribute_checksums(unit, &std::mem::take(&mut self.column_checksums), key);
    }

    /// The column this sink's Form-B checksums are keyed to, or `None` when they are
    /// un-keyed.
    ///
    /// Keyed only when the key column survived into the DEST batch — `checksum_key_col` is
    /// its index there, so its absence means a full export with no cursor, or a
    /// stripped/synthetic one. Every runner needs this answer when it hands its checksums
    /// on, and each used to spell it out by reaching into both private fields; one name
    /// instead of six copies of the rule.
    pub(in crate::pipeline) fn checksum_key(&self) -> Option<String> {
        self.checksum_key_col.and(self.cursor_column.clone())
    }

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
            bytes_read: std::sync::Arc::clone(&plan.bytes_read),
            part_rows: 0,
            cursor_column: plan.strategy.cursor_extract_column().map(str::to_string),
            settle_columns: plan
                .strategy
                .incremental_plan()
                .map(|p| p.settle_columns())
                .unwrap_or_default(),
            last_cursor_value: None,
            first_cursor_value: None,
            source_cursor: None,
            schema: None,
            dest_schema: None,
            meta: plan.meta_columns.clone(),
            enriched_schema: None,
            exported_at_us,
            quality: QualityTracker::new(plan.quality.clone()),
            max_file_size: plan.max_file_size_bytes,
            completed_parts: Vec::new(),
            strip_internal_column,
            column_max_bytes: std::collections::HashMap::new(),
            max_batch_memory_bytes: plan.tuning.max_batch_memory_mb.map(|mb| mb * 1024 * 1024),
            // `0` (or None) disables the per-value guard; otherwise convert MB→bytes.
            max_value_bytes: plan.tuning.max_value_bytes(),
            batch_memory_policy: plan.tuning.on_batch_memory_exceeded,
            oversized_batch_count: 0,
            parquet_config: plan.parquet.clone(),
            parquet_row_group_rows: None,
            column_checksums: std::collections::BTreeMap::new(),
            checksum_key_col: None,
            row_progress: None,
            partition: PartBudget::new(plan.partition_rollover.clone()),
        })
    }

    /// Attach a per-batch row-progress feed (see [`RowProgress`]). Returns self
    /// so a chunk worker can `ExportSink::new(plan)?.with_row_progress(...)`.
    pub(in crate::pipeline) fn with_row_progress(
        mut self,
        handle: crate::pipeline::progress::ChunkProgressHandle,
        streamed: Arc<std::sync::atomic::AtomicI64>,
    ) -> Self {
        self.row_progress = Some(RowProgress {
            handle,
            streamed,
            last_tick: std::time::Instant::now(),
        });
        self
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
        let written = self.writer.as_ref().map(|w| w.bytes_written()).unwrap_or(0);
        if should_split(written, self.max_file_size, self.part_rows) {
            self.split_now()?;
        }
        Ok(())
    }

    /// Close the current part and open the next one, whatever asked for it — the byte
    /// cap or the partition budget.
    /// Close the current writer, recording in its footer what the part holds — the ONE
    /// close path. Every runner ends its last part here as well as every rotation: a
    /// bare `writer.take()` + `finish()` shipped the final part of every export (and
    /// every keyset page) without the note the loader bounds it by.
    pub(in crate::pipeline) fn finish_writer(&mut self) -> Result<()> {
        if let Some(mut w) = self.writer.take() {
            if let Some(note) = self.partition.footer_note() {
                w.note(crate::plan::rollover::PARTITION_BUCKETS_KEY, &note);
            }
            w.finish()?;
        }
        Ok(())
    }

    pub(in crate::pipeline) fn split_now(&mut self) -> Result<()> {
        self.finish_writer()?;

        let old_tmp = std::mem::replace(&mut self.tmp, tempfile::NamedTempFile::new()?);
        self.completed_parts.push(CompletedPart {
            tmp: old_tmp,
            rows: self.part_rows,
        });
        self.part_rows = 0;
        // The partition budget is per load job, hence per FILE: the new part starts with
        // its whole budget. Carrying the closed part's partitions over leaves the rows
        // that forced this rotation still not fitting, which rotates again on the same
        // rows and never terminates (caught as a stack overflow by
        // `a_batch_past_the_partition_budget_closes_the_part_mid_batch`).
        self.partition.reset();

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
        self.quality.track(batch);
    }

    /// Hard per-value guard (OPT-1): abort with `RIVET_VALUE_TOO_LARGE` when a
    /// single variable-length cell exceeds `max_value_bytes`. Only Utf8 /
    /// LargeUtf8 / Binary / LargeBinary values can be individually huge; fixed-
    /// width types (ints, floats, dates) cannot, so they need no check. Runs
    /// before the batch is split/encoded so the giant cell never reaches the
    /// row-group writer or the auto-shrink splitter (which can't divide one
    /// oversized value). `O(rows × var-length-cols)` length reads — no copies.
    ///
    /// KNOWN LIMITATION (security audit V22, CWE-770, accepted): this guard runs
    /// *post-materialization* — the cell has already been decoded by the driver
    /// and built into the Arrow array before its length is checked here, so a
    /// single adversarial cell of N bytes costs ~2N RAM (driver copy + Arrow
    /// copy) before the guard fires. The guard still prevents *further*
    /// amplification (encode / row-group / split) and aborts the run, and it is
    /// ON by default (`max_value_mb = Some(256)` in every tuning profile), so
    /// the realistic blast radius is one ≤256 MB-class over-allocation per
    /// oversized cell, not an unbounded OOM. A true pre-materialization cap
    /// would need a source-side length probe (`SUBSTRING(col,1,max+1)` — lossy,
    /// changes data) or a per-field driver limit (Postgres exposes none;
    /// MySQL's `max_allowed_packet` is connection- not field-scoped), so it is
    /// deliberately not attempted here.
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
    /// Form B: fold each column's xxh3 (keyed to the cursor column when present —
    /// `xxh3(key ‖ value)`) into the per-column accumulator, XOR-combined and
    /// keyed by column NAME. Over the dest batch (data columns), so enrichment /
    /// meta columns are naturally excluded; `validate` re-reads and recomputes
    /// this by name to catch an Arrow→Parquet encode / post-write fault.
    pub fn track_checksum(&mut self, batch: &RecordBatch) {
        use crate::source::value_checksum::{arrow_batch_checksums, arrow_batch_checksums_keyed};
        // Form B re-read verification only works for Parquet (a CSV→Arrow re-read
        // is not byte-faithful), so only Parquet exports record the checksum.
        if self.format_type != FormatType::Parquet {
            return;
        }
        let sums = match self.checksum_key_col {
            Some(k) => arrow_batch_checksums_keyed(batch, k),
            None => arrow_batch_checksums(batch),
        };
        // wrapping_add, matching `value_checksum::Fold::Sum` — the fold every
        // other site uses. This one was MISSED when the fold changed, and the
        // consequence was invisible below 500 rows: with a single batch per part
        // `0 ^ s == 0 + s`, so the write and read sides agreed exactly as long as
        // no part spanned more than one read batch (PROBE_BATCH_SIZE = 500). Past
        // that the recorded checksum diverged from the re-read and `validate
        // --depth full` reported post-write corruption on healthy data — measured
        // at the boundary: 500 rows exit 0, 501 rows exit 3.
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let e = self
                .column_checksums
                .entry(field.name().clone())
                .or_insert(0);
            *e = e.wrapping_add(sums[i]);
        }
    }

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
        self.quality.issues(self.total_rows)
    }

    /// Core batch processing: keep the part inside the load's partition budget, then
    /// track quality/shape, enrich and write. Called after the memory check.
    ///
    /// Nothing splits one Parquet file at load time, so a part written past the budget
    /// cannot be loaded at any granularity the operator wants — the writer is the only
    /// place that can prevent it. The part is closed BEFORE the rows that would overspend
    /// it, slicing the batch when the boundary falls inside. A bucket the part already
    /// holds costs nothing, so a wide batch over a narrow range never rotates.
    ///
    /// Iterative, not recursive: the number of rotations one batch needs is data-driven
    /// (its distinct partitions over the budget) and unbounded, so a frame per rotation
    /// overflows the stack on a batch that is merely wide — measured, as an abort rather
    /// than a test failure, which is a far worse way to learn it.
    fn on_batch_inner(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        let Some((buckets, cap)) = self.partition.buckets_for(dest_batch) else {
            return self.write_batch_part(dest_batch);
        };
        if buckets.is_empty() {
            return self.write_batch_part(dest_batch);
        }
        let mut offset = 0;
        while offset < buckets.len() {
            let fit = crate::plan::rollover::rows_that_fit(
                self.partition.held(),
                &buckets[offset..],
                cap,
            );
            if fit == 0 {
                // The part is full. Closing it frees the whole budget, and `cap` is never
                // zero here (`rows_that_fit` treats a zero budget as unbudgeted), so the
                // next pass takes at least one row — the loop cannot spin.
                self.split_now()?;
                continue;
            }
            self.partition.spend(&buckets[offset..offset + fit]);
            self.write_batch_part(&dest_batch.slice(offset, fit))?;
            offset += fit;
            // No rotation here: the `fit == 0` pass above closes a spent part, and the
            // byte cap may already have rotated inside `write_batch_part` — a second
            // close would ship an empty file and a 0-row manifest entry.
        }
        Ok(())
    }

    /// Write one slice that is known to fit the current part's partition budget.
    fn write_batch_part(&mut self, dest_batch: &RecordBatch) -> Result<()> {
        self.total_rows += dest_batch.num_rows();
        // Feed the running row count to the progress bar *during* the read, not
        // only when the chunk completes (throttled to ~8/s).
        if let Some(rp) = self.row_progress.as_mut() {
            let n = dest_batch.num_rows() as i64;
            let total = rp
                .streamed
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed)
                + n;
            if rp.last_tick.elapsed() >= std::time::Duration::from_millis(120) {
                rp.handle.set_rows(total);
                rp.last_tick = std::time::Instant::now();
            }
        }
        self.part_rows += dest_batch.num_rows();
        self.track_quality(dest_batch);
        self.track_shape(dest_batch);
        self.track_checksum(dest_batch);

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
        for name in &self.settle_columns {
            if let Ok(field) = schema.field_with_name(name)
                && !matches!(
                    field.data_type(),
                    arrow::datatypes::DataType::Date32
                        | arrow::datatypes::DataType::Date64
                        | arrow::datatypes::DataType::Timestamp(_, _)
                )
            {
                anyhow::bail!(
                    "settle column `{name}` is {}, not a date/timestamp — the settle window \
                     compares it against the source clock. Point `settle.column` at the row's \
                     insert/update time (the cursor is used when `settle.column` is omitted).",
                    field.data_type()
                );
            }
        }
        let enriched = enrich::enrich_schema(&dest_schema, &self.meta)?;
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
        // Warn loud (#6/#29, the process rules "never a silent no-op"): `max_file_size`
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
        self.quality.resolve_columns(&dest_schema)?;
        // `schema` keeps internal columns so cursor extraction (e.g. synthetic
        // `_rivet_coalesced_cursor`) can index by name. `dest_schema` is what
        // downstream consumers see — used for schema-change detection.
        // Form B: resolve the cursor/key column index in the dest batch (the pk
        // for the keyed checksum). `None` when there's no cursor, or it's a
        // stripped/synthetic column not present in the dest batch (→ un-keyed).
        self.checksum_key_col = self
            .cursor_column
            .as_ref()
            .and_then(|c| dest_schema.index_of(c).ok());
        // The partition column each part is budgeted against.
        self.partition.resolve(&dest_schema);
        // Coverage is visible, not silent: warn once per export about any column the
        // value checksum does NOT cover (UUID / List / Decimal256 / ns-timestamp),
        // so its 0 contribution can't read as "verified".
        let skips = crate::source::value_checksum::coverage_skips(&dest_schema);
        if !skips.is_empty() {
            log::warn!(
                "value checksum covers {}/{} columns; NOT checked: {}",
                dest_schema.fields().len() - skips.len(),
                dest_schema.fields().len(),
                skips
                    .iter()
                    .map(|(n, r)| format!("{n} ({r})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        self.schema = Some(schema);
        self.dest_schema = Some(dest_schema);
        self.enriched_schema = Some(enriched);
        Ok(())
    }

    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Bytes READ from the source: the in-memory Arrow size of the batch as
        // received, BEFORE any internal-column strip. Incremented on the RUN's
        // shared counter (plan.bytes_read), so every sink — per chunk, per
        // worker — feeds one total with no per-runner harvest to forget (#175).
        self.bytes_read.fetch_add(
            batch.get_array_memory_size() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
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
            // Only ADVANCE the mark, never erase it. A batch with no readable
            // cursor value (every row NULL) says nothing about progress; the
            // unconditional assignment let it overwrite the good mark
            // accumulated from every prior batch with None, after which nothing
            // was recorded and nothing committed.
            if self.first_cursor_value.is_none() {
                self.first_cursor_value = extract_first_cursor_value(batch, col, schema);
            }
            if let Some(v) = extract_last_cursor_value(batch, col, schema) {
                self.last_cursor_value = Some(v);
            }
        }
        Ok(())
    }

    fn set_source_cursor(&mut self, token: String) {
        self.source_cursor = Some(token);
    }
}

impl ExportSink {
    /// The keyset high-water mark to advance/checkpoint from: the source's own
    /// lossless token when it reported one (MongoDB BSON `_id`), else the string
    /// extracted from the output column (every SQL engine).
    pub(in crate::pipeline) fn effective_cursor(&self) -> Option<String> {
        self.source_cursor
            .clone()
            .or_else(|| self.last_cursor_value.clone())
    }
}

#[cfg(test)]
mod tests;
