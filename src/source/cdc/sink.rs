//! CDC file sink — canonical change stream → typed Arrow `RecordBatch` → a temp
//! part → the existing **commit seam** (`write_part_file` → `Destination`), then a
//! `RunManifest` + `_SUCCESS` at clean end.
//!
//! Output shape (the downstream contract chosen in the architecture review):
//! `[__op, __pos]` + the source columns, **typed**, as the **after-image**
//! (upsert shape). A `DELETE` carries its key columns from the before-image.
//! Downstream MERGEs by PK + `__op` — the latest full image per key wins.
//!
//! Column typing flows through [`super::value`] (`RivetValue` → Arrow), so
//! temporals/decimals land as real `Timestamp`/`Date32`/`Decimal128` columns.
//! Each part is uploaded through [`crate::pipeline::commit::write_part_file`] —
//! the same destination + content-MD5 + transit-integrity path the batch export
//! uses (ADR-0004) — so a `--output gs://…` / `s3://…` works, with no-download
//! MD5 verification. The run-level manifest + `_SUCCESS` is a bounded-run concept:
//! it is written when the stream ends cleanly (e.g. `--max-events`); an unbounded
//! stream still uploads each part, it just has no terminal `_SUCCESS` until it
//! stops.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tempfile::NamedTempFile;

use crate::config::{CompressionType, FormatType};
use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{
    MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
    PartStatus, RunManifest,
};
use crate::pipeline::commit::{PartRecord, write_part_file};
use crate::pipeline::manifest_writer::write_manifest;
use crate::source::cdc::value::{self, RivetValue};
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream};
use crate::types::{TypeMapping, build_arrow_field};

/// Everything the sink needs that isn't the stream itself.
pub(crate) struct SinkConfig<'a> {
    /// Resolved source column type mappings — carry the Arrow type *and* its
    /// logical-type metadata (`json`/`uuid`/…), so the sink writes the same typed
    /// columns the batch export does (via [`build_arrow_field`]).
    pub columns: &'a [TypeMapping],
    pub dest: &'a dyn Destination,
    pub dest_uri: String,
    pub engine: &'a str,
    pub table: &'a str,
    pub format: FormatType,
    pub tables: Vec<String>,
    pub checkpoint: Option<PathBuf>,
    pub max_events: Option<usize>,
    pub rollover: usize,
    /// Roll a part once the buffered changes reach this many bytes (estimated),
    /// whichever comes first with `rollover`. `None` ⇒ row-count only.
    pub rollover_memory_bytes: Option<usize>,
    /// RFC3339 start time (passed in — `Utc::now()` is the caller's to stamp).
    pub started_at: String,
    /// RFC3339 stamp used as both `finished_at` and the run id seed.
    pub run_id: String,
}

/// Stream canonical changes to typed Parquet/CSV parts, uploading each through the
/// commit seam, then writing a manifest + `_SUCCESS` at clean end. Rolls a part at
/// the first transaction boundary past `rollover` rows; checkpoints a commit
/// position after each part is durably committed.
pub(crate) fn run_to_files(
    stream: &mut dyn ChangeStream,
    cfg: SinkConfig<'_>,
) -> Result<RunManifest> {
    // The schema is built lazily at the first flush so decimal column scales can
    // be refined from the data (SQL Server's metadata-only resolve gives a
    // placeholder scale of 0 — the same gap the batch path fills from rows).
    let mut columns = cfg.columns.to_vec();
    let mut schema: Option<SchemaRef> = None;

    let mut buf: Vec<ChangeEvent> = Vec::new();
    let mut buf_bytes = 0usize;
    let mut parts: Vec<PartRecord> = Vec::new();
    let mut seq = 0usize;
    let mut emitted = 0usize;

    while let Some(ev) = stream.next_change() {
        let ev = ev?;
        if !cfg.tables.is_empty() && !cfg.tables.iter().any(|t| t == &ev.table) {
            continue;
        }
        let committed = ev.committed;
        buf_bytes += ev.estimated_bytes();
        buf.push(ev);
        emitted += 1;
        // Roll a part only at a transaction boundary (never split a transaction),
        // once it hits the row-count OR the memory budget — whichever first.
        let hit_budget = cfg.rollover_memory_bytes.is_some_and(|b| buf_bytes >= b);
        if committed && (buf.len() >= cfg.rollover || hit_budget) {
            let sch = ensure_schema(&mut schema, &mut columns, &buf);
            parts.push(flush(&buf, &sch, &columns, cfg.format, seq, cfg.dest)?);
            let last_pos = &buf.last().unwrap().position;
            if let Some(p) = &cfg.checkpoint {
                last_pos.save(p)?;
            }
            // The part is durable + checkpointed — only now let a consume-on-read
            // source (PostgreSQL) advance past it. A crash before here re-reads it.
            stream.ack(last_pos)?;
            seq += 1;
            buf.clear();
            buf_bytes = 0;
        }
        if cfg.max_events.is_some_and(|m| emitted >= m) {
            break;
        }
    }
    if !buf.is_empty() {
        let sch = ensure_schema(&mut schema, &mut columns, &buf);
        parts.push(flush(&buf, &sch, &columns, cfg.format, seq, cfg.dest)?);
        // Only advance/checkpoint at a real commit boundary — a stream that ended
        // mid-transaction is re-read from the last committed position on resume.
        if let Some(last) = buf.last()
            && last.committed
        {
            if let Some(p) = &cfg.checkpoint {
                last.position.save(p)?;
            }
            stream.ack(&last.position)?;
        }
    }

    // Clean end → write the run manifest + _SUCCESS (bounded-run concept).
    let manifest = build_manifest(&cfg, &parts);
    write_manifest(cfg.dest, &manifest)?;
    Ok(manifest)
}

/// Build the sink schema once, on the first flush — refining decimal scales from
/// the first batch's values first (`__op`, `__pos`, then the typed columns).
fn ensure_schema(
    schema: &mut Option<SchemaRef>,
    columns: &mut [TypeMapping],
    events: &[ChangeEvent],
) -> SchemaRef {
    if schema.is_none() {
        refine_decimal_scales(columns, events);
        let mut fields = vec![
            Field::new("__op", DataType::Utf8, false),
            Field::new("__pos", DataType::Utf8, false),
        ];
        for m in columns.iter() {
            // Reuse the batch path's field builder so json/uuid/enum carry their
            // logical-type metadata + Parquet extension and ints keep their width.
            // For a type the sink can't build exactly (or `Unsupported`), fall back
            // to a plain `Utf8` field — matching the `Utf8` array `build_column`
            // will produce — so the schema and the data never disagree.
            let field = match &m.arrow_type {
                Some(dt) if value::is_buildable(dt) => build_arrow_field(m)
                    .unwrap_or_else(|| Field::new(&m.column_name, DataType::Utf8, m.nullable)),
                _ => Field::new(&m.column_name, DataType::Utf8, m.nullable),
            };
            fields.push(field);
        }
        *schema = Some(Arc::new(Schema::new(fields)));
    }
    schema.clone().unwrap()
}

/// Fill a `Decimal128` column's scale from the data when the resolved scale is the
/// `0` placeholder (SQL Server). A column with a real declared scale
/// (MySQL/PostgreSQL metadata) is left untouched — only the placeholder is
/// refined, from the max fractional-digit count seen in the batch.
fn refine_decimal_scales(columns: &mut [TypeMapping], events: &[ChangeEvent]) {
    for (i, m) in columns.iter_mut().enumerate() {
        let Some(DataType::Decimal128(p, 0)) = m.arrow_type else {
            continue;
        };
        let scale = events
            .iter()
            .filter_map(|e| {
                let img = match e.op {
                    ChangeOp::Delete => e.before.as_ref(),
                    _ => e.after.as_ref(),
                };
                img.and_then(|v| v.get(i))
            })
            .filter_map(|rv| match rv {
                RivetValue::Bytes(b) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.split_once('.').map(|(_, f)| f.len())),
                _ => None,
            })
            .max();
        if let Some(s) = scale.filter(|s| *s > 0) {
            m.arrow_type = Some(DataType::Decimal128(p, s as i8));
        }
    }
}

/// Build one `RecordBatch` from `events`, write it to a temp part, and upload it
/// through the commit seam (destination write + content-MD5 + transit check).
fn flush(
    events: &[ChangeEvent],
    schema: &SchemaRef,
    columns: &[TypeMapping],
    format: FormatType,
    seq: usize,
    dest: &dyn Destination,
) -> Result<PartRecord> {
    let ops: ArrayRef = Arc::new(
        events
            .iter()
            .map(|e| Some(e.op.as_str()))
            .collect::<StringArray>(),
    );
    let poss: ArrayRef = Arc::new(
        events
            .iter()
            .map(|e| Some(e.position.0.to_string()))
            .collect::<StringArray>(),
    );
    let mut arrays: Vec<ArrayRef> = vec![ops, poss];
    for (i, m) in columns.iter().enumerate() {
        let cells: Vec<Option<&RivetValue>> = events
            .iter()
            .map(|e| {
                // after-image for insert/update; before-image (the key) for delete
                let image = match e.op {
                    ChangeOp::Delete => e.before.as_ref(),
                    _ => e.after.as_ref(),
                };
                image.and_then(|vals| vals.get(i))
            })
            .collect();
        // The render type matches exactly the field `ensure_schema` declared.
        arrays.push(value::build_column(
            &value::render_type(m.arrow_type.as_ref()),
            &cells,
        )?);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let tmp = NamedTempFile::new()?;
    let compression = match format {
        FormatType::Csv => CompressionType::None,
        FormatType::Parquet => CompressionType::Zstd,
    };
    let fmt = crate::format::create_format(format, compression, None, None);
    let writer: Box<dyn std::io::Write + Send> = Box::new(tmp.reopen()?);
    let mut w = fmt.create_writer(schema, writer)?;
    w.write_batch(&batch)?;
    w.finish()?;

    let file_name = format!("cdc-{seq:06}.{}", format.label());
    write_part_file(dest, tmp.path(), events.len() as i64, file_name)
}

/// Assemble a `RunManifest` from the committed parts (hand-built — no plan
/// coupling; `record_part` is the plan-bound path the batch export uses).
fn build_manifest(cfg: &SinkConfig<'_>, parts: &[PartRecord]) -> RunManifest {
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        run_id: cfg.run_id.clone(),
        export_name: cfg.table.to_string(),
        started_at: cfg.started_at.clone(),
        finished_at: cfg.run_id.clone(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            engine: cfg.engine.to_string(),
            schema: None,
            table: Some(cfg.table.to_string()),
        },
        destination: ManifestDestination {
            kind: "cdc".to_string(),
            uri: cfg.dest_uri.clone(),
        },
        format: cfg.format.label().to_string(),
        compression: "zstd".to_string(),
        schema_fingerprint: String::new(),
        row_count: parts.iter().map(|p| p.rows).sum(),
        part_count: parts.len() as u32,
        parts: parts
            .iter()
            .enumerate()
            .map(|(i, p)| ManifestPart {
                part_id: (i + 1) as u32,
                path: p.file_name.clone(),
                rows: p.rows,
                size_bytes: p.bytes,
                content_fingerprint: p.fingerprint.clone(),
                content_md5: p.md5.clone(),
                status: PartStatus::Committed,
            })
            .collect(),
    }
}
