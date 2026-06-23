//! CDC file sink — canonical change stream → typed Arrow `RecordBatch` → the
//! existing [`crate::format`] writer (Parquet/CSV), rolled over into files.
//!
//! Output shape (the downstream contract chosen in the architecture review):
//! `[__op, __pos]` + the source columns, **typed**, as the **after-image**
//! (upsert shape). A `DELETE` carries its key columns from the before-image.
//! Downstream MERGEs by PK + `__op` — the latest full image per key wins.
//!
//! This reuses the interchange layer (`create_format` / `FormatWriter`); wiring
//! to the full `ExportSink` (cloud destination + manifest + content-MD5) is the
//! next layer. Per-type fidelity is coarsened (see [`coarse_type`]) — full
//! temporal/decimal typing is the completion step.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde_json::Value as Json;

use crate::config::{CompressionType, FormatType};
use crate::error::Result;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream};

/// Coarsen a resolved Arrow type to one this sink builds from JSON values: ints →
/// `Int64`, floats → `Float64`, bool → `Boolean`, strings → `Utf8`, everything
/// else (temporal, decimal, binary, nested) → `Utf8` (stringified). Keeps the
/// schema and the built arrays consistent; full per-type fidelity is the
/// completion step.
fn coarse_type(dt: &DataType) -> DataType {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Boolean => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

/// Build one Arrow column from JSON cells (one per row; `None` ⇒ null). `dt` is
/// the already-coarsened type.
fn build_column(dt: &DataType, cells: &[Option<&Json>]) -> ArrayRef {
    match dt {
        DataType::Int64 => Arc::new(
            cells
                .iter()
                .map(|c| c.and_then(Json::as_i64))
                .collect::<Int64Array>(),
        ),
        DataType::Float64 => Arc::new(
            cells
                .iter()
                .map(|c| c.and_then(Json::as_f64))
                .collect::<Float64Array>(),
        ),
        DataType::Boolean => Arc::new(
            cells
                .iter()
                .map(|c| c.and_then(Json::as_bool))
                .collect::<BooleanArray>(),
        ),
        _ => Arc::new(
            cells
                .iter()
                .map(|c| c.map(json_to_string))
                .collect::<StringArray>(),
        ),
    }
}

fn json_to_string(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Stream canonical changes to typed Parquet/CSV files under `out_dir`, rolling a
/// new file every `rollover` rows. Persists the checkpoint after each file is
/// durably written. Stops at end of stream, `max_events`, or interruption.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_to_files(
    stream: &mut dyn ChangeStream,
    columns: &[(String, DataType)],
    out_dir: &Path,
    format: FormatType,
    tables: Vec<String>,
    checkpoint: Option<PathBuf>,
    max_events: Option<usize>,
    rollover: usize,
) -> Result<()> {
    std::fs::create_dir_all(out_dir)?;
    // __op, __pos, then the coarsened source columns (all nullable — a DELETE row
    // carries only its key).
    let mut fields = vec![
        Field::new("__op", DataType::Utf8, false),
        Field::new("__pos", DataType::Utf8, false),
    ];
    for (name, dt) in columns {
        fields.push(Field::new(name, coarse_type(dt), true));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let mut buf: Vec<ChangeEvent> = Vec::new();
    let mut seq = 0usize;
    let mut emitted = 0usize;

    while let Some(ev) = stream.next_change() {
        let ev = ev?;
        if !tables.is_empty() && !tables.iter().any(|t| t == &ev.table) {
            continue;
        }
        let committed = ev.committed;
        buf.push(ev);
        emitted += 1;
        // Roll a file only at a transaction boundary — never split a transaction
        // across files, and only checkpoint a commit position.
        if committed && buf.len() >= rollover {
            flush(&buf, &schema, columns, out_dir, format, seq)?;
            if let Some(p) = &checkpoint {
                buf.last().unwrap().position.save(p)?;
            }
            seq += 1;
            buf.clear();
        }
        if max_events.is_some_and(|m| emitted >= m) {
            break;
        }
    }
    if !buf.is_empty() {
        flush(&buf, &schema, columns, out_dir, format, seq)?;
        if let (Some(p), Some(last)) = (&checkpoint, buf.last())
            && last.committed
        {
            last.position.save(p)?;
        }
    }
    Ok(())
}

/// Build one `RecordBatch` from `events` and write it as a single file.
fn flush(
    events: &[ChangeEvent],
    schema: &SchemaRef,
    columns: &[(String, DataType)],
    out_dir: &Path,
    format: FormatType,
    seq: usize,
) -> Result<()> {
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
    for (i, (_, dt)) in columns.iter().enumerate() {
        let coarse = coarse_type(dt);
        let cells: Vec<Option<&Json>> = events
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
        arrays.push(build_column(&coarse, &cells));
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let path = out_dir.join(format!("cdc-{seq:06}.{}", format.label()));
    let file = File::create(&path)?;
    let compression = match format {
        FormatType::Csv => CompressionType::None,
        FormatType::Parquet => CompressionType::Zstd,
    };
    let fmt = crate::format::create_format(format, compression, None, None);
    let writer: Box<dyn std::io::Write + Send> = Box::new(file);
    let mut w = fmt.create_writer(schema, writer)?;
    w.write_batch(&batch)?;
    w.finish()?;
    Ok(())
}
