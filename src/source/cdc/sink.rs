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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
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
use crate::pipeline::manifest_writer::{write_manifest, write_run_unique_manifest_copy};
use crate::source::cdc::value::{self, RivetValue};
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position, TxnSeq};
use crate::types::{TypeMapping, build_arrow_field};

/// One table's wiring in a (possibly multi-table) CDC run: where its parts go
/// and its resolved column types. Events are **routed** by table name — an
/// event for a table with no output is skipped (the filter).
pub(crate) struct TableOutput<'a> {
    pub table: String,
    /// Resolved source column type mappings — carry the Arrow type *and* its
    /// logical-type metadata (`json`/`uuid`/…), so the sink writes the same typed
    /// columns the batch export does (via [`build_arrow_field`]).
    pub columns: Vec<TypeMapping>,
    pub dest: &'a dyn Destination,
    pub dest_uri: String,
}

/// Everything the sink needs that isn't the stream itself. `outputs` carries one
/// entry per captured table — several tables share ONE stream (one slot / one
/// binlog connection) and ONE checkpoint, because the resume position is a
/// property of the stream, not of a table.
pub(crate) struct SinkConfig<'a> {
    pub outputs: Vec<TableOutput<'a>>,
    pub engine: super::CdcEngine,
    pub format: FormatType,
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

/// When to roll a part: at a transaction boundary, once the buffer reaches the row
/// count OR the memory budget. Pure — unit-tested without a stream or destination.
struct RolloverPolicy {
    rollover_rows: usize,
    rollover_bytes: Option<usize>,
}

impl RolloverPolicy {
    /// Never split a transaction across parts, so a part can only roll on a
    /// committed event; then roll on whichever of count / byte-budget hits first.
    fn should_roll(&self, buf_rows: usize, buf_bytes: usize, committed: bool) -> bool {
        committed
            && (buf_rows >= self.rollover_rows
                || self.rollover_bytes.is_some_and(|b| buf_bytes >= b))
    }
}

/// Per-table sink state: the lazily-built schema, the buffered (not yet
/// flushed) changes, and the committed parts.
struct TableSink<'a> {
    out: TableOutput<'a>,
    schema: Option<SchemaRef>,
    buf: Vec<ChangeEvent>,
    parts: Vec<PartRecord>,
    seq: usize,
    /// Finding #38: per-column value checksums, XOR-accumulated across parts
    /// (the same combining rule `validate_recorded_checksums` applies on
    /// re-read) — recorded into the manifest so `rivet validate` Form B
    /// covers CDC prefixes instead of silently skipping the value leg.
    column_sums: std::collections::BTreeMap<String, u64>,
}

impl TableSink<'_> {
    /// Encode + upload this table's buffered changes as one part (no-op when
    /// the buffer is empty). Does NOT touch the checkpoint or the stream — the
    /// ack decision is global (see [`roll_all`]).
    fn flush_buffered(
        &mut self,
        engine: super::CdcEngine,
        format: FormatType,
        run_token: &str,
    ) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // The schema is built lazily at the first flush so decimal column
        // scales can be refined from the data (SQL Server's metadata-only
        // resolve gives a placeholder scale of 0 — the same gap the batch path
        // fills from rows).
        let sch = ensure_schema(&mut self.schema, &mut self.out.columns, &self.buf);
        let (part, sums) = flush(
            &self.buf,
            &sch,
            &self.out.columns,
            engine,
            format,
            run_token,
            self.seq,
            self.out.dest,
        )?;
        for (name, sum) in sums {
            *self.column_sums.entry(name).or_insert(0) ^= sum;
        }
        self.parts.push(part);
        self.seq += 1;
        self.buf.clear();
        Ok(())
    }
}

/// Does a config `table:` entry match an event's identity? Config may be bare
/// (`orders` — matches the bare table name in any schema) or schema-qualified
/// (`public.orders` — matches schema AND table). Adapters always emit schema
/// and table separately; comparing the config string verbatim against the
/// bare event table silently routed ZERO events for qualified configs.
pub(super) fn table_matches(cfg: &str, schema: &str, table: &str) -> bool {
    // Full-name match FIRST: a MongoDB collection name may contain dots
    // (`my.coll`) and has no schema qualifier, so splitting it into a bogus
    // `schema.table` dropped every event (bug-hunt: 0-row success forever). This
    // is safe for SQL — no real table is literally named `schema.table`.
    if cfg == table {
        return true;
    }
    // Otherwise a SQL `schema.table` qualifier.
    match cfg.split_once('.') {
        Some((cs, ct)) => cs == schema && ct == table,
        None => false,
    }
}

/// The **durable sequence** for one roll — the invariant that makes the run
/// at-least-once: encode + upload EVERY table's buffered part, THEN persist the
/// resume checkpoint, THEN ack the source, in that exact order. A crash between
/// any two steps re-reads on resume; reordering would risk dropping a change a
/// consume-on-read source (PostgreSQL) had already advanced past.
///
/// The flush-ALL-tables-first step is what makes the multi-table stream safe:
/// the position is a property of the stream, so acking after flushing only one
/// table would advance past another table's still-buffered changes. Checkpoint
/// and ack happen only at a real commit boundary (`last_commit`); a trailing
/// mid-transaction tail is flushed but never acked past — it is re-read (and
/// deduped downstream) rather than lost.
#[allow(clippy::too_many_arguments)]
fn roll_all(
    sinks: &mut [TableSink<'_>],
    stream: &mut dyn ChangeStream,
    engine: super::CdcEngine,
    format: FormatType,
    run_token: &str,
    checkpoint: Option<&Path>,
    last_commit: &Option<Position>,
    unacked_commit: &mut bool,
) -> Result<()> {
    for s in sinks.iter_mut() {
        s.flush_buffered(engine, format, run_token)?;
    }
    // Fault point: the parts are durable but the checkpoint/ack have NOT run. A
    // crash here must re-read on resume (at-least-once) — never lose the change.
    crate::test_hook::maybe_panic_at("cdc_after_flush_before_ack");
    if *unacked_commit && let Some(p) = last_commit {
        if let Some(ck) = checkpoint {
            p.save(ck)?;
        }
        // Fault point: checkpoint persisted, source NOT acked — a crash here
        // must re-read (PG would re-peek; the file checkpoint already moved,
        // so MySQL resumes from it — both are at-least-once).
        crate::test_hook::maybe_panic_at("cdc_after_checkpoint_before_ack");
        stream.ack(p)?;
        // Fault point: fully durable + acked — a crash here loses nothing and
        // must not duplicate on resume (the checkpoint already advanced).
        crate::test_hook::maybe_panic_at("cdc_after_ack");
        *unacked_commit = false;
    }
    Ok(())
}

/// Stream canonical changes to typed Parquet/CSV parts — routed to each table's
/// own output — uploading each part through the commit seam, then writing a
/// per-table manifest + `_SUCCESS` at clean end. The loop only pulls + routes +
/// asks the [`RolloverPolicy`] (on totals across tables); the durable
/// flush→checkpoint→ack sequence lives in [`roll_all`]. Returns one manifest
/// per output, in `outputs` order.
pub(crate) fn run_to_files(
    stream: &mut dyn ChangeStream,
    cfg: SinkConfig<'_>,
) -> Result<Vec<RunManifest>> {
    let run_token = run_token(&cfg.run_id);
    let mut sinks: Vec<TableSink<'_>> = cfg
        .outputs
        .into_iter()
        .map(|out| TableSink {
            out,
            schema: None,
            buf: Vec::new(),
            parts: Vec::new(),
            seq: 0,
            column_sums: std::collections::BTreeMap::new(),
        })
        .collect();

    let policy = RolloverPolicy {
        rollover_rows: cfg.rollover,
        rollover_bytes: cfg.rollover_memory_bytes,
    };
    let checkpoint = cfg.checkpoint.as_deref();
    let (mut total_rows, mut total_bytes, mut emitted) = (0usize, 0usize, 0usize);
    // The last commit-boundary position seen, and whether a commit has arrived
    // since the last ack — the only position it is ever valid to advance to.
    let mut last_commit: Option<Position> = None;
    let mut unacked_commit = false;
    // Stamp each change with its intra-transaction ordinal (`__seq`) over the
    // WHOLE stream, before routing — a `(position, seq)` total order the load
    // dedup can trust even when a PK is touched twice in one transaction.
    let mut txn_seq = TxnSeq::default();

    while let Some(ev) = stream.next_change() {
        let mut ev = ev?;
        txn_seq.stamp(&mut ev);
        // The commit boundary is a property of the STREAM, not of any routed
        // table — record it BEFORE the routing filter. MySQL marks only the
        // LAST event of a transaction committed; if that event lands on an
        // uncaptured table (audit-log-written-last is a common ORM shape),
        // filtering first would drop the boundary, stall the checkpoint
        // forever, and duplicate the captured rows on every scheduler cycle.
        let committed = ev.committed;
        if committed {
            last_commit = Some(ev.position.clone());
            unacked_commit = true;
        }
        let Some(sink) = sinks
            .iter_mut()
            .find(|s| table_matches(&s.out.table, &ev.schema, &ev.table))
        else {
            continue; // not a captured table
        };
        total_bytes += ev.estimated_bytes();
        sink.buf.push(ev);
        total_rows += 1;
        emitted += 1;
        if policy.should_roll(total_rows, total_bytes, committed) {
            roll_all(
                &mut sinks,
                stream,
                cfg.engine,
                cfg.format,
                &run_token,
                checkpoint,
                &last_commit,
                &mut unacked_commit,
            )?;
            total_rows = 0;
            total_bytes = 0;
        }
        if cfg.max_events.is_some_and(|m| emitted >= m) {
            break;
        }
    }
    // Final roll fires when a captured table has buffered rows OR when a commit
    // boundary is unacked. The latter is the fix for a stream whose ONLY traffic
    // was uncaptured tables: `last_commit`/`unacked_commit` advanced (before the
    // routing filter) but no captured buffer ever triggered a roll, so without
    // this the checkpoint never moved — every scheduler cycle re-read the whole
    // uncaptured backlog until the log rolled past it (bug-hunt K, cross-engine).
    // `roll_all` flushes nothing when the buffers are empty; it just persists the
    // checkpoint + acks.
    if unacked_commit || sinks.iter().any(|s| !s.buf.is_empty()) {
        roll_all(
            &mut sinks,
            stream,
            cfg.engine,
            cfg.format,
            &run_token,
            checkpoint,
            &last_commit,
            &mut unacked_commit,
        )?;
    }

    // Fault point: all parts durable + acked, no manifest yet — a crash here
    // leaves a resumable prefix (parts without _SUCCESS), and the retry must
    // complete it without re-capturing acked data.
    crate::test_hook::maybe_panic_at("cdc_before_manifest");

    // Clean end → write each table's run manifest + _SUCCESS (bounded-run concept).
    let mut manifests = Vec::with_capacity(sinks.len());
    for s in &sinks {
        let manifest = build_manifest(
            cfg.engine,
            &s.column_sums,
            &s.out,
            cfg.format,
            &cfg.run_id,
            &cfg.started_at,
            &s.parts,
        );
        write_manifest(s.out.dest, &manifest)?;
        // The canonical `manifest.json` above is last-writer-wins; leave an
        // immutable run-unique copy so a prefix accumulating several `until_current`
        // cycles keeps EACH run's manifest for the Pro loader's cross-run reconcile.
        write_run_unique_manifest_copy(s.out.dest, &manifest)?;
        manifests.push(manifest);
    }
    Ok(manifests)
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
            // Intra-transaction ordinal — `(__pos, __seq)` is the total change
            // order the load dedup sorts by (see `TxnSeq`).
            Field::new("__seq", DataType::Int64, false),
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
/// Filename-safe token from the run id. Part names must be unique per run — a
/// later run into the same prefix has to append alongside prior parts, never
/// overwrite them (mirrors the batch path's timestamp-named parts). The CLI path
/// passes an RFC3339 run id (`:`/`+` — `:` is not even legal on Windows), so map
/// anything outside `[A-Za-z0-9._-]` to `-`.
fn run_token(run_id: &str) -> String {
    run_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn flush(
    events: &[ChangeEvent],
    schema: &SchemaRef,
    columns: &[TypeMapping],
    engine: super::CdcEngine,
    format: FormatType,
    run_token: &str,
    seq: usize,
    dest: &dyn Destination,
) -> Result<(PartRecord, Vec<(String, u64)>)> {
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
    let seqs: ArrayRef = Arc::new(events.iter().map(|e| e.seq as i64).collect::<Int64Array>());
    // Finding #37: a mid-window DDL desynchronizes the event images from the
    // resolved schema — positional mapping then puts a dropped column's value
    // into its NEIGHBOR (observed live: after DROP COLUMN a, row1's 'AAA'
    // landed in column b, silently, status success). Binlog row events carry
    // no column names, so v1 is the honest loud check: any image whose arity
    // differs from the resolved schema aborts the flush with the recovery
    // path spelled out. (Same-arity DDL — rename — is positionally safe;
    // type changes are a schema-history feature, see the docs limitation.)
    for ev in events {
        // A DELETE's before-image may legitimately carry only the key
        // columns (PostgreSQL test_decoding emits just the key; MySQL FULL
        // row-image carries everything) — a SHORTER delete image maps by
        // prefix; an image WIDER than the schema, or a non-delete image of
        // ANY other arity, proves a stale pre-DDL layout.
        let is_delete = ev.op == ChangeOp::Delete;
        if ev.image_names.is_some() {
            continue; // named image — mapped by name, arity-proof for any op
        }
        let img = if is_delete {
            ev.before.as_ref()
        } else {
            ev.after.as_ref()
        };
        let bad = |n: usize| {
            if is_delete {
                n > columns.len()
            } else {
                n != columns.len()
            }
        };
        if let Some(vals) = img
            && bad(vals.len())
        {
            anyhow::bail!(
                "cdc: an event for table '{}' carries {} column(s) but the resolved \
                 schema has {} — a DDL landed inside this capture window, and mapping \
                 by position would put values into the WRONG columns. Recover by \
                 re-snapshotting the table (delete its snapshot/_SUCCESS marker under \
                 `initial: snapshot`) or by resetting the checkpoint past the DDL. \
                 To make mid-stream DDL safe going forward, set \
                 binlog_row_metadata=FULL on the MySQL server (8.0.1+) — rivet then \
                 maps binlog images by column NAME and this error class disappears.",
                ev.table,
                vals.len(),
                columns.len()
            );
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![ops, poss, seqs];
    let mut col_sums: Vec<(String, u64)> = Vec::with_capacity(columns.len());
    for (i, m) in columns.iter().enumerate() {
        // Engine/native-type cell normalisation (e.g. MySQL binlog quirks: BIT
        // bytes, ENUM indexes, epoch-text TIMESTAMPs, NUL-trimmed BINARY) —
        // computed once per column, applied per cell.
        let fix = value::mysql_cell_fix(engine, &m.source_native_type);
        // after-image for insert/update; before-image (the key) for delete.
        // Finding #41: a NAMED key-only image (PG DELETE) maps by COLUMN NAME
        // into the resolved schema — positional mapping put a non-first PK's
        // value into column 0 and NULLed the PK, silently losing the delete
        // downstream. Unnamed images stay positional (full rows).
        fn image_cell<'e>(
            e: &'e ChangeEvent,
            i: usize,
            col: &str,
            ncols: usize,
            memo: Option<(&std::sync::Arc<[String]>, Option<usize>)>,
        ) -> Option<&'e RivetValue> {
            let vals = match e.op {
                ChangeOp::Delete => e.before.as_ref()?,
                _ => e.after.as_ref()?,
            };
            match &e.image_names {
                Some(names) => match memo
                    .filter(|(m, _)| std::sync::Arc::ptr_eq(m, names))
                    .map(|(_, j)| j)
                    .unwrap_or_else(|| names.iter().position(|n| n == col))
                {
                    Some(j) => vals.get(j),
                    // Name absent: a mid-window RENAME leaves the value under
                    // its OLD name — when the arity still matches, position is
                    // trustworthy and the value must not silently degrade to
                    // NULL. Arity mismatch (mid-window ADD/DROP) ⇒ the column
                    // genuinely has no value in this image ⇒ NULL.
                    None if vals.len() == ncols => vals.get(i),
                    None => None,
                },
                None => vals.get(i),
            }
        }
        // O(1) name lookup for the common case: all events in a flush share
        // one names-Arc (same TABLE_MAP / same wire session), so resolve this
        // column's image index once and reuse it by pointer identity.
        let memo_arc = events.iter().find_map(|e| e.image_names.as_ref());
        let memo = memo_arc.map(|names| (names, names.iter().position(|n| n == &m.column_name)));
        let render = value::render_type(m.arrow_type.as_ref());
        let owned: Option<Vec<Option<RivetValue>>> = fix.as_ref().map(|fix| {
            events
                .iter()
                .map(|e| {
                    image_cell(e, i, &m.column_name, columns.len(), memo).map(|v| fix.apply(v))
                })
                .collect()
        });
        let cells: Vec<Option<&RivetValue>> = match &owned {
            Some(o) => o.iter().map(|c| c.as_ref()).collect(),
            None => events
                .iter()
                .map(|e| image_cell(e, i, &m.column_name, columns.len(), memo))
                .collect(),
        };
        let arr = value::build_column(&render, &cells)?;
        // Two-ended value check, same contract as the batch export's Form A:
        // an independent fold of the typed cells vs a fold of the BUILT array.
        // A mismatch means the builder changed a value between decode and
        // Arrow — fail loud BEFORE the part is written, naming the column.
        let source_sum = value::cells_checksum(&render, &cells);
        let arrow_sum = crate::source::value_checksum::array_checksum(arr.as_ref());
        col_sums.push((m.column_name.clone(), arrow_sum));
        if source_sum != arrow_sum {
            anyhow::bail!(
                "cdc value checksum mismatch in column '{}': source={source_sum} \
                 arrow={arrow_sum} — the value converter changed a value between \
                 decode and Arrow build",
                m.column_name
            );
        }
        arrays.push(arr);
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

    let file_name = format!("cdc-{run_token}-{seq:06}.{}", format.label());
    let part = write_part_file(dest, tmp.path(), events.len() as i64, file_name)?;
    Ok((part, col_sums))
}

/// Assemble one table's `RunManifest` from its committed parts (hand-built — no
/// plan coupling; `record_part` is the plan-bound path the batch export uses).
fn build_manifest(
    engine: super::CdcEngine,
    column_sums: &std::collections::BTreeMap<String, u64>,
    out: &TableOutput<'_>,
    format: FormatType,
    run_id: &str,
    started_at: &str,
    parts: &[PartRecord],
) -> RunManifest {
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        mode: "cdc".to_string(),
        run_id: run_id.to_string(),
        export_name: out.table.clone(),
        started_at: started_at.to_string(),
        finished_at: run_id.to_string(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            extraction: Some(crate::manifest::ExtractionMetadata {
                strategy: "cdc".to_string(),
                cursor_column: None,
                cursor_type: None,
                cursor_low: None,
                cursor_high: None,
                source_row_count: None,
            }),
            engine: engine.label().to_string(),
            schema: None,
            table: Some(out.table.clone()),
        },
        destination: ManifestDestination {
            kind: "cdc".to_string(),
            uri: out.dest_uri.clone(),
        },
        format: format.label().to_string(),
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
        column_checksums: Some(
            column_sums
                .iter()
                .map(|(name, sum)| crate::manifest::ColumnChecksum {
                    name: name.clone(),
                    checksum: sum.to_string(),
                })
                .collect(),
        ),
        checksum_key_column: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::source::cdc::value::RivetValue;
    use crate::source::cdc::{ChangeEvent, ChangeOp, Position};

    #[test]
    fn rollover_policy_count_budget_and_commit_gate() {
        let by_count = RolloverPolicy {
            rollover_rows: 3,
            rollover_bytes: None,
        };
        assert!(!by_count.should_roll(2, 0, true), "under the row count");
        assert!(by_count.should_roll(3, 0, true), "hits the row count");
        assert!(
            !by_count.should_roll(9, 0, false),
            "never roll mid-transaction (uncommitted)"
        );

        let by_budget = RolloverPolicy {
            rollover_rows: 1_000_000,
            rollover_bytes: Some(100),
        };
        assert!(!by_budget.should_roll(2, 50, true), "under the byte budget");
        assert!(
            by_budget.should_roll(2, 100, true),
            "byte budget rolls before the (huge) row count"
        );
    }

    /// A fake stream that yields a fixed list of changes and records every `ack`,
    /// so the test can assert the durable sequence ran once per committed part.
    struct FakeStream {
        events: VecDeque<ChangeEvent>,
        acked: Vec<Position>,
    }

    impl ChangeStream for FakeStream {
        fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
            self.events.pop_front().map(Ok)
        }
        fn ack(&mut self, position: &Position) -> Result<()> {
            self.acked.push(position.clone());
            Ok(())
        }
    }

    // Ultrareview bug_002: MySQL marks only the LAST event of a transaction
    // committed. If that event lands on an UNCAPTURED table, filtering before
    // the commit bookkeeping dropped the boundary — checkpoint never advanced,
    // the captured rows re-read (and re-written) on every scheduler cycle.
    // The boundary is a STREAM property: it must be recorded before routing.
    #[test]
    fn commit_boundary_on_an_uncaptured_table_still_advances_the_checkpoint() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let ckpt = d.path().join("ckpt");
        let mut captured = insert(1);
        captured.committed = false; // mid-transaction
        let mut foreign = insert(2);
        foreign.table = "audit_log".into(); // NOT captured
        foreign.committed = true; // the transaction's commit boundary
        let mut stream = FakeStream {
            events: vec![captured, foreign].into(),
            acked: Vec::new(),
        };
        let cfg = SinkConfig {
            checkpoint: Some(ckpt.clone()),
            ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 10)
        };
        run_to_files(&mut stream, cfg).unwrap();
        assert!(
            Position::load(&ckpt).unwrap().is_some(),
            "the stream's commit boundary must advance the checkpoint even when \
             its event routes to an uncaptured table"
        );
        assert_eq!(stream.acked.len(), 1, "and the source must be acked");
    }

    // Ultrareview bug_004: a schema-qualified config (`table: public.orders`)
    // compared verbatim against the adapter's BARE event table matched zero
    // events — the whole stream silently dropped into a 0-row success.
    #[test]
    fn table_matches_handles_bare_and_qualified_configs() {
        assert!(
            table_matches("orders", "public", "orders"),
            "bare matches any schema"
        );
        assert!(
            table_matches("public.orders", "public", "orders"),
            "qualified matches"
        );
        assert!(
            !table_matches("audit.orders", "public", "orders"),
            "wrong schema differs"
        );
        assert!(
            !table_matches("orders", "public", "users"),
            "different table differs"
        );
    }

    #[test]
    fn roast_dotted_collection_name_routes_by_full_name() {
        // A MongoDB collection literally named `my.data` (dots are legal, no
        // schema concept) must route by its FULL name — before this it was
        // mis-split into schema=`my`, table=`data` and routed ZERO events forever.
        assert!(table_matches("my.data", "shopdb", "my.data"));
        // Still distinguishes a genuinely different collection.
        assert!(!table_matches("my.data", "shopdb", "my.other"));
    }

    // The nameless (binlog_row_metadata=MINIMAL) guard path: an image whose
    // arity differs from the resolved schema must abort the flush loudly —
    // name-mapped engines skip this, MySQL-without-FULL depends on it.
    #[test]
    fn nameless_arity_drift_fails_the_flush_loudly() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut ev = insert(1);
        ev.after = Some(vec![RivetValue::Int(1), RivetValue::Int(2)]); // 2 vs 1 col
        ev.image_names = None;
        let mut stream = FakeStream {
            events: vec![ev].into(),
            acked: Vec::new(),
        };
        let cfg = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        let err = run_to_files(&mut stream, cfg).expect_err("arity drift must fail");
        assert!(
            err.to_string().contains("WRONG columns"),
            "must explain the misalignment: {err}"
        );
    }

    fn insert(id: i64) -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(id)]),
            position: Position(serde_json::json!({ "lsn": format!("{id:08X}") })),
            committed: true,
            image_names: None,
            seq: 0,
        }
    }

    fn int_col() -> Vec<TypeMapping> {
        vec![TypeMapping {
            column_name: "v".into(),
            source_native_type: "bigint".into(),
            rivet_type: crate::types::RivetType::Int64,
            arrow_type: Some(DataType::Int64),
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
        }]
    }

    // Exercises the whole sink — encode + commit-seam upload + manifest + the
    // flush→checkpoint→ack sequence — against a real LocalDestination (temp dir)
    // and a fake stream, with no live database.
    #[test]
    fn run_to_files_rolls_parts_and_acks_each_committed_part() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            acked: Vec::new(),
        };
        // 3 events, roll at 2 ⇒ part0=[1,2], part1=[3]
        let manifests = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
        )
        .unwrap();
        let manifest = &manifests[0];

        assert_eq!(manifest.part_count, 2, "rollover=2 over 3 events ⇒ 2 parts");
        assert_eq!(manifest.row_count, 3);
        assert_eq!(
            stream.acked.len(),
            2,
            "the durable sequence acked once per committed part"
        );
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "_SUCCESS marks the clean end"
        );
    }

    // RED test for the finding: the documented continuous model (a scheduler re-running
    // `rivet run` with `until_current: true`) points every cycle at the SAME
    // destination prefix. Each cycle's parts must survive the next cycle — the
    // batch path guarantees this with run-stamped part names. A fixed per-run
    // name (`cdc-000000`) silently overwrites the prior run's part AFTER the
    // source has already been acked past those changes: unrecoverable loss
    // (not in the slot, not in the destination).
    #[test]
    fn roast_second_run_into_same_prefix_must_not_clobber_prior_parts() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();

        // Cycle 1 captures changes 1,2 — one part.
        let mut run1 = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2)]),
            acked: Vec::new(),
        };
        run_to_files(
            &mut run1,
            SinkConfig {
                run_id: "t_cdc_20260702T100000000".into(),
                ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
            },
        )
        .unwrap();

        // Cycle 2 (a later scheduler tick, distinct run id) captures change 3.
        let mut run2 = FakeStream {
            events: VecDeque::from(vec![insert(3)]),
            acked: Vec::new(),
        };
        run_to_files(
            &mut run2,
            SinkConfig {
                run_id: "t_cdc_20260702T100500000".into(),
                ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
            },
        )
        .unwrap();

        // The union of both cycles must be readable from the prefix: 3 data rows.
        let mut data_rows = 0usize;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "csv") {
                data_rows += std::fs::read_to_string(&path)
                    .unwrap()
                    .lines()
                    .count()
                    .saturating_sub(1); // header
            }
        }
        assert_eq!(
            data_rows, 3,
            "run 2 must append its parts alongside run 1's in the same prefix — \
             a fixed part name silently overwrites already-acked changes"
        );
    }

    // Sibling to the parts test above: the PARTS are run-token-named (they
    // survive), but the manifest SIDECAR was fixed-name (`manifest.json`), so
    // the second cycle clobbered the first's manifest. A consumer summing row
    // counts ACROSS runs (the Pro loader's reconcile) then saw only the LAST
    // run's `row_count` — a live 45-min soak loaded 30 parts (1650 rows) but the
    // surviving manifest declared 55, and the count gate refused the load.
    // Each run must leave its own immutable run-unique manifest copy.
    #[test]
    fn roast_second_run_into_same_prefix_must_not_clobber_prior_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();

        let mut run1 = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2)]),
            acked: Vec::new(),
        };
        run_to_files(
            &mut run1,
            SinkConfig {
                run_id: "t_cdc_20260702T100000000".into(),
                ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
            },
        )
        .unwrap();

        let mut run2 = FakeStream {
            events: VecDeque::from(vec![insert(3)]),
            acked: Vec::new(),
        };
        run_to_files(
            &mut run2,
            SinkConfig {
                run_id: "t_cdc_20260702T100500000".into(),
                ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
            },
        )
        .unwrap();

        let run_unique: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("manifest-") && n.ends_with(".json"))
            .collect();
        assert_eq!(
            run_unique.len(),
            2,
            "each cycle must leave its own run-unique manifest copy so a \
             cross-run consumer sums both — got {run_unique:?}"
        );
    }

    fn decimal_col(name: &str, precision: u8, scale: i8) -> TypeMapping {
        TypeMapping {
            column_name: name.into(),
            source_native_type: "decimal".into(),
            // refine_decimal_scales only reads arrow_type — the logical type is irrelevant here.
            rivet_type: crate::types::RivetType::Int64,
            arrow_type: Some(DataType::Decimal128(precision, scale)),
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn refine_decimal_scales_fills_placeholder_keeps_real_scale() {
        let event = ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![
                RivetValue::Bytes(b"150.05".to_vec()),
                RivetValue::Bytes(b"7.5".to_vec()),
            ]),
            position: Position(serde_json::json!({})),
            committed: true,
            image_names: None,
            seq: 0,
        };
        let mut cols = vec![
            decimal_col("placeholder", 38, 0), // SQL Server: scale unknown at resolve
            decimal_col("real", 10, 2),        // MySQL/PG: scale already declared
        ];
        refine_decimal_scales(&mut cols, std::slice::from_ref(&event));
        assert_eq!(cols[0].arrow_type, Some(DataType::Decimal128(38, 2))); // filled from "150.05"
        assert_eq!(cols[1].arrow_type, Some(DataType::Decimal128(10, 2))); // left untouched
    }

    fn local_dest(dir: &tempfile::TempDir) -> Box<dyn crate::destination::Destination> {
        crate::destination::create_destination(&crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap()
    }

    fn cfg<'a>(
        dest: &'a dyn crate::destination::Destination,
        cols: &'a [TypeMapping],
        format: FormatType,
        rollover: usize,
    ) -> SinkConfig<'a> {
        SinkConfig {
            outputs: vec![TableOutput {
                table: "t".into(),
                columns: cols.to_vec(),
                dest,
                dest_uri: String::new(),
            }],
            engine: crate::source::cdc::CdcEngine::Mysql,
            format,
            checkpoint: None,
            max_events: None,
            rollover,
            rollover_memory_bytes: None,
            started_at: "2026-06-23T00:00:00Z".into(),
            run_id: "r".into(),
        }
    }

    #[test]
    fn a_transaction_is_never_split_across_parts() {
        // Four uncommitted changes + one committed = a 5-row transaction. rollover=2
        // would split it, but the commit-boundary gate holds the part open until the
        // commit — so the whole transaction lands in ONE part, acked once.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut events: VecDeque<ChangeEvent> = (1..=4)
            .map(|id| ChangeEvent {
                committed: false,
                ..insert(id)
            })
            .collect();
        events.push_back(insert(5)); // the COMMIT
        let mut stream = FakeStream {
            events,
            acked: Vec::new(),
        };
        let manifest = &run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
        )
        .unwrap()[0];
        assert_eq!(
            manifest.part_count, 1,
            "the 5-row transaction must not split at rollover=2"
        );
        assert_eq!(manifest.row_count, 5);
        assert_eq!(
            stream.acked.len(),
            1,
            "one ack, at the single commit boundary"
        );
    }

    #[test]
    fn delete_carries_the_before_image_not_an_empty_after() {
        // A DELETE has no after-image; the sink must write the BEFORE-image (the key
        // being removed), marked __op=delete — never an all-null row.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Int(7)]),
            after: None,
            ..insert(0)
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![del]),
            acked: Vec::new(),
        };
        run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10)).unwrap();
        let csv = std::fs::read_to_string(dir.path().join("cdc-r-000000.csv")).unwrap();
        assert!(csv.contains("delete"), "row marked __op=delete:\n{csv}");
        assert!(
            csv.lines().any(|l| l.contains("delete") && l.contains('7')),
            "the delete row carries the before-image key 7, not an empty after:\n{csv}"
        );
    }

    #[test]
    fn csv_output_has_a_header_and_one_row_per_change() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(10), insert(20)]),
            acked: Vec::new(),
        };
        let manifest =
            &run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10)).unwrap()[0];
        assert_eq!(manifest.row_count, 2);
        let csv = std::fs::read_to_string(dir.path().join("cdc-r-000000.csv")).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 3, "header + 2 data rows:\n{csv}");
        assert!(
            lines[0].contains("__op") && lines[0].contains('v'),
            "header carries the meta + source columns: {}",
            lines[0]
        );
    }

    #[test]
    fn empty_stream_writes_a_zero_row_manifest_and_success() {
        // A bounded run that drains no changes (`--until-current` with nothing new)
        // must still close cleanly: a 0-part manifest + `_SUCCESS`, not an error or a
        // missing marker that would look like a crash to the next run.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::new(),
            acked: Vec::new(),
        };
        let manifest = &run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 10),
        )
        .unwrap()[0];
        assert_eq!(manifest.row_count, 0);
        assert_eq!(manifest.part_count, 0);
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "a clean no-change run still marks _SUCCESS"
        );
    }

    #[test]
    fn table_filter_drops_changes_for_other_tables() {
        // The capture filters by table name client-side (one binlog/slot carries every
        // table). A change for an unrequested table must never land in the output —
        // a leak would mix unrelated tables into one export.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let ev = |id: i64, table: &str| ChangeEvent {
            table: table.into(),
            ..insert(id)
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![ev(1, "t"), ev(2, "other"), ev(3, "t")]),
            acked: Vec::new(),
        };
        // cfg() wires an output for table "t" only — routing IS the filter.
        let c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        let manifest = &run_to_files(&mut stream, c).unwrap()[0];
        assert_eq!(
            manifest.row_count, 2,
            "only the two 't' changes are kept; 'other' is filtered out"
        );
    }

    // ── Multi-table stream (slot multiplexing) invariants ─────────────────────

    fn ev_for(id: i64, table: &str, committed: bool) -> ChangeEvent {
        ChangeEvent {
            table: table.into(),
            committed,
            ..insert(id)
        }
    }

    fn two_outputs<'a>(
        dest_a: &'a dyn crate::destination::Destination,
        dest_b: &'a dyn crate::destination::Destination,
        cols: &[TypeMapping],
        format: FormatType,
        rollover: usize,
    ) -> SinkConfig<'a> {
        SinkConfig {
            outputs: vec![
                TableOutput {
                    table: "a".into(),
                    columns: cols.to_vec(),
                    dest: dest_a,
                    dest_uri: "a".into(),
                },
                TableOutput {
                    table: "b".into(),
                    columns: cols.to_vec(),
                    dest: dest_b,
                    dest_uri: "b".into(),
                },
            ],
            engine: crate::source::cdc::CdcEngine::Mysql,
            format,
            checkpoint: None,
            max_events: None,
            rollover,
            rollover_memory_bytes: None,
            started_at: "2026-06-23T00:00:00Z".into(),
            run_id: "r".into(),
        }
    }

    #[test]
    fn multi_table_stream_routes_to_per_table_outputs_with_own_manifests() {
        // Two tables through ONE stream: each table's changes land in its own
        // destination with its own manifest + _SUCCESS — the point of slot
        // multiplexing (N tables ≠ N slots).
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest_a, dest_b) = (local_dest(&da), local_dest(&db));
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![
                ev_for(1, "a", true),
                ev_for(2, "b", true),
                ev_for(3, "a", true),
            ]),
            acked: Vec::new(),
        };
        let manifests = run_to_files(
            &mut stream,
            two_outputs(
                dest_a.as_ref(),
                dest_b.as_ref(),
                &cols,
                FormatType::Csv,
                100,
            ),
        )
        .unwrap();

        assert_eq!(manifests.len(), 2, "one manifest per table");
        assert_eq!(manifests[0].export_name, "a");
        assert_eq!(manifests[0].row_count, 2);
        assert_eq!(manifests[1].export_name, "b");
        assert_eq!(manifests[1].row_count, 1);
        assert!(da.path().join("_SUCCESS").exists());
        assert!(db.path().join("_SUCCESS").exists());
        assert_eq!(
            stream.acked.len(),
            1,
            "one final roll ⇒ one ack for the whole stream"
        );
    }

    #[test]
    fn multi_table_roll_flushes_every_table_before_the_single_ack() {
        // The multiplexing safety invariant: the stream position is global, so a
        // roll must flush BOTH tables' buffers before the one ack — acking after
        // flushing only one table would advance past the other's buffered rows.
        // rollover=3 ⇒ the roll fires at event 3 (committed) while table 'b'
        // still has its row in the buffer; that row must be durable at ack time.
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest_a, dest_b) = (local_dest(&da), local_dest(&db));
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![
                ev_for(1, "a", false),
                ev_for(2, "b", false),
                ev_for(3, "a", true), // the commit that triggers the roll
            ]),
            acked: Vec::new(),
        };
        let manifests = run_to_files(
            &mut stream,
            two_outputs(dest_a.as_ref(), dest_b.as_ref(), &cols, FormatType::Csv, 3),
        )
        .unwrap();

        assert_eq!(manifests[0].row_count, 2, "table a: both rows in its part");
        assert_eq!(
            manifests[1].row_count, 1,
            "table b: flushed at the same roll"
        );
        assert_eq!(stream.acked.len(), 1, "exactly one ack for the roll");
        // The ack is at the COMMIT event's position (event 3).
        assert_eq!(
            stream.acked[0].0.get("lsn").and_then(|v| v.as_str()),
            Some(format!("{:08X}", 3).as_str()),
            "acked at the commit boundary"
        );
    }

    #[test]
    fn multi_table_trailing_uncommitted_tail_is_flushed_but_not_acked_past() {
        // A committed tx, then a trailing HALF-transaction when the bounded drain
        // ends: the tail is flushed (durable, deduped downstream on re-read) but
        // the ack stays at the last commit boundary — never past it.
        let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (dest_a, dest_b) = (local_dest(&da), local_dest(&db));
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![
                ev_for(1, "a", true),  // committed
                ev_for(2, "b", false), // trailing, tx never commits before EOF
            ]),
            acked: Vec::new(),
        };
        let manifests = run_to_files(
            &mut stream,
            two_outputs(
                dest_a.as_ref(),
                dest_b.as_ref(),
                &cols,
                FormatType::Csv,
                100,
            ),
        )
        .unwrap();

        assert_eq!(manifests[0].row_count, 1);
        assert_eq!(manifests[1].row_count, 1, "the tail is still made durable");
        assert_eq!(stream.acked.len(), 1);
        assert_eq!(
            stream.acked[0].0.get("lsn").and_then(|v| v.as_str()),
            Some(format!("{:08X}", 1).as_str()),
            "ack stays at the last commit boundary, not the uncommitted tail"
        );
    }

    /// A destination whose every write fails — to drive the *failure* branch of the
    /// durable sequence, which the other sink tests (all happy-path) never reach.
    struct FailingDestination;
    impl crate::destination::Destination for FailingDestination {
        fn write(&self, _local: &Path, _key: &str) -> Result<crate::destination::WriteOutcome> {
            Err(anyhow::anyhow!("injected destination write failure"))
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            crate::destination::DestinationCapabilities {
                commit_protocol: crate::destination::WriteCommitProtocol::Atomic,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    #[test]
    fn flush_failure_never_checkpoints_or_acks() {
        // The at-least-once invariant at the FAILURE boundary: the durable sequence is
        // flush → checkpoint → ack, and if the part write fails the checkpoint must NOT
        // be persisted and the source must NOT be acked — else a crash would drop a
        // change a consume-on-read source (PostgreSQL) had already advanced past. The
        // `?` on `flush` enforces this by construction; this test locks it against a
        // refactor that reorders the steps or swallows the flush error. (The crash
        // *between* flush and ack is covered by the live `cdc_after_flush_before_ack`
        // hook; this covers flush *itself* failing — a path no live test exercises.)
        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("cdc.ckpt");
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1)]), // committed ⇒ would ack if flush succeeded
            acked: Vec::new(),
        };
        let dest = FailingDestination;
        let mut c = cfg(&dest, &cols, FormatType::Parquet, 10);
        c.checkpoint = Some(ckpt.clone());

        let res = run_to_files(&mut stream, c);

        assert!(
            res.is_err(),
            "a destination write failure must fail the run"
        );
        assert!(
            stream.acked.is_empty(),
            "the source must NOT be acked when the part never became durable"
        );
        assert!(
            !ckpt.exists(),
            "the checkpoint must NOT be persisted when the part never became durable"
        );
    }
}
