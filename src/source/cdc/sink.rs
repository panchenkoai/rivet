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

/// Owns the **durable sequence** for one part — the invariant that makes the run
/// at-least-once: encode + upload the part, THEN persist the resume checkpoint,
/// THEN ack the source, in that exact order. A crash between any two steps re-reads
/// on resume; reordering would risk dropping a change a consume-on-read source
/// (PostgreSQL) had already advanced past. Checkpoint + ack happen only at a real
/// commit boundary — the final part can end mid-transaction (resumed from the last
/// committed position).
struct PartCommitter<'a> {
    dest: &'a dyn Destination,
    format: FormatType,
    checkpoint: Option<&'a Path>,
    seq: usize,
}

impl PartCommitter<'_> {
    fn commit(
        &mut self,
        buf: &[ChangeEvent],
        schema: &SchemaRef,
        columns: &[TypeMapping],
        stream: &mut dyn ChangeStream,
    ) -> Result<PartRecord> {
        let part = flush(buf, schema, columns, self.format, self.seq, self.dest)?;
        // Fault point: the part is durable but the checkpoint/ack have NOT run. A
        // crash here must re-read on resume (at-least-once) — never lose the change.
        crate::test_hook::maybe_panic_at("cdc_after_flush_before_ack");
        if let Some(last) = buf.last()
            && last.committed
        {
            if let Some(p) = self.checkpoint {
                last.position.save(p)?;
            }
            stream.ack(&last.position)?;
        }
        self.seq += 1;
        Ok(part)
    }
}

/// Stream canonical changes to typed Parquet/CSV parts, uploading each through the
/// commit seam, then writing a manifest + `_SUCCESS` at clean end. The loop only
/// pulls + buffers + asks the [`RolloverPolicy`]; the durable flush→checkpoint→ack
/// sequence lives in [`PartCommitter`].
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
    let mut emitted = 0usize;

    let policy = RolloverPolicy {
        rollover_rows: cfg.rollover,
        rollover_bytes: cfg.rollover_memory_bytes,
    };
    let mut committer = PartCommitter {
        dest: cfg.dest,
        format: cfg.format,
        checkpoint: cfg.checkpoint.as_deref(),
        seq: 0,
    };

    while let Some(ev) = stream.next_change() {
        let ev = ev?;
        if !cfg.tables.is_empty() && !cfg.tables.iter().any(|t| t == &ev.table) {
            continue;
        }
        let committed = ev.committed;
        buf_bytes += ev.estimated_bytes();
        buf.push(ev);
        emitted += 1;
        if policy.should_roll(buf.len(), buf_bytes, committed) {
            let sch = ensure_schema(&mut schema, &mut columns, &buf);
            parts.push(committer.commit(&buf, &sch, &columns, stream)?);
            buf.clear();
            buf_bytes = 0;
        }
        if cfg.max_events.is_some_and(|m| emitted >= m) {
            break;
        }
    }
    if !buf.is_empty() {
        let sch = ensure_schema(&mut schema, &mut columns, &buf);
        parts.push(committer.commit(&buf, &sch, &columns, stream)?);
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
        // Form B value-checksum recording is batch-path only for now.
        column_checksums: None,
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

    fn insert(id: i64) -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(id)]),
            position: Position(serde_json::json!({ "lsn": format!("{id:08X}") })),
            committed: true,
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
        let manifest = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
        )
        .unwrap();

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
            columns: cols,
            dest,
            dest_uri: String::new(),
            engine: "test",
            table: "t",
            format,
            tables: Vec::new(),
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
        let manifest = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
        )
        .unwrap();
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
        let csv = std::fs::read_to_string(dir.path().join("cdc-000000.csv")).unwrap();
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
            run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10)).unwrap();
        assert_eq!(manifest.row_count, 2);
        let csv = std::fs::read_to_string(dir.path().join("cdc-000000.csv")).unwrap();
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
        let manifest = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 10),
        )
        .unwrap();
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
        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        c.tables = vec!["t".into()];
        let manifest = run_to_files(&mut stream, c).unwrap();
        assert_eq!(
            manifest.row_count, 2,
            "only the two 't' changes are kept; 'other' is filtered out"
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
