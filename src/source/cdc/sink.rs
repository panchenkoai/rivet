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
use crate::pipeline::manifest_writer::{write_manifest, write_manifest_without_success_marker};
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
    /// `_rivet_row_hash` on the drain, using the same Rust render the snapshot
    /// leg applies (repair-design.md §5h). Carried per TABLE rather than per
    /// stream because a DECLARED column set names one table's columns; a
    /// multi-table stream with one is refused at config-load.
    pub row_hash: crate::config::RowHash,
}

/// Everything the sink needs that isn't the stream itself. `outputs` carries one
/// entry per captured table — several tables share ONE stream (one slot / one
/// binlog connection) and ONE checkpoint, because the resume position is a
/// property of the stream, not of a table.
pub(crate) struct SinkConfig<'a> {
    /// The rivet EXPORT these tables belong to (`exports[].name`). Recorded into
    /// every manifest's `export_family`, so the load's shared-prefix guard
    /// compares what the writer knew instead of re-deriving it from the table
    /// string — which is what made the drain and the snapshot leg read as two
    /// different exports whenever `name:` differed from `table:`.
    pub export_name: String,
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
    /// The central ledger, so every part reaches the DATABASE as it becomes
    /// durable — not just the manifest beside it. `None` only where there is no
    /// state store to write to (the unit sinks, which have no database).
    pub state: Option<&'a crate::state::StateStore>,
    /// Cumulative DECODED bytes READ from the source this run (#196) — the read
    /// leg, so a CDC run records bytes_read like a batch run instead of a false 0.
    /// Accumulated per captured event; never reset (unlike the rollover buffer's
    /// own byte counter). A shared Arc so the caller reads the total after the
    /// drain. `Default` = a throwaway counter (unit sinks / tests that ignore it).
    pub read_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
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
    /// Finding #38: per-column value checksums, sum-accumulated across parts
    /// (the same combining rule `validate_recorded_checksums` applies on
    /// re-read — see `value_checksum::Fold`) — recorded into the manifest so `rivet validate` Form B
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
        ledger: Option<(&crate::state::StateStore, &str, &str)>,
    ) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // The schema is built lazily at the first flush so decimal column
        // scales can be refined from the data (SQL Server's metadata-only
        // resolve gives a placeholder scale of 0 — the same gap the batch path
        // fills from rows).
        let sch = ensure_schema(
            &mut self.schema,
            &mut self.out.columns,
            &self.buf,
            &self.out.row_hash,
        );
        let (part, sums) = flush(
            &self.buf,
            &sch,
            &self.out.columns,
            engine,
            format,
            run_token,
            self.seq,
            self.out.dest,
            &self.out.row_hash,
        )?;
        for (name, sum) in sums {
            // wrapping_add, not XOR — the same fold `Fold::Sum` applies on
            // re-read. Under `^` two parts whose column checksums coincide
            // cancelled to zero, so a CDC prefix could publish a checksum that
            // verified anything.
            let e = self.column_sums.entry(name).or_insert(0);
            *e = e.wrapping_add(sum);
        }
        // The part is durable at the destination NOW — so the ledger learns
        // about it NOW, at the same seam the batch path uses (`commit.rs`
        // `record_file`), and strictly BEFORE the checkpoint/ack that
        // `roll_all` performs next.
        //
        // Without this the state DB was not a record of a CDC run at all: it
        // got `begin_run` at the start and one aggregate `export_metrics` row at
        // finalize, and NOTHING per part — so a run that died mid-stream left
        // durable, manifest-covered rows the database had never heard of. The
        // manifest was finer-grained AND earlier than the ledger, which inverts
        // the intended relationship: the database is the record, the manifest is
        // its projection at the destination.
        //
        // Non-fatal, matching ADR-0001 I7: the file is already durable, so a
        // ledger write failure is logged, never a reason to fail a run whose
        // data is safe.
        // The ledger key is (run_id, file_name), and the part name carries only
        // this sink's OWN sequence — `cdc-<run_token>-000000` — so in a
        // multi-table CDC export every table's first part has the SAME name.
        // The files do not collide (each table writes under its own sub-prefix,
        // `cdc_job::dest_for_table`), but the ledger rows did: `record_durable_part`
        // deletes-then-inserts on that key, so each roll erased the sibling
        // table's row and the projected aggregate under-reported both parts and
        // rows. Qualify the LEDGER name with the table — the same distinction the
        // destination already makes — rather than renaming the object on disk.
        let ledger_name = ledger_part_name(&self.out.table, &part.file_name);
        if let Some((state, export_name, run_id)) = ledger
            && let Err(e) = state.record_durable_part(crate::state::DurablePart {
                run_id,
                export_name,
                file_name: &ledger_name,
                rows: part.rows,
                bytes: part.bytes as i64,
                format: format.label(),
                compression: Some(part_compression(format).label()),
                mode: "cdc",
                cursor_high: None, // CDC sink has no keyset page cursor
            })
        {
            log::warn!(
                "cdc: ledger write failed for '{}' (the part IS durable): {e:#}",
                part.file_name
            );
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
/// Warn ONCE per (schema, table) per process that images are mapping by position.
///
/// The truth about a MySQL binlog image is on the wire — a `TABLE_MAP` written at
/// `binlog_row_metadata=MINIMAL` carries no column names, whatever the server's
/// setting is today. `row_metadata_warning` (the open-time probe) is the early
/// hint; this is the one that cannot be wrong.
fn warn_positional_once(schema: &str, table: &str) {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let key = format!("{schema}.{table}");
    let mut seen = SEEN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if seen.insert(key.clone()) {
        log::warn!(
            "cdc: {key}: this change image carries no column NAMES, so values are \
             mapped by POSITION. On MySQL that is `binlog_row_metadata = MINIMAL` \
             (the server default) — note the events replay the setting in force when \
             they were WRITTEN, so a server switched to FULL still drains a MINIMAL \
             backlog this way. A same-arity DDL that reorders columns then silently \
             SWAPS them; an arity-changing one aborts the flush. Set \
             `binlog_row_metadata = FULL` (8.0.1+) and re-capture from before the DDL"
        );
    }
}

/// Does this configured `table:` name the relation an event came from?
///
/// The ENGINE decides how many readings a dotted string has, and passing it is not
/// ceremony: on a store with schemas, `a.b` may be a qualifier; on one without, it
/// can only be a name. While Mongo shared SQL's two-reading rule, a collection whose
/// first segment equalled the DATABASE name matched twice — `table: shopdb.orders`
/// took both the collection literally named `shopdb.orders` and the sibling
/// `orders`, interleaving two collections into one destination with every count
/// intact (round-3B bughunt).
pub(crate) fn table_matches(
    engine: super::CdcEngine,
    cfg: &str,
    schema: &str,
    table: &str,
) -> bool {
    // Full-name match FIRST: a MongoDB collection name may contain dots
    // (`my.coll`) and has no schema qualifier, so splitting it into a bogus
    // `schema.table` dropped every event (bug-hunt: 0-row success forever). This
    // is safe for SQL — no real table is literally named `schema.table`.
    if cfg == table {
        return true;
    }
    // A document store has no schema to qualify with, so there is no second
    // reading — the full-name arm above was the whole answer.
    if engine == super::CdcEngine::Mongo {
        return false;
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
    export_name: &str,
    run_token: &str,
    checkpoint: Option<&Path>,
    last_commit: &Option<Position>,
    unacked_commit: &mut bool,
    run_id: &str,
    started_at: &str,
    state: Option<&crate::state::StateStore>,
) -> Result<()> {
    for s in sinks.iter_mut() {
        s.flush_buffered(
            engine,
            format,
            run_token,
            state.map(|st| (st, export_name, run_id)),
        )?;
    }
    // Fault point: the parts are durable but the checkpoint/ack have NOT run. A
    // crash here must re-read on resume (at-least-once) — never lose the change.
    crate::test_hook::maybe_panic_at("cdc_after_flush_before_ack");
    if *unacked_commit && let Some(p) = last_commit {
        // Round-2 audit #11: make the just-flushed parts durably manifest-covered
        // BEFORE the checkpoint/ack advances past them. The ack advances a consume-
        // on-read slot (PG) irreversibly; the manifest-authoritative `rivet load`
        // only loads parts a `Success` manifest declares — so without this, a crash
        // in the ack→terminal-manifest window would orphan the acked parts (silent,
        // count-gate-invisible loss). A `Success` run-unique manifest (no `_SUCCESS`
        // marker yet — the prefix is not complete) is idempotently rewritten as a
        // superset each roll; the terminal write at clean end adds `_SUCCESS`.
        for s in sinks.iter() {
            let manifest = build_manifest(
                engine,
                &s.column_sums,
                &s.out,
                export_name,
                format,
                run_id,
                started_at,
                &s.parts,
            );
            write_manifest_without_success_marker(s.out.dest, &manifest)?;
        }
        if let Some(ck) = checkpoint {
            p.save(ck)?;
        }
        // Fault point: manifest + checkpoint persisted, source NOT acked — a crash
        // here must re-read (PG would re-peek; the file checkpoint already moved,
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
/// Drain the stream to files, returning WHAT WAS MADE DURABLE alongside the
/// outcome.
///
/// The tuple is the point. This returned `Result<Vec<RunManifest>>`, so an
/// error discarded the manifests entirely and the caller had nothing to record
/// but zeros — see the comment on the drain closure below. `(manifests, result)`
/// mirrors `mongo_parallel::range_worker`, which was changed to the same shape
/// for the same reason: durable work and run outcome are independent facts, and
/// a type that can only express one of them forces the caller to lie about the
/// other.
pub(crate) fn run_to_files(
    stream: &mut dyn ChangeStream,
    cfg: SinkConfig<'_>,
) -> (Vec<RunManifest>, Result<()>) {
    let run_token = run_token(&cfg.run_id);
    let read_bytes = std::sync::Arc::clone(&cfg.read_bytes);
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

    // Re-drain loop. One inner pass drains everything readable from the stream's
    // CURRENT position, then rolls (flush → checkpoint → ack). The ack advances a
    // consume-on-read slot (PostgreSQL) past the whole consumed span — INCLUDING
    // uncaptured-table transactions and empty (DDL) spans, whose commit boundary
    // was recorded before the routing filter — so the next inner pass peeks FRESH
    // WAL beyond it. Without this, a foreign/empty span larger than one peek
    // window starved the slot: the peek re-read the same window, the run
    // exhausted, and it wrote `_SUCCESS` with in-bound captured data still
    // unread (the density-below-1/3 gap the ×3 peek escalation only partly
    // covered — an uncaptured or empty span has an unbounded wire:capture ratio).
    // Engines whose read cursor advances on its own (MySQL binlog / MSSQL from-LSN
    // / Mongo token) never starve: their re-drain pass yields nothing and the loop
    // exits at once. Termination: each pass that yields ≥1 event advances the slot
    // toward the open bound (finite WAL); a pass yielding zero has drained to the
    // bound.
    let mut hit_max = false;
    // The drain is wrapped so that an error does NOT discard what is already
    // durable. Every `?` below used to abandon `sinks`, whose `parts` list the
    // parts this run flushed, checkpointed and ACKED — so a run that captured
    // and acked N rows and then failed reported 0 rows / 0 files / no
    // FileWritten events, while the object store held the data and the source
    // was advanced past it. The data was never at risk (`roll_all` writes a
    // durable run-unique manifest before each ack), but every RECORD of it was:
    // an operator reading "failed, 0 rows" concludes nothing was captured and
    // re-runs, and the changes are already gone from the log.
    let drain: Result<()> = (|| {
        loop {
            // Graded LIVE, not offline, and deliberately not excluded: `+= -> *=`
            // pins this at 0, `drain_is_complete` then ends the loop after one pass,
            // and the re-drain starvation tests go RED — measured 2026-08-27, six of
            // them, led by roast_pg_cdc_reaches_open_bound_past_a_large_uncaptured_
            // transaction. An exclusion would have to name `+= in run_to_files`,
            // which also matches `total_rows`, `emitted` and `total_bytes` — all
            // three graded (the first two by the lib suite, the third by
            // `the_byte_rollover_cap_really_rolls_and_is_not_a_dead_accumulator`).
            // Over-excluding graded mutants is worse than leaving one MISSED.
            let mut yielded_this_pass = 0usize;
            while let Some(ev) = stream.next_change() {
                let mut ev = ev?;
                yielded_this_pass += 1;
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
                // Routing is a BLOCK, not a `continue`: the soft cap below must run
                // for every event, because a commit boundary is a STREAM property
                // that can land on an UNCAPTURED table (the `commit_boundary_on_an_
                // uncaptured_table_…` shape). A `continue` here made the cap dead on
                // exactly those streams — unbounded overshoot. An uncaptured event
                // simply skips the block, which also drops its deferred poison, as
                // the `continue` used to.
                if let Some(sink) = sinks
                    .iter_mut()
                    .find(|s| table_matches(cfg.engine, &s.out.table, &ev.schema, &ev.table))
                {
                    // Confirmed routed to a captured table → surface any deferred
                    // decode error (uncaptured tables' poison never applies).
                    ev.raise_poison()?;
                    let eb = ev.estimated_bytes();
                    total_bytes += eb;
                    read_bytes.fetch_add(eb as u64, std::sync::atomic::Ordering::Relaxed);
                    sink.buf.push(ev);
                    total_rows += 1;
                    emitted += 1;
                    if policy.should_roll(total_rows, total_bytes, committed) {
                        roll_all(
                            &mut sinks,
                            stream,
                            cfg.engine,
                            cfg.format,
                            &cfg.export_name,
                            &run_token,
                            checkpoint,
                            &last_commit,
                            &mut unacked_commit,
                            &cfg.run_id,
                            &cfg.started_at,
                            cfg.state,
                        )?;
                        total_rows = 0;
                        total_bytes = 0;
                    }
                }
                // SOFT cap, judged at the COMMIT BOUNDARY — the same treatment
                // `should_roll` gives every other budget. Judged per event, the cap
                // cut a transaction mid-flight: the partial tail flushed as a real
                // part and the end-of-pass roll skipped checkpoint AND ack, so every
                // later run re-read the same transaction forever (on PostgreSQL the
                // slot then pins WAL until the disk fills). It runs AFTER the routed
                // push — the boundary event itself must be captured before stopping,
                // or the ack advances past an unwritten row — and OUTSIDE the routed
                // block, so a boundary on an uncaptured table still honours it.
                if committed && cfg.max_events.is_some_and(|m| emitted >= m) {
                    hit_max = true;
                    break;
                }
            }
            // End-of-pass roll: flush any buffered captured tail AND ack the consumed
            // span. Fires when a captured table has buffered rows OR when a commit
            // boundary is unacked — the latter advances the slot past an
            // uncaptured-only span (bug-hunt K) and, in the re-drain loop, is what
            // lets the next pass slide forward. `roll_all` flushes nothing when the
            // buffers are empty; it just persists the checkpoint + acks. The
            // checkpoint only ever lands on `last_commit` (a transaction boundary),
            // so a `max_events` stop mid-span still checkpoints a whole transaction.
            let buffered_rows: usize = sinks.iter().map(|s| s.buf.len()).sum();
            if pass_must_roll(unacked_commit, buffered_rows) {
                roll_all(
                    &mut sinks,
                    stream,
                    cfg.engine,
                    cfg.format,
                    &cfg.export_name,
                    &run_token,
                    checkpoint,
                    &last_commit,
                    &mut unacked_commit,
                    &cfg.run_id,
                    &cfg.started_at,
                    cfg.state,
                )?;
                total_rows = 0;
                total_bytes = 0;
            }
            // A pass that yielded nothing has drained to the bound; `max_events`
            // stops the whole run at the cap.
            if drain_is_complete(hit_max, yielded_this_pass) {
                break;
            }
        }
        Ok(())
    })();

    // Fault point: all parts durable + acked, terminal manifest (+`_SUCCESS`) not
    // yet written. A crash here is recoverable WITHOUT re-capturing acked data:
    // `roll_all` already wrote a durable `Success` run-unique manifest covering
    // every acked part before each ack (round-2 audit #11), so the manifest-
    // authoritative loader still sees those rows; only the `_SUCCESS` completion
    // marker is missing, which the next cycle's terminal write supplies.
    crate::test_hook::maybe_panic_at("cdc_before_manifest");

    // Built from `sinks` on BOTH paths. On the clean path this also writes the
    // terminal manifest + `_SUCCESS`; on the error path the parts are already
    // covered by the per-roll run-unique manifest `roll_all` wrote before each
    // ack, so the manifest is built for the CALLER's accounting and no
    // `_SUCCESS` is claimed — the run did not succeed.
    let mut manifests = Vec::with_capacity(sinks.len());
    // A failure to write the TERMINAL manifest becomes the run's outcome, but
    // must not discard the manifests either — the parts it describes are
    // durable regardless of whether the marker landed.
    let mut write_err: Option<anyhow::Error> = None;
    for s in &sinks {
        let manifest = build_manifest(
            cfg.engine,
            &s.column_sums,
            &s.out,
            &cfg.export_name,
            cfg.format,
            &cfg.run_id,
            &cfg.started_at,
            &s.parts,
        );
        // write_manifest leaves the canonical `manifest.json` (latest-run pointer)
        // AND an immutable run-unique copy, so a prefix accumulating several
        // `until_current` cycles keeps EACH run's manifest for cross-run reconcile.
        //
        // Only on the clean path: writing `_SUCCESS` for a run that FAILED would
        // tell every downstream reader the prefix is complete. A failed run's
        // parts stay declared by the per-roll manifest, which carries no marker.
        if drain.is_ok()
            && write_err.is_none()
            && let Err(e) = write_manifest(s.out.dest, &manifest)
        {
            write_err = Some(e);
        }
        manifests.push(manifest);
    }
    match (drain, write_err) {
        (Err(e), _) => (manifests, Err(e)),
        (Ok(()), Some(e)) => (manifests, Err(e)),
        (Ok(()), None) => (manifests, Ok(())),
    }
}

/// Build the sink schema once, on the first flush — refining decimal scales from
/// the first batch's values first (`__op`, `__pos`, then the typed columns).
fn ensure_schema(
    schema: &mut Option<SchemaRef>,
    columns: &mut [TypeMapping],
    events: &[ChangeEvent],
    row_hash: &crate::config::RowHash,
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
        if row_hash.enabled() {
            fields.push(Field::new(
                crate::enrich::COL_ROW_HASH,
                DataType::Int64,
                false,
            ));
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
                let img = if e.op.values_live_in_before() {
                    e.before.as_ref()
                } else {
                    e.after.as_ref()
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
        // No `.filter(|s| *s > 0)`: the arm above binds only `Decimal128(p, 0)`, so
        // a scale of 0 would reassign the value the field already holds. The filter
        // was unkillable — `>` and `>=` are observationally identical here — and an
        // unkillable mutant is redundant code, not an exclusion to write down.
        if let Some(s) = scale {
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

/// One cell of a row image — resolved by NAME when the image carries names, by
/// POSITION when it does not.
///
/// Hoisted out of `flush` because it was NESTED inside it. `flush` is live-only
/// glue, so nothing offline could reach this, and a mutation run over
/// `src/source/cdc/sink.rs` measured FOUR survivors in these few lines — every
/// decision it makes:
///
/// * `n == col` → `!=` picks the first column that is NOT the one asked for,
///   so every cell comes back holding a neighbour's value;
/// * the `vals.len() == ncols` guard → `true` restores the pre-round-13 read
///   (a short image indexed by position, reading past its end), → `false`
///   degrades a mid-window RENAME to NULL, which is the silent-loss shape the
///   guard was added against.
///
/// It is pure — an event, an index, a name, an arity and a memo in; a borrowed
/// value out — so nesting bought nothing and cost the grade. See
/// `image_cell_resolves_by_name_and_falls_back_only_at_matching_arity`.
pub(crate) fn image_cell<'e>(
    e: &'e ChangeEvent,
    i: usize,
    col: &str,
    ncols: usize,
    memo: Option<(&std::sync::Arc<[String]>, Option<usize>)>,
) -> Option<&'e RivetValue> {
    let vals = if e.op.values_live_in_before() {
        e.before.as_ref()?
    } else {
        e.after.as_ref()?
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

/// Must this drain pass ROLL — flush, checkpoint and ack?
///
/// Two reasons, and the second is the one that is easy to lose. Buffered rows
/// obviously have to reach a part. But a pass that consumed only UNCAPTURED or
/// empty WAL has nothing buffered and still must roll: on a consume-retention
/// engine the reader only slides forward when the sink ACKS, so skipping the roll
/// re-reads the same window forever (`roast_pg_cdc_reaches_open_bound_past_a_large_
/// uncaptured_transaction`, and the DDL-churn slot pin beside it).
///
/// `||` → `&&` makes the ack wait for rows that an uncaptured span never produces;
/// deleting the `!` rolls only while the buffers are EMPTY, which is every case
/// except the one that has data. Both survived a mutation run over this file
/// because `run_to_files` is live-only glue and nothing offline reached the
/// condition inside it.
/// Takes the buffered ROW COUNT rather than a bool, so the emptiness test lives
/// here too. A first cut took `any_buffered: bool` and left
/// `sinks.iter().any(|s| !s.buf.is_empty())` at the call site — the `delete !`
/// mutant then SURVIVED the predicate's own unit matrix, because the predicate
/// could not see how its argument was computed. Moving an operator out of glue is
/// not the same as grading it; the signature has to swallow the decision.
pub(crate) fn pass_must_roll(unacked_commit: bool, buffered_rows: usize) -> bool {
    unacked_commit || buffered_rows > 0
}

/// Has the drain loop finished — the cap, or a pass that yielded nothing?
///
/// `yielded == 0` is the bound-reached signal: the re-drain loop keeps re-peeking
/// fresh log after each ack, so "this pass produced no event" is the only honest
/// end. `== 0` → `!= 0` inverts it into "stop as soon as anything arrives",
/// i.e. one pass per run; `||` → `&&` never stops at the cap unless the source
/// also happens to be empty.
pub(crate) fn drain_is_complete(hit_max: bool, yielded_this_pass: usize) -> bool {
    hit_max || yielded_this_pass == 0
}

/// A NAMED image whose value count disagrees with its name count — a PARTIAL
/// image. `Some((values, names))` when they disagree, and the caller formats.
///
/// Round 13's guard, which lived as an `if` inside `flush` and was proven only by a
/// live test (`SET GLOBAL binlog_row_image=MINIMAL`, one UPDATE). Offline, its `!=`
/// → `==` mutant survived: refusing every image whose arity AGREES is the whole
/// stream, so a green suite said nothing about the case that matters.
///
/// A key-only DELETE is not a counter-example: PostgreSQL names only the key
/// columns it also supplies, so its lengths agree.
pub(crate) fn named_image_arity_mismatch(ev: &ChangeEvent) -> Option<(usize, usize)> {
    let names = ev.image_names.as_deref()?;
    let n = ev.after.as_ref().or(ev.before.as_ref()).map_or(0, Vec::len);
    (n != names.len()).then_some((n, names.len()))
}

/// A NAMELESS image mapped by POSITION whose width disagrees with the schema —
/// a DDL landed inside the capture window. `Some(values)` when it does.
///
/// The DELETE arm is `>` and not `!=` on purpose: a key-only delete legitimately
/// carries FEWER values than the table has columns, so only an image WIDER than the
/// schema is evidence of drift. `>` → `<` inverts that into "refuse every key-only
/// delete and accept every wide one" — it survived offline because the condition
/// sat inside live-only glue.
pub(crate) fn positional_image_width_mismatch(ev: &ChangeEvent, ncols: usize) -> Option<usize> {
    if ev.image_names.is_some() {
        return None;
    }
    let is_delete = ev.op == ChangeOp::Delete;
    let vals = if is_delete {
        ev.before.as_ref()?
    } else {
        ev.after.as_ref()?
    };
    let n = vals.len();
    let bad = if is_delete { n > ncols } else { n != ncols };
    bad.then_some(n)
}

/// The O(1) name-lookup memo `image_cell` takes: this column's index in the
/// image-names vector every event in the flush shares.
///
/// Extracted for the reason `pass_must_roll` was: the decision was at the CALL
/// SITE. `image_cell`'s own matrix passes the memo in, so `n == col` here — a
/// different `==` from the one inside the predicate — sat in live-only glue and its
/// `!=` mutant survived. A wrong memo is silent and total: every event in the flush
/// shares one names-Arc, so a memo pointing at the wrong index maps EVERY row of
/// that column to a neighbour's value.
///
/// `None` when no event carries names (the positional engines) — then `image_cell`
/// indexes by position and there is nothing to memoise.
pub(crate) fn image_name_memo<'a>(
    events: &'a [ChangeEvent],
    col: &str,
) -> Option<(&'a std::sync::Arc<[String]>, Option<usize>)> {
    let names = events.iter().find_map(|e| e.image_names.as_ref())?;
    Some((names, names.iter().position(|n| n == col)))
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
    row_hash: &crate::config::RowHash,
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
        if ev.image_names.is_some() {
            // Named, but NOT unconditionally trustworthy — this bypass used to be
            // `continue`, and a partial row image walked straight through it.
            //
            // MySQL builds `image_names` from the TABLE_MAP's full column list while
            // the VALUES come from `BinlogRow::unwrap()`, which yields only the
            // columns set in the image bitmap. Under `binlog_row_image = MINIMAL` (or
            // NOBLOB, or a per-SESSION override the open-time `@@global` probe cannot
            // see) the two disagree, `image_cell` resolves every name past the end of
            // the short value vector, and the row lands ALL NULL. MEASURED on the
            // mysql-cdc stand: an UPDATE written under MINIMAL delivered
            // `id=NULL, a=NULL, b=NULL` at `status: success, rows: 1`, no warning,
            // checkpoint advanced past it — the change gone for good.
            //
            // The events replay the setting in force when they were WRITTEN, which is
            // the same present-tense trap the `binlog_row_metadata` probe below
            // already documents. Arity is the evidence that survives the wire.
            //
            // A key-only DELETE is not a counter-example: PostgreSQL names only the
            // key columns it also supplies, so its lengths agree.
            let Some((n, nnames)) = named_image_arity_mismatch(ev) else {
                continue; // named AND complete — mapped by name, arity-proof for any op
            };
            anyhow::bail!(
                "cdc: {}.{}: a row image carries {n} value(s) under {} column \
                     name(s) — a PARTIAL image, which rivet cannot map without \
                     fabricating the missing cells as NULL. On MySQL this is \
                     `binlog_row_image` set to something other than FULL when the \
                     change was WRITTEN (a session override counts, and the open-time \
                     probe reads only the global). Set `binlog_row_image = FULL` and \
                     re-capture from before the affected DDL/DML; the changes are \
                     still in the binlog, so this is a delay rather than a loss.",
                ev.schema,
                ev.table,
                nnames
            );
        }
        // A NAMELESS image maps by POSITION, and this is the only place that knows
        // it for certain. The open-time probe asks the server what
        // `binlog_row_metadata` is set to NOW; these events replay whatever was in
        // force when they were WRITTEN. MEASURED: a server at FULL draining a
        // backlog written under MINIMAL, with a same-arity `MODIFY .. AFTER`
        // across the resume boundary, produced `a='BBB', b='AAA'` — swapped,
        // status success, and ZERO warnings, because the probe answered a question
        // about the present.
        //
        // Once per table per flush, not per event: a MINIMAL backlog is every
        // event, and a line per row is a line nobody reads.
        warn_positional_once(&ev.schema, &ev.table);
        if let Some(n) = positional_image_width_mismatch(ev, columns.len()) {
            anyhow::bail!(
                "cdc: an event for table '{}' carries {} column(s) but the resolved \
                 schema has {} — a DDL landed inside this capture window, and mapping \
                 by position would put values into the WRONG columns. Recover by \
                 re-snapshotting the table — clear its `cdc_snapshot` row in the \
                 state DB AND delete its snapshot/_SUCCESS marker (the two done- \
                 signals are OR-ed; leaving either in place skips the snapshot) — or \
                 by resetting the checkpoint past the DDL. \
                 To make mid-stream DDL safe going forward, set \
                 binlog_row_metadata=FULL on the MySQL server (8.0.1+) — rivet then \
                 maps binlog images by column NAME and this error class disappears.",
                ev.table,
                n,
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
        // O(1) name lookup for the common case: all events in a flush share
        // one names-Arc (same TABLE_MAP / same wire session), so resolve this
        // column's image index once and reuse it by pointer identity.
        let memo = image_name_memo(events, &m.column_name);
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
    // `_rivet_row_hash` covers the DATA columns only — never this sink's own
    // `__op`/`__pos`/`__seq`, which the snapshot leg does not have. Both legs
    // write the same `__changes` log, so folding the meta columns in here would
    // give the two legs different hashes for the same row and make the column
    // useless for exactly the comparison it exists to serve.
    //
    // The image the engine actually sent is what gets hashed. For a DELETE that
    // is the before-image, which some engines populate with only the key
    // columns; the resulting hash covers NULL content by that engine's own
    // model, and the audit never reads it, because a tombstoned row is filtered
    // out (`NOT __is_deleted`).
    //
    // `col_sums` deliberately excludes the hash: it records SOURCE column
    // checksums, and the batch leg likewise omits its own added columns.
    let batch = if row_hash.enabled() {
        let data: Vec<String> = columns.iter().map(|m| m.column_name.clone()).collect();
        let covered = crate::enrich::row_hash_columns_of(&data, row_hash)?;
        // The batch is assembled twice: once without the hash so the hash can
        // be computed from it, once with. The alternative — hashing the raw
        // arrays — would duplicate the row loop this sink already has.
        let base = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .take(arrays.len())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let bare = RecordBatch::try_new(base, arrays.clone())?;
        arrays.push(crate::enrich::row_hash_array(&bare, &covered)?);
        RecordBatch::try_new(schema.clone(), arrays)?
    } else {
        RecordBatch::try_new(schema.clone(), arrays)?
    };

    let tmp = NamedTempFile::new()?;
    let fmt = crate::format::create_format(format, part_compression(format), None, None);
    let writer: Box<dyn std::io::Write + Send> = Box::new(tmp.reopen()?);
    let mut w = fmt.create_writer(schema, writer)?;
    w.write_batch(&batch)?;
    w.finish()?;

    let file_name = format!("cdc-{run_token}-{seq:06}.{}", format.label());
    let part = write_part_file(dest, tmp.path(), events.len() as i64, file_name)?;
    Ok((part, col_sums))
}

/// The name a CDC part is recorded under in `file_log`.
///
/// The ledger key is `(run_id, file_name)` and a part's own name carries only its
/// sink's sequence — `cdc-<run_token>-000000` — so in a MULTI-TABLE cdc export
/// every table's first part is called the same thing. The objects do not collide
/// (each table writes under its own sub-prefix, `cdc_job::dest_for_table`), but
/// the ledger rows did: `record_durable_part` deletes-then-inserts on that key,
/// so each roll erased the sibling table's row and the projected aggregate
/// under-reported both parts and rows.
///
/// One function so the sink and its test agree by CONSTRUCTION rather than by two
/// copies of a format string.
pub(crate) fn ledger_part_name(table: &str, file_name: &str) -> String {
    format!("{table}/{file_name}")
}

/// How a CDC part is compressed, per format — the ONE place that decides.
///
/// It was two places: the writer picked `None` for CSV and `Zstd` for Parquet,
/// and `build_manifest`, sixty lines below, wrote the literal `"zstd"` for both.
/// So a `format: csv` CDC export shipped plain-text parts under a manifest
/// advertising zstd. Nothing in the pipeline reads the field back, which is why
/// it survived — but the manifest is the contract a cross-boundary consumer
/// decodes by, and a batch CSV export records the truth (`plan.compression
/// .label()`), so the two legs described the same artifact differently.
pub(crate) fn part_compression(format: FormatType) -> CompressionType {
    match format {
        FormatType::Csv => CompressionType::None,
        FormatType::Parquet => CompressionType::Zstd,
    }
}

/// Assemble one table's `RunManifest` from its committed parts (hand-built — no
/// plan coupling; `record_part` is the plan-bound path the batch export uses).
#[allow(clippy::too_many_arguments)] // the export identity joined an existing 7-arg builder
fn build_manifest(
    engine: super::CdcEngine,
    column_sums: &std::collections::BTreeMap<String, u64>,
    out: &TableOutput<'_>,
    export_name: &str,
    format: FormatType,
    run_id: &str,
    started_at: &str,
    parts: &[PartRecord],
) -> RunManifest {
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        split_window: None, // CDC is a stream, never range-split
        mode: "cdc".to_string(),
        run_id: run_id.to_string(),
        // `export_name` stays the TABLE string — established wire format that
        // multiplexed loads key sub-prefixes on. The FAMILY is the parent
        // export, recorded so the load guard can group the drain with its
        // snapshot leg without guessing from either string.
        export_family: export_name.to_string(),
        export_name: out.table.clone(),
        started_at: started_at.to_string(),
        // A real finish instant (RFC3339) — NOT the run_id. The field is parsed as
        // a timestamp by the load's `latest_full`; a run_id here was a dormant
        // landmine (safe only while CDC never sorts by finished_at).
        finished_at: chrono::Utc::now().to_rfc3339(),
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
        // From the SAME TableOutput the flush hashed with, so the manifest can
        // never advertise a contract this run did not apply.
        row_hash: crate::enrich::RowHashContract::of(&out.row_hash),
        format: format.label().to_string(),
        compression: part_compression(format).label().to_string(),
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
        // Stamped because this manifest DOES carry checksums: without the
        // marker a reader falls back to the v1 (annihilating XOR) fold and
        // reports every CDC prefix as corrupt.
        checksum_render: Some(crate::source::value_checksum::CHECKSUM_RENDER_ID.to_string()),
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
    use crate::source::cdc::CdcEngine;
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
        fn engine(&self) -> super::super::CdcEngine {
            super::super::CdcEngine::Postgres
        }

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
    /// `rollover_memory_bytes` really rolls — the BYTE cap, driven end to end.
    ///
    /// `should_roll` is pure and its unit matrix feeds `buf_bytes` every arm, so the
    /// DECIDER was well tested. Its SUPPLIER was not: `total_bytes += eb` → `*=`
    /// pins the accumulator at 0 forever, and the mutant survived the lib suite AND
    /// the whole live CDC suite (106 tests, measured). Both halves correct, the seam
    /// between them observed by nothing — the third defect class in CLAUDE.md, and
    /// the one mutation testing is structurally blind to when you only mutate the
    /// consumer.
    ///
    /// What it costs in production: with the byte cap silently dead, a stream of wide
    /// rows buffers to the ROW cap instead, so the memory ceiling an operator set is
    /// not the one they get.
    ///
    /// The row cap is set far above the event count on purpose, so the ONLY thing
    /// that can split these parts is bytes.
    #[test]
    fn the_byte_rollover_cap_really_rolls_and_is_not_a_dead_accumulator() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let events: Vec<ChangeEvent> = (1..=6).map(insert).collect();
        let one = events[0].estimated_bytes();
        assert!(
            one > 0,
            "the fixture is inert: an event must weigh something"
        );

        let mut stream = FakeStream {
            events: events.into(),
            acked: Vec::new(),
        };
        let cfg = SinkConfig {
            // Rows can never trigger: 100 ≫ 6.
            rollover_memory_bytes: Some(one * 2),
            ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 100)
        };
        let (m, r) = run_to_files(&mut stream, cfg);
        r.unwrap();
        let manifest = m.into_iter().next().expect("one table, one manifest");
        assert_eq!(manifest.row_count, 6, "every event must still be delivered");
        // EXACTLY three, not merely "more than one". The cap is `>=`, so it rolls AT
        // two events' worth, giving 2+2+2; `>` would roll only past it, giving 3+3.
        // Both are "more than one part", so the loose assertion could not tell the
        // boundary apart — measured, the `>= -> >` mutant survived it.
        // The manifest's part_ids are 1-BASED, matching what the batch path records.
        // `(i + 1)` -> `i * 1` renumbers them 0..n-1: still unique, so the manifest's
        // own consistency check passes, and the ledger keys parts by file name so
        // nothing cross-references them either — the mutant is behaviourally
        // equivalent and survived. The `+ 1` is not redundant though: it is a wire
        // convention a downstream consumer can join on, so it is PINNED here rather
        // than deleted. (Contrast the `*s > 0` filter in `refine_decimal_scales`,
        // which guarded a write that changed nothing and was removed.)
        assert_eq!(
            manifest.parts.iter().map(|p| p.part_id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "CDC manifest part ids are 1-based and contiguous; a silent renumbering \
             is a wire-format change for anything joining on them"
        );
        assert_eq!(
            manifest.part_count, 3,
            "6 events at a byte cap of exactly two events' worth must land in three \
             parts: one part means the byte accumulator never moved and the cap is \
             decoration, two means it rolls one event late"
        );
    }

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
            export_name: "t".into(),
            checkpoint: Some(ckpt.clone()),
            ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 10)
        };
        {
            let (m, r) = run_to_files(&mut stream, cfg);
            r.unwrap();
            m
        };
        assert!(
            Position::load(&ckpt).unwrap().is_some(),
            "the stream's commit boundary must advance the checkpoint even when \
             its event routes to an uncaptured table"
        );
        assert_eq!(stream.acked.len(), 1, "and the source must be acked");
    }

    // Parallel-CDC contamination fix: a deferred `poison` (e.g. PG unchanged-TOAST
    // with no pre-image) on a table this run does NOT capture must be dropped WITH
    // the event, never surfaced. One mis-configured table sharing the slot (a
    // DEFAULT-replica-identity TOAST table) previously bailed capture of every
    // unrelated table — the class that RED'd two live PG CDC tests off a foreign
    // fixture. RED against a pre-fix build: the poison bails at the source.
    #[test]
    fn poison_on_an_uncaptured_table_is_dropped_never_bails_the_run() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut foreign = insert(2);
        foreign.table = "audit_log".into(); // NOT captured (captured table is "t")
        foreign.poison = Some("pg cdc: s.audit_log: column [big] unchanged-TOAST".into());
        let captured = insert(1); // captured table "t", clean
        let mut stream = FakeStream {
            events: vec![foreign, captured].into(),
            acked: Vec::new(),
        };
        let cfg = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        {
            let (mm, r) = run_to_files(&mut stream, cfg);
            r.expect("poison on an uncaptured table must be dropped, never bail the run");
            mm
        };
    }

    // The safety half: a poison on a CAPTURED table still fails loud (the deferral
    // must not swallow a real integrity refusal for a table we DO write).
    #[test]
    fn poison_on_a_captured_table_bails_with_its_message() {
        let d = tempfile::tempdir().unwrap();
        let dest = local_dest(&d);
        let cols = int_col();
        let mut poisoned = insert(1); // captured table "t"
        poisoned.poison = Some(
            "pg cdc: s.t: column [big] unchanged-TOAST — ALTER TABLE ... REPLICA IDENTITY FULL"
                .into(),
        );
        let mut stream = FakeStream {
            events: vec![poisoned].into(),
            acked: Vec::new(),
        };
        let cfg = cfg(dest.as_ref(), &cols, FormatType::Parquet, 10);
        let err = run_to_files(&mut stream, cfg)
            .1
            .expect_err("poison on a captured table must bail");
        assert!(
            format!("{err:#}").contains("REPLICA IDENTITY FULL"),
            "must surface the deferred message, got: {err:#}"
        );
    }

    // Ultrareview bug_004: a schema-qualified config (`table: public.orders`)
    // compared verbatim against the adapter's BARE event table matched zero
    // events — the whole stream silently dropped into a 0-row success.
    #[test]
    fn table_matches_handles_bare_and_qualified_configs() {
        assert!(
            table_matches(CdcEngine::Postgres, "orders", "public", "orders"),
            "bare matches any schema"
        );
        assert!(
            table_matches(CdcEngine::Postgres, "public.orders", "public", "orders"),
            "qualified matches"
        );
        assert!(
            !table_matches(CdcEngine::Postgres, "audit.orders", "public", "orders"),
            "wrong schema differs"
        );
        assert!(
            !table_matches(CdcEngine::Postgres, "orders", "public", "users"),
            "different table differs"
        );
    }

    /// Mongo has NO schema qualifier, so the `schema.table` split arm must not run
    /// there — and while it did, one config matched two collections.
    ///
    /// Round-3B bughunt. A collection whose first dot-segment equals the DATABASE
    /// name (db `shopdb`, collections `orders` and `shopdb.orders` — both legal)
    /// matched `table: shopdb.orders` twice: once by full name, once by the split.
    /// Two collections' events interleaved into one destination under a green run,
    /// invisible to any count.
    ///
    /// The split arm exists for SQL's `schema.table`; on a store with no schemas it
    /// is a second reading of a string that has only one.
    #[test]
    fn a_mongo_dotted_name_never_splits_into_a_schema_qualifier() {
        assert!(
            table_matches(CdcEngine::Mongo, "shopdb.orders", "shopdb", "shopdb.orders"),
            "the collection LITERALLY named `shopdb.orders` is the only reading"
        );
        assert!(
            !table_matches(CdcEngine::Mongo, "shopdb.orders", "shopdb", "orders"),
            "the sibling collection `orders` must NOT also match — that is the \
             interleave: two collections into one destination, counts intact"
        );
        // SQL keeps both arms: `schema.table` is a real qualifier there.
        assert!(table_matches(
            CdcEngine::Postgres,
            "public.orders",
            "public",
            "orders"
        ));
    }

    #[test]
    fn roast_dotted_collection_name_routes_by_full_name() {
        // A MongoDB collection literally named `my.data` (dots are legal, no
        // schema concept) must route by its FULL name — before this it was
        // mis-split into schema=`my`, table=`data` and routed ZERO events forever.
        assert!(table_matches(
            CdcEngine::Mongo,
            "my.data",
            "shopdb",
            "my.data"
        ));
        // Still distinguishes a genuinely different collection.
        assert!(!table_matches(
            CdcEngine::Mongo,
            "my.data",
            "shopdb",
            "my.other"
        ));
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
        let err = run_to_files(&mut stream, cfg)
            .1
            .expect_err("arity drift must fail");
        assert!(
            err.to_string().contains("WRONG columns"),
            "must explain the misalignment: {err}"
        );
    }

    // Round-2 audit #11: at the instant the slot is acked, a durable `Success`
    // run-unique manifest covering the just-acked parts MUST already exist on the
    // destination — otherwise a crash in the ack→terminal-manifest window orphans
    // those parts from the manifest-authoritative loader (silent row loss). This
    // stream asserts the invariant from INSIDE `ack`, the exact moment the slot
    // advances. RED before the pre-ack `write_manifest_without_success_marker`
    // in `roll_all` (the manifest was written only at clean end, after the ack).
    struct ManifestBeforeAckStream {
        events: VecDeque<ChangeEvent>,
        dest_dir: std::path::PathBuf,
        ack_count: usize,
    }
    impl ChangeStream for ManifestBeforeAckStream {
        fn engine(&self) -> super::super::CdcEngine {
            super::super::CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
            self.events.pop_front().map(Ok)
        }
        fn ack(&mut self, _position: &Position) -> Result<()> {
            self.ack_count += 1;
            // A run-unique manifest copy (`manifest-<run_id>.json`), distinct from
            // the canonical `manifest.json`, must be durable BEFORE this ack.
            let manifest_path = std::fs::read_dir(&self.dest_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .find(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("manifest-") && n.ends_with(".json"))
                })
                .expect("a run-unique manifest must be durable BEFORE the slot ack (#11)");
            let m: RunManifest =
                serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
            assert_eq!(
                m.status,
                ManifestStatus::Success,
                "the pre-ack manifest must be Success — the loader ignores non-Success runs"
            );
            assert!(
                m.row_count >= 1,
                "the pre-ack manifest must cover the acked part's rows, got {}",
                m.row_count
            );
            Ok(())
        }
    }

    #[test]
    fn roast_manifest_is_durable_before_the_slot_is_acked() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = ManifestBeforeAckStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            dest_dir: dir.path().to_path_buf(),
            ack_count: 0,
        };
        // rollover=2 over 3 committed events ⇒ ≥1 mid-stream roll+ack, each of
        // which the stream's `ack` gates on a durable Success manifest.
        {
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
            );
            r.unwrap();
            m
        };
        assert!(
            stream.ack_count >= 1,
            "the fixture must exercise at least one ack"
        );
        // The clean end still leaves the terminal manifest + _SUCCESS.
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "_SUCCESS at clean end"
        );
    }

    /// A drain that commits parts and THEN fails must report those parts.
    ///
    /// `run_to_files` returned `Result<Vec<RunManifest>>`, so any error threw the
    /// manifests away and `cdc_job` had nothing to record but hard-coded zeros.
    /// The CDC sink rolls many times per run (flush -> per-roll manifest ->
    /// checkpoint -> ack), so a run that captured and ACKED N rows before
    /// failing reported "failed, 0 rows, 0 files" over an object store holding
    /// the parts and a source already advanced past them. No data was lost —
    /// every RECORD of it was, which is worse than it sounds: an operator
    /// reading that re-runs expecting to recapture changes the log no longer has.
    ///
    /// The fixture must roll at least one part BEFORE the error, or it proves
    /// nothing — a stream that fails on its first event has no durable work to
    /// forget. `rollover: 2` with three good events guarantees one committed
    /// part, then the fourth read fails.
    #[test]
    fn a_failed_drain_still_reports_the_parts_it_made_durable() {
        struct FailAfter {
            events: VecDeque<ChangeEvent>,
            acked: Vec<Position>,
        }
        impl ChangeStream for FailAfter {
            fn engine(&self) -> super::super::CdcEngine {
                super::super::CdcEngine::Postgres
            }

            fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
                match self.events.pop_front() {
                    Some(e) => Some(Ok(e)),
                    // Exhausted -> the read itself fails, mid-drain.
                    None => Some(Err(anyhow::anyhow!("stream read failed mid-drain"))),
                }
            }
            fn ack(&mut self, position: &Position) -> Result<()> {
                self.acked.push(position.clone());
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FailAfter {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3)]),
            acked: Vec::new(),
        };
        let (manifests, outcome) = run_to_files(
            &mut stream,
            cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
        );

        assert!(outcome.is_err(), "the drain must report the failure");
        // Fixture integrity: without a committed+acked part this test would pass
        // against the pre-fix code for the wrong reason.
        assert!(
            !stream.acked.is_empty(),
            "fixture is inert — nothing was acked, so there is no durable work to forget"
        );
        let rows: i64 = manifests.iter().map(|m| m.row_count).sum();
        let parts: usize = manifests.iter().map(|m| m.part_count as usize).sum();
        assert!(
            rows > 0 && parts > 0,
            "a failed drain reported {rows} rows / {parts} parts — the durable work was \
             discarded with the error, which is exactly what made `cdc_job` record zeros"
        );
        // A failed run must NOT claim the prefix is complete.
        assert!(
            !dir.path().join("_SUCCESS").exists(),
            "_SUCCESS must not be written for a run that failed"
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
            poison: None,
        }
    }

    /// Every decision `image_cell` makes, one case each — the four mutants that
    /// survived a run over this file before it was hoisted out of `flush`.
    ///
    /// Two FIELDS throughout, never one: with a single column there is no
    /// neighbour to pick up and `n == col` → `!=` is indistinguishable from the
    /// truth. Same reason the row-hash injectivity guard needed two.
    #[test]
    fn image_cell_resolves_by_name_and_falls_back_only_at_matching_arity() {
        use std::sync::Arc;
        let names: Arc<[String]> = Arc::from(vec!["a".to_string(), "b".to_string()]);
        let ev = |vals: Vec<RivetValue>, names: Option<Arc<[String]>>| ChangeEvent {
            after: Some(vals),
            image_names: names,
            ..insert(0)
        };

        // BY NAME: `b` is at index 1 whatever the caller's positional `i` says.
        // `n == col` → `!=` returns index 0 here — a neighbour's value, silently.
        let e = ev(
            vec![RivetValue::Int(10), RivetValue::Int(20)],
            Some(names.clone()),
        );
        assert_eq!(
            image_cell(&e, 0, "b", 2, None),
            Some(&RivetValue::Int(20)),
            "a named image resolves by NAME; matching the first column that is NOT \
             the one asked for hands every cell its neighbour's value"
        );
        assert_eq!(image_cell(&e, 1, "a", 2, None), Some(&RivetValue::Int(10)));

        // The MEMO is a pointer-identity shortcut and must agree with the search —
        // and it comes from `image_name_memo`, not hand-built here, so this grades
        // the real producer. A closure re-implementing the rule would pass against
        // the very mutant it is written to catch.
        let batch = [e.clone()];
        let memo = image_name_memo(&batch, "b");
        assert_eq!(
            memo.expect("the batch carries names").1,
            Some(1),
            "the memo must point at `b`'s own index; matching the first name that is \
             NOT `b` maps every row of that column to a neighbour's value"
        );
        assert_eq!(
            image_name_memo(&batch, "absent")
                .expect("the batch carries names")
                .1,
            None,
            "a column the image does not name has no memo — a Some here would index \
             the wrong cell for the whole flush"
        );
        assert!(
            image_name_memo(&[insert(0)], "v").is_none(),
            "no event carries names ⇒ no memo; the positional engines index by \
             position and have nothing to memoise"
        );
        assert_eq!(image_cell(&e, 0, "b", 2, memo), Some(&RivetValue::Int(20)));
        // A memo for a DIFFERENT names-Arc must be ignored, not trusted.
        let other: Arc<[String]> = Arc::from(vec!["b".to_string(), "a".to_string()]);
        assert_eq!(
            image_cell(&e, 9, "b", 2, Some((&other, Some(0)))),
            Some(&RivetValue::Int(20)),
            "the memo is keyed by Arc identity; a stale one must fall back to the \
             search rather than index another table's layout"
        );

        // NAME ABSENT, ARITY MATCHES — a mid-window RENAME. The value is there,
        // under its old name, and position is trustworthy. `== ncols` → `false`
        // degrades it to NULL: the silent-loss shape this arm exists to refuse.
        assert_eq!(
            image_cell(&e, 1, "renamed", 2, None),
            Some(&RivetValue::Int(20)),
            "a renamed column keeps its value at the same position while the arity \
             agrees — returning None here is a column that silently becomes NULL"
        );

        // NAME ABSENT, ARITY DIFFERS — a mid-window ADD/DROP, or a partial image.
        // The column genuinely has no value here. `== ncols` → `true` restores the
        // pre-round-13 read: index past the end of a short image.
        //
        // The index must land INSIDE the short image, or the two branches agree by
        // accident and the guard is untested: with `i` past the end, `vals.get(i)`
        // is `None` and so is the correct answer. Measured — a first draft used
        // `i = 1` over a 1-value image and the `-> true` mutant SURVIVED it. Here
        // the image holds two values under a three-column table, so position 0 is
        // readable and wrong: exactly a MINIMAL row image handing back a
        // neighbour's cell under the name of a column it does not carry.
        let three: std::sync::Arc<[String]> =
            Arc::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let short = ev(
            vec![RivetValue::Int(10), RivetValue::Int(20)],
            Some(three.clone()),
        );
        assert_eq!(
            image_cell(&short, 0, "absent", 3, None),
            None,
            "a short image has no value for a column it does not name; reading it by \
             position returns the FIRST column's value under another column's name"
        );
        // …and past the end too, so the arm is exercised on both sides.
        assert_eq!(image_cell(&short, 2, "absent", 3, None), None);

        // A DELETE reads the BEFORE image, an INSERT/UPDATE the AFTER.
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Int(7), RivetValue::Int(8)]),
            after: None,
            image_names: Some(names.clone()),
            ..insert(0)
        };
        assert_eq!(image_cell(&del, 0, "b", 2, None), Some(&RivetValue::Int(8)));
        assert_eq!(image_cell(&del, 0, "a", 2, None), Some(&RivetValue::Int(7)));

        // NO NAMES: purely positional, the pre-names engines' shape.
        let anon = ev(vec![RivetValue::Int(10), RivetValue::Int(20)], None);
        assert_eq!(
            image_cell(&anon, 1, "b", 2, None),
            Some(&RivetValue::Int(20))
        );
        assert_eq!(image_cell(&anon, 5, "b", 2, None), None);
    }

    /// The drain loop's two decisions, every combination — four mutants that
    /// survived while they were `if` conditions inside live-only glue.
    #[test]
    fn a_pass_rolls_for_rows_or_for_an_unacked_commit_and_the_drain_ends_only_when_dry() {
        // ROLL. The uncaptured-span case is the load-bearing one: nothing buffered,
        // and the ack still has to happen or a consume-retention reader re-reads the
        // same window forever.
        assert!(
            pass_must_roll(true, 0),
            "an unacked commit boundary with NO buffered rows must still roll — that \
             is what advances the slot past an uncaptured or empty span"
        );
        assert!(pass_must_roll(false, 1), "buffered rows must reach a part");
        assert!(pass_must_roll(true, 3));
        assert!(
            !pass_must_roll(false, 0),
            "nothing consumed and nothing buffered is not a roll; rolling here would \
             checkpoint a position the run never reached"
        );

        // END. `yielded == 0` is the only honest bound signal in a re-drain loop:
        // each pass re-peeks fresh log after acking, so a non-empty pass says
        // nothing about whether more is coming.
        assert!(
            drain_is_complete(false, 0),
            "a pass that yielded nothing has drained to the bound"
        );
        assert!(
            !drain_is_complete(false, 1),
            "a pass that yielded events must be followed by another — stopping here \
             is one pass per run, which is the starvation the re-drain loop replaced"
        );
        assert!(
            drain_is_complete(true, 5),
            "the cap stops the run even mid-stream, or `max_events` is advisory"
        );
        assert!(drain_is_complete(true, 0));
    }

    /// A DELETE's decimal scale comes from the BEFORE image.
    ///
    /// `refine_decimal_scales` is pure and had no unit test at all, so `delete match
    /// arm ChangeOp::Delete` survived: with it gone a delete reads `after`, which is
    /// `None` on every delete, so a batch of deletes keeps scale 0 and every decimal
    /// is written TRUNCATED to whole units. Counts and row totals all agree.
    #[test]
    fn a_deletes_decimal_scale_is_read_from_the_before_image() {
        let mut cols = vec![TypeMapping {
            column_name: "amount".into(),
            source_native_type: "numeric".into(),
            rivet_type: crate::types::RivetType::Decimal {
                precision: 18,
                scale: 0,
            },
            arrow_type: Some(DataType::Decimal128(18, 0)),
            ..int_col().remove(0)
        }];
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Bytes(b"12.345".to_vec())]),
            after: None,
            ..insert(0)
        };
        refine_decimal_scales(&mut cols, std::slice::from_ref(&del));
        assert_eq!(
            cols[0].arrow_type,
            Some(DataType::Decimal128(18, 3)),
            "a delete carries its values in `before`; reading `after` finds None and \
             leaves scale 0, which writes 12.345 as 12"
        );

        // The insert/update side, so the arm that stays is not the only one graded.
        let mut cols2 = cols.clone();
        cols2[0].arrow_type = Some(DataType::Decimal128(18, 0));
        let ins = ChangeEvent {
            after: Some(vec![RivetValue::Bytes(b"9.87".to_vec())]),
            ..insert(0)
        };
        refine_decimal_scales(&mut cols2, std::slice::from_ref(&ins));
        assert_eq!(cols2[0].arrow_type, Some(DataType::Decimal128(18, 2)));

        // The WIDEST scale in the batch wins — one row would make `.max()` and
        // `.min()` and `.last()` all agree.
        let mut cols3 = cols.clone();
        cols3[0].arrow_type = Some(DataType::Decimal128(18, 0));
        let batch = [
            ChangeEvent {
                after: Some(vec![RivetValue::Bytes(b"1.5".to_vec())]),
                ..insert(0)
            },
            ChangeEvent {
                after: Some(vec![RivetValue::Bytes(b"2.0625".to_vec())]),
                ..insert(1)
            },
            ChangeEvent {
                after: Some(vec![RivetValue::Bytes(b"3.25".to_vec())]),
                ..insert(2)
            },
        ];
        refine_decimal_scales(&mut cols3, &batch);
        assert_eq!(
            cols3[0].arrow_type,
            Some(DataType::Decimal128(18, 4)),
            "a narrower scale than the batch's widest silently truncates the widest row"
        );
    }

    /// The schema may only DECLARE a type `build_column` will actually produce.
    ///
    /// `ensure_schema` is pure and this guard had no unit test, so `replace match
    /// guard value::is_buildable(dt) with true` survived. With it always true the
    /// field is declared from `arrow_type` verbatim, while the array builder still
    /// falls back to `Utf8` for a type it cannot build — schema and data disagree,
    /// which is the one thing the fallback exists to prevent.
    #[test]
    fn a_type_the_array_builder_cannot_build_is_declared_utf8_not_verbatim() {
        let unbuildable = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Decimal128(10, 2),
            true,
        )));
        assert!(
            !value::is_buildable(&unbuildable),
            "the fixture is inert: this type must be one the builder REFUSES, or \
             both arms agree and the guard is untested"
        );
        let mut cols = vec![TypeMapping {
            column_name: "amounts".into(),
            source_native_type: "numeric[]".into(),
            arrow_type: Some(unbuildable),
            ..int_col().remove(0)
        }];
        let mut schema = None;
        let sch = ensure_schema(
            &mut schema,
            &mut cols,
            &[insert(0)],
            &crate::config::RowHash::default(),
        );
        let f = sch
            .field_with_name("amounts")
            .expect("the column is declared");
        assert_eq!(
            f.data_type(),
            &DataType::Utf8,
            "a type the array builder falls back to Utf8 for must be DECLARED Utf8 — \
             declaring it verbatim makes the schema describe data that is never written"
        );

        // The buildable side, so the arm that stays is graded too.
        let mut cols2 = int_col();
        let mut schema2 = None;
        let sch2 = ensure_schema(
            &mut schema2,
            &mut cols2,
            &[insert(0)],
            &crate::config::RowHash::default(),
        );
        assert_eq!(
            sch2.field_with_name("v").expect("declared").data_type(),
            &DataType::Int64,
            "a buildable type keeps its own width; degrading everything to Utf8 would \
             satisfy the assertion above and lose every type in the stream"
        );
    }

    /// Both arity guards, every arm — four mutants that survived while these were
    /// `if` conditions inside `flush`.
    ///
    /// The NAMED one is round 13's partial-image guard, proven live (`SET GLOBAL
    /// binlog_row_image=MINIMAL`, one UPDATE, an all-NULL row) and ungraded offline:
    /// `!=` → `==` refuses every image whose arity AGREES, i.e. the whole stream, so
    /// a green suite said nothing about the case that matters.
    #[test]
    fn a_partial_named_image_and_a_post_ddl_positional_image_are_both_refused() {
        use std::sync::Arc;
        let names: Arc<[String]> =
            Arc::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let named = |vals: Vec<i64>| ChangeEvent {
            after: Some(vals.into_iter().map(RivetValue::Int).collect()),
            image_names: Some(names.clone()),
            ..insert(0)
        };

        assert_eq!(
            named_image_arity_mismatch(&named(vec![1, 2, 3])),
            None,
            "a complete named image is mapped by NAME and is arity-proof — refusing \
             it stops every stream rivet can actually read"
        );
        assert_eq!(
            named_image_arity_mismatch(&named(vec![1])),
            Some((1, 3)),
            "one value under three names is a MINIMAL row image; mapping it resolves \
             every name past the end of the vector and the row lands all NULL"
        );
        // WIDER than its names too — the inequality is not a one-sided `<`.
        assert_eq!(
            named_image_arity_mismatch(&named(vec![1, 2, 3, 4])),
            Some((4, 3))
        );
        // A key-only PG DELETE names only what it supplies, so its lengths agree.
        let key_only: Arc<[String]> = Arc::from(vec!["a".to_string()]);
        let del = ChangeEvent {
            op: ChangeOp::Delete,
            before: Some(vec![RivetValue::Int(1)]),
            after: None,
            image_names: Some(key_only),
            ..insert(0)
        };
        assert_eq!(
            named_image_arity_mismatch(&del),
            None,
            "PostgreSQL names only the key columns it also supplies — refusing this \
             would refuse every PG delete"
        );
        // A NAMELESS image is not this guard's business.
        assert_eq!(
            named_image_arity_mismatch(&ChangeEvent {
                after: Some(vec![RivetValue::Int(1)]),
                image_names: None,
                ..insert(0)
            }),
            None
        );

        // POSITIONAL. Three schema columns throughout.
        let anon = |op: ChangeOp, vals: Vec<i64>| {
            let v: Vec<RivetValue> = vals.into_iter().map(RivetValue::Int).collect();
            match op {
                ChangeOp::Delete => ChangeEvent {
                    op: ChangeOp::Delete,
                    before: Some(v),
                    after: None,
                    image_names: None,
                    ..insert(0)
                },
                _ => ChangeEvent {
                    after: Some(v),
                    image_names: None,
                    ..insert(0)
                },
            }
        };
        assert_eq!(
            positional_image_width_mismatch(&anon(ChangeOp::Insert, vec![1, 2, 3]), 3),
            None
        );
        assert_eq!(
            positional_image_width_mismatch(&anon(ChangeOp::Insert, vec![1, 2]), 3),
            Some(2),
            "a non-delete of any other width proves a stale pre-DDL layout; mapping \
             it by position puts values into the WRONG columns"
        );
        assert_eq!(
            positional_image_width_mismatch(&anon(ChangeOp::Insert, vec![1, 2, 3, 4]), 3),
            Some(4)
        );

        // The DELETE arm is `>` and not `!=` ON PURPOSE, and this is the pair that
        // says so: SHORTER is legitimate (a key-only delete), WIDER is drift.
        // `>` → `<` swaps exactly these two answers.
        assert_eq!(
            positional_image_width_mismatch(&anon(ChangeOp::Delete, vec![1]), 3),
            None,
            "a key-only delete carries fewer values than the table has columns and \
             maps by prefix — refusing it is an outage on every engine that emits one"
        );
        assert_eq!(
            positional_image_width_mismatch(&anon(ChangeOp::Delete, vec![1, 2, 3]), 3),
            None,
            "a FULL delete image carries every column — MySQL's `binlog_row_image = \
             FULL` emits exactly this, and it is the BOUNDARY: without it `>` and \
             `>=` agree on every fixture and the mutant survives (measured)"
        );
        assert_eq!(
            positional_image_width_mismatch(&anon(ChangeOp::Delete, vec![1, 2, 3, 4]), 3),
            Some(4),
            "an image WIDER than the schema is drift whatever the op"
        );
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
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
            );
            r.unwrap();
            m
        };
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
    /// Every durable CDC part reaches the DATABASE, not only the manifest.
    ///
    /// The database is meant to be the record of what a run produced and the
    /// manifest its projection at the destination. For CDC it was the other way
    /// round: `cdc_job` wrote `begin_run` at the start and one aggregate
    /// `export_metrics` row at finalize, and the sink — which knows about parts —
    /// never touched the state store at all. So the manifest was both FINER
    /// (per part) and EARLIER (written before each ack, deliberately, so a crash
    /// keeps the data visible) than the ledger, and a run that died mid-stream
    /// left durable rows the database had never heard of. The batch path has
    /// recorded each part through `commit.rs::record_file` all along; this is
    /// the same seam on the CDC side.
    ///
    /// The oracle is the LEDGER, read back after a real `run_to_files` — the
    /// value observed at the boundary, produced by the real writer, not a count
    /// the test hands itself. `rollover: 2` over five changes so there is more
    /// than one part: with a single part any per-part loop and any
    /// write-once-at-the-end both look identical.
    /// TWO tables in one CDC export: every table's parts must survive in the
    /// ledger, not just the last one to flush.
    ///
    /// The part name carries only its own sink's sequence, so table `a` and table
    /// `b` both produce `cdc-<run_token>-000000`. The objects are fine — each
    /// table writes under its own sub-prefix — but `file_log` is keyed
    /// `(run_id, file_name)` and `record_durable_part` deletes-then-inserts on
    /// that key, so each roll erased the sibling's row. The projected run
    /// aggregate then under-reported both parts and rows, and no single-table
    /// test could see it: at N=1 there is no sibling to erase.
    #[test]
    fn two_tables_in_one_cdc_export_do_not_erase_each_others_ledger_rows() {
        let dir = tempfile::tempdir().unwrap();
        let dest_a = local_dest(&dir);
        let dir_b = tempfile::tempdir().unwrap();
        let dest_b = local_dest(&dir_b);
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");

        let ev = |table: &str, id: i64| {
            let mut e = insert(id);
            e.table = table.into();
            e
        };
        let mut stream = FakeStream {
            events: VecDeque::from(vec![ev("a", 1), ev("b", 2), ev("a", 3), ev("b", 4)]),
            acked: Vec::new(),
        };

        let base = cfg(dest_a.as_ref(), &cols, FormatType::Parquet, 1);
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                SinkConfig {
                    state: Some(&state),
                    outputs: vec![
                        TableOutput {
                            table: "a".into(),
                            columns: cols.clone(),
                            dest: dest_a.as_ref(),
                            dest_uri: String::new(),
                            row_hash: crate::config::RowHash::All(false),
                        },
                        TableOutput {
                            table: "b".into(),
                            columns: cols.clone(),
                            dest: dest_b.as_ref(),
                            dest_uri: String::new(),
                            row_hash: crate::config::RowHash::All(false),
                        },
                    ],
                    ..base
                },
            );
            r.unwrap();
            m
        };

        let manifest_parts: usize = manifests.iter().map(|m| m.parts.len()).sum();
        assert!(
            manifests.len() == 2 && manifest_parts >= 2,
            "fixture is inert — both tables must have produced parts, or there is no sibling \
             row to erase; got {} manifest(s), {manifest_parts} part(s)",
            manifests.len()
        );

        let logged = state.list_files_for_run("r").expect("read file_log");
        assert_eq!(
            logged.len(),
            manifest_parts,
            "the two manifests list {manifest_parts} part(s) between them and the ledger kept \
             {} — a table's parts were erased by its sibling's, so every sum over file_log \
             under-reports the run",
            logged.len()
        );
        for table in ["a", "b"] {
            assert!(
                logged
                    .iter()
                    .any(|r| r.file_name.starts_with(&format!("{table}/"))),
                "table '{table}' has no row in the ledger at all"
            );
        }
    }

    #[test]
    fn every_durable_cdc_part_is_recorded_in_the_state_db_not_just_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3), insert(4), insert(5)]),
            acked: Vec::new(),
        };
        let manifest = {
            let (m, r) = run_to_files(
                &mut stream,
                SinkConfig {
                    state: Some(&state),
                    ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 2)
                },
            );
            r.unwrap();
            m
        };

        let logged = state.list_files_for_run("r").expect("read file_log");
        let parts = &manifest[0].parts;
        assert!(
            parts.len() > 1,
            "fixture must produce SEVERAL parts, or a per-part ledger write and a single \
             write-at-the-end are indistinguishable; got {}",
            parts.len()
        );
        assert_eq!(
            logged.len(),
            parts.len(),
            "the manifest lists {} part(s) and the ledger recorded {} — every part the run made \
             durable must be IN THE DATABASE, or a crashed run leaves rows nothing in the state \
             store knows about",
            parts.len(),
            logged.len()
        );
        for p in parts {
            let row = logged
                .iter()
                .find(|r| r.file_name == ledger_part_name("t", &p.path))
                .unwrap_or_else(|| {
                    panic!("part '{}' is in the manifest but not in file_log", p.path)
                });
            assert_eq!(
                row.row_count, p.rows,
                "ledger and manifest disagree about how many rows '{}' holds",
                p.path
            );
            assert_eq!(row.export_name, "t", "the ledger row must name the export");
        }
    }

    /// A run in flight has an aggregate in the DATABASE, and a finished run has
    /// exactly ONE.
    ///
    /// Two halves, and the second is the dangerous one. Writing progress is easy;
    /// writing it as a NEW row each roll would leave a five-roll run as five
    /// `export_metrics` rows, and every consumer that sums `total_rows` across
    /// runs would report five times the data — a far worse defect than the gap it
    /// closes. So the progress row is keyed on the run and the terminal write
    /// clears it.
    ///
    /// The oracle is the ledger itself, read back around a real `run_to_files`:
    /// during the run the row must exist and carry the totals so far; after the
    /// terminal write there must be exactly one row and it must not say
    /// `running`. `rollover: 2` over five changes so several rolls happen — with
    /// one roll a per-roll upsert and a single write are indistinguishable, and
    /// the duplicate-row failure cannot appear at all.
    #[test]
    fn a_cdc_run_has_exactly_one_metrics_row_and_it_exists_before_the_run_ends() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let mut stream = FakeStream {
            events: VecDeque::from(vec![insert(1), insert(2), insert(3), insert(4), insert(5)]),
            acked: Vec::new(),
        };
        let manifest = {
            let (m, r) = run_to_files(
                &mut stream,
                SinkConfig {
                    state: Some(&state),
                    ..cfg(dest.as_ref(), &cols, FormatType::Parquet, 2)
                },
            );
            r.unwrap();
            m
        };
        assert!(
            manifest[0].parts.len() > 1,
            "fixture must roll more than once, or neither half of this test can fail"
        );

        // The sink never reaches finalize (that is `cdc_job`'s job), so what the
        // ledger holds here is precisely the in-flight row the old code never
        // wrote at all.
        let rows = state
            .get_metrics(Some("t"), 100)
            .expect("read export_metrics");
        assert_eq!(
            rows.len(),
            1,
            "a run must leave ONE aggregate row, not one per roll — {} rows would make every \
             sum over export_metrics count this run {} times",
            rows.len(),
            rows.len()
        );
        assert_eq!(
            rows[0].status, "running",
            "an unfinished run reads as running"
        );
        assert_eq!(
            rows[0].total_rows, 5,
            "the in-flight aggregate must carry the rows made durable so far"
        );

        // The terminal write replaces it rather than adding to it.
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "t".into(),
                run_id: "r".into(),
                total_rows: 5,
                status: "success".into(),
                ..Default::default()
            })
            .unwrap();
        let after = state
            .get_metrics(Some("t"), 100)
            .expect("read export_metrics");
        assert_eq!(
            after.len(),
            1,
            "the terminal row must REPLACE the in-flight one; {} rows means a finished run is \
             double-counted forever",
            after.len()
        );
        assert_eq!(after[0].status, "success");
    }

    /// The manifest must describe the compression the part actually has.
    ///
    /// The writer picked `None` for CSV and `Zstd` for Parquet; `build_manifest`
    /// wrote the literal `"zstd"` for both. So a `format: csv` CDC export shipped
    /// PLAIN-TEXT parts under a manifest advertising zstd — a cross-boundary
    /// consumer that decodes by the field gets a zstd error on a readable file,
    /// and the batch leg, which records `plan.compression.label()`, describes the
    /// same artifact differently.
    ///
    /// The oracle is the FILE, not the other branch of the same match: the
    /// assertion reads the bytes the run actually wrote and requires the manifest
    /// to agree with them. Comparing the manifest to `part_compression` would
    /// share the producer's opinion and pass on any consistent lie. Both formats
    /// run, because with one the match arm is unobservable — the literal `"zstd"`
    /// was RIGHT for parquet, which is exactly how it stayed for months.
    #[test]
    fn a_cdc_manifest_declares_the_compression_the_part_actually_has() {
        for (format, ext) in [(FormatType::Csv, "csv"), (FormatType::Parquet, "parquet")] {
            let dir = tempfile::tempdir().unwrap();
            let dest = local_dest(&dir);
            let cols = int_col();
            let mut stream = FakeStream {
                events: VecDeque::from(vec![insert(1), insert(2)]),
                acked: Vec::new(),
            };
            let manifest = {
                let (m, r) = run_to_files(&mut stream, cfg(dest.as_ref(), &cols, format, 10));
                r.unwrap();
                m
            };
            let declared = &manifest[0].compression;

            let part = std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .find(|p| p.extension().is_some_and(|e| e == ext))
                .unwrap_or_else(|| panic!("{format:?}: no .{ext} part was written"));
            let bytes = std::fs::read(&part).unwrap();

            // What the bytes say they are, decided WITHOUT asking rivet: a zstd
            // frame opens with the magic 28 B5 2F FD; a plain CSV opens with its
            // header text; a Parquet file opens with "PAR1" and carries its codec
            // inside, which `compression:` is describing.
            let zstd_framed = bytes.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]);
            match format {
                FormatType::Csv => {
                    assert!(
                        !zstd_framed && bytes.starts_with(b"__op"),
                        "the CSV part is plain text, so the manifest cannot say it is compressed"
                    );
                    assert_eq!(
                        declared, "none",
                        "the CSV part on disk is uncompressed plain text and the manifest \
                         declares `{declared}` — the sidecar is describing a different file \
                         than the one beside it"
                    );
                }
                FormatType::Parquet => {
                    assert!(bytes.starts_with(b"PAR1"), "not a parquet file");
                    assert_eq!(
                        declared, "zstd",
                        "parquet parts are written with the zstd codec and the manifest must \
                         still say so — a fix for the CSV arm that flips this one has moved \
                         the defect rather than removed it"
                    );
                }
            }
        }
    }

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
        {
            let (m, r) = run_to_files(
                &mut run1,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100000000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

        // Cycle 2 (a later scheduler tick, distinct run id) captures change 3.
        let mut run2 = FakeStream {
            events: VecDeque::from(vec![insert(3)]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(
                &mut run2,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100500000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

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
        {
            let (m, r) = run_to_files(
                &mut run1,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100000000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

        let mut run2 = FakeStream {
            events: VecDeque::from(vec![insert(3)]),
            acked: Vec::new(),
        };
        {
            let (m, r) = run_to_files(
                &mut run2,
                SinkConfig {
                    export_name: "t".into(),
                    run_id: "t_cdc_20260702T100500000".into(),
                    ..cfg(dest.as_ref(), &cols, FormatType::Csv, 10)
                },
            );
            r.unwrap();
            m
        };

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
            poison: None,
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
            export_name: "t".into(),
            outputs: vec![TableOutput {
                table: "t".into(),
                columns: cols.to_vec(),
                dest,
                dest_uri: String::new(),
                row_hash: crate::config::RowHash::All(false),
            }],
            engine: crate::source::cdc::CdcEngine::Mysql,
            format,
            checkpoint: None,
            max_events: None,
            rollover,
            rollover_memory_bytes: None,
            started_at: "2026-06-23T00:00:00Z".into(),
            run_id: "r".into(),
            // The unit sinks write to a tempdir with no database behind them.
            state: None,
            read_bytes: Default::default(),
        }
    }

    /// `max_events` must be a SOFT cap that lands on a commit boundary — the same
    /// treatment `should_roll` gives every other budget. The hard break sat inside
    /// the per-event loop with no boundary condition, so a transaction larger than
    /// the cap (or one straddling it with no earlier commit in the pass) was cut
    /// mid-flight: the partial transaction flushed as a real part — violating
    /// `roll_all`'s own "never split a transaction across parts" doc — and the
    /// end-of-pass roll skipped checkpoint AND ack (no commit boundary seen), so
    /// the next run resumed at the same transaction, re-read it, wrote another
    /// run-uniquely-named part, and reported success. Every cycle. On PostgreSQL
    /// the self-rescue is disabled too (`release_empty_frontier` bails once data
    /// was yielded), so the slot pins WAL until the disk fills.
    ///
    /// Fixture note: FIVE events, ONE transaction (committed only on the last),
    /// cap = 3 — past the activation threshold on both counts (a cap the fixture
    /// never reaches, or a fixture of single-event transactions, cannot express
    /// the defect: with per-event commits the cap always lands on a boundary).
    /// The hole the first soft-cap placement left: the cap check sat in the
    /// ROUTED section, but a commit boundary can land on an UNCAPTURED table
    /// (the exact stream shape `commit_boundary_on_an_uncaptured_table_still_
    /// advances_the_checkpoint` exists for) — and the routing `continue` skipped
    /// the check entirely. A stream of transactions committing on uncaptured
    /// tables then NEVER fires the cap: unbounded overshoot, the opposite
    /// failure of the hard cut the soft cap replaced. Found by the focused hunt
    /// over the fix branch, before merge.
    #[test]
    fn max_events_fires_at_a_commit_boundary_on_an_uncaptured_table() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let ckpt = dir.path().join("ckpt");

        // Tx1: three captured uncommitted rows, boundary on an UNCAPTURED table.
        let mut events: VecDeque<ChangeEvent> = (1..=3)
            .map(|id| ChangeEvent {
                committed: false,
                ..insert(id)
            })
            .collect();
        events.push_back(ChangeEvent {
            table: "other".into(),
            ..insert(4)
        });
        // Tx2: two more captured rows the cap must NOT reach.
        events.push_back(ChangeEvent {
            committed: false,
            ..insert(5)
        });
        events.push_back(insert(6));
        let mut stream = FakeStream {
            events,
            acked: Vec::new(),
        };

        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        c.max_events = Some(3);
        c.checkpoint = Some(ckpt);
        let manifests = {
            let (mm, r) = run_to_files(&mut stream, c);
            r.expect("run");
            mm
        };

        let rows: i64 = manifests.iter().map(|m| m.row_count).sum();
        assert_eq!(
            rows, 3,
            "the cap must fire at the uncaptured-table boundary after 3 routed \
             events — capturing tx2 means the cap is dead on this stream shape"
        );
    }

    #[test]
    fn max_events_stops_at_a_commit_boundary_never_inside_a_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let ckpt = dir.path().join("ckpt");

        let mut events: VecDeque<ChangeEvent> = (1..=4)
            .map(|id| ChangeEvent {
                committed: false,
                ..insert(id)
            })
            .collect();
        events.push_back(insert(5)); // the commit boundary
        let mut stream = FakeStream {
            events,
            acked: Vec::new(),
        };

        let mut c = cfg(dest.as_ref(), &cols, FormatType::Parquet, 100);
        c.max_events = Some(3);
        c.checkpoint = Some(ckpt.clone());
        let manifests = {
            let (mm, r) = run_to_files(&mut stream, c);
            r.expect("run");
            mm
        };

        // The whole 5-row transaction is durable — the cap deferred to the
        // boundary instead of cutting after event 3…
        let rows: i64 = manifests.iter().map(|m| m.row_count).sum();
        assert_eq!(
            rows, 5,
            "the cap must include the whole transaction, not cut it at 3"
        );
        // …and the durable sequence RAN: the boundary was acked and the
        // checkpoint written, so the next run does NOT re-read this transaction.
        assert_eq!(
            stream.acked.len(),
            1,
            "the commit boundary must be acked exactly once"
        );
        assert!(
            ckpt.exists(),
            "the checkpoint must be written — without it every later run re-reads \
             and re-writes this same transaction forever"
        );
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
        let manifest = &{
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 2),
            );
            r.unwrap();
            m
        }[0];
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
        {
            let (m, r) = run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10));
            r.unwrap();
            m
        };
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
        let manifest = &{
            let (m, r) = run_to_files(&mut stream, cfg(dest.as_ref(), &cols, FormatType::Csv, 10));
            r.unwrap();
            m
        }[0];
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
        // A bounded run that drains no changes (the default drain, nothing new)
        // must still close cleanly: a 0-part manifest + `_SUCCESS`, not an error or a
        // missing marker that would look like a crash to the next run.
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(&dir);
        let cols = int_col();
        let mut stream = FakeStream {
            events: VecDeque::new(),
            acked: Vec::new(),
        };
        let manifest = &{
            let (m, r) = run_to_files(
                &mut stream,
                cfg(dest.as_ref(), &cols, FormatType::Parquet, 10),
            );
            r.unwrap();
            m
        }[0];
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
        let manifest = &{
            let (m, r) = run_to_files(&mut stream, c);
            r.unwrap();
            m
        }[0];
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
            export_name: "t".into(),
            outputs: vec![
                TableOutput {
                    table: "a".into(),
                    columns: cols.to_vec(),
                    dest: dest_a,
                    dest_uri: "a".into(),
                    row_hash: crate::config::RowHash::All(false),
                },
                TableOutput {
                    table: "b".into(),
                    columns: cols.to_vec(),
                    dest: dest_b,
                    dest_uri: "b".into(),
                    row_hash: crate::config::RowHash::All(false),
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
            // The unit sinks write to a tempdir with no database behind them.
            state: None,
            read_bytes: Default::default(),
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
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                two_outputs(
                    dest_a.as_ref(),
                    dest_b.as_ref(),
                    &cols,
                    FormatType::Csv,
                    100,
                ),
            );
            r.unwrap();
            m
        };

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
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                two_outputs(dest_a.as_ref(), dest_b.as_ref(), &cols, FormatType::Csv, 3),
            );
            r.unwrap();
            m
        };

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
        let manifests = {
            let (m, r) = run_to_files(
                &mut stream,
                two_outputs(
                    dest_a.as_ref(),
                    dest_b.as_ref(),
                    &cols,
                    FormatType::Csv,
                    100,
                ),
            );
            r.unwrap();
            m
        };

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
            res.1.is_err(),
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
