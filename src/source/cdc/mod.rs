//! Canonical change-data-capture types + driver, shared across engines.
//!
//! Each engine's reader (`source::<engine>::cdc`) is an **adapter** that yields
//! these canonical types; the driver [`run`] — and the future Parquet/CSV sink —
//! is written once against the [`ChangeStream`] seam, not per engine. Three
//! adapters (MySQL binlog, PG logical slot, SQL Server change-table poll) make
//! the seam real.
//!
//! `#![allow(dead_code)]`: the consumer is `cli::dispatch` (the `rivet cdc`
//! command), which lives only in the binary crate; the library crate also
//! compiles `source` for the integration tests but has no CDC consumer of its
//! own. Same pattern as `source::mysql::cdc`.

#![allow(dead_code)]

pub(crate) mod identity;
pub(crate) mod sink;
pub(crate) mod spill;
pub(crate) mod validate;
pub(crate) mod value;

use std::path::{Path, PathBuf};

use serde_json::Value as Json;

use crate::config::TlsConfig;
use crate::error::Result;
use value::RivetValue;

/// The buffered-transaction memory backstop, in ONE place for every adapter.
///
/// PostgreSQL, MySQL and SQL Server each buffer a transaction WHOLE — the
/// never-split invariant — and each grew its own copy of this check: a row cap, a
/// byte cap, and a refusal message per engine. Three copies of one rule, and none
/// of them graded, which is the shape this codebase keeps paying for.
///
/// Consolidating it is also what makes the next step tractable: turning "refuse an
/// oversized transaction" into "spill it to disk" then changes ONE decision instead
/// of three, and the three engines cannot drift on the threshold while it happens.
///
/// `Ok(())` while the transaction still fits.
pub(crate) fn check_tx_buffer_caps(engine: &str, rows: usize, bytes: usize) -> Result<()> {
    // WHAT the buffer holds differs by engine, and the message must say the true
    // one. PostgreSQL and MySQL buffer exactly one transaction; SQL Server's poll
    // reads a BATCH — several runs of rows sharing a `__$start_lsn` — so telling its
    // operator "a single transaction has more than N rows" sends them looking for a
    // huge transaction that may not exist. Unifying the three engines' backstops
    // into one home (511ead5) collapsed this distinction and made the SQL Server
    // message untrue; a claim in a product message is a testable claim.
    let subject = if engine == "mssql" {
        "one poll batch (one or more transactions)"
    } else {
        "a single transaction"
    };
    let row_cap = max_tx_rows();
    if rows > row_cap {
        anyhow::bail!(
            "{engine} cdc: {subject} has more than {row_cap} rows — it must be \
             buffered whole (a transaction is never split across parts, which is what \
             makes a crash resume transaction-atomic), so this would exhaust memory. \
             Split the source transaction, or raise RIVET_CDC_MAX_TX_ROWS only if a \
             transaction this large is genuinely expected."
        );
    }
    let byte_cap = max_tx_bytes();
    if bytes > byte_cap {
        anyhow::bail!(
            // No "(large cells)" diagnosis: the estimate is RESIDENT memory now
            // (struct + position + names + values), so ~2.8M narrow rows cross the
            // default 2 GiB with no large value anywhere — the old wording sent the
            // operator hunting multi-hundred-MB cells that need not exist. At
            // resident rates the byte cap also fires BEFORE the 5M-row cap on any
            // realistic row, so it is the guard that actually speaks.
            "{engine} cdc: {subject} needs more than {byte_cap} bytes of buffer \
             memory — it must be buffered whole (a transaction is never split \
             across parts), so this would exhaust memory. The estimate is resident \
             cost, so wide cells and sheer row count both land here. Split the \
             source transaction, or raise RIVET_CDC_MAX_TX_BYTES only if this much \
             buffering is genuinely acceptable."
        );
    }
    Ok(())
}

/// The cap a `RIVET_CDC_MAX_TX_*` override resolves to — the pure half of
/// [`max_tx_rows`] and [`max_tx_bytes`].
///
/// Extracted because the callers cache in a `OnceLock`: the body runs at most once
/// per process, so no unit test can exercise both the default and an override, and
/// TWELVE mutants survived in those two functions — `-> 0`, `-> 1`, `> -> <`,
/// `> -> >=`, `* -> +`, `* -> /`. `-> 0` is the worst of them: a zero cap makes
/// `tx.len() > cap` true on the FIRST row, so every transaction is refused as
/// oversized and CDC stops entirely.
///
/// A non-positive or unparseable override falls back to the default rather than
/// being taken literally — `RIVET_CDC_MAX_TX_ROWS=0` must not mean "refuse
/// everything", and `=abc` must not mean "cap at nothing".
pub(crate) fn tx_cap_from_env(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Default row cap — 5M rows, far above any real OLTP transaction.
pub(crate) const DEFAULT_MAX_TX_ROWS: usize = 5_000_000;
/// Default byte cap — 2 GiB, likewise.
pub(crate) const DEFAULT_MAX_TX_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Canonical DML kind. Engine framing — PostgreSQL `BEGIN`/`COMMIT` markers, the
/// SQL Server update before/after split — is normalised away by each adapter; a
/// row change is exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ChangeOp::Insert => "insert",
            ChangeOp::Update => "update",
            ChangeOp::Delete => "delete",
        }
    }
}

impl ChangeOp {
    /// Does this op carry its values in the BEFORE image?
    ///
    /// A DELETE does; everything else carries them after. One fact, previously
    /// restated as a `match` in at least three places — `image_cell`, and twice in
    /// Mongo's `to_change_event`, where BOTH restatements were ungraded (`delete
    /// match arm ChangeOp::Delete` survived each). With the arm gone a delete reads
    /// the post-image, which does not exist on a delete, and is then framed as an
    /// AFTER image: the change is delivered as an insert of NULLs, and the row it
    /// was meant to retract stays in the destination forever.
    pub(crate) fn values_live_in_before(self) -> bool {
        match self {
            ChangeOp::Delete => true,
            ChangeOp::Insert | ChangeOp::Update => false,
        }
    }
}

/// An opaque, engine-shaped resume position — MySQL `{file, pos}`, a PostgreSQL
/// LSN, a SQL Server LSN. Persisted verbatim as the checkpoint; each engine
/// interprets its own shape when resuming. Compared only for equality.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Position(pub(crate) Json);

impl Position {
    /// Load a persisted checkpoint, or `None` on first run (absent).
    pub(crate) fn load(path: &Path) -> Result<Option<Self>> {
        use anyhow::Context as _;
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(Some(Position(serde_json::from_str(&s).with_context(
                || {
                    format!(
                        "checkpoint '{}' is corrupt or truncated (not valid JSON) — refusing to \
                     silently treat it as absent and re-anchor CDC at 'current', which would \
                     permanently skip every change since the last checkpoint. Restore the file, \
                     or delete it to accept a new anchor from a fresh snapshot.",
                        path.display()
                    )
                },
            )?))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => {
                Err(anyhow::Error::new(e)
                    .context(format!("reading checkpoint '{}'", path.display())))
            }
        }
    }

    /// Persist atomically (temp file + rename) so a crash never leaves a torn
    /// checkpoint that would resume from a corrupt position. Creates the
    /// parent directory: `rivet init --mode cdc` scaffolds
    /// `checkpoint: ./cdc/<table>.ckpt`, and the first client-flow rehearsal
    /// (finding #43) died on the missing `./cdc/` with an ENOENT dressed in
    /// the grants hint — a quickstart-blocking wall for every fresh user.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        use anyhow::Context as _;
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating checkpoint directory '{}'", parent.display()))?;
        }
        // A WRITER-UNIQUE tmp, never the fixed `.tmp` sibling: two processes
        // saving one checkpoint (an overlapping scheduler cycle, or two exports
        // misconfigured onto one path) interleaved their writes into the SAME
        // tmp file — create-truncate and write are two syscalls — and the rename
        // then promoted a file holding one saver's JSON with the other's tail:
        // invalid at load, and the load error's own remedy (delete to re-anchor)
        // skips the window if followed. pid+nanos, so namespaces (two pid-1
        // containers on one volume) cannot collide either.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let tmp = path.with_extension(format!("tmp.{}.{nanos:x}", std::process::id()));
        std::fs::write(&tmp, serde_json::to_vec(&self.0)?)
            .with_context(|| format!("writing checkpoint '{}'", path.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("committing checkpoint '{}'", path.display()))?;
        Ok(())
    }
}

/// One canonical row-level change.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub(crate) op: ChangeOp,
    pub(crate) schema: String,
    pub(crate) table: String,
    /// Pre-image — present for `Update`/`Delete` when the engine carries it.
    pub(crate) before: Option<Vec<RivetValue>>,
    /// Post-image — present for `Insert`/`Update` when the engine carries it.
    pub(crate) after: Option<Vec<RivetValue>>,
    /// Resume position after this change.
    pub(crate) position: Position,
    /// `true` if this is the last change in its source transaction — the only
    /// point it is safe to advance the checkpoint (transaction-atomic resume) and
    /// to roll an output file (never split a transaction across files). MySQL sets
    /// it at the XID/commit marker; the poll-based PG / SQL Server adapters only
    /// ever read already-committed data, so every change is a commit boundary.
    pub(crate) committed: bool,
    /// Column NAMES of this event's image, when the engine carries them
    /// (PostgreSQL wire text names every column; SQL Server change-table rows
    /// are name-addressable). With names present the sink maps the image BY
    /// NAME into the resolved schema — the positional-mapping corruption
    /// class (findings #37/#41/#42: mid-window DDL shifts, non-first PK
    /// deletes) is unrepresentable. `None` ⇒ positional full row (MySQL
    /// binlog carries no names; its arity guard stays load-bearing).
    pub(crate) image_names: Option<std::sync::Arc<[String]>>,
    /// Ordinal of this change **within its source transaction** (0-based),
    /// stamped by [`TxnSeq`] as the stream is consumed. `position` alone is the
    /// commit position — every change in one transaction shares it — so ordering
    /// a current-state dedup by `position` picks an arbitrary row when a PK is
    /// touched more than once per transaction. `(position, seq)` is the total
    /// order; being log-derived it is identical on an at-least-once re-emit.
    pub(crate) seq: u64,
    /// A DEFERRED per-event decode error, surfaced by the sink **only when this
    /// event routes to a captured table**. A single logical stream (one PG
    /// `test_decoding` slot) decodes EVERY table in the database, so an
    /// unrecoverable decode on an UN-captured table (e.g. an unchanged-TOAST datum
    /// with no pre-image) must not bail the whole run — it would poison capture of
    /// unrelated tables sharing the slot. The source records the error here instead
    /// of bailing; the sink raises it iff the event matches a captured table (the
    /// single routing authority), and drops it silently otherwise. `None` = clean.
    pub(crate) poison: Option<String>,
}

/// Stamps each change with its intra-transaction ordinal ([`ChangeEvent::seq`]).
/// The ordinal resets whenever the change's `position` (the commit position)
/// changes — every change in one transaction shares that position, so this is
/// the reliable transaction boundary on ALL engines. (`committed` cannot serve:
/// the poll-based PostgreSQL / SQL Server adapters read already-committed data
/// and mark EVERY change `committed`, which would reset the ordinal every row.)
/// Being derived from `position` + log order, the ordinal is reproduced exactly
/// on an at-least-once replay.
#[derive(Default)]
pub(crate) struct TxnSeq {
    counter: u64,
    prev: Option<Position>,
}

impl TxnSeq {
    /// Ordinal for a change at commit `position`: 0 when `position` differs from
    /// the previous change (a new transaction), else one more than the last.
    pub(crate) fn next(&mut self, position: &Position) -> u64 {
        if self.prev.as_ref() == Some(position) {
            self.counter += 1;
        } else {
            self.counter = 0;
            self.prev = Some(position.clone());
        }
        self.counter
    }

    pub(crate) fn stamp(&mut self, ev: &mut ChangeEvent) {
        ev.seq = self.next(&ev.position);
    }
}

/// Closes a buffered source transaction (#158): the at-least-once contract's
/// load-bearing rule, previously four inline copies inside live-server `fill`
/// loops (mysql/pg/mssql), each re-deriving the same loss argument in prose —
/// the exact shape that shipped the PG/MSSQL `committed:true`-on-every-event
/// bugs (see the process rules's committed-flag section).
///
/// The rule: stamp the group's commit `position` on EVERY event, and mark ONLY
/// its LAST event `committed`. The sink rolls (flush → checkpoint → ack) on a
/// `committed` event, so marking every event committed would roll + checkpoint
/// MID-transaction and a crash before the tail's flush would advance the resume
/// position past the commit — resume reads strictly after it and skips the tail
/// (an at-least-once break). Each engine's group DETECTION stays engine-side
/// (XID / BEGIN…COMMIT / __$start_lsn run); only the CLOSE is shared here.
pub(crate) struct TxnFramer;

impl TxnFramer {
    /// Frame one committed source transaction: `position = commit` on all,
    /// `committed = true` on the last event only. Empty group is a no-op.
    pub(crate) fn close_group(events: &mut [ChangeEvent], commit: &Position) {
        Self::close_head_of_group(events, commit, 0);
    }

    /// Close the IN-MEMORY head of a transaction whose tail is still on disk.
    ///
    /// A transaction past the memory cap keeps its head in `events` and spills the
    /// rest as raw wire rows, so the LAST event of the transaction is in the tail
    /// whenever the tail is non-empty. `committed` must follow the transaction, not
    /// the segment: marking the head's last event committed would let the sink roll,
    /// checkpoint and ack MID-transaction, and a crash before the tail's flush would
    /// advance the slot past the commit — the resume reads strictly after it and the
    /// tail is gone. That is the `committed`-on-every-event break arriving through a
    /// second door.
    ///
    /// [`Self::close_group`] is this with an empty tail, so the unspilled path and
    /// the spilled one cannot drift into two rules.
    pub(crate) fn close_head_of_group(
        events: &mut [ChangeEvent],
        commit: &Position,
        tail_len: usize,
    ) {
        let n = events.len();
        for (i, ev) in events.iter_mut().enumerate() {
            ev.position = commit.clone();
            ev.committed = tail_len == 0 && i + 1 == n;
        }
    }

    /// Close ONE event of a spilled tail: `at` is its index within the tail,
    /// `tail_len` the tail's length. Only the tail's last event closes the
    /// transaction.
    pub(crate) fn close_tail_event(
        event: &mut ChangeEvent,
        commit: &Position,
        at: usize,
        tail_len: usize,
    ) {
        event.position = commit.clone();
        event.committed = at + 1 == tail_len;
    }

    /// Mark a stand-alone event as its own commit boundary (#158 — a NAMED decision, not a
    /// divergence). Correct where each event's write is a distinct commit (Mongo single-document
    /// writes). A Mongo MULTI-document transaction shares one commit across N events; each is still
    /// marked committed here (so a large one can roll the sink mid-transaction) — safe ONLY because
    /// Mongo's per-event, consume-free resume token re-reads a mid-transaction tail rather than
    /// skipping it (see `mongo/cdc.rs`). Engines whose resume position skips (PG slot / MSSQL
    /// from-LSN) must frame the TRUE boundary instead. `position` is already per-event, so this
    /// only sets the flag.
    pub(crate) fn single_event_commit(event: &mut ChangeEvent) {
        event.committed = true;
    }
}

/// Resident cost of a `serde_json::Value`, for the CDC memory budgets.
///
/// The load-bearing arm is `Object`. `serde_json`'s map is a `BTreeMap`, and a
/// B-tree node is allocated at FULL capacity — so a one-key object like a commit
/// position costs a whole node, measured at 475 B, not the ~20 bytes its text
/// suggests. That single fact is 62% of what a buffered CDC change costs, because
/// the framer clones the position onto every event of a transaction.
///
/// Scalars are charged 0: they live INLINE in the parent's slot, which is already
/// counted by the parent's capacity.
fn json_resident_bytes(v: &serde_json::Value) -> usize {
    use serde_json::Value;
    /// MEASURED, not derived. `serde_json`'s map is a `BTreeMap` whose node is
    /// allocated at full capacity, so a one-key object costs a whole node — but the
    /// obvious derivation (`11 * (size_of::<String>() + size_of::<Value>())` = 616)
    /// over-counts: the real figure is 475 B for `{"lsn":"…"}`, of which ~448 is the
    /// node itself. Modelling it structurally and trusting the model was wrong in
    /// BOTH directions here, an hour apart — first 12.7x under, then 1.8x over — so
    /// the constant is the measurement and
    /// the budget assertion inside
    /// `what_does_one_buffered_change_actually_cost` is what keeps it one.
    const JSON_OBJECT_NODE_BYTES: usize = 448;
    match v {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(s) => s.len(),
        Value::Array(a) => {
            a.capacity() * std::mem::size_of::<Value>()
                + a.iter().map(json_resident_bytes).sum::<usize>()
        }
        // An EMPTY map allocates NOTHING — `BTreeMap::new()` has no node until the
        // first insert — so the node is charged only when there is one.
        Value::Object(m) if m.is_empty() => 0,
        Value::Object(m) => {
            JSON_OBJECT_NODE_BYTES
                + m.iter()
                    .map(|(k, val)| k.len() + json_resident_bytes(val))
                    .sum::<usize>()
        }
    }
}

impl ChangeEvent {
    /// Rough in-memory footprint of this buffered change — drives the sink's
    /// memory-budget rollover (`rollover_memory_mb`). The before/after value
    /// images dominate; schema/table names + a small fixed overhead are added.
    /// DECODED payload size — what "bytes read from the source" means.
    ///
    /// A separate method from [`Self::estimated_bytes`] because the two units
    /// serve different masters and conflating them was a measured regression:
    /// when `estimated_bytes` was re-based to RESIDENT cost (struct + cloned
    /// position + Vec slots), the `bytes_read` metric silently inflated ~4-13x
    /// and stopped being comparable with the batch path's Arrow-columnar figure
    /// — the exact comparability #196 introduced it for. The BUDGETS want
    /// resident cost; the METRIC wants this.
    pub(crate) fn payload_bytes(&self) -> usize {
        let img = |v: &Option<Vec<RivetValue>>| {
            v.as_ref()
                .map_or(0, |vs| vs.iter().map(RivetValue::payload_bytes).sum())
        };
        self.schema.len() + self.table.len() + img(&self.before) + img(&self.after)
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        // RESIDENT cost, not payload size. The old model charged
        // `schema + table + values + 32` and under-counted a narrow event 12.7x
        // (measured: 61 B charged, 772 B real — see
        // `spill::event_cost::what_does_one_buffered_change_actually_cost`). Every
        // budget built on it therefore meant something ~12x larger than it said:
        // `RIVET_CDC_MAX_TX_BYTES: 2 GiB` was ~25 GiB of real memory, which is not
        // a guard, and `rollover_memory_mb` rolled far later than an operator
        // asking for N megabytes would expect.
        //
        // Three things the payload view misses, in the order they cost:
        //
        // 1. the COMMIT POSITION, which the framer clones onto EVERY event of a
        //    transaction — 475 B of the 772, because `serde_json`'s object is a
        //    `BTreeMap` whose node allocates at FULL capacity even for one key;
        // 2. the struct itself, which sits in the queue's backing array whatever it
        //    points at;
        // 3. the per-allocation overhead of the images' `Vec`s.
        //
        // `image_names` is charged AMORTISED, because whether it is shared depends
        // on the ENGINE and the first version of this comment got that wrong: it
        // asserted the `Arc` is "shared by every event of a relation", which holds
        // for MySQL (cached per TABLE_MAP) and Mongo (a static) and is FALSE for
        // PostgreSQL (`postgres/cdc.rs`: a fresh `Arc` and a fresh `String` per
        // column, per row) and SQL Server (`mssql/cdc.rs`: a fresh `Vec<String>`
        // inside the per-row loop). On those two the names are real, retained,
        // per-event memory — measured at ~434 B/event for 10 columns — and skipping
        // them put the estimate back under the tolerance it was just fixed to meet.
        //
        // `strong_count` answers it exactly and cheaply: one Arc held by N buffered
        // events reads N and each pays 1/N; a fresh Arc per event reads 1 and pays
        // in full. No engine-specific branch, and it stays right if a producer
        // starts or stops sharing.
        let img = |v: &Option<Vec<RivetValue>>| {
            v.as_ref().map_or(0, |vs| {
                vs.capacity() * std::mem::size_of::<RivetValue>()
                    + vs.iter().map(RivetValue::estimated_bytes).sum::<usize>()
            })
        };
        let names = self.image_names.as_ref().map_or(0, |a| {
            let own: usize =
                a.len() * std::mem::size_of::<String>() + a.iter().map(String::len).sum::<usize>();
            own / std::sync::Arc::strong_count(a).max(1)
        });
        std::mem::size_of::<Self>()
            + self.schema.len()
            + self.table.len()
            + self.poison.as_ref().map_or(0, String::len)
            + names
            + img(&self.before)
            + img(&self.after)
            + json_resident_bytes(&self.position.0)
    }

    /// Surface this event's DEFERRED decode error ([`poison`](Self::poison)) if it
    /// carries one. EVERY consumer that turns a captured event into output MUST
    /// call this right after confirming the event is captured — the deferral only
    /// holds because an uncaptured table's poison is dropped, so a driver that
    /// forgets to raise it on a CAPTURED event would emit corrupt data (the class
    /// the round-1 hunt caught on the NDJSON driver). A discoverable method the
    /// next sink calls, not a rule two drivers must each remember to inline.
    pub(crate) fn raise_poison(&self) -> Result<()> {
        if let Some(poison) = &self.poison {
            anyhow::bail!("{poison}");
        }
        Ok(())
    }
}

/// The seam every engine reader satisfies: a blocking pull of canonical changes.
///
/// `None` ⇒ no more changes available now. MySQL blocks until one arrives, so it
/// only ends when the connection closes; the poll-based PostgreSQL / SQL Server
/// adapters return `None` once their current backlog drains (a continuous daemon
/// wraps the driver in an outer poll loop).
pub(crate) trait ChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>>;

    /// Acknowledge that every change up to and including `position` is **durably
    /// persisted** at the destination. Engines that consume-on-read (PostgreSQL:
    /// reading a logical slot advances it) defer the actual consume to here — so a
    /// crash between reading and a durable write re-reads the un-acked changes
    /// (at-least-once). MySQL (binlog) and SQL Server (change tables) retain on the
    /// server independently of reads, so this is a no-op for them — the resume
    /// checkpoint alone makes them at-least-once.
    fn ack(&mut self, _position: &Position) -> Result<()> {
        Ok(())
    }

    /// The relation this stream's events will actually name, when the engine knows
    /// it from a CATALOG rather than from the configured string.
    ///
    /// The schema probe (`SELECT * FROM <name>`) resolves the CONFIGURED string in
    /// the connection's default schema, which is a second, independent reading of
    /// one config. On SQL Server the two disagreed in silence: `open` resolves the
    /// capture instance through `cdc.change_tables` and tags events
    /// `<schema>.<table>`, while the probe read a same-named table in `dbo` — so an
    /// export was written with one relation's column names over another's events,
    /// `status: success`, no warning, the captured table's only data column absent
    /// from the output. Engines that cannot know better return `None` and keep the
    /// configured string.
    fn resolved_identity(&self, _configured: &str) -> Option<(String, String)> {
        None
    }

    /// Which engine this stream speaks — required, never defaulted.
    ///
    /// Routing semantics differ by engine (a document store has no schema to
    /// qualify with, so `a.b` has ONE reading there and two on SQL), and a
    /// default would hand a new adapter the wrong one in silence. Making it a
    /// required method means the compiler asks.
    fn engine(&self) -> CdcEngine;
}

/// `rivet cdc` driver. Streams canonical changes from any engine adapter,
/// emitting one NDJSON object per change to stdout and persisting the resume
/// position after each (when `checkpoint` is set). Stops at end of stream,
/// `max_events`, or interruption.
///
/// (The typed Parquet/CSV sink is the separate [`sink::run_to_files`] driver —
/// ADR-0023 keeps the two loops apart on purpose.)
pub(crate) fn run(
    stream: &mut dyn ChangeStream,
    checkpoint: Option<PathBuf>,
    tables: Vec<String>,
    max_events: Option<usize>,
) -> Result<()> {
    let mut emitted = 0usize;
    // A cap of 0 means emit nothing — check BEFORE consuming the stream. The
    // post-emit `emitted >= m` check let exactly one event escape at m=0 (it
    // printed, incremented to 1, then 1 >= 0 broke), an off-by-one.
    if max_events == Some(0) {
        return Ok(());
    }
    let eng = stream.engine();
    let mut txn_seq = TxnSeq::default();
    while let Some(ev) = stream.next_change() {
        let mut ev = ev?;
        txn_seq.stamp(&mut ev);
        let committed = ev.committed;
        // A commit boundary on an UNLISTED table produces no output line — advance
        // the checkpoint past it NOW (before the filter `continue`): the resume
        // position is a stream property, so a transaction whose last event lands on
        // an unlisted table must still move it (mirrors the file sink). There is no
        // data to lose here because nothing is emitted.
        let filtered = !tables.is_empty()
            && !tables
                .iter()
                .any(|t| sink::table_matches(eng, t, &ev.schema, &ev.table));
        if filtered {
            if committed && let Some(p) = &checkpoint {
                ev.position.save(p)?;
            }
            continue;
        }
        // Surface a deferred decode error (e.g. PG unchanged-TOAST with no
        // pre-image) only now that the event is confirmed captured — mirrors the
        // file sink. Without this the NDJSON path would print the raw
        // `unchanged-toast-datum` sentinel verbatim as the column value (silent
        // corruption). An uncaptured table's poison was already dropped above.
        ev.raise_poison()?;
        let to_json = |img: &Option<Vec<RivetValue>>| {
            img.as_ref()
                .map(|vs| vs.iter().map(RivetValue::to_json).collect::<Vec<_>>())
        };
        let line = serde_json::json!({
            "op": ev.op.as_str(),
            "schema": ev.schema,
            "table": ev.table,
            "before": to_json(&ev.before),
            "after": to_json(&ev.after),
            "pos": ev.position.0,
            "seq": ev.seq,
        });
        println!("{line}");
        emitted += 1;
        // Checkpoint AFTER emitting the captured event — never before. A crash in
        // the window between the checkpoint save and the emit would advance the
        // resume position past a line that was never printed, so the next run reads
        // strictly after it and SKIPS the transaction tail (#9 bughunt: an
        // at-least-once break, the save ran before the println). Emit→checkpoint
        // means a crash there re-emits on resume (a duplicate, never a loss).
        if committed && let Some(p) = &checkpoint {
            ev.position.save(p)?;
        }
        // A SOFT cap, landing on the commit boundary — the same semantics the file
        // sink already had (`max_events_stops_at_a_commit_boundary_never_inside_a_
        // transaction`). Breaking on the count alone lost nothing (the save above is
        // gated on `committed`, so a cut transaction re-emits on resume), but a
        // transaction LONGER than the cap then held no boundary to save: every run
        // re-read the same position, re-printed the same prefix, and stopped in the
        // same place. `--checkpoint ck --max-events 100` against a bulk load made no
        // progress, ever. Overshooting to the boundary is the price of progressing.
        if committed && max_events.is_some_and(|m| emitted >= m) {
            break;
        }
    }
    Ok(())
}

/// The per-engine CDC knobs — each variant carries ONLY the parameters its
/// engine reads, so the live set is obvious at a glance (a MySQL run has no
/// `slot`, a Mongo run has no `server_id`) and a new engine adds a self-contained
/// variant instead of a field every other engine ignores. The engine identity is
/// the variant itself — [`create_change_stream`] matches it directly, no
/// re-resolution from the URL.
#[derive(Debug, Clone)]
pub(crate) enum CdcEngineOpts {
    /// MySQL replica id (the binlog `server_id`).
    Mysql {
        server_id: u32,
        /// The `table:` values the CONFIG asked for.
        ///
        /// The binlog is ONE stream per server, so it carries every table's
        /// changes. An UNDECODABLE rows event is checked against this set before
        /// it is allowed to fail the run — otherwise one PARTIAL_JSON table is a
        /// server-wide outage for exports that never read it.
        configured_tables: Vec<String>,
    },
    /// PostgreSQL logical slot name.
    Postgres {
        slot: String,
        /// The `table:` values the CONFIG asked for — the same cross-check the
        /// SQL Server arm carries, for a different reason.
        ///
        /// `test_decoding` names the relation a row PHYSICALLY landed in. A
        /// PARTITIONED parent stores none, so every change carries a PARTITION's
        /// name and byte-exact routing drops all of it — while the slot advances,
        /// which on PostgreSQL makes the loss terminal rather than a delay.
        configured_tables: Vec<String>,
    },
    /// SQL Server capture instance (required for `sqlserver://`).
    Mssql {
        capture_instance: Option<String>,
        /// The `table:` values the CONFIG asked for.
        ///
        /// SQL Server resolves an event's identity from `cdc.change_tables`, but
        /// the sink routes by byte-exact comparison against these strings. The
        /// two were never compared, and SQL Server's default collation is
        /// case-INSENSITIVE — so `table: dbo.orders` against a catalog
        /// `dbo.Orders` resolved its schema perfectly and then matched no event.
        configured_tables: Vec<String>,
    },
    /// Render the `document` blob as canonical (type-tagged) extended JSON — the
    /// `source.mongo.json: canonical` mode, so a CDC stream and a full export
    /// produce identical text. Config-driven only; the CLI defaults to relaxed.
    Mongo {
        canonical: bool,
        /// The export's configured `table:` names. Every other engine's variant
        /// carried these and Mongo's did not, which is why it was the one engine
        /// with no routing cross-check of any kind: a typo'd collection ran to
        /// `status: success, rows: 0` in silence.
        configured_tables: Vec<String>,
    },
}

/// How a capture run ends — ONE name for the concept that used to cross the
/// adapter seam as three differently-aliased bools (`bound_at_open`,
/// `non_block`, `until_current`), and the canonical home of the bounded run's
/// termination contract.
///
/// `BoundedAtOpen` (`until_current: true` — the scheduler model): capture up to
/// the source's position AS OF STREAM OPEN, then exit. Every adapter pins the
/// ceiling at open (PostgreSQL `pg_current_wal_lsn()`; MySQL the binlog
/// coordinates; SQL Server `fn_cdc_get_max_lsn()`; MongoDB the cluster
/// `operationTime`), so a hot table whose writers outpace the drain cannot keep
/// the run alive — the run's work is O(backlog at open). Termination is
/// LOAD-BEARING on this bound for the two engines with a re-reading / tailable
/// reader: PostgreSQL (the non-consuming slot peek re-reads from the un-acked
/// position, otherwise chasing a moving log end) and MongoDB (a tailable change
/// stream whose `next_if_any` keeps returning events under sustained writes, so
/// the empty-poll target check never fires — the cluster-time bound is what
/// stops it, verified by disabling the pin: the sustained-writes test then
/// hangs). MySQL (`BINLOG_DUMP_NON_BLOCK` EOF) and SQL Server (the capture
/// Agent's scan-gap empty poll) terminate NATIVELY, so their bound is a
/// precise-stop refinement (verified fix-invariant for termination by a
/// disable-bound probe). The excluded tail is deferred, never lost: the
/// checkpoint stops at the last in-bound commit and the next run resumes there —
/// the defer-not-drop contract every engine's two-run test proves
/// (`roast_*_until_current_open_bound_two_runs_lose_nothing`; the PG variant, at
/// rollover 5, is the one whose TERMINATION genuinely goes RED without the
/// bound).
///
/// `Continuous` (the daemon model): no open-time ceiling. MySQL blocks on the
/// binlog; the poll adapters still exit on catch-up and an outer loop re-wraps
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainMode {
    BoundedAtOpen,
    Continuous,
}

impl DrainMode {
    /// The user-facing surface stays a bool (`cdc.until_current`, default true;
    /// the `rivet cdc` CLI opts OUT with `--stream`); internally the mode
    /// travels under one name.
    pub(crate) fn from_until_current(until_current: bool) -> Self {
        if until_current {
            DrainMode::BoundedAtOpen
        } else {
            DrainMode::Continuous
        }
    }

    pub(crate) fn is_bounded(self) -> bool {
        matches!(self, DrainMode::BoundedAtOpen)
    }
}

/// Memory backstop shared by every log/poll adapter: a transaction is buffered
/// WHOLE (never split across parts), so an oversized one would grow the tx buffer
/// unbounded. Each adapter caps its per-transaction buffer at this and bails
/// loudly rather than OOM. Default 5M rows (a real OLTP transaction is far
/// below). `RIVET_CDC_MAX_TX_ROWS` overrides it — an OPERATOR override (the
/// refusal messages prescribe raising it when a transaction this large is
/// genuinely expected; an earlier doc said "test-only" while the product's own
/// errors said otherwise), and the way tests make the cap
/// reachable without seeding a 5-million-row transaction. Read once.
pub(crate) fn max_tx_rows() -> usize {
    use std::sync::OnceLock;
    static CELL: OnceLock<usize> = OnceLock::new();
    *CELL.get_or_init(|| {
        tx_cap_from_env(
            std::env::var("RIVET_CDC_MAX_TX_ROWS").ok().as_deref(),
            DEFAULT_MAX_TX_ROWS,
        )
    })
}

/// Byte sibling of [`max_tx_rows`] (round-2 audit #9): the row cap is a poor bound
/// on the buffered-transaction FOOTPRINT when cells are large — a few thousand
/// rows of multi-hundred-MB TEXT/BLOB stay far under 5M rows yet exhaust memory.
/// Each adapter also caps the transaction's running `estimated_bytes` at this and
/// bails loudly. Default 2 GiB (a real OLTP transaction is far below).
/// `RIVET_CDC_MAX_TX_BYTES` overrides it — an operator override, like its row
/// sibling, and the way tests make the cap reachable
/// without seeding a multi-GB transaction. Read once.
pub(crate) fn max_tx_bytes() -> usize {
    use std::sync::OnceLock;
    static CELL: OnceLock<usize> = OnceLock::new();
    *CELL.get_or_init(|| {
        tx_cap_from_env(
            std::env::var("RIVET_CDC_MAX_TX_BYTES").ok().as_deref(),
            DEFAULT_MAX_TX_BYTES,
        )
    })
}

/// Connection + resume parameters for `rivet cdc`, across engines — the CDC
/// sibling of [`crate::source::create_source`]'s `SourceConfig`. The fields here
/// are engine-agnostic; per-engine knobs live in [`CdcEngineOpts`].
pub(crate) struct CdcConfig {
    pub url: String,
    /// MySQL checkpoint file (PG resumes via the slot; SQL Server via its LSN;
    /// MongoDB via the resume token).
    pub checkpoint: Option<PathBuf>,
    /// The CONFIG's directory — the anchor for every relative path this run
    /// resolves (checkpoint already arrives resolved; the spill dir resolves
    /// against this). Never the process cwd: the shipped image runs at `/`.
    pub config_dir: PathBuf,
    /// How this capture run ends — see [`DrainMode`], the canonical home of the
    /// termination contract.
    pub drain: DrainMode,
    /// Transport security, applied by every adapter through the same
    /// `require_tls_or_loopback` gate the batch path uses (refuse remote
    /// plaintext / unauthenticated TLS). `None` ⇒ loopback-only (the CLI default).
    pub tls: Option<crate::config::TlsConfig>,
    /// The engine + its knobs — the CDC engine identity for dispatch.
    pub engine: CdcEngineOpts,
}

/// The sink's ACK CADENCE, handed to a poll adapter to size one peek — the
/// drain's memory bound (O(rollover), never O(total backlog)). On PostgreSQL the
/// peek is non-consuming: it re-reads from the slot's un-acked position every
/// time, so a peek NEVER slides forward on its own — only an ack (slot advance)
/// moves it. Reaching the open bound past a foreign/empty span larger than one
/// window is therefore NOT this budget's job (no budget covers an uncaptured or
/// empty span, whose wire:capture ratio is unbounded): the sink's re-drain loop
/// acks the consumed span and re-peeks the fresh WAL beyond it
/// ([`sink::run_to_files`]). `Sized` just carries the rollover — one ack's worth
/// per peek; the non-acking NDJSON driver is `Unbounded` (one peek drains
/// everything, the frontier check ends the stream, no re-drain).
#[derive(Debug, Clone, Copy)]
pub(crate) enum PeekBound {
    /// The sink's part `rollover` — one ack cadence per peek.
    Sized(usize),
    /// One peek pulls the whole backlog (the non-acking NDJSON path).
    Unbounded,
}

impl PeekBound {
    /// Resolve to a positive `upto_nchanges`-style row cap the adapters clamp to
    /// their SQL arg width. `Unbounded` ⇒ the i32 ceiling (effectively "all").
    pub(crate) fn rows_capped(self) -> usize {
        match self {
            PeekBound::Sized(n) => n.clamp(1, i32::MAX as usize),
            PeekBound::Unbounded => i32::MAX as usize,
        }
    }
}

/// The CDC engine, resolved ONCE from the source URL's scheme. Every
/// downstream dispatch matches on this enum — never on the URL string — so
/// adding engine #4 is one variant plus compiler-led match arms, and a
/// mistyped scheme fails in exactly one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdcEngine {
    Mysql,
    Postgres,
    Mssql,
    Mongo,
}

/// What image of a row this source can actually supply per change event.
///
/// rivet's change stream is a WHOLE-ROW image per event, and whether the source
/// can produce one is a SERVER setting, not a rivet setting. Each engine has its
/// own knob, so the question was being asked in two different layers with a third
/// engine not asked at all:
///
///   mysql    `binlog_row_image`         inside the stream's `open`
///   postgres `REPLICA IDENTITY`         a layer up, in `run_capture`
///   mongo    —                          whole by construction (UpdateLookup)
///   mssql    `@captured_column_list`    nowhere
///
/// One enum makes the answers comparable and moves the POLICY — refuse, warn, or
/// proceed — out of each engine's prose into one place. A new engine then cannot
/// forget: the method is a hole the compiler makes it fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowImage {
    /// Every column, on every event. Nothing to say.
    Whole,
    /// Inserts and updates carry the whole new row; DELETE carries only the key.
    /// Not corruption — a delete's job is to name the key that is gone — but it
    /// moves any per-row hash computed over deletes, so it is worth one line.
    KeyOnlyDeletes { why: String },
    /// The event cannot represent the row. Capturing would record events that are
    /// wrong rather than merely thin, so this is refused.
    Partial { why: String },
}
/// The native column TYPE a catalog listing gives for `column`.
///
/// Extracted from `CdcSchemaResolver::resolve`, which is live-only glue — it opens
/// a connection and reads `information_schema` — so its `*n == m.column_name`
/// survived a mutation run. `!=` takes the first column that is NOT the one asked
/// for, so every mapping is enriched with a NEIGHBOUR's native type: a `varchar`
/// column reported as `int`, silently, in the schema every consumer reads.
///
/// The third comparison of this exact shape found this session — `image_cell`'s
/// name lookup and `image_name_memo`'s were the others — and all three were a
/// lookup-by-name sitting in a body no offline test could reach.
pub(crate) fn native_type_for<'a>(
    catalog: &'a [(String, String)],
    column: &str,
) -> Option<&'a str> {
    catalog
        .iter()
        .find(|(name, _)| name == column)
        .map(|(_, ty)| ty.as_str())
}

impl CdcEngine {
    /// Can this engine's wire format ever map a row image by POSITION rather than
    /// by column name?
    ///
    /// MySQL only, and it is a property of the binlog: `binlog_row_metadata` may
    /// omit column names, and the events replay whatever was in force when they
    /// were WRITTEN. PostgreSQL's `test_decoding` names every column, SQL Server's
    /// change tables are relational, and Mongo's events are documents — none of
    /// them can produce a nameless image, so `None` there is a FACT and not a TODO.
    ///
    /// Extracted from `positional_mapping_warning`'s dispatch, which is live-only
    /// glue: `-> None` survived, and with it the whole warning disappears on the one
    /// engine that needs it. The fact now has one home instead of being restated in
    /// a match arm and a doc comment.
    pub(crate) fn maps_by_position(self) -> bool {
        match self {
            Self::Mysql => true,
            Self::Postgres | Self::Mssql | Self::Mongo => false,
        }
    }

    /// Does this engine make the SERVER retain log on the reader's behalf?
    ///
    /// PostgreSQL only, and structurally: a replication slot is the one CDC anchor
    /// that pins WAL until the reader acks. MySQL's binlog and SQL Server's change
    /// tables expire on their own schedule whatever rivet does — which is why they
    /// can LOSE data to retention and PostgreSQL instead fills a disk — and Mongo's
    /// oplog is capped. There is nothing to pin, so nothing to warn about.
    ///
    /// The inverse of this predicate is the reason those three engines need the
    /// retention checks in `doctor` that PostgreSQL does not.
    pub(crate) fn pins_log_for_reader(self) -> bool {
        match self {
            Self::Postgres => true,
            Self::Mysql | Self::Mssql | Self::Mongo => false,
        }
    }
}

impl CdcEngine {
    /// Ask the source what it can supply for the tables about to be captured.
    ///
    /// Best-effort by construction: a catalog the reader cannot query answers
    /// `Whole`. Refusing on a permission error would lock out sources that are
    /// fine, and this check exists to catch a CONFIGURATION, not to police access.
    pub(crate) fn row_image(
        &self,
        url: &str,
        tls: Option<&TlsConfig>,
        tables: &[String],
        opts: &CdcEngineOpts,
    ) -> RowImage {
        match self {
            Self::Mysql => crate::source::mysql::cdc::MysqlChangeStream::row_image(url, tls),
            Self::Postgres => {
                crate::source::postgres::cdc::PgChangeStream::row_image(url, tls, tables)
            }
            Self::Mssql => {
                // The gate must grade the instance the poll will READ; anything
                // else either sums across instances (masking a partial one) or
                // matches a same-named table in another schema.
                let ci = match opts {
                    CdcEngineOpts::Mssql {
                        capture_instance, ..
                    } => capture_instance.as_deref(),
                    _ => None,
                };
                crate::source::mssql::cdc::row_image(url, tls, tables, ci)
            }
            // The reader requests `FullDocumentType::UpdateLookup`, so the
            // post-image is the whole document whatever the server is set to.
            Self::Mongo => RowImage::Whole,
        }
    }

    /// Does this engine's CURRENT configuration map change images by position
    /// rather than by name, and what does that cost?
    ///
    /// MySQL is the only engine that can answer yes: its binlog carries column
    /// names only at `binlog_row_metadata=FULL`, and the default is MINIMAL. The
    /// other three name their columns unconditionally — PostgreSQL's
    /// `test_decoding` prints `name[type]:value`, SQL Server's change table IS a
    /// table, Mongo's events are documents — so there is no positional mapping to
    /// warn about, and a `None` here is structural rather than unimplemented.
    pub(crate) fn positional_mapping_warning(
        &self,
        url: &str,
        tls: Option<&TlsConfig>,
    ) -> Option<String> {
        if !self.maps_by_position() {
            return None;
        }
        crate::source::mysql::cdc::MysqlChangeStream::row_metadata(url, tls)
    }

    /// WAL this source is holding that an operator should know about before the run
    /// starts, not after the disk fills.
    ///
    /// PostgreSQL only, and structurally so: a replication slot is the one CDC
    /// anchor that makes the SERVER retain log on the reader's behalf. MySQL's
    /// binlog and SQL Server's change tables expire on their own schedule whatever
    /// rivet does (which is why they can lose data to retention and PG cannot), and
    /// Mongo's oplog is a capped collection. There is nothing to pin, so nothing to
    /// warn about — a `None` here is a fact about those engines, not a TODO.
    ///
    /// Two questions, and the second is the dangerous one. This export's OWN slot
    /// being far behind is worth saying (the drain will be long and WAL grows
    /// meanwhile) but the run does fix it. A slot NOBODY owns is pinned until a
    /// human acts — measured live at 9 abandoned slots holding 1.5 GiB each.
    ///
    /// Best-effort like `row_image`: a catalog the reader cannot query answers with
    /// nothing. This exists to surface a CONFIGURATION hazard, not to police access,
    /// and a run that refuses because it could not read `pg_replication_slots` would
    /// trade a warning for an outage.
    pub(crate) fn retention_warnings(
        &self,
        url: &str,
        tls: Option<&TlsConfig>,
        opts: &CdcEngineOpts,
    ) -> Vec<String> {
        if !self.pins_log_for_reader() {
            return Vec::new();
        }
        let CdcEngineOpts::Postgres { slot, .. } = opts else {
            return Vec::new();
        };
        let slot = slot.clone();
        let Ok(mut client) = crate::source::postgres::connect_client(url, tls) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        if let Ok(Some(row)) = client.query_opt(
            "SELECT active, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        ) && let Some(w) = crate::preflight::cdc_health::pg_retained_wal_warning(
            &slot,
            row.get(1),
            row.get(0),
        ) {
            out.push(w);
        }
        // ONE export's slot is all this seam knows: `CdcCapture` carries a single
        // `cdc_cfg`, so a sibling export's slot — drained by the same `rivet run` a
        // moment later — is indistinguishable from an abandoned one here. Hence
        // `may_be_owned_elsewhere = true`: report the WAL, never the verdict, and
        // never the drop command. `doctor` sees the whole config and does both.
        let ours = vec![slot];
        if let Ok(rows) = client.query(
            "SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint, \
                    slot_type \
             FROM pg_replication_slots WHERE NOT active AND slot_name <> ALL($1)",
            &[&ours],
        ) {
            let foreign: Vec<(String, i64, String)> = rows
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect();
            if let Some(w) = crate::preflight::cdc_health::pg_foreign_slots_warning(&foreign, true) {
                out.push(w);
            }
        }
        out
    }

    pub(crate) fn from_url(url: &str) -> Result<Self> {
        if url.starts_with("mysql://") {
            Ok(Self::Mysql)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres)
        } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
            Ok(Self::Mssql)
        } else if url.starts_with("mongodb://") || url.starts_with("mongodb+srv://") {
            Ok(Self::Mongo)
        } else {
            anyhow::bail!(
                "rivet cdc: unsupported source url — expected mysql:// / postgresql:// / sqlserver:// / mongodb://"
            )
        }
    }

    /// Stable lowercase label for metrics / run records / hints.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
            Self::Mssql => "mssql",
            Self::Mongo => "mongo",
        }
    }

    /// Ensure the resume anchor EXISTS — `initial: snapshot` step ① and the
    /// single entry point for anchor creation (idempotent: a present anchor is
    /// never moved). The per-engine anchor models (see the process rules):
    /// PG pins server-side at slot creation; MySQL has NO server-side anchor —
    /// the checkpoint is pinned at first open; MSSQL floors at
    /// `fn_cdc_get_min_lsn` without one (over-reads, never skips).
    /// `resume_expected` = prior-run evidence exists — a missing server-side
    /// anchor then fails LOUDLY instead of silently re-anchoring at "current".
    pub(crate) fn ensure_anchor(
        self,
        url: &str,
        slot: &str,
        checkpoint: Option<&std::path::Path>,
        tls: Option<&crate::config::TlsConfig>,
        resume_expected: bool,
    ) -> Result<()> {
        match self {
            Self::Postgres => {
                // Slot creation IS the anchor; open() creates it only on a
                // genuine FIRST run (resume_expected=false).
                // Anchor-only open: it creates the slot and is dropped without
                // reading, so the peek bound is irrelevant.
                drop(crate::source::postgres::cdc::PgChangeStream::open(
                    url,
                    slot,
                    resume_expected,
                    tls,
                    PeekBound::Unbounded,
                    DrainMode::Continuous, // anchor-only open — never read, no bound to pin
                    // No routing cross-check here, deliberately. This open exists
                    // to CREATE the slot and is dropped without reading or
                    // acking, so it cannot lose anything; the capture open that
                    // follows carries the tables and bails there. Refusing before
                    // the anchor would be actively worse for the operator —
                    // changes written between a bad config and its fix would have
                    // no slot holding them, whereas a slot created first keeps
                    // every one of them until the corrected run drains it.
                    &[],
                    // Nothing is read here, so there is nothing to spill.
                    None,
                )?);
                Ok(())
            }
            Self::Mysql | Self::Mssql | Self::Mongo => {
                let ckpt = checkpoint.ok_or_else(|| {
                    anyhow::anyhow!(
                        "{} cdc: an anchor needs cdc.checkpoint (no server-side anchor exists)",
                        self.label()
                    )
                })?;
                if Position::load(ckpt)?.is_some() {
                    return Ok(()); // anchored already — never move it
                }
                if resume_expected {
                    // Prior-run evidence (a completed snapshot marker) with a
                    // MISSING checkpoint: pinning "current" would silently skip
                    // everything since the loss — and on MSSQL would actively
                    // destroy the min-LSN over-read floor. Fail loudly.
                    anyhow::bail!(
                        // BOTH signals, because they are OR-ed: `snapshot_done` reads
                        // the state DB's `cdc_snapshot` row (authoritative) OR the
                        // destination's `snapshot/_SUCCESS` marker (legacy co-signal).
                        // An earlier hint named only the marker — an operator who
                        // deleted it still had `done == true` from the row, and the
                        // identical bail fired again on the next run, forever.
                        "{} cdc: checkpoint '{}' is missing but prior-run evidence exists — \
                         either restore the checkpoint file, or re-snapshot: clear the \
                         export's `cdc_snapshot` row in the state DB AND delete the \
                         destination's snapshot/_SUCCESS marker (the two done-signals \
                         are OR-ed, so leaving either in place skips the snapshot). If a \
                         warehouse load consumes this stream, ALSO truncate its \
                         `<table>__changes` table before the next load: a re-snapshot \
                         row carries NULL `__pos` and LOSES the dedup to every \
                         already-loaded change row, so the current-state view would \
                         silently serve pre-gap values (see cdc-failure-modes.md)",
                        self.label(),
                        ckpt.display()
                    );
                }
                match self {
                    Self::Mysql => {
                        crate::source::mysql::cdc::MysqlChangeStream::pin_checkpoint_at_current(
                            url, ckpt, tls,
                        )
                    }
                    Self::Mssql => {
                        crate::source::mssql::cdc::pin_checkpoint_at_max_lsn(url, ckpt, tls)
                    }
                    _ => crate::source::mongo::cdc::pin_checkpoint_at_current(url, tls, ckpt),
                }
            }
        }
    }
}

/// Add an engine's setup hint ONLY to errors rivet did not raise itself.
///
/// Every `open` is wrapped in a `wal_level` / binlog-grants / Agent-setup hint,
/// because a connection that fails for a permissions reason gives an opaque driver
/// message and the hint is the whole answer. But rivet's OWN verdicts are already
/// the whole answer, and the wrap puts the wrong cause FIRST — measured on the PG
/// anti-gap guard, which reports permanent DATA LOSS:
///
///   Error: if this is a permissions/setup error: PostgreSQL CDC needs wal_level=
///   logical and a role with the REPLICATION attribute — see … : pg cdc: slot
///   'x' is missing but a resume checkpoint exists — the changes since then are no
///   longer in the log. Re-snapshot …
///
/// The operator reads a wal_level troubleshooting line while the sentence that
/// matters — re-snapshot, the data is gone — sits past a colon at the end. Two
/// call sites were already hoisted OUT of their wrap one at a time for exactly
/// this (`precheck_configured_tables` on both MySQL and PG, each with a comment
/// saying the hint "sends the operator to fix permissions they never had a problem
/// with"). Hoisting works only for a check that can run BEFORE `open`; this covers
/// the ones raised inside it.
///
/// The discriminator is the prefix rivet puts on its own CDC messages
/// (`mysql cdc:`, `pg cdc:`, …), which is also what the refusals and guards in each
/// adapter are built from.
fn with_setup_hint(e: anyhow::Error, hint: &'static str) -> anyhow::Error {
    let rendered = format!("{e:#}");
    if [
        "mysql cdc:",
        "pg cdc:",
        "mssql cdc:",
        "mongodb cdc:",
        "cdc:",
    ]
    .iter()
    .any(|p| rendered.contains(p))
    {
        return e;
    }
    e.context(hint)
}

/// Setup/permission hints appended to a CDC start-up error — so a missing grant
/// surfaces the fix, not just a raw driver error. Phrased "if this is a
/// permissions/setup error" because the same call can fail for other reasons.
pub(crate) const MYSQL_CDC_HINT: &str = "if this is a permissions/setup error: MySQL CDC needs binlog_format=ROW plus a REPLICATION SLAVE + REPLICATION CLIENT grant (and SELECT on the table) — see the 'MySQL — the binlog grants' section of docs/reference/cdc.md";
pub(crate) const PG_CDC_HINT: &str = "if this is a permissions/setup error: PostgreSQL CDC needs wal_level=logical and a role with the REPLICATION attribute — see the 'PostgreSQL — the logical slot' section of docs/reference/cdc.md";
pub(crate) const MSSQL_CDC_HINT: &str = "if this is a permissions/setup error: SQL Server CDC must be enabled on the table (sys.sp_cdc_enable_table) with SQL Server Agent running, and the reader needs SELECT on the cdc schema — see the 'SQL Server — CDC change tables' section of docs/reference/cdc.md";
pub(crate) const MONGO_CDC_HINT: &str = "if this is a setup error: MongoDB change streams require a replica set (a single-node replica set is fine) — a standalone mongod cannot watch(); the reader needs a role that can run changeStream (readAnyDatabase / read on the db) — see the 'MongoDB — change streams' section of docs/reference/cdc.md";

/// Where an oversized transaction spills — `None` unless the operator named a
/// directory in `RIVET_CDC_SPILL_DIR`.
///
/// OPT-IN, and the reason is a measurement rather than caution. Spilling was built
/// to replace the cap's refusal ("a transaction past the cap fails the run") with
/// something better. It is not better yet: the sink cannot roll a part
/// mid-transaction (`RolloverPolicy::should_roll` requires `committed`, the
/// invariant that makes crash resume transaction-atomic), so it holds the whole
/// transaction whatever the adapter does. Measured on a 100k-row transaction: 202 MB
/// with spilling, 226 MB without — ~11%, not a ceiling.
///
/// So spilling ON BY DEFAULT would trade a guard that works for a mitigation that
/// mostly does not: the run stops failing loudly and proceeds toward the same OOM,
/// now with a false sense of protection. Four live tests
/// (`roast_*_oversized_transaction_bails_loud_not_oom`,
/// `cdc_oversized_transaction_by_bytes_bails_loudly`) pin that guard on three
/// engines and are the only reason this was caught.
///
/// Naming the directory is also the honest way to ask: a CDC spill can be gigabytes,
/// and writing them into `.rivet/spill` because rivet felt like it is not a decision
/// rivet gets to make. When the SINK learns to spill too, this becomes a default
/// worth arguing for — and the soak stand's 202-vs-226 gap is where that argument
/// will be settled.
pub(crate) fn spill_dir_for(
    checkpoint: Option<&std::path::Path>,
    config_dir: &std::path::Path,
) -> Option<std::path::PathBuf> {
    let raw = std::env::var("RIVET_CDC_SPILL_DIR").ok()?;
    // TRIMMED: a `.env` line or a YAML `environment:` entry trivially carries a
    // trailing space, and `"1 "` would otherwise become a directory literally named
    // `1 ` — spilling ON, into junk, for a value the operator meant as the switch.
    let named = raw.trim();
    if named.is_empty() {
        return None;
    }
    // FALSY values mean OFF. Recognising truthiness in one direction only was the
    // trap: `RIVET_CDC_SPILL_DIR=0`, written to DISABLE spilling, fell through to
    // the path arm and enabled it into a directory named `0` — while disabling the
    // oversized-transaction refusal, which is the guard this switch exists to keep.
    if ["0", "false", "off", "no"].contains(&named.to_ascii_lowercase().as_str()) {
        return None;
    }
    // …and the truthy list carries `on` because the falsy one carries `off`: an
    // operator mirroring an accepted spelling must not get a directory named `on`.
    // These four are also exactly the strings YAML-1.1 tooling treats as booleans.
    if ["1", "true", "yes", "on"].contains(&named.to_ascii_lowercase().as_str()) {
        // A truthy value means "spill, you pick where": beside the CHECKPOINT when
        // there is one — a directory the operator already chose and sized for
        // rivet's state — and beside the CONFIG otherwise. Never the system temp (a
        // CDC spill can be gigabytes and a tmpfs takes down more than rivet), and
        // never the process CWD: the shipped image runs with cwd=`/` and a non-root
        // user, so a cwd-relative `.rivet/spill` worked for weeks and then failed
        // with a path-less EACCES at the exact moment the cap was crossed — and on
        // a host, spills landed per-INVOCATION-cwd where no later sweep could find
        // them. `resolve_checkpoint`'s doc records the same cwd-anchor causing loss
        // for checkpoints; this is the same rule applied to the spill.
        return Some(match checkpoint.and_then(std::path::Path::parent) {
            Some(dir) if !dir.as_os_str().is_empty() => dir.join(".rivet-spill"),
            _ => config_dir.join(".rivet").join("spill"),
        });
    }
    // An explicit path: relative forms anchor to the CONFIG's directory, exactly
    // like `cdc.checkpoint:` — one config must not read two locations depending on
    // where the process happened to start.
    let p = std::path::Path::new(named);
    Some(if p.is_absolute() {
        p.to_path_buf()
    } else {
        config_dir.join(p)
    })
}

/// Construct the right [`ChangeStream`] adapter for the source URL's scheme —
/// dispatching by engine exactly as [`crate::source::create_source`] does for the
/// batch path. `cfg.drain` reaches every adapter as-is — the open-time-ceiling
/// contract lives on [`DrainMode`].
pub(crate) fn create_change_stream(
    cfg: &CdcConfig,
    peek: PeekBound,
) -> Result<Box<dyn ChangeStream>> {
    let url = cfg.url.as_str();
    // A host-less URL is a config/parse error, not a per-engine setup problem —
    // validate BEFORE the engine match so it never gets blanketed by the binlog/
    // slot/CDC grants hint below (dogfood LOW).
    crate::source::require_url_has_host(url)?;
    let tls = cfg.tls.as_ref();
    // Sweep crashed runs' spill orphans ONCE per open — not only when a NEW
    // spill is born in the same dir (round-5 lifecycle: a one-off giant
    // transaction that also crashed the run leaked its multi-GB spill until
    // another transaction crossed the cap there, possibly never). flock decides
    // liveness, so a concurrent run's held spill is spared.
    if let Some(dir) = spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir) {
        spill::sweep_dead_spills_now(&dir);
    }
    // The engine identity IS the opts variant — no re-resolution from the URL.
    match &cfg.engine {
        CdcEngineOpts::Mysql {
            server_id,
            configured_tables,
        } => {
            // Validate the checkpoint BEFORE open_or_resume so a corrupt/truncated
            // checkpoint (or a directory path) surfaces cleanly, not blanketed by
            // the MYSQL_CDC_HINT binlog-grants message — the same hoist PG/MSSQL
            // already do below (dogfood MED: MySQL reported a checkpoint-file
            // error as a permissions/setup problem). open_or_resume re-reads it;
            // the double read of a tiny file is cheap.
            if let Some(p) = cfg.checkpoint.as_deref() {
                Position::load(std::path::Path::new(p))?;
            }
            // Same hoist, same reason: a configured name the binlog can never carry
            // (a VIEW, whose Table_map names the BASE table) is a CONFIG problem, and
            // raising it inside open would prefix it with the binlog-grants hint.
            crate::source::mysql::cdc::MysqlChangeStream::precheck_configured_tables(
                url,
                tls,
                configured_tables,
            )?;
            Ok(Box::new(
                crate::source::mysql::cdc::MysqlChangeStream::open_or_resume(
                    url,
                    *server_id,
                    cfg.checkpoint.as_deref(),
                    cfg.drain,
                    tls,
                    configured_tables.clone(),
                    spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir),
                )
                .map_err(|e| with_setup_hint(e, MYSQL_CDC_HINT))?,
            ))
        }
        CdcEngineOpts::Postgres {
            slot,
            configured_tables,
        } => {
            // A persisted checkpoint proves a prior run happened — if the slot is
            // then MISSING, it was dropped/invalidated and silently recreating it
            // at the current position would skip everything since (a silent gap).
            // Propagate a corrupt/truncated checkpoint (#99): `.ok()` swallowed it
            // into resume_expected=false, so a dropped slot got silently recreated
            // at 'current' and skipped every change since — the anti-gap guard
            // (missing slot + resume_expected) never fired.
            let resume_expected = match cfg.checkpoint.as_deref() {
                Some(p) => Position::load(p)?.is_some(),
                None => false,
            };
            // Hoisted out of the PG_CDC_HINT wrap below: a configured name the
            // stream can never route is a CONFIG problem, and raising it inside
            // open prefixes it with a wal_level/REPLICATION hint that sends the
            // operator to fix permissions they never had a problem with.
            crate::source::postgres::cdc::PgChangeStream::precheck_configured_tables(
                url,
                tls,
                configured_tables,
            )?;
            Ok(Box::new(
                crate::source::postgres::cdc::PgChangeStream::open(
                    url,
                    slot,
                    resume_expected,
                    tls,
                    peek,
                    cfg.drain,
                    configured_tables,
                    spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir).as_deref(),
                )
                .map_err(|e| with_setup_hint(e, PG_CDC_HINT))?,
            ))
        }
        CdcEngineOpts::Mssql {
            capture_instance,
            configured_tables,
        } => {
            let ci = capture_instance.as_deref().ok_or_else(|| {
                anyhow::anyhow!("sqlserver cdc requires --capture-instance (e.g. dbo_orders)")
            })?;
            // Resume from the checkpoint's position if one was persisted (SQL Server
            // has no server-side cursor — the from-LSN is what makes it at-least-once
            // instead of re-reading the whole change table each run). One load, and
            // the DECISION — including what an `lsn`-less file means — lives in
            // `resume_from_checkpoint` where a unit test can grade it.
            let resume = match cfg.checkpoint.as_deref() {
                Some(p) => crate::source::mssql::cdc::resume_from_checkpoint(
                    Position::load(p)?.as_ref(),
                    &p.display().to_string(),
                )?,
                None => crate::source::mssql::cdc::resume_from_checkpoint(None, "")?,
            };
            Ok(Box::new(
                crate::source::mssql::cdc::MssqlChangeStream::from_url(
                    url,
                    ci,
                    resume,
                    tls,
                    peek,
                    cfg.drain,
                    configured_tables,
                    spill_dir_for(cfg.checkpoint.as_deref(), &cfg.config_dir),
                )
                .map_err(|e| with_setup_hint(e, MSSQL_CDC_HINT))?,
            ))
        }
        CdcEngineOpts::Mongo {
            canonical,
            configured_tables,
        } => {
            // Validate the checkpoint BEFORE open so a corrupt/truncated one
            // surfaces cleanly, not blanketed by MONGO_CDC_HINT — the same hoist
            // MySQL/PG/MSSQL do (bughunt MED: MongoChangeStream::open loaded it
            // INSIDE the hint wrap). open re-reads it; a tiny double read is cheap.
            if let Some(p) = cfg.checkpoint.as_deref() {
                Position::load(std::path::Path::new(p))?;
            }
            Ok(Box::new(
                // Whole-database change stream; resumes from the persisted token
                // when one exists. `document` JSON fidelity follows
                // `source.mongo.json` (canonical vs relaxed), so CDC and batch
                // render it identically.
                crate::source::mongo::cdc::MongoChangeStream::open(
                    url,
                    tls,
                    cfg.checkpoint.as_deref(),
                    *canonical,
                    cfg.drain,
                    configured_tables,
                )
                // The setup hint is a GUESS about the cause, so it must not be
                // pasted onto an error that already names one. An oversized change
                // event (`BSONObjectTooLarge`) surfaced here wearing "change streams
                // require a replica set" — on a stand that IS one — which sends the
                // operator to inspect a healthy topology and, finding nothing,
                // eventually delete the checkpoint: the one action that turns a
                // stalled run into lost data. Round 9, measured.
                .map_err(|e| {
                    if crate::source::mongo::cdc::error_names_its_own_cause(&e) {
                        e
                    } else {
                        with_setup_hint(e, MONGO_CDC_HINT)
                    }
                })?,
            ))
        }
    }
}

/// Resolve CDC tables' column type mappings from the source — the **same**
/// `RivetType` → Arrow pipeline the batch export uses — so the typed file sink
/// writes identical columns (logical types `json`/`uuid`/…, real int widths, …)
/// via [`crate::types::build_arrow_field`]. Session-based: ONE source
/// connection (plus, for MySQL, one enrichment connection) serves every table
/// of a multi-table export — the per-table constructor cost was 2 connections
/// per table per run.
pub(crate) struct CdcSchemaResolver {
    src: Box<dyn crate::source::Source>,
    /// MySQL-only: one connection for the `information_schema.COLUMN_TYPE`
    /// enrichment (wire metadata has no widths/labels for BIT/BINARY/ENUM/SET).
    enrich: Option<mysql::PooledConn>,
}

impl CdcSchemaResolver {
    pub(crate) fn connect(url: &str, tls: Option<&crate::config::TlsConfig>) -> Result<Self> {
        let engine = CdcEngine::from_url(url)?;
        let src: Box<dyn crate::source::Source> = match engine {
            CdcEngine::Mysql => Box::new(crate::source::mysql::MysqlSource::connect_with_tls(
                url, tls,
            )?),
            CdcEngine::Postgres => Box::new(
                crate::source::postgres::PostgresSource::connect_with_tls(url, tls)?,
            ),
            CdcEngine::Mssql => Box::new(crate::source::mssql::MssqlSource::connect_with_tls(
                url, tls,
            )?),
            // The JSON-blob model has a fixed 2-column schema (`_id`, `document`),
            // resolved by `MongoSource::type_mappings` — same as the batch path.
            CdcEngine::Mongo => {
                Box::new(crate::source::mongo::MongoSource::connect(url, tls, None)?)
            }
        };
        let enrich = match engine {
            CdcEngine::Mysql => Some(crate::source::mysql::connect_pool(url, tls)?.get_conn()?),
            _ => None,
        };
        Ok(Self { src, enrich })
    }

    /// One table's mappings. `overrides` are the export's `columns:`
    /// declarations for THIS table (already narrowed by
    /// `types::overrides_for_table`) — the same override surface batch honours.
    pub(crate) fn resolve(
        &mut self,
        table: &str,
        overrides: &crate::types::ColumnOverrides,
    ) -> Result<Vec<crate::types::TypeMapping>> {
        validate_table_ident(table)?;
        let mut mappings = self
            .src
            .type_mappings(&format!("SELECT * FROM {table}"), overrides)?;
        // A source column in the CDC meta namespace collides with the columns the
        // sink prepends, and the collision is silent in the direction that matters.
        // The sink puts `__op`/`__pos`/`__seq` at batch indices 0-2, and
        // `row_hash_array` resolves each covered name with `index_of`, which returns
        // the FIRST match — so `_rivet_row_hash` over a table with a column called
        // `__op` folds the sink's operation string instead of the source cell, and
        // the drain and the snapshot leg then produce DIFFERENT hashes for the same
        // row. That breaks precisely the cross-leg comparison the column exists for.
        // The part would also carry two fields of one name, which no reader resolves
        // the same way twice.
        //
        // Round-10 bughunt, read-only there; refused here rather than papered over,
        // because there is no rendering of this table that is both faithful and
        // unambiguous.
        if let Some(m) = mappings
            .iter()
            .find(|m| matches!(m.column_name.as_str(), "__op" | "__pos" | "__seq"))
        {
            anyhow::bail!(
                "cdc: `{table}` has a column named `{}`, which is one of the names the \
                 CDC sink adds to every part (`__op`, `__pos`, `__seq`). Capturing it \
                 would put two fields of that name in one part and silently redirect \
                 `_rivet_row_hash` to the sink's value instead of the source's — the \
                 drain and the snapshot leg would then disagree about the same row. \
                 Project the column to another name with a `query:`, or exclude it.",
                m.column_name
            );
        }
        // MySQL: enrich `source_native_type` with the full
        // `information_schema.COLUMN_TYPE` ("bit(8)", "binary(4)",
        // "enum('a','b','c')") — the binlog cell fixes need widths + labels the
        // wire metadata lacks. CDC-only; batch's contract-pinned native names
        // stay untouched.
        if let Some(conn) = self.enrich.as_mut() {
            use mysql::prelude::Queryable;
            // A qualified `db.table` carries its OWN schema, which may differ from
            // the connection's DATABASE() (a cross-database capture), and a db-less
            // URL has no DATABASE() at all. The old query dropped the qualifier and
            // pinned `TABLE_SCHEMA = DATABASE()`, so both cases enriched NOTHING —
            // ENUM/SET columns then kept their raw wire index / bitmask instead of
            // the `enum('a',…)` / `set('x',…)` labels the binlog cell fixes need,
            // silently corrupting every such column. Split like the batch path
            // (mysql::mod) and query the EXPLICIT schema.
            let default_db: Option<String> = if table.contains('.') {
                None
            } else {
                conn.query_first("SELECT DATABASE()")?
            };
            let (schema, bare) = enrich_schema_and_table(table, default_db.as_deref());
            let full: Vec<(String, String)> = conn.exec(
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (&schema, &bare),
            )?;
            for m in &mut mappings {
                if let Some(ct) = native_type_for(&full, &m.column_name) {
                    m.source_native_type = ct.to_string();
                }
            }
        }
        Ok(mappings)
    }
}

/// Split a possibly-qualified `db.table` into the `(schema, bare_table)` the
/// MySQL `information_schema.COLUMNS` enrichment query needs. A qualified name
/// carries its OWN schema — which may differ from the connection's default DB (a
/// cross-database capture) — so its qualifier wins; an unqualified name falls
/// back to `default_db` (the connection's `DATABASE()`, `""` when the URL has no
/// default database). Mirrors the batch introspection split (mysql::mod), so CDC
/// and batch resolve the same schema for the same table. `default_db` is only
/// consulted for an unqualified name (the caller passes `None` for a qualified
/// one to skip the extra `SELECT DATABASE()` round-trip).
fn enrich_schema_and_table(table: &str, default_db: Option<&str>) -> (String, String) {
    match table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => (
            default_db.unwrap_or_default().to_string(),
            table.to_string(),
        ),
    }
}

/// Single-table convenience over [`CdcSchemaResolver`] (CLI path + tests).
pub(crate) fn resolve_cdc_columns(
    url: &str,
    table: &str,
    tls: Option<&crate::config::TlsConfig>,
    overrides: &crate::types::ColumnOverrides,
) -> Result<Vec<crate::types::TypeMapping>> {
    // Validate BEFORE connecting, so a hostile table name needs no database.
    validate_table_ident(table)?;
    CdcSchemaResolver::connect(url, tls)?.resolve(table, overrides)
}

/// The table name is interpolated into `SELECT * FROM {table}` for the schema
/// probe — refuse anything but a plain `[schema.]table` identifier (no quote,
/// paren, semicolon, or space can break out).
///
/// `pub(crate)` because the type-report resolver builds the SAME probe query for
/// each table of a `tables:` stream: config-load only gates those names for
/// FILENAME safety (they become destination path segments), which is a weaker
/// alphabet than SQL interpolation needs.
pub(crate) fn validate_table_ident(table: &str) -> Result<()> {
    if table.is_empty()
        || !table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        anyhow::bail!(
            "rivet cdc table must be a plain [schema.]table identifier (got {table:?}); \
             refusing to interpolate it into SQL"
        );
    }
    Ok(())
}

/// One table's destination wiring for a capture — see [`CdcCapture::outputs`].
pub(crate) struct CaptureOutput<'a> {
    pub table: String,
    pub dest: &'a dyn crate::destination::Destination,
    pub dest_uri: String,
    /// The export's `columns:` type overrides for THIS table — already
    /// narrowed by `types::overrides_for_table` (bare keys apply everywhere;
    /// `"table.column"` keys target one table and win over bare).
    pub overrides: crate::types::ColumnOverrides,
    /// `exports[].meta_columns.row_hash` — the drain must emit the SAME hash
    /// column the snapshot leg does, or the warehouse table ends up
    /// half-populated.
    pub row_hash: crate::config::RowHash,
}

/// Everything needed to capture a change stream to typed files, assembled once —
/// the source/output differ between the `rivet cdc` CLI and a `mode: cdc` run, but
/// the capture itself (open the stream, resolve the schemas, drive the file sink)
/// is identical. Both entry points fill this in and call [`run_capture`].
/// `outputs` carries one entry per captured table: several tables ride ONE stream
/// (one slot / one binlog connection) and one checkpoint.
pub(crate) struct CdcCapture<'a> {
    /// `exports[].name` — recorded into each manifest's `export_family` so the
    /// load's shared-prefix guard groups the drain with its snapshot leg by what
    /// was WRITTEN, not by re-deriving it from the table string.
    pub export_name: String,
    pub cdc_cfg: CdcConfig,
    pub outputs: Vec<CaptureOutput<'a>>,
    pub format: crate::config::FormatType,
    pub max_events: Option<usize>,
    pub rollover: usize,
    pub rollover_memory_bytes: Option<usize>,
    /// RFC3339 stamps the caller owns (`Utc::now()` is theirs to call).
    pub run_id: String,
    pub started_at: String,
    /// The central ledger. A `mode: cdc` run passes its store so every part is
    /// recorded in the DATABASE as it becomes durable; the `rivet cdc` CLI has
    /// no state store and passes `None`.
    pub state: Option<&'a crate::state::StateStore>,
}

/// Open the change stream (with the engine's permission/TLS gate), resolve each
/// table's typed schema, and drive the commit-seam file sink — the single place
/// the typed CDC capture is assembled. Returns one `RunManifest` per output, in
/// `outputs` order — PAIRED with the outcome, so a run that failed after
/// committing parts still hands the caller what it made durable. Returning a
/// bare `Result` discarded exactly that, and the caller recorded zeros.
pub(crate) fn run_capture(
    cap: CdcCapture<'_>,
    read_bytes: &std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> (Vec<crate::manifest::RunManifest>, Result<()>) {
    let url = cap.cdc_cfg.url.clone();
    let tls = cap.cdc_cfg.tls.clone();
    let checkpoint = cap.cdc_cfg.checkpoint.clone();
    // Derive the peek bound from the ONE rollover the sink also uses — so the
    // PG peek is always ≥ the part rollover (never starves). The single source
    // of truth for both is `cap.rollover`.
    // Setup failures happen BEFORE anything is durable, so an empty manifest
    // list is the truth here — unlike the drain, where it was a lie.
    let mut stream = match create_change_stream(&cap.cdc_cfg, PeekBound::Sized(cap.rollover)) {
        Ok(s) => s,
        Err(e) => return (Vec::new(), Err(e)),
    };
    // Fault point: stream (and any server-side anchor) opened, nothing read.
    crate::test_hook::maybe_panic_at("cdc_after_open");
    let engine = match CdcEngine::from_url(&url) {
        Ok(e) => e,
        Err(e) => return (Vec::new(), Err(e)),
    };
    let cap_tables: Vec<String> = cap.outputs.iter().map(|o| o.table.clone()).collect();
    let mut outputs = Vec::with_capacity(cap.outputs.len());
    // ONE resolver session serves every table (was: 2 fresh connections per
    // table per run — the multi-table per-cycle cost the roast flagged).
    crate::test_hook::maybe_panic_at("cdc_before_resolve");
    // ONE place decides what each verdict MEANS. The captured tables are known
    // here and nowhere earlier, which is why the question is asked at this seam
    // rather than inside each stream's `open`: scoped to what is actually being
    // captured, the answer is one line an operator can act on instead of a census
    // of the database (a first cut counted every table and said "704").
    match engine.row_image(&url, tls.as_ref(), &cap_tables, &cap.cdc_cfg.engine) {
        RowImage::Whole => {}
        RowImage::KeyOnlyDeletes { why } => log::warn!(
            "{} cdc: {why}. Counts still reconcile, but a per-row hash over deletes will differ \
             from the same table's batch export.",
            engine.label()
        ),
        RowImage::Partial { why } => {
            return (
                Vec::new(),
                Err(anyhow::anyhow!(
                    "{} cdc: {why}. Capturing under this setting would report success over \
                     events that cannot represent the row.",
                    engine.label()
                )),
            );
        }
    }
    // `warn`, at run start, before a single event is read: an operator whose
    // capture is about to map by position must learn it from the run rather than
    // from a swapped column months later. `info` would be functionally silent at
    // the default log level — the same rule the sparse-chunk warning follows.
    if let Some(why) = engine.positional_mapping_warning(&url, tls.as_ref()) {
        log::warn!("{} cdc: {why}.", engine.label());
    }
    for why in engine.retention_warnings(&url, tls.as_ref(), &cap.cdc_cfg.engine) {
        log::warn!("{} cdc: {why}.", engine.label());
    }
    let mut resolver = match CdcSchemaResolver::connect(&url, tls.as_ref()) {
        Ok(r) => r,
        Err(e) => return (Vec::new(), Err(e)),
    };
    for o in cap.outputs {
        // Probe the relation the STREAM resolved, not the string the config spelled
        // — the two are the same on every engine that cannot do better, and on SQL
        // Server the difference was a silently mis-columned export.
        let probe = match stream.resolved_identity(&o.table) {
            Some((schema, table)) => format!("{schema}.{table}"),
            None => o.table.clone(),
        };
        let columns = match resolver.resolve(&probe, &o.overrides) {
            Ok(c) => c,
            Err(e) => return (Vec::new(), Err(e)),
        };
        outputs.push(sink::TableOutput {
            table: o.table,
            columns,
            dest: o.dest,
            dest_uri: o.dest_uri,
            row_hash: o.row_hash,
        });
    }
    let sink_cfg = sink::SinkConfig {
        export_name: cap.export_name,
        outputs,
        engine,
        format: cap.format,
        checkpoint,
        max_events: cap.max_events,
        rollover: cap.rollover,
        rollover_memory_bytes: cap.rollover_memory_bytes,
        started_at: cap.started_at,
        run_id: cap.run_id,
        state: cap.state,
        read_bytes: std::sync::Arc::clone(read_bytes),
    };
    sink::run_to_files(stream.as_mut(), sink_cfg)
}

/// Resolve a configured `cdc.checkpoint:` path.
///
/// An ABSOLUTE path is used as written. A RELATIVE one is resolved against the
/// CONFIG FILE's directory — the way rivet already resolves the state DB and every
/// other relative path — rather than against the process working directory, which
/// is what it did until round 9 measured the cost: `rivet init` scaffolds
/// `checkpoint: ./cdc/<table>.ckpt`, so the same config invoked from a cron entry, a
/// systemd unit with its own `WorkingDirectory`, a container entrypoint or by hand
/// looked somewhere else, found nothing, and re-anchored at the CURRENT log
/// position. Measured: three green runs delivered `[3]` of a source holding
/// `[1,2,3]`.
///
/// COMPATIBILITY, and it is the reason this is a function rather than a `join`: a
/// deployment whose working directory happens to be where its checkpoint already
/// lives must not be moved out from under it by this fix — that would cause exactly
/// the loss the fix exists to prevent, once, on upgrade. So if the config-relative
/// location has no file and the CWD-relative one does, the existing file wins and
/// the run says where it will live from now on.
///
/// It lives HERE, beside `Position`, rather than in the runner, because `rivet
/// doctor` must answer about the SAME file the run will open. It did not: the
/// preflight read the raw string against the process working directory, so a
/// config invoked from anywhere else graded a checkpoint that was not there —
/// and "not there" is this check's GREEN answer ("no checkpoint yet — the first
/// run pins the open position"). The one check whose job is to catch a position
/// about to fall off binlog retention was reporting on an absent file.
pub(crate) fn resolve_checkpoint(raw: &str, config_dir: &std::path::Path) -> PathBuf {
    let p = std::path::Path::new(raw);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    // `components()` drops the `.` that `rivet init` scaffolds into every path it
    // writes (`./cdc/<table>.ckpt`), so the location rivet REPORTS is one an
    // operator can compare against `ls` — `/etc/rivet/./cdc/t.ckpt` is the same
    // file and reads like a bug in the message that names it. Rendering only: the
    // components are unchanged, so this cannot move where the file lives.
    let by_config: PathBuf = config_dir.join(p).components().collect();
    if !by_config.exists() && p.exists() {
        log::warn!(
            "cdc: using the existing checkpoint at `{}` (relative to the working \
             directory). rivet now resolves a relative `cdc.checkpoint:` against the \
             config's directory, so this run would otherwise have re-anchored at the \
             current log position and skipped everything since. Move it to `{}` — or \
             make the path absolute — so the location no longer depends on where \
             rivet is invoked from.",
            p.display(),
            by_config.display()
        );
        return p.to_path_buf();
    }
    by_config
}

#[cfg(test)]
mod mod_decisions {
    use super::*;

    /// The two buffer caps, the sequence stamp and the byte estimate — the clusters
    /// a mutation run over this file found ungraded (45+ survivors, 12 of them in
    /// the caps alone).
    #[test]
    fn a_tx_cap_falls_back_to_its_default_on_anything_that_is_not_a_positive_number() {
        // The override, when it is one.
        assert_eq!(tx_cap_from_env(Some("7"), 100), 7);
        // ABSENT, unparseable, negative, and ZERO all mean "use the default".
        // `-> 0` is the mutant that matters: a zero cap makes `len() > cap` true on
        // the FIRST row, so every transaction is refused as oversized and CDC stops.
        assert_eq!(tx_cap_from_env(None, 100), 100);
        assert_eq!(tx_cap_from_env(Some("abc"), 100), 100);
        assert_eq!(tx_cap_from_env(Some("-1"), 100), 100);
        assert_eq!(
            tx_cap_from_env(Some("0"), 100),
            100,
            "`RIVET_CDC_MAX_TX_ROWS=0` must not mean `refuse every transaction` — \
             taking it literally turns a tuning knob into a kill switch"
        );
        // The defaults are named so `*` -> `+` / `/` in the constant is graded.
        assert_eq!(DEFAULT_MAX_TX_ROWS, 5_000_000);
        assert_eq!(
            DEFAULT_MAX_TX_BYTES,
            2 * 1024 * 1024 * 1024,
            "2 GiB — `*` -> `+` collapses it to 3074 bytes, which refuses every \
             transaction that carries more than a couple of rows"
        );
    }

    /// Every op answers where its values live, and only DELETE says "before".
    ///
    /// One fact that was restated as a `match` in three places, ungraded in each.
    /// With the delete arm gone in Mongo's `to_change_event`, a delete reads the
    /// post-image — which does not exist on a delete — and is then framed as an
    /// AFTER image: the change arrives as an insert of NULLs, and the row it was
    /// meant to retract stays in the destination forever, with every count
    /// agreeing.
    ///
    /// Enumerated, not spot-checked: `matches!(self, Delete)` and `true` differ
    /// only on the ops a one-variant fixture would not exercise.
    #[test]
    fn only_a_delete_carries_its_values_in_the_before_image() {
        assert!(
            ChangeOp::Delete.values_live_in_before(),
            "a delete has no post-image; reading `after` finds None and the event \
             carries nothing"
        );
        assert!(
            !ChangeOp::Insert.values_live_in_before(),
            "an insert's values are the AFTER image — framing them as `before` \
             delivers a retraction of a row that was just created"
        );
        assert!(!ChangeOp::Update.values_live_in_before());
    }

    /// A column's native type comes from ITS row in the catalog, not a neighbour's.
    ///
    /// TWO columns minimum: with one, `==` and `!=` are indistinguishable because
    /// there is no neighbour to pick up — the same reason the row-hash injectivity
    /// guard needed two fields.
    #[test]
    fn a_columns_native_type_is_looked_up_by_its_own_name() {
        let catalog = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "varchar(64)".to_string()),
        ];
        assert_eq!(native_type_for(&catalog, "id"), Some("bigint"));
        assert_eq!(
            native_type_for(&catalog, "name"),
            Some("varchar(64)"),
            "`!=` returns the FIRST row that is not this column, so every mapping \
             is enriched with a neighbour's native type — a varchar reported as \
             bigint, in the schema every consumer downstream reads"
        );
        assert_eq!(
            native_type_for(&catalog, "absent"),
            None,
            "a column the catalog does not list has no native type; `!=` answers \
             `bigint` here, which is a type for a column that does not exist"
        );
        assert_eq!(native_type_for(&[], "id"), None);
    }

    /// A checkpoint that cannot be READ is not a checkpoint that is ABSENT.
    ///
    /// `Position::load` returns `Ok(None)` on `NotFound` — "first run, anchor here"
    /// — and propagates every other io error. `replace match guard e.kind() ==
    /// ErrorKind::NotFound with true` survived, and it collapses the distinction:
    /// a permissions error, a directory where a file belongs, a dead mount all
    /// become "no checkpoint", which re-anchors the stream at the CURRENT position
    /// and permanently skips everything written since. `status: success, rows: 0`.
    ///
    /// This is the same class the corrupt-JSON arm above it already refuses in
    /// prose, one line apart — it just had no test on the io side.
    #[test]
    fn an_unreadable_checkpoint_is_an_error_not_an_absent_one() {
        let dir = tempfile::tempdir().expect("temp dir");

        // ABSENT — the only case that may read as "first run".
        assert!(
            Position::load(&dir.path().join("nope.ckpt"))
                .expect("a missing checkpoint is not an error")
                .is_none(),
            "a file that is not there IS the first run"
        );

        // PRESENT and valid — the control, so "always error" cannot satisfy this.
        let ok = dir.path().join("ok.ckpt");
        std::fs::write(&ok, br#"{"lsn":"0/10"}"#).expect("write a checkpoint");
        assert!(Position::load(&ok).expect("valid").is_some());

        // UNREADABLE: a DIRECTORY where a file is expected. `read_to_string` fails
        // with `IsADirectory`/`InvalidInput`, never `NotFound` — so the guard is
        // what decides, and with it always true this returns `Ok(None)` and the
        // run re-anchors at NOW.
        let as_dir = dir.path().join("a_directory.ckpt");
        std::fs::create_dir(&as_dir).expect("create the impostor");
        let err = Position::load(&as_dir)
            .expect_err("a path that cannot be read must ERROR, not read as absent");
        let text = format!("{err:#}");
        assert!(
            text.contains("reading checkpoint") && text.contains("a_directory.ckpt"),
            "the error must name the file it failed to read, or an operator cannot \
             tell it from the corrupt-JSON case one line above: {text}"
        );
    }

    /// Both engine facts, EVERY variant — derived from the enum rather than typed
    /// in, so a fifth CDC engine cannot arrive without an answer here.
    ///
    /// These were `match` arms inside live-only dispatchers, and their mutants
    /// (`-> None`, `delete match arm`) survived: with `positional_mapping_warning`
    /// silenced, the one engine that CAN map by position stops warning about it,
    /// which is the exact class rounds 15-17 spent three rounds on.
    #[test]
    fn every_cdc_engine_answers_both_engine_facts_and_only_one_engine_answers_yes() {
        let all = [
            CdcEngine::Mysql,
            CdcEngine::Postgres,
            CdcEngine::Mssql,
            CdcEngine::Mongo,
        ];

        let positional: Vec<CdcEngine> = all
            .iter()
            .copied()
            .filter(|e| e.maps_by_position())
            .collect();
        assert_eq!(
            positional,
            vec![CdcEngine::Mysql],
            "MySQL is the ONLY engine whose wire format can omit column names — \
             `test_decoding` names every column, SQL Server's change tables are \
             relational, Mongo's events are documents. Answering `false` for MySQL \
             silences the warning on the one engine that needs it; answering `true` \
             elsewhere warns about something those engines cannot do."
        );

        let pinning: Vec<CdcEngine> = all
            .iter()
            .copied()
            .filter(|e| e.pins_log_for_reader())
            .collect();
        assert_eq!(
            pinning,
            vec![CdcEngine::Postgres],
            "a replication slot is the only anchor that makes the SERVER retain log \
             until the reader acks — which is why PostgreSQL fills a disk where the \
             others lose data to retention. Both errors are silent: `false` for PG \
             drops the WAL-growth warning, `true` elsewhere promises a retention \
             guarantee those engines do not give."
        );

        // MUTUALLY EXCLUSIVE, not a partition: SQL Server and MongoDB answer `false`
        // to both, and that is correct — their logs neither omit column names nor
        // wait on a reader. What must never happen is ONE engine claiming both,
        // which would mean a nameless wire format whose retention rivet also owns.
        for e in all {
            assert!(
                !(e.maps_by_position() && e.pins_log_for_reader()),
                "{e:?} claims both facts. No engine has a nameless image AND a \
                 reader-pinned log; an engine that did would need the positional \
                 warning and the WAL-growth warning to agree about the same events, \
                 and nothing in the sink arranges that."
            );
        }
        assert_eq!(
            all.iter().filter(|e| e.maps_by_position()).count()
                + all.iter().filter(|e| e.pins_log_for_reader()).count(),
            2,
            "exactly two of the four answer yes to exactly one fact each — a count \
             that would move the moment either predicate became `true` everywhere"
        );
    }

    /// A stream that resolves no catalog identity must say NOTHING, not a name.
    ///
    /// `ChangeStream::resolved_identity` defaults to `None` — "this adapter cannot
    /// resolve the configured string against a catalog". Four mutants replaced that
    /// with `Some((...))`, and each hands the router a fabricated schema/table pair
    /// for every adapter that does not override it. Names are labels; catalogs are
    /// truth, and a fabricated label routes events to a relation nobody asked for.
    #[test]
    fn an_adapter_that_resolves_no_identity_returns_none_not_a_fabricated_pair() {
        struct Unresolving;
        impl ChangeStream for Unresolving {
            fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
                None
            }
            fn ack(&mut self, _position: &Position) -> Result<()> {
                Ok(())
            }
            fn engine(&self) -> CdcEngine {
                CdcEngine::Mongo
            }
        }
        assert_eq!(
            Unresolving.resolved_identity("anything"),
            None,
            "the default must be `no answer`. Any `Some` here — including a pair of \
             empty strings — is a claim about a catalog the adapter never read."
        );
        assert_eq!(Unresolving.resolved_identity(""), None);
    }

    /// `__seq` is the intra-transaction ordinal the load's dedup sorts by, together
    /// with `__pos`. `stamp` is the one line that WRITES it, and `-> ()` survived:
    /// every event then keeps `seq = 0`, so `(__pos, __seq)` no longer orders the
    /// changes within a transaction and the warehouse can pick the wrong row as the
    /// winner for a key. Silent, and correct-looking at every count.
    #[test]
    fn the_sequence_stamp_writes_the_ordinal_onto_the_event() {
        let at = |lsn: &str| Position(serde_json::json!({ "lsn": lsn }));
        let ev = |lsn: &str| ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: None,
            position: at(lsn),
            committed: false,
            image_names: None,
            seq: 999, // a value the stamp must OVERWRITE, so `-> ()` cannot pass
            poison: None,
        };
        let mut seq = TxnSeq::default();

        // Three changes in ONE transaction: 0, 1, 2. Two would not distinguish
        // `counter += 1` from `counter = 1`.
        let mut a = ev("0/10");
        let mut b = ev("0/10");
        let mut c = ev("0/10");
        seq.stamp(&mut a);
        seq.stamp(&mut b);
        seq.stamp(&mut c);
        assert_eq!(
            (a.seq, b.seq, c.seq),
            (0, 1, 2),
            "the ordinal must be written onto the EVENT — leaving the constructor's \
             value there is what `stamp -> ()` does, and every row then sorts equal"
        );

        // A new commit position restarts the ordinal.
        let mut d = ev("0/20");
        seq.stamp(&mut d);
        assert_eq!(d.seq, 0, "a new transaction restarts the ordinal");
        let mut e = ev("0/20");
        seq.stamp(&mut e);
        assert_eq!(e.seq, 1);
    }

    /// `estimated_bytes` is the SUPPLIER of the sink's byte-cap accounting, whose
    /// consumer (`should_roll`) has a full unit matrix. Five mutants survived here
    /// — `+` -> `*`, `+` -> `-`, and the whole body — because nothing observed the
    /// value the consumer was handed. Same seam as the retry decider fed a `0`.
    #[test]
    fn the_byte_estimate_counts_both_images_the_names_and_the_overhead() {
        let mk = |before: Option<Vec<RivetValue>>, after: Option<Vec<RivetValue>>| ChangeEvent {
            op: ChangeOp::Update,
            schema: "sch".into(), // 3
            table: "tab".into(),  // 3
            before,
            after,
            position: Position(serde_json::json!({})),
            committed: false,
            image_names: None,
            seq: 0,
            poison: None,
        };
        let empty = mk(None, None).estimated_bytes();
        // The FIXED cost of a buffered change: the struct in the queue's backing
        // array, plus the names. No hardcoded total — `size_of` is what makes it
        // track the type instead of a number someone has to remember to update.
        assert_eq!(
            empty,
            std::mem::size_of::<ChangeEvent>() + 3 + 3,
            "an empty event still costs the struct itself — charging only the \
             PAYLOAD is how this estimate came to under-count a real event 12.7x, \
             which made `RIVET_CDC_MAX_TX_BYTES: 2 GiB` mean ~25 GiB of memory"
        );

        // The COMMIT POSITION is charged, and it is the dominant term: the framer
        // clones it onto every event of a transaction, and a one-key JSON object
        // costs a whole BTreeMap node (measured 475 B). An estimate that ignores it
        // is wrong by ~60% on every narrow event.
        let mut positioned = mk(None, None);
        positioned.position = Position(serde_json::json!({ "lsn": "0/16B2E00" }));
        assert!(
            positioned.estimated_bytes() > empty + 400,
            "the cloned commit position must be charged — it is 475 of the 772 \
             bytes a narrow event really costs"
        );

        // IMAGE NAMES are charged, amortised by how many events share the Arc.
        // PostgreSQL and SQL Server build a FRESH one per row, so on those engines
        // the names are real per-event memory; the first version of this estimate
        // skipped them on a comment asserting the Arc is always shared, which is
        // true of MySQL and Mongo only.
        let names: std::sync::Arc<[String]> =
            std::sync::Arc::from(["alpha", "beta"].map(String::from).to_vec());
        let mut exclusive = mk(None, None);
        exclusive.image_names = Some(names.clone());
        drop(names); // the event now holds the only reference — it pays in full
        let solo = exclusive.estimated_bytes();
        assert!(
            solo > empty + 2 * std::mem::size_of::<String>(),
            "an Arc held by ONE event is that event's memory and must be charged"
        );

        // Shared by many events, each pays a share — otherwise a transaction's
        // worth of events would each be charged the whole relation's names.
        let shared: std::sync::Arc<[String]> =
            std::sync::Arc::from(["alpha", "beta"].map(String::from).to_vec());
        let many: Vec<ChangeEvent> = (0..8)
            .map(|_| {
                let mut e = mk(None, None);
                e.image_names = Some(shared.clone());
                e
            })
            .collect();
        assert!(
            many[0].estimated_bytes() < solo,
            "the SAME names shared across events must cost each of them less than \
             an exclusive copy — charging in full would over-count by the \
             transaction's length, which is the opposite error and just as wrong"
        );

        // A Bytes value carrying ALLOCATOR SLACK is charged its capacity — the
        // Mongo shape: `serde_json::to_string`'s doubling growth leaves
        // capacity/len in (1, 2], and Mongo's whole event is one such cell, so
        // charging len under-counted a large-document stream up to 2x. The
        // exact-capacity engines are unaffected (capacity == len there).
        let mut slack = Vec::with_capacity(1024);
        slack.extend_from_slice(&[b'x'; 600]); // len 600, capacity 1024
        assert!(
            slack.capacity() > slack.len(),
            "the fixture must carry slack"
        );
        let with_slack = mk(None, Some(vec![RivetValue::Bytes(slack)])).estimated_bytes();
        let exact = mk(None, Some(vec![RivetValue::Bytes(vec![b'x'; 600])])).estimated_bytes();
        assert!(
            with_slack >= exact + 300,
            "resident must charge the CAPACITY ({with_slack} vs exact {exact}) — \
             len-charging is how a 256 MiB budget held ~512 MiB of documents"
        );

        // A nested ARRAY is charged its Vec's slots, like the top-level image —
        // a flat constant under-counted a 1000-element `integer[]` ~4x.
        let flat = mk(None, Some(vec![RivetValue::Int(1)])).estimated_bytes();
        let arr = mk(
            None,
            Some(vec![RivetValue::Array(vec![RivetValue::Int(1); 100])]),
        )
        .estimated_bytes();
        assert!(
            arr > flat + 100 * std::mem::size_of::<RivetValue>(),
            "an array's SLOTS cost as much as any other Vec's — PostgreSQL is the \
             engine that produces them and the one whose budget was blind to them"
        );

        // BOTH images count, and independently: a before-only event and an
        // after-only event of the same width must weigh the same, and an event
        // carrying both must weigh more than either.
        let one = vec![RivetValue::Int(1)];
        let b_only = mk(Some(one.clone()), None).estimated_bytes();
        let a_only = mk(None, Some(one.clone())).estimated_bytes();
        let both = mk(Some(one.clone()), Some(one.clone())).estimated_bytes();
        assert_eq!(b_only, a_only, "the two images are weighed the same way");
        assert!(b_only > empty, "a value must add to the estimate");
        assert_eq!(
            both - empty,
            2 * (b_only - empty),
            "both images are SUMMED — dropping either, or folding them with `*`, \
             makes the sink's memory ceiling describe a different event"
        );

        // More cells weigh more — a one-cell fixture cannot tell a sum from a max.
        let two = mk(None, Some(vec![RivetValue::Int(1), RivetValue::Int(2)])).estimated_bytes();
        assert!(two > a_only, "the per-cell estimates are summed, not maxed");
    }
}

#[cfg(test)]
mod setup_hint {
    /// Both directions, because only the pair says anything: a hint on everything
    /// is the bug, and a hint on nothing is the bug it would be replaced by.
    ///
    /// MEASURED live on the pg-cdc stand, both ways — a dropped slot with a
    /// checkpoint present now leads with `pg cdc: slot '…' is missing …`, and a
    /// `wal_level = replica` server still leads with the wal_level hint.
    #[test]
    fn a_setup_hint_is_added_to_driver_errors_and_withheld_from_rivets_own_verdicts() {
        let hint = super::PG_CDC_HINT;

        // A driver error says nothing actionable on its own — the hint IS the answer.
        let driver = anyhow::anyhow!("db error: ERROR: logical decoding requires wal_level >= lo");
        let wrapped = format!("{:#}", super::with_setup_hint(driver, hint));
        assert!(
            wrapped.starts_with("if this is a permissions/setup error"),
            "an opaque driver error must keep its setup hint, or removing the wrap \
             from rivet's verdicts would silently take it from the case it exists \
             for. Got: {wrapped}"
        );

        // rivet's own verdict is already the whole answer, and it is about DATA LOSS.
        let ours = anyhow::anyhow!(
            "pg cdc: slot 'x' is missing but a resume checkpoint exists — the changes \
             since then are no longer in the log. Re-snapshot the table (mode: full)"
        );
        let kept = format!("{:#}", super::with_setup_hint(ours, hint));
        assert!(
            kept.starts_with("pg cdc: slot 'x' is missing"),
            "rivet's own verdict must LEAD. Prefixed, the operator reads a wal_level \
             troubleshooting line while `re-snapshot, the data is gone` sits past a \
             colon at the end. Got: {kept}"
        );

        // The sink's engine-agnostic prefix counts too — it raises the partial-image
        // and arity refusals, which are not setup problems either.
        let sink =
            anyhow::anyhow!("cdc: rivet.t: a row image carries 1 value(s) under 3 column name(s)");
        assert!(
            format!("{:#}", super::with_setup_hint(sink, hint)).starts_with("cdc: rivet.t"),
            "an engine-agnostic sink refusal is rivet's verdict as much as an \
             engine-prefixed one"
        );
    }
}

#[cfg(test)]
mod tests {

    /// `--max-events` must not be able to WEDGE a checkpointed NDJSON run, and
    /// must still CAP.
    ///
    /// The two drivers of the same flag disagreed. The file sink defers the cap
    /// to a commit boundary (`max_events_stops_at_a_commit_boundary_never_inside_
    /// a_transaction`); this NDJSON loop broke on the event count alone. That
    /// loses nothing — the checkpoint save is gated on `committed`, so a cut
    /// transaction re-emits — but a transaction LONGER than the cap held no
    /// boundary to save, so every run re-read the same position, re-printed the
    /// same prefix, and stopped in the same place. `rivet cdc --checkpoint ck
    /// --max-events 100` against a 10k-row bulk load made no progress, ever.
    ///
    /// TWO transactions, deliberately. A single-transaction fixture cannot tell
    /// "stopped at the boundary" from "ran to the end of the stream", because its
    /// only commit IS the end — the `>=`→`<` mutant survived exactly that shape
    /// (CI mutation gate, PR #238). With a second transaction the saved position
    /// distinguishes them: stopping at tx1's boundary saves tx1's commit, running
    /// on saves tx2's.
    #[test]
    fn max_events_stops_at_the_first_boundary_past_the_cap_and_checkpoints_there() {
        use super::*;
        use std::collections::VecDeque;

        struct Fake(VecDeque<ChangeEvent>);
        impl ChangeStream for Fake {
            fn engine(&self) -> CdcEngine {
                CdcEngine::Postgres
            }

            fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
                self.0.pop_front().map(Ok)
            }
        }
        let ev = |id: i64, committed: bool| ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![crate::source::cdc::value::RivetValue::Int(id)]),
            position: Position(serde_json::json!({ "lsn": format!("{id:08X}") })),
            committed,
            image_names: None,
            seq: 0,
            poison: None,
        };
        // tx1 = 1,2,3 (boundary at 3) and tx2 = 4,5,6 (boundary at 6). The cap is
        // 2, so it lands INSIDE tx1 — the shape with no boundary to stop at.
        let mut stream = Fake(
            [
                ev(1, false),
                ev(2, false),
                ev(3, true),
                ev(4, false),
                ev(5, false),
                ev(6, true),
            ]
            .into_iter()
            .collect(),
        );

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("ck");
        run(&mut stream, Some(ckpt.clone()), Vec::new(), Some(2)).expect("run");

        assert!(
            ckpt.exists(),
            "a cap landing inside a transaction must still reach the boundary and \
             checkpoint — otherwise the next run resumes at the same place and the \
             export never progresses"
        );
        let saved = Position::load(&ckpt).expect("load").expect("some");
        assert_eq!(
            saved.0["lsn"], "00000003",
            "the cap must stop at tx1's COMMIT — 00000006 means it ran to the end of \
             the stream and capped nothing, 00000001/2 would mean it saved a \
             mid-transaction position"
        );
        assert_eq!(
            stream.0.len(),
            3,
            "tx2 must be left UNCONSUMED for the next run; draining it means the cap \
             stopped nothing"
        );
    }

    // The offline mutation guard for the DrainMode glue: both helpers are
    // otherwise exercised only through I/O paths (dispatch, cdc_job, adapter
    // opens), so an inverted mapping would survive the CI mutants gate's
    // `--lib` run.
    // Finding #3: MySQL CDC enriched ENUM/SET labels from
    // information_schema.COLUMNS pinned to `TABLE_SCHEMA = DATABASE()` while
    // dropping any `db.` qualifier — so a cross-database (or db-less-URL) capture
    // enriched nothing and every ENUM/SET column kept its raw wire index/bitmask.
    // enrich_schema_and_table must resolve a qualified name's OWN schema, not the
    // connection default. RED against the old `rsplit('.').next()` + DATABASE()
    // pinning (which yielded schema == default_db even for `otherdb.orders`).
    #[test]
    fn enrich_uses_the_qualified_schema_not_the_connection_default() {
        use super::enrich_schema_and_table;
        // Cross-database capture: the qualifier wins over the connection default.
        assert_eq!(
            enrich_schema_and_table("otherdb.orders", Some("conndb")),
            ("otherdb".to_string(), "orders".to_string())
        );
        // Unqualified: falls back to the connection's DATABASE().
        assert_eq!(
            enrich_schema_and_table("orders", Some("conndb")),
            ("conndb".to_string(), "orders".to_string())
        );
        // Db-less URL (DATABASE() is NULL) unqualified → empty schema (query
        // matches nothing, but never the WRONG database's same-named table).
        assert_eq!(
            enrich_schema_and_table("orders", None),
            (String::new(), "orders".to_string())
        );
    }

    #[test]
    fn drain_mode_maps_the_config_bool_and_bounds() {
        use super::DrainMode;
        assert_eq!(
            DrainMode::from_until_current(true),
            DrainMode::BoundedAtOpen
        );
        assert_eq!(DrainMode::from_until_current(false), DrainMode::Continuous);
        assert!(DrainMode::BoundedAtOpen.is_bounded());
        assert!(!DrainMode::Continuous.is_bounded());
    }

    /// Finding #43: `rivet init --mode cdc` scaffolds
    /// `checkpoint: ./cdc/<table>.ckpt`; the first save must create the
    /// parent, or every fresh quickstart dies on ENOENT dressed in the
    /// grants hint.
    #[test]
    fn corrupt_checkpoint_fails_loud_not_silently_absent() {
        // #99: a corrupt/truncated checkpoint must ERROR, never silently read as
        // absent — which let PG CDC treat a dropped slot as a fresh first run,
        // recreate it at 'current', and permanently skip changes. `.ok().flatten()`
        // at three sites (cdc_job resume_expected, PG + MSSQL create_change_stream)
        // swallowed it; Position::load now carries a clear corrupt-checkpoint error.
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("ck.json");
        std::fs::write(&path, b"{not valid json").unwrap();
        let err = Position::load(&path).unwrap_err();
        assert!(
            err.to_string().contains("corrupt or truncated"),
            "a corrupt checkpoint must fail loud, not read as absent: {err}"
        );

        // An absent checkpoint stays a clean first run (None).
        assert!(
            Position::load(&d.path().join("absent.json"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn mysql_corrupt_checkpoint_error_is_not_masked_by_the_grants_hint() {
        // #dogfood MED: MySQL's `Position::load` lived INSIDE open_or_resume,
        // wrapped by MYSQL_CDC_HINT — so a corrupt/truncated checkpoint was
        // reported as a binlog permissions/setup problem. The load is now hoisted
        // ABOVE the wrap (like PG/MSSQL), so the corrupt-checkpoint error surfaces
        // cleanly and NO network connect is attempted (the `?` returns first —
        // hence the unreachable port is never dialed). RED against the old code:
        // without the hoist the error carries the REPLICATION-SLAVE grants hint.
        let d = tempfile::tempdir().unwrap();
        let ckpt = d.path().join("ck.json");
        std::fs::write(&ckpt, b"{not valid json").unwrap();
        let cfg = CdcConfig {
            config_dir: std::path::PathBuf::from("."),
            url: "mysql://rivet:rivet@127.0.0.1:1/rivet".into(),
            checkpoint: Some(ckpt),
            drain: DrainMode::BoundedAtOpen,
            tls: None,
            engine: CdcEngineOpts::Mysql {
                server_id: 4321,
                configured_tables: Vec::new(),
            },
        };
        let err = match create_change_stream(&cfg, PeekBound::Unbounded) {
            Ok(_) => panic!("a corrupt checkpoint must error, not open a stream"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("corrupt or truncated"),
            "the error must be the clean corrupt-checkpoint message: {msg}"
        );
        assert!(
            !msg.contains("REPLICATION SLAVE") && !msg.contains("binlog_format"),
            "a checkpoint-file error must NOT carry the binlog-grants hint: {msg}"
        );
    }

    #[test]
    fn checkpoint_save_creates_missing_parent_directories() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("cdc").join("nested").join("orders.ckpt");
        let pos = Position(serde_json::json!({"file": "binlog.000001", "pos": 4}));
        pos.save(&path).expect("save must create parents");
        let loaded = Position::load(&path).unwrap().expect("roundtrip");
        assert_eq!(loaded.0["pos"], 4);
        // And the error context names the path, not the grants, when the
        // parent CANNOT be created (a file where the dir should be).
        let blocker = d.path().join("blocked");
        std::fs::write(&blocker, b"file").unwrap();
        let bad = blocker.join("x.ckpt");
        let err = pos.save(&bad).unwrap_err().to_string();
        assert!(
            err.contains("checkpoint directory"),
            "the failure must name the real cause: {err}"
        );
    }

    use super::*;

    // Ultrareview bug_001: the loud-fail-on-missing-anchor promise held only
    // for PostgreSQL. On MySQL/MSSQL a deleted checkpoint with prior-run
    // evidence behind it silently re-pinned at "current" (and on MSSQL that
    // pin actively destroys the min-LSN over-read floor). The bail must fire
    // BEFORE any connection — so this needs no live database.
    #[test]
    fn ensure_anchor_missing_checkpoint_with_evidence_fails_loudly() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("nonexistent.ckpt");
        for engine in [CdcEngine::Mysql, CdcEngine::Mssql] {
            let err = engine
                .ensure_anchor(
                    "mysql://u:p@127.0.0.1:1/db",
                    "unused",
                    Some(&missing),
                    None,
                    true, // resume evidence exists
                )
                .expect_err("missing checkpoint + evidence must bail, not re-pin");
            let msg = err.to_string();
            assert!(
                msg.contains("prior-run evidence"),
                "{engine:?}: must explain the evidence: {msg}"
            );
        }
    }

    #[test]
    fn resolve_cdc_columns_rejects_a_non_identifier_table() {
        // The table is interpolated into `SELECT * FROM {table}` for the schema
        // probe — a name carrying a quote / paren / semicolon / space must be
        // refused *before* any connection, so this needs no live database.
        for bad in ["orders; DROP TABLE x", "orders WHERE 1=1", "a b", "o'r", ""] {
            let err = resolve_cdc_columns(
                "mysql://u:p@127.0.0.1:3306/db",
                bad,
                None,
                &crate::types::ColumnOverrides::new(),
            )
            .expect_err(&format!("{bad:?} must be rejected"));
            assert!(
                err.to_string().contains("plain [schema.]table identifier"),
                "{bad:?} → {err}"
            );
        }
    }

    fn framer_ev(id: i64) -> super::ChangeEvent {
        super::ChangeEvent {
            op: super::ChangeOp::Insert,
            schema: "public".into(),
            table: "orders".into(),
            before: None,
            after: Some(vec![RivetValue::Int(id)]),
            position: Position(serde_json::json!({ "lsn": "stale" })),
            committed: false,
            image_names: None,
            seq: 0,
            poison: None,
        }
    }

    /// #158: the shared transaction close — commit position on ALL, committed on
    /// the LAST only. RED against a `committed:true`-on-every-event mutant (the
    /// exact shape that shipped the PG/MSSQL bugs).
    #[test]
    fn txn_framer_close_group_marks_only_the_last_committed() {
        let commit = Position(serde_json::json!({ "lsn": "COMMIT" }));

        // N-event group: every event gets the commit position; only the last
        // is committed.
        let mut g: Vec<super::ChangeEvent> = (0..4).map(framer_ev).collect();
        super::TxnFramer::close_group(&mut g, &commit);
        assert!(g.iter().all(|e| e.position == commit), "commit on all");
        assert_eq!(
            g.iter().map(|e| e.committed).collect::<Vec<_>>(),
            vec![false, false, false, true],
            "only the last event of a transaction is committed"
        );

        // 1-event group: that single event IS the boundary.
        let mut one = vec![framer_ev(9)];
        super::TxnFramer::close_group(&mut one, &commit);
        assert!(one[0].committed && one[0].position == commit);

        // Two groups back-to-back: each ends with exactly one committed.
        let c1 = Position(serde_json::json!({ "lsn": "C1" }));
        let c2 = Position(serde_json::json!({ "lsn": "C2" }));
        let mut a: Vec<super::ChangeEvent> = (0..2).map(framer_ev).collect();
        let mut b: Vec<super::ChangeEvent> = (0..3).map(framer_ev).collect();
        super::TxnFramer::close_group(&mut a, &c1);
        super::TxnFramer::close_group(&mut b, &c2);
        assert_eq!(a.iter().filter(|e| e.committed).count(), 1);
        assert_eq!(b.iter().filter(|e| e.committed).count(), 1);
        assert!(a.iter().all(|e| e.position == c1));
        assert!(b.iter().all(|e| e.position == c2));

        // Empty group: no-op, no panic.
        let mut empty: Vec<super::ChangeEvent> = Vec::new();
        super::TxnFramer::close_group(&mut empty, &commit);
        assert!(empty.is_empty());
    }

    /// Mongo's named single-event-commit model.
    #[test]
    fn txn_framer_single_event_commit_marks_the_event() {
        let mut e = framer_ev(1);
        assert!(!e.committed);
        super::TxnFramer::single_event_commit(&mut e);
        assert!(e.committed);
    }

    #[test]
    fn txn_seq_ordinals_reset_when_commit_position_changes() {
        // `position` (commit-scoped) alone ties every change in a transaction;
        // `__seq` restores intra-transaction order and RESETS when the commit
        // position changes — the reliable txn boundary on every engine (PG/MSSQL
        // mark every change `committed`, so `committed` can't be it).
        let mut ts = TxnSeq::default();
        let pa = Position(serde_json::json!({ "lsn": "A" })); // transaction A
        let pb = Position(serde_json::json!({ "lsn": "B" })); // transaction B
        // A = 3 changes, B = 2 changes.
        let seqs: Vec<u64> = [&pa, &pa, &pa, &pb, &pb]
            .iter()
            .map(|p| ts.next(p))
            .collect();
        assert_eq!(seqs, vec![0, 1, 2, 0, 1]);

        // Same position again after B still counts up within B.
        assert_eq!(ts.next(&pb), 2);
        // A new position resets.
        assert_eq!(ts.next(&Position(serde_json::json!({ "lsn": "C" }))), 0);
    }

    // ── NDJSON driver honours ChangeEvent.poison (silent-corruption guard) ──
    struct OneShot(Option<super::ChangeEvent>);
    impl super::ChangeStream for OneShot {
        fn engine(&self) -> super::CdcEngine {
            super::CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<super::ChangeEvent>> {
            self.0.take().map(Ok)
        }
    }

    fn poison_event(table: &str) -> super::ChangeEvent {
        super::ChangeEvent {
            op: super::ChangeOp::Update,
            schema: "public".into(),
            table: table.into(),
            before: None,
            after: Some(vec![RivetValue::Bytes(b"unchanged-toast-datum".to_vec())]),
            position: Position(serde_json::json!({ "lsn": "0/ABC" })),
            committed: true,
            image_names: None,
            seq: 0,
            poison: Some(
                "pg cdc: public.orders: column [big] unchanged-TOAST — REPLICA IDENTITY FULL"
                    .into(),
            ),
        }
    }

    // The NDJSON path (`rivet cdc` without --output) must surface a deferred poison
    // for a CAPTURED table — never print the raw `unchanged-toast-datum` sentinel as
    // data. RED against the pre-fix loop, which had no poison check and emitted it.
    #[test]
    fn ndjson_run_raises_poison_for_a_captured_table() {
        let mut s = OneShot(Some(poison_event("orders")));
        let err = super::run(&mut s, None, vec!["orders".into()], None)
            .expect_err("captured poison must bail");
        assert!(
            format!("{err:#}").contains("REPLICA IDENTITY FULL"),
            "got: {err:#}"
        );
    }

    // An UNCAPTURED table's poison must be dropped (parallel-slot contamination
    // fix): the run succeeds, never bailing on a table we do not capture.
    #[test]
    fn ndjson_run_drops_poison_for_an_uncaptured_table() {
        let mut s = OneShot(Some(poison_event("audit_log")));
        super::run(&mut s, None, vec!["orders".into()], None)
            .expect("uncaptured poison must not bail the NDJSON run");
    }

    // A stream that MUST NOT be consumed — `next_change` panics if polled.
    struct Forbidden;
    impl super::ChangeStream for Forbidden {
        fn engine(&self) -> super::CdcEngine {
            super::CdcEngine::Postgres
        }

        fn next_change(&mut self) -> Option<Result<super::ChangeEvent>> {
            panic!("run must not poll the stream when --max-events 0");
        }
    }

    // #dogfood LOW: `--max-events 0` emitted exactly ONE event — the cap was
    // checked AFTER the emit (`emitted += 1; if emitted >= 0 break`), an
    // off-by-one. It is now a true no-op: the early return means the stream is
    // never even polled. RED against the old loop, which would poll (→ panic).
    #[test]
    fn max_events_zero_is_a_true_no_op_never_polls_the_stream() {
        let mut s = Forbidden;
        super::run(&mut s, None, vec!["orders".into()], Some(0))
            .expect("--max-events 0 must be a clean no-op");
    }

    /// The metric's unit and the budget's unit are DIFFERENT, on purpose.
    ///
    /// One `estimated_bytes` feeding both was a measured regression: re-basing the
    /// estimate to resident cost silently inflated `bytes_read` ~13x on narrow
    /// rows and broke comparability with the batch path — the exact thing the
    /// metric exists for. This pins the split: payload counts the VALUES, resident
    /// counts the allocation, and the gap between them is the position clone +
    /// struct overhead that must never leak into "bytes read from the source".
    #[test]
    fn payload_bytes_counts_values_and_estimated_bytes_counts_memory() {
        let mut ev = ChangeEvent {
            op: ChangeOp::Insert,
            schema: "s".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(1), RivetValue::Bytes(vec![b'x'; 100])]),
            position: Position(serde_json::json!({ "lsn": "0/1" })),
            committed: false,
            image_names: None,
            seq: 0,
            poison: None,
        };
        let payload = ev.payload_bytes();
        assert_eq!(
            payload,
            1 + 1 + 8 + 100,
            "payload = schema + table + the values' own bytes, nothing else"
        );
        assert!(
            ev.estimated_bytes() > payload + 400,
            "resident must exceed payload by at least the position clone — if the \
             two converge, one of them changed meaning and a metric or a budget is \
             now lying"
        );
        // The position is RESIDENT cost, never payload: re-stamping it must not
        // move the metric's number.
        let before = ev.payload_bytes();
        ev.position = Position(serde_json::json!({ "lsn": "0/FFFFFFFF", "extra": "x" }));
        assert_eq!(
            ev.payload_bytes(),
            before,
            "the commit position is bookkeeping, not data read from the source"
        );
    }

    /// Spilling is OPT-IN, and every resolved location is CONFIG-anchored.
    ///
    /// The default is `None`: with no directory named the cap keeps its original
    /// meaning and REFUSES an oversized transaction. And nothing here is ever
    /// cwd-relative — the shipped image runs at `/` with a non-root user, where a
    /// cwd-anchored `.rivet/spill` worked for weeks and then failed with a
    /// path-less EACCES at the exact moment the cap was crossed; on a host it
    /// scattered per-invocation orphans no later sweep could find.
    #[test]
    fn spilling_is_off_until_a_directory_is_named() {
        use std::path::Path;
        let cfg = Path::new("/etc/rivet");
        let guard = EnvGuard::unset("RIVET_CDC_SPILL_DIR");
        assert_eq!(
            spill_dir_for(Some(Path::new("/var/lib/rivet/cdc.json")), cfg),
            None,
            "with nothing named, an oversized transaction must still FAIL — a \
             silent spill would remove the OOM guard and say nothing"
        );

        // FALSY values mean OFF, and `on` must be truthy because `off` is falsy:
        // an operator mirroring an accepted spelling must not get a directory
        // literally named `on`. These pairs are exactly what YAML-1.1 tooling
        // treats as booleans.
        for off in ["0", "false", "FALSE", "off", "no", "  "] {
            guard.set(off);
            assert_eq!(
                spill_dir_for(None, cfg),
                None,
                "`RIVET_CDC_SPILL_DIR={off:?}` must mean OFF, not a directory of \
                 that name"
            );
        }
        for on in ["1", " 1 ", "true", "yes", "ON"] {
            guard.set(on);
            assert_eq!(
                spill_dir_for(Some(Path::new("/var/lib/rivet/cdc.json")), cfg),
                Some(Path::new("/var/lib/rivet/.rivet-spill").to_path_buf()),
                "`{on:?}` is the switch, not a directory name"
            );
        }

        // Truthy with NO checkpoint: beside the CONFIG, never the cwd. This is
        // the common PostgreSQL shape (slot-anchored, no checkpoint) — the engine
        // whose transactions are largest must get an ABSOLUTE directory.
        guard.set("1");
        assert_eq!(
            spill_dir_for(None, cfg),
            Some(Path::new("/etc/rivet/.rivet/spill").to_path_buf()),
        );

        // An explicit ABSOLUTE path is used verbatim; a RELATIVE one anchors to
        // the config dir, exactly like `cdc.checkpoint:` — one config must not
        // read two locations depending on where the process started.
        guard.set("/mnt/big/spill");
        assert_eq!(
            spill_dir_for(Some(Path::new("/var/lib/rivet/cdc.json")), cfg),
            Some(Path::new("/mnt/big/spill").to_path_buf()),
        );
        guard.set("spill-here");
        assert_eq!(
            spill_dir_for(None, cfg),
            Some(Path::new("/etc/rivet/spill-here").to_path_buf()),
            "a relative explicit path anchors to the CONFIG's directory"
        );
    }

    /// Save/restore one env var around a test that must read it.
    struct EnvGuard {
        key: &'static str,
        prior: Option<String>,
    }

    impl EnvGuard {
        fn unset(key: &'static str) -> Self {
            let prior = std::env::var(key).ok();
            unsafe { std::env::remove_var(key) };
            Self { key, prior }
        }
        fn set(&self, v: &str) {
            unsafe { std::env::set_var(self.key, v) };
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prior {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }
}
