//! PostgreSQL CDC adapter — logical replication slot → canonical
//! [`cdc::ChangeEvent`].
//!
//! Consumes a `test_decoding` slot via `pg_logical_slot_get_changes()` with the
//! sync `postgres` crate rivet already depends on — the *poll* model (no
//! streaming-protocol crate; `START_REPLICATION` needs the immature
//! `pg_walstream`/`pgwire-replication` ecosystem). `next_change` polls the slot
//! once into a buffer and drains it; a continuous daemon wraps [`crate::source::cdc::run`]
//! in an outer poll loop.
//!
//! Pre-images / typed values are deferred — like MySQL was before its typed pass,
//! this adapter carries op + schema + table + position; the `test_decoding`
//! payload parse into typed before/after is the PostgreSQL completion step.
//!
//! Prereqs: `wal_level = logical`, a role with `REPLICATION`, `pg_hba.conf`
//! allowing it. Caveat: a logical slot **pins WAL** until consumed — an abandoned
//! slot fills the disk.
//!
//! `#![allow(dead_code)]`: consumed by `cli::dispatch` (binary crate); the lib
//! crate compiles `source` for tests but has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::VecDeque;

use postgres::{Client, NoTls};
use serde_json::json;

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};
use crate::source::require_tls_or_loopback;

/// Polls a logical slot and yields canonical changes.
pub(crate) struct PgChangeStream {
    client: Client,
    slot: String,
    pending: VecDeque<ChangeEvent>,
    /// Wire budget per `peek` — the memory bound of the drain (O(batch), not
    /// O(total backlog)). One ack cadence (the part rollover); see
    /// [`wire_budget`]. Slot progress past a foreign/empty span larger than one
    /// window is NOT this budget's job — it comes from the sink's re-drain loop
    /// acking the consumed span so the next peek slides forward (see
    /// [`crate::source::cdc::sink::run_to_files`]).
    batch_limit: i32,
    /// Largest COMMIT LSN already yielded THIS run. A refill re-peeks from the
    /// slot's (un-acked) `restart_lsn`, so any transaction at/below this was
    /// already delivered — it is dropped, making the refill idempotent.
    frontier: u64,
    /// A peek that yields no NEW transaction, or returns fewer than a full
    /// batch, has drained everything readable *from the current slot position*.
    /// It is NOT terminal for an acking consumer: [`ChangeStream::ack`] (and the
    /// zero-yield [`Self::release_empty_frontier`]) advance the slot and clear
    /// this, so the sink's re-drain loop peeks fresh WAL past a consumed
    /// foreign/empty span. Only a non-acking consumer (NDJSON, one big
    /// `Unbounded` peek) treats it as the end.
    exhausted: bool,
    /// Open-time COMMIT-LSN ceiling for a bounded run — the first transaction
    /// committing past it ends the stream; `None` (daemon / anchor-only open)
    /// keeps the pure catch-up exit. The contract lives on [`DrainMode`].
    bound: Option<u64>,
    /// Any DATA event pushed this run. When still `false` at clean exhaust,
    /// every frontier-covered transaction was EMPTY (DDL churn decodes as
    /// row-less BEGIN/COMMIT) — the sink has nothing to flush, so it never
    /// acks, and the slot would pin WAL behind the noise forever on an idle
    /// database. A zero-yield run releases the span itself
    /// ([`Self::release_empty_frontier`]): advancing past a data-free span can
    /// lose nothing by construction.
    yielded_data: bool,
    /// Rendered LSN of the last frontier advance — the zero-yield release
    /// target. `take()`n once at exhaust.
    frontier_text: Option<String>,
}

impl PgChangeStream {
    /// Connect and ensure a `test_decoding` logical slot named `slot` exists
    /// (idempotent — reuses an existing slot, which is how a real run resumes).
    ///
    /// `resume_expected` = a prior run's checkpoint exists. In that case a
    /// MISSING slot is a loud error, never a silent re-create: the slot was
    /// dropped or invalidated, and a fresh slot would anchor at the *current*
    /// position — silently skipping every change since the drop.
    ///
    /// A [`DrainMode::BoundedAtOpen`] run snapshots `pg_current_wal_lsn()` once
    /// and stops at the first commit past it — see [`Self::bound`].
    pub(crate) fn open(
        conn_str: &str,
        slot: &str,
        resume_expected: bool,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
    ) -> Result<Self> {
        // Same gate the batch path uses: refuse remote plaintext (CWE-319), and
        // use a verifying TLS connector when a TlsConfig is enforced.
        require_tls_or_loopback(conn_str, tls)?;
        let mut client = match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let connector = crate::source::tls::build_native_tls(cfg)?;
                Client::connect(
                    conn_str,
                    postgres_native_tls::MakeTlsConnector::new(connector),
                )?
            }
            _ => Client::connect(conn_str, NoTls)?,
        };
        // test_decoding renders values as TEXT in the polling SESSION's format, so
        // pin the formats this reader's parser assumes — otherwise a non-default
        // database `datestyle` (e.g. 'German, DMY') nulls every timestamp and a
        // non-hex `bytea_output` corrupts every bytea, silently (verified via the
        // source-parity sweep under a flipped session). Immune to the DB default.
        client.batch_execute(
            "SET datestyle = 'ISO, MDY'; SET bytea_output = 'hex'; SET intervalstyle = 'postgres';",
        )?;
        // A bounded run cannot work on a STANDBY: it pins its ceiling with
        // pg_current_wal_lsn() (unavailable during recovery) and a fresh run
        // creates the logical slot (also refused in recovery). Detect recovery
        // up front so the error names the fix, not whichever operation happens
        // to fail first (slot-create vs wal_lsn).
        if mode.is_bounded() {
            let in_recovery: bool = client.query_one("SELECT pg_is_in_recovery()", &[])?.get(0);
            if in_recovery {
                anyhow::bail!(
                    "bounded (until_current) CDC cannot run on a PostgreSQL standby — it is in \
                     recovery, where pg_current_wal_lsn() is unavailable and a logical slot cannot \
                     be created. Stream continuously (until_current: false) or point the source at \
                     the primary."
                );
            }
        }
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )?
            .get(0);
        if !exists {
            if resume_expected {
                anyhow::bail!(
                    "pg cdc: slot '{slot}' is missing but a resume checkpoint exists — the slot \
                     was dropped or invalidated, and the changes since then are no longer in the \
                     log. Re-snapshot the table (mode: full) and restart CDC from a fresh \
                     checkpoint (delete the checkpoint file to accept a new slot)."
                );
            }
            client.execute(
                "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&slot],
            )?;
        }
        // Snapshot the bound AFTER the slot exists, so a commit landing between
        // slot creation and this read is ≤ bound (captured this run, not lost
        // between the anchor and the ceiling). A malformed rendering falls back
        // to unbounded — pure catch-up — never an early exit.
        let bound = if mode.is_bounded() {
            use anyhow::Context as _;
            let lsn: String = client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .context(
                    "bounded (until_current) CDC pins its ceiling with pg_current_wal_lsn(), \
                     which is unavailable during recovery — on a standby, stream continuously \
                     (until_current: false) or point the source at the primary",
                )?
                .get(0);
            parse_lsn(&lsn)
        } else {
            None
        };
        Ok(Self {
            client,
            slot: slot.to_string(),
            pending: VecDeque::new(),
            batch_limit: wire_budget(peek),
            frontier: 0,
            exhausted: false,
            bound,
            yielded_data: false,
            frontier_text: None,
        })
    }

    /// Peek **one bounded batch** into `pending` **without consuming it**
    /// (`pg_logical_slot_peek_changes(slot, NULL, batch_limit)`). `upto_nchanges`
    /// caps the batch at a **commit boundary** (PostgreSQL only stops after a
    /// whole transaction), so memory is O(batch), never O(total backlog). The
    /// slot is advanced later, in [`ChangeStream::ack`], once the changes are
    /// durably written — a crash before durability re-reads them (at-least-once).
    ///
    /// Refill safety: a peek always starts at the slot's `restart_lsn`, which the
    /// consumer's ack advances between batches. Until that ack lands the same
    /// changes are visible again, so this drops any transaction whose COMMIT LSN
    /// is at or below [`Self::frontier`] (already yielded). A peek that adds no
    /// new transaction — or returns less than a full batch — has drained
    /// everything past the ack frontier and marks the stream [`Self::exhausted`].
    fn fill(&mut self) -> Result<()> {
        let rows = self.client.query(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, $2)",
            &[&self.slot, &self.batch_limit],
        )?;
        let n_rows = rows.len();
        // Frame transactions (BEGIN … changes … COMMIT) and stamp every change with
        // its transaction's COMMIT LSN — the only valid slot-advance boundary and
        // the commit-boundary resume position. Logical decoding only ever emits
        // complete, committed transactions.
        let mut tx: Vec<ChangeEvent> = Vec::new();
        let mut yielded_any = false;
        for r in rows {
            let lsn: String = r.get(0);
            let data: String = r.get(1);
            if data.starts_with("COMMIT") {
                let commit_lsn = parse_lsn(&lsn).unwrap_or(0);
                match tx_disposition(commit_lsn, self.frontier, self.bound) {
                    TxDisposition::Yield => {
                        if !tx.is_empty() {
                            self.yielded_data = true;
                        }
                        let commit = Position(json!({ "lsn": lsn }));
                        // `committed` marks the COMMIT BOUNDARY, and the sink
                        // only rolls (flush → checkpoint → ack) on a committed
                        // event — "never split a transaction across parts". Every
                        // event `parse_test_decoding` builds carries
                        // `committed: true`, but they all belong to ONE source
                        // transaction here, so mark ONLY THE LAST one committed
                        // (mirroring MySQL's XID model). Otherwise a transaction
                        // larger than `rollover` rolls + acks MID-transaction,
                        // and a crash between that ack and the tail's flush loses
                        // the un-flushed tail (the slot advanced past the commit,
                        // so resume never re-reads it — an at-least-once break).
                        let n = tx.len();
                        for (i, mut ev) in tx.drain(..).enumerate() {
                            ev.position = commit.clone();
                            ev.committed = i + 1 == n;
                            self.pending.push_back(ev);
                        }
                        self.frontier = commit_lsn;
                        self.frontier_text = Some(lsn.clone());
                        yielded_any = true;
                    }
                    // Already yielded on a prior (un-acked) peek ⇒ drop, idempotent.
                    TxDisposition::AlreadyYielded => tx.clear(),
                    // Committed after this bounded run opened — the next run's
                    // work. Peeks return transactions in commit order, so
                    // everything after this one is past the bound too: stop.
                    TxDisposition::PastBound => {
                        tx.clear();
                        self.exhausted = true;
                        break;
                    }
                }
            } else if data.starts_with("BEGIN") {
                tx.clear();
            } else if let Some(ev) = parse_test_decoding(&lsn, &data) {
                tx.push(ev);
                // Memory backstop, matching the MySQL adapter's MAX_TX_ROWS: a
                // transaction is buffered whole (never split across parts), so an
                // oversized one grows unbounded. `upto_nchanges` cannot split a
                // transaction, so `peek_changes` already materialised the whole
                // thing into `rows` — this bails loudly instead of compounding it
                // into `pending` + the sink buffer, and names the (upstream) fix.
                let cap = crate::source::cdc::max_tx_rows();
                if tx.len() > cap {
                    anyhow::bail!(
                        "pg cdc: a single transaction has more than {cap} rows — \
                         it must be buffered whole (a transaction is never split across parts), \
                         so this would exhaust memory. Split the source transaction, or raise \
                         the cap only if a transaction this large is genuinely expected."
                    );
                }
            }
        }
        // Short window (backlog fit in one peek) OR a full window that yielded
        // nothing new (every transaction in it was already yielded on a prior
        // un-acked peek — the slot is starved because the sink has not yet acked
        // past the consumed span): either way there is nothing more readable
        // from the CURRENT slot position. Mark exhausted and hand control back
        // to the sink. The sink's re-drain loop then flushes + acks the consumed
        // span (`run_to_files`), which advances the slot and clears `exhausted`,
        // so the NEXT peek slides past a foreign/empty span of ANY size — no
        // budget escalation, no premature "caught up" while in-bound data
        // remains (the bug the escalation only partially covered: a foreign or
        // empty span larger than the escalated window still exhausted early).
        if n_rows < self.batch_limit as usize || !yielded_any {
            self.exhausted = true;
        }
        Ok(())
    }

    /// Zero-yield release: called at clean exhaust. A run whose every
    /// frontier-covered transaction was EMPTY (see [`Self::yielded_data`])
    /// advances the slot itself — the sink will never ack (it has nothing to
    /// flush), and a data-free span has nothing to lose. A run that yielded
    /// data leaves acking to the sink (the flush→checkpoint→ack durability
    /// order); its trailing empty span becomes the NEXT run's zero-yield case
    /// and is released then. Failure here only delays WAL release — warn, never
    /// fail an otherwise-clean run.
    fn release_empty_frontier(&mut self) {
        if self.yielded_data {
            return;
        }
        let Some(lsn) = self.frontier_text.take() else {
            return;
        };
        if let Err(e) = self.advance_slot(&lsn) {
            log::warn!("pg cdc: could not release the empty-transaction span at {lsn}: {e:#}");
        }
    }

    /// Advance the slot's `confirmed_flush_lsn` to `lsn`, validated to the
    /// pg_lsn charset before interpolation — never trust a value into SQL
    /// unchecked, even the slot's own output. Advancing frees the WAL up to
    /// `lsn`, so the next peek starts THERE: clear `exhausted` so the sink's
    /// re-drain reads the fresh span instead of stopping (the slot moved, there
    /// may now be readable WAL that a prior starved peek could not reach).
    fn advance_slot(&mut self, lsn: &str) -> Result<()> {
        if lsn.is_empty() || !lsn.bytes().all(|b| b.is_ascii_hexdigit() || b == b'/') {
            anyhow::bail!("pg cdc: refusing to advance to a malformed LSN {lsn:?}");
        }
        // The postgres crate can't bind `&str` → `pg_lsn`, so the LSN is inlined.
        self.client.execute(
            &format!("SELECT pg_replication_slot_advance($1, '{lsn}'::pg_lsn)"),
            &[&self.slot],
        )?;
        self.exhausted = false;
        Ok(())
    }
}

/// Where one decoded transaction goes, given its COMMIT LSN — the pure heart of
/// the drain's termination contract (see [`PgChangeStream::bound`]).
#[derive(Debug, PartialEq)]
enum TxDisposition {
    /// New and in-bound — yield it and advance the frontier.
    Yield,
    /// At/below the frontier: an un-acked re-read from a prior peek — drop.
    AlreadyYielded,
    /// Past the open-time bound: the bounded run stops here; the next run's
    /// resume (from the un-advanced slot) picks it up.
    PastBound,
}

/// Bound check FIRST: a commit past the ceiling must stop the run even on the
/// very first peek (frontier still 0). A parse-miss `commit_lsn` of 0 can never
/// test past the bound — a malformed LSN delays termination, never loses data.
fn tx_disposition(commit_lsn: u64, frontier: u64, bound: Option<u64>) -> TxDisposition {
    if bound.is_some_and(|b| commit_lsn > b) {
        TxDisposition::PastBound
    } else if commit_lsn > frontier {
        TxDisposition::Yield
    } else {
        TxDisposition::AlreadyYielded
    }
}

/// Wire budget per peek: the sink's ack cadence (the part rollover), clamped to
/// the `pg_logical_slot_peek_changes` int4 arg. Slot progress past a span larger
/// than one window is the sink re-drain loop's job (ack → slide), not a bigger
/// budget's — so this is a flat 1×, and drain RSS stays O(rollover). Pure — an
/// offline mutation guard for the budget.
fn wire_budget(peek: crate::source::cdc::PeekBound) -> i32 {
    peek.rows_capped().min(i32::MAX as usize) as i32
}

/// Parse a `pg_lsn` rendering `X/Y` (two hex halves of a 64-bit position) into a
/// comparable `u64`. `None` on a malformed value — the frontier check then treats
/// it as `0` (never drops a real transaction on a parse miss).
fn parse_lsn(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    let hi = u32::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u32::from_str_radix(lo.trim(), 16).ok()?;
    Some((u64::from(hi) << 32) | u64::from(lo))
}

impl ChangeStream for PgChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        loop {
            // Refill a bounded batch whenever the buffer drains — the ack (from
            // the sink, after a durable part) has advanced the slot, so the next
            // peek reads fresh changes. `fill` marks `exhausted` once nothing new
            // remains readable from the current slot position.
            while self.pending.is_empty() && !self.exhausted {
                if let Err(e) = self.fill() {
                    return Some(Err(e));
                }
            }
            if !self.pending.is_empty() {
                return self.pending.pop_front().map(Ok);
            }
            // Exhausted with nothing to yield. A pure-empty span (DDL churn: many
            // row-less transactions) yields no events to the sink, so the sink's
            // re-drain loop never acks and would stop here — but the span may be
            // LARGER than one peek window. Release the empty prefix (advance the
            // slot past it, which clears `exhausted`), then LOOP to re-peek the
            // fresh WAL beyond it, walking the WHOLE empty span in one call rather
            // than one window per scheduler run. `release_empty_frontier` is a
            // no-op once any data was yielded (the sink owns acking then) or when
            // there is nothing left to release, and `frontier_text.take()` makes
            // it advance at most once per new window — so a run that cannot
            // advance falls through to `None` and the loop terminates.
            self.release_empty_frontier();
            if self.exhausted {
                // Release did not advance the slot (data was yielded, or the span
                // is fully drained) — genuinely nothing more.
                return None;
            }
        }
    }

    /// Advance the slot's `confirmed_flush_lsn` to the last durably-written change
    /// — only now is it safe to let PostgreSQL free that WAL and skip those changes
    /// on the next peek. Called by the sink after a part commits.
    fn ack(&mut self, position: &Position) -> Result<()> {
        let lsn = position
            .0
            .get("lsn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("pg cdc ack: position missing 'lsn'"))?
            .to_string();
        self.advance_slot(&lsn)
    }
}

/// Parse one `test_decoding` line into a canonical change, or `None` for the
/// `BEGIN`/`COMMIT` transaction markers and anything unrecognised. The line shape
/// is `table <schema>.<table>: <OP>: <columns…>`; pre-images / typed before-after
/// are deferred.
fn parse_test_decoding(lsn: &str, data: &str) -> Option<ChangeEvent> {
    let (qual, tail) = data.strip_prefix("table ")?.split_once(": ")?;
    let (schema, table) = match qual.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => (String::new(), qual.to_string()),
    };
    let op = if tail.starts_with("INSERT") {
        ChangeOp::Insert
    } else if tail.starts_with("UPDATE") {
        ChangeOp::Update
    } else if tail.starts_with("DELETE") {
        ChangeOp::Delete
    } else {
        return None;
    };
    // After `<OP>: ` comes the `col[type]:value …` list (all columns for
    // INSERT/UPDATE; the key for DELETE).
    let body = tail.split_once(": ").map(|(_, c)| c).unwrap_or("");
    // Finding #42: an UPDATE that changes the PRIMARY KEY renders BOTH
    // sections — `old-key: <cols> new-tuple: <cols>` — and a naive scan
    // glues them into one over-long image (the arity guard then bricks the
    // stream on a perfectly legal operation, permanently, with a misleading
    // "DDL" diagnosis). Split them: old-key → before, new-tuple → after.
    let (old_key_part, new_part) = match body.strip_prefix("old-key: ") {
        Some(rest) => match rest.split_once(" new-tuple: ") {
            Some((old, new)) => (Some(old), new),
            None => (None, rest),
        },
        None => (None, body),
    };
    let named = parse_columns(new_part);
    let old_named = old_key_part.map(parse_columns);
    let names: std::sync::Arc<[String]> = named.iter().map(|(n, _)| n.clone()).collect();
    let cols: Vec<RivetValue> = named.into_iter().map(|(_, v)| v).collect();
    // The wire text names EVERY column — carry the names for every op, so
    // the sink maps by NAME and the whole positional-corruption class
    // (findings #37/#41/#42) is unrepresentable on PostgreSQL.
    let (before, after, image_names) = match op {
        ChangeOp::Delete => (Some(cols), None, Some(names)),
        // A PK-changing UPDATE carries its old key too; the after-image is
        // the new tuple (its names). The old key rides `before`.
        ChangeOp::Update => (
            old_named.map(|o| o.into_iter().map(|(_, v)| v).collect()),
            Some(cols),
            Some(names),
        ),
        ChangeOp::Insert => (None, Some(cols), Some(names)),
    };
    Some(ChangeEvent {
        op,
        schema,
        table,
        before,
        after,
        image_names,
        position: Position(json!({ "lsn": lsn })),
        // Placeholder — `fill` overrides this, marking only the LAST event of the
        // transaction as the commit boundary (so the sink never rolls mid-tx).
        // Default `false` is the safe value: a stray event that bypassed `fill`
        // would not trigger a premature roll.
        committed: false,
        seq: 0, // stamped by TxnSeq as the stream is consumed
    })
}

/// Parse a `test_decoding` column list (`name[type]:value name[type]:value …`)
/// into typed [`RivetValue`]s, in column order. Values are quoted with `''`
/// escaping or unquoted (numbers / `t`/`f` / `null`).
fn parse_columns(s: &str) -> Vec<(String, RivetValue)> {
    let mut out = Vec::new();
    let mut rest = s.trim_start();
    while !rest.is_empty() {
        let Some(lb) = rest.find('[') else { break };
        let Some(rel) = rest[lb..].find("]:") else {
            break;
        };
        // The column NAME precedes '[' — it is DATA for key-only images
        // (finding #41): a DELETE's key must map by name, not position.
        let name = rest[..lb].trim().to_string();
        let typ = &rest[lb + 1..lb + rel];
        let after_colon = &rest[lb + rel + 2..];
        let (val, quoted, consumed) = parse_value(after_colon);
        out.push((name, map_pg_value(typ, &val, quoted)));
        rest = after_colon[consumed..].trim_start();
    }
    out
}

/// Parse one value at the start of `s`. Returns `(value, quoted, bytes_consumed)`.
fn parse_value(s: &str) -> (String, bool, usize) {
    let b = s.as_bytes();
    if b.first() != Some(&b'\'') {
        let end = s.find(' ').unwrap_or(s.len());
        return (s[..end].to_string(), false, end);
    }
    // quoted: copy chars, collapsing `''` → `'`, until the lone closing quote.
    let mut v = String::new();
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'\'' {
            if b.get(i + 1) == Some(&b'\'') {
                v.push('\'');
                i += 2;
            } else {
                return (v, true, i + 1);
            }
        } else {
            let n = utf8_len(b[i]);
            v.push_str(&s[i..i + n]);
            i += n;
        }
    }
    (v, true, i)
}

fn utf8_len(lead: u8) -> usize {
    match lead {
        b if b < 0x80 => 1,
        b if b >> 5 == 0b110 => 2,
        b if b >> 4 == 0b1110 => 3,
        _ => 4,
    }
}

/// Map a `test_decoding` `(type, value)` to a typed [`RivetValue`]. The column
/// type is explicit in the stream, so timestamp-vs-timestamptz is never guessed
/// (no naive-vs-instant hazard). Decimals carry exact text → `Decimal128`.
fn map_pg_value(typ: &str, val: &str, quoted: bool) -> RivetValue {
    if !quoted && val == "null" {
        return RivetValue::Null;
    }
    // One-dimensional arrays: `text[]` / `integer[]` / … render as the PG
    // array literal (`{a,"with,comma",NULL}`); parse to element values so the
    // sink builds a real List column (batch parity), never the literal text.
    if let Some(inner) = typ.strip_suffix("[]") {
        return parse_pg_array_literal(inner, val).map_or_else(
            || RivetValue::Bytes(val.as_bytes().to_vec()),
            RivetValue::Array,
        );
    }
    let t = typ;
    if t == "integer" || t == "bigint" || t == "smallint" || t == "oid" {
        return val.parse::<i64>().map_or(RivetValue::Null, RivetValue::Int);
    }
    if t.starts_with("numeric") || t.starts_with("decimal") {
        return RivetValue::Bytes(val.as_bytes().to_vec());
    }
    if t == "boolean" {
        return RivetValue::Bool(val == "t" || val == "true");
    }
    if t == "double precision" || t == "real" {
        return val
            .parse::<f64>()
            .map_or(RivetValue::Null, RivetValue::Float);
    }
    if t.starts_with("timestamp") {
        return parse_pg_timestamp(val);
    }
    if t == "time" || t == "time without time zone" {
        // "HH:MM:SS[.ffffff]" → microseconds since midnight (the Time64 column
        // the batch export uses; the text rendering would silently null there).
        return parse_pg_time_micros(val).map_or(RivetValue::Null, RivetValue::TimeMicros);
    }
    if t == "interval" {
        // Canonicalise the text rendering ("1 year 2 mons 3 days") to the SAME
        // ISO 8601 string the batch export emits ("P1Y2M3D") — one canon, so
        // CDC and batch outputs of the same value are byte-identical.
        return parse_pg_interval(val)
            .map(|(months, days, us)| {
                RivetValue::Bytes(
                    crate::source::postgres::arrow_convert::pg_interval_to_iso8601(
                        months, days, us,
                    )
                    .into_bytes(),
                )
            })
            .unwrap_or_else(|| RivetValue::Bytes(val.as_bytes().to_vec()));
    }
    if t == "date" {
        return chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map_or(RivetValue::Null, RivetValue::DateTime);
    }
    if t == "uuid" {
        // test_decoding renders the uuid as 36-char hyphenated text; the sink's
        // FixedSizeBinary(16) column (same as the batch export) needs the raw
        // 16 bytes — the text rendering would silently degrade to NULL there.
        return decode_hex(&val.replace('-', ""))
            .filter(|b| b.len() == 16)
            .map_or(RivetValue::Null, RivetValue::Bytes);
    }
    if t == "bytea" {
        // Rendered as `\x…` hex; a Binary column must carry the raw bytes, not
        // the hex string.
        if let Some(hex) = val.strip_prefix("\\x")
            && let Some(b) = decode_hex(hex)
        {
            return RivetValue::Bytes(b);
        }
        return RivetValue::Bytes(val.as_bytes().to_vec());
    }
    // text / varchar / char / json / … → string bytes.
    RivetValue::Bytes(val.as_bytes().to_vec())
}

/// Parse a PG array literal (`{alpha,"with,comma","he said \"hi\"",NULL}`)
/// into element values, mapped through [`map_pg_value`] with the element type.
/// Quoted elements un-escape `\"` and `\\`; the bare token `NULL` is an inner
/// NULL. `None` for anything that isn't a `{…}` literal (fail open to text).
fn parse_pg_array_literal(inner_type: &str, val: &str) -> Option<Vec<RivetValue>> {
    let body = val.strip_prefix('{')?.strip_suffix('}')?;
    if body.is_empty() {
        return Some(Vec::new());
    }
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i <= b.len() {
        if b.get(i) == Some(&b'"') {
            // Quoted element: copy until the closing quote, un-escaping.
            let mut elem = String::new();
            i += 1;
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' && i + 1 < b.len() {
                    elem.push(b[i + 1] as char);
                    i += 2;
                } else {
                    let n = utf8_len(b[i]);
                    elem.push_str(&body[i..i + n]);
                    i += n;
                }
            }
            i += 1; // closing quote
            out.push(map_pg_value(inner_type, &elem, true));
            if b.get(i) == Some(&b',') {
                i += 1;
            } else {
                break;
            }
        } else {
            let end = body[i..].find(',').map(|p| i + p).unwrap_or(body.len());
            let tok = &body[i..end];
            out.push(if tok == "NULL" {
                RivetValue::Null
            } else {
                map_pg_value(inner_type, tok, false)
            });
            if end == body.len() {
                break;
            }
            i = end + 1;
        }
    }
    Some(out)
}

/// Parse "HH:MM:SS[.ffffff]" into microseconds since midnight.
fn parse_pg_time_micros(val: &str) -> Option<i64> {
    let (hms, frac) = match val.split_once('.') {
        Some((h, f)) => (h, f),
        None => (val, ""),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(0..24).contains(&h) {
        return None;
    }
    let us: i64 = if frac.is_empty() {
        0
    } else {
        // Right-pad to 6 digits: ".5" ⇒ 500000 µs.
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    Some(((h * 3600 + m * 60 + s) * 1_000_000) + us)
}

/// Parse PostgreSQL's `postgres`-style interval text rendering —
/// `[N year(s)] [N mon(s)] [N day(s)] [±HH:MM:SS[.ffffff]]`, each part
/// optional — into `(months, days, microseconds)`.
fn parse_pg_interval(val: &str) -> Option<(i32, i32, i64)> {
    let (mut months, mut days, mut micros) = (0i32, 0i32, 0i64);
    let mut tokens = val.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if tok.contains(':') {
            // The time tail: ±HH:MM:SS[.ffffff].
            let (sign, rest) = match tok.strip_prefix('-') {
                Some(r) => (-1i64, r),
                None => (1i64, tok),
            };
            let t = parse_pg_time_micros_unbounded(rest)?;
            micros = sign * t;
            continue;
        }
        let n: i32 = tok.parse().ok()?;
        match tokens.next()? {
            u if u.starts_with("year") => months += n * 12,
            u if u.starts_with("mon") => months += n,
            u if u.starts_with("day") => days += n,
            _ => return None,
        }
    }
    Some((months, days, micros))
}

/// As [`parse_pg_time_micros`] but without the 24h bound — an interval's time
/// component may exceed a day (e.g. "25:00:00").
fn parse_pg_time_micros_unbounded(val: &str) -> Option<i64> {
    let (hms, frac) = match val.split_once('.') {
        Some((h, f)) => (h, f),
        None => (val, ""),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let us: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    Some(((h * 3600 + m * 60 + s) * 1_000_000) + us)
}

/// Decode an even-length hex string to bytes; `None` on any non-hex input.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Parse a PostgreSQL timestamp rendering (`YYYY-MM-DD HH:MM:SS[.ffffff][±TZ]`).
/// For `timestamptz` the trailing offset is DATA, not decoration: test_decoding
/// renders the instant in the polling session's zone, so at any non-UTC
/// session the offset is non-zero ('… 12:00:00+09') — convert to the UTC
/// instant. (The old code stripped '+…' and treated the wall-clock as UTC —
/// +9h corruption at a Tokyo session — and failed outright on negative
/// offsets, silently nulling every value at a western session.)
fn parse_pg_timestamp(val: &str) -> RivetValue {
    let v = val.trim_end();
    // tz-aware renderings first: %#z accepts +09 / +09:30 / +0930.
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(v, fmt) {
            return RivetValue::DateTime(dt.naive_utc());
        }
    }
    let naive = v.trim_end_matches('Z');
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(naive, fmt) {
            return RivetValue::DateTime(dt);
        }
    }
    RivetValue::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    // URL form (not key=value) so the require_tls_or_loopback gate recognises
    // 127.0.0.1 as loopback.
    // The `postgres-cdc` instance (cdc profile, :5434) — wal_level=logical.
    const CONN: &str = "postgresql://rivet:rivet@127.0.0.1:5434/rivet";
    const SLOT: &str = "rivet_cdc_test";

    // The until_current termination contract, as a pure matrix: the bound wins
    // over the frontier (stop even on the first peek), the frontier dedups
    // un-acked re-reads, and a parse-miss LSN (0) can never trip the bound —
    // a malformed rendering delays termination, never loses data.
    #[test]
    fn tx_disposition_bound_frontier_matrix() {
        use TxDisposition::*;
        // Unbounded (daemon / anchor-only): pure frontier behaviour.
        assert_eq!(tx_disposition(10, 0, None), Yield);
        assert_eq!(tx_disposition(10, 10, None), AlreadyYielded);
        assert_eq!(tx_disposition(9, 10, None), AlreadyYielded);
        // Bounded: at the bound is IN scope (committed before open), past is not.
        assert_eq!(tx_disposition(10, 0, Some(10)), Yield);
        assert_eq!(tx_disposition(11, 0, Some(10)), PastBound);
        // Bound wins over the frontier — even a would-be re-read stops the run.
        assert_eq!(tx_disposition(11, 11, Some(10)), PastBound);
        // Parse-miss commit (0) never trips the bound — it falls through to the
        // frontier path, same as the unbounded stream.
        assert_eq!(tx_disposition(0, 0, Some(10)), AlreadyYielded);
        assert_eq!(tx_disposition(0, 0, None), AlreadyYielded);
    }

    // The offline mutation guard for the peek-budget contract: the CI mutants
    // gate runs `--lib` only, so without this a clamp/cap mutant survives
    // everything but a live run.
    #[test]
    fn wire_budget_is_the_ack_cadence_clamped_to_int4() {
        use crate::source::cdc::PeekBound;
        assert_eq!(wire_budget(PeekBound::Sized(100_000)), 100_000);
        assert_eq!(wire_budget(PeekBound::Sized(0)), 1); // rows_capped clamps up
        assert_eq!(wire_budget(PeekBound::Unbounded), i32::MAX);
    }

    // Staff class #6 (generative fuzz, stable-toolchain flavour): the parsers
    // that face WIRE TEXT must never panic on arbitrary input — they return
    // Option/skip, loudly or silently, but never bring the stream down. The
    // timestamptz-offset and array-escape bugs were classic fuzz shapes; this
    // keeps a generative net under every future parser edit.
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        #[test]
        fn parse_test_decoding_never_panics(s in ".{0,200}") {
            let _ = parse_test_decoding("0/ABC", &s);
        }

        #[test]
        fn map_pg_value_never_panics(
            typ in "[a-z ]{1,20}(\\[\\])?",
            val in ".{0,120}",
            quoted in proptest::prelude::any::<bool>(),
        ) {
            let _ = map_pg_value(&typ, &val, quoted);
        }

        #[test]
        fn pg_timestamp_parse_total_and_offset_correct(
            h in 0u32..24, mi in 0u32..60, sec in 0u32..60,
            off_h in -12i32..=14, junk in ".{0,40}",
        ) {
            // Total on junk:
            let _ = parse_pg_timestamp(&junk);
            // Correct on every well-formed offset rendering:
            let rendered = format!("2024-06-15 {h:02}:{mi:02}:{sec:02}{off_h:+03}");
            if let RivetValue::DateTime(dt) = parse_pg_timestamp(&rendered) {
                let wall = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(h, mi, sec)
                    .unwrap();
                let expect = wall - chrono::Duration::hours(off_h as i64);
                proptest::prop_assert_eq!(dt, expect);
            } else {
                proptest::prop_assert!(false, "well-formed rendering must parse: {}", rendered);
            }
        }

        #[test]
        fn array_literal_roundtrips_arbitrary_text_elements(
            elems in proptest::collection::vec(
                proptest::option::of("[^\u{0}]{0,24}"), 0..6
            )
        ) {
            // Render the PG literal the way test_decoding would (quote +
            // escape every non-NULL element), parse, and require the exact
            // element vector back — inner NULLs included.
            let body: Vec<String> = elems
                .iter()
                .map(|e| match e {
                    None => "NULL".to_string(),
                    Some(t) => format!(
                        "\"{}\"",
                        t.replace('\\', "\\\\").replace('"', "\\\"")
                    ),
                })
                .collect();
            let lit = format!("{{{}}}", body.join(","));
            let parsed = parse_pg_array_literal("text", &lit)
                .expect("a rendered literal always parses");
            proptest::prop_assert_eq!(parsed.len(), elems.len());
            for (p, e) in parsed.iter().zip(&elems) {
                match (p, e) {
                    (RivetValue::Null, None) => {}
                    (RivetValue::Bytes(b), Some(t)) => {
                        proptest::prop_assert_eq!(b.as_slice(), t.as_bytes())
                    }
                    other => proptest::prop_assert!(false, "mismatch: {:?}", other),
                }
            }
        }
    }

    // Finding #42: a PK-changing UPDATE renders `old-key: … new-tuple: …`.
    // The naive scan glued both sections into one over-long after-image and
    // the arity guard then PERMANENTLY bricked the stream on a legal
    // operation. The after-image must be exactly the new tuple; the old key
    // rides `before`.
    #[test]
    fn pk_changing_update_splits_old_key_from_new_tuple() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 \
                    new-tuple: id[integer]:2 v[text]:'a'";
        let ev = parse_test_decoding("0/ABC", line).unwrap();
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(2), RivetValue::Bytes(b"a".to_vec())]),
            "after-image is the NEW tuple only"
        );
        assert_eq!(
            ev.before,
            Some(vec![RivetValue::Int(1)]),
            "the old key rides before"
        );
        // A normal (non-PK) update stays a plain after-image.
        let ev = parse_test_decoding("0/ABC", "table public.t: UPDATE: id[integer]:1 v[text]:'b'")
            .unwrap();
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(1), RivetValue::Bytes(b"b".to_vec())])
        );
        assert_eq!(ev.before, None);
    }

    // RED for finding #24 (non-UTC session): test_decoding renders timestamptz
    // in the POLLING SESSION's zone — at a Tokyo session '03:00Z' renders as
    // '2024-06-15 12:00:00+09'. The parser stripped the offset and treated the
    // wall-clock as UTC (+9h corruption); a NEGATIVE offset ('-05') was not
    // even stripped, so the parse failed and the value silently became NULL.
    // Every prior test ran the session at UTC, where the offset is always +00
    // and the bug is invisible.
    #[test]
    fn timestamptz_offset_is_data_not_decoration() {
        use chrono::NaiveDate;
        let cases = [
            ("2024-06-15 12:00:00+09", (2024, 6, 15, 3, 0, 0, 0)),
            ("2024-06-14 22:00:00-05", (2024, 6, 15, 3, 0, 0, 0)),
            (
                "2024-06-15 08:30:00.123456+05:30",
                (2024, 6, 15, 3, 0, 0, 123_456),
            ),
            ("2024-06-15 03:00:00+00", (2024, 6, 15, 3, 0, 0, 0)),
        ];
        for (rendered, (y, mo, d, h, mi, s, us)) in cases {
            let expected = NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, us)
                .unwrap();
            assert_eq!(
                parse_pg_timestamp(rendered),
                RivetValue::DateTime(expected),
                "offset must convert to the UTC instant for {rendered:?}"
            );
        }
    }

    // RED tests for the all-types matrix audit findings: TIME arrived as text
    // (the "timestamp" prefix check does not match "time without time zone"),
    // so the strict Time64 builder silently nulled every value; INTERVAL rode
    // as PostgreSQL's text rendering ("1 year 2 mons 3 days") while the batch
    // export canonicalises to ISO 8601 ("P1Y2M3D") — same value, two spellings,
    // breaking CDC↔batch parity.
    #[test]
    fn time_parses_to_micros_and_interval_canonicalises_to_iso8601() {
        let line = "table public.t: INSERT: \
                    t1[time without time zone]:'14:30:00.123456' \
                    iv1[interval]:'1 year 2 mons 3 days' \
                    iv2[interval]:'-1 years' \
                    iv3[interval]:'00:00:00' \
                    iv4[interval]:'3 days 04:05:06.789'";
        let ev = parse_test_decoding("0/ABC", line).unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[0],
            RivetValue::TimeMicros((14 * 3600 + 30 * 60) * 1_000_000 + 123456),
            "TIME must parse to microseconds-since-midnight"
        );
        assert_eq!(after[1], RivetValue::Bytes(b"P1Y2M3D".to_vec()));
        assert_eq!(after[2], RivetValue::Bytes(b"P-1Y".to_vec()));
        assert_eq!(after[3], RivetValue::Bytes(b"PT0S".to_vec()));
        assert_eq!(after[4], RivetValue::Bytes(b"P3DT4H5M6.789000S".to_vec()));
    }

    // RED test for the finding (caught live on a GCS export, by eye): a uuid
    // column rode through as its 36-char TEXT rendering, but the sink's
    // FixedSizeBinary(16) builder accepts only exactly-16-byte values and
    // silently degrades everything else to NULL — so 100% of the column was
    // lost while every count/sum check still passed. The parse must produce
    // the same raw 16 bytes the batch path produces. Same class: bytea rides
    // as its `\x…` hex TEXT — a Binary column would store the hex string.
    #[test]
    fn uuid_and_bytea_decode_to_raw_bytes_not_their_text_rendering() {
        let line = "table public.t: INSERT: \
                    u[uuid]:'0b0e0af9-27ec-4c33-b428-a01b27fdd576' \
                    b[bytea]:'\\x48656c6c6f'";
        let ev = parse_test_decoding("0/ABC", line).unwrap();
        let after = ev.after.unwrap();
        let RivetValue::Bytes(u) = &after[0] else {
            panic!("uuid must be Bytes, got {:?}", after[0]);
        };
        assert_eq!(
            u.len(),
            16,
            "uuid must be the raw 16 bytes, not 36-char text"
        );
        assert_eq!(
            u[..4],
            [0x0b, 0x0e, 0x0a, 0xf9],
            "uuid bytes must match the hyphenated hex"
        );
        assert_eq!(
            after[1],
            RivetValue::Bytes(b"Hello".to_vec()),
            "bytea must decode the \\x hex rendering to raw bytes"
        );
    }

    // Arrays parse to element values (a real List column downstream), never
    // the literal text — including the hostile shapes: commas inside quoted
    // elements, escaped quotes, inner NULLs, and the empty array.
    #[test]
    fn array_literals_parse_to_typed_elements() {
        let line = "table public.t: INSERT: \
                    tags[text[]]:'{alpha,\"with,comma\",\"he said \\\"hi\\\"\",NULL}' \
                    nums[integer[]]:'{1,NULL,3}' \
                    empty[text[]]:'{}'";
        let ev = parse_test_decoding("0/ABC", line).unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[0],
            RivetValue::Array(vec![
                RivetValue::Bytes(b"alpha".to_vec()),
                RivetValue::Bytes(b"with,comma".to_vec()),
                RivetValue::Bytes(b"he said \"hi\"".to_vec()),
                RivetValue::Null,
            ])
        );
        assert_eq!(
            after[1],
            RivetValue::Array(vec![
                RivetValue::Int(1),
                RivetValue::Null,
                RivetValue::Int(3),
            ])
        );
        assert_eq!(after[2], RivetValue::Array(Vec::new()));
    }

    #[test]
    fn parses_typed_columns_from_test_decoding() {
        let line = "table public.t: INSERT: id[integer]:1 name[text]:'alice o''brien' \
                    amount[numeric]:150.05 ts[timestamp without time zone]:'2026-06-23 11:58:01' \
                    flag[boolean]:t maybe[integer]:null";
        let ev = parse_test_decoding("0/ABC", line).unwrap();
        assert_eq!(ev.op, ChangeOp::Insert);
        assert_eq!(ev.table, "t");
        let after = ev.after.unwrap();
        assert_eq!(after[0], RivetValue::Int(1));
        assert_eq!(after[1], RivetValue::Bytes(b"alice o'brien".to_vec())); // '' → '
        assert_eq!(after[2], RivetValue::Bytes(b"150.05".to_vec())); // decimal text
        assert!(matches!(after[3], RivetValue::DateTime(_)));
        assert_eq!(after[4], RivetValue::Bool(true));
        assert_eq!(after[5], RivetValue::Null);
    }

    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn streams_insert_update_delete() {
        let mut admin = Client::connect(CONN, NoTls).unwrap();
        // Fresh slot so the test owns its watermark.
        admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .unwrap();

        // Slot must exist BEFORE the changes for them to be captured. No bound:
        // this test's changes are committed AFTER the open.
        let mut s = PgChangeStream::open(
            CONN,
            SLOT,
            false,
            None,
            crate::source::cdc::PeekBound::Sized(10_000),
            DrainMode::Continuous,
        )
        .unwrap();
        admin
            .batch_execute(
                "DROP TABLE IF EXISTS cdc_unit; CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)",
            )
            .unwrap();
        admin
            .batch_execute(
                "INSERT INTO cdc_unit VALUES (1, 10); \
                 UPDATE cdc_unit SET v = 20 WHERE id = 1; \
                 DELETE FROM cdc_unit WHERE id = 1",
            )
            .unwrap();

        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            let ev = ev.unwrap();
            if ev.table == "cdc_unit" {
                ops.push(ev.op);
            }
        }

        // cleanup before asserting (slot pins WAL).
        admin
            .execute("SELECT pg_drop_replication_slot($1)", &[&SLOT])
            .ok();

        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "logical slot must decode INSERT, UPDATE, DELETE in commit order"
        );
    }
}
