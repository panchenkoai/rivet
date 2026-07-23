//! MySQL CDC adapter — binlog ROW stream → canonical [`cdc::ChangeEvent`].
//!
//! The sync `mysql` crate (already a rivet dependency) performs the replica
//! handshake + event decoding under the `binlog` feature; this adapts its
//! `Event` stream to the shared [`ChangeStream`] seam (no async runtime, unlike
//! `mysql_async`). The driver, checkpoint, and output live in
//! [`crate::source::cdc`]; this file is only the MySQL adapter.
//!
//! Source prerequisites: `log_bin = ON`, `binlog_format = ROW` (ideally
//! `binlog_row_image = FULL` for complete UPDATE pre-images), a `REPLICATION
//! SLAVE` grant, and a `server_id` distinct from the source's.
//!
//! `#![allow(dead_code)]`: the consumer is `cli::dispatch` (the `rivet cdc`
//! command) in the binary crate; the lib crate compiles `source` for tests but
//! has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::path::Path;

use mysql::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql::binlog::value::BinlogValue;
use mysql::prelude::Queryable;
use mysql::{BinlogDumpFlags, BinlogRequest, BinlogStream, Conn, Opts, OptsBuilder};
use serde_json::{Value as Json, json};

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};
use crate::source::require_tls_or_loopback;

/// A blocking iterator of canonical [`ChangeEvent`]s over a MySQL binlog stream.
///
/// A single binlog ROWS event carries many rows, so decoded changes are buffered
/// in `pending` and drained one at a time. A `TableMapEvent` (which always
/// precedes its ROWS event and carries the table identity + column metadata) is
/// cached by `table_id` and applied to the following rows. `file` tracks the
/// current binlog file across `ROTATE` events — it plus the event header's
/// position form the [`Position`] checkpoint.
/// One TABLE_MAP's decode context: the owned event + its parsed column names.
type CachedTableMap = (
    std::sync::Arc<TableMapEvent<'static>>,
    Option<std::sync::Arc<[String]>>,
);

pub(crate) struct MysqlChangeStream {
    stream: BinlogStream,
    /// TABLE_MAP cache: the event (Arc — rows-event decode borrows it) plus
    /// the column names parsed from its optional metadata ONCE. The first cut
    /// re-parsed the metadata and re-allocated every name on EVERY rows-event
    /// — thousands of identical parses inside a single large transaction.
    tables: HashMap<u64, CachedTableMap>,
    pending: VecDeque<ChangeEvent>,
    /// The current transaction's rows, held until the `XID` (commit) event so the
    /// whole transaction is released atomically with the commit position.
    tx: Vec<ChangeEvent>,
    /// Running byte footprint of `tx` (round-2 audit #9): the row cap alone is a
    /// poor bound when cells are large. Reset when `tx` is drained/cleared.
    tx_bytes: usize,
    file: String,
    /// Open-time `(binlog_file, pos)` ceiling for a bounded run — the first
    /// commit past it ends the stream (`BINLOG_DUMP_NON_BLOCK`'s EOF stays as
    /// the catch-up backstop, which backpressure alone can push out
    /// indefinitely); `None` (daemon) streams forever. The contract lives on
    /// [`DrainMode`].
    bound: Option<(String, u64)>,
    /// Bound tripped — the stream ended at the open-time ceiling. Sticky, so the
    /// sink's re-drain pass (which re-calls `next_change` after acking) returns
    /// `None` at once instead of consuming — and re-deferring — the past-bound
    /// events the next run will re-read from the checkpoint.
    past_bound: bool,
}

impl MysqlChangeStream {
    /// Open a stream from an explicit `(binlog_file, pos)` coordinate.
    ///
    /// A [`DrainMode::BoundedAtOpen`] run sets `BINLOG_DUMP_NON_BLOCK` (the
    /// server streams to the current end and sends EOF instead of blocking —
    /// the catch-up backstop) AND pins the open-time coordinates as the stop
    /// ceiling (see [`Self::bound`]).
    pub(crate) fn open(
        url: &str,
        server_id: u32,
        file: String,
        pos: u64,
        mode: DrainMode,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        // Snapshot the ceiling BEFORE the dump starts: every commit already in
        // the log is ≤ it, and anything racing in after is the next run's work.
        let bound = if mode.is_bounded() {
            Some(Self::current_coordinates(url, tls)?)
        } else {
            None
        };
        let conn = connect_conn(url, tls)?;
        let mut req = BinlogRequest::new(server_id)
            .with_filename(file.clone().into_bytes())
            .with_pos(pos);
        if mode.is_bounded() {
            req = req.with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);
        }
        let stream = conn.get_binlog_stream(req)?;
        Ok(Self {
            stream,
            tables: HashMap::new(),
            pending: VecDeque::new(),
            tx: Vec::new(),
            tx_bytes: 0,
            file,
            bound,
            past_bound: false,
        })
    }

    /// The source's current `(binlog_file, position)` coordinate.
    /// MySQL 8.4 REMOVED `SHOW MASTER STATUS` (finding #36, caught by the
    /// version scout); its replacement `SHOW BINARY LOG STATUS` does not exist
    /// before 8.2 — try the new form first, fall back to the old.
    fn current_coordinates(url: &str, tls: Option<&TlsConfig>) -> Result<(String, u64)> {
        let mut c = connect_conn(url, tls)?;
        let row: mysql::Row = match c.query_first("SHOW BINARY LOG STATUS") {
            Ok(Some(row)) => row,
            // The 8.2+ statement EXISTS and returned nothing ⇒ binlog is
            // definitively disabled — falling back would surface 8.4's
            // "SHOW MASTER STATUS" syntax error instead of the real cause.
            Ok(None) => anyhow::bail!("mysql: binlog disabled (binlog status empty)"),
            // Err ⇒ pre-8.2 server without the new statement — legacy form.
            Err(_) => c.query_first("SHOW MASTER STATUS")?.ok_or_else(|| {
                anyhow::anyhow!("mysql: binlog disabled (SHOW MASTER STATUS empty)")
            })?,
        };
        let file: String = row.get(0).expect("binlog file column");
        let pos: u64 = row.get(1).expect("binlog pos column");
        Ok((file, pos))
    }

    /// Persist the source's CURRENT position to `ckpt` — the anchor for
    /// `cdc.initial: snapshot` (taken BEFORE the snapshot read, so everything
    /// changed during the snapshot also appears in the stream). Idempotent-by-
    /// caller: only invoked when no checkpoint exists yet.
    pub(crate) fn pin_checkpoint_at_current(
        url: &str,
        ckpt: &Path,
        tls: Option<&TlsConfig>,
    ) -> Result<()> {
        let (file, pos) = Self::current_coordinates(url, tls)?;
        Position(serde_json::json!({ "file": file, "pos": pos })).save(ckpt)
    }

    /// Open a stream from the source's *current* position (`SHOW MASTER STATUS`).
    /// Test-only convenience: every production path goes through
    /// [`Self::open_or_resume`].
    #[cfg(test)]
    pub(crate) fn open_from_current(
        url: &str,
        server_id: u32,
        mode: DrainMode,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let (file, pos) = Self::current_coordinates(url, tls)?;
        Self::open(url, server_id, file, pos, mode, tls)
    }

    /// Resume from a persisted [`Position`] checkpoint, or start from the current
    /// position on the first run (no checkpoint yet). The MySQL position shape is
    /// `{"file": String, "pos": u64}`. Bounded/continuous per `mode` (see
    /// [`Self::open`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_or_resume(
        url: &str,
        server_id: u32,
        ckpt: Option<&Path>,
        mode: DrainMode,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        if let Some(path) = ckpt
            && let Some(pos) = Position::load(path)?
        {
            let file = pos
                .0
                .get("file")
                .and_then(Json::as_str)
                .ok_or_else(|| anyhow::anyhow!("mysql cdc checkpoint missing 'file'"))?
                .to_string();
            let p = pos
                .0
                .get("pos")
                .and_then(Json::as_u64)
                .ok_or_else(|| anyhow::anyhow!("mysql cdc checkpoint missing 'pos'"))?;
            return Self::open(url, server_id, file, p, mode, tls);
        }
        // First run (no checkpoint yet): anchor at the current position and persist
        // it IMMEDIATELY. PostgreSQL pins its anchor server-side at open (slot
        // creation); MySQL's only anchor is this file, and the sink writes it only
        // at a part commit — so an idle bounded run (zero changes drained) would
        // otherwise leave no checkpoint, the next run would re-anchor to a newer
        // "current" position, and every change in between would be silently skipped.
        let (file, pos) = Self::current_coordinates(url, tls)?;
        if let Some(path) = ckpt {
            Position(serde_json::json!({ "file": file, "pos": pos })).save(path)?;
        }
        Self::open(url, server_id, file, pos, mode, tls)
    }

    /// Pull one binlog event and expand it into `pending`. `Ok(false)` ⇒ stream
    /// ended; `Ok(true)` ⇒ consumed an event.
    fn fill(&mut self) -> Result<bool> {
        if self.past_bound {
            return Ok(false); // ended at the open-time ceiling — stay ended
        }
        let ev = match self.stream.next() {
            Some(ev) => ev?,
            None => return Ok(false),
        };
        let log_pos = ev.header().log_pos() as u64;
        match ev.read_data()? {
            // ROTATE → the next binlog file; subsequent positions are in it.
            Some(EventData::RotateEvent(re)) => {
                self.file = re.name().to_string();
            }
            Some(EventData::TableMapEvent(tme)) => {
                let tme = tme.into_owned();
                let names: Option<std::sync::Arc<[String]>> =
                    tme.iter_optional_meta().find_map(|f| match f {
                        Ok(mysql::binlog::events::OptionalMetadataField::ColumnName(names)) => {
                            Some(
                                names
                                    .iter_names()
                                    .filter_map(|n| n.ok())
                                    .map(|n| String::from_utf8_lossy(n.name_raw()).into_owned())
                                    .collect(),
                            )
                        }
                        _ => None,
                    });
                self.tables
                    .insert(tme.table_id(), (std::sync::Arc::new(tme), names));
            }
            Some(EventData::RowsEvent(re)) => {
                let op = match &re {
                    RowsEventData::WriteRowsEvent(_) => ChangeOp::Insert,
                    RowsEventData::UpdateRowsEvent(_) => ChangeOp::Update,
                    RowsEventData::DeleteRowsEvent(_) => ChangeOp::Delete,
                    _ => return Ok(true),
                };
                let Some((tme, image_names)) = self.tables.get(&re.table_id()).cloned() else {
                    return Ok(true); // started mid-stream, no TABLE_MAP yet — skip
                };
                let schema = tme.database_name().to_string();
                let table = tme.table_name().to_string();
                // Provisional position; rewritten to the commit position at XID.
                let position = Position(json!({ "file": self.file, "pos": log_pos }));
                for row in re.rows(&tme) {
                    let (before, after) = row?;
                    let ev = ChangeEvent {
                        op,
                        schema: schema.clone(),
                        table: table.clone(),
                        before: before.map(render_row),
                        after: after.map(render_row),
                        position: position.clone(),
                        committed: false,
                        image_names: image_names.clone(),
                        seq: 0, // stamped by TxnSeq as the stream is consumed
                        poison: None,
                    };
                    self.tx_bytes = self.tx_bytes.saturating_add(ev.estimated_bytes());
                    self.tx.push(ev);
                }
                let cap = crate::source::cdc::max_tx_rows();
                if self.tx.len() > cap {
                    anyhow::bail!(
                        "mysql cdc: a single transaction buffered more than {cap} rows \
                         before its commit — refusing to buffer unbounded (raise the cap only if \
                         a transaction this large is genuinely expected)"
                    );
                }
                // Round-2 audit #9: byte backstop — a few large-cell rows stay
                // under the row cap yet exhaust memory (MySQL streams the binlog
                // event-by-event, so this is the most load-bearing engine).
                let byte_cap = crate::source::cdc::max_tx_bytes();
                if self.tx_bytes > byte_cap {
                    anyhow::bail!(
                        "mysql cdc: a single transaction buffered more than {byte_cap} bytes \
                         (large cells) before its commit — refusing to buffer unbounded (raise \
                         RIVET_CDC_MAX_TX_BYTES only if a transaction this large is expected)"
                    );
                }
            }
            // XID = transaction commit. Stamp the commit position on every change
            // in the transaction and mark the last one committed, then release the
            // whole transaction atomically.
            Some(EventData::XidEvent(_)) => {
                // A commit past the open-time ceiling belongs to the next run:
                // end the stream WITHOUT releasing the transaction — the
                // checkpoint never advances past it, so the resume re-reads it
                // in full. `past_bound` is sticky so the sink's re-drain pass
                // returns `None` at once rather than consuming (and re-deferring)
                // more past-bound events.
                if commit_past_bound(&self.file, log_pos, self.bound.as_ref()) {
                    self.tx.clear();
                    self.tx_bytes = 0;
                    self.past_bound = true;
                    return Ok(false);
                }
                let commit = Position(json!({ "file": self.file, "pos": log_pos }));
                let tx: Vec<ChangeEvent> = self.tx.drain(..).collect();
                self.tx_bytes = 0;
                let n = tx.len();
                for (i, mut ev) in tx.into_iter().enumerate() {
                    ev.position = commit.clone();
                    ev.committed = i + 1 == n;
                    self.pending.push_back(ev);
                }
            }
            _ => {}
        }
        Ok(true)
    }
}

/// Connect a MySQL `Conn`, applying the same TLS gate the batch path uses —
/// refuse remote plaintext (CWE-319), and map an enforced `TlsConfig` to the
/// driver's SSL options (`super::build_mysql_ssl_opts`).
fn connect_conn(url: &str, tls: Option<&TlsConfig>) -> Result<Conn> {
    require_tls_or_loopback(url, tls)?;
    let mut conn = match tls {
        Some(cfg) if cfg.mode.is_enforced() => Conn::new(
            OptsBuilder::from_opts(Opts::from_url(url)?)
                .ssl_opts(Some(super::build_mysql_ssl_opts(cfg))),
        )?,
        _ => Conn::new(Opts::from_url(url)?)?,
    };
    // CDC streams the binlog (COM_BINLOG_DUMP) — a replication protocol that
    // ProxySQL / MaxScale / transaction-mode multiplexers do not carry. Detect the
    // proxy here and fail with the fix, not a cryptic dump-protocol error or a hang.
    let kind = super::proxy::detect_proxy_on_conn(&mut conn);
    if kind.is_proxy() {
        anyhow::bail!(
            "CDC cannot stream the binlog through {} — it does not proxy the MySQL \
             replication protocol (COM_BINLOG_DUMP). Point the source at the MySQL host \
             directly (the replication endpoint), not the proxy.",
            kind.log_label()
        );
    }
    Ok(conn)
}

/// Is the commit at `(file, pos)` PAST the open-time bound? — the pure heart of
/// the bounded run's termination contract (see [`MysqlChangeStream::bound`]).
/// Binlog files order by their numeric suffix (`binlog.000042`); a lexicographic
/// compare breaks at the 999999 → 1000000 width rollover, so parse the ordinal
/// (one server has one basename — rotation never changes it mid-stream). Fails
/// open: an unparseable name is never "past" — the `BINLOG_DUMP_NON_BLOCK` EOF
/// backstop still ends the run (delayed termination, never a dropped commit).
fn commit_past_bound(file: &str, pos: u64, bound: Option<&(String, u64)>) -> bool {
    let Some((bound_file, bound_pos)) = bound else {
        return false;
    };
    let (Some(cur), Some(bnd)) = (binlog_file_ordinal(file), binlog_file_ordinal(bound_file))
    else {
        return false;
    };
    cur > bnd || (cur == bnd && pos > *bound_pos)
}

/// The numeric suffix of a binlog file name (`mysql-bin.000042` → 42).
fn binlog_file_ordinal(name: &str) -> Option<u64> {
    name.rsplit_once('.')?.1.parse().ok()
}

/// Decode a binlog row's cells to typed [`RivetValue`]s (structural — no string
/// reparse of temporals).
fn render_row(r: mysql::binlog::row::BinlogRow) -> Vec<RivetValue> {
    r.unwrap().iter().map(binlog_value_to_rivet).collect()
}

fn binlog_value_to_rivet(bv: &BinlogValue) -> RivetValue {
    match bv {
        BinlogValue::Value(v) => RivetValue::from_mysql(v),
        // A JSON column arrives as MySQL's internal JSONB binary — convert it to
        // JSON text in the SAME rendering the server itself produces (", " and
        // ": " separators), so CDC and batch outputs of one value are
        // byte-identical; compact serde output would differ only in whitespace.
        BinlogValue::Jsonb(j) => match serde_json::Value::try_from(j.clone()) {
            Ok(json) => RivetValue::Bytes(mysql_style_json(&json).into_bytes()),
            Err(_) => RivetValue::Null,
        },
        // JSONB partial-update diffs (rare) — carry the debug bytes as text.
        other => RivetValue::Bytes(format!("{other:?}").into_bytes()),
    }
}

/// Serialise with MySQL's own JSON text spacing: `", "` between elements and
/// `": "` after keys (`{"n": 1, "tier": "gold"}`), which is what the batch
/// export reads from the server as text.
fn mysql_style_json(v: &serde_json::Value) -> String {
    struct MySqlFmt;
    impl serde_json::ser::Formatter for MySqlFmt {
        fn begin_array_value<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> std::io::Result<()> {
            if !first {
                w.write_all(b", ")?;
            }
            Ok(())
        }
        fn begin_object_key<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
            first: bool,
        ) -> std::io::Result<()> {
            if !first {
                w.write_all(b", ")?;
            }
            Ok(())
        }
        fn begin_object_value<W: ?Sized + std::io::Write>(
            &mut self,
            w: &mut W,
        ) -> std::io::Result<()> {
            w.write_all(b": ")
        }
    }
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, MySqlFmt);
    use serde::Serialize as _;
    if v.serialize(&mut ser).is_err() {
        return v.to_string();
    }
    String::from_utf8(out).unwrap_or_else(|_| v.to_string())
}

impl Iterator for MysqlChangeStream {
    type Item = Result<ChangeEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ev) = self.pending.pop_front() {
                return Some(Ok(ev));
            }
            match self.fill() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl ChangeStream for MysqlChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        self.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The `mysql-cdc` instance (cdc profile, :3307) — binlog + a REPLICATION grant.
    const URL: &str = "mysql://rivet:rivet@127.0.0.1:3307/rivet";

    // The until_current termination contract, as a pure matrix: at the bound is
    // in scope, past it (by pos or by a later file) is not, the file compare is
    // ordinal (survives the 999999 → 1000000 suffix-width rollover where a
    // lexicographic compare inverts), and unparseable names fail open.
    #[test]
    fn commit_past_bound_matrix() {
        let bound = ("binlog.000042".to_string(), 1000u64);
        // Daemon (no bound): never past.
        assert!(!commit_past_bound("binlog.000099", u64::MAX, None));
        // Same file: at the bound is in scope, one byte past is not.
        assert!(!commit_past_bound("binlog.000042", 999, Some(&bound)));
        assert!(!commit_past_bound("binlog.000042", 1000, Some(&bound)));
        assert!(commit_past_bound("binlog.000042", 1001, Some(&bound)));
        // Earlier / later file: pos is irrelevant.
        assert!(!commit_past_bound("binlog.000041", u64::MAX, Some(&bound)));
        assert!(commit_past_bound("binlog.000043", 4, Some(&bound)));
        // Suffix-width rollover: 1000000 > 999999 ordinally, though it is
        // lexicographically SMALLER ("1…" < "9…").
        let wide = ("mysql-bin.999999".to_string(), 1000u64);
        assert!(commit_past_bound("mysql-bin.1000000", 4, Some(&wide)));
        assert!(!commit_past_bound("mysql-bin.999999", 500, Some(&wide)));
        // Unparseable names fail open (never end the run early).
        assert!(!commit_past_bound("garbage", 99, Some(&bound)));
        assert!(!commit_past_bound(
            "binlog.000042",
            99,
            Some(&("garbage".to_string(), 0))
        ));
    }

    #[test]
    fn binlog_file_ordinal_parses_the_numeric_suffix() {
        assert_eq!(binlog_file_ordinal("mysql-bin.000042"), Some(42));
        assert_eq!(binlog_file_ordinal("binlog.1000000"), Some(1000000));
        // Dotted basenames take the LAST dot (rsplit).
        assert_eq!(binlog_file_ordinal("my.replica.000007"), Some(7));
        assert_eq!(binlog_file_ordinal("no-suffix"), None);
        assert_eq!(binlog_file_ordinal("binlog.notanum"), None);
    }

    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn streams_typed_insert_update_delete() {
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_unit").unwrap();
        c.query_drop("CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)")
            .unwrap();

        let mut stream =
            MysqlChangeStream::open_from_current(URL, 4243, DrainMode::Continuous, None).unwrap();
        c.query_drop("INSERT INTO cdc_unit VALUES (1, 10)").unwrap();
        c.query_drop("UPDATE cdc_unit SET v = 20 WHERE id = 1")
            .unwrap();
        c.query_drop("DELETE FROM cdc_unit WHERE id = 1").unwrap();

        let mut events = Vec::new();
        for ev in stream.by_ref() {
            let ev = ev.unwrap();
            if ev.table == "cdc_unit" {
                events.push(ev);
            }
            if events.len() >= 3 {
                break;
            }
        }

        let ops: Vec<ChangeOp> = events.iter().map(|e| e.op).collect();
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete]
        );
        // Typed values: the INSERT after-image is [1, 10] as JSON numbers.
        assert_eq!(
            events[0].after.as_deref(),
            Some([RivetValue::Int(1), RivetValue::Int(10)].as_slice())
        );
        assert_eq!(events[1].before.as_ref().unwrap()[1], RivetValue::Int(10));
        assert_eq!(events[1].after.as_ref().unwrap()[1], RivetValue::Int(20));
        // Every event carries a canonical position for checkpointing.
        assert!(
            events[2]
                .position
                .0
                .get("pos")
                .and_then(Json::as_u64)
                .unwrap()
                > 0
        );
    }

    // RED test for the idle-first-run hole: PostgreSQL pins its resume anchor
    // server-side at open (slot creation); MySQL has no server-side anchor — the
    // checkpoint file is the ONLY resume position, and it was written only at a
    // part commit. So a bounded first run that drains ZERO changes left no
    // checkpoint, the next run re-anchored to a newer "current" position, and
    // every change in between was silently skipped. Enable-CDC-during-a-quiet-
    // period is exactly the realistic ops sequence that hits this.
    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn first_run_with_zero_changes_pins_the_checkpoint_at_open() {
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_idle_first").unwrap();
        c.query_drop("CREATE TABLE cdc_idle_first (id INT PRIMARY KEY, v INT)")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("mysql.ckpt.json");

        // Run 1: checkpoint path configured, no file yet, nothing to drain
        // (BoundedAtOpen ⇒ bounded drain-and-exit, the scheduler model).
        let mut s = MysqlChangeStream::open_or_resume(
            URL,
            4245,
            Some(&ckpt),
            DrainMode::BoundedAtOpen,
            None,
        )
        .unwrap();
        assert!(
            s.by_ref()
                .map(|e| e.unwrap())
                .all(|e| e.table != "cdc_idle_first"),
            "run 1 must drain zero changes for this table"
        );
        drop(s);
        assert!(
            Position::load(&ckpt).unwrap().is_some(),
            "a first run with no prior checkpoint must pin the open position, \
             even when it captures nothing"
        );

        // A change made BETWEEN the idle run 1 and run 2.
        c.query_drop("INSERT INTO cdc_idle_first VALUES (1, 100)")
            .unwrap();

        // Run 2 must resume from the pinned position and capture it.
        let mut s2 = MysqlChangeStream::open_or_resume(
            URL,
            4245,
            Some(&ckpt),
            DrainMode::BoundedAtOpen,
            None,
        )
        .unwrap();
        let got = s2
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_idle_first");
        assert!(
            got.is_some(),
            "the change between an idle run and the next run must be captured, not skipped"
        );
    }

    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn resumes_from_checkpoint() {
        // Distinct table + server_id so this runs safely in parallel with the
        // other live test (cargo runs tests concurrently).
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_resume").unwrap();
        c.query_drop("CREATE TABLE cdc_resume (id INT PRIMARY KEY, v INT)")
            .unwrap();

        // Read change A and checkpoint at its position.
        let mut s =
            MysqlChangeStream::open_from_current(URL, 4244, DrainMode::Continuous, None).unwrap();
        c.query_drop("INSERT INTO cdc_resume VALUES (1, 100)")
            .unwrap();
        let a = s
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_resume")
            .unwrap();
        assert_eq!(a.after.as_ref().unwrap()[0], RivetValue::Int(1));

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("mysql.ckpt.json");
        a.position.save(&ckpt).unwrap();
        assert_eq!(Position::load(&ckpt).unwrap().as_ref(), Some(&a.position));
        drop(s);

        // A change made AFTER the checkpoint.
        c.query_drop("INSERT INTO cdc_resume VALUES (2, 200)")
            .unwrap();

        // Resuming from the checkpoint must yield B (id=2), never re-read A.
        let mut s2 =
            MysqlChangeStream::open_or_resume(URL, 4244, Some(&ckpt), DrainMode::Continuous, None)
                .unwrap();
        let b = s2
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_resume")
            .unwrap();
        assert_eq!(b.op, ChangeOp::Insert);
        assert_eq!(
            b.after.as_ref().unwrap()[0],
            RivetValue::Int(2),
            "resumed stream must start after the checkpoint, not re-read A"
        );
    }
}
