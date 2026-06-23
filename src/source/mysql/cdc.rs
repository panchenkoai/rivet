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
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};
use crate::source::require_tls_or_loopback;

/// A blocking iterator of canonical [`ChangeEvent`]s over a MySQL binlog stream.
///
/// A single binlog ROWS event carries many rows, so decoded changes are buffered
/// in `pending` and drained one at a time. A `TableMapEvent` (which always
/// precedes its ROWS event and carries the table identity + column metadata) is
/// cached by `table_id` and applied to the following rows. `file` tracks the
/// current binlog file across `ROTATE` events — it plus the event header's
/// position form the [`Position`] checkpoint.
pub(crate) struct MysqlChangeStream {
    stream: BinlogStream,
    tables: HashMap<u64, TableMapEvent<'static>>,
    pending: VecDeque<ChangeEvent>,
    /// The current transaction's rows, held until the `XID` (commit) event so the
    /// whole transaction is released atomically with the commit position.
    tx: Vec<ChangeEvent>,
    file: String,
}

impl MysqlChangeStream {
    /// Open a stream from an explicit `(binlog_file, pos)` coordinate.
    ///
    /// `non_block` ⇒ set `BINLOG_DUMP_NON_BLOCK`: the server streams all binlog up
    /// to the current end and then sends EOF instead of blocking for more. That
    /// turns MySQL into the same drain-and-exit poll model as PostgreSQL / SQL
    /// Server — a bounded "catch up to now and stop" run, ideal for a scheduler.
    pub(crate) fn open(
        url: &str,
        server_id: u32,
        file: String,
        pos: u64,
        non_block: bool,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let conn = connect_conn(url, tls)?;
        let mut req = BinlogRequest::new(server_id)
            .with_filename(file.clone().into_bytes())
            .with_pos(pos);
        if non_block {
            req = req.with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);
        }
        let stream = conn.get_binlog_stream(req)?;
        Ok(Self {
            stream,
            tables: HashMap::new(),
            pending: VecDeque::new(),
            tx: Vec::new(),
            file,
        })
    }

    /// Open a stream from the source's *current* position (`SHOW MASTER STATUS`).
    pub(crate) fn open_from_current(
        url: &str,
        server_id: u32,
        non_block: bool,
        tls: Option<&TlsConfig>,
    ) -> Result<Self> {
        let mut c = connect_conn(url, tls)?;
        let row: mysql::Row = c
            .query_first("SHOW MASTER STATUS")?
            .ok_or_else(|| anyhow::anyhow!("mysql: binlog disabled (SHOW MASTER STATUS empty)"))?;
        let file: String = row.get(0).expect("binlog file column");
        let pos: u64 = row.get(1).expect("binlog pos column");
        Self::open(url, server_id, file, pos, non_block, tls)
    }

    /// Resume from a persisted [`Position`] checkpoint, or start from the current
    /// position on the first run (no checkpoint yet). The MySQL position shape is
    /// `{"file": String, "pos": u64}`. `non_block` ⇒ drain to the current end and
    /// exit (see [`Self::open`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_or_resume(
        url: &str,
        server_id: u32,
        ckpt: Option<&Path>,
        non_block: bool,
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
            return Self::open(url, server_id, file, p, non_block, tls);
        }
        Self::open_from_current(url, server_id, non_block, tls)
    }

    /// Pull one binlog event and expand it into `pending`. `Ok(false)` ⇒ stream
    /// ended; `Ok(true)` ⇒ consumed an event.
    fn fill(&mut self) -> Result<bool> {
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
                self.tables.insert(tme.table_id(), tme.into_owned());
            }
            Some(EventData::RowsEvent(re)) => {
                let op = match &re {
                    RowsEventData::WriteRowsEvent(_) => ChangeOp::Insert,
                    RowsEventData::UpdateRowsEvent(_) => ChangeOp::Update,
                    RowsEventData::DeleteRowsEvent(_) => ChangeOp::Delete,
                    _ => return Ok(true),
                };
                let Some(tme) = self.tables.get(&re.table_id()).cloned() else {
                    return Ok(true); // started mid-stream, no TABLE_MAP yet — skip
                };
                let schema = tme.database_name().to_string();
                let table = tme.table_name().to_string();
                // Provisional position; rewritten to the commit position at XID.
                let position = Position(json!({ "file": self.file, "pos": log_pos }));
                for row in re.rows(&tme) {
                    let (before, after) = row?;
                    self.tx.push(ChangeEvent {
                        op,
                        schema: schema.clone(),
                        table: table.clone(),
                        before: before.map(render_row),
                        after: after.map(render_row),
                        position: position.clone(),
                        committed: false,
                    });
                }
                if self.tx.len() > MAX_TX_ROWS {
                    anyhow::bail!(
                        "mysql cdc: a single transaction buffered more than {MAX_TX_ROWS} rows \
                         before its commit — refusing to buffer unbounded (raise the cap only if \
                         a transaction this large is genuinely expected)"
                    );
                }
            }
            // XID = transaction commit. Stamp the commit position on every change
            // in the transaction and mark the last one committed, then release the
            // whole transaction atomically.
            Some(EventData::XidEvent(_)) => {
                let commit = Position(json!({ "file": self.file, "pos": log_pos }));
                let tx: Vec<ChangeEvent> = self.tx.drain(..).collect();
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
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => Ok(Conn::new(
            OptsBuilder::from_opts(Opts::from_url(url)?)
                .ssl_opts(Some(super::build_mysql_ssl_opts(cfg))),
        )?),
        _ => Ok(Conn::new(Opts::from_url(url)?)?),
    }
}

/// Memory backstop: a single transaction is buffered until its `XID`, so an
/// oversized (or crafted) transaction would grow `tx` unbounded. Cap it and bail
/// loudly rather than OOM. A real OLTP transaction is far below this.
const MAX_TX_ROWS: usize = 5_000_000;

/// Decode a binlog row's cells to typed [`RivetValue`]s (structural — no string
/// reparse of temporals).
fn render_row(r: mysql::binlog::row::BinlogRow) -> Vec<RivetValue> {
    r.unwrap().iter().map(binlog_value_to_rivet).collect()
}

fn binlog_value_to_rivet(bv: &BinlogValue) -> RivetValue {
    match bv {
        BinlogValue::Value(v) => RivetValue::from_mysql(v),
        // JSONB partial-update diffs (rare) — carry the debug bytes as text.
        other => RivetValue::Bytes(format!("{other:?}").into_bytes()),
    }
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

    const URL: &str = "mysql://rivet:rivet@127.0.0.1:3306/rivet";

    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn streams_typed_insert_update_delete() {
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_unit").unwrap();
        c.query_drop("CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)")
            .unwrap();

        let mut stream = MysqlChangeStream::open_from_current(URL, 4243, false, None).unwrap();
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
        let mut s = MysqlChangeStream::open_from_current(URL, 4244, false, None).unwrap();
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
            MysqlChangeStream::open_or_resume(URL, 4244, Some(&ckpt), false, None).unwrap();
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
