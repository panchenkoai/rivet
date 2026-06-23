//! MySQL CDC via binlog streaming (ROW format) — the simplest of the three
//! engines. The sync `mysql` crate (already a rivet dependency) performs the
//! replica handshake + event decoding under the `binlog` feature; this wraps its
//! `Event` stream into a [`ChangeEvent`] iterator that fits rivet's blocking
//! source loop (no async runtime, unlike `mysql_async`).
//!
//! This is the *reader seam* only — not yet wired to a `mode: cdc` / CLI. Source
//! prerequisites: `log_bin = ON`, `binlog_format = ROW` (ideally
//! `binlog_row_image = FULL` for complete UPDATE pre-images), a `REPLICATION
//! SLAVE` grant, and a `server_id` distinct from the source's.
#![allow(dead_code)] // reader seam; the CLI / mode wiring lands in a later increment.

use std::collections::{HashMap, VecDeque};

use mysql::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql::prelude::Queryable;
use mysql::{BinlogRequest, BinlogStream, Conn, Opts};

use crate::error::Result;

/// The DML kind of a row change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// One row-level change decoded from the binlog.
///
/// Cell values are rendered to owned `String`s for now — `BinlogValue` borrows
/// from the event buffer (can't outlive it), and the *typed* mapping
/// (`BinlogValue` → rivet's value system, honouring the UINT64 / DECIMAL /
/// naive-vs-instant-timestamp / JSON hazards) is the deferred "CDC semantics"
/// work, not the reader seam.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub op: ChangeOp,
    pub schema: String,
    pub table: String,
    /// Pre-image — present for `Update` and `Delete`.
    pub before: Option<Vec<String>>,
    /// Post-image — present for `Insert` and `Update`.
    pub after: Option<Vec<String>>,
}

/// A blocking iterator of [`ChangeEvent`]s over a MySQL binlog stream.
///
/// A single binlog ROWS event carries many rows, so decoded changes are buffered
/// in `pending` and drained one at a time. A `TableMapEvent` (which always
/// precedes its ROWS event and carries the table identity + column metadata) is
/// cached by `table_id` and applied to the following rows.
pub(crate) struct MysqlChangeStream {
    stream: BinlogStream,
    tables: HashMap<u64, TableMapEvent<'static>>,
    pending: VecDeque<ChangeEvent>,
}

impl MysqlChangeStream {
    /// Open a stream from an explicit `(binlog_file, pos)` coordinate — the
    /// resume path a real CDC run takes from its persisted checkpoint.
    pub(crate) fn open(url: &str, server_id: u32, file: Vec<u8>, pos: u64) -> Result<Self> {
        let conn = Conn::new(Opts::from_url(url)?)?;
        let req = BinlogRequest::new(server_id)
            .with_filename(file)
            .with_pos(pos);
        let stream = conn.get_binlog_stream(req)?;
        Ok(Self {
            stream,
            tables: HashMap::new(),
            pending: VecDeque::new(),
        })
    }

    /// Open a stream from the source's *current* position (`SHOW MASTER STATUS`)
    /// — tail only changes committed from now on.
    pub(crate) fn open_from_current(url: &str, server_id: u32) -> Result<Self> {
        let mut c = Conn::new(Opts::from_url(url)?)?;
        let row: mysql::Row = c
            .query_first("SHOW MASTER STATUS")?
            .ok_or_else(|| anyhow::anyhow!("mysql: binlog disabled (SHOW MASTER STATUS empty)"))?;
        let file: Vec<u8> = row.get(0).expect("binlog file column");
        let pos: u64 = row.get(1).expect("binlog pos column");
        Self::open(url, server_id, file, pos)
    }

    /// Pull one binlog event and expand it into `pending`. `Ok(false)` ⇒ stream
    /// ended; `Ok(true)` ⇒ consumed an event (which may or may not have produced
    /// change rows — e.g. a TABLE_MAP or a non-row event).
    fn fill(&mut self) -> Result<bool> {
        let ev = match self.stream.next() {
            Some(ev) => ev?,
            None => return Ok(false),
        };
        match ev.read_data()? {
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
                // The TABLE_MAP for this table_id must have arrived first; if not
                // (we started mid-stream), skip until we see it.
                let Some(tme) = self.tables.get(&re.table_id()).cloned() else {
                    return Ok(true);
                };
                let schema = tme.database_name().to_string();
                let table = tme.table_name().to_string();
                for row in re.rows(&tme) {
                    let (before, after) = row?;
                    self.pending.push_back(ChangeEvent {
                        op,
                        schema: schema.clone(),
                        table: table.clone(),
                        before: before
                            .map(|r| r.unwrap().iter().map(|v| format!("{v:?}")).collect()),
                        after: after.map(|r| r.unwrap().iter().map(|v| format!("{v:?}")).collect()),
                    });
                }
            }
            _ => {}
        }
        Ok(true)
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

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "mysql://rivet:rivet@127.0.0.1:3306/rivet";

    #[test]
    #[ignore = "live: requires docker compose mysql (binlog_format=ROW)"]
    fn streams_insert_update_delete() {
        let mut c = Conn::new(Opts::from_url(URL).unwrap()).unwrap();
        c.query_drop("DROP TABLE IF EXISTS cdc_unit").unwrap();
        c.query_drop("CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)")
            .unwrap();

        // Open at the current binlog position, THEN mutate — the changes tail in.
        let mut stream = MysqlChangeStream::open_from_current(URL, 4243).unwrap();
        c.query_drop("INSERT INTO cdc_unit VALUES (1, 10)").unwrap();
        c.query_drop("UPDATE cdc_unit SET v = 20 WHERE id = 1")
            .unwrap();
        c.query_drop("DELETE FROM cdc_unit WHERE id = 1").unwrap();

        let mut ops = Vec::new();
        for ev in stream.by_ref() {
            let ev = ev.unwrap();
            if ev.table == "cdc_unit" {
                ops.push(ev.op);
            }
            if ops.len() >= 3 {
                break;
            }
        }
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "binlog must yield INSERT, UPDATE, DELETE in commit order"
        );
    }
}
