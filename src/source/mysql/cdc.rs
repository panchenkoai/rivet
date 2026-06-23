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
use mysql::{BinlogRequest, BinlogStream, Conn, Opts};
use serde_json::{Value as Json, json};

use crate::error::Result;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};

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
    file: String,
}

impl MysqlChangeStream {
    /// Open a stream from an explicit `(binlog_file, pos)` coordinate.
    pub(crate) fn open(url: &str, server_id: u32, file: String, pos: u64) -> Result<Self> {
        let conn = Conn::new(Opts::from_url(url)?)?;
        let req = BinlogRequest::new(server_id)
            .with_filename(file.clone().into_bytes())
            .with_pos(pos);
        let stream = conn.get_binlog_stream(req)?;
        Ok(Self {
            stream,
            tables: HashMap::new(),
            pending: VecDeque::new(),
            file,
        })
    }

    /// Open a stream from the source's *current* position (`SHOW MASTER STATUS`).
    pub(crate) fn open_from_current(url: &str, server_id: u32) -> Result<Self> {
        let mut c = Conn::new(Opts::from_url(url)?)?;
        let row: mysql::Row = c
            .query_first("SHOW MASTER STATUS")?
            .ok_or_else(|| anyhow::anyhow!("mysql: binlog disabled (SHOW MASTER STATUS empty)"))?;
        let file: String = row.get(0).expect("binlog file column");
        let pos: u64 = row.get(1).expect("binlog pos column");
        Self::open(url, server_id, file, pos)
    }

    /// Resume from a persisted [`Position`] checkpoint, or start from the current
    /// position on the first run (no checkpoint yet). The MySQL position shape is
    /// `{"file": String, "pos": u64}`.
    pub(crate) fn open_or_resume(url: &str, server_id: u32, ckpt: Option<&Path>) -> Result<Self> {
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
            return Self::open(url, server_id, file, p);
        }
        Self::open_from_current(url, server_id)
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
                let position = Position(json!({ "file": self.file, "pos": log_pos }));
                for row in re.rows(&tme) {
                    let (before, after) = row?;
                    self.pending.push_back(ChangeEvent {
                        op,
                        schema: schema.clone(),
                        table: table.clone(),
                        before: before.map(render_row),
                        after: after.map(render_row),
                        position: position.clone(),
                    });
                }
            }
            _ => {}
        }
        Ok(true)
    }
}

/// Render a binlog row's cells to JSON. (Candidate 2 routes this through the
/// `RivetType` pipeline for typed Arrow output.)
fn render_row(r: mysql::binlog::row::BinlogRow) -> Vec<Json> {
    r.unwrap().iter().map(binlog_value_to_json).collect()
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

/// Decode a binlog cell to JSON. Plain `Value`s map by type; JSONB partial-update
/// diffs (rare) are rendered textually for now.
fn binlog_value_to_json(bv: &BinlogValue) -> Json {
    match bv {
        BinlogValue::Value(v) => value_to_json(v),
        other => Json::String(format!("{other:?}")),
    }
}

/// Map a MySQL [`mysql::Value`] to JSON. Documented fidelity gaps: `Bytes` is
/// rendered utf8-lossy (text-vs-binary is ambiguous in the binlog without column
/// metadata), and a `UInt` above 2^53 loses precision in a JSON number — the same
/// value-mapping hazards rivet already tracks. Candidate 2 replaces this with the
/// `RivetType` pipeline.
fn value_to_json(v: &mysql::Value) -> Json {
    use mysql::Value;
    match v {
        Value::NULL => Json::Null,
        Value::Int(i) => (*i).into(),
        Value::UInt(u) => (*u).into(),
        Value::Float(f) => Json::from(*f as f64),
        Value::Double(d) => Json::from(*d),
        Value::Bytes(b) => Json::String(String::from_utf8_lossy(b).into_owned()),
        Value::Date(..) | Value::Time(..) => {
            Json::String(v.as_sql(false).trim_matches('\'').to_string())
        }
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

        let mut stream = MysqlChangeStream::open_from_current(URL, 4243).unwrap();
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
            Some([Json::from(1), Json::from(10)].as_slice())
        );
        assert_eq!(events[1].before.as_ref().unwrap()[1], Json::from(10));
        assert_eq!(events[1].after.as_ref().unwrap()[1], Json::from(20));
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
        let mut s = MysqlChangeStream::open_from_current(URL, 4244).unwrap();
        c.query_drop("INSERT INTO cdc_resume VALUES (1, 100)")
            .unwrap();
        let a = s
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_resume")
            .unwrap();
        assert_eq!(a.after.as_ref().unwrap()[0], Json::from(1));

        let dir = tempfile::tempdir().unwrap();
        let ckpt = dir.path().join("mysql.ckpt.json");
        a.position.save(&ckpt).unwrap();
        assert_eq!(Position::load(&ckpt).unwrap().as_ref(), Some(&a.position));
        drop(s);

        // A change made AFTER the checkpoint.
        c.query_drop("INSERT INTO cdc_resume VALUES (2, 200)")
            .unwrap();

        // Resuming from the checkpoint must yield B (id=2), never re-read A.
        let mut s2 = MysqlChangeStream::open_or_resume(URL, 4244, Some(&ckpt)).unwrap();
        let b = s2
            .by_ref()
            .map(|e| e.unwrap())
            .find(|e| e.table == "cdc_resume")
            .unwrap();
        assert_eq!(b.op, ChangeOp::Insert);
        assert_eq!(
            b.after.as_ref().unwrap()[0],
            Json::from(2),
            "resumed stream must start after the checkpoint, not re-read A"
        );
    }
}
