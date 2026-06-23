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

use crate::error::Result;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};

/// Polls a logical slot and yields canonical changes.
pub(crate) struct PgChangeStream {
    client: Client,
    slot: String,
    pending: VecDeque<ChangeEvent>,
    drained: bool,
}

impl PgChangeStream {
    /// Connect and ensure a `test_decoding` logical slot named `slot` exists
    /// (idempotent — reuses an existing slot, which is how a real run resumes).
    pub(crate) fn open(conn_str: &str, slot: &str) -> Result<Self> {
        let mut client = Client::connect(conn_str, NoTls)?;
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )?
            .get(0);
        if !exists {
            client.execute(
                "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&slot],
            )?;
        }
        Ok(Self {
            client,
            slot: slot.to_string(),
            pending: VecDeque::new(),
            drained: false,
        })
    }

    /// Poll the slot once (consuming it) into `pending`.
    /// `pg_logical_slot_get_changes` **advances** the slot — changes are gone
    /// after this returns, so the driver persists the position before re-polling.
    fn fill(&mut self) -> Result<()> {
        let rows = self.client.query(
            "SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, NULL)",
            &[&self.slot],
        )?;
        for r in rows {
            let lsn: String = r.get(0);
            let data: String = r.get(1);
            if let Some(ev) = parse_test_decoding(&lsn, &data) {
                self.pending.push_back(ev);
            }
        }
        Ok(())
    }
}

impl ChangeStream for PgChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        if self.pending.is_empty() && !self.drained {
            self.drained = true;
            if let Err(e) = self.fill() {
                return Some(Err(e));
            }
        }
        self.pending.pop_front().map(Ok)
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
    Some(ChangeEvent {
        op,
        schema,
        table,
        before: None,
        after: None,
        position: Position(json!({ "lsn": lsn })),
        // The slot only ever yields already-committed changes.
        committed: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONN: &str = "host=127.0.0.1 user=rivet password=rivet dbname=rivet";
    const SLOT: &str = "rivet_cdc_test";

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

        // Slot must exist BEFORE the changes for them to be captured.
        let mut s = PgChangeStream::open(CONN, SLOT).unwrap();
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
