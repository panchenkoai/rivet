//! PostgreSQL CDC by consuming a logical replication slot via
//! `pg_logical_slot_get_changes()` — the *poll* model, using the sync `postgres`
//! crate rivet already depends on. No streaming-protocol crate
//! (`START_REPLICATION` needs the immature `pg_walstream`/`pgwire-replication`
//! ecosystem); polling the slot function reuses the existing connection and is
//! the simplest correct reader.
//!
//! `test_decoding` (built-in, no extension) gives readable text per change. A
//! production reader would prefer `pgoutput` (binary, typed via a Relation cache)
//! or `wal2json` (JSON), and likely the streaming protocol for lower latency —
//! both are the deferred "CDC semantics" work, not the reader seam.
//!
//! This is the *reader seam* only — not yet wired to a `mode: cdc` / CLI. Source
//! prerequisites: `wal_level = logical`, a role with the `REPLICATION` attribute,
//! `pg_hba.conf` allowing it. Caveat: a logical slot **pins WAL** on the server
//! until consumed/advanced — an abandoned slot fills the disk.
#![allow(dead_code)] // reader seam; the CLI / mode wiring lands in a later increment.

use postgres::{Client, NoTls};

use crate::error::Result;

/// The change kind, including the transaction boundaries `test_decoding` emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Begin,
    Commit,
    Insert,
    Update,
    Delete,
    Other,
}

/// One decoded change from the slot.
///
/// `raw` is the `test_decoding` payload (e.g.
/// `id[integer]:1 name[text]:'alice'`). The per-column typed parse into rivet's
/// value system is the deferred CDC-semantics work, not the reader seam.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub lsn: String,
    pub op: ChangeOp,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub raw: String,
}

/// Polls a logical slot for accumulated changes.
pub(crate) struct PgChangeStream {
    client: Client,
    slot: String,
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
        })
    }

    /// Consume and decode every change accumulated in the slot since the last
    /// poll. `pg_logical_slot_get_changes` **advances** the slot — the changes are
    /// gone after this returns, so persist downstream before the next poll.
    pub(crate) fn poll(&mut self) -> Result<Vec<ChangeEvent>> {
        let rows = self.client.query(
            "SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, NULL)",
            &[&self.slot],
        )?;
        Ok(rows
            .iter()
            .map(|r| parse_test_decoding(r.get(0), r.get(1)))
            .collect())
    }
}

/// Parse one `test_decoding` line: the leading `BEGIN`/`COMMIT`, or
/// `table <schema>.<table>: <OP>: <columns…>`.
fn parse_test_decoding(lsn: String, data: String) -> ChangeEvent {
    let mk = |op, schema, table| ChangeEvent {
        lsn: lsn.clone(),
        op,
        schema,
        table,
        raw: data.clone(),
    };
    if data.starts_with("BEGIN") {
        return mk(ChangeOp::Begin, None, None);
    }
    if data.starts_with("COMMIT") {
        return mk(ChangeOp::Commit, None, None);
    }
    if let Some((qual, tail)) = data.strip_prefix("table ").and_then(|r| r.split_once(": ")) {
        let (schema, table) = match qual.split_once('.') {
            Some((s, t)) => (Some(s.to_string()), Some(t.to_string())),
            None => (None, Some(qual.to_string())),
        };
        let op = if tail.starts_with("INSERT") {
            ChangeOp::Insert
        } else if tail.starts_with("UPDATE") {
            ChangeOp::Update
        } else if tail.starts_with("DELETE") {
            ChangeOp::Delete
        } else {
            ChangeOp::Other
        };
        return mk(op, schema, table);
    }
    mk(ChangeOp::Other, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONN: &str = "host=127.0.0.1 user=rivet password=rivet dbname=rivet";
    const SLOT: &str = "rivet_cdc_test";

    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn poll_captures_insert_update_delete() {
        let mut admin = Client::connect(CONN, NoTls).unwrap();
        // Fresh slot so the test owns its watermark.
        admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .unwrap();

        let mut s = PgChangeStream::open(CONN, SLOT).unwrap();
        let _ = s.poll().unwrap(); // drain anything between create and now

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

        let ops: Vec<ChangeOp> = s
            .poll()
            .unwrap()
            .into_iter()
            .filter(|e| e.table.as_deref() == Some("cdc_unit"))
            .map(|e| e.op)
            .collect();

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
