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
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, Position};
use crate::source::require_tls_or_loopback;

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
    pub(crate) fn open(conn_str: &str, slot: &str, tls: Option<&TlsConfig>) -> Result<Self> {
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

    /// Poll the slot once into `pending` **without consuming it**
    /// (`pg_logical_slot_peek_changes`). The slot is only advanced later, in
    /// [`ChangeStream::ack`], once the changes are durably written — so a crash
    /// before durability re-reads them (at-least-once) instead of losing them.
    fn fill(&mut self) -> Result<()> {
        let rows = self.client.query(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, NULL)",
            &[&self.slot],
        )?;
        // Frame transactions (BEGIN … changes … COMMIT) and stamp every change with
        // its transaction's COMMIT LSN — that is the only valid slot-advance
        // boundary (advancing to a change's own mid-transaction LSN re-reads the
        // whole transaction) and the commit-boundary resume position. Logical
        // decoding only ever emits complete, committed transactions.
        let mut tx: Vec<ChangeEvent> = Vec::new();
        for r in rows {
            let lsn: String = r.get(0);
            let data: String = r.get(1);
            if data.starts_with("COMMIT") {
                let commit = Position(json!({ "lsn": lsn }));
                for mut ev in tx.drain(..) {
                    ev.position = commit.clone();
                    self.pending.push_back(ev);
                }
            } else if data.starts_with("BEGIN") {
                tx.clear();
            } else if let Some(ev) = parse_test_decoding(&lsn, &data) {
                tx.push(ev);
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

    /// Advance the slot's `confirmed_flush_lsn` to the last durably-written change
    /// — only now is it safe to let PostgreSQL free that WAL and skip those changes
    /// on the next peek. Called by the sink after a part commits.
    fn ack(&mut self, position: &Position) -> Result<()> {
        let lsn = position
            .0
            .get("lsn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("pg cdc ack: position missing 'lsn'"))?;
        // The postgres crate can't bind `&str` → `pg_lsn`, so the LSN is inlined.
        // It comes from the slot's own output; still validate it to the pg_lsn
        // charset (`[0-9A-Fa-f]+/[0-9A-Fa-f]+`) before interpolating — never trust
        // a value into SQL unchecked.
        if lsn.is_empty() || !lsn.bytes().all(|b| b.is_ascii_hexdigit() || b == b'/') {
            anyhow::bail!("pg cdc ack: refusing to advance to a malformed LSN {lsn:?}");
        }
        self.client.execute(
            &format!("SELECT pg_replication_slot_advance($1, '{lsn}'::pg_lsn)"),
            &[&self.slot],
        )?;
        Ok(())
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
    let cols = tail
        .split_once(": ")
        .map(|(_, c)| parse_columns(c))
        .unwrap_or_default();
    let (before, after) = match op {
        ChangeOp::Delete => (Some(cols), None),
        _ => (None, Some(cols)),
    };
    Some(ChangeEvent {
        op,
        schema,
        table,
        before,
        after,
        position: Position(json!({ "lsn": lsn })),
        // The slot only ever yields already-committed changes.
        committed: true,
    })
}

/// Parse a `test_decoding` column list (`name[type]:value name[type]:value …`)
/// into typed [`RivetValue`]s, in column order. Values are quoted with `''`
/// escaping or unquoted (numbers / `t`/`f` / `null`).
fn parse_columns(s: &str) -> Vec<RivetValue> {
    let mut out = Vec::new();
    let mut rest = s.trim_start();
    while !rest.is_empty() {
        let Some(lb) = rest.find('[') else { break };
        let Some(rel) = rest[lb..].find("]:") else {
            break;
        };
        let typ = &rest[lb + 1..lb + rel];
        let after_colon = &rest[lb + rel + 2..];
        let (val, quoted, consumed) = parse_value(after_colon);
        out.push(map_pg_value(typ, &val, quoted));
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
    if t == "date" {
        return chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map_or(RivetValue::Null, RivetValue::DateTime);
    }
    // text / varchar / char / uuid / json / … → string bytes.
    RivetValue::Bytes(val.as_bytes().to_vec())
}

/// Parse a PostgreSQL timestamp rendering (`YYYY-MM-DD HH:MM:SS[.ffffff][+TZ]`).
/// For `timestamptz` the offset is stripped to the instant's UTC wall-clock.
fn parse_pg_timestamp(val: &str) -> RivetValue {
    let naive = val
        .split('+')
        .next()
        .unwrap_or(val)
        .trim_end()
        .trim_end_matches('Z');
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

        // Slot must exist BEFORE the changes for them to be captured.
        let mut s = PgChangeStream::open(CONN, SLOT, None).unwrap();
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
