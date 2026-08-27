//! The `pgoutput` logical-decoding message format — PURE bytes-in, message-out.
//!
//! Why this exists at all: `test_decoding`, which this replaces, is documented by
//! PostgreSQL as an EXAMPLE output plugin whose format is explicitly not an API.
//! Building on it meant owning a 278-line text parser AND inheriting a defect class
//! that cannot exist here — measured on the stand, all three found this session:
//!
//! | | test_decoding | pgoutput |
//! | --- | --- | --- |
//! | NULL | the string `NULL`, indistinguishable from data | its own tag `n` |
//! | unchanged TOAST | the string `unchanged-toast-datum`, likewise | its own tag `u` |
//! | column types | guessed from `[integer]` in the text | an OID per column, from `Relation` |
//! | key columns | not reported | a flag per column |
//! | session state | `datestyle`/`bytea_output`/TimeZone shape the text | none in binary mode |
//!
//! The module is deliberately I/O-free: it takes the bytes a caller already has and
//! returns a typed message. That makes every decision in it offline-gradeable,
//! which is the whole reason the old parser's siblings kept shipping ungraded.
//!
//! Layouts are PostgreSQL's "Logical Streaming Replication Protocol" chapter. Every
//! integer is big-endian; strings are NUL-terminated.

#![allow(dead_code)]
// ^ Nothing in the non-test build calls this YET: the commit that adds a decoder
// and the commit that swaps the reader are deliberately separate, because a pure
// decoder is fully gradeable offline and a reader swap is not. The unit tests do
// exercise every line of it against real wire bytes; `--all-targets` clippy counts
// only the lib target, hence the allow. It comes OUT with the reader swap — if it
// is still here after that, this module is dead and should be deleted, not kept.

use crate::error::Result;

/// One cell of a row image. The tag is the protocol's, not ours — which is the
/// point: `Null` and `ToastUnchanged` are values the wire distinguishes, where
/// `test_decoding` rendered both as text a real column could also hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cell {
    Null,
    /// The column was not transmitted because its TOASTed value did not change.
    /// NOT a value — a caller that treats it as one writes the marker into data.
    ToastUnchanged,
    /// `t` — the type's TEXT output. Present when the slot was read without
    /// `binary`, and session-state-shaped, so prefer binary.
    Text(Vec<u8>),
    /// `b` — the type's BINARY output, decodable with `postgres_types::FromSql`
    /// against the OID `Relation` gave for this column.
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Column {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    /// Part of the replica identity — the wire says so, so rivet never has to ask
    /// the catalog which columns identify a row.
    pub is_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    Begin {
        final_lsn: u64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
    },
    Relation {
        relation_id: u32,
        namespace: String,
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        relation_id: u32,
        after: Vec<Cell>,
    },
    Update {
        relation_id: u32,
        /// Present only when the table's replica identity supplies one (`FULL`, or
        /// `DEFAULT` when a key column changed). `None` is not "no change" — it is
        /// "the server sent no pre-image", which is a different fact.
        before: Option<Vec<Cell>>,
        after: Vec<Cell>,
    },
    Delete {
        relation_id: u32,
        /// The key under `DEFAULT`, the whole row under `FULL`.
        before: Vec<Cell>,
    },
    Truncate {
        relation_ids: Vec<u32>,
    },
    /// `O`rigin, `Y` type, and the streaming variants. Named rather than dropped so
    /// a caller can decide; silently skipping an unknown message is how a protocol
    /// bump becomes a silent gap.
    Other(char),
}

struct Reader<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.i + n > self.b.len() {
            anyhow::bail!(
                "pgoutput: message truncated — wanted {n} byte(s) at offset {}, have {}",
                self.i,
                self.b.len()
            );
        }
        let s = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("2")))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8")))
    }
    /// A NUL-terminated string. Refuses an unterminated one rather than reading to
    /// the end of the message — a missing NUL means the layout is not what we think.
    fn cstr(&mut self) -> Result<String> {
        let start = self.i;
        while self.i < self.b.len() && self.b[self.i] != 0 {
            self.i += 1;
        }
        if self.i >= self.b.len() {
            anyhow::bail!("pgoutput: unterminated string at offset {start}");
        }
        let s = String::from_utf8_lossy(&self.b[start..self.i]).into_owned();
        self.i += 1;
        Ok(s)
    }
    fn tuple(&mut self) -> Result<Vec<Cell>> {
        let n = self.u16()? as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(match self.u8()? {
                b'n' => Cell::Null,
                b'u' => Cell::ToastUnchanged,
                tag @ (b't' | b'b') => {
                    let len = self.i32()?;
                    if len < 0 {
                        anyhow::bail!("pgoutput: negative value length {len}");
                    }
                    let raw = self.take(len as usize)?.to_vec();
                    if tag == b'b' {
                        Cell::Binary(raw)
                    } else {
                        Cell::Text(raw)
                    }
                }
                other => anyhow::bail!(
                    "pgoutput: unknown tuple cell tag {:?} — the protocol version \
                     may be newer than this decoder",
                    other as char
                ),
            });
        }
        Ok(out)
    }
}

/// Decode ONE `pgoutput` message.
pub(crate) fn decode(bytes: &[u8]) -> Result<Message> {
    let mut r = Reader::new(bytes);
    Ok(match r.u8()? {
        b'B' => {
            let final_lsn = r.u64()?;
            let _commit_time = r.u64()?;
            Message::Begin {
                final_lsn,
                xid: r.u32()?,
            }
        }
        b'C' => {
            let _flags = r.u8()?;
            let commit_lsn = r.u64()?;
            let end_lsn = r.u64()?;
            Message::Commit {
                commit_lsn,
                end_lsn,
            }
        }
        b'R' => {
            let relation_id = r.u32()?;
            let namespace = r.cstr()?;
            let name = r.cstr()?;
            let _replica_identity = r.u8()?;
            let n = r.u16()? as usize;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                let flags = r.u8()?;
                columns.push(Column {
                    name: r.cstr()?,
                    type_oid: r.u32()?,
                    type_modifier: r.i32()?,
                    is_key: flags & 1 == 1,
                });
            }
            Message::Relation {
                relation_id,
                namespace,
                name,
                columns,
            }
        }
        b'I' => {
            let relation_id = r.u32()?;
            expect_tag(&mut r, b'N')?;
            Message::Insert {
                relation_id,
                after: r.tuple()?,
            }
        }
        b'U' => {
            let relation_id = r.u32()?;
            // `K` (key only) or `O` (whole old tuple) is OPTIONAL and comes first;
            // `N` always follows. Treating an absent pre-image as an empty one
            // would make "the server sent no before-image" look like "the row had
            // no values", which is the distinction the old text format could not
            // make at all.
            let mut before = None;
            let mut tag = r.u8()?;
            if tag == b'K' || tag == b'O' {
                before = Some(r.tuple()?);
                tag = r.u8()?;
            }
            if tag != b'N' {
                anyhow::bail!(
                    "pgoutput: UPDATE without a new tuple (tag {:?})",
                    tag as char
                );
            }
            Message::Update {
                relation_id,
                before,
                after: r.tuple()?,
            }
        }
        b'D' => {
            let relation_id = r.u32()?;
            let tag = r.u8()?;
            if tag != b'K' && tag != b'O' {
                anyhow::bail!(
                    "pgoutput: DELETE without an old tuple (tag {:?})",
                    tag as char
                );
            }
            Message::Delete {
                relation_id,
                before: r.tuple()?,
            }
        }
        b'T' => {
            let n = r.u32()? as usize;
            let _flags = r.u8()?;
            let mut relation_ids = Vec::with_capacity(n);
            for _ in 0..n {
                relation_ids.push(r.u32()?);
            }
            Message::Truncate { relation_ids }
        }
        other => Message::Other(other as char),
    })
}

fn expect_tag(r: &mut Reader<'_>, want: u8) -> Result<()> {
    let got = r.u8()?;
    if got != want {
        anyhow::bail!(
            "pgoutput: expected tuple tag {:?}, got {:?}",
            want as char,
            got as char
        );
    }
    Ok(())
}

/// Turn one BINARY `pgoutput` cell into a [`RivetValue`], dispatching on the type
/// OID the `Relation` message carried.
///
/// This is where the format's real payoff lands. `test_decoding` hands over the
/// type's TEXT rendering, so every one of these becomes a parse — and each parse is
/// shaped by session state the reader does not control. Here the server sends the
/// type's own binary form and `postgres_types::FromSql` decodes it, the SAME
/// implementations the batch export already trusts for the identical wire format.
///
/// An OID with no arm is a REFUSAL, not a guess. Falling back to "keep the bytes as
/// text" is how a type nobody mapped becomes a column of hex in the destination,
/// which every count and sum agrees with.
pub(crate) fn value_from_binary(
    oid: u32,
    raw: &[u8],
) -> Result<crate::source::cdc::value::RivetValue> {
    use crate::source::cdc::value::RivetValue as V;
    use postgres_types::{FromSql, Type};

    let Some(ty) = Type::from_oid(oid) else {
        anyhow::bail!("pgoutput: no PostgreSQL type for OID {oid}");
    };
    fn de<'a, T: FromSql<'a>>(ty: &Type, raw: &'a [u8], what: &str) -> Result<T> {
        T::from_sql(ty, raw)
            .map_err(|e| anyhow::anyhow!("pgoutput: decoding {what} (oid {}): {e}", ty.oid()))
    }
    Ok(match oid {
        16 => V::Bool(de::<bool>(&ty, raw, "bool")?),
        20 => V::Int(de::<i64>(&ty, raw, "int8")?),
        21 => V::Int(de::<i16>(&ty, raw, "int2")? as i64),
        23 => V::Int(de::<i32>(&ty, raw, "int4")? as i64),
        700 => V::Float(de::<f32>(&ty, raw, "float4")? as f64),
        701 => V::Float(de::<f64>(&ty, raw, "float8")?),
        // timestamptz is an INSTANT on the wire: microseconds from 2000-01-01 UTC,
        // with no rendering and so no `TimeZone`/`datestyle` to get wrong. That is
        // the whole of the class this repo measured three defects in.
        // `.naive_utc()` and `.naive_local()` are the SAME call on a `DateTime<Utc>`
        // — the type parameter makes them identical, so that mutant is equivalent by
        // construction rather than ungraded. What is NOT equivalent is the type
        // parameter itself: decode as `Local` and the instant shifts by the host's
        // offset, which is the +9h corruption class in a new place.
        1184 => {
            V::DateTime(de::<chrono::DateTime<chrono::Utc>>(&ty, raw, "timestamptz")?.naive_utc())
        }
        1114 => V::DateTime(de::<chrono::NaiveDateTime>(&ty, raw, "timestamp")?),
        1082 => V::DateTime(
            de::<chrono::NaiveDate>(&ty, raw, "date")?
                .and_hms_opt(0, 0, 0)
                .expect("midnight is a valid time"),
        ),
        // uuid: 16 raw bytes, so no hyphen-strip and no length guard — the shape
        // that turned an entire column NULL when the text rendering was 36 chars.
        2950 => V::Bytes(de::<uuid::Uuid>(&ty, raw, "uuid")?.as_bytes().to_vec()),
        17 => V::Bytes(de::<Vec<u8>>(&ty, raw, "bytea")?),
        // json/jsonb/numeric/text all travel as bytes rivet already renders; jsonb's
        // binary form carries a 1-byte version prefix that `FromSql` strips.
        114 | 3802 => V::Bytes(
            serde_json::to_vec(&de::<serde_json::Value>(&ty, raw, "json")?)
                .map_err(|e| anyhow::anyhow!("pgoutput: re-encoding json: {e}"))?,
        ),
        25 | 1042 | 1043 => V::Bytes(de::<String>(&ty, raw, "text")?.into_bytes()),
        1700 => V::Bytes(numeric_text(raw)?.into_bytes()),
        // One-dimensional arrays of the element types the batch list builder makes.
        1005 => array_of(de::<Vec<Option<i16>>>(&ty, raw, "int2[]")?, |v| {
            V::Int(v as i64)
        }),
        1007 => array_of(de::<Vec<Option<i32>>>(&ty, raw, "int4[]")?, |v| {
            V::Int(v as i64)
        }),
        1016 => array_of(de::<Vec<Option<i64>>>(&ty, raw, "int8[]")?, V::Int),
        1000 => array_of(de::<Vec<Option<bool>>>(&ty, raw, "bool[]")?, V::Bool),
        1021 => array_of(de::<Vec<Option<f32>>>(&ty, raw, "float4[]")?, |v| {
            V::Float(v as f64)
        }),
        1022 => array_of(de::<Vec<Option<f64>>>(&ty, raw, "float8[]")?, V::Float),
        1009 => array_of(de::<Vec<Option<String>>>(&ty, raw, "text[]")?, |v| {
            V::Bytes(v.into_bytes())
        }),
        _ => anyhow::bail!(
            "pgoutput: column type `{}` (oid {oid}) has no binary decoder. Refusing \
             rather than passing the raw bytes through as text — an unmapped type \
             that ships as hex is a column every count and sum agrees with and no \
             consumer can read.",
            ty.name()
        ),
    })
}

fn array_of<T>(
    items: Vec<Option<T>>,
    f: impl Fn(T) -> crate::source::cdc::value::RivetValue,
) -> crate::source::cdc::value::RivetValue {
    use crate::source::cdc::value::RivetValue as V;
    V::Array(items.into_iter().map(|o| o.map_or(V::Null, &f)).collect())
}

/// PostgreSQL `numeric` in its binary form -> exact decimal text.
///
/// `postgres-types` has no `FromSql` for it, so this is the one type the format
/// does not hand over ready-made. The layout is base-10000 digit groups:
/// ndigits, weight, sign, dscale, then the groups.
fn numeric_text(raw: &[u8]) -> Result<String> {
    if raw.len() < 8 {
        anyhow::bail!("pgoutput: numeric header truncated ({} bytes)", raw.len());
    }
    let g = |i: usize| i16::from_be_bytes([raw[i], raw[i + 1]]);
    let (ndigits, weight, sign, dscale) = (g(0) as usize, g(2) as i32, g(4) as u16, g(6) as usize);
    match sign {
        0xC000 => return Ok("NaN".into()),
        0xD000 => return Ok("Infinity".into()),
        0xF000 => return Ok("-Infinity".into()),
        _ => {}
    }
    if raw.len() < 8 + ndigits * 2 {
        anyhow::bail!("pgoutput: numeric digits truncated");
    }
    let digits: Vec<i16> = (0..ndigits).map(|i| g(8 + i * 2)).collect();
    let mut int_part = String::new();
    for d in 0..=weight.max(0) {
        let v = digits.get(d as usize).copied().unwrap_or(0);
        if d == 0 {
            int_part.push_str(&v.to_string());
        } else {
            int_part.push_str(&format!("{v:04}"));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }
    let mut frac = String::new();
    let mut d = weight + 1;
    while frac.len() < dscale {
        let v = if d < 0 {
            0
        } else {
            digits.get(d as usize).copied().unwrap_or(0)
        };
        frac.push_str(&format!("{v:04}"));
        d += 1;
    }
    frac.truncate(dscale);
    let neg = if sign == 0x4000 { "-" } else { "" };
    Ok(if dscale == 0 {
        format!("{neg}{int_part}")
    } else {
        format!("{neg}{int_part}.{frac}")
    })
}

/// Fill an UPDATE's unchanged-TOAST cells from the pre-image, where it has them.
///
/// A pre-image cell that is ITSELF a marker is not a source — that recovers the
/// marker, reports success, and ships it as data. The text reader had the same
/// guard and it was ungraded until this session; here the two are different bytes
/// so the check is exact rather than a string comparison.
fn merge_unchanged_toast(after: &[Cell], before: Option<&[Cell]>) -> Vec<Cell> {
    let Some(before) = before else {
        return after.to_vec();
    };
    after
        .iter()
        .enumerate()
        .map(|(i, cell)| match (cell, before.get(i)) {
            // NO `pre != ToastUnchanged` guard, and the difference from the text
            // reader is the point. There, the marker is a STRING, so copying it
            // across "recovers" a value and clears the unchanged flag — the guard
            // is load-bearing and its absence ships the placeholder as data (found
            // and fixed earlier this session). Here cloning a marker yields a
            // marker, which `decode_tuple` refuses on the next line, so the guard
            // is provably redundant: witness-searched, the two forms agree on every
            // input. An unkillable mutant means redundant code, not an exclusion.
            (Cell::ToastUnchanged, Some(pre)) => pre.clone(),
            _ => cell.clone(),
        })
        .collect()
}

/// Assembles [`Message`]s into [`ChangeEvent`]s, holding the relation cache the
/// protocol requires.
///
/// `pgoutput` sends a `Relation` ONCE per table per connection and then references
/// it by OID, so a stateless decode cannot name the table a row belongs to. That
/// cache is the only state here — everything else is a function of the message.
///
/// The transaction framing is the load-bearing part, and it is the rule CLAUDE.md
/// already states for every adapter: `committed` marks the LAST event of a source
/// transaction, never every event. `pgoutput` gives that boundary explicitly
/// (`Begin` … `Commit`), which is exactly what the text reader had to infer — and
/// what PostgreSQL and SQL Server both got wrong once, marking every event
/// committed and breaking at-least-once on a crash mid-transaction.
#[derive(Default)]
pub(crate) struct Assembler {
    relations: std::collections::HashMap<u32, RelationInfo>,
    /// Rows of the transaction currently open, held until its `Commit`.
    open: Vec<PendingRow>,
    /// A refusal inside the CURRENT transaction poisons the rest of it.
    ///
    /// Without this, a caller that logs an error and keeps feeding gets a PARTIAL
    /// transaction: measured on the fixture, a refused UPDATE followed by its
    /// sibling DELETE yielded a one-row "transaction" that never existed. The
    /// at-least-once contract is transaction-atomic, so half of one is worse than
    /// none — and the caller is not the place to remember that.
    poisoned: Option<String>,
}

struct RelationInfo {
    schema: String,
    table: String,
    columns: Vec<Column>,
    /// Built once per relation, shared by every event that references it — the
    /// sink compares these by `Arc::ptr_eq` to memoise its column lookup.
    names: std::sync::Arc<[String]>,
}

struct PendingRow {
    op: crate::source::cdc::ChangeOp,
    relation_id: u32,
    before: Option<Vec<crate::source::cdc::value::RivetValue>>,
    after: Option<Vec<crate::source::cdc::value::RivetValue>>,
}

impl Assembler {
    /// Feed one message. Returns the transaction's events when it COMMITS, and
    /// nothing before then.
    ///
    /// Buffering to the commit is not an optimisation — it is what lets the last
    /// event carry `committed`, which is the only point the sink may roll a part
    /// and advance the checkpoint.
    pub(crate) fn push(&mut self, msg: Message) -> Result<Vec<crate::source::cdc::ChangeEvent>> {
        // A `Begin` clears the poison; everything else inside a poisoned
        // transaction is refused with the ORIGINAL cause, so the caller sees why
        // rather than a cascade of consequences.
        if let Some(why) = self.poisoned.clone()
            && !matches!(msg, Message::Begin { .. })
        {
            if matches!(msg, Message::Commit { .. }) {
                self.open.clear();
            }
            anyhow::bail!("{why}");
        }
        let out = self.push_inner(msg);
        if let Err(e) = &out {
            self.poisoned = Some(format!("{e:#}"));
            self.open.clear();
        }
        out
    }

    fn push_inner(&mut self, msg: Message) -> Result<Vec<crate::source::cdc::ChangeEvent>> {
        use crate::source::cdc::ChangeOp;
        match msg {
            Message::Begin { .. } => {
                // A `Begin` while rows are open means the previous transaction never
                // committed — the stream is not what we think it is. Dropping them
                // silently is how a partial transaction reaches the destination.
                self.poisoned = None;
                if !self.open.is_empty() {
                    anyhow::bail!(
                        "pgoutput: BEGIN with {} row(s) still open from an uncommitted \
                         transaction — refusing rather than shipping a partial one",
                        self.open.len()
                    );
                }
                Ok(Vec::new())
            }
            Message::Relation {
                relation_id,
                namespace,
                name,
                columns,
            } => {
                let names: std::sync::Arc<[String]> =
                    columns.iter().map(|c| c.name.clone()).collect();
                self.relations.insert(
                    relation_id,
                    RelationInfo {
                        schema: namespace,
                        table: name,
                        columns,
                        names,
                    },
                );
                Ok(Vec::new())
            }
            Message::Insert { relation_id, after } => {
                let after = self.decode_tuple(relation_id, &after)?;
                self.open.push(PendingRow {
                    op: ChangeOp::Insert,
                    relation_id,
                    before: None,
                    after: Some(after),
                });
                Ok(Vec::new())
            }
            Message::Update {
                relation_id,
                before,
                after,
            } => {
                // RECOVER an unchanged-TOAST cell from the pre-image before
                // decoding. `REPLICA IDENTITY FULL` does not stop the server
                // omitting an unchanged TOASTed value from the NEW tuple — identity
                // shapes the OLD one — so refusing here would refuse every UPDATE
                // that leaves a large column alone, which is most of them. The text
                // reader already recovers exactly this; the difference is that here
                // the marker is a TAG, so "the pre-image is itself a marker" is a
                // distinguishable case rather than a string collision.
                let after = merge_unchanged_toast(&after, before.as_deref());
                let before = before
                    .as_deref()
                    .map(|t| self.decode_tuple(relation_id, t))
                    .transpose()?;
                let after = self.decode_tuple(relation_id, &after)?;
                self.open.push(PendingRow {
                    op: ChangeOp::Update,
                    relation_id,
                    before,
                    after: Some(after),
                });
                Ok(Vec::new())
            }
            Message::Delete {
                relation_id,
                before,
            } => {
                let before = self.decode_tuple(relation_id, &before)?;
                self.open.push(PendingRow {
                    op: ChangeOp::Delete,
                    relation_id,
                    before: Some(before),
                    after: None,
                });
                Ok(Vec::new())
            }
            Message::Commit { commit_lsn, .. } => Ok(self.close(commit_lsn)),
            // A TRUNCATE removes rows with no per-row events, so nothing downstream
            // can retract them — the same refusal the text reader already makes,
            // and here it arrives as a typed message instead of a parsed statement.
            Message::Truncate { relation_ids } => {
                let named: Vec<String> = relation_ids
                    .iter()
                    .map(|id| {
                        self.relations.get(id).map_or_else(
                            || format!("oid {id}"),
                            |r| format!("{}.{}", r.schema, r.table),
                        )
                    })
                    .collect();
                anyhow::bail!(
                    "pgoutput: TRUNCATE of {} — this reader cannot represent it as a \
                     change. Every row the truncate removed would sit in the \
                     destination with no DELETE to retract it. Recover in rivet's own \
                     order: re-anchor FIRST (a fresh checkpoint), THEN re-snapshot \
                     (`mode: full`).",
                    named.join(", ")
                )
            }
            Message::Other(_) => Ok(Vec::new()),
        }
    }

    /// Close the open transaction, stamping the commit position on every row and
    /// `committed` on the LAST one only.
    fn close(&mut self, commit_lsn: u64) -> Vec<crate::source::cdc::ChangeEvent> {
        let position = crate::source::cdc::Position(serde_json::json!({
            "lsn": format!("{:X}/{:X}", commit_lsn >> 32, commit_lsn & 0xFFFF_FFFF)
        }));
        let rows: Vec<PendingRow> = std::mem::take(&mut self.open);
        let last = rows.len().saturating_sub(1);
        rows.into_iter()
            .enumerate()
            .map(|(i, r)| {
                let rel = self.relations.get(&r.relation_id);
                crate::source::cdc::ChangeEvent {
                    op: r.op,
                    schema: rel.map_or_else(String::new, |x| x.schema.clone()),
                    table: rel.map_or_else(String::new, |x| x.table.clone()),
                    before: r.before,
                    after: r.after,
                    position: position.clone(),
                    committed: i == last,
                    image_names: rel.map(|x| x.names.clone()),
                    seq: 0, // stamped by TxnSeq as the stream is consumed
                    poison: None,
                }
            })
            .collect()
    }

    fn decode_tuple(
        &self,
        relation_id: u32,
        cells: &[Cell],
    ) -> Result<Vec<crate::source::cdc::value::RivetValue>> {
        use crate::source::cdc::value::RivetValue as V;
        let Some(rel) = self.relations.get(&relation_id) else {
            anyhow::bail!(
                "pgoutput: a row referenced relation {relation_id} before its \
                 Relation message — the protocol sends one first, so this means a \
                 message was dropped"
            );
        };
        if cells.len() != rel.columns.len() {
            anyhow::bail!(
                "pgoutput: {}.{}: a row image carries {} value(s) under {} column(s)",
                rel.schema,
                rel.table,
                cells.len(),
                rel.columns.len()
            );
        }
        cells
            .iter()
            .zip(&rel.columns)
            .map(|(cell, col)| match cell {
                Cell::Null => Ok(V::Null),
                Cell::Binary(raw) => value_from_binary(col.type_oid, raw),
                // An unchanged-TOAST cell is NOT a value. The text reader had to
                // defer this as a `poison` because the marker was indistinguishable
                // from data; here it is its own tag, so the refusal can be exact.
                // Still a marker AFTER the pre-image merge means there was no
                // pre-image to recover from — the table's replica identity does not
                // carry one. Refusing is the only honest answer: writing the marker
                // would put the words `unchanged-toast-datum` where data belongs,
                // and writing NULL would erase a value that still exists.
                Cell::ToastUnchanged => anyhow::bail!(
                    "pgoutput: {}.{}: column `{}` is an unchanged TOAST value this \
                     UPDATE did not carry, and no pre-image holds it. Set REPLICA \
                     IDENTITY FULL on the table so the old row travels with the \
                     change, then re-capture.",
                    rel.schema,
                    rel.table,
                    col.name
                ),
                Cell::Text(_) => anyhow::bail!(
                    "pgoutput: {}.{}: column `{}` arrived in TEXT form. The slot must \
                     be read with `binary=true`; the text rendering is shaped by the \
                     session's datestyle/bytea_output/TimeZone, which is the defect \
                     class this reader exists to remove.",
                    rel.schema,
                    rel.table,
                    col.name
                ),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real bytes from a real server, not a hand-built fixture.
    ///
    /// `tests/fixtures/pgoutput/messages.hex` was captured from the pg-cdc stand
    /// (PostgreSQL 16.14) by exercising INSERT / UPDATE under both replica
    /// identities / DELETE / TRUNCATE against a table carrying the types that broke
    /// the text parser: quoted text, an array with a NULL element, a `timestamptz`
    /// in a non-UTC literal, uuid, bytea, jsonb, and an INCOMPRESSIBLE 12 KB column
    /// so the unchanged-TOAST case is really out of line (a compressible one stays
    /// inline and never produces the marker — measured, and the reason an earlier
    /// probe saw no `u` at all).
    fn fixture() -> Vec<Vec<u8>> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/messages.hex");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let out: Vec<Vec<u8>> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect()
            })
            .collect();
        assert!(
            out.len() >= 15,
            "the fixture is inert: {} messages is fewer than the capture recorded, \
             so the assertions below would grade almost nothing",
            out.len()
        );
        out
    }

    /// Every message in the capture decodes, and the SHAPE is what the wire said.
    #[test]
    fn every_captured_message_decodes_and_the_counts_match_the_capture() {
        let msgs: Vec<Message> = fixture()
            .iter()
            .map(|b| decode(b).unwrap_or_else(|e| panic!("decode {b:02x?}: {e}")))
            .collect();
        let count = |f: fn(&Message) -> bool| msgs.iter().filter(|m| f(m)).count();
        assert_eq!(count(|m| matches!(m, Message::Begin { .. })), 4);
        assert_eq!(count(|m| matches!(m, Message::Commit { .. })), 4);
        assert_eq!(count(|m| matches!(m, Message::Relation { .. })), 3);
        assert_eq!(count(|m| matches!(m, Message::Insert { .. })), 3);
        assert_eq!(count(|m| matches!(m, Message::Update { .. })), 2);
        assert_eq!(count(|m| matches!(m, Message::Delete { .. })), 1);
        assert_eq!(count(|m| matches!(m, Message::Truncate { .. })), 1);
        assert_eq!(
            count(|m| matches!(m, Message::Other(_))),
            0,
            "an unrecognised message in a capture of ordinary DML means the decoder \
             is behind the protocol, which is exactly how a bump becomes a gap"
        );
    }

    /// `Relation` carries the column TYPES and the replica-identity flags — the two
    /// facts the text format made rivet infer or ask the catalog for.
    #[test]
    fn relation_names_the_columns_their_type_oids_and_which_ones_are_the_key() {
        let rel = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .find_map(|m| match m {
                Message::Relation {
                    namespace,
                    name,
                    columns,
                    ..
                } => Some((namespace, name, columns)),
                _ => None,
            })
            .expect("the capture contains a Relation");
        assert_eq!(
            (rel.0.as_str(), rel.1.as_str()),
            ("public", "rivet_pgout_fx")
        );
        let names: Vec<&str> = rel.2.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "id", "txt", "arr", "ts", "u", "b", "n", "f", "ok", "d", "j", "big"
            ]
        );
        // Type OIDs, so a value decoder never has to guess: int8, text, _int4,
        // timestamptz, uuid, bytea, jsonb, text.
        let oids: Vec<u32> = rel.2.iter().map(|c| c.type_oid).collect();
        assert_eq!(
            oids,
            [20, 25, 1007, 1184, 2950, 17, 1700, 701, 16, 1082, 3802, 25]
        );
        // The KEY is `id` alone under the identity in force at capture time.
        let keys: Vec<&str> = rel
            .2
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            keys,
            ["id"],
            "the wire states the replica identity; rivet asking the catalog for it \
             is the `names are labels, catalogs are truth` detour this removes"
        );
    }

    /// The two cases `test_decoding` renders as ordinary text, and so cannot tell
    /// from a column that genuinely holds that text.
    #[test]
    fn a_null_and_an_unchanged_toast_are_their_own_tags_not_strings() {
        let msgs: Vec<Message> = fixture().iter().filter_map(|b| decode(b).ok()).collect();

        // A COLUMN-level NULL, which is a different thing from the NULL inside the
        // array value the other INSERT carries. The first capture had only the
        // latter, and the `Cell::Null` -> `Cell::Text` mutant SURVIVED it — a
        // fixture that never produces the tag cannot grade it.
        let nulls = msgs.iter().any(|m| match m {
            Message::Insert { after, .. } => {
                after.iter().filter(|c| **c == Cell::Null).count() >= 7
            }
            _ => false,
        });
        assert!(
            nulls,
            "the capture's all-NULL INSERT must decode to `Cell::Null` cells. \
             `test_decoding` writes the string `null` here, indistinguishable from a \
             text column whose value is `null`"
        );

        // The UPDATE under DEFAULT identity is where the TOAST marker appears.
        let toast = msgs.iter().any(|m| match m {
            Message::Update { after, .. } => after.contains(&Cell::ToastUnchanged),
            _ => false,
        });
        assert!(
            toast,
            "the capture's DEFAULT-identity UPDATE must carry an unchanged-TOAST \
             cell. `test_decoding` writes the STRING `unchanged-toast-datum` here, \
             which a column holding that text produces identically — the collision \
             this format removes"
        );

        // And the marker is never confused with a value: no Text/Binary cell in the
        // whole capture holds the old format's marker text.
        for m in &msgs {
            let cells: Vec<&Cell> = match m {
                Message::Insert { after, .. } => after.iter().collect(),
                Message::Update { before, after, .. } => {
                    before.iter().flatten().chain(after.iter()).collect()
                }
                Message::Delete { before, .. } => before.iter().collect(),
                _ => vec![],
            };
            for c in cells {
                if let Cell::Text(v) | Cell::Binary(v) = c {
                    assert!(
                        !String::from_utf8_lossy(v).contains("unchanged-toast-datum"),
                        "a VALUE cell must never carry the text marker"
                    );
                }
            }
        }
    }

    /// An UPDATE's pre-image is OPTIONAL, and absent is not empty.
    #[test]
    fn an_update_reports_a_missing_pre_image_as_none_not_as_an_empty_tuple() {
        let updates: Vec<Option<Vec<Cell>>> = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .filter_map(|m| match m {
                Message::Update { before, .. } => Some(before),
                _ => None,
            })
            .collect();
        assert_eq!(updates.len(), 2, "the capture has one UPDATE per identity");
        // DEFAULT identity on an unchanged key sends no pre-image; FULL sends one.
        assert!(
            updates.iter().any(|b| b.is_none()),
            "the DEFAULT-identity UPDATE must report NO pre-image"
        );
        let full = updates
            .iter()
            .flatten()
            .next()
            .expect("the FULL-identity UPDATE must carry one");
        assert_eq!(
            full.len(),
            12,
            "REPLICA IDENTITY FULL sends every column, so the pre-image is the whole \
             row — an empty tuple here would read as `the row had no values`"
        );
    }

    /// A DELETE carries its values in the pre-image, and nothing after.
    #[test]
    fn a_delete_carries_only_a_before_image() {
        let del = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .find_map(|m| match m {
                Message::Delete { before, .. } => Some(before),
                _ => None,
            })
            .expect("the capture contains a DELETE");
        // TWELVE cells, not one — and that is the wire's shape, measured. A tuple
        // ALWAYS has as many cells as the relation has columns; the replica
        // identity decides which of them carry a VALUE, and a key-only image pads
        // the rest with the NULL tag. (I assumed a key-only image was narrower and
        // the fixture corrected me — which is also why the arity guard in
        // `decode_tuple` can be an equality and not a bound.)
        assert_eq!(del.len(), 12, "a tuple is always as wide as its relation");
        assert!(
            del.iter().filter(|c| **c == Cell::Null).count() >= 10,
            "under DEFAULT identity only the key carries a value; the rest are the \
             NULL tag, not absent cells"
        );
    }

    /// Truncation is its OWN message, not a DDL statement to be string-matched.
    ///
    /// MySQL's adapter parses `TRUNCATE <name>` out of a QueryEvent, and that
    /// parser panicked on a non-ASCII table name (round 13). Here it is a typed
    /// message carrying relation OIDs.
    #[test]
    fn truncate_is_a_typed_message_carrying_relation_ids() {
        let ids = fixture()
            .iter()
            .filter_map(|b| decode(b).ok())
            .find_map(|m| match m {
                Message::Truncate { relation_ids } => Some(relation_ids),
                _ => None,
            })
            .expect("the capture contains a TRUNCATE");
        assert_eq!(ids.len(), 1, "one table was truncated");
        assert_ne!(ids[0], 0, "a relation OID is never zero");
    }

    /// Every value type, decoded from the type's OWN BINARY FORM.
    ///
    /// A second capture, taken with `binary=true`, because the first was text-mode
    /// and text cells cannot grade a binary decoder — the fixture must produce the
    /// tag the code under test consumes.
    ///
    /// This is the payoff measured end to end: `timestamptz` arrives as an INSTANT
    /// (no `TimeZone` to strip, no `datestyle` to parse), `bytea` as raw bytes (no
    /// `\x` and no `bytea_output`), `uuid` as its 16 bytes (no hyphen strip and no
    /// length guard), and an array as elements (no `{1,NULL,3}` to parse, and its
    /// inner NULL is a real one rather than the four letters).
    #[test]
    fn every_binary_value_decodes_through_its_type_oid() {
        use crate::source::cdc::value::RivetValue as V;

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/binary_values.hex");
        let msgs: Vec<Message> = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let b: Vec<u8> = (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect();
                decode(&b).expect("decode")
            })
            .collect();

        let cols = msgs
            .iter()
            .find_map(|m| match m {
                Message::Relation { columns, .. } => Some(columns.clone()),
                _ => None,
            })
            .expect("a Relation");
        let inserts: Vec<&Vec<Cell>> = msgs
            .iter()
            .filter_map(|m| match m {
                Message::Insert { after, .. } => Some(after),
                _ => None,
            })
            .collect();
        assert_eq!(
            inserts.len(),
            3,
            "three rows in one transaction — see the framing test for why"
        );

        let decoded: Vec<V> = inserts[0]
            .iter()
            .zip(&cols)
            .map(|(cell, col)| match cell {
                Cell::Null => V::Null,
                Cell::Binary(raw) => value_from_binary(col.type_oid, raw)
                    .unwrap_or_else(|e| panic!("{}: {e}", col.name)),
                other => panic!("{}: expected a BINARY cell, got {other:?}", col.name),
            })
            .collect();

        let by = |name: &str| -> &V {
            let i = cols.iter().position(|c| c.name == name).expect("column");
            &decoded[i]
        };
        assert_eq!(
            by("id"),
            &V::Int(52),
            "the binary capture re-seeds with 52/53/54"
        );
        assert_eq!(by("txt"), &V::Bytes(br#"a,b "q""#.to_vec()));
        assert_eq!(by("ok"), &V::Bool(true));
        assert_eq!(by("f"), &V::Float(2.5));
        assert_eq!(
            by("arr"),
            &V::Array(vec![V::Int(1), V::Null, V::Int(3)]),
            "the array's inner NULL is a real NULL, not the four letters `NULL` that \
             the text form is indistinguishable from"
        );
        assert_eq!(
            by("u"),
            &V::Bytes(vec![
                0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55,
                0x55, 0x55
            ]),
            "16 raw bytes — the text form is 36 chars, which is what turned a whole \
             uuid column NULL when a builder guarded on length"
        );
        assert_eq!(
            by("b"),
            &V::Bytes(vec![0x00, 0xff]),
            "raw bytes, no `\\x` prefix"
        );
        assert_eq!(
            by("n"),
            &V::Bytes(b"-12345.6789".to_vec()),
            "numeric is exact decimal text, sign and scale preserved"
        );
        assert_eq!(by("j"), &V::Bytes(br#"{"k":[1,2]}"#.to_vec()));
        // `2026-03-01 12:00:00+05` is `07:00:00Z` — an INSTANT, with no session
        // rendering in between. The +9h corruption class cannot arise here.
        assert_eq!(
            by("ts"),
            &V::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 3, 1)
                    .unwrap()
                    .and_hms_opt(7, 0, 0)
                    .unwrap()
            )
        );
        assert_eq!(
            by("d"),
            &V::DateTime(
                chrono::NaiveDate::from_ymd_opt(2026, 3, 2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            )
        );

        // The all-NULL row: every cell is the NULL TAG, never a decoded value.
        assert!(
            inserts[1].iter().skip(1).all(|c| *c == Cell::Null),
            "every nullable column of the all-NULL row must be `Cell::Null`"
        );

        // An unmapped OID REFUSES rather than passing bytes through.
        let err = value_from_binary(3220, b"\x00\x00\x00\x01").expect_err("pg_lsn has no arm");
        assert!(
            format!("{err:#}").contains("no binary decoder"),
            "an unmapped type must name itself and refuse: {err:#}"
        );
    }

    /// The transaction framing, from the same real capture.
    ///
    /// `committed` must mark the LAST event of a source transaction and no other.
    /// Both PostgreSQL and SQL Server once set it on EVERY event, and the cost was
    /// measured: a transaction larger than `rollover` rolled and checkpointed
    /// mid-transaction, and a crash before the tail flushed advanced the resume
    /// position past the commit — 7 of 12 rows gone. Here the boundary is explicit
    /// on the wire, so the rule is enforced rather than inferred.
    #[test]
    fn a_transaction_yields_nothing_until_its_commit_and_marks_only_its_last_event() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgoutput/binary_values.hex");
        let msgs: Vec<Message> = std::fs::read_to_string(&path)
            .expect("fixture")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let b: Vec<u8> = (0..l.len() / 2)
                    .map(|i| u8::from_str_radix(&l[i * 2..i * 2 + 2], 16).expect("hex"))
                    .collect();
                decode(&b).expect("decode")
            })
            .collect();

        let mut a = Assembler::default();
        let mut yielded: Vec<Vec<crate::source::cdc::ChangeEvent>> = Vec::new();
        let mut refused: Vec<String> = Vec::new();
        for m in msgs {
            let is_commit = matches!(m, Message::Commit { .. });
            match a.push(m) {
                Ok(out) if is_commit => yielded.push(out),
                Ok(out) => assert!(
                    out.is_empty(),
                    "nothing may be yielded before the COMMIT — an event released \
                     early cannot carry the transaction's own commit position"
                ),
                Err(e) => refused.push(format!("{e:#}")),
            }
        }

        // The capture holds four transactions and TWO of them are REFUSALS, which
        // is the correct outcome and worth asserting rather than filtering out:
        // the DEFAULT-identity UPDATE cannot be represented (its TOASTed column did
        // not travel) and a TRUNCATE cannot be represented at all.
        // DISTINCT causes, not occurrences: a poisoned transaction refuses every
        // later message with the ORIGINAL cause, which is the point — the caller
        // sees why rather than a cascade of consequences.
        let causes: std::collections::BTreeSet<&str> = refused.iter().map(String::as_str).collect();
        assert_eq!(
            causes.len(),
            2,
            "two distinct refusals — the unchanged TOAST and the TRUNCATE: {causes:#?}"
        );
        assert!(
            causes.iter().any(|e| e.contains("unchanged TOAST")),
            "the DEFAULT-identity UPDATE must refuse, naming the column: {refused:?}"
        );
        assert!(
            causes.iter().any(|e| e.contains("TRUNCATE")),
            "a TRUNCATE has no per-row representation: {refused:?}"
        );

        // What DID assemble: the three-row insert transaction, and the
        // FULL-identity UPDATE. The refused transaction contributes an empty yield
        // at its COMMIT, which is correct — its rows never entered `open`.
        let real: Vec<&Vec<crate::source::cdc::ChangeEvent>> =
            yielded.iter().filter(|t| !t.is_empty()).collect();
        assert_eq!(
            real.iter().map(|t| t.len()).collect::<Vec<_>>(),
            vec![3, 1],
            "MULTI-row transactions, deliberately: with one row per transaction \
             `i == last`, `true` and `i == 0` are the same answer and every one of \
             those mutants survives — measured, and the sixth time this session a \
             fixture had to cross a threshold before the assertion could bite"
        );

        for tx in &real {
            let flags: Vec<bool> = tx.iter().map(|e| e.committed).collect();
            let want: Vec<bool> = (0..tx.len()).map(|i| i == tx.len() - 1).collect();
            assert_eq!(
                flags, want,
                "exactly the LAST event of a transaction is `committed`; setting it \
                 on every event is what let a crash advance the slot past a commit \
                 whose tail was never flushed"
            );
            let first = &tx[0].position;
            assert!(
                tx.iter().all(|e| &e.position == first),
                "one transaction, one commit position — a per-event position would \
                 let the checkpoint land mid-transaction"
            );
        }

        let ev = &real[0][0];
        assert_eq!(
            (ev.schema.as_str(), ev.table.as_str()),
            ("public", "rivet_pgout_fx")
        );
        assert_eq!(
            ev.image_names.as_deref().map(|n| n.len()),
            Some(12),
            "the image is name-addressed from the Relation message, so the sink maps \
             by NAME and the positional-corruption class cannot arise"
        );
    }

    /// The relation cache is REQUIRED by the protocol, and its absence is an error.
    #[test]
    fn a_row_before_its_relation_message_is_refused_not_guessed() {
        let mut a = Assembler::default();
        let err = a
            .push(Message::Insert {
                relation_id: 999,
                after: vec![Cell::Null],
            })
            .expect_err("a row with no known relation must fail");
        assert!(
            format!("{err:#}").contains("before its Relation message"),
            "the error must name the cause; guessing a schema/table here routes \
             events to a relation nobody configured: {err:#}"
        );
    }

    /// An unchanged-TOAST cell is refused with the column named, not written out.
    #[test]
    fn an_unchanged_toast_cell_is_refused_and_names_the_column() {
        let mut a = Assembler::default();
        a.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![Column {
                name: "big".into(),
                type_oid: 25,
                type_modifier: -1,
                is_key: false,
            }],
        })
        .expect("relation");
        let err = a
            .push(Message::Update {
                relation_id: 1,
                before: None,
                after: vec![Cell::ToastUnchanged],
            })
            .expect_err("an unchanged TOAST value is not a value");
        let text = format!("{err:#}");
        assert!(
            text.contains("big") && text.contains("REPLICA IDENTITY FULL"),
            "{text}"
        );
    }

    /// A tuple whose width disagrees with the relation is refused.
    ///
    /// The capture cannot produce this — the server always sends as many cells as
    /// the relation has columns — so it is constructed. That is the honest shape
    /// here: a guard against a message the wire should never send still has to be
    /// graded, or it is a comment. MySQL's arity guard was ungraded offline for the
    /// same reason and cost a round to find.
    #[test]
    fn a_tuple_wider_or_narrower_than_its_relation_is_refused() {
        let rel = || Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "v".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        };
        let cell = |n: i64| Cell::Binary(n.to_be_bytes().to_vec());

        for (label, cells) in [
            ("too narrow", vec![cell(1)]),
            ("too wide", vec![cell(1), cell(2), cell(3)]),
        ] {
            let mut a = Assembler::default();
            a.push(rel()).expect("relation");
            let err = a
                .push(Message::Insert {
                    relation_id: 1,
                    after: cells,
                })
                .expect_err(
                    "a tuple that disagrees with its relation must be REFUSED — \
                     mapping it by position puts values into the wrong columns, \
                     which is the class a name-addressed image exists to remove",
                );
            let text = format!("{err:#}");
            assert!(
                text.contains("public.t") && text.contains("column(s)"),
                "{label}: the refusal must name the relation and both widths: {text}"
            );
        }
    }

    /// A pre-image cell that is ITSELF a marker is not a recovery source.
    ///
    /// Constructed, because the capture does not produce it — and that is the point.
    /// The text reader had exactly this guard, ungraded, until this session: taking
    /// the marker from a marker "recovers" it, clears the unchanged flag, and ships
    /// the placeholder as data while reporting success. Here the marker is a TAG, so
    /// the check is exact instead of a string comparison, but the mistake is the
    /// same one and it survives a fixture that never puts a marker on both sides.
    #[test]
    fn a_pre_image_cell_that_is_also_a_marker_cannot_recover_anything() {
        let mut a = Assembler::default();
        a.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "big".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        })
        .expect("relation");
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");

        let id = Cell::Binary(7i64.to_be_bytes().to_vec());
        let err = a
            .push(Message::Update {
                relation_id: 1,
                // BOTH images carry the marker for `big`.
                before: Some(vec![id.clone(), Cell::ToastUnchanged]),
                after: vec![id, Cell::ToastUnchanged],
            })
            .expect_err("a marker cannot be recovered from a marker");
        let text = format!("{err:#}");
        assert!(
            text.contains("big") && text.contains("no pre-image holds it"),
            "the refusal must name the column and say the pre-image is no help — \
             copying the marker across reports a clean recovery and writes the \
             placeholder where data belongs: {text}"
        );

        // …and the SAME shape recovers when the pre-image holds a real value.
        let mut b = Assembler::default();
        b.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "big".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        })
        .expect("relation");
        b.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        let id = Cell::Binary(7i64.to_be_bytes().to_vec());
        b.push(Message::Update {
            relation_id: 1,
            before: Some(vec![id.clone(), Cell::Binary(b"real".to_vec())]),
            after: vec![id, Cell::ToastUnchanged],
        })
        .expect("a real pre-image value IS a recovery source");
        let out = b
            .push(Message::Commit {
                commit_lsn: 16,
                end_lsn: 16,
            })
            .expect("commit");
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].after.as_ref().and_then(|v| v.get(1)),
            Some(&crate::source::cdc::value::RivetValue::Bytes(
                b"real".to_vec()
            )),
            "the unchanged column takes the pre-image's REAL value"
        );
    }

    /// A BEGIN while rows are still open means a transaction never committed.
    #[test]
    fn a_begin_over_an_uncommitted_transaction_is_refused() {
        let mut a = Assembler::default();
        a.push(Message::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
                is_key: true,
            }],
        })
        .expect("relation");
        a.push(Message::Begin {
            final_lsn: 1,
            xid: 1,
        })
        .expect("begin");
        a.push(Message::Insert {
            relation_id: 1,
            after: vec![Cell::Binary(42i64.to_be_bytes().to_vec())],
        })
        .expect("insert");
        let err = a
            .push(Message::Begin {
                final_lsn: 2,
                xid: 2,
            })
            .expect_err("a second BEGIN with rows open must fail");
        assert!(
            format!("{err:#}").contains("still open"),
            "dropping the open rows silently ships a partial transaction: {err:#}"
        );
    }

    /// Truncated input FAILS rather than returning a half-read message.
    #[test]
    fn a_truncated_message_is_an_error_not_a_partial_decode() {
        let full = &fixture()[0];
        for cut in 1..full.len().min(12) {
            let err = decode(&full[..cut]);
            if let Ok(m) = err {
                assert!(
                    matches!(m, Message::Other(_)),
                    "a {cut}-byte prefix decoded as {m:?}; a partial read is how a \
                     protocol desync becomes silent corruption"
                );
            }
        }
        assert!(decode(&[]).is_err(), "an empty message must error");
        // A tuple whose declared length runs past the buffer.
        assert!(
            decode(&[b'I', 0, 0, 0, 1, b'N', 0, 1, b't', 0x7f, 0xff, 0xff, 0xff]).is_err(),
            "a length that exceeds the buffer must error, never read out of bounds"
        );
    }
}
