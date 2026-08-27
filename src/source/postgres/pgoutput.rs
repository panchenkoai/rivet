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
        assert_eq!(count(|m| matches!(m, Message::Begin { .. })), 6);
        assert_eq!(count(|m| matches!(m, Message::Commit { .. })), 6);
        assert_eq!(count(|m| matches!(m, Message::Relation { .. })), 3);
        assert_eq!(count(|m| matches!(m, Message::Insert { .. })), 2);
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
        assert_eq!((rel.0.as_str(), rel.1.as_str()), ("public", "fx_t"));
        let names: Vec<&str> = rel.2.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "txt", "arr", "ts", "u", "b", "j", "big"]);
        // Type OIDs, so a value decoder never has to guess: int8, text, _int4,
        // timestamptz, uuid, bytea, jsonb, text.
        let oids: Vec<u32> = rel.2.iter().map(|c| c.type_oid).collect();
        assert_eq!(oids, [20, 25, 1007, 1184, 2950, 17, 3802, 25]);
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
            8,
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
        assert_eq!(del.len(), 8, "under FULL the whole row rides the pre-image");
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
