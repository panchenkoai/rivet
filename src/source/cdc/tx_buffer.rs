//! A buffered source transaction and its overflow, in one place for every engine that buffers.
//!
//! An adapter frames its transactions (PostgreSQL BEGIN…COMMIT, a MySQL XID, a SQL
//! Server `__$start_lsn` run) and hands each row to a [`TxBuffer`]. The buffer owns
//! the rest: the memory cap, spill to `RIVET_CDC_SPILL_DIR` or refuse naming the
//! cap, closing the in-memory head so only the transaction's LAST row is
//! `committed`, and replaying the spilled tail in order ([`replay`]).
//!
//! Oracle does not use the buffer: a commit SCN's rows are re-ordered by
//! `(transaction, SEQUENCE#)` only once the whole group is read, so no prefix of
//! it can be released as a head. It shares the cap and keeps the refusal.

use std::fmt::Display;
use std::path::PathBuf;

use super::spill::{SpillFile, SpooledGroups, SpooledTx};
use super::{CdcEngine, ChangeEvent, Position, TxnFramer, max_tx_bytes, max_tx_rows};
use crate::error::Result;

/// How an engine's buffer is named to the operator.
struct Terms {
    /// Log prefix: `{tag} cdc:`.
    tag: &'static str,
    /// What one buffer holds, for the refusal.
    subject: &'static str,
    /// What one buffer is, for the spill lines.
    unit: &'static str,
    /// The spill file's label.
    label: &'static str,
    /// Why the spill line may repeat.
    repeats: &'static str,
}

/// The operator's words for `engine`'s buffer.
fn terms(engine: CdcEngine) -> Terms {
    // A claim in a product message is a testable claim: SQL Server's buffer is a
    // poll BATCH and Oracle's a commit SCN, never "a single transaction".
    let (tag, subject, unit, label) = match engine {
        CdcEngine::Postgres => ("pg", "a single transaction", "transaction", "pg-tx"),
        CdcEngine::Mysql => ("mysql", "a single transaction", "transaction", "mysql-tx"),
        CdcEngine::Mssql => (
            "mssql",
            "one poll batch (one or more transactions)",
            "poll batch",
            "mssql-batch",
        ),
        CdcEngine::Oracle => (
            "oracle",
            "one commit SCN (one or more transactions)",
            "commit SCN",
            "oracle-scn",
        ),
        CdcEngine::Mongo => ("mongo", "a single transaction", "transaction", "mongo-tx"),
    };
    // A slot peek does not consume, so PostgreSQL re-reads an un-acked transaction.
    let repeats = match engine {
        CdcEngine::Postgres => {
            " Expect this line once per peek until the sink acks past the commit: a \
             slot peek does not consume, so an un-acked transaction is re-read (and \
             re-spilled) on the next pass."
        }
        _ => "",
    };
    Terms {
        tag,
        subject,
        unit,
        label,
        repeats,
    }
}

/// The buffered-transaction memory backstop at `(row, byte)` caps; the refusal names the cap, why, and the way out.
pub(crate) fn check_tx_buffer_caps(
    engine: CdcEngine,
    rows: usize,
    bytes: usize,
    caps: (usize, usize),
) -> Result<()> {
    let Terms { tag, subject, .. } = terms(engine);
    let (row_cap, byte_cap) = caps;
    if rows > row_cap {
        anyhow::bail!(
            "{tag} cdc: {subject} has more than {row_cap} rows — it must be \
             buffered whole (a transaction is never split across parts, which is what \
             makes a crash resume transaction-atomic), so this would exhaust memory. \
             Split the source transaction, or raise RIVET_CDC_MAX_TX_ROWS only if a \
             transaction this large is genuinely expected."
        );
    }
    if bytes > byte_cap {
        anyhow::bail!(
            // Resident cost, not payload: ~2.8M narrow rows cross 2 GiB with no large
            // value anywhere, so the message must not send anyone hunting big cells.
            "{tag} cdc: {subject} needs more than {byte_cap} bytes of buffer \
             memory — it must be buffered whole (a transaction is never split \
             across parts), so this would exhaust memory. The estimate is resident \
             cost, so wide cells and sheer row count both land here. Split the \
             source transaction, or raise RIVET_CDC_MAX_TX_BYTES only if this much \
             buffering is genuinely acceptable."
        );
    }
    Ok(())
}

/// One engine's in-flight transaction (or SQL Server poll batch): an in-memory head and, past the cap, a tail on disk.
pub(crate) struct TxBuffer {
    engine: CdcEngine,
    spill_dir: Option<PathBuf>,
    caps: (usize, usize),
    head: Vec<ChangeEvent>,
    bytes: usize,
    spill: Option<SpillFile>,
}

/// A closed transaction's tail on disk, replayed after its head by [`replay`].
pub(crate) enum SpooledTail {
    /// One transaction: the tail's last row closes it.
    One(SpooledTx),
    /// Several transactions (SQL Server): each row's successor decides whether it closes one.
    Groups(SpooledGroups),
}

impl TxBuffer {
    /// An empty buffer at the process caps; `spill_dir: None` makes crossing a cap a refusal.
    pub(crate) fn new(engine: CdcEngine, spill_dir: Option<PathBuf>) -> Self {
        Self::with_caps(engine, spill_dir, (max_tx_rows(), max_tx_bytes()))
    }

    /// An empty buffer at explicit `(row, byte)` caps.
    pub(crate) fn with_caps(
        engine: CdcEngine,
        spill_dir: Option<PathBuf>,
        caps: (usize, usize),
    ) -> Self {
        Self {
            engine,
            spill_dir,
            caps,
            head: Vec::new(),
            bytes: 0,
            spill: None,
        }
    }

    /// Buffer `ev`, or append `record(&ev)` to the spill once a cap was crossed; `at` names the source position for the log.
    pub(crate) fn push(
        &mut self,
        ev: ChangeEvent,
        record: impl FnOnce(&ChangeEvent) -> Vec<u8>,
        at: &dyn Display,
    ) -> Result<()> {
        if let Some(sp) = self.spill.as_mut() {
            return sp.push(&record(&ev));
        }
        self.bytes = self.bytes.saturating_add(ev.estimated_bytes());
        self.head.push(ev);
        let Err(cap) = check_tx_buffer_caps(self.engine, self.head.len(), self.bytes, self.caps)
        else {
            return Ok(());
        };
        // No directory named ⇒ the cap keeps its meaning and REFUSES: spilling does not
        // bound memory end to end (the sink still holds the transaction), so it must not
        // silently replace a guard that does.
        let Some(dir) = self.spill_dir.as_deref() else {
            return Err(cap);
        };
        log::warn!(
            "{}",
            spill_line(self.engine, at, self.head.len(), self.bytes, dir)
        );
        self.spill = Some(SpillFile::create(dir, terms(self.engine).label)?);
        Ok(())
    }

    /// Drop everything buffered and spilled: a transaction that is re-read, deferred or discarded.
    pub(crate) fn clear(&mut self) {
        self.head.clear();
        self.bytes = 0;
        self.spill = None;
    }

    /// The in-memory head, in arrival order.
    pub(crate) fn head(&self) -> &[ChangeEvent] {
        &self.head
    }

    /// Rows on disk, `None` while nothing has spilled.
    pub(crate) fn spilled_rows(&self) -> Option<usize> {
        self.spill.as_ref().map(SpillFile::len)
    }

    /// Close ONE transaction at `commit`: the head to deliver now, then the tail to [`replay`].
    pub(crate) fn close_transaction(
        &mut self,
        commit: &Position,
        at: &dyn Display,
    ) -> Result<(Vec<ChangeEvent>, Option<SpooledTail>)> {
        let mut head = std::mem::take(&mut self.head);
        self.bytes = 0;
        let Some(sp) = self.spill.take() else {
            TxnFramer::close_group(&mut head, commit);
            return Ok((head, None));
        };
        // The transaction's last row is on DISK, so the head must not close it.
        TxnFramer::close_head_of_group(&mut head, commit, sp.len());
        // `warn`: the default level hides `info`, and the split is the only externally
        // visible proof that memory was bounded (the delivered rows are identical).
        log::warn!(
            "{}",
            split_line(self.engine, at, head.len(), sp.len(), sp.bytes())
        );
        let tail = SpooledTx::new(sp.into_reader()?, commit.clone());
        Ok((head, Some(SpooledTail::One(tail))))
    }

    /// Close a buffer of SEVERAL transactions whose rows carry their own commit position.
    pub(crate) fn close_groups(
        &mut self,
        decode: fn(&[u8]) -> Result<ChangeEvent>,
        at: &dyn Display,
    ) -> Result<(Vec<ChangeEvent>, Option<SpooledTail>)> {
        let mut head = std::mem::take(&mut self.head);
        self.bytes = 0;
        // Seal the tail FIRST: its first row says whether the head's last group continues on disk.
        let tail = match self.spill.take() {
            None => None,
            Some(sp) => {
                log::warn!(
                    "{}",
                    split_line(self.engine, at, head.len(), sp.len(), sp.bytes())
                );
                Some(SpooledGroups::new(sp.into_reader()?, decode)?)
            }
        };
        close_runs(
            &mut head,
            tail.as_ref().and_then(SpooledGroups::first_position),
        );
        Ok((head, tail.map(SpooledTail::Groups)))
    }
}

/// The line that says a buffer crossed the cap and where its tail goes.
fn spill_line(
    engine: CdcEngine,
    at: &dyn Display,
    rows: usize,
    bytes: usize,
    dir: &std::path::Path,
) -> String {
    let Terms {
        tag, unit, repeats, ..
    } = terms(engine);
    format!(
        "{tag} cdc: {unit} at {at} passed the in-memory cap at {rows} rows / {bytes} \
         bytes — spilling the rest to {} rather than failing the run. Every \
         transaction is still delivered whole and atomically. Note this moves the \
         ADAPTER's copy to disk; the sink still holds the whole transaction (a part \
         is never split across one), so peak memory falls only modestly — measured \
         ~11% on PostgreSQL's 100k-row transaction, not to the cap.{repeats}",
        dir.display()
    )
}

/// The line live tests and the soak parse for the memory/disk split.
fn split_line(engine: CdcEngine, at: &dyn Display, head: usize, tail: usize, bytes: u64) -> String {
    let Terms { tag, unit, .. } = terms(engine);
    format!(
        "{tag} cdc: {unit} at {at} delivered {head} rows from memory and {tail} from disk \
         ({bytes} bytes spilled)"
    )
}

/// Does the head's group continue onto the tail? Only the last can: spilled rows follow every buffered one.
fn head_group_continues_on_disk(
    group: &Position,
    is_last_head_group: bool,
    tail_first: Option<&Position>,
) -> bool {
    is_last_head_group && tail_first == Some(group)
}

/// Close each run of rows sharing a position at its last row; the last run stays open when the tail starts in it.
fn close_runs(evs: &mut [ChangeEvent], tail_first: Option<&Position>) {
    let n = evs.len();
    let mut seen = 0;
    for group in evs.chunk_by_mut(|a, b| a.position == b.position) {
        seen += group.len();
        let commit = group[0].position.clone();
        let continues = head_group_continues_on_disk(&commit, seen == n, tail_first);
        TxnFramer::close_head_of_group(group, &commit, usize::from(continues));
    }
}

impl SpooledTail {
    /// The next tail row, decoded and closed, or `None` once the tail is done.
    fn next_event(
        &mut self,
        decode: impl Fn(&[u8]) -> Result<ChangeEvent>,
    ) -> Result<Option<ChangeEvent>> {
        match self {
            Self::One(t) => t.next_event(decode),
            Self::Groups(g) => g.next_event(decode),
        }
    }

    /// Rows not yet handed out.
    fn remaining(&self) -> usize {
        match self {
            Self::One(t) => t.remaining(),
            Self::Groups(g) => g.remaining(),
        }
    }
}

/// The next row of `slot`'s tail; the tail (and its file) is dropped the moment it is finished.
pub(crate) fn replay(
    slot: &mut Option<SpooledTail>,
    decode: impl Fn(&[u8]) -> Result<ChangeEvent>,
) -> Result<Option<ChangeEvent>> {
    let Some(tail) = slot.as_mut() else {
        return Ok(None);
    };
    let out = tail.next_event(decode)?;
    if out.is_none() || tail.remaining() == 0 {
        *slot = None;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::cdc::ChangeOp;
    use crate::source::cdc::spill::{decode_event, encode_event};
    use crate::source::cdc::value::RivetValue;
    use serde_json::json;

    fn row(id: i64, lsn: &str) -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Insert,
            schema: "public".into(),
            table: "t".into(),
            before: None,
            after: Some(vec![RivetValue::Int(id)]),
            position: Position(json!({ "lsn": lsn })),
            // Poisoned: the close has to CLEAR it, not merely leave it.
            committed: true,
            image_names: None,
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
            before_poison: None,
        }
    }

    fn id(ev: &ChangeEvent) -> i64 {
        match ev.after.as_deref() {
            Some([RivetValue::Int(i)]) => *i,
            other => panic!("unexpected image {other:?}"),
        }
    }

    /// Head then the replayed tail, as `(id, lsn, committed)`.
    fn deliver(
        (head, mut tail): (Vec<ChangeEvent>, Option<SpooledTail>),
    ) -> Vec<(i64, String, bool)> {
        let mut out: Vec<ChangeEvent> = head;
        while let Some(ev) = replay(&mut tail, decode_event).expect("replay") {
            out.push(ev);
        }
        assert!(tail.is_none(), "a finished tail must be dropped");
        out.iter()
            .map(|e| {
                (
                    id(e),
                    e.position.0["lsn"].as_str().unwrap().to_string(),
                    e.committed,
                )
            })
            .collect()
    }

    fn fill(buf: &mut TxBuffer, rows: &[(i64, &str)]) {
        for &(i, lsn) in rows {
            buf.push(row(i, lsn), encode_event, &lsn).expect("push");
        }
    }

    /// Five rows at a cap of 2: the cap is crossed mid-transaction and the last row is on disk.
    #[test]
    fn a_spilled_transaction_closes_only_on_its_last_row_on_disk() {
        let d = tempfile::tempdir().expect("dir");
        let mut buf = TxBuffer::with_caps(
            CdcEngine::Postgres,
            Some(d.path().to_path_buf()),
            (2, usize::MAX),
        );
        fill(
            &mut buf,
            &[(1, "x"), (2, "x"), (3, "x"), (4, "x"), (5, "x")],
        );
        assert_eq!(
            buf.head().len(),
            3,
            "the row that crosses the cap stays in memory"
        );
        assert_eq!(buf.spilled_rows(), Some(2));
        let commit = Position(json!({ "lsn": "0/C" }));
        let got = deliver(buf.close_transaction(&commit, &"0/C").expect("close"));
        assert_eq!(
            got,
            [1, 2, 3, 4, 5]
                .map(|i| (i, "0/C".to_string(), i == 5))
                .to_vec(),
            "every row carries the COMMIT position and only the last closes the \
             transaction — a `committed` on the head lets the sink roll, checkpoint \
             and ack mid-transaction, and a crash before the tail's flush loses it"
        );
        assert_eq!(buf.spilled_rows(), None, "a close leaves the buffer empty");
        assert!(buf.head().is_empty());
    }

    /// Under the cap nothing spills: one close, last row committed, no tail.
    #[test]
    fn an_unspilled_transaction_closes_on_its_last_row_with_no_tail() {
        let mut buf = TxBuffer::with_caps(CdcEngine::Mysql, None, (5, usize::MAX));
        fill(&mut buf, &[(1, "x"), (2, "x")]);
        let commit = Position(json!({ "lsn": "f:9" }));
        let (head, tail) = buf.close_transaction(&commit, &"f:9").expect("close");
        assert!(tail.is_none());
        assert_eq!(
            head.iter().map(|e| e.committed).collect::<Vec<_>>(),
            [false, true]
        );
    }

    /// With no spill directory the cap is a refusal that names it, on the row that crosses it.
    #[test]
    fn crossing_a_cap_without_a_spill_dir_refuses_naming_the_cap() {
        let mut buf = TxBuffer::with_caps(CdcEngine::Mssql, None, (2, usize::MAX));
        fill(&mut buf, &[(1, "a"), (2, "a")]);
        let err = buf
            .push(row(3, "a"), encode_event, &"a")
            .expect_err("past the cap")
            .to_string();
        assert!(
            err.contains(
                "mssql cdc: one poll batch (one or more transactions) has more than 2 rows"
            ) && err.contains("RIVET_CDC_MAX_TX_ROWS"),
            "{err}"
        );
        let mut buf = TxBuffer::with_caps(CdcEngine::Postgres, None, (usize::MAX, 1));
        let err = buf
            .push(row(1, "a"), encode_event, &"a")
            .expect_err("past the byte cap")
            .to_string();
        assert!(
            err.contains("pg cdc: a single transaction needs more than 1 bytes")
                && err.contains("RIVET_CDC_MAX_TX_BYTES"),
            "{err}"
        );
    }

    /// A cap is crossed only strictly past it, on rows and on bytes.
    #[test]
    fn a_buffer_exactly_at_a_cap_still_fits() {
        assert!(check_tx_buffer_caps(CdcEngine::Oracle, 2, 10, (2, 10)).is_ok());
        assert!(check_tx_buffer_caps(CdcEngine::Oracle, 3, 10, (2, 10)).is_err());
        let err = check_tx_buffer_caps(CdcEngine::Oracle, 2, 11, (2, 10)).expect_err("bytes");
        assert!(
            err.to_string().contains(
                "oracle cdc: one commit SCN (one or more transactions) needs more than 10 bytes"
            ),
            "{err}"
        );
    }

    /// The two operator lines carry what live tests and the soak parse, in the engine's own words.
    #[test]
    fn the_spill_lines_name_the_engine_the_cap_and_the_split() {
        let dir = std::path::Path::new("/spill");
        let pg = spill_line(CdcEngine::Postgres, &"0/A", 3, 99, dir);
        assert!(
            pg.starts_with(
                "pg cdc: transaction at 0/A passed the in-memory cap at 3 rows / 99 bytes"
            ) && pg.contains("spilling the rest to /spill")
                && pg.contains("once per peek"),
            "{pg}"
        );
        let ms = spill_line(CdcEngine::Mssql, &"0x01", 3, 99, dir);
        assert!(
            ms.starts_with("mssql cdc: poll batch at 0x01 passed the in-memory cap")
                && !ms.contains("once per peek"),
            "only a non-consuming peek re-spills: {ms}"
        );
        assert_eq!(
            split_line(CdcEngine::Mysql, &"b.000001:4", 3, 2, 70),
            "mysql cdc: transaction at b.000001:4 delivered 3 rows from memory and 2 from \
             disk (70 bytes spilled)"
        );
    }

    /// `clear` drops head, byte count and spill, so the next transaction starts under the cap.
    #[test]
    fn a_cleared_buffer_forgets_the_head_the_bytes_and_the_spill() {
        let d = tempfile::tempdir().expect("dir");
        let mut buf =
            TxBuffer::with_caps(CdcEngine::Mysql, Some(d.path().to_path_buf()), (1, 1 << 20));
        fill(&mut buf, &[(1, "x"), (2, "x"), (3, "x")]);
        assert_eq!(buf.spilled_rows(), Some(1));
        buf.clear();
        assert_eq!(buf.spilled_rows(), None);
        assert!(buf.head().is_empty());
        assert_eq!(
            buf.bytes, 0,
            "a stale byte count would trip the next transaction's cap"
        );
    }

    /// A poll batch of 2-1-2 groups spilled after the 4th row: each group closes on its own last row.
    #[test]
    fn a_spilled_batch_closes_each_group_on_its_own_last_row() {
        let d = tempfile::tempdir().expect("dir");
        let mut buf = TxBuffer::with_caps(
            CdcEngine::Mssql,
            Some(d.path().to_path_buf()),
            (3, usize::MAX),
        );
        fill(
            &mut buf,
            &[(1, "a"), (2, "a"), (3, "b"), (4, "c"), (5, "c"), (6, "d")],
        );
        assert_eq!(buf.spilled_rows(), Some(2), "rows 5 and 6 are on disk");
        let got = deliver(buf.close_groups(decode_event, &"d").expect("close"));
        assert_eq!(
            got,
            vec![
                (1, "a".into(), false),
                (2, "a".into(), true),
                (3, "b".into(), true),
                // `c` continues on disk: closing it in memory would ack mid-transaction.
                (4, "c".into(), false),
                (5, "c".into(), true),
                (6, "d".into(), true),
            ]
        );
    }

    /// An unspilled batch closes every run in memory.
    #[test]
    fn an_unspilled_batch_closes_every_run_in_memory() {
        let mut buf = TxBuffer::with_caps(CdcEngine::Mssql, None, (10, usize::MAX));
        fill(&mut buf, &[(1, "a"), (2, "a"), (3, "b")]);
        let (head, tail) = buf.close_groups(decode_event, &"b").expect("close");
        assert!(tail.is_none());
        assert_eq!(
            head.iter().map(|e| e.committed).collect::<Vec<_>>(),
            [false, true, true]
        );
    }

    /// Every arm of the head/tail group-boundary question.
    #[test]
    fn a_head_group_continues_on_disk_only_when_the_tail_starts_in_it() {
        let a = Position(json!({ "lsn": "0x01" }));
        let b = Position(json!({ "lsn": "0x02" }));
        assert!(head_group_continues_on_disk(&a, true, Some(&a)));
        assert!(
            !head_group_continues_on_disk(&a, true, Some(&b)),
            "the tail starts a new one"
        );
        assert!(
            !head_group_continues_on_disk(&a, true, None),
            "nothing spilled"
        );
        assert!(
            !head_group_continues_on_disk(&a, false, Some(&a)),
            "a group followed by another in memory has ended, whatever is on disk"
        );
    }

    /// Only the LAST run can continue onto disk.
    #[test]
    fn each_run_commits_only_on_its_last_row() {
        let close = |tail: Option<&str>| {
            let mut evs: Vec<ChangeEvent> = ["a", "a", "b", "c", "c"]
                .iter()
                .enumerate()
                .map(|(i, l)| row(i as i64, l))
                .collect();
            let tail = tail.map(|l| Position(json!({ "lsn": l })));
            close_runs(&mut evs, tail.as_ref());
            evs.iter().map(|e| e.committed).collect::<Vec<_>>()
        };
        assert_eq!(close(None), [false, true, true, false, true]);
        assert_eq!(
            close(Some("c")),
            [false, true, true, false, false],
            "c continues on disk"
        );
        assert_eq!(
            close(Some("a")),
            [false, true, true, false, true],
            "only the last run can continue"
        );
    }

    /// A tail is handed out row by row and dropped (its file deleted) after the last one.
    #[test]
    fn replay_drops_the_tail_after_its_last_row_and_not_before() {
        let d = tempfile::tempdir().expect("dir");
        let mut buf = TxBuffer::with_caps(
            CdcEngine::Mysql,
            Some(d.path().to_path_buf()),
            (1, usize::MAX),
        );
        fill(&mut buf, &[(1, "x"), (2, "x"), (3, "x"), (4, "x")]);
        let (_, mut tail) = buf
            .close_transaction(&Position(json!({ "lsn": "c" })), &"c")
            .expect("close");
        let files = || std::fs::read_dir(d.path()).unwrap().count();
        assert_eq!(files(), 1);
        assert_eq!(id(&replay(&mut tail, decode_event).unwrap().unwrap()), 3);
        assert!(tail.is_some(), "one row is still on disk");
        assert_eq!(id(&replay(&mut tail, decode_event).unwrap().unwrap()), 4);
        assert!(tail.is_none(), "the finished tail is dropped at once");
        assert_eq!(files(), 0, "and its file with it");
        assert!(replay(&mut tail, decode_event).unwrap().is_none());
    }
}
