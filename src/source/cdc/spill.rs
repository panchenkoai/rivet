//! Spilling a buffered transaction to disk — the frame, shared by both encodings.
//!
//! A transaction is buffered WHOLE (it is never split across parts, which is what
//! makes a crash resume transaction-atomic), so a large one used to be REFUSED:
//! `check_tx_buffer_caps` bails, and capture stops until someone splits the source
//! transaction or raises a cap. That is a memory ceiling wearing a refusal.
//!
//! This is the container, not the encoding. Two encodings ride on it and the choice
//! is per adapter:
//!
//! * **raw wire bytes** where the engine gives them — PostgreSQL's `pgoutput`
//!   messages arrive as bytes and Mongo's events as BSON, so there is nothing to
//!   encode and the existing decoder reads them back. SQL Server has no wire to
//!   keep (its "stream" is a query result set) and MySQL's crate hands over parsed
//!   events, so neither can use this path;
//! * **Arrow IPC** as the general fallback — rivet already converts events to Arrow
//!   to write Parquet, so this reuses machinery that exists and is tested, in a
//!   self-describing format with a stable reader.
//!
//! What this file owns is the part both need and neither should re-invent: append
//! a record, read them back IN ORDER, and never leave the file behind.

use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::PathBuf;

use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, Position};

/// A length-prefixed record log in a temp file, deleted when dropped.
///
/// Records are read back in the order written — which is the whole contract, since
/// a transaction's rows are ordered and `__seq` is derived from that order.
///
/// The prefix is a `u32` length, and the reader REFUSES a length that runs past the
/// file rather than reading what it can: a truncated spill is a torn transaction,
/// and half of one is worse than none. That is the same rule the sink applies to a
/// torn part.
pub(crate) struct SpillFile {
    /// `None` once [`SpillFile::into_reader`] has handed the file over — the
    /// reader owns the cleanup from that point, and a `Drop` that deleted it
    /// anyway would pull the log out from under the read on any platform that
    /// does not allow an unlinked-but-open file.
    path: Option<PathBuf>,
    writer: Option<BufWriter<std::fs::File>>,
    records: usize,
    bytes: u64,
}

impl SpillFile {
    /// Create one in `dir`, named for the caller so a leaked file says who left it.
    ///
    /// In a caller-supplied directory rather than the system temp: a CDC run's
    /// spill can be gigabytes, and the system temp is often a small tmpfs — filling
    /// it takes down more than rivet. The caller passes somewhere it already knows
    /// is sized for the data.
    pub(crate) fn create(dir: &std::path::Path, label: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("rivet-spill-{label}-{}.bin", std::process::id()));
        // read+write, not `File::create`: `drain` rewinds and reads the same
        // handle back, and a write-only descriptor fails there with `Bad file
        // descriptor` — which reads as a TRUNCATED log, i.e. the one error this
        // type raises to mean "your transaction is torn". A wrong open mode
        // masquerading as data loss is worth the explicit options.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            path: Some(path),
            writer: Some(BufWriter::new(file)),
            records: 0,
            bytes: 0,
        })
    }

    /// Append one record.
    pub(crate) fn push(&mut self, record: &[u8]) -> Result<()> {
        let len = u32::try_from(record.len()).map_err(|_| {
            anyhow::anyhow!(
                "cdc spill: a single record is {} bytes, past the 4 GiB frame limit",
                record.len()
            )
        })?;
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("cdc spill: pushed after the log was sealed"))?;
        w.write_all(&len.to_be_bytes())?;
        w.write_all(record)?;
        self.records += 1;
        self.bytes += 4 + u64::from(len);
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.records
    }

    /// On-disk footprint — what the memory cap traded away, so a caller can say so.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Seal the log and hand back a STREAMING reader over it.
    ///
    /// A `Vec<Vec<u8>>` return would defeat the point: spilling exists to keep an
    /// oversized transaction OUT of memory, and materialising every record to read
    /// it back trades one whole-transaction allocation for another plus the disk
    /// write. The caller pulls one record, decodes it, hands the event on, and
    /// drops it — so the ceiling is one record, not one transaction.
    ///
    /// Consuming, because a spill is read ONCE: the transaction it holds is
    /// released at its commit and the file has no reason to outlive that.
    pub(crate) fn into_reader(mut self) -> Result<SpillReader> {
        let Some(w) = self.writer.take() else {
            anyhow::bail!("cdc spill: sealed twice");
        };
        let mut file = w.into_inner().map_err(|e| {
            anyhow::anyhow!("cdc spill: flushing the log before reading it back: {e}")
        })?;
        file.flush()?;
        file.rewind()?;
        Ok(SpillReader {
            // Taken, not cloned: `self` is dropped at the end of this function, and
            // a `Drop` still holding the path would delete the file the reader is
            // about to read.
            path: self.path.take(),
            reader: BufReader::new(file),
            read: 0,
            total: self.records,
        })
    }

    /// Read every record back into memory, in order — TESTS only.
    ///
    /// Convenient for a round-trip assertion and wrong for the product, for the
    /// reason [`SpillFile::into_reader`] gives.
    #[cfg(test)]
    pub(crate) fn drain(self) -> Result<Vec<Vec<u8>>> {
        let mut r = self.into_reader()?;
        let mut out = Vec::new();
        while let Some(rec) = r.next_record() {
            out.push(rec?);
        }
        Ok(out)
    }
}

/// One-pass reader over a sealed [`SpillFile`], deleting the file when dropped.
///
/// The record COUNT is carried from the writer rather than inferred from EOF, so a
/// log that ends early is a named error instead of a short read. That distinction is
/// the whole reason this type refuses to be lenient: a spilled transaction read short
/// is a TORN transaction, the sink would flush it, checkpoint past its commit, and
/// the tail would be gone from both the source log and the destination.
pub(crate) struct SpillReader {
    path: Option<PathBuf>,
    reader: BufReader<std::fs::File>,
    read: usize,
    total: usize,
}

impl SpillReader {
    /// How many records the log holds in total — the transaction's tail length,
    /// which is what tells the caller WHICH record carries the commit flag.
    pub(crate) fn len(&self) -> usize {
        self.total
    }

    /// Records not yet handed out.
    pub(crate) fn remaining(&self) -> usize {
        self.total - self.read
    }

    /// The next record, or `None` once all `len()` of them have been read.
    ///
    /// `None` means DONE, never "the file ended sooner than expected" — that is an
    /// `Err`. Collapsing the two is how half a transaction ships as a whole one.
    pub(crate) fn next_record(&mut self) -> Option<Result<Vec<u8>>> {
        if self.read >= self.total {
            return None;
        }
        let i = self.read;
        self.read += 1;
        Some(self.read_one(i))
    }

    fn read_one(&mut self, i: usize) -> Result<Vec<u8>> {
        let mut len = [0u8; 4];
        self.reader.read_exact(&mut len).map_err(|e| {
            anyhow::anyhow!(
                "cdc spill: record {i} of {} has no length prefix — the log is \
                 truncated, and a torn transaction is worse than none: {e}",
                self.total
            )
        })?;
        let len = u32::from_be_bytes(len) as usize;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).map_err(|e| {
            anyhow::anyhow!(
                "cdc spill: record {i} of {} declares {len} bytes the log does not \
                 hold — truncated: {e}",
                self.total
            )
        })?;
        Ok(buf)
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        if let Some(p) = self.path.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

impl Drop for SpillFile {
    /// Best effort, and deliberately silent: a leaked spill file is a disk-space
    /// problem, and failing a run in `Drop` over one would turn a tidy-up into an
    /// outage. The name carries the pid so a leak can be traced.
    fn drop(&mut self) {
        self.writer.take();
        if let Some(p) = self.path.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("spill dir")
    }

    /// Records come back in ORDER and byte-identical.
    ///
    /// Order is the contract, not a nicety: a transaction's rows are ordered and
    /// `__seq` is derived from that order, so a spill that reorders silently
    /// rewrites which change won for a key.
    #[test]
    fn records_round_trip_in_the_order_they_were_written() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "order").expect("create");
        // THREE records, not two: with one, order is meaningless; with two, a
        // reversal and a swap are the same mutation. Distinct LENGTHS too, so a
        // frame that reads a fixed size cannot pass.
        let written: Vec<Vec<u8>> = vec![b"a".to_vec(), b"bbbb".to_vec(), b"cc".to_vec()];
        for r in &written {
            s.push(r).expect("push");
        }
        assert_eq!(s.len(), 3);
        assert_eq!(s.bytes(), (4 + 1) + (4 + 4) + (4 + 2));
        assert_eq!(s.drain().expect("drain"), written);
    }

    /// An EMPTY record is a record — not a terminator, not a skip.
    #[test]
    fn an_empty_record_survives_the_round_trip() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "empty").expect("create");
        for r in [b"x".as_slice(), b"".as_slice(), b"y".as_slice()] {
            s.push(r).expect("push");
        }
        assert_eq!(
            s.drain().expect("drain"),
            vec![b"x".to_vec(), Vec::new(), b"y".to_vec()],
            "a zero-length record must round-trip as one; treating it as an end \
             marker silently truncates the transaction at the first NULL-ish row"
        );
    }

    /// Binary content is not text: NULs and invalid UTF-8 pass through untouched.
    #[test]
    fn arbitrary_bytes_pass_through_unchanged() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "bin").expect("create");
        let nasty = vec![0u8, 0xff, b'\n', 0x00, 0xc3, 0x28, 4, 0, 0, 0];
        s.push(&nasty).expect("push");
        assert_eq!(s.drain().expect("drain"), vec![nasty]);
    }

    /// A large record crosses the buffered-writer boundary rather than sitting
    /// inside one buffer — the threshold a small fixture never reaches.
    #[test]
    fn a_record_larger_than_the_write_buffer_round_trips() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "big").expect("create");
        let big: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        s.push(b"before").expect("push");
        s.push(&big).expect("push");
        s.push(b"after").expect("push");
        let back = s.drain().expect("drain");
        assert_eq!(back.len(), 3);
        assert_eq!(back[1], big, "a multi-buffer record must survive intact");
        assert_eq!(
            back[2],
            b"after".to_vec(),
            "and the record AFTER it must too"
        );
    }

    /// The file is GONE once the spill is dropped.
    ///
    /// A CDC spill can be gigabytes, and one leaked per oversized transaction fills
    /// a disk on a scheduler. The cleanup is in `Drop` so a panic mid-transaction
    /// cannot skip it — the same reason the server-setting guard puts its revert
    /// there.
    #[test]
    fn the_file_does_not_outlive_the_spill() {
        let d = dir();
        let path = {
            let mut s = SpillFile::create(d.path(), "cleanup").expect("create");
            s.push(b"payload").expect("push");
            s.path
                .clone()
                .expect("the file is still owned by the spill")
        };
        assert!(
            !path.exists(),
            "the spill file outlived its owner at {}",
            path.display()
        );

        // …and after a PANIC too, which is the case a manual delete misses.
        let path = std::panic::catch_unwind(|| {
            let mut s = SpillFile::create(d.path(), "panicky").expect("create");
            s.push(b"payload").expect("push");
            let p = s.path.clone().expect("owned");
            std::panic::panic_any(p);
        })
        .expect_err("the closure panics on purpose");
        let path = path.downcast::<PathBuf>().expect("the path rode the panic");
        assert!(!path.exists(), "a panic must not leak a spill file");
    }

    /// A truncated log is an ERROR, never a short read.
    ///
    /// Half a transaction is worse than none: the sink would flush it, checkpoint
    /// past its commit, and the tail would be gone from both the source log and the
    /// destination. Same rule the sink applies to a torn part.
    #[test]
    fn a_truncated_log_is_refused_rather_than_partially_read() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "torn").expect("create");
        s.push(b"one").expect("push");
        s.push(b"two").expect("push");
        // Cut the file to the first record plus a partial second.
        let path = s.path.clone().expect("owned");
        s.writer.take(); // close the writer without dropping the guard
        let full = std::fs::read(&path).expect("read the log");
        std::fs::write(&path, &full[..full.len() - 2]).expect("truncate");
        // Reopen so `drain` reads the truncated file.
        let mut torn = SpillFile::create(d.path(), "torn2").expect("create");
        torn.records = s.records;
        let torn_path = torn.path.clone().expect("owned");
        std::fs::copy(&path, &torn_path).expect("stage the torn log");
        torn.writer = Some(BufWriter::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&torn_path)
                .expect("reopen"),
        ));
        let err = torn.drain().expect_err("a truncated log must ERROR");
        let text = format!("{err:#}");
        assert!(
            text.contains("truncated"),
            "the error must name the cause — a short read here is a torn \
             transaction, and the caller has to know that is what happened: {text}"
        );
    }

    /// The reader hands records out ONE at a time, in order, and stops at `len()`.
    #[test]
    fn the_reader_streams_records_in_order_and_counts_down() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "stream").expect("create");
        for r in [b"a".as_slice(), b"bbbb".as_slice(), b"cc".as_slice()] {
            s.push(r).expect("push");
        }
        let mut r = s.into_reader().expect("seal");
        assert_eq!(r.len(), 3);
        assert_eq!(r.remaining(), 3);
        assert_eq!(r.next_record().expect("one").expect("ok"), b"a".to_vec());
        assert_eq!(r.remaining(), 2, "remaining must fall as records are read");
        assert_eq!(r.next_record().expect("two").expect("ok"), b"bbbb".to_vec());
        assert_eq!(r.next_record().expect("three").expect("ok"), b"cc".to_vec());
        assert_eq!(r.remaining(), 0);
        assert!(
            r.next_record().is_none(),
            "past the declared count the reader is DONE — a fourth record here \
             would mean the count and the file disagree"
        );
    }

    /// A reader that ends EARLY errors; it does not quietly return `None`.
    ///
    /// This is the distinction the type exists for: `None` means the transaction was
    /// delivered whole, so a truncated log answering `None` would ship half a
    /// transaction as a complete one — the sink flushes it, checkpoints past its
    /// commit, and the tail is gone from the source log too.
    #[test]
    fn a_short_log_errors_rather_than_ending_the_stream() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "short").expect("create");
        s.push(b"one").expect("push");
        s.push(b"two").expect("push");
        let path = s.path.clone().expect("owned");
        s.writer.take();
        let full = std::fs::read(&path).expect("read");

        let mut torn = SpillFile::create(d.path(), "short2").expect("create");
        torn.records = 2;
        let torn_path = torn.path.clone().expect("owned");
        std::fs::write(&torn_path, &full[..full.len() - 2]).expect("truncate");
        torn.writer = Some(BufWriter::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&torn_path)
                .expect("reopen"),
        ));
        let mut r = torn.into_reader().expect("seal");
        assert_eq!(
            r.next_record().expect("first").expect("ok"),
            b"one".to_vec()
        );
        let err = r
            .next_record()
            .expect("the count says a second record exists")
            .expect_err("and the log does not hold it");
        assert!(
            format!("{err:#}").contains("truncated"),
            "the error must name the cause: {err:#}"
        );
    }

    /// The file is gone once the READER is dropped — the handover must not leak.
    #[test]
    fn the_reader_takes_over_the_cleanup() {
        let d = dir();
        let mut s = SpillFile::create(d.path(), "handover").expect("create");
        s.push(b"payload").expect("push");
        let path = s.path.clone().expect("owned");
        let mut r = s.into_reader().expect("seal");
        assert!(
            path.exists(),
            "the file must survive the handover — the reader still needs it"
        );
        assert_eq!(
            r.next_record().expect("one").expect("ok"),
            b"payload".to_vec()
        );
        drop(r);
        assert!(!path.exists(), "the reader must delete the log it finished");
    }
}

/// A committed transaction whose tail is on disk, streaming out one row at a time.
///
/// Shared by every engine that spills, because the rule it enforces is not
/// engine-specific: each row of the tail carries the transaction's COMMIT position,
/// and only the LAST row closes the transaction. Two copies of that rule is one
/// copy too many — a `committed` flag on the wrong row lets the sink roll,
/// checkpoint and ack MID-transaction, and a crash before the tail's flush advances
/// the resume position past the commit.
///
/// What differs per engine is only how a record DECODES (raw wire rows for
/// PostgreSQL, the tagged frame above for MySQL and SQL Server), which is why the
/// decoder arrives as a closure rather than as a second implementation of this.
pub(crate) struct SpooledTx {
    reader: SpillReader,
    commit: Position,
    at: usize,
}

impl SpooledTx {
    pub(crate) fn new(reader: SpillReader, commit: Position) -> Self {
        Self {
            reader,
            commit,
            at: 0,
        }
    }

    /// Rows still on disk — what tells the caller the tail is not yet finished.
    pub(crate) fn remaining(&self) -> usize {
        self.reader.remaining()
    }

    /// The next row of the tail, decoded and stamped, or `None` once it is done.
    pub(crate) fn next_event(
        &mut self,
        decode: impl FnOnce(&[u8]) -> Result<ChangeEvent>,
    ) -> Result<Option<ChangeEvent>> {
        let Some(rec) = self.reader.next_record() else {
            return Ok(None);
        };
        let mut ev = decode(&rec?)?;
        crate::source::cdc::TxnFramer::close_tail_event(
            &mut ev,
            &self.commit,
            self.at,
            self.reader.len(),
        );
        self.at += 1;
        Ok(Some(ev))
    }
}

/// A row closes its transaction when nothing follows it, or what follows belongs to
/// a different one.
///
/// A NAMED predicate rather than a condition inline in the drain: it is the whole
/// of the group-boundary decision, and the consequence of getting it wrong is not
/// visible in the delivered rows — `committed` decides when the sink ROLLS, so an
/// early one lets it checkpoint and ack mid-transaction.
pub(crate) fn closes_group(this: &Position, next: Option<&Position>) -> bool {
    next.is_none_or(|n| n.0 != this.0)
}

/// A spilled tail holding MORE THAN ONE transaction — SQL Server's shape.
///
/// PostgreSQL and MySQL buffer exactly one transaction at a time, so their tail
/// length alone says which row closes it ([`SpooledTx`]). SQL Server's poll buffer
/// holds a whole batch: several runs of rows sharing a `__$start_lsn`, with the
/// boundaries only visible by comparing neighbours. So this one reads ONE RECORD
/// AHEAD — a row cannot be handed out until the row after it is known, because that
/// is what says whether it closes its transaction.
///
/// The lookahead is one record, not one group: a group can be arbitrarily large,
/// and buffering a whole one would put back exactly the memory the spill removed.
pub(crate) struct SpooledGroups {
    reader: SpillReader,
    /// Decoded, not yet handed out — held so the NEXT row's position can decide
    /// whether this one closes its transaction.
    peeked: Option<ChangeEvent>,
}

impl SpooledGroups {
    /// Seal a spill into a group-aware tail, priming the lookahead.
    pub(crate) fn new(
        reader: SpillReader,
        decode: impl Fn(&[u8]) -> Result<ChangeEvent>,
    ) -> Result<Self> {
        let mut me = Self {
            reader,
            peeked: None,
        };
        me.peeked = me.read_one(&decode)?;
        Ok(me)
    }

    fn read_one(
        &mut self,
        decode: &impl Fn(&[u8]) -> Result<ChangeEvent>,
    ) -> Result<Option<ChangeEvent>> {
        match self.reader.next_record() {
            None => Ok(None),
            Some(rec) => Ok(Some(decode(&rec?)?)),
        }
    }

    /// The position of the FIRST row on disk.
    ///
    /// The caller needs it to decide whether the IN-MEMORY head's last group
    /// continues onto disk: if it does, the head's last row must NOT be marked
    /// committed, because the transaction it belongs to has not ended yet.
    pub(crate) fn first_position(&self) -> Option<&Position> {
        self.peeked.as_ref().map(|e| &e.position)
    }

    /// The next row, with `committed` set from the row that follows it.
    pub(crate) fn next_event(
        &mut self,
        decode: impl Fn(&[u8]) -> Result<ChangeEvent>,
    ) -> Result<Option<ChangeEvent>> {
        let Some(mut ev) = self.peeked.take() else {
            return Ok(None);
        };
        self.peeked = self.read_one(&decode)?;
        ev.committed = closes_group(&ev.position, self.peeked.as_ref().map(|n| &n.position));
        Ok(Some(ev))
    }

    /// Rows not yet handed out — the peeked one plus whatever is still on disk.
    pub(crate) fn remaining(&self) -> usize {
        self.reader.remaining() + usize::from(self.peeked.is_some())
    }
}

// ─── the general fallback encoding ───────────────────────────────────────────
//
// PostgreSQL and MongoDB spill the RAW WIRE bytes: the row arrived as text or BSON
// and the decoder that reads it back is the one the in-memory path uses, so a
// spilled row and a buffered one cannot decode differently. MySQL's crate hands
// over PARSED events and SQL Server's "stream" is a query result set — neither has
// a wire to keep, so their events need an encoding of their own.
//
// ARROW IPC was the plan here, on the argument that rivet already turns events into
// Arrow to write Parquet, so the machinery exists and is tested. Checking that
// against what the sink actually does shows it is the wrong fit, for a reason that
// is specific rather than aesthetic: the Arrow type of a column is decided by the
// SINK, at its first flush, from the export's type mapping — and
// `sink::refine_decimal_scales` derives a Decimal column's SCALE from the values in
// that first batch. `RivetValue::Bytes` is deliberately undecided until then (Utf8,
// Binary, or a Decimal string). An adapter that spilled through Arrow would have to
// invent a schema BEFORE any of that, so a SQL Server placeholder-scale column
// would take its scale from whichever rows happened to still be in memory — the
// spilled tail and the buffered head would disagree about the same column's type.
// The reuse argument does not survive either: nothing in rivet builds an Arrow
// union today, so a lossless Arrow encoding of a sum type would be new, untested
// code exactly like this one, with a schema negotiation on top.
//
// So the fallback is a TAGGED frame: one byte of variant tag, everything
// length-prefixed, values recursive for arrays. It is lossless, self-describing,
// and — the property that matters — it makes no type decision at all, which is
// precisely what the sink needs it not to do.

/// Wire tags. Explicit discriminants, never the enum's declaration order: a spill
/// written before a variant is added must not be read as a different variant after.
mod tag {
    pub(super) const NULL: u8 = 0;
    pub(super) const BOOL: u8 = 1;
    pub(super) const INT: u8 = 2;
    pub(super) const UINT: u8 = 3;
    pub(super) const FLOAT: u8 = 4;
    pub(super) const DATETIME: u8 = 5;
    pub(super) const TIME_MICROS: u8 = 6;
    pub(super) const BYTES: u8 = 7;
    pub(super) const ARRAY: u8 = 8;
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u64).to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_len(out, b.len());
    out.extend_from_slice(b);
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

/// Cursor over a frame. Every read is bounds-checked and every shortfall is an
/// error — a spilled transaction read short is a TORN transaction.
struct Cur<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "cdc spill: a frame asks for {n} bytes at offset {} of {} — truncated",
                    self.at,
                    self.b.len()
                )
            })?;
        let out = &self.b[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn len(&mut self) -> Result<usize> {
        let raw = u64::from_be_bytes(self.take(8)?.try_into().expect("8 bytes"));
        usize::try_from(raw).map_err(|_| {
            anyhow::anyhow!("cdc spill: a frame declares {raw} bytes, past this platform's usize")
        })
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.len()?;
        Ok(self.take(n)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        let b = self.bytes()?;
        String::from_utf8(b).map_err(|e| anyhow::anyhow!("cdc spill: not utf-8: {e}"))
    }
}

fn put_value(out: &mut Vec<u8>, v: &RivetValue) {
    match v {
        RivetValue::Null => out.push(tag::NULL),
        RivetValue::Bool(b) => {
            out.push(tag::BOOL);
            out.push(u8::from(*b));
        }
        RivetValue::Int(i) => {
            out.push(tag::INT);
            out.extend_from_slice(&i.to_be_bytes());
        }
        RivetValue::UInt(u) => {
            out.push(tag::UINT);
            out.extend_from_slice(&u.to_be_bytes());
        }
        // The BIT PATTERN, not a rendering: `to_be_bytes` round-trips every f64
        // including NaN and the two zeros, where a decimal string does not.
        RivetValue::Float(f) => {
            out.push(tag::FLOAT);
            out.extend_from_slice(&f.to_be_bytes());
        }
        // Structural: seconds since the epoch plus nanoseconds, never a formatted
        // string — a text rendering is exactly the session-dependent leg this repo
        // has been bitten by, and a spill must not introduce one.
        RivetValue::DateTime(dt) => {
            out.push(tag::DATETIME);
            out.extend_from_slice(&dt.and_utc().timestamp().to_be_bytes());
            out.extend_from_slice(&dt.and_utc().timestamp_subsec_nanos().to_be_bytes());
        }
        RivetValue::TimeMicros(us) => {
            out.push(tag::TIME_MICROS);
            out.extend_from_slice(&us.to_be_bytes());
        }
        RivetValue::Bytes(b) => {
            out.push(tag::BYTES);
            put_bytes(out, b);
        }
        RivetValue::Array(items) => {
            out.push(tag::ARRAY);
            put_len(out, items.len());
            for it in items {
                put_value(out, it);
            }
        }
    }
}

fn get_value(c: &mut Cur<'_>) -> Result<RivetValue> {
    let t = c.u8()?;
    Ok(match t {
        tag::NULL => RivetValue::Null,
        tag::BOOL => RivetValue::Bool(c.u8()? != 0),
        tag::INT => RivetValue::Int(i64::from_be_bytes(c.take(8)?.try_into().expect("8"))),
        tag::UINT => RivetValue::UInt(u64::from_be_bytes(c.take(8)?.try_into().expect("8"))),
        tag::FLOAT => RivetValue::Float(f64::from_be_bytes(c.take(8)?.try_into().expect("8"))),
        tag::DATETIME => {
            let secs = i64::from_be_bytes(c.take(8)?.try_into().expect("8"));
            let nanos = u32::from_be_bytes(c.take(4)?.try_into().expect("4"));
            RivetValue::DateTime(
                chrono::DateTime::from_timestamp(secs, nanos)
                    .ok_or_else(|| {
                        anyhow::anyhow!("cdc spill: {secs}s + {nanos}ns is not a date-time")
                    })?
                    .naive_utc(),
            )
        }
        tag::TIME_MICROS => {
            RivetValue::TimeMicros(i64::from_be_bytes(c.take(8)?.try_into().expect("8")))
        }
        tag::BYTES => RivetValue::Bytes(c.bytes()?),
        tag::ARRAY => {
            let n = c.len()?;
            let mut items = Vec::with_capacity(n.min(4096));
            for _ in 0..n {
                items.push(get_value(c)?);
            }
            RivetValue::Array(items)
        }
        // NEVER a lenient default. An unknown tag means the frame and this build
        // disagree, and guessing NULL would turn that into a column of nulls that
        // every count and sum check passes — the exact silent-loss shape a
        // "degrade to null" cell path produces.
        other => anyhow::bail!("cdc spill: unknown value tag {other}"),
    })
}

/// Encode one event for the spill — lossless, and type-decision-free.
pub(crate) fn encode_event(ev: &ChangeEvent) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.push(match ev.op {
        ChangeOp::Insert => 0,
        ChangeOp::Update => 1,
        ChangeOp::Delete => 2,
    });
    put_str(&mut out, &ev.schema);
    put_str(&mut out, &ev.table);
    // `position` and `committed` are re-stamped at the commit boundary by the
    // framer, and carried anyway: a spill that dropped them would be a second place
    // where the transaction's framing is decided.
    put_str(&mut out, &ev.position.0.to_string());
    out.push(u8::from(ev.committed));
    out.extend_from_slice(&ev.seq.to_be_bytes());
    put_opt_str(&mut out, ev.poison.as_deref());
    match ev.image_names.as_deref() {
        None => out.push(0),
        Some(names) => {
            out.push(1);
            put_len(&mut out, names.len());
            for n in names {
                put_str(&mut out, n);
            }
        }
    }
    put_image(&mut out, ev.before.as_deref());
    put_image(&mut out, ev.after.as_deref());
    out
}

fn put_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => out.push(0),
        Some(v) => {
            out.push(1);
            put_str(out, v);
        }
    }
}

/// An image is `None` (absent) or a list of values — and the two are DISTINCT.
///
/// `None` means the engine sent no such image (a DELETE has no after-image); an
/// EMPTY list means it sent one with no columns. Encoding both as "zero values"
/// would let a spilled DELETE come back looking like an INSERT of nothing.
fn put_image(out: &mut Vec<u8>, img: Option<&[RivetValue]>) {
    match img {
        None => out.push(0),
        Some(vs) => {
            out.push(1);
            put_len(out, vs.len());
            for v in vs {
                put_value(out, v);
            }
        }
    }
}

fn get_image(c: &mut Cur<'_>) -> Result<Option<Vec<RivetValue>>> {
    if c.u8()? == 0 {
        return Ok(None);
    }
    let n = c.len()?;
    let mut vs = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        vs.push(get_value(c)?);
    }
    Ok(Some(vs))
}

/// Inverse of [`encode_event`], refusing anything it cannot read exactly.
pub(crate) fn decode_event(rec: &[u8]) -> Result<ChangeEvent> {
    let mut c = Cur { b: rec, at: 0 };
    let op = match c.u8()? {
        0 => ChangeOp::Insert,
        1 => ChangeOp::Update,
        2 => ChangeOp::Delete,
        other => anyhow::bail!("cdc spill: unknown op tag {other}"),
    };
    let schema = c.string()?;
    let table = c.string()?;
    let position = Position(
        serde_json::from_str(&c.string()?)
            .map_err(|e| anyhow::anyhow!("cdc spill: the position is not json: {e}"))?,
    );
    let committed = c.u8()? != 0;
    let seq = u64::from_be_bytes(c.take(8)?.try_into().expect("8"));
    let poison = if c.u8()? == 0 {
        None
    } else {
        Some(c.string()?)
    };
    let image_names: Option<std::sync::Arc<[String]>> = if c.u8()? == 0 {
        None
    } else {
        let n = c.len()?;
        let mut names = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            names.push(c.string()?);
        }
        Some(names.into())
    };
    let before = get_image(&mut c)?;
    let after = get_image(&mut c)?;
    // TRAILING BYTES are an error, not slack. A frame this build reads as complete
    // while the writer wrote more means the two disagree about the layout, and
    // accepting it delivers a silently truncated event.
    if c.at != rec.len() {
        anyhow::bail!(
            "cdc spill: {} trailing byte(s) after a complete event — the frame and \
             this build disagree about the layout",
            rec.len() - c.at
        );
    }
    Ok(ChangeEvent {
        op,
        schema,
        table,
        before,
        after,
        position,
        committed,
        image_names,
        seq,
        poison,
    })
}

#[cfg(test)]
mod frame_tests {
    use super::*;
    use serde_json::json;

    fn ev() -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Update,
            schema: "public".into(),
            table: "orders".into(),
            before: None,
            after: None,
            position: Position(json!({ "lsn": "0/ABC" })),
            committed: false,
            image_names: None,
            // NON-ZERO on purpose. With `seq: 0` everywhere, a codec that drops the
            // field entirely round-trips perfectly — measured, the mutant was
            // unkillable until this line changed. Same class as a one-row fixture
            // hiding accumulation arithmetic.
            seq: 7,
            poison: None,
        }
    }

    /// Compare the DECODED event against the ORIGINAL, field by field.
    ///
    /// The first cut compared `encode(decode(encode(e)))` to `encode(e)` and was a
    /// self-oracle: a LOSSY encode agrees with itself perfectly. Proven — dropping
    /// the sub-second part of a date-time left it green, because the re-encode
    /// dropped the same nanoseconds. The oracle has to be the value the test built,
    /// which is the one thing the codec did not produce.
    ///
    /// `ChangeEvent` has no `PartialEq`, so the fields are compared one by one —
    /// and every field is named here on purpose: a new field defaulted by the
    /// decoder shows up as a compile error in this destructuring, not as a silent
    /// pass.
    fn round_trips(e: &ChangeEvent) {
        let back = decode_event(&encode_event(e)).expect("decode");
        let ChangeEvent {
            op,
            schema,
            table,
            before,
            after,
            position,
            committed,
            image_names,
            seq,
            poison,
        } = &back;
        assert_eq!(op.as_str(), e.op.as_str(), "op");
        assert_eq!(schema, &e.schema, "schema");
        assert_eq!(table, &e.table, "table");
        assert_eq!(position.0, e.position.0, "position");
        assert_eq!(committed, &e.committed, "committed");
        assert_eq!(seq, &e.seq, "seq");
        assert_eq!(poison, &e.poison, "poison");
        assert_eq!(
            image_names.as_deref(),
            e.image_names.as_deref(),
            "image_names"
        );
        // NaN != NaN, so the images are compared through their DEBUG rendering,
        // which distinguishes NaN from a number, -0.0 from 0.0, and a NULL element
        // from an empty one — everything `PartialEq` gets right plus the one case
        // it deliberately does not.
        assert_eq!(format!("{before:?}"), format!("{:?}", e.before), "before");
        assert_eq!(format!("{after:?}"), format!("{:?}", e.after), "after");
    }

    /// EVERY `RivetValue` variant survives, including the ones a text rendering
    /// loses.
    ///
    /// A new variant added without a case here is a spill that decodes it as an
    /// error at best and silently as something else at worst — the reason the tags
    /// are explicit discriminants rather than declaration order.
    #[test]
    fn every_value_variant_round_trips() {
        let values = vec![
            RivetValue::Null,
            RivetValue::Bool(true),
            RivetValue::Bool(false),
            RivetValue::Int(i64::MIN),
            RivetValue::Int(-1),
            RivetValue::UInt(u64::MAX),
            // The two zeros and NaN: a decimal RENDERING collapses -0.0 into 0.0
            // and cannot express NaN, which is why the bit pattern is stored.
            RivetValue::Float(-0.0),
            RivetValue::Float(0.0),
            RivetValue::Float(f64::NAN),
            RivetValue::Float(f64::NEG_INFINITY),
            RivetValue::DateTime(
                chrono::DateTime::from_timestamp(-1, 999_999_999)
                    .expect("pre-epoch with nanos")
                    .naive_utc(),
            ),
            RivetValue::TimeMicros(-1),
            RivetValue::Bytes(Vec::new()),
            // Invalid UTF-8 and an embedded NUL: a cell path that went through a
            // string would lose both.
            RivetValue::Bytes(vec![0x00, 0xff, 0xc3, 0x28]),
            RivetValue::Array(Vec::new()),
            // A NULL *inside* an array is not the same as an absent element —
            // Arrow's container display renders both as the empty string, which is
            // exactly how the row-hash lost them.
            RivetValue::Array(vec![RivetValue::Null, RivetValue::Bytes(b"a".to_vec())]),
            // Nested, so the recursion is exercised past one level.
            RivetValue::Array(vec![RivetValue::Array(vec![RivetValue::Int(1)])]),
        ];
        for v in &values {
            let mut e = ev();
            e.after = Some(vec![v.clone()]);
            round_trips(&e);
        }
        // EVERY op, not just the fixture's. A codec that maps DELETE onto INSERT
        // resurrects the row it was deleting, and one op in the fixture cannot see
        // it (measured — `ChangeOp::Delete => 0` was unkillable).
        for op in [ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete] {
            let mut e = ev();
            e.op = op;
            e.before = Some(vec![RivetValue::Int(1)]);
            round_trips(&e);
        }
        // …and all of them in ONE image, so a per-value cursor bug that only shows
        // when a value FOLLOWS another is reachable.
        let mut e = ev();
        e.after = Some(values);
        round_trips(&e);
    }

    /// Two different events NEVER encode alike.
    ///
    /// This is the injectivity question, and one field cannot express it: the ways a
    /// value imitates a DELIMITER, a LENGTH, or ABSENCE all need a neighbour to
    /// imitate it against. Each pair below is one of those three.
    #[test]
    fn distinct_events_have_distinct_frames() {
        let mut cases: Vec<(&str, ChangeEvent)> = Vec::new();

        // DELIMITER: the split between two adjacent strings must not be forgeable.
        let mut a = ev();
        (a.schema, a.table) = ("a".into(), "bc".into());
        cases.push(("schema a | table bc", a));
        let mut b = ev();
        (b.schema, b.table) = ("ab".into(), "c".into());
        cases.push(("schema ab | table c", b));

        // ABSENCE: an image the engine never sent vs one it sent empty. A DELETE
        // carries no after-image; conflating the two makes it an INSERT of nothing.
        let mut c = ev();
        c.after = None;
        cases.push(("after absent", c));
        let mut d = ev();
        d.after = Some(Vec::new());
        cases.push(("after empty", d));

        // ABSENCE again, one level down: no value vs a NULL value vs an empty one.
        for (name, v) in [
            ("after [null]", RivetValue::Null),
            ("after [empty bytes]", RivetValue::Bytes(Vec::new())),
            ("after [empty array]", RivetValue::Array(Vec::new())),
        ] {
            let mut e = ev();
            e.after = Some(vec![v]);
            cases.push((name, e));
        }

        // LENGTH: one array of two vs two arrays of one — same elements, different
        // structure.
        let mut f = ev();
        f.after = Some(vec![RivetValue::Array(vec![
            RivetValue::Int(1),
            RivetValue::Int(2),
        ])]);
        cases.push(("after [[1,2]]", f));
        let mut g = ev();
        g.after = Some(vec![
            RivetValue::Array(vec![RivetValue::Int(1)]),
            RivetValue::Array(vec![RivetValue::Int(2)]),
        ]);
        cases.push(("after [[1],[2]]", g));

        // The remaining optional fields, each absent vs present-and-empty.
        let mut h = ev();
        h.poison = Some(String::new());
        cases.push(("poison empty", h));
        let mut i = ev();
        i.image_names = Some(Vec::<String>::new().into());
        cases.push(("names empty", i));
        let mut j = ev();
        j.committed = true;
        cases.push(("committed", j));
        // Each op is a distinct change: a DELETE that shares a frame with an INSERT
        // of the same row is a row that comes back from the dead.
        // UPDATE is the base fixture's own op, so only the other two are added —
        // a third case identical to the base makes this test fail on itself, which
        // is how this comment exists.
        for (n, op) in [
            ("op insert", ChangeOp::Insert),
            ("op delete", ChangeOp::Delete),
        ] {
            let mut e = ev();
            e.op = op;
            cases.push((n, e));
        }
        // …and a different `seq` is a different change, since `(__pos, __seq)` is
        // the total order the load dedup sorts by.
        let mut k = ev();
        k.seq = 8;
        cases.push(("seq 8", k));

        for (i, (n1, e1)) in cases.iter().enumerate() {
            round_trips(e1);
            for (j, (n2, e2)) in cases.iter().enumerate() {
                if i == j {
                    continue;
                }
                assert_ne!(
                    encode_event(e1),
                    encode_event(e2),
                    "`{n1}` and `{n2}` encode identically — two different changes \
                     that share one frame is a change the spill cannot give back"
                );
            }
        }
    }

    /// A frame that is short, long, or carries an unknown tag is REFUSED.
    ///
    /// Never a lenient default: an unknown tag decoded as NULL would turn a version
    /// disagreement into a column of nulls that every count and sum check passes.
    #[test]
    fn a_frame_that_does_not_decode_exactly_is_refused() {
        let mut e = ev();
        e.after = Some(vec![RivetValue::Int(7)]);
        let full = encode_event(&e);

        for cut in 1..full.len() {
            assert!(
                decode_event(&full[..cut]).is_err(),
                "a frame truncated to {cut} of {} bytes decoded as a whole event — \
                 half a change is worse than none",
                full.len()
            );
        }
        let mut long = full.clone();
        long.push(0);
        assert!(
            decode_event(&long).is_err(),
            "trailing bytes mean the writer and this build disagree about the \
             layout; accepting them delivers a silently truncated event"
        );
        // A one-value image whose only value is NULL ends with the TAG byte, so
        // flipping the last byte really does forge an unknown variant. (The first
        // cut flipped the last byte of an `Int`, which only changes the number —
        // the mutation has to land on the tag to test the tag.)
        let mut null_ev = ev();
        null_ev.after = Some(vec![RivetValue::Null]);
        let mut bad_tag = encode_event(&null_ev);
        assert_eq!(
            bad_tag.last().copied(),
            Some(tag::NULL),
            "the fixture must END on the value tag, or this asserts nothing"
        );
        *bad_tag.last_mut().expect("non-empty") = 200;
        assert!(
            decode_event(&bad_tag).is_err(),
            "an unknown value tag must ERROR, never degrade to NULL"
        );
        assert!(decode_event(&[]).is_err(), "an empty frame is not an event");
        assert!(
            decode_event(&[99]).is_err(),
            "an unknown op tag must ERROR — a spilled DELETE read as an INSERT \
             would resurrect the row it was deleting"
        );
    }

    /// A drained tail carries the commit position on every row and `committed` on
    /// exactly ONE — the last.
    ///
    /// This grades `SpooledTx` itself, which the live tests cannot: they count rows,
    /// and every arrangement of the flag delivers the same rows. What the flag
    /// decides is when the sink ROLLS — a `committed` on an early row lets it flush,
    /// checkpoint and ack MID-transaction, and a crash before the rest is written
    /// advances the resume position past the commit. The rows are all present right
    /// up until the crash that loses them.
    ///
    /// Measured: stamping every tail row committed was unkillable by the live suite.
    #[test]
    fn a_drained_tail_closes_on_its_last_row_only() {
        let d = tempfile::tempdir().expect("dir");
        // THREE rows: with one, "first" and "last" are the same row; with two, so
        // are "every" and "the last two".
        const N: usize = 3;
        let mut f = SpillFile::create(d.path(), "tail").expect("create");
        for i in 0..N {
            let mut e = ev();
            e.after = Some(vec![RivetValue::Int(i as i64)]);
            // Poisoned, so the stamp has to CLEAR it rather than merely leave it.
            e.committed = true;
            f.push(&encode_event(&e)).expect("push");
        }
        let commit = Position(json!({ "lsn": "0/FEED" }));
        let mut sp = SpooledTx::new(f.into_reader().expect("seal"), commit.clone());

        let mut flags = Vec::new();
        while let Some(e) = sp.next_event(decode_event).expect("decode") {
            assert_eq!(
                e.position.0, commit.0,
                "every row of a spilled tail carries the transaction's COMMIT \
                 position — the resume position depends on it"
            );
            flags.push(e.committed);
        }
        assert_eq!(flags.len(), N, "the whole tail must come back");
        assert_eq!(
            flags,
            vec![false, false, true],
            "exactly the LAST row closes the transaction; a flag anywhere else \
             lets the sink roll and ack mid-transaction"
        );
        assert_eq!(sp.remaining(), 0);
    }

    /// A spilled tail holding SEVERAL transactions closes each one on ITS last row.
    ///
    /// SQL Server's poll buffer is not one transaction: it is a batch of runs
    /// sharing a `__$start_lsn`. A tail that marked only its final row committed
    /// would fuse every spilled transaction into one — the sink would then hold them
    /// all before rolling, and a crash would replay the lot. A tail that marked
    /// every row would roll mid-transaction, which is the at-least-once break.
    ///
    /// The fixture is 2-1-2, so it crosses three thresholds a flatter one misses: a
    /// group of more than one row (a boundary exists to get wrong), a group of
    /// EXACTLY one (first and last are the same row), and a group boundary that is
    /// not the end of the tail.
    #[test]
    fn a_spilled_batch_closes_each_transaction_on_its_own_last_row() {
        let d = tempfile::tempdir().expect("dir");
        let lsns = ["0/A", "0/A", "0/B", "0/C", "0/C"];
        let mut f = SpillFile::create(d.path(), "groups").expect("create");
        for (i, l) in lsns.iter().enumerate() {
            let mut e = ev();
            e.position = Position(json!({ "lsn": l }));
            e.after = Some(vec![RivetValue::Int(i as i64)]);
            // Poisoned, so the drain has to CLEAR it rather than leave it.
            e.committed = true;
            f.push(&encode_event(&e)).expect("push");
        }
        let mut sp =
            SpooledGroups::new(f.into_reader().expect("seal"), decode_event).expect("prime");
        assert_eq!(
            sp.first_position().map(|p| p.0["lsn"].as_str()),
            Some(Some("0/A")),
            "the caller needs the tail's FIRST position to know whether the \
             in-memory head's last group continues onto disk"
        );
        assert_eq!(sp.remaining(), lsns.len());

        let mut got = Vec::new();
        while let Some(e) = sp.next_event(decode_event).expect("decode") {
            got.push((
                e.position.0["lsn"].as_str().expect("lsn").to_string(),
                e.committed,
            ));
        }
        assert_eq!(
            got,
            vec![
                ("0/A".into(), false),
                ("0/A".into(), true),
                ("0/B".into(), true),
                ("0/C".into(), false),
                ("0/C".into(), true),
            ],
            "each transaction closes on its OWN last row — not once at the end of \
             the tail (which fuses them) and not on every row (which rolls \
             mid-transaction)"
        );
        assert_eq!(sp.remaining(), 0);
    }

    /// The group-boundary predicate, at its two ends.
    #[test]
    fn a_row_closes_its_group_when_nothing_of_it_follows() {
        let a = Position(json!({ "lsn": "0/A" }));
        let b = Position(json!({ "lsn": "0/B" }));
        assert!(closes_group(&a, None), "the last row always closes");
        assert!(
            closes_group(&a, Some(&b)),
            "a different transaction follows"
        );
        assert!(
            !closes_group(&a, Some(&a.clone())),
            "the same transaction continues — closing here lets the sink ack \
             mid-transaction, and a crash before the rest advances the resume past \
             the commit"
        );
    }
}
