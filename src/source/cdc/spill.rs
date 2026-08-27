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
