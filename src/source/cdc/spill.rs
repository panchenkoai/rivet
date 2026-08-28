//! Spilling a buffered transaction to disk — the frame, shared by both encodings.
//!
//! A transaction is buffered WHOLE (it is never split across parts, which is what
//! makes a crash resume transaction-atomic), so a large one used to be REFUSED:
//! `check_tx_buffer_caps` bails, and capture stops until someone splits the source
//! transaction or raises a cap. That is a memory ceiling wearing a refusal.
//!
//! MEASURED LIMIT, so nobody has to rediscover it: this bounds the ADAPTER's copy
//! and not the run. `sink::RolloverPolicy::should_roll` requires a `committed`
//! event — the "never split a transaction across parts" invariant that makes crash
//! resume transaction-atomic — so the sink buffers a whole transaction whatever the
//! adapter does. A 100k-row transaction peaked at 202 MB with spilling and 226 MB
//! without: ~11%, not a ceiling. What this DOES buy is that the run no longer fails
//! outright, which was the point. An end-to-end ceiling needs the SINK to spill too,
//! and that is a separate piece of work — the soak stand
//! (`tests/live/soak_spill.rs`) is where the two numbers are produced, so the claim
//! stays honest as the code changes.
//!
//! This is the container, not the encoding. Two encodings ride on it and the choice
//! is per adapter:
//!
//! * **raw wire bytes** where the engine gives them — PostgreSQL only: its
//!   `test_decoding` rows arrive as text, so there is nothing to encode and the
//!   same parser reads them back. (NOT `pgoutput` — that module is staged, unused
//!   here — and NOT MongoDB, which spills nothing by construction: it has no
//!   transaction buffer, every event is its own commit, and it serves as the soak
//!   stand's control. An earlier header listed both wrongly; the mid-file
//!   encoding note already told the true story while this summary contradicted
//!   it, which is exactly how a contributor gets sent the wrong way.) SQL Server
//!   has no wire to keep (its "stream" is a query result set) and MySQL's crate
//!   hands over parsed events, so neither can use this path;
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
#[derive(Debug)]
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

/// This process's spill identity: `{pid}-{startup-hex}`.
///
/// The pid ALONE was the identity, and two container generations are both pid 1 —
/// so a name could collide across namespaces (`create` then truncated a live
/// neighbour's inode through the shared volume) and the sweep's pid probe read a
/// foreign namespace's live writer as dead-or-self. The startup stamp makes the
/// name unique per PROCESS INSTANCE, not per pid number; the pid stays in front
/// for traceability.
fn process_token() -> &'static str {
    static TOKEN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    TOKEN.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64 ^ (d.as_secs() << 20))
            .unwrap_or(0);
        format!("{}-{nanos:x}", std::process::id())
    })
}

/// The pid a spill file's name carries, if it is one of ours.
///
/// The label can itself hold dashes (`pg-tx`, `mssql-batch`), so the pid is the
/// segment between the LAST dash and the extension — not the third field.
pub(crate) fn spill_file_pid(name: &str) -> Option<u32> {
    let core = name.strip_prefix("rivet-spill-")?.strip_suffix(".bin")?;
    // `{label}-{seq}-{pid}-{startup}`: the pid is SECOND from the end — the
    // startup token took the last slot when pid alone stopped being an identity
    // (two containers on one volume are both pid 1). A name without the token
    // (the short-lived pre-token shape) still parses via the fallback, so a
    // dev-machine orphan from that window is recognised rather than immortal.
    let (rest, last) = core.rsplit_once('-')?;
    if let Some((_, pid)) = rest.rsplit_once('-')
        && let Ok(p) = pid.parse()
    {
        return Some(p);
    }
    last.parse().ok()
}

/// Remove spill files left behind by a process that is no longer running.
///
/// `Drop` does not run on SIGKILL, so a hard-killed rivet leaves its spill on disk —
/// one per oversized transaction, which fills a disk on a scheduler. The name
/// carries the writer's pid, and pid LIVENESS is the authoritative, clock-free
/// discriminator: the spill directory is local, so the pid is local too.
///
/// Deliberately one-directional about doubt. A pid number can be REUSED by an
/// unrelated process, and in that case this spares a file that is genuinely dead —
/// wasted disk, which a later sweep under a different pid landscape reclaims.
/// Deleting a file a LIVE writer is still appending to would tear a transaction in
/// half. Same rule as the orphan GC: gate the ambiguous case on a lifecycle signal,
/// and where it is unclear, spare.
pub(crate) fn sweep_dead_spills(dir: &std::path::Path, is_alive: impl Fn(u32) -> bool) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return; // no directory yet is not an error — there is nothing to sweep
    };
    for e in rd.flatten() {
        let path = e.path();
        let Some(pid) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(spill_file_pid)
        else {
            continue; // not ours; never touch a file we did not name
        };
        let _ = pid; // recognised as OURS by the name shape; liveness comes below
        // The discriminator is the WRITER-HELD FLOCK, not pid liveness. Pid
        // numbers do not survive namespaces (two containers on one volume are
        // both pid 1 — the old probe read a live neighbour as "self", i.e. an
        // orphan, and deleted its in-flight spill) and are recycled within one.
        // The lock is a true lifecycle record: held while any writer or reader
        // owns the fd, released by the kernel on any death including SIGKILL.
        // `is_alive` remains the fallback where flock does not exist.
        if spill_is_held(&path, &is_alive, pid) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => log::info!("cdc spill: removed an orphan left by pid {pid}"),
            // Best effort: a sweep that failed a run would turn tidy-up into an
            // outage, and a file it could not remove costs disk, not correctness.
            Err(err) => log::debug!("cdc spill: could not remove {}: {err}", path.display()),
        }
    }
}

/// Is this spill file still OWNED — i.e. does a live writer/reader hold its lock?
///
/// `flock` where it exists (unix): try to take the lock; failure means a live
/// owner in ANY pid namespace, success means the kernel already released it —
/// the owner is dead, whatever its pid number said. Elsewhere, fall back to the
/// pid probe, sparing on doubt.
fn spill_is_held(path: &std::path::Path, is_alive: &impl Fn(u32) -> bool, pid: u32) -> bool {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        match std::fs::OpenOptions::new().read(true).open(path) {
            Ok(f) => {
                let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                // rc == 0: we got the lock — nobody held it — the file is an
                // orphan (the lock dies with `f` at the end of this scope).
                rc != 0
            }
            // Unreadable: cannot prove it is dead — spare, the safe direction.
            Err(_) => {
                let _ = (is_alive, pid);
                true
            }
        }
    }
    #[cfg(not(unix))]
    {
        is_alive(pid)
    }
}

/// Whether a local process is running.
///
/// `kill(pid, 0)` sends no signal and reports reachability. `EPERM` means the
/// process EXISTS and is not ours, which is still alive — reading it as dead would
/// delete another user's in-flight spill.
/// ONE function with the platform split INSIDE, not two `#[cfg]` siblings: as two
/// functions, the `not(unix)` stub was a separate mutation target that no machine
/// running mutants can ever compile IN — a permanently-missed mutant that invited
/// an exclusion, and the first exclusion written for it matched the unix arm's
/// mutants too. Off unix there is no portable liveness probe, and a wrong answer
/// here DELETES data, so the answer is "alive": the sweep does nothing rather than
/// guessing — a leaked spill costs disk; a deleted live one tears a transaction.
fn pid_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
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
        // Per DIRECTORY, not per process: a run with two CDC exports whose
        // checkpoints live in different directories spills into both, and a
        // process-wide `Once` (the first version) swept only whichever came
        // first — the second directory's orphans lived forever. A readdir per
        // spill-open is cheap next to the spill itself.
        {
            static SWEPT: std::sync::Mutex<Option<std::collections::BTreeSet<PathBuf>>> =
                std::sync::Mutex::new(None);
            let mut seen = SWEPT
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if seen
                .get_or_insert_with(Default::default)
                .insert(dir.to_path_buf())
            {
                sweep_dead_spills(dir, pid_is_alive);
            }
        }
        // `{label}-{seq}-{pid}-{startup}`: the counter makes the name unique
        // WITHIN a process, the token across PROCESS INSTANCES — two containers
        // sharing a checkpoint volume are both pid 1, so a pid-only name collided
        // across namespaces and `truncate` then destroyed a live neighbour's
        // spilled transaction through the shared inode.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = dir.join(format!("rivet-spill-{label}-{seq}-{}.bin", process_token()));
        // read+write, not `File::create`: `drain` rewinds and reads the same
        // handle back, and a write-only descriptor fails there with `Bad file
        // descriptor` — which reads as a TRUNCATED log, i.e. the one error this
        // type raises to mean "your transaction is torn". And `create_new`, never
        // `truncate`: a name collision must be an ERROR, not a silent truncation
        // of whoever owns the existing inode. With the startup token in the name
        // a collision means a bug, and a bug must be loud.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        // The WRITER-HELD LOCK is the liveness signal the sweep reads. Pid
        // liveness cannot cross a pid namespace (two containers on one volume are
        // both pid 1: a live neighbour probed as "self" or as "dead"), and the
        // GC rule is that ambiguity gates on a LIFECYCLE signal, never a clock —
        // flock IS that signal: held for the file's whole life (the fd passes
        // into the reader), released by the kernel on ANY death including
        // SIGKILL, and visible through shared volumes.
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc != 0 {
                anyhow::bail!(
                    "cdc spill: {} exists AND is locked by a live writer — the \
                     startup-token name should have made this impossible; refusing \
                     to touch it",
                    path.display()
                );
            }
        }
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
        // Exactly `len()` pulls, then assert DONE — never `while let Some(..)`.
        // The unbounded loop turned a "reader always answers Some" mutant into an
        // infinite hang: twelve mutants survived as TIMEOUTs, a CI stall where an
        // assertion should be. The count is the writer's own, so a reader that
        // disagrees with it in either direction fails here, loudly.
        let mut out = Vec::with_capacity(r.len());
        for _ in 0..r.len() {
            out.push(r.next_record().expect("len() promises this record")?);
        }
        assert!(
            r.next_record().is_none(),
            "the reader must be DONE after len() records — more means the count \
             and the file disagree"
        );
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

    /// Two spills open at once never share a path.
    ///
    /// `create` TRUNCATES, so a shared name means the second spill destroys the
    /// first's transaction while its reader still holds the file — silent, and
    /// silent in the worst way: the reader's `read_exact` then fails as a TORN
    /// LOG, which is the error that means "your transaction is half gone".
    ///
    /// One process drives one CDC stream today, so label+pid would not actually
    /// collide. This pins the property rather than the circumstance.
    #[test]
    fn two_spills_in_one_process_do_not_share_a_file() {
        let d = dir();
        let mut a = SpillFile::create(d.path(), "pg-tx").expect("a");
        let mut b = SpillFile::create(d.path(), "pg-tx").expect("b");
        assert_ne!(
            a.path, b.path,
            "same label, same pid — the names must differ"
        );
        a.push(b"aaa").expect("push a");
        b.push(b"bbbb").expect("push b");
        assert_eq!(a.drain().expect("drain a"), vec![b"aaa".to_vec()]);
        assert_eq!(b.drain().expect("drain b"), vec![b"bbbb".to_vec()]);
    }

    /// The sweep still finds the pid after the counter was added to the name.
    #[test]
    fn a_counted_spill_name_still_yields_the_writer_pid() {
        let d = dir();
        let s = SpillFile::create(d.path(), "mssql-batch").expect("create");
        let name = s
            .path
            .as_ref()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .expect("named")
            .to_string();
        assert_eq!(
            spill_file_pid(&name),
            Some(std::process::id()),
            "the pid must stay the LAST field, or the sweep stops recognising \
             rivet's own files and every orphan lives forever: {name}"
        );
    }

    /// `create` REFUSES an existing path — it must never truncate whoever owns
    /// the inode. The pre-token behaviour (`truncate(true)`) destroyed a live
    /// neighbour's spilled transaction through a shared volume when two pid-1
    /// containers collided on `{label}-{seq}-{pid}`.
    ///
    /// The SEQ is process-global and every parallel test advances it, so the
    /// "plant the next name" fixture retries until its prediction lands — a
    /// missed prediction is a sibling test racing, not a failure.
    #[test]
    fn create_refuses_an_existing_path_rather_than_truncating_it() {
        let d = dir();
        for _ in 0..50 {
            // Probe the counter: this create's name carries the CURRENT seq.
            let probe = SpillFile::create(d.path(), "collide").expect("probe");
            let probe_name = probe
                .path
                .as_ref()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .expect("named")
                .to_string();
            let (head, tail) = probe_name.split_once("collide-").expect("label");
            let (seq, rest) = tail.split_once('-').expect("seq");
            let next: u64 = seq.parse::<u64>().expect("numeric") + 1;
            let victim = d.path().join(format!("{head}collide-{next}-{rest}"));
            std::fs::write(&victim, b"someone else's bytes").expect("plant");

            match SpillFile::create(d.path(), "collide") {
                Err(err) => {
                    assert!(
                        err.downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::AlreadyExists),
                        "the refusal must be the collision, got: {err:#}"
                    );
                    assert_eq!(
                        std::fs::read(&victim).expect("re-read"),
                        b"someone else's bytes",
                        "the existing inode must be untouched — truncating it was \
                         the loss"
                    );
                    return;
                }
                Ok(second) => {
                    // A sibling test consumed our predicted seq — clean up, retry.
                    drop(second);
                    let _ = std::fs::remove_file(&victim);
                }
            }
        }
        panic!("50 predictions raced away — the fixture never landed");
    }

    /// An UNLOCKED spill is collected; a LOCKED one is spared; a foreign file is
    /// untouched.
    ///
    /// The discriminator is the writer-held flock, because pid liveness cannot
    /// cross a pid namespace: two containers sharing a checkpoint volume are both
    /// pid 1, and the pid probe read a live neighbour as "self" — an orphan — and
    /// deleted its in-flight spill. The lock is a true lifecycle record: held
    /// while any writer/reader owns the fd, released by the kernel on any death
    /// including SIGKILL, visible through shared volumes.
    #[cfg(unix)]
    #[test]
    fn a_sweep_collects_unlocked_spills_and_spares_locked_ones() {
        use std::os::fd::AsRawFd as _;
        let d = dir();
        let token = "9999-cafef00d"; // a foreign process instance's token
        let live = d.path().join(format!("rivet-spill-pg-tx-0-{token}.bin"));
        let dead = d
            .path()
            .join(format!("rivet-spill-mssql-batch-1-{token}.bin"));
        let foreign = d.path().join("some-other-tool.bin");
        for f in [&live, &dead, &foreign] {
            std::fs::write(f, b"x").expect("stage");
        }
        // The "live" file's owner: a held flock, exactly what a writer holds.
        let holder = std::fs::OpenOptions::new()
            .read(true)
            .open(&live)
            .expect("open the live file");
        assert_eq!(
            unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0,
            "the fixture must actually HOLD the lock, or the spare arm is vacuous"
        );

        sweep_dead_spills(d.path(), |_| false);
        assert!(
            live.exists(),
            "a spill whose lock is HELD has a live owner in some pid namespace — \
             deleting it mid-append tears the transaction it holds"
        );
        assert!(
            !dead.exists(),
            "a spill nobody holds is an orphan: the kernel released its lock at \
             the owner's death, whatever pid number the name carries"
        );
        assert!(
            foreign.exists(),
            "a file rivet did not name is not rivet's to delete"
        );
        drop(holder);
    }

    /// Off unix the fallback is the injected pid probe, sparing on doubt.
    #[cfg(not(unix))]
    #[test]
    fn a_sweep_falls_back_to_the_pid_probe_off_unix() {
        let d = dir();
        let live = d.path().join("rivet-spill-pg-tx-0-111-aa.bin");
        let dead = d.path().join("rivet-spill-pg-tx-1-222-bb.bin");
        for f in [&live, &dead] {
            std::fs::write(f, b"x").expect("stage");
        }
        sweep_dead_spills(d.path(), |pid| pid == 111);
        assert!(live.exists() && !dead.exists());
    }

    /// The pid still parses out of the token-bearing name — second from the end —
    /// and the pre-token shape stays recognisable so its orphans are not immortal.
    #[test]
    fn a_spill_name_yields_the_writer_pid() {
        assert_eq!(
            spill_file_pid("rivet-spill-pg-tx-0-4242-1a2b3c.bin"),
            Some(4242),
            "the startup token holds the LAST slot; the pid is second from the end"
        );
        assert_eq!(
            spill_file_pid("rivet-spill-mssql-batch-7-9-deadbeef.bin"),
            Some(9)
        );
        // The short-lived pre-token shape.
        assert_eq!(spill_file_pid("rivet-spill-pg-tx-4242.bin"), Some(4242));
        assert_eq!(spill_file_pid("some-other-tool-9.bin"), None);
        assert_eq!(spill_file_pid("rivet-spill-pg-tx-4242.txt"), None);
    }

    /// The PRODUCTION probe, all three arms: alive, dead, and exists-but-foreign.
    ///
    /// The sweep tests above use an injected probe, which grades the sweep and says
    /// nothing about `kill(pid, 0)` itself. Each arm fails in its own direction: a
    /// probe that cannot see a LIVE process deletes a spill being written right now;
    /// one that reports a DEAD process alive never collects an orphan (the mutant
    /// `pid_is_alive -> true` survived the suite until the dead arm was added); and
    /// reading EPERM as dead deletes another user's in-flight spill.
    #[cfg(unix)]
    #[test]
    fn the_real_liveness_probe_tells_alive_dead_and_foreign_apart() {
        assert!(
            pid_is_alive(std::process::id()),
            "if the probe cannot see its own process, the sweep deletes every spill \
             it finds, including one being written right now"
        );

        // A pid the test KNOWS is dead: spawn a child, wait for it, probe its pid.
        // Reaping it first is what makes this deterministic — an un-reaped child is
        // a zombie, which `kill(pid, 0)` still reaches. (A recycled pid could in
        // principle be live again, but recycling within microseconds of the reap
        // would have to wrap the whole pid space; accepted.)
        let dead = std::process::Command::new("true")
            .status()
            .map(|_| ())
            .and_then(|()| {
                let child = std::process::Command::new("true").spawn()?;
                let pid = child.id();
                let mut child = child;
                child.wait()?;
                Ok(pid)
            })
            .expect("spawn+reap a short-lived child");
        assert!(
            !pid_is_alive(dead),
            "a reaped child's pid must read DEAD — a probe that says alive here \
             never collects any orphan, and the sweep exists for nothing"
        );

        // Pid 1 exists on every unix and is not ours, so an unprivileged probe gets
        // EPERM — which must read ALIVE: the process is real, merely foreign, and
        // deleting its spill would tear another user's transaction. (Under root the
        // call succeeds outright and the assertion holds through the other arm.)
        assert!(
            pid_is_alive(1),
            "EPERM means the process EXISTS — reading it as dead deletes another \
             user's in-flight spill"
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
// PostgreSQL spills the RAW WIRE bytes: the row arrived as text and the decoder
// that reads it back is the one the in-memory path uses, so a spilled row and a
// buffered one cannot decode differently. MySQL's crate hands over PARSED events
// and SQL Server's "stream" is a query result set — neither has a wire to keep, so
// their events need an encoding of their own. MongoDB spills NOTHING, by
// construction rather than omission: it has no transaction buffer at all (every
// change-stream event is its own commit), which is why it serves as the soak
// stand's CONTROL — an earlier version of this comment listed it beside
// PostgreSQL as a raw-BSON spiller, and a contributor adding Mongo buffering
// would have believed the encoding decision already covered it.
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

        // `remaining` must COUNT DOWN as the tail drains: the engines' `next_spooled`
        // drop the reader the moment it hits zero, so a `remaining` pinned at 0
        // (the surviving mutant) would drop the tail after its FIRST row — the rest
        // of the transaction silently gone. Only the final `== 0` was asserted, and
        // that is the one value the mutant agrees with.
        assert_eq!(sp.remaining(), N, "all rows still on disk before the drain");
        let mut flags = Vec::new();
        while let Some(e) = sp.next_event(decode_event).expect("decode") {
            assert!(
                flags.len() < N,
                "the tail answered a {}th event though it holds {N}",
                flags.len() + 1
            );
            assert_eq!(
                sp.remaining(),
                N - flags.len() - 1,
                "remaining must fall by exactly one per drained row"
            );
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
            assert!(
                got.len() < lsns.len(),
                "the tail answered a {}th event though it holds {}",
                got.len() + 1,
                lsns.len()
            );
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

/// MEASURED cost of spilling, not estimated — the repo's rule about latency claims
/// applies to size and throughput too.
///
/// `#[ignore]`d because it is a measurement, not an assertion about behaviour, and
/// it wants a release build to mean anything:
///     cargo test --release --lib spill_cost -- --ignored --nocapture
#[cfg(test)]
mod spill_cost {
    use super::*;
    use serde_json::json;

    /// Measurements must be ASKED for, not swept up. CI's offline job runs
    /// `cargo test -- --ignored`, which selects every `#[ignore]`d test at default
    /// parallelism — and `ru_maxrss` is a process-wide monotonic peak, so these
    /// arms racing each other (or any sibling) corrupt every delta and the budget
    /// assertion grades noise. Measured: 1497 B/event reported where a clean
    /// sequential run reports 772. The env var is the same opt-in shape as the
    /// fixture regenerator: a run that did not ask must not produce a verdict.
    fn measuring() -> bool {
        if std::env::var("RIVET_MEASURE").is_ok() {
            return true;
        }
        eprintln!(
            "SKIP: set RIVET_MEASURE=1 and run with --test-threads=1 — this is a \
             measurement, and swept up in a parallel --ignored pass it grades noise"
        );
        false
    }

    /// A row shaped like a real captured one: a key, some text, a timestamp, a
    /// decimal carried as bytes, a flag, and a wide column. A one-integer fixture
    /// would report a bytes-per-row that no real table produces.
    fn realistic(i: u64) -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Update,
            schema: "public".into(),
            table: "orders".into(),
            before: None,
            after: Some(vec![
                RivetValue::Int(i as i64),
                RivetValue::Bytes(format!("customer-{i}-acme-industries").into_bytes()),
                RivetValue::DateTime(
                    chrono::DateTime::from_timestamp(1_700_000_000 + i as i64, 0)
                        .expect("ts")
                        .naive_utc(),
                ),
                RivetValue::Bytes(b"12345.6789".to_vec()),
                RivetValue::Bool(i.is_multiple_of(2)),
                RivetValue::Bytes(vec![b'x'; 200]),
            ]),
            position: Position(json!({ "lsn": "0/16B2E00" })),
            committed: false,
            image_names: Some(std::sync::Arc::from(
                ["id", "name", "at", "amount", "ok", "pad"]
                    .map(String::from)
                    .to_vec(),
            )),
            seq: i,
            poison: None,
        }
    }

    /// Bytes ON DISK per row, against the in-memory footprint the cap counts.
    ///
    /// The ratio is the number that matters operationally: it says how much disk a
    /// given `RIVET_CDC_MAX_TX_BYTES` overflow actually costs.
    #[test]
    #[ignore = "measurement: run with --release --nocapture"]
    fn how_much_disk_does_a_spilled_row_take() {
        if !measuring() {
            return;
        }
        const N: u64 = 100_000;
        let d = tempfile::tempdir().expect("dir");
        let mut f = SpillFile::create(d.path(), "cost").expect("create");
        let mut in_memory = 0usize;
        for i in 0..N {
            let e = realistic(i);
            in_memory += e.estimated_bytes();
            f.push(&encode_event(&e)).expect("push");
        }
        let on_disk = f.bytes();
        println!(
            "spill cost: {N} rows | on disk {} B total, {:.1} B/row | in memory {} B \
             total, {:.1} B/row | ratio {:.2}x",
            on_disk,
            on_disk as f64 / N as f64,
            in_memory,
            in_memory as f64 / N as f64,
            on_disk as f64 / in_memory as f64,
        );
    }

    /// How fast the log is written and read back — the cost the spill adds to a
    /// transaction that would otherwise have failed the run outright.
    #[test]
    #[ignore = "measurement: run with --release --nocapture"]
    fn how_fast_is_a_spilled_transaction_written_and_parsed() {
        if !measuring() {
            return;
        }
        const N: u64 = 100_000;
        let d = tempfile::tempdir().expect("dir");
        let events: Vec<ChangeEvent> = (0..N).map(realistic).collect();

        let t0 = std::time::Instant::now();
        let mut f = SpillFile::create(d.path(), "speed").expect("create");
        for e in &events {
            f.push(&encode_event(e)).expect("push");
        }
        let wrote = t0.elapsed();
        let bytes = f.bytes();

        let t1 = std::time::Instant::now();
        let mut r = f.into_reader().expect("seal");
        let mut n = 0u64;
        let mut cells = 0usize;
        while let Some(rec) = r.next_record() {
            let e = decode_event(&rec.expect("record")).expect("decode");
            // Touch the decoded values so the work cannot be optimised away.
            cells += e.after.as_ref().map_or(0, Vec::len);
            n += 1;
        }
        let read = t1.elapsed();
        assert_eq!(n, N);
        assert_eq!(cells, N as usize * 6);
        println!(
            "spill speed: {N} rows, {} B | write {:?} ({:.0} rows/s) | \
             read+decode {:?} ({:.0} rows/s, {:.0} MB/s)",
            bytes,
            wrote,
            N as f64 / wrote.as_secs_f64(),
            read,
            N as f64 / read.as_secs_f64(),
            bytes as f64 / read.as_secs_f64() / 1e6,
        );
    }

    /// The same two numbers for PostgreSQL's RAW-ROW encoding, which is a different
    /// shape: no per-value framing at all, just the text the server sent.
    #[test]
    #[ignore = "measurement: run with --release --nocapture"]
    fn how_much_does_a_raw_wire_row_cost() {
        if !measuring() {
            return;
        }
        const N: u64 = 100_000;
        let d = tempfile::tempdir().expect("dir");
        let line = |i: u64| {
            format!(
                "table public.orders: UPDATE: id[integer]:{i} \
                 name[text]:'customer-{i}-acme-industries' \
                 at[timestamp]:'2023-11-14 22:13:20' amount[numeric]:12345.6789 \
                 ok[boolean]:true pad[text]:'{}'",
                "x".repeat(200)
            )
        };
        let mut f = SpillFile::create(d.path(), "raw").expect("create");
        let t0 = std::time::Instant::now();
        for i in 0..N {
            // Mirrors `postgres::cdc::encode_wire_row`: a u32 lsn length, the lsn,
            // then the row text.
            let (lsn, data) = ("0/16B2E00", line(i));
            let mut rec = Vec::with_capacity(4 + lsn.len() + data.len());
            rec.extend_from_slice(&(lsn.len() as u32).to_be_bytes());
            rec.extend_from_slice(lsn.as_bytes());
            rec.extend_from_slice(data.as_bytes());
            f.push(&rec).expect("push");
        }
        let wrote = t0.elapsed();
        let bytes = f.bytes();
        let t1 = std::time::Instant::now();
        let mut r = f.into_reader().expect("seal");
        let mut n = 0u64;
        while let Some(rec) = r.next_record() {
            let _ = rec.expect("record").len();
            n += 1;
        }
        let read = t1.elapsed();
        assert_eq!(n, N);
        println!(
            "raw-row cost: {N} rows | on disk {} B, {:.1} B/row | write {:?} | \
             read {:?} ({:.0} rows/s)",
            bytes,
            bytes as f64 / N as f64,
            wrote,
            read,
            N as f64 / read.as_secs_f64(),
        );
    }
}

/// What ONE buffered change actually costs in memory — measured, because the cap
/// that is supposed to bound it charges a number computed a different way.
///
///     cargo test --release --lib event_cost -- --ignored --nocapture
#[cfg(test)]
mod event_cost {
    use super::*;
    use serde_json::json;

    /// Measurements must be ASKED for, not swept up. CI's offline job runs
    /// `cargo test -- --ignored`, which selects every `#[ignore]`d test at default
    /// parallelism — and `ru_maxrss` is a process-wide monotonic peak, so these
    /// arms racing each other (or any sibling) corrupt every delta and the budget
    /// assertion grades noise. Measured: 1497 B/event reported where a clean
    /// sequential run reports 772. The env var is the same opt-in shape as the
    /// fixture regenerator: a run that did not ask must not produce a verdict.
    fn measuring() -> bool {
        if std::env::var("RIVET_MEASURE").is_ok() {
            return true;
        }
        eprintln!(
            "SKIP: set RIVET_MEASURE=1 and run with --test-threads=1 — this is a \
             measurement, and swept up in a parallel --ignored pass it grades noise"
        );
        false
    }

    #[cfg(target_os = "macos")]
    fn rss_bytes() -> u64 {
        // `ru_maxrss` is BYTES on macOS, KILOBYTES on Linux — one field, two
        // meanings, and reading it wrong is a 1024x error.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
            return 0;
        }
        ru.ru_maxrss.max(0) as u64
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    fn rss_bytes() -> u64 {
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
            return 0;
        }
        (ru.ru_maxrss.max(0) as u64) * 1024
    }

    fn narrow(i: u64, names: &std::sync::Arc<[String]>, pos: &Position) -> ChangeEvent {
        ChangeEvent {
            op: ChangeOp::Insert,
            schema: "public".into(),
            table: "orders".into(),
            before: None,
            after: Some(vec![
                RivetValue::Int(i as i64),
                RivetValue::Int(i as i64),
                RivetValue::Bytes(b"x".to_vec()),
            ]),
            position: pos.clone(),
            committed: false,
            image_names: Some(names.clone()),
            seq: i,
            poison: None,
        }
    }

    /// Where a buffered transaction's memory actually goes.
    ///
    /// The soak measures a whole process; this measures the events alone, so the
    /// cost can be ATTRIBUTED instead of guessed. Peak RSS is monotonic, so each
    /// arm runs from a fresh high-water mark by allocating the largest arm first
    /// and reporting deltas — read the per-event numbers, not the totals.
    #[test]
    #[ignore = "measurement: run with --release --nocapture"]
    fn what_does_one_buffered_change_actually_cost() {
        if !measuring() {
            return;
        }
        const N: u64 = 1_000_000;
        let names: std::sync::Arc<[String]> =
            std::sync::Arc::from(["id", "v", "pad"].map(String::from).to_vec());
        let pos = Position(json!({ "lsn": "0/16B2E00" }));

        println!(
            "size_of::<ChangeEvent>() = {} B (inline only — every String, Vec, Arc \
             and the position's JSON map are separate heap allocations on top)",
            std::mem::size_of::<ChangeEvent>()
        );

        let before = rss_bytes();
        let mut v: Vec<ChangeEvent> = Vec::with_capacity(N as usize);
        for i in 0..N {
            v.push(narrow(i, &names, &pos));
        }
        let after = rss_bytes();
        let per = (after - before) as f64 / N as f64;
        let estimated: usize = v.iter().map(ChangeEvent::estimated_bytes).sum();
        println!(
            "{N} narrow events: RSS +{:.0} MB -> {per:.0} B/event | \
             estimated_bytes says {:.0} B/event | the cap under-counts {:.1}x",
            (after - before) as f64 / 1e6,
            estimated as f64 / N as f64,
            per / (estimated as f64 / N as f64),
        );

        // Attribute the position: the framer stamps `ev.position = commit.clone()`
        // on EVERY event, and `Position` wraps a `serde_json::Value` — an object
        // whose map, key and value are separate allocations, cloned a million times
        // for one transaction. Sharing it would cost one.
        let mid = rss_bytes();
        let shared: Vec<serde_json::Value> = (0..N).map(|_| pos.0.clone()).collect();
        let end = rss_bytes();
        println!(
            "{N} cloned positions alone: RSS +{:.0} MB -> {:.0} B/clone",
            (end - mid) as f64 / 1e6,
            (end - mid) as f64 / N as f64,
        );
        // Touch both so nothing is optimised away.
        assert_eq!(v.len(), N as usize);
        assert_eq!(shared.len(), N as usize);

        // …and the BUDGETS' estimate must track the real cost, within 25% either
        // way. Both directions are failures, and this function produced both within
        // an hour: 12.7x UNDER (silent — `RIVET_CDC_MAX_TX_BYTES: 2 GiB` meant ~25
        // GiB of real memory, so the guard fired long after the machine was in
        // trouble), then 1.8x OVER (loud, and still wrong: it fails runs that were
        // fine). The constants in `json_resident_bytes` are pinned by nothing else.
        //
        // Asserted HERE rather than in its own test because `ru_maxrss` is a
        // process-wide monotonic peak: two measurements in one process run as
        // parallel threads and corrupt each other's deltas — which is exactly what
        // happened, reporting 1497 B/event where a clean run reports 772.
        let ratio = (estimated as f64 / N as f64) / per;
        println!(
            "budget check: charged {:.0} B/event vs real {per:.0} B/event — {ratio:.2}x",
            estimated as f64 / N as f64
        );
        assert!(
            (0.75..=1.25).contains(&ratio),
            "the memory estimate is {ratio:.2}x the real cost. A budget that does \
             not track what it budgets is not a budget."
        );
    }
}
