use crate::error::Result;

use super::StateStore;

/// One row from `file_log` (formerly `file_manifest`; renamed in schema v8).
#[derive(Debug, serde::Serialize)]
#[allow(dead_code)]
pub struct FileRecord {
    pub run_id: String,
    pub export_name: String,
    pub file_name: String,
    pub row_count: i64,
    pub bytes: i64,
    pub format: String,
    pub compression: Option<String>,
    pub created_at: String,
}

/// File log store — reads and writes `file_log`.
///
/// Historical note: this table was named `file_manifest` prior to schema v8.
/// The name was reclaimed for the 0.7.0 cloud-output JSON manifest contract;
/// the internal SQLite log was renamed to `file_log` to remove the overload.
///
/// Invariant I2 (Write Before Log) governs when `record_file` is called:
/// only after a destination write succeeds.  Failed writes produce no log entry.
/// Invariant I7 (File-Log Failure Is Non-Fatal) means callers use `let _ = record_file(...)`.
/// The seven facts about a part that `record_file` stores. Named, because seven
/// positional arguments — three of them same-typed strings in a row — say nothing
/// at the call site about which string is the export and which is the file.
pub struct FilePart<'a> {
    pub run_id: &'a str,
    pub export_name: &'a str,
    pub file_name: &'a str,
    pub rows: i64,
    pub bytes: i64,
    pub format: &'a str,
    pub compression: Option<&'a str>,
    /// v25: page high-water key (keyset cursor-atomic checkpoint); `None` for non-keyset parts.
    pub cursor_high: Option<&'a str>,
}

/// One part of one run, as it lands. A named value rather than eight
/// positional arguments — three of them same-typed strings in an order nothing
/// checks, which is exactly the mix-up a parameter object exists to prevent.
pub struct DurablePart<'a> {
    pub run_id: &'a str,
    pub export_name: &'a str,
    pub file_name: &'a str,
    pub rows: i64,
    pub bytes: i64,
    pub format: &'a str,
    pub compression: Option<&'a str>,
    /// The extraction strategy that produced it (`full`, `chunked`, `cdc`, …) —
    /// recorded on the run's aggregate, which may not exist yet when the first
    /// part lands.
    pub mode: &'a str,
    /// v25: the page's high-water key, for the keyset checkpoint's cursor-atomic resume — written
    /// in this SAME row as the part, so a resume reconciles the cursor from committed parts and
    /// never re-reads a committed page. `None` for every non-keyset-checkpoint part.
    pub cursor_high: Option<&'a str>,
}

impl StateStore {
    /// ONE part became durable: record the part AND the run's aggregate.
    ///
    /// "A part just landed" is a single moment in the domain, and the code said it
    /// in three production places — `commit.rs`, `parallel_checkpoint.rs`, the CDC
    /// sink — of which exactly ONE also wrote the run's in-flight aggregate. So a
    /// crashed CDC run left both records and a crashed BATCH run left half: its
    /// parts in `file_log` and nothing saying what run they amounted to. Nothing
    /// in the types said the two travel together, so the pairing arrived one call
    /// site at a time and stopped at the first.
    ///
    /// The aggregate is DERIVED from `file_log` rather than passed in. That is the
    /// point of the seam: two numbers computed by two writers can disagree, a
    /// projection cannot. No caller accumulates totals, no caller can forget to,
    /// and a re-recorded part corrects the aggregate instead of double-counting
    /// it.
    ///
    /// Non-fatal by ADR-0001 I7 — the file is already durable at the destination,
    /// so a ledger failure is worth logging and never worth failing a run whose
    /// data is safe. Returns the error for the caller to log with its own words.
    pub fn record_durable_part(&self, p: DurablePart<'_>) -> Result<()> {
        let DurablePart {
            run_id,
            export_name,
            file_name,
            rows,
            bytes,
            format,
            compression,
            mode,
            cursor_high,
        } = p;
        // A part is identified by its NAME within its run. Recording it twice is the
        // same fact stated twice, not two parts — and `file_log` has no uniqueness
        // to say so, so a retried attempt that rewrites the same file name would
        // add a second row and inflate every total derived from it. The projection
        // made that visible the first time it was asserted: three parts summing 42
        // became 52 after one re-record. Delete-then-insert keeps the table a SET
        // of parts, which is what "the parts this run produced" means.
        self.forget_part(run_id, file_name)?;
        self.record_file(FilePart {
            run_id,
            export_name,
            file_name,
            rows,
            bytes,
            format,
            compression,
            cursor_high,
        })?;
        self.project_running_aggregate(run_id, export_name, mode, format)
    }

    /// Drop any prior row for this run's part, so re-recording replaces rather
    /// than accumulates. Scoped to (run_id, file_name): another run's part with
    /// the same name is a different part.
    fn forget_part(&self, run_id: &str, file_name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM file_log WHERE run_id = ?1 AND file_name = ?2",
            &[run_id.into(), file_name.into()],
        )?;
        Ok(())
    }

    pub fn record_file(&self, p: FilePart<'_>) -> Result<()> {
        let FilePart {
            run_id,
            export_name,
            file_name,
            rows: row_count,
            bytes,
            format,
            compression,
            cursor_high,
        } = p;
        self.execute(
            "INSERT INTO file_log (run_id, export_name, file_name, row_count, bytes, format, compression, created_at, cursor_high) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            &[
                run_id.into(),
                export_name.into(),
                file_name.into(),
                row_count.into(),
                bytes.into(),
                format.into(),
                compression.into(),
                chrono::Utc::now().to_rfc3339().into(),
                cursor_high.into(),
            ],
        )?;
        Ok(())
    }

    pub fn get_files(&self, export_name: Option<&str>, limit: usize) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        match export_name {
            Some(name) => self.query(
                &format!(
                    "SELECT {cols} FROM file_log WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2"
                ),
                &[name.into(), (limit as i64).into()],
                file_record,
            ),
            None => self.query(
                &format!("SELECT {cols} FROM file_log ORDER BY id DESC LIMIT ?1"),
                &[(limit as i64).into()],
                file_record,
            ),
        }
    }

    /// Every `file_log` row for one `run_id`, in write order. Unlike `chunk_task`
    /// (one `file_name` per chunk, the FIRST rotation sibling only), file_log
    /// records EVERY committed part — including all `max_file_size` rotation
    /// siblings — with its real `bytes`, written per-part before `complete_chunk_
    /// task`. This is the crash-consistent source the chunked resume reconstructs a
    /// COMPLETE destination manifest from when none exists (round-5 fix).
    pub fn list_files_for_run(&self, run_id: &str) -> Result<Vec<FileRecord>> {
        let cols =
            "run_id, export_name, file_name, row_count, bytes, format, compression, created_at";
        self.query(
            &format!("SELECT {cols} FROM file_log WHERE run_id = ?1 ORDER BY id ASC"),
            &[run_id.into()],
            file_record,
        )
    }

    /// v25: the high-water key of the LAST committed keyset page for `run_id` — the resume point
    /// for the cursor-atomic checkpoint. Keyset pages commit in KEY ORDER (monotonic), so the
    /// highest-`id` row carrying a `cursor_high` holds the max key; `ORDER BY id DESC LIMIT 1`
    /// deliberately avoids a LEXICAL SQL `MAX()` (which mis-orders numeric-as-text keys — "999" >
    /// "1000"). `None` when this run committed no keyset page (a non-keyset or first-page-crash run).
    pub fn last_committed_cursor_high(&self, run_id: &str) -> Result<Option<String>> {
        let rows = self.query(
            "SELECT cursor_high FROM file_log \
             WHERE run_id = ?1 AND cursor_high IS NOT NULL ORDER BY id DESC LIMIT 1",
            &[run_id.into()],
            |r| r.text(0),
        )?;
        Ok(rows.into_iter().next())
    }
}

/// The `FileRecord` projection, written once for both backends.
fn file_record(r: &dyn super::row::StateRow) -> FileRecord {
    FileRecord {
        run_id: r.text(0),
        export_name: r.text(1),
        file_name: r.text(2),
        row_count: r.i64(3),
        bytes: r.i64(4),
        format: r.text(5),
        compression: r.opt_text(6),
        created_at: r.text(7),
    }
}

#[cfg(test)]
mod tests {

    /// Recording a durable part writes BOTH records, and the aggregate is a
    /// PROJECTION of the parts rather than a second opinion.
    ///
    /// The pairing used to live in the callers: three production sites wrote the
    /// part and exactly ONE also wrote the aggregate, so a crashed batch run left
    /// its parts in `file_log` with nothing saying what run they amounted to.
    /// Deriving the totals from `file_log` is what makes the two unable to
    /// disagree — no caller accumulates, so no caller can forget.
    ///
    /// THREE parts, not one: with a single part every arithmetic is the identity
    /// and a sum that ignored its input would still look right.
    #[test]
    fn recording_a_durable_part_projects_the_run_aggregate_from_the_parts() {
        let s = StateStore::open_in_memory().expect("in-memory state");
        for (i, rows) in [(0, 10i64), (1, 25), (2, 7)] {
            s.record_durable_part(DurablePart {
                run_id: "r1",
                export_name: "orders",
                file_name: &format!("orders_{i}.parquet"),
                rows,
                bytes: 100 * (i + 1) as i64,
                format: "parquet",
                compression: Some("zstd"),
                mode: "chunked",
                cursor_high: None,
            })
            .expect("record");
        }

        let rows = s.get_metrics(Some("orders"), 10).expect("metrics");
        assert_eq!(
            rows.len(),
            1,
            "ONE aggregate per run — a row per part would make every sum over \
             export_metrics count this run three times"
        );
        assert_eq!(rows[0].status, "running", "the run has not finished");
        assert_eq!(
            rows[0].total_rows, 42,
            "the aggregate must equal the SUM of the parts (10+25+7), because it is \
             computed from them"
        );
        assert_eq!(s.list_files_for_run("r1").expect("files").len(), 3);

        // The projection corrects itself: re-recording a part must not double it.
        s.record_durable_part(DurablePart {
            run_id: "r1",
            export_name: "orders",
            file_name: "orders_0.parquet",
            rows: 10,
            bytes: 100,
            format: "parquet",
            compression: Some("zstd"),
            mode: "chunked",
            cursor_high: None,
        })
        .expect("re-record");
        let again = s.get_metrics(Some("orders"), 10).expect("metrics");
        assert_eq!(
            again[0].total_rows, 42,
            "a re-recorded part must not inflate the aggregate — that is the difference \
             between a projection and an accumulator"
        );
    }

    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn record_and_query_files() {
        let s = store();
        s.record_file(FilePart {
            run_id: "run_001",
            export_name: "orders",
            file_name: "orders_20260329.parquet",
            rows: 50000,
            bytes: 4096,
            format: "parquet",
            compression: Some("zstd"),
            cursor_high: None,
        })
        .unwrap();
        s.record_file(FilePart {
            run_id: "run_001",
            export_name: "orders",
            file_name: "orders_20260329_chunk1.parquet",
            rows: 25000,
            bytes: 2048,
            format: "parquet",
            compression: Some("zstd"),
            cursor_high: None,
        })
        .unwrap();
        s.record_file(FilePart {
            run_id: "run_002",
            export_name: "users",
            file_name: "users_20260329.csv",
            rows: 1000,
            bytes: 500,
            format: "csv",
            compression: None,
            cursor_high: None,
        })
        .unwrap();

        let files = s.get_files(Some("orders"), 10).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].run_id, "run_001");
        assert_eq!(files[0].row_count, 25000);

        let all = s.get_files(None, 10).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn files_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_file(FilePart {
                run_id: &format!("r{}", i),
                export_name: "t",
                file_name: &format!("f{}.parquet", i),
                rows: i,
                bytes: i * 100,
                format: "parquet",
                compression: None,
                cursor_high: None,
            })
            .unwrap();
        }
        let files = s.get_files(Some("t"), 3).unwrap();
        assert_eq!(files.len(), 3);
    }
}
