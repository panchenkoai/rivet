//! The machine document of `rivet load` and `rivet compact` (ADR-0039 D3): one entry per table, printed by `--json` and written by `--summary-output`.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;

/// The load's one skip reason: nothing new to load.
pub(crate) const UP_TO_DATE: &str = "up_to_date";

/// What one table of a load or compact came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableDone {
    /// The command did its work.
    Work(Work),
    /// Nothing to do, for a word of the command's closed set of skip reasons.
    Skipped(&'static str),
}

/// The counts of one table's work, each as the warehouse or the ledger returned it; `None` is unknown, never 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Work {
    pub rows: u64,
    pub rows_inserted: Option<u64>,
    pub rows_updated: Option<u64>,
    pub rows_deleted: Option<u64>,
    pub rows_merged: Option<u64>,
    pub base_rows_after: Option<u64>,
}

impl Work {
    /// A load whose `rows` are what it landed in the table consumers read, or appended to its
    /// log; what it inserted in all, a baseline leg included, is the journal's.
    pub(crate) fn loaded(rows: u64) -> Self {
        Work {
            rows,
            ..Work::default()
        }
    }

    /// A compact that merged `rows` buffer rows: `dml` is what its MERGE jobs reported, and a
    /// delete `flagged` instead of removed is an update there, so its deletes are unknown.
    pub(crate) fn compacted(
        rows: u64,
        dml: Option<DmlCounts>,
        flagged: bool,
        base_rows_after: Option<u64>,
    ) -> Self {
        Work {
            rows,
            rows_inserted: dml.map(|d| d.inserted),
            rows_updated: dml.map(|d| d.updated),
            rows_deleted: dml.filter(|_| !flagged).map(|d| d.deleted),
            rows_merged: Some(rows),
            base_rows_after,
        }
    }
}

/// The rows a DML job reported it inserted, updated and deleted (BigQuery `dmlStats`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DmlCounts {
    pub inserted: u64,
    pub updated: u64,
    pub deleted: u64,
}

impl DmlCounts {
    /// The sum of two jobs' counts.
    pub(crate) fn plus(self, other: DmlCounts) -> DmlCounts {
        DmlCounts {
            inserted: self.inserted + other.inserted,
            updated: self.updated + other.updated,
            deleted: self.deleted + other.deleted,
        }
    }
}

/// One job (BigQuery) or statement (ClickHouse) a command ran, as the warehouse reported it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WarehouseJob {
    pub id: String,
    pub bytes_processed: Option<u64>,
    pub bytes_billed: Option<u64>,
    pub dml: Option<DmlCounts>,
}

/// What one table's command ran and consumed, shared by the clients that run its jobs.
#[derive(Debug, Clone, Default)]
pub struct JobJournal {
    inner: std::sync::Arc<std::sync::Mutex<Journal>>,
    /// Whether the target names its jobs; one that does not answers `null` for the ids.
    names_jobs: bool,
}

#[derive(Debug, Default)]
struct Journal {
    jobs: Vec<WarehouseJob>,
    /// The files and runs the load consumed (the ledger's) and the rows it landed (the warehouse's).
    consumed: Option<(u64, u64, u64)>,
}

impl JobJournal {
    /// A journal for a target that names its jobs (`true`) or not.
    pub(crate) fn new(names_jobs: bool) -> Self {
        JobJournal {
            names_jobs,
            ..JobJournal::default()
        }
    }

    fn journal(&self) -> std::sync::MutexGuard<'_, Journal> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record a finished job.
    pub(crate) fn record(&self, job: WarehouseJob) {
        self.journal().jobs.push(job);
    }

    /// Record the files and runs the load consumed and the rows it landed.
    pub(crate) fn consumed(&self, files: usize, runs: usize, landed: u64) {
        self.journal().consumed = Some((files as u64, runs as u64, landed));
    }

    /// The jobs recorded so far.
    pub(crate) fn jobs(&self) -> Vec<WarehouseJob> {
        self.journal().jobs.clone()
    }

    /// The summed DML counts of the jobs that reported any, or `None` when none did.
    pub(crate) fn dml(&self) -> Option<DmlCounts> {
        self.journal()
            .jobs
            .iter()
            .filter_map(|j| j.dml)
            .reduce(DmlCounts::plus)
    }
}

/// The sum of the values the jobs reported, or `None` when none reported one.
fn sum_reported(jobs: &[WarehouseJob], field: fn(&WarehouseJob) -> Option<u64>) -> Option<u64> {
    jobs.iter().filter_map(field).reduce(|a, b| a + b)
}

/// One `per_table[]` entry.
#[derive(Debug, Serialize)]
pub(crate) struct TableResult {
    pub export: String,
    pub table: String,
    pub status: &'static str,
    pub skip_reason: Option<&'static str>,
    pub rows: u64,
    pub rows_inserted: Option<u64>,
    pub rows_updated: Option<u64>,
    pub rows_deleted: Option<u64>,
    pub rows_merged: Option<u64>,
    pub base_rows_after: Option<u64>,
    pub files: Option<u64>,
    pub runs: Option<u64>,
    pub warehouse_job_ids: Option<Vec<String>>,
    pub bytes_processed: Option<u64>,
    pub bytes_billed: Option<u64>,
    pub error: Option<serde_json::Value>,
}

/// The document one `rivet load` or `rivet compact` prints.
#[derive(Debug, Serialize)]
pub(crate) struct CommandResult {
    pub run_id: String,
    pub per_table: Vec<TableResult>,
}

/// The entry of table `table` of export `export`; `done` is the command's word for work done, and
/// `journal` the jobs the command ran for the table, whatever its outcome.
pub(crate) fn table_result(
    export: &str,
    table: &str,
    done: &'static str,
    outcome: &Result<TableDone>,
    journal: &JobJournal,
) -> TableResult {
    let (status, skip_reason, work, error) = match outcome {
        Ok(TableDone::Work(work)) => (done, None, *work, None),
        Ok(TableDone::Skipped(why)) => ("skipped", Some(*why), Work::default(), None),
        Err(e) => (
            "failed",
            None,
            Work::default(),
            Some(crate::redact::error_object(e)),
        ),
    };
    let jobs = journal.jobs();
    let consumed = journal.journal().consumed;
    TableResult {
        export: export.to_string(),
        table: table.to_string(),
        status,
        skip_reason,
        rows: work.rows,
        rows_inserted: work.rows_inserted.or(consumed.map(|(_, _, landed)| landed)),
        rows_updated: work.rows_updated,
        rows_deleted: work.rows_deleted,
        rows_merged: work.rows_merged,
        base_rows_after: work.base_rows_after,
        files: consumed.map(|(files, _, _)| files),
        runs: consumed.map(|(_, runs, _)| runs),
        warehouse_job_ids: journal
            .names_jobs
            .then(|| jobs.iter().map(|j| j.id.clone()).collect()),
        bytes_processed: sum_reported(&jobs, |j| j.bytes_processed),
        bytes_billed: sum_reported(&jobs, |j| j.bytes_billed),
        error,
    }
}

/// Where a load or compact sends its human lines and its document.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Print the document on stdout, and the human lines on stderr.
    pub json: bool,
    /// Write the document to this file.
    pub summary_output: Option<PathBuf>,
}

impl Report {
    /// A human result line: stdout, unless stdout carries the document.
    pub(crate) fn say(&self, line: &str) {
        if self.says_on_stderr() {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
    }

    /// Whether the human lines go to stderr: they do when stdout carries the document.
    pub(crate) fn says_on_stderr(&self) -> bool {
        self.json
    }

    /// Write and print `result` as asked; a file that cannot be written is the command's failure.
    pub(crate) fn finish(&self, result: &CommandResult) -> Result<()> {
        if self.json {
            println!("{}", document(result)?);
        }
        match &self.summary_output {
            Some(path) => write_document(path, result),
            None => Ok(()),
        }
    }
}

/// The document's JSON text.
fn document(result: &CommandResult) -> Result<String> {
    serde_json::to_string_pretty(result).map_err(|e| {
        crate::error::coded(
            crate::error::codes::INTERNAL_RUN_INVARIANT,
            format!("serde_json: {e:#}"),
        )
    })
}

/// Write the document to `path`, creating its directory.
fn write_document(path: &Path, result: &CommandResult) -> Result<()> {
    let text = document(result)?;
    let unwritable = |what: String, e: std::io::Error| {
        crate::error::coded_io(
            crate::error::codes::CLI_OUTPUT_UNWRITABLE,
            format!("--summary-output: {what}: {e}"),
            e,
        )
    };
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)
            .map_err(|e| unwritable(format!("create_dir_all({})", dir.display()), e))?;
    }
    std::fs::write(path, text).map_err(|e| unwritable(format!("write({})", path.display()), e))
}

/// The command's exit: the tables' failure when there is one, else the document's.
pub(crate) fn exit_of(tables: Result<()>, document: Result<()>) -> Result<()> {
    match (tables, document) {
        (Err(e), Err(doc)) => {
            eprintln!("  {}", crate::redact::redact_error(&doc));
            Err(e)
        }
        (Err(e), Ok(())) | (Ok(()), Err(e)) => Err(e),
        (Ok(()), Ok(())) => Ok(()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn the_human_lines_leave_stdout_only_when_stdout_carries_the_document() {
        let json = Report {
            json: true,
            summary_output: None,
        };
        let file = Report {
            json: false,
            summary_output: Some("s.json".into()),
        };
        assert!(json.says_on_stderr());
        assert!(!file.says_on_stderr());
        assert!(!Report::default().says_on_stderr());
    }

    #[test]
    fn each_outcome_fills_the_keys_its_status_owns() {
        let none = JobJournal::default();
        let loaded = table_result(
            "e",
            "t",
            "loaded",
            &Ok(TableDone::Work(Work::loaded(7))),
            &none,
        );
        assert_eq!(
            (loaded.status, loaded.skip_reason, loaded.rows),
            ("loaded", None, 7)
        );
        assert!(loaded.error.is_none());
        let skipped = table_result(
            "e",
            "t",
            "loaded",
            &Ok(TableDone::Skipped(UP_TO_DATE)),
            &none,
        );
        assert_eq!(
            (skipped.status, skipped.skip_reason, skipped.rows),
            ("skipped", Some("up_to_date"), 0)
        );
        let refused = crate::error::coded(crate::error::codes::LOAD_TABLE_BUSY, "busy");
        let failed = table_result("e", "t", "compacted", &Err(refused), &none);
        assert_eq!((failed.status, failed.skip_reason), ("failed", None));
        let error = failed
            .error
            .expect("a failed table carries its error object");
        assert_eq!(error["code"], "RIVET_LOAD_TABLE_BUSY");
        assert_eq!(
            error["exit_code"],
            crate::error::classify_exit(&crate::error::coded(
                crate::error::codes::LOAD_TABLE_BUSY,
                "busy"
            ))
        );
    }

    /// A journal of a target that names its jobs, holding `jobs`.
    pub(crate) fn journal_of(jobs: &[(&str, Option<u64>, Option<DmlCounts>)]) -> JobJournal {
        let journal = JobJournal::new(true);
        for (id, bytes, dml) in jobs {
            journal.record(WarehouseJob {
                id: id.to_string(),
                bytes_processed: *bytes,
                bytes_billed: *bytes,
                dml: *dml,
            });
        }
        journal
    }

    /// The document `rivet load` builds from the outcomes the fixture describes, key for key.
    #[test]
    fn the_load_document_is_the_contract_fixture() {
        let orders = journal_of(&[
            ("rivet_load_orders_1", Some(0), None),
            ("rivet_load_orders_2", Some(0), None),
        ]);
        orders.consumed(3, 2, 1200);
        let doc = CommandResult {
            run_id: "20261006T101900.456-load".into(),
            per_table: vec![
                table_result(
                    "orders_cdc",
                    "orders",
                    "loaded",
                    &Ok(TableDone::Work(Work::loaded(1200))),
                    &orders,
                ),
                table_result(
                    "countries",
                    "countries",
                    "loaded",
                    &Ok(TableDone::Skipped(UP_TO_DATE)),
                    &JobJournal::new(true),
                ),
                table_result(
                    "events",
                    "events",
                    "loaded",
                    &Err(anyhow::anyhow!("table 'events': connection reset by peer")),
                    &journal_of(&[("rivet_load_events_1", None, None)]),
                ),
            ],
        };
        assert_eq!(
            serde_json::to_value(&doc).unwrap(),
            fixture("load_result.json")
        );
    }

    /// Unknown is `null`, never 0: no job reported bytes, no DML job ran, a target names no jobs.
    #[test]
    fn what_no_job_reported_is_null() {
        let quiet = table_result(
            "e",
            "t",
            "loaded",
            &Ok(TableDone::Work(Work::loaded(5))),
            &journal_of(&[("q1", None, None), ("q2", None, None)]),
        );
        assert_eq!(
            quiet.warehouse_job_ids,
            Some(vec!["q1".into(), "q2".into()])
        );
        assert_eq!((quiet.bytes_processed, quiet.bytes_billed), (None, None));
        assert_eq!((quiet.rows_updated, quiet.rows_deleted), (None, None));
        assert_eq!((quiet.files, quiet.runs), (None, None));
        let unnamed = JobJournal::new(false);
        unnamed.record(WarehouseJob::default());
        let entry = table_result(
            "e",
            "t",
            "loaded",
            &Ok(TableDone::Work(Work::loaded(5))),
            &unnamed,
        );
        assert_eq!(entry.warehouse_job_ids, None);
        let summed = journal_of(&[
            ("a", Some(10), None),
            ("b", None, None),
            ("c", Some(5), None),
        ]);
        let entry = table_result(
            "e",
            "t",
            "loaded",
            &Ok(TableDone::Work(Work::loaded(5))),
            &summed,
        );
        assert_eq!(
            (entry.bytes_processed, entry.bytes_billed),
            (Some(15), Some(15))
        );
    }

    /// A compact reports the DML its jobs reported, summed; a flagged delete is an update there.
    #[test]
    fn a_compact_reports_the_dml_its_jobs_reported() {
        let dml = |inserted, updated, deleted| {
            Some(DmlCounts {
                inserted,
                updated,
                deleted,
            })
        };
        let journal = journal_of(&[
            ("m1", Some(1), dml(1, 2, 3)),
            ("p", Some(1), None),
            ("m2", Some(1), dml(10, 20, 30)),
        ]);
        assert_eq!(journal.dml(), dml(11, 22, 33));
        assert_eq!(journal_of(&[("p", None, None)]).dml(), None);
        let hard = Work::compacted(66, journal.dml(), false, Some(100));
        assert_eq!(
            (
                hard.rows,
                hard.rows_inserted,
                hard.rows_updated,
                hard.rows_deleted
            ),
            (66, Some(11), Some(22), Some(33))
        );
        assert_eq!(
            (hard.rows_merged, hard.base_rows_after),
            (Some(66), Some(100))
        );
        let flagged = Work::compacted(66, journal.dml(), true, None);
        assert_eq!(
            (flagged.rows_updated, flagged.rows_deleted),
            (Some(22), None)
        );
        let loaded = Work::loaded(9);
        assert_eq!(
            (loaded.rows, loaded.rows_inserted, loaded.rows_merged),
            (9, None, None)
        );
    }

    /// A fixture of the scheduler contract (ADR-0039).
    pub(crate) fn fixture(name: &str) -> serde_json::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/scheduler")
            .join(name);
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap()
    }

    #[test]
    fn a_table_failure_decides_the_exit_over_an_unwritable_document() {
        let table = || Err(anyhow::anyhow!("table"));
        let doc = || Err(anyhow::anyhow!("doc"));
        assert_eq!(exit_of(table(), doc()).unwrap_err().to_string(), "table");
        assert_eq!(exit_of(Ok(()), doc()).unwrap_err().to_string(), "doc");
        assert_eq!(exit_of(table(), Ok(())).unwrap_err().to_string(), "table");
        assert!(exit_of(Ok(()), Ok(())).is_ok());
    }

    #[test]
    fn an_unwritable_summary_path_is_refused_by_code() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "").unwrap();
        let report = Report {
            json: false,
            summary_output: Some(file.join("sub").join("out.json")),
        };
        let result = CommandResult {
            run_id: "r".into(),
            per_table: Vec::new(),
        };
        let err = report.finish(&result).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_CLI_OUTPUT_UNWRITABLE")
        );
    }

    #[test]
    fn the_summary_file_holds_the_document() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new").join("load.json");
        let report = Report {
            json: false,
            summary_output: Some(path.clone()),
        };
        let result = CommandResult {
            run_id: "r1".into(),
            per_table: vec![table_result(
                "e",
                "t",
                "loaded",
                &Ok(TableDone::Work(Work::loaded(3))),
                &JobJournal::default(),
            )],
        };
        report.finish(&result).unwrap();
        let back: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(back["run_id"], "r1");
        assert_eq!(back["per_table"][0]["rows"], 3);
        assert_eq!(back["per_table"][0]["status"], "loaded");
    }
}
