//! The machine document of `rivet load` and `rivet compact` (ADR-0039 D3): one entry per table, printed by `--json` and written by `--summary-output`.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;

/// The load's one skip reason: nothing new to load.
pub(crate) const UP_TO_DATE: &str = "up_to_date";

/// What one table of a load or compact came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TableDone {
    /// The command did its work on `rows` rows.
    Work { rows: u64 },
    /// Nothing to do, for a word of the command's closed set of skip reasons.
    Skipped(&'static str),
}

/// One `per_table[]` entry.
#[derive(Debug, Serialize)]
pub(crate) struct TableResult {
    pub export: String,
    pub table: String,
    pub status: &'static str,
    pub skip_reason: Option<&'static str>,
    pub rows: u64,
    pub error: Option<serde_json::Value>,
}

/// The document one `rivet load` or `rivet compact` prints.
#[derive(Debug, Serialize)]
pub(crate) struct CommandResult {
    pub run_id: String,
    pub per_table: Vec<TableResult>,
}

/// The entry of table `table` of export `export`; `done` is the command's word for work done.
pub(crate) fn table_result(
    export: &str,
    table: &str,
    done: &'static str,
    outcome: &Result<TableDone>,
) -> TableResult {
    let (status, skip_reason, rows, error) = match outcome {
        Ok(TableDone::Work { rows }) => (done, None, *rows, None),
        Ok(TableDone::Skipped(why)) => ("skipped", Some(*why), 0, None),
        Err(e) => ("failed", None, 0, Some(crate::redact::error_object(e))),
    };
    TableResult {
        export: export.to_string(),
        table: table.to_string(),
        status,
        skip_reason,
        rows,
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
        if self.json {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
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
        crate::error::coded(
            crate::error::codes::CLI_OUTPUT_UNWRITABLE,
            format!("--summary-output: {what}: {e}"),
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
    fn each_outcome_fills_the_keys_its_status_owns() {
        let loaded = table_result("e", "t", "loaded", &Ok(TableDone::Work { rows: 7 }));
        assert_eq!(
            (loaded.status, loaded.skip_reason, loaded.rows),
            ("loaded", None, 7)
        );
        assert!(loaded.error.is_none());
        let skipped = table_result("e", "t", "loaded", &Ok(TableDone::Skipped(UP_TO_DATE)));
        assert_eq!(
            (skipped.status, skipped.skip_reason, skipped.rows),
            ("skipped", Some("up_to_date"), 0)
        );
        let refused = crate::error::coded(crate::error::codes::LOAD_TABLE_BUSY, "busy");
        let failed = table_result("e", "t", "compacted", &Err(refused));
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

    /// The document `rivet load` builds from the outcomes the fixture describes, key for key.
    #[test]
    fn the_load_document_is_the_contract_fixture() {
        let doc = CommandResult {
            run_id: "20261006T101900.456-load".into(),
            per_table: vec![
                table_result(
                    "orders_cdc",
                    "orders",
                    "loaded",
                    &Ok(TableDone::Work { rows: 1200 }),
                ),
                table_result(
                    "countries",
                    "countries",
                    "loaded",
                    &Ok(TableDone::Skipped(UP_TO_DATE)),
                ),
                table_result(
                    "events",
                    "events",
                    "loaded",
                    &Err(anyhow::anyhow!("table 'events': connection reset by peer")),
                ),
            ],
        };
        assert_eq!(
            serde_json::to_value(&doc).unwrap(),
            fixture("load_result.json")
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
                &Ok(TableDone::Work { rows: 3 }),
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
