//! The run aggregate of a `mode: cdc` export with `initial: snapshot` (ADR-0039 D3): its one
//! `per_export[]` entry counts what the snapshot legs AND the stream wrote in that invocation.
//!
//! The first run of such an export writes its baseline through synthesized `mode: full` legs
//! (`<export>__snapshot_<table>`) before it drains the stream. The entry carried the stream's
//! counts only, so a scheduler reading `--summary-output` saw `rows: 0, files: 0` for the run that
//! wrote the whole table (0.31.0 and earlier).
//!
//! Oracle: the Parquet parts on disk, counted from their footers; never rivet's report.

use crate::common::*;

const ROWS: i64 = 30;

/// Rows, parts and bytes of every Parquet part under `dir`, at any depth, read from the files.
fn parquet_on_disk(dir: &std::path::Path) -> (i64, i64, u64) {
    use parquet::file::reader::FileReader as _;
    let parts: Vec<_> = files_under(dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    let rows = parts
        .iter()
        .map(|p| {
            let f = std::fs::File::open(p).expect("open part");
            parquet::file::reader::SerializedFileReader::new(f)
                .expect("read part footer")
                .metadata()
                .file_metadata()
                .num_rows()
        })
        .sum();
    let bytes = parts
        .iter()
        .map(|p| std::fs::metadata(p).expect("part size").len())
        .sum();
    (rows, parts.len() as i64, bytes)
}

/// The run's `--summary-output` document, after a run with `extra` flags that must exit 0.
fn summary_of_run(rig: &Rig, extra: &[&str]) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("summary.json");
    let mut args = vec!["--summary-output", path.to_str().unwrap()];
    args.extend_from_slice(extra);
    let out = rig.run_args(&args);
    assert!(
        out.status.success(),
        "rivet run must exit 0:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_str(&std::fs::read_to_string(&path).expect("summary written"))
        .expect("summary is JSON")
}

/// The CDC export's entry counts every part its first run wrote, legs included, and no leg is a unit of its own.
fn the_entry_counts_the_snapshot_legs(mut scn: CdcScenario, extra: &[&str]) {
    for id in 1..=ROWS {
        scn.insert(id);
    }
    scn.settle();
    let doc = summary_of_run(&scn.rig, extra);
    let (rows, files, bytes) = parquet_on_disk(&scn.rig.out_dir());
    assert!(
        rows >= ROWS,
        "fixture: the snapshot leg wrote the {ROWS} seeded rows, the disk holds {rows}"
    );
    let entries = doc["per_export"].as_array().expect("per_export[]");
    let named: Vec<&str> = entries
        .iter()
        .map(|e| e["export_name"].as_str().unwrap_or_default())
        .collect();
    assert!(
        named.iter().all(|n| !n.contains("__snapshot_")),
        "a snapshot leg is not a unit of the run: {named:?}"
    );
    let entry = entries
        .iter()
        .find(|e| e["export_name"] == scn.rig.export_name())
        .unwrap_or_else(|| panic!("no entry for the CDC export in {named:?}"));
    assert_eq!(entry["status"], "success", "{entry}");
    assert_eq!(
        (
            entry["rows"].as_i64(),
            entry["files"].as_i64(),
            entry["bytes"].as_u64()
        ),
        (Some(rows), Some(files), Some(bytes)),
        "the entry counts what the run wrote (rows, parts, bytes on disk): {entry}"
    );
    let sum = |k: &str| -> i64 { entries.iter().map(|e| e[k].as_i64().unwrap_or(0)).sum() };
    assert_eq!(doc["total_rows"].as_i64(), Some(sum("rows")), "{doc}");
    assert_eq!(doc["total_files"].as_i64(), Some(sum("files")), "{doc}");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn cdc_entry_counts_the_snapshot_legs_postgres() {
    the_entry_counts_the_snapshot_legs(
        CdcScenario::pg_with("agg_legs_pg", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            r.cdc_line("initial: snapshot")
        }),
        &[],
    );
}

/// The process runner builds the parent's entry from the children's metric rows, not from a summary.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn cdc_entry_counts_the_snapshot_legs_under_child_processes_postgres() {
    the_entry_counts_the_snapshot_legs(
        CdcScenario::pg_with("agg_legs_pp", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            r.cdc_line("initial: snapshot")
                .also_export("agg_side", "SELECT 1 AS x")
        }),
        &["--parallel-export-processes"],
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_entry_counts_the_snapshot_legs_mysql() {
    the_entry_counts_the_snapshot_legs(
        CdcScenario::mysql_with("agg_legs_my", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
            r.cdc_line("initial: snapshot")
        }),
        &[],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn cdc_entry_counts_the_snapshot_legs_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    the_entry_counts_the_snapshot_legs(
        CdcScenario::mssql_with("agg_legs_ms", "id BIGINT PRIMARY KEY, v BIGINT", |r, t| {
            r.repoint(&format!("dbo.{t}")).cdc_line("initial: snapshot")
        }),
        &[],
    );
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn cdc_entry_counts_the_snapshot_legs_mongo() {
    the_entry_counts_the_snapshot_legs(
        CdcScenario::mongo_with("agg_legs_mg", |r, _| r.cdc_line("initial: snapshot")),
        &[],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn cdc_entry_counts_the_snapshot_legs_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    the_entry_counts_the_snapshot_legs(
        CdcScenario::oracle_with("agg_legs_ora", |r, _| r.cdc_line("initial: snapshot")),
        &[],
    );
}
