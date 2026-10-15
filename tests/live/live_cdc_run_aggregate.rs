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
    the_entry_is_the_disk(&doc, &scn.rig, ROWS);
}

/// The rig's CDC entry in `doc` counts the rows, parts and bytes on its disk (at least `seeded` rows), and no leg is a unit.
fn the_entry_is_the_disk(doc: &serde_json::Value, rig: &Rig, seeded: i64) {
    let (rows, files, bytes) = parquet_on_disk(&rig.out_dir());
    assert!(
        rows >= seeded,
        "fixture: the snapshot leg wrote the {seeded} seeded rows, the disk holds {rows}"
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
        .find(|e| e["export_name"] == rig.export_name())
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

/// Two holders of one export name and one table name on one PostgreSQL state, differing only in source, each under child processes: each entry counts the legs its own child ran.
///
/// The MySQL holder is paused in its drain (`cdc_after_open`) after its leg, so the PostgreSQL holder's
/// leg lands on the shared state while the MySQL parent still waits for its children: a parent that
/// took "the newest leg row by name" would count the other source's baseline.
#[test]
#[ignore = "live: requires postgres-cdc, mysql-cdc and RIVET_TEST_STATE_URL (postgres)"]
fn cdc_entries_of_one_name_on_two_sources_count_their_own_legs() {
    use mysql::prelude::Queryable as _;
    let Some(state) = pg_state_url() else {
        return;
    };
    const PG_ROWS: i64 = 30;
    const MY_ROWS: i64 = 70;
    let name = unique_name("agg_two");
    let ids = |n: i64| -> String {
        (1..=n)
            .map(|i| format!("({i}, {i})"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut pg =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("postgres-cdc");
    pg.batch_execute(&format!(
        "DROP TABLE IF EXISTS {name}; CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT); \
         INSERT INTO {name} (id, v) VALUES {}",
        ids(PG_ROWS)
    ))
    .unwrap();
    let _pg_table = PgTable::adopt_on(POSTGRES_CDC_URL, name.clone());
    let slot = unique_name("agg_two_slot");
    let _slot = Slot::new(slot.clone());
    pg.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let mut my = cdc_conn();
    my.query_drop(format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT)"
    ))
    .unwrap();
    let _my_table = MysqlCdcTable(name.clone());
    my.query_drop(format!(
        "INSERT INTO {name} (id, v) VALUES {}",
        ids(MY_ROWS)
    ))
    .unwrap();

    let shape = |r: Rig, side: &str| {
        r.export_named(&name)
            .cdc_line("initial: snapshot")
            .also_export(&format!("{name}_{side}"), "SELECT 1 AS x")
    };
    let first = shape(Rig::mysql_cdc(&name), "my");
    let second = shape(Rig::pg_cdc(&name, &slot), "pg");
    let docs = tempfile::tempdir().unwrap();
    let (first_doc, second_doc) = (
        docs.path().join("first.json"),
        docs.path().join("second.json"),
    );
    let leg = format!("{name}__snapshot_{name}");
    let first_leg_landed = || {
        postgres::Client::connect(&state, postgres::NoTls)
            .expect("the state server")
            .query_one(
                "SELECT count(*) FROM export_metrics WHERE export_name = $1 AND status = 'success'",
                &[&leg],
            )
            .is_ok_and(|row| row.get::<_, i64>(0) > 0)
    };

    let mut live = first.spawn_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            first_doc.to_str().unwrap(),
        ],
        &[
            ("RIVET_STATE_URL", state.as_str()),
            ("RIVET_TEST_PAUSE_AT", "cdc_after_open:90000"),
        ],
    );
    let t0 = std::time::Instant::now();
    while !first_leg_landed() {
        assert!(
            std::process::Child::try_wait(&mut live)
                .expect("try_wait")
                .is_none(),
            "fixture: the first holder ended before its snapshot leg was recorded"
        );
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(120),
            "fixture: the first holder's snapshot leg never landed"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    let out = second.run_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            second_doc.to_str().unwrap(),
        ],
        &[("RIVET_STATE_URL", state.as_str())],
    );
    assert!(
        out.status.success(),
        "the second holder runs beside the first:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        std::process::Child::try_wait(&mut live)
            .expect("try_wait")
            .is_none(),
        "fixture: the first holder must still wait in its drain when the second's leg lands"
    );
    let said = live.wait_with_output().expect("reap the first holder");
    assert!(
        said.status.success(),
        "the first holder delivers:\n{}",
        String::from_utf8_lossy(&said.stderr)
    );
    let read = |p: &std::path::Path| -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(p).expect("summary written"))
            .expect("summary is JSON")
    };
    the_entry_is_the_disk(&read(&first_doc), &first, MY_ROWS);
    the_entry_is_the_disk(&read(&second_doc), &second, PG_ROWS);
}
