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

/// A CDC child SIGKILLed in its drain after its snapshot leg finished: the parent's failed entry counts
/// the leg the child reported, which is what the disk holds (ADR-0039 D3, a killed child's entry).
#[cfg(unix)]
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_cdc_child_killed_after_its_leg_counts_the_leg_postgres() {
    let mut scn = CdcScenario::pg_with("agg_kill_pp", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
        r.cdc_line("initial: snapshot")
            .also_export("agg_kill_side", "SELECT 1 AS x")
    });
    for id in 1..=ROWS {
        scn.insert(id);
    }
    scn.settle();
    let docs = tempfile::tempdir().unwrap();
    let doc = docs.path().join("summary.json");
    let mut live = scn.rig.spawn_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            doc.to_str().unwrap(),
        ],
        &[("RIVET_TEST_PAUSE_AT", "cdc_after_open:90000")],
    );
    // The drain's run row opens only after the leg's run ended and reported.
    until_ready(&mut live, "the drain started after its leg", || {
        drain_running(&scn.rig)
    });
    let export_flag = format!("--export {}", scn.rig.export_name());
    let cdc_child = String::from_utf8_lossy(
        &std::process::Command::new("pgrep")
            .args(["-P", &live.id().to_string()])
            .output()
            .expect("pgrep")
            .stdout,
    )
    .lines()
    .filter_map(|l| l.trim().parse::<u32>().ok())
    .find(|pid| {
        let cmd = std::process::Command::new("ps")
            .args(["-o", "command=", "-p", &pid.to_string()])
            .output()
            .expect("ps");
        String::from_utf8_lossy(&cmd.stdout).contains(&export_flag)
    })
    .expect("fixture: the CDC export's child process is alive");
    let killed = std::process::Command::new("kill")
        .args(["-KILL", &cdc_child.to_string()])
        .status()
        .expect("kill");
    assert!(killed.success(), "fixture: SIGKILL the CDC child");
    // The leg delivered beside the killed drain; the disk is compared with the source below.
    let status = std::process::Child::wait(&mut live).expect("reap the parent");
    let said = live.reaped_beside_a_delivery(status);
    assert!(!said.status.success(), "a killed child fails the run");
    let doc = read_doc(&doc);
    let entry = doc["per_export"]
        .as_array()
        .expect("per_export[]")
        .iter()
        .find(|e| e["export_name"] == scn.rig.export_name())
        .unwrap_or_else(|| panic!("no entry for the CDC export in {doc}"))
        .clone();
    let (rows, files, bytes) = parquet_on_disk(&scn.rig.out_dir());
    assert_eq!(
        rows, ROWS,
        "fixture: the leg wrote the seeded rows before the kill"
    );
    assert_eq!(
        (
            entry["status"].as_str(),
            entry["run_id"].as_str(),
            entry["duration_ms"].as_i64()
        ),
        (Some("failed"), Some(""), Some(0)),
        "{entry}"
    );
    assert_eq!(
        (
            entry["rows"].as_i64(),
            entry["files"].as_i64(),
            entry["bytes"].as_u64()
        ),
        (Some(rows), Some(files), Some(bytes)),
        "the killed child's entry counts the finished leg it reported: {entry}"
    );
    assert!(
        entry["error_message"]
            .as_str()
            .is_some_and(|m| m.contains("signal")),
        "{entry}"
    );
}

/// Whether the rig's CDC export has a `running` run row; false while the state does not exist yet.
fn drain_running(rig: &Rig) -> bool {
    let sql = "SELECT count(*) FROM run_status WHERE export_name = $1 AND status = 'running'";
    match state_url_under_test() {
        Some(url) => state_count(&url, sql, rig.export_name()) > 0,
        None => rusqlite::Connection::open_with_flags(
            rig.config_path().with_file_name(".rivet_state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .and_then(|c| {
            c.query_row(&sql.replace("$1", "?1"), [rig.export_name()], |r| {
                r.get::<_, i64>(0)
            })
        })
        .is_ok_and(|n| n > 0),
    }
}

/// Two holders of one export name and one table name on one PostgreSQL state, differing only in source, each under child processes: each entry counts the legs its own child ran.
#[test]
#[ignore = "live: requires postgres-cdc, mysql-cdc and RIVET_TEST_STATE_URL (postgres)"]
fn cdc_entries_of_one_name_on_two_sources_count_their_own_legs() {
    let Some(state) = pg_state_url() else {
        return;
    };
    two_holders_count_their_own_legs(&state);
}

/// Of two holders of one export name on one PostgreSQL state, the one that fails is reported failed under its own run, never as the other's success.
#[test]
#[ignore = "live: requires postgres-cdc, mysql-cdc and RIVET_TEST_STATE_URL (postgres)"]
fn a_failed_holder_of_a_shared_name_is_reported_under_its_own_run_id() {
    let Some(state) = pg_state_url() else {
        return;
    };
    the_failed_holder_is_reported_failed_by_its_own_run_id(&state);
}

/// Rows in the PostgreSQL and the MySQL table of [`TwoHolders`].
const PG_ROWS: i64 = 30;
const MY_ROWS: i64 = 70;

/// One export name and one table name on PostgreSQL and on MySQL, each with a side export so the
/// run spawns child processes; the tables and the slot are dropped with it.
struct TwoHolders {
    name: String,
    my: Rig,
    pg: Rig,
    _guards: Vec<Box<dyn std::any::Any>>,
}

impl TwoHolders {
    /// The two tables seeded, the PostgreSQL slot made after its rows so its stream starts empty.
    fn new(label: &str) -> Self {
        use mysql::prelude::Queryable as _;
        let name = unique_name(label);
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
        let pg_table = PgTable::adopt_on(POSTGRES_CDC_URL, name.clone());
        let slot = unique_name(&format!("{label}_slot"));
        let slot_guard = Slot::new(slot.clone());
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
        let my_table = MysqlCdcTable(name.clone());
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
        Self {
            my: shape(Rig::mysql_cdc(&name), "my"),
            pg: shape(Rig::pg_cdc(&name, &slot), "pg"),
            name,
            _guards: vec![Box::new(pg_table), Box::new(slot_guard), Box::new(my_table)],
        }
    }

    /// The snapshot leg's export name, the same for both holders.
    fn leg(&self) -> String {
        format!("{0}__snapshot_{0}", self.name)
    }
}

/// `count(*)` of `sql` with the one text parameter `arg` on the PostgreSQL state; 0 before the table exists.
fn state_count(state: &str, sql: &str, arg: &str) -> i64 {
    postgres::Client::connect(state, postgres::NoTls)
        .expect("the state server")
        .query_one(sql, &[&arg])
        .map_or(0, |row| row.get::<_, i64>(0))
}

/// Poll until `ready`, failing the fixture when `live` ends first or two minutes pass.
fn until_ready(live: &mut Spawned<'_>, what: &str, ready: impl Fn() -> bool) {
    let t0 = std::time::Instant::now();
    while !ready() {
        assert!(
            std::process::Child::try_wait(live)
                .expect("try_wait")
                .is_none(),
            "fixture: the first holder ended before {what}"
        );
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(120),
            "fixture: {what} never happened"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// The `--summary-output` document at `path`.
fn read_doc(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).expect("summary written"))
        .expect("summary is JSON")
}

/// The MySQL holder pauses in its drain (`cdc_after_open`) after its leg, so the PostgreSQL holder's
/// leg lands on the shared `state` while the MySQL parent still waits for its children: a parent that
/// took "the newest leg row by name" would count the other source's baseline.
fn two_holders_count_their_own_legs(state: &str) {
    let two = TwoHolders::new("agg_two");
    let docs = tempfile::tempdir().unwrap();
    let (first_doc, second_doc) = (
        docs.path().join("first.json"),
        docs.path().join("second.json"),
    );
    let leg = two.leg();
    let mut live = two.my.spawn_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            first_doc.to_str().unwrap(),
        ],
        &[
            ("RIVET_STATE_URL", state),
            ("RIVET_TEST_PAUSE_AT", "cdc_after_open:90000"),
        ],
    );
    until_ready(&mut live, "its snapshot leg was recorded", || {
        state_count(
            state,
            "SELECT count(*) FROM export_metrics WHERE export_name = $1 AND status = 'success'",
            &leg,
        ) > 0
    });
    let out = two.pg.run_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            second_doc.to_str().unwrap(),
        ],
        &[("RIVET_STATE_URL", state)],
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
    the_entry_is_the_disk(&read_doc(&first_doc), &two.my, MY_ROWS);
    the_entry_is_the_disk(&read_doc(&second_doc), &two.pg, PG_ROWS);
}

/// The PostgreSQL holder pauses in its snapshot leg (`pg_after_snapshot_open`) and then fails its first
/// part write; the MySQL holder runs to success meanwhile, so the newest row of the export's name on the
/// shared `state` is the other holder's success when the failed parent builds its entry.
fn the_failed_holder_is_reported_failed_by_its_own_run_id(state: &str) {
    let two = TwoHolders::new("agg_fail");
    let docs = tempfile::tempdir().unwrap();
    let (failed_doc, ok_doc) = (docs.path().join("failed.json"), docs.path().join("ok.json"));
    let leg = two.leg();
    let mut live = two.pg.spawn_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            failed_doc.to_str().unwrap(),
        ],
        &[
            ("RIVET_STATE_URL", state),
            ("RIVET_TEST_PAUSE_AT", "pg_after_snapshot_open:90000"),
            ("RIVET_TEST_ERROR_AT", "sink_part_write:0"),
        ],
    );
    until_ready(&mut live, "its snapshot leg opened", || {
        state_count(
            state,
            "SELECT count(*) FROM run_status WHERE export_name = $1 AND status = 'running'",
            &leg,
        ) > 0
    });
    let out = two.my.run_args_env(
        &[
            "--parallel-export-processes",
            "--summary-output",
            ok_doc.to_str().unwrap(),
        ],
        &[("RIVET_STATE_URL", state)],
    );
    assert!(
        out.status.success(),
        "the other holder runs beside the paused one:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        std::process::Child::try_wait(&mut live)
            .expect("try_wait")
            .is_none(),
        "fixture: the failing holder must still be paused when the other one finishes"
    );
    let said = live.wait_with_output().expect("reap the failing holder");
    assert!(
        !said.status.success(),
        "fixture: the injected part-write error fails the run"
    );
    let entry_of = |doc: &serde_json::Value| -> serde_json::Value {
        doc["per_export"]
            .as_array()
            .expect("per_export[]")
            .iter()
            .find(|e| e["export_name"] == two.name.as_str())
            .unwrap_or_else(|| panic!("no entry for `{}` in {doc}", two.name))
            .clone()
    };
    let ok = entry_of(&read_doc(&ok_doc));
    let failed = entry_of(&read_doc(&failed_doc));
    assert_eq!(ok["status"], "success", "{ok}");
    assert_eq!(
        failed["status"], "failed",
        "the failed run is not reported as the other holder's success: {failed}"
    );
    let run_id = failed["run_id"].as_str().unwrap_or_default();
    assert_ne!(
        run_id,
        ok["run_id"].as_str().unwrap_or_default(),
        "{failed}"
    );
    assert!(
        state_count(
            state,
            "SELECT count(*) FROM export_metrics WHERE run_id = $1 AND status = 'failed'",
            run_id,
        ) > 0,
        "the entry names its own failed run, `{run_id}`, which the state records as failed: {failed}"
    );
}
