//! Every entry point runs the run set `rivet run` runs: `rivet apply <config>`, `--pool`,
//! `--parallel-export-processes` and a sealed plan must not deliver something else, exit 0.
//!
//! The delivered side is re-read from the destination (Parquet parts per directory, stdout
//! bytes) and compared with the fixture the test wrote into the source.
//!
//! `rivet apply` has no stdout cell: it prints its wave headers on stdout as well, so no reader
//! can take an export's bytes out of it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::common::*;

/// Rows in the partition fixture.
const ROWS: i64 = 60;
/// Days the fixture spreads its rows over.
const DAYS: i64 = 5;
/// Every `NULL_EVERY`-th id has no partition value.
const NULL_EVERY: i64 = 13;

/// Ids per directory that holds Parquet parts, keyed by the directory's name.
type Tree = BTreeMap<String, BTreeSet<i64>>;

/// The fixture's partition value for `id`: a day of March 2026, or `None` for the NULL bucket.
fn fixture_day(id: i64) -> Option<String> {
    (id % NULL_EVERY != 0).then(|| format!("2026-03-{:02}", 1 + (id - 1) % DAYS))
}

/// The tree the fixture must land as: one `<col>=<day>` directory per day, plus the NULL bucket.
fn fixture_tree(col: &str) -> Tree {
    let mut tree = Tree::new();
    for id in 1..=ROWS {
        let value = fixture_day(id).unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string());
        tree.entry(format!("{col}={value}")).or_default().insert(id);
    }
    tree
}

/// `VALUES` rows of the fixture, with `stamp` rendering one day as the engine's timestamp literal.
fn fixture_values(stamp: &dyn Fn(&str) -> String) -> String {
    (1..=ROWS)
        .map(|id| match fixture_day(id) {
            Some(day) => format!("({id}, {})", stamp(&day)),
            None => format!("({id}, NULL)"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The first column of every Parquet part in `dir`, as ids.
fn part_ids(dir: &Path) -> Vec<i64> {
    read_all_parts(dir)
        .iter()
        .flat_map(|batch| {
            let col = batch.column(0).clone();
            (0..batch.num_rows()).map(move |row| {
                let text = arrow::util::display::array_value_to_string(&col, row).unwrap();
                text.trim()
                    .parse::<f64>()
                    .unwrap_or_else(|_| panic!("id cell `{text}` is not a number"))
                    as i64
            })
        })
        .collect()
}

/// What `root` holds: the ids in each immediate sub-directory's Parquet parts.
fn delivered_tree(root: &Path) -> Tree {
    let mut tree = Tree::new();
    for entry in std::fs::read_dir(root).expect("destination root").flatten() {
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let ids: BTreeSet<i64> = part_ids(&dir).into_iter().collect();
        if !ids.is_empty() {
            tree.insert(entry.file_name().to_string_lossy().into_owned(), ids);
        }
    }
    tree
}

/// A fresh rig over the partition fixture `table`, delivering under `root/{partition}`.
fn partitioned(rig: Rig, col: &str, root: &Path) -> Rig {
    rig.export_line(&format!("partition_by: {col}"))
        .export_line("partition_granularity: day")
        .dest_path(root.join("{partition}"))
}

/// `output`'s stdout and stderr as one text.
fn text_of(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// `rivet apply <config> <flags>` on a fresh destination delivers the tree the fixture defines.
fn assert_apply_partitions(rig_of: &dyn Fn() -> Rig, col: &str, flags: &[&str]) {
    let root = tempfile::tempdir().unwrap();
    let rig = partitioned(rig_of(), col, root.path());
    let cfg: PathBuf = rig.config_path();
    let out = rig.apply_env(&cfg, flags, &[]);
    assert!(
        out.status.success(),
        "apply {flags:?} failed:\n{}",
        text_of(&out)
    );
    assert_eq!(
        delivered_tree(root.path()),
        fixture_tree(col),
        "`rivet apply <config> {flags:?}` must deliver the partition tree `rivet run` delivers"
    );
}

/// The control: `rivet run` delivers the fixture tree, so a red apply arm is the entry point's.
fn assert_run_partitions(rig_of: &dyn Fn() -> Rig, col: &str) {
    let root = tempfile::tempdir().unwrap();
    let rig = partitioned(rig_of(), col, root.path());
    let out = rig.run_args(&[]);
    assert!(
        out.status.success(),
        "control run failed:\n{}",
        text_of(&out)
    );
    assert_eq!(
        delivered_tree(root.path()),
        fixture_tree(col),
        "control: `rivet run` must deliver one directory per day plus the NULL bucket"
    );
}

/// Every whole-config entry point of one engine against the `rivet run` control.
fn assert_every_apply_form_partitions(rig_of: &dyn Fn() -> Rig, col: &str) {
    assert_run_partitions(rig_of, col);
    assert_apply_partitions(rig_of, col, &[]);
    assert_apply_partitions(rig_of, col, &["--pool", "2"]);
    assert_apply_partitions(rig_of, col, &["--parallel-export-processes"]);
}

/// The PostgreSQL partition fixture.
fn pg_partition_fixture() -> PgTable {
    let table = unique_name("run_set_part");
    pg_connect()
        .batch_execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, created_at TIMESTAMP);
             INSERT INTO {table} (id, created_at) VALUES {};",
            fixture_values(&|day| format!("'{day} 10:00:00'"))
        ))
        .expect("seed the partition fixture");
    PgTable::adopt(table)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_partitions_the_fixture_postgres() {
    require_alive(LiveService::Postgres);
    let table = pg_partition_fixture();
    assert_run_partitions(&|| Rig::pg_batch(table.name()), "created_at");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_config_partitions_like_run_postgres() {
    require_alive(LiveService::Postgres);
    let table = pg_partition_fixture();
    assert_apply_partitions(&|| Rig::pg_batch(table.name()), "created_at", &[]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_pool_partitions_like_run_postgres() {
    require_alive(LiveService::Postgres);
    let table = pg_partition_fixture();
    assert_apply_partitions(
        &|| Rig::pg_batch(table.name()),
        "created_at",
        &["--pool", "2"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn apply_child_processes_partitions_like_run_postgres() {
    require_alive(LiveService::Postgres);
    let table = pg_partition_fixture();
    assert_apply_partitions(
        &|| Rig::pg_batch(table.name()),
        "created_at",
        &["--parallel-export-processes"],
    );
}

/// Unchanged from before the shared run set: `rivet run --parallel-export-processes` over a
/// partitioned export runs in-process, says so in the same words, and delivers the tree.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_child_processes_flag_keeps_its_partition_fallback_postgres() {
    require_alive(LiveService::Postgres);
    let table = pg_partition_fixture();
    let root = tempfile::tempdir().unwrap();
    let rig = partitioned(Rig::pg_batch(table.name()), "created_at", root.path());
    let out = rig.run_args(&["--parallel-export-processes"]);
    assert!(out.status.success(), "run failed:\n{}", text_of(&out));
    assert!(
        text_of(&out).contains(
            "partition_by: --parallel-export-processes is disabled with partitioned exports \
             (child processes re-load the config and can't see synthesised partitions); \
             running in-process"
        ),
        "the fallback must keep its message:\n{}",
        text_of(&out)
    );
    assert_eq!(delivered_tree(root.path()), fixture_tree("created_at"));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn apply_forms_partition_like_run_mysql() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let table = unique_name("run_set_part");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, created_at DATETIME NULL) ENGINE=InnoDB"
    ))
    .expect("create the partition fixture");
    c.query_drop(format!(
        "INSERT INTO {table} (id, created_at) VALUES {}",
        fixture_values(&|day| format!("'{day} 10:00:00'"))
    ))
    .expect("seed the partition fixture");
    let table = MysqlTable::adopt(table);
    assert_every_apply_form_partitions(&|| Rig::mysql_batch(table.name()), "created_at");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn apply_forms_partition_like_run_mssql() {
    require_alive(LiveService::Mssql);
    let table = unique_name("run_set_part");
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, created_at DATETIME2 NULL)"
    ));
    let table = MssqlTable::adopt(table);
    mssql_exec(&format!(
        "INSERT INTO {} (id, created_at) VALUES {}",
        table.name(),
        fixture_values(&|day| format!("'{day}T10:00:00'"))
    ));
    assert_every_apply_form_partitions(&|| Rig::mssql_batch(table.name()), "created_at");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn apply_forms_partition_like_run_oracle() {
    require_alive(LiveService::Oracle);
    let table = OracleTable::create("run_set_part", "id NUMBER(10) PRIMARY KEY, d DATE");
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, CASE WHEN MOD(LEVEL, {NULL_EVERY}) = 0 THEN NULL \
         ELSE DATE '2026-03-01' + MOD(LEVEL - 1, {DAYS}) END FROM dual CONNECT BY LEVEL <= {ROWS}",
        table.name()
    ));
    assert_every_apply_form_partitions(&|| Rig::oracle_batch(table.name()), "D");
}

/// A sealed plan cannot carry a `partition_by` expansion: `plan` says so, `apply` refuses it
/// before writing anything, and the remedy the refusal names delivers the partition tree.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_sealed_plan_of_a_partitioned_export_is_refused_and_its_remedy_works_postgres() {
    require_alive(LiveService::Postgres);
    let table = pg_partition_fixture();
    let root = tempfile::tempdir().unwrap();
    let rig = partitioned(
        Rig::pg_batch(table.name()).source_url_env("DATABASE_URL"),
        "created_at",
        root.path(),
    );
    let env = [("DATABASE_URL", POSTGRES_URL)];
    let plan = root.path().join("plan.json");

    let planned = rig.plan_json_env(&plan, &[], &env);
    assert!(
        planned.status.success() && plan.is_file(),
        "plan must still be written (it is the preview):\n{}",
        text_of(&planned)
    );
    let applied = rig.apply_env(&plan, &[], &env);
    assert!(
        !applied.status.success(),
        "`rivet apply <plan-file>` of a partition_by export must not exit 0:\n{}",
        text_of(&applied)
    );
    assert!(
        text_of(&applied).contains(
            "still holds the `{partition}` token: a `partition_by` export is expanded into one \
             export per partition when the config runs, and this plan was built without that \
             expansion"
        ) && text_of(&applied).contains("rivet apply <config.yaml>"),
        "the refusal must name its cause and the remedy:\n{}",
        text_of(&applied)
    );
    assert_eq!(
        delivered_tree(root.path()),
        Tree::new(),
        "a refused plan must not have written a part"
    );
    assert!(
        text_of(&planned).contains(
            "is a `partition_by` export: its plan is a preview of the whole table, and `rivet \
             apply <plan-file>` refuses it"
        ),
        "plan must say the artifact cannot be applied:\n{}",
        text_of(&planned)
    );

    let cfg = rig.config_path();
    let remedy = rig.apply_env(&cfg, &[], &env);
    assert!(
        remedy.status.success(),
        "the remedy must run from the refused state:\n{}",
        text_of(&remedy)
    );
    assert_eq!(delivered_tree(root.path()), fixture_tree("created_at"));
}

/// The standalone MongoDB of the test stack.
const MONGO_PORT: u16 = 27017;

/// Rows in the table the stdout export reads, and in the table the local export beside it reads.
const STDOUT_ROWS: [i64; 2] = [50, 70];

/// The ids `stdout` carries, sorted: the first cell of every CSV data line, or of every row of the one Parquet file.
fn stdout_ids(format: &str, stdout: &[u8]) -> Vec<i64> {
    let mut ids: Vec<i64> = if format == "csv" {
        String::from_utf8_lossy(stdout)
            .lines()
            .filter_map(|line| line.split(',').next()?.trim().parse::<f64>().ok())
            .map(|id| id as i64)
            .collect()
    } else if stdout.is_empty() {
        Vec::new()
    } else {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("stdout.parquet"), stdout).unwrap();
        part_ids(dir.path())
    };
    ids.sort_unstable();
    ids
}

/// A `destination: stdout` export beside a local one delivers the same stdout with and without child processes.
fn assert_stdout_survives_child_processes(rig_of: &dyn Fn(&'static str) -> Rig, first_id: i64) {
    let fixture: Vec<i64> = (first_id..first_id + STDOUT_ROWS[0]).collect();
    for format in ["csv", "parquet"] {
        let serial = rig_of(format).run_args(&[]);
        assert!(
            serial.status.success(),
            "{format}: serial control failed:\n{}",
            String::from_utf8_lossy(&serial.stderr)
        );
        assert_eq!(
            stdout_ids(format, &serial.stdout),
            fixture,
            "{format}: control: the serial stdout must carry every source row"
        );
        let children = rig_of(format).run_args(&["--parallel-export-processes"]);
        assert!(
            children.status.success(),
            "{format}: the child-process run failed:\n{}",
            String::from_utf8_lossy(&children.stderr)
        );
        assert_eq!(
            stdout_ids(format, &children.stdout),
            fixture,
            "{format}: --parallel-export-processes must deliver the rows the serial run delivers"
        );
        if format == "csv" {
            assert_eq!(
                children.stdout.len(),
                serial.stdout.len(),
                "csv: --parallel-export-processes must deliver the serial run's stdout bytes"
            );
            assert!(
                children.stdout == serial.stdout,
                "csv: same length, different bytes under --parallel-export-processes"
            );
        }
        assert!(
            String::from_utf8_lossy(&children.stderr).contains(
                "--parallel-export-processes is disabled when an export writes to stdout"
            ),
            "{format}: the in-process fallback must say why:\n{}",
            String::from_utf8_lossy(&children.stderr)
        );
    }
}

/// A rig whose own export prints `a` to stdout, with a second export landing `b` in a local directory.
fn stdout_rig(rig: Rig, a: &str, b: &str, cols: &str, format: &'static str) -> Rig {
    rig.query(&format!("SELECT {cols} FROM {a}"))
        .also_export(b, &format!("SELECT {cols} FROM {b}"))
        .with_format(format)
        .dest_stdout()
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn stdout_survives_child_processes_postgres() {
    require_alive(LiveService::Postgres);
    let (a, b) = (
        seed_pg_numeric_table(STDOUT_ROWS[0]),
        seed_pg_numeric_table(STDOUT_ROWS[1]),
    );
    let rig_of = |format| {
        stdout_rig(
            Rig::pg_batch(a.name()),
            a.name(),
            b.name(),
            "id, name",
            format,
        )
    };
    assert_stdout_survives_child_processes(&rig_of, 0);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn stdout_survives_child_processes_mysql() {
    require_alive(LiveService::Mysql);
    let (a, b) = (
        seed_mysql_numeric_table(STDOUT_ROWS[0]),
        seed_mysql_numeric_table(STDOUT_ROWS[1]),
    );
    let rig_of = |format| {
        stdout_rig(
            Rig::mysql_batch(a.name()),
            a.name(),
            b.name(),
            "id, name",
            format,
        )
    };
    assert_stdout_survives_child_processes(&rig_of, 0);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn stdout_survives_child_processes_mssql() {
    require_alive(LiveService::Mssql);
    let (a, b) = (
        seed_mssql_numeric_table(STDOUT_ROWS[0]),
        seed_mssql_numeric_table(STDOUT_ROWS[1]),
    );
    let rig_of = |format| {
        stdout_rig(
            Rig::mssql_batch(a.name()),
            a.name(),
            b.name(),
            "id, name",
            format,
        )
    };
    assert_stdout_survives_child_processes(&rig_of, 0);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn stdout_survives_child_processes_oracle() {
    require_alive(LiveService::Oracle);
    let (a, b) = (
        seed_oracle_numeric_table(STDOUT_ROWS[0]),
        seed_oracle_numeric_table(STDOUT_ROWS[1]),
    );
    let rig_of = |format| {
        stdout_rig(
            Rig::oracle_batch(a.name()),
            a.name(),
            b.name(),
            "ID, NAME",
            format,
        )
    };
    assert_stdout_survives_child_processes(&rig_of, 1);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn stdout_survives_child_processes_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("run_set_stdout");
    let _guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    let mongo = MongoTest::connect(MONGO_PORT, &db);
    mongo.seed_int_id("a", STDOUT_ROWS[0]);
    mongo.seed_int_id("b", STDOUT_ROWS[1]);
    let rig_of = |format| {
        Rig::mongo_batch("a")
            .source_url(&MongoTest::url(MONGO_PORT, &db))
            .also_batch_export("b", "b", "full")
            .with_format(format)
            .dest_stdout()
    };
    assert_stdout_survives_child_processes(&rig_of, 1);
}
