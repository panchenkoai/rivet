//! The operator contract, from the 2026-10-07 CLI sweep by hand (docs/operator-contract-matrix.yaml):
//! what rivet's exit, its refusal text and its advice commands promise an operator, one generator
//! per pattern and one cell per engine. A cell asserts the correct behaviour; one that fails today
//! is an `open_defect_*` cell acknowledged in dev/release_oracle/known_red.py.
//!
//! Oracles: the exit code and `Error:` line of the real binary, the destination re-read, the
//! source, and the engine's own catalog. Refusals with a remedy go through `Rig::refuses_twice_then`.

use crate::common::*;

const ROWS: i64 = 40;
const MONGO_PORT: u16 = 27017;

/// A fresh `(id, v)` table holding `v = id` for ids `1..=n`, and its drop guard.
fn id_v_table(engine: SqlEngine, tag: &str, n: i64) -> (String, Box<dyn std::any::Any>) {
    keyed_table(engine, tag, n, engine.int64())
}

/// [`id_v_table`] with the integer key type range chunking takes on this engine (Oracle: `NUMBER(18)`).
fn range_table(engine: SqlEngine, tag: &str, n: i64) -> (String, Box<dyn std::any::Any>) {
    let key = if engine.folds_upper() {
        "NUMBER(18)"
    } else {
        engine.int64()
    };
    keyed_table(engine, tag, n, key)
}

/// An `(id, v)` table whose key has type `key`, holding `v = id` for ids `1..=n`.
fn keyed_table(
    engine: SqlEngine,
    tag: &str,
    n: i64,
    key: &str,
) -> (String, Box<dyn std::any::Any>) {
    engine.alive();
    let (id, v, int) = (engine.col("id"), engine.col("v"), engine.int64());
    let (table, guard) = engine.create(tag, &format!("{id} {key} PRIMARY KEY, {v} {int} NOT NULL"));
    insert_ids(engine, &table, 1..=n);
    (table, guard)
}

/// A fresh database on the standalone MongoDB with `n` int-`_id` documents in `t`: its URL, handle and drop guard.
fn mongo_db(tag: &str, n: i64) -> (String, MongoTest, MongoDbGuard) {
    require_alive(LiveService::Mongo);
    let db = unique_name(tag);
    let guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    let m = MongoTest::connect(MONGO_PORT, &db);
    m.seed_int_id("t", n);
    (MongoTest::url(MONGO_PORT, &db), m, guard)
}

/// Insert `ids` with `v = id`.
fn insert_ids(engine: SqlEngine, table: &str, ids: std::ops::RangeInclusive<i64>) {
    let (id, v) = (engine.col("id"), engine.col("v"));
    let rows: Vec<String> = ids.map(|g| format!("({g}, {g})")).collect();
    engine.exec(&format!(
        "INSERT INTO {table} ({id}, {v}) VALUES {}",
        rows.join(", ")
    ));
}

/// Everything an invocation printed, whitespace collapsed.
fn text(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
    .split_whitespace()
    .collect::<Vec<_>>()
    .join(" ")
}

/// Rows in every parquet part under `out`.
fn parquet_rows(out: &std::path::Path) -> usize {
    read_all_parts(out).iter().map(|b| b.num_rows()).sum()
}

/// Every file under `dir`, recursively.
fn files_below(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.filter_map(Result::ok)
        .flat_map(|e| {
            if e.path().is_dir() {
                files_below(&e.path())
            } else {
                vec![e.path()]
            }
        })
        .collect()
}

/// `url` with its host and port replaced by one nothing listens on.
fn unreachable(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("a URL");
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let user = authority
        .rsplit_once('@')
        .map_or(String::new(), |(u, _)| format!("{u}@"));
    format!("{scheme}://{user}127.0.0.1:9/{path}")
}

/// The MinIO credentials a rig's S3 destination reads.
const MINIO_ENV: &[(&str, &str)] = &[
    ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
    ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
    ("AWS_EC2_METADATA_DISABLED", "true"),
];

/// A bucket the empty-prefix cells create and never write to.
const EMPTY_PREFIX_BUCKET: &str = "oc-emptyprefix";

const RANGE_CHECKPOINT: &[&str] = &[
    "chunk_column: id",
    "chunk_size: 5",
    "chunk_checkpoint: true",
];

/// A range-chunked, checkpointed rig over `table`.
fn range_checkpoint_rig(engine: SqlEngine, table: &str) -> Rig {
    RANGE_CHECKPOINT
        .iter()
        .fold(engine.rig(table).mode("chunked"), |r, l| r.export_line(l))
}

// (a) exit 0 over a wrong destination

/// RESULTS 1: a run without the checkpoint beside an unfinished checkpointed run is refused, so no finished run can stand beside a checkpoint a later run would resume.
fn stale_chunk_checkpoint(engine: SqlEngine) {
    let (table, _guard) = range_table(engine, "oc_stale", ROWS);
    let mut rig = range_checkpoint_rig(engine, &table);
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_chunk_complete:2");
    assert!(
        !crash.status.success(),
        "fixture: the first run crashes after its third chunk"
    );
    rig.replace_export_line("chunk_checkpoint", "chunk_checkpoint: false");
    let export = table.clone();
    rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::by_code("RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH", 5),
        vec![
            Remedy::new(
                "restore the checkpoint setting",
                Then::DeliversTheSource,
                |r| {
                    r.replace_export_line("chunk_checkpoint", "chunk_checkpoint: true");
                },
            ),
            Remedy::new(
                "abandons it and the next run starts with a full pass",
                Then::DeliversTheSource,
                move |r| {
                    let reset = r.cli(&["state", "reset-chunks", "--export", &export]);
                    assert!(reset.status.success(), "{}", text(&reset));
                },
            ),
        ],
    );
}

/// RESULTS 2: `run --validate` over a destination that fails verification exits as `rivet validate` does on that prefix.
fn run_validate_flag(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_validate", ROWS);
    run_validate_flag_on(engine.rig(&table));
}

fn run_validate_flag_on(rig: Rig) {
    let passing = rig.run_args(&["--validate"]);
    assert!(
        passing.status.success() && text(&passing).contains("validated: pass"),
        "fixture: `run --validate` passes while the destination meets the policy\n{}",
        text(&passing)
    );
    let mut rig = rig.export_line("verify: content").a_failed_run_may_leave(
        &[Leftover::DeliveredRun],
        "`run --validate` is a verdict on an export that completed: its parts, manifest and success row stay",
    );
    let unchecked = rig.run();
    assert!(
        unchecked.status.success(),
        "fixture: without `--validate` nothing verifies the policy\n{}",
        text(&unchecked)
    );
    let said = rig.refuses_twice_then(
        &["run", "--validate"],
        &[],
        Refused::by_code("RIVET_VALIDATE_FAILED", 3),
        Vec::new(),
    );
    assert!(
        said.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .contains("validated: FAIL"),
        "the summary of a run that failed `--validate` says `validated: FAIL`\n{said}"
    );
    let standalone = rig.cli(&["validate"]);
    assert_eq!(
        standalone.status.code(),
        Some(3),
        "`rivet validate`, which the failure names, gives the same prefix the same exit class\n{}",
        text(&standalone)
    );
}

/// The environment that keeps a cell's state in SQLite beside the config, whichever backend the gate grades.
const SQLITE_STATE: &[(&str, &str)] = &[("RIVET_STATE_URL", "")];

/// Run `rig` once on the SQLite state; panic unless it succeeds.
fn first_run_on_sqlite(rig: &Rig) {
    let out = rig.run_with_envs(SQLITE_STATE);
    assert!(
        out.status.success(),
        "fixture: the first run succeeds\n{}",
        text(&out)
    );
}

/// An incremental export of ids `1..=10` that has run once on the SQLite state, then three more source rows.
fn incremental_with_a_pending_delta(engine: SqlEngine, tag: &str) -> (Rig, Box<dyn std::any::Any>) {
    let (table, guard) = id_v_table(engine, tag, 10);
    let rig = engine
        .rig(&table)
        .mode("incremental")
        .export_line("cursor_column: id");
    first_run_on_sqlite(&rig);
    insert_ids(engine, &table, 11..=13);
    (rig, guard)
}

/// RESULTS 3: a run over a state it cannot write is refused before it exports, and runs once the state is writable.
fn read_only_state(engine: SqlEngine) {
    let (rig, _guard) = incremental_with_a_pending_delta(engine, "oc_rostate");
    read_only_state_on(rig);
}

/// RESULTS 3 on MongoDB: a `full` export that has run once, over a state it can no longer write.
fn mongo_read_only_state() {
    let (url, _m, _guard) = mongo_db("oc_rostate", ROWS);
    let rig = Rig::mongo_batch("t").source_url(&url);
    first_run_on_sqlite(&rig);
    read_only_state_on(rig);
}

/// `rig` has run once on the SQLite state: for each read-only shape, lock it, refuse twice, unlock, deliver.
fn read_only_state_on(mut rig: Rig) {
    for shape in [Local::State, Local::StateDirectory] {
        let locked = rig.read_only(shape);
        let said = rig.refuses_twice_then(
            &["run"],
            SQLITE_STATE,
            Refused::by_code("RIVET_STATE_NOT_WRITABLE", 1),
            vec![Remedy::new(
                "Make the state database writable and run again.",
                Then::DeliversTheSource,
                move |_| drop(locked),
            )],
        );
        assert!(
            !said.contains("Another rivet process"),
            "{shape:?}: a read-only state is not another process's lock\n{said}"
        );
    }
}

/// Sorted `id` values of every part under `out`, whatever the column's integer width (Oracle delivers NUMBER(19) as a decimal).
fn delivered_ids(out: &std::path::Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let mut ids = Vec::new();
    for b in read_all_parts(out) {
        let col = b.column_by_name("id").expect("an id column");
        let col =
            arrow::compute::cast(col, &arrow::datatypes::DataType::Int64).expect("an integer id");
        let col = col.as_any().downcast_ref::<Int64Array>().expect("Int64");
        ids.extend((0..col.len()).map(|i| col.value(i)));
    }
    ids.sort_unstable();
    ids
}

/// RESULTS 3 (the write after the manifest): a run whose cursor write fails delivers its rows and does not exit 0.
fn cursor_write_fails(engine: SqlEngine) {
    let (rig, _guard) = incremental_with_a_pending_delta(engine, "oc_nocursor");
    let rig = rig.a_failed_run_may_leave(
        &[Leftover::DeliveredRun],
        "the cursor write fails after the rows and the manifest are durable: the exit is a verdict on a delivered run",
    );
    let state = rusqlite::Connection::open(rig.config_path().with_file_name(".rivet_state.db"))
        .expect("the SQLite state beside the config");
    let cursor = |state: &rusqlite::Connection| -> String {
        state
            .query_row("SELECT last_cursor_value FROM export_state", [], |r| {
                r.get(0)
            })
            .expect("one export_state row")
    };
    assert_eq!(
        cursor(&state),
        "10",
        "fixture: the first run stored its cursor"
    );
    state
        .execute_batch(
            "CREATE TRIGGER oc_frozen_u BEFORE UPDATE ON export_state \
             BEGIN SELECT RAISE(ABORT, 'export_state is frozen'); END; \
             CREATE TRIGGER oc_frozen_i BEFORE INSERT ON export_state \
             BEGIN SELECT RAISE(ABORT, 'export_state is frozen'); END;",
        )
        .expect("freeze export_state");
    let out = rig.run_with_envs(SQLITE_STATE);
    assert_refused(&out, Refused::by_code("RIVET_STATE_CURSOR_NOT_STORED", 1));
    assert!(
        text(&out).contains("export_state is frozen"),
        "the failure names its cause\n{}",
        text(&out)
    );
    assert_eq!(
        cursor(&state),
        "10",
        "the cursor stays where the last stored run left it"
    );
    assert_eq!(
        delivered_ids(&rig.out_dir()),
        (1..=13).collect::<Vec<i64>>(),
        "the run that could not store its cursor delivered its rows once"
    );
}

/// RESULTS 4: a second `full` run beside a live one is refused, or the prefix still holds each key once.
fn concurrent_full_runs(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_twofull", 200);
    concurrent_full_runs_on(engine.rig(&table));
}

fn concurrent_full_runs_on(rig: Rig) {
    two_full_runs(rig.slowed(200), 200, 1500);
}

/// Start `rig`, start it again `after_ms` later while the first is alive: both exiting 0 must not leave more than `N` rows.
#[allow(non_snake_case)]
fn two_full_runs(rig: Rig, N: i64, after_ms: u64) {
    let mut first = rig.spawn_args_env(&[], &[]);
    std::thread::sleep(std::time::Duration::from_millis(after_ms));
    assert!(
        first.try_wait().unwrap().is_none(),
        "fixture: the first run is alive when the second starts"
    );
    let second = rig.run();
    let first = first.wait().unwrap();
    let rows = parquet_rows(&rig.out_dir());
    assert!(
        !(first.success() && second.status.success()) || rows as i64 == N,
        "two concurrent full runs both exited 0 and the prefix holds {rows} rows for {N} keys"
    );
}

/// RESULTS 5: `validate` on a bucket that does not exist cannot verify (exit 1) and names the missing bucket.
fn validate_on_a_missing_bucket(rig: Rig, envs: &[(&str, &str)]) {
    let out = rig.cli_env(&["validate"], envs);
    let said = text(&out);
    assert!(
        !out.status.success(),
        "`validate` on a bucket that does not exist exited 0{}\n{said}",
        if said.contains("legacy_run") {
            " with `status: legacy_run`"
        } else {
            ""
        },
    );
    assert_eq!(
        out.status.code(),
        Some(1),
        "could-not-verify is exit 1\n{said}"
    );
    assert!(
        said.contains("[RIVET_VERIFY_MANIFEST_READ_ERROR]")
            && said.contains("the bucket or container does not exist")
            && !said.contains("legacy_run"),
        "the verdict names the missing bucket under its verify code\n{said}"
    );
}

/// The neighbour of [`validate_on_a_missing_bucket`]: an empty prefix of a bucket that exists is still a legacy prefix, exit 0.
fn validate_on_an_empty_prefix(rig: Rig, envs: &[(&str, &str)]) {
    let out = rig.cli_env(&["validate"], envs);
    let said = text(&out);
    assert!(
        out.status.success() && said.contains("legacy_run"),
        "`validate` on an empty prefix of an existing bucket is `legacy_run`, exit 0\n{said}"
    );
}

// (b) a failure before the first write

/// RESULTS 8 (owner decision 2026-10-07): a run that never connected leaves the previous `_SUCCESS` and manifest alone.
fn never_connected(mut rig: Rig, url: &str) {
    rig.run_ok();
    let out_dir = rig.out_dir();
    assert!(
        out_dir.join("_SUCCESS").is_file(),
        "fixture: a complete export"
    );
    let dead = unreachable(url);
    rig.rebuilt(|r| r.source_url(&dead));
    let out = rig.run();
    assert!(!out.status.success(), "fixture: nothing listens at {dead}");
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(out_dir.join("manifest.json")).expect("manifest.json"),
    )
    .expect("a JSON manifest");
    let marker = if out_dir.join("_SUCCESS").is_file() {
        "kept"
    } else {
        "withdrawn"
    };
    assert!(
        marker == "kept" && manifest["status"] == "success",
        "a run that never connected left `_SUCCESS` {marker} and manifest.json `{}` over a complete export (exit {:?})",
        manifest["status"].as_str().unwrap_or("?"),
        out.status.code()
    );
}

fn never_connected_sql(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_noconn", ROWS);
    never_connected(engine.rig(&table), engine.url());
}

// (c) refusal -> remedy

/// RESULTS 12: `--resume` over a complete prefix names `--force`; `--resume --force` then runs.
fn resume_force(engine: SqlEngine) {
    let (table, _guard) = range_table(engine, "oc_resume", ROWS);
    resume_force_on(range_checkpoint_rig(engine, &table));
}

fn resume_force_on(mut rig: Rig) {
    rig.run_ok();
    rig.refuses_twice_then(
        &["run", "--resume"],
        &[],
        Refused::uncoded_known_defect(
            1,
            "`--resume refused` is a deliberate refusal the registry has no code for",
        ),
        vec![
            Remedy::new("Pass --force to override", Then::DeliversTheSource, |_| {})
                .rerun_as(&["run", "--resume", "--force"]),
        ],
    );
}

const CHECKPOINT_UNCODED: &str = "a corrupt CDC checkpoint is refused with exit 1 and no code; the registry has RIVET_SOURCE_CDC_CHECKPOINT_INVALID (5), pinned by the *_is_refused_by_code_* cells";

/// A stream with a baseline (`initial: snapshot`) and one drained change, and then a checkpoint that is not JSON.
fn with_a_corrupt_checkpoint(mut s: CdcScenario) -> CdcScenario {
    s.insert(1);
    s.settle();
    s.rig.run_ok();
    s.insert(2);
    s.settle();
    s.rig.run_ok();
    assert!(
        s.rig.checkpoint().is_file(),
        "fixture: the baseline run wrote a checkpoint"
    );
    std::fs::write(s.rig.checkpoint(), b"{not json").expect("corrupt the checkpoint");
    s
}

/// RESULTS 13: the corrupt-checkpoint refusal says "delete it to accept a new anchor"; deleting it then runs.
fn corrupt_checkpoint_remedy(s: CdcScenario) {
    let mut s = with_a_corrupt_checkpoint(s);
    s.rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::uncoded_known_defect(1, CHECKPOINT_UNCODED),
        vec![Remedy::new(
            "delete it to accept a new anchor from a fresh snapshot",
            Then::DeliversTheSource,
            |r| std::fs::remove_file(r.checkpoint()).expect("delete the checkpoint"),
        )],
    );
}

/// RESULTS 14: the heterogeneous-`_id` refusal of a resumed keyset run names "Remove `page_size`"; without it the export runs.
fn mongo_heterogeneous_resume_remedy() {
    require_alive(LiveService::Mongo);
    let db = unique_name("oc_mhet");
    let m = MongoTest::connect(MONGO_PORT, &db);
    let _guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    m.seed_int_id("t", 100);
    let mut rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(MONGO_PORT, &db))
        .mongo("page_size: 40, resume: true")
        .oracle_known_defect(
            "a failed run left: resume-point",
            "known defect: a resumed keyset run the heterogeneous-_id guard refuses has already written its claim (resume_run_id, resume_owner) on the cursor row; the guard must come before the claim",
        );
    rig.run_ok();
    m.insert_many("t", vec![mongodb::bson::doc! { "_id": "zzz", "v": "s" }]);
    rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::uncoded_known_defect(1, "the heterogeneous-_id refusal has no registry code"),
        vec![Remedy::new(
            "Remove `page_size`",
            Then::DeliversTheSource,
            |r| r.rebuilt(|r| r.mongo("")),
        )],
    );
}

/// RESULTS 15: a PostgreSQL CDC run refused for a table that does not exist leaves no replication slot, and creating the table then runs; with a baseline (`initial: snapshot`) or as a changes-only stream.
fn pg_cdc_missing_table_leaves_no_slot(baseline: bool) {
    let slot = unique_name("oc_noslot");
    let _guard = Slot::new(slot.clone());
    let table = unique_name("oc_absent");
    let _table = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());
    let mut rig = Rig::pg_cdc(&table, &slot);
    if baseline {
        rig = rig.cdc("initial: snapshot");
    }
    // A changes-only stream anchors after the table is created: rows inserted before that are not its to deliver.
    let seed = if baseline {
        format!("; INSERT INTO {table} VALUES (1, 1)")
    } else {
        String::new()
    };
    rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::by_code(CDC_PREREQUISITE, 1),
        vec![Remedy::new(
            "Create the table",
            Then::DeliversTheSource,
            |_| {
                let mut pg = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls)
                    .expect("connect postgres-cdc");
                let left: i64 = pg
                    .query_one(
                        "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1",
                        &[&slot],
                    )
                    .expect("pg_replication_slots")
                    .get(0);
                assert_eq!(
                    left, 0,
                    "a PostgreSQL CDC run refused for a table that does not exist left its replication slot"
                );
                pg.batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT){seed}"
                ))
                .expect("create the table the refusal named");
            },
        )],
    );
}

// (d) the exit-code contract

/// RESULTS 11: a second run beside a live checkpointed run is refused as `RIVET_STATE_RUN_IN_PROGRESS` (exit 5).
fn second_run_beside_a_live_checkpointed_one(engine: SqlEngine) {
    let (table, _guard) = range_table(engine, "oc_live", 300);
    let rig = engine
        .rig(&table)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 150")
        .export_line("chunk_checkpoint: true")
        .slowed(600);
    second_run_beside(rig, 300);
}

/// The second run of `rig` while its first is mid-export is refused as `RIVET_STATE_RUN_IN_PROGRESS`, and the first delivers `n` rows.
fn second_run_beside(rig: Rig, n: usize) {
    let (first, second) = rig
        .beside_a_live_run(&[], Rig::has_a_part, Rig::run)
        .answered_while_alive();
    assert!(first.status.success(), "the live run finishes");
    let delivered: usize = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        delivered, n,
        "the live run delivers every row beside the refused one"
    );
    assert_refused(&second, Refused::by_code("RIVET_STATE_RUN_IN_PROGRESS", 5));
}

/// Mac report C: a TRUNCATE inside the stream is refused as `RIVET_SOURCE_CDC_TRUNCATED` (exit 5), every cycle.
fn truncate_is_refused_by_code(mut s: CdcScenario) {
    s.rig.run_ok();
    s.insert(1);
    s.settle();
    s.rig.run_ok();
    s.truncate();
    s.rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::by_code("RIVET_SOURCE_CDC_TRUNCATED", 5),
        vec![],
    );
}

/// Both reports: a corrupt checkpoint is refused as `RIVET_SOURCE_CDC_CHECKPOINT_INVALID` (exit 5), every cycle.
fn corrupt_checkpoint_is_refused_by_code(s: CdcScenario) {
    let mut s = with_a_corrupt_checkpoint(s);
    s.rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::by_code("RIVET_SOURCE_CDC_CHECKPOINT_INVALID", 5),
        vec![],
    );
}

/// RESULTS 17 (PostgreSQL): a slot dropped under a baselined stream is refused as `RIVET_SOURCE_CDC_LOG_GAP` (exit 5).
fn pg_dropped_slot_is_refused_by_code(after_a_changes_run: bool) {
    let slot = unique_name("oc_slotgone_slot");
    let _slot = Slot::new(slot.clone());
    let table = unique_name("oc_slotgone");
    let mut pg =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect postgres-cdc");
    pg.batch_execute(&format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO {table} VALUES (1, 1), (2, 2)"))
        .unwrap();
    let _table = PgCdcTable(table.clone());
    let mut rig = Rig::pg_cdc(&table, &slot).cdc("initial: snapshot");
    rig.run_ok();
    if after_a_changes_run {
        pg.batch_execute(&format!("INSERT INTO {table} VALUES (3, 3)"))
            .unwrap();
        rig.run_ok();
    }
    pg.batch_execute(&format!("INSERT INTO {table} VALUES (4, 4)"))
        .unwrap();
    pg.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .expect("drop the cell's own slot");
    rig.refuses_twice_then(
        &["run"],
        &[],
        Refused::by_code("RIVET_SOURCE_CDC_LOG_GAP", 5),
        vec![],
    );
}

/// Drops a table on the postgres-cdc stand.
struct PgCdcTable(String);

impl Drop for PgCdcTable {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls) {
            let _ = c.batch_execute(&format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

// (e) advice parity

const PLAN_UNREADABLE: &str = "RIVET_PLAN_SOURCE_UNREADABLE";

/// RESULTS 10: `plan` over a table that does not exist fails by code and seals no artifact.
fn plan_on_a_missing_table(engine: SqlEngine) {
    engine.alive();
    plan_on_an_unreadable_source(engine.rig(&unique_name("oc_absent")), 1);
}

/// RESULTS 10 (hand sweep): `plan` with a password the source refuses fails by code and seals no artifact.
fn plan_with_a_wrong_password(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_planpw", 3);
    let url = with_password(engine.url(), "not_the_password");
    plan_on_an_unreadable_source(engine.rig(&table).source_url(&url), 1);
}

/// `url` with its password replaced.
fn with_password(url: &str, password: &str) -> String {
    let (scheme, rest) = url.split_once("://").expect("a URL");
    let (userinfo, host) = rest.rsplit_once('@').expect("a URL with credentials");
    let user = userinfo.split_once(':').map_or(userinfo, |(u, _)| u);
    format!("{scheme}://{user}:{password}@{host}")
}

/// `plan` over a source it cannot read ends as `RIVET_PLAN_SOURCE_UNREADABLE` with exit `exit`, and writes no plan file.
fn plan_on_an_unreadable_source(rig: Rig, exit: i32) {
    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("plan.json");
    let out = rig.plan_json_env(&plan, &[], &[]);
    assert!(
        !out.status.success() && !plan.exists(),
        "`plan` over a table that does not exist exited {} and {} the artifact\n{}",
        out.status
            .code()
            .map_or("on a signal".to_string(), |c| c.to_string()),
        if plan.exists() {
            "sealed"
        } else {
            "did not seal"
        },
        text(&out)
    );
    assert_refused(&out, Refused::by_code(PLAN_UNREADABLE, exit));
}

/// Mac report E: `doctor` is not green for a CDC export on a server whose `wal_level` the run refuses.
fn doctor_agrees_on_a_server_without_logical_wal() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = SqlEngine::Pg.table("oc_nowal");
    let slot = unique_name("oc_nowal_slot");
    let _slot = Slot::new(slot.clone());
    let rig = Rig::pg_cdc(&table, &slot).source_url(POSTGRES_URL);
    let (_, run) = rig.run_after_doctor();
    assert!(
        !run.status.success(),
        "fixture: the batch stand runs wal_level=replica, which CDC refuses"
    );
}

// (f) single rows

/// RESULTS 9: a `RIVET_STATE_URL` rivet cannot use is refused, not replaced by a SQLite file beside the config.
fn unusable_state_url(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_stateurl", 10);
    unusable_state_url_on(engine.rig(&table), None);
}

/// Refuse the typo twice, then each remedy: unset it, and (with a state server) the corrected URL.
fn unusable_state_url_on(mut rig: Rig, corrected: Option<&str>) {
    let mut remedies = vec![
        Remedy::new(
            "unset it to keep the state in `.rivet_state.db` beside the config",
            Then::DeliversTheSource,
            |_| {},
        )
        .rerun_env(&[]),
    ];
    if let Some(url) = corrected {
        remedies.push(
            Remedy::new(
                "Set RIVET_STATE_URL to a `postgres://` or `postgresql://` URL",
                Then::DeliversTheSource,
                |_| {},
            )
            .rerun_env(&[("RIVET_STATE_URL", url)]),
        );
    }
    rig.refuses_twice_then(
        &["run"],
        &[(
            "RIVET_STATE_URL",
            "postgre://rivet:rivet@127.0.0.1:5433/rivet_nope",
        )],
        Refused::by_code("RIVET_STATE_URL_SCHEME_UNSUPPORTED", 1),
        remedies,
    );
}

// round two: the rows round one left as gaps

/// RESULTS 11 on MongoDB: a resumable paged export beside its own live run.
fn mongo_second_run_beside_a_live_resumable_one() {
    let (url, _m, _guard) = mongo_db("oc_live", MONGO_SLOW);
    let rig = Rig::mongo_batch("t")
        .source_url(&url)
        .mongo("page_size: 500, resume: true");
    second_run_beside(rig, MONGO_SLOW as usize);
}

/// RESULTS 12 on MongoDB: `--resume` over a complete resumable export names `--force`, and `--resume --force` runs.
fn mongo_resume_force() {
    let (url, _m, _guard) = mongo_db("oc_resume", ROWS);
    resume_force_on(
        Rig::mongo_batch("t")
            .source_url(&url)
            .mongo("page_size: 10, resume: true"),
    );
}

/// The earlier night's finding: range chunking takes an Oracle `NUMBER(19)` primary key (scale 0) as an integer key.
#[cfg(feature = "oracle")]
fn oracle_range_chunking_takes_a_number_19_key() {
    let (table, _guard) = id_v_table(SqlEngine::Oracle, "oc_n19", ROWS);
    let out = range_checkpoint_rig(SqlEngine::Oracle, &table).run();
    assert!(
        out.status.success(),
        "range chunking refused an Oracle NUMBER(19) integer key (exit {:?})\n{}",
        out.status.code(),
        text(&out)
    );
}

/// Documents enough that a MongoDB read (which `tuning.throttle_ms` does not slow) outlives the start of a second run.
const MONGO_SLOW: i64 = 300_000;

const LOG_GAP: &str = "RIVET_SOURCE_CDC_LOG_GAP";
const CDC_PREREQUISITE: &str = "RIVET_SOURCE_CDC_PREREQUISITE";

/// SQL Server: the change table cleaned past the checkpoint (what retention does) is refused as a log gap, pinned checkpoint or not.
fn mssql_cleanup_past_the_checkpoint(after_a_changes_run: bool) {
    let mut s = CdcScenario::mssql_with("oc_gap", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
        r.cdc("initial: snapshot")
    });
    s.insert(1);
    s.settle();
    s.rig.run_ok();
    if after_a_changes_run {
        s.insert(2);
        s.settle();
        s.rig.run_ok();
    }
    s.update(1);
    s.insert(3);
    s.settle();
    mssql_cdc_exec(&format!(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); \
         EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_{}', \
         @low_water_mark = @lw, @threshold = 5000;",
        s.table
    ));
    s.rig
        .refuses_twice_then(&["run"], &[], Refused::by_code(LOG_GAP, 5), vec![]);
}

/// A single-node replica set in its own container with the smallest oplog the server accepts (990 MB), removed on drop.
struct ThrowawayReplicaSet {
    container: String,
    port: u16,
}

impl ThrowawayReplicaSet {
    fn docker(args: &[&str]) -> std::process::Output {
        std::process::Command::new("docker")
            .args(args)
            .output()
            .expect("docker on PATH")
    }

    fn start() -> Self {
        let container = unique_name("rivet_oc_oplog");
        let image = format!(
            "mongo:{}",
            std::env::var("MONGO_VERSION").unwrap_or_else(|_| "7".into())
        );
        let run = Self::docker(&[
            "run",
            "-d",
            "--rm",
            "--name",
            &container,
            "-p",
            "127.0.0.1::27017",
            &image,
            "--replSet",
            "rs0",
            "--oplogSize",
            "990",
            "--bind_ip_all",
        ]);
        assert!(
            run.status.success(),
            "fixture: docker run {image}:\n{}",
            text(&run)
        );
        let mut rs = Self { container, port: 0 };
        let mapped = text(&Self::docker(&["port", &rs.container, "27017/tcp"]));
        rs.port = mapped
            .lines()
            .find_map(|l| l.trim().rsplit_once(':')?.1.parse().ok())
            .unwrap_or_else(|| panic!("fixture: no mapped port in `{mapped}`"));
        let t0 = std::time::Instant::now();
        while !rs
            .mongosh("try { rs.initiate({_id: 'rs0', members: [{_id: 0, host: '127.0.0.1:27017'}]}) } catch (e) {}; print(db.hello().isWritablePrimary)")
            .contains("true")
        {
            assert!(
                t0.elapsed().as_secs() < 90,
                "fixture: the throwaway replica set never elected a primary"
            );
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        rs
    }

    /// Evaluate `js` in the container's `mongosh`; stdout and stderr together.
    fn mongosh(&self, js: &str) -> String {
        text(&Self::docker(&[
            "exec",
            &self.container,
            "mongosh",
            "--quiet",
            "--eval",
            js,
        ]))
    }

    /// Write more than one oplog of filler and keep writing (the server truncates nothing newer than its last checkpoint, and only as a later write closes a truncate marker) until the oplog no longer holds anything as old as this call.
    fn roll_the_oplog(&self) {
        let before: i64 = self
            .mongosh("print(db.hello().operationTime.t)")
            .trim()
            .parse()
            .expect("fixture: the server's operationTime");
        std::thread::sleep(std::time::Duration::from_secs(2));
        let said = self.mongosh(
            "const f = db.getSiblingDB('oc_filler').f; const p = 'x'.repeat(15 * 1024 * 1024); \
             for (let i = 0; i < 80; i++) { f.insertOne({p}); } db.getSiblingDB('oc_filler').dropDatabase(); print('filled')",
        );
        assert!(said.contains("filled"), "fixture: filler writes:\n{said}");
        let t0 = std::time::Instant::now();
        loop {
            let first = self.mongosh(
                "const f = db.getSiblingDB('oc_filler').f; const p = 'x'.repeat(15 * 1024 * 1024); \
                 for (let i = 0; i < 5; i++) { f.insertOne({p}); } f.drop(); \
                 print(db.getSiblingDB('local').oplog.rs.find().sort({$natural: 1}).limit(1).next().ts.t)",
            );
            if first.trim().parse::<i64>().is_ok_and(|t| t > before) {
                return;
            }
            assert!(
                t0.elapsed().as_secs() < 240,
                "fixture: more than 1.2 GB of writes did not roll a 990 MB oplog: its first entry is at {first}, the checkpoint at {before}"
            );
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    }
}

impl Drop for ThrowawayReplicaSet {
    fn drop(&mut self) {
        let _ = Self::docker(&["rm", "-f", "-v", &self.container]);
    }
}

/// MongoDB: the oplog rolled past the resume token is refused as a log gap. Server-wide, hence on a replica set of the cell's own.
fn mongo_oplog_rolled_past_the_checkpoint(after_a_changes_run: bool) {
    let rs = ThrowawayReplicaSet::start();
    let mut s = CdcScenario::mongo_on(rs.port, "oc_gap", |r, _| {
        r.cdc("initial: snapshot")
            .no_oracle("the rig oracle has no reader for a replica set the cell starts itself")
    });
    s.insert(1);
    s.rig.run_ok();
    if after_a_changes_run {
        s.insert(2);
        s.rig.run_ok();
    }
    s.update(1);
    s.insert(3);
    rs.roll_the_oplog();
    s.rig
        .refuses_twice_then(&["run"], &[], Refused::by_code(LOG_GAP, 5), vec![]);
}

const REPLICA_PRIMARY: &str = "mysql://root:rivet@127.0.0.1:3308/rivet";
const REPLICA_ROOT: &str = "mysql://root:rivet@127.0.0.1:3309/rivet";
const REPLICA_RIVET: &str = "mysql://rivet:rivet@127.0.0.1:3309/rivet";
const REPLICA_NOLOG_RIVET: &str = "mysql://rivet:rivet@127.0.0.1:3310/rivet";

/// Drops a table on the replica stand's primary (the drop replicates).
struct ReplicatedTable(String);

impl Drop for ReplicatedTable {
    fn drop(&mut self) {
        use mysql::prelude::Queryable as _;
        if let Ok(mut c) = mysql::Conn::new(REPLICA_PRIMARY) {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

/// RESULTS 17 (MySQL): the binlogs purged past the checkpoint are refused as a log gap. Server-wide on the replica (:3309), hence serial.
fn mysql_binlogs_purged_past_the_checkpoint(after_a_changes_run: bool) {
    use mysql::prelude::Queryable as _;
    let mut p = mysql::Conn::new(REPLICA_PRIMARY).expect("connect mysql-primary :3308");
    let mut r = mysql::Conn::new(REPLICA_ROOT).expect("connect mysql-replica :3309");
    let running: Option<mysql::Row> = r.query_first("SHOW REPLICA STATUS").unwrap();
    let io: Option<String> = running.and_then(|row| row.get("Replica_SQL_Running"));
    assert_eq!(
        io.as_deref(),
        Some("Yes"),
        "fixture: the replica stand replicates (live_cdc_replica wires it)"
    );
    let table = unique_name("oc_purge");
    let _table = ReplicatedTable(table.clone());
    p.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"
    ))
    .unwrap();
    let mut seen = 0;
    let mut write = |p: &mut mysql::Conn, r: &mut mysql::Conn, id: i64| {
        p.query_drop(format!("INSERT INTO {table} VALUES ({id}, {id})"))
            .unwrap();
        seen += 1;
        let q = format!("SELECT COUNT(*) FROM {table}");
        let t0 = std::time::Instant::now();
        while r.query_first::<i64, _>(&q).ok().flatten() != Some(seen) {
            assert!(
                t0.elapsed().as_secs() < 30,
                "fixture: the replica never held row {id}"
            );
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    };
    write(&mut p, &mut r, 1);
    let mut rig = Rig::mysql_cdc(&table)
        .source_url(REPLICA_RIVET)
        .cdc("initial: snapshot");
    rig.run_ok();
    if after_a_changes_run {
        write(&mut p, &mut r, 2);
        rig.run_ok();
    }
    write(&mut p, &mut r, 3);
    r.query_drop("FLUSH BINARY LOGS").unwrap();
    let last: mysql::Row = r.query_first("SHOW MASTER STATUS").unwrap().unwrap();
    let file: String = last.get(0).unwrap();
    r.query_drop(format!("PURGE BINARY LOGS TO '{file}'"))
        .unwrap();
    rig.refuses_twice_then(&["run"], &[], Refused::by_code(LOG_GAP, 5), vec![]);
}

/// Both reports, E/I: `doctor` is not green for a CDC export whose prerequisite the run then refuses.
fn doctor_agrees_where_the_stream_is_refused(rig: Rig) {
    let (_, run) = rig.run_after_doctor();
    assert!(
        !run.status.success(),
        "fixture: the stream lacks its prerequisite, so the run refuses"
    );
}

/// RESULTS 16: three `rivet cdc` drains to stdout over one stream emit three changes once, not on every run.
fn cdc_stdout_emits_each_change_once(mut s: CdcScenario) {
    let anchor = s.rig.cli_cdc_ndjson(true);
    assert_eq!(
        anchor.len(),
        0,
        "fixture: nothing changed before the anchor run"
    );
    for id in 1..=3 {
        s.insert(id);
    }
    s.settle();
    let drains: Vec<usize> = (0..3).map(|_| s.rig.cli_cdc_ndjson(true).len()).collect();
    assert_eq!(
        drains,
        [3, 0, 0],
        "three `rivet cdc` runs to stdout over three changes emitted {drains:?} events"
    );
}

/// `rivet init --mode <mode>` over `table`: the config it wrote, of that mode.
fn init_config(url: &str, table: &str, mode: &str) -> InitConfig {
    let cfg = InitConfig::generate(url, &["--table", table, "--mode", mode]);
    assert!(
        cfg.init.status.success(),
        "`rivet init --mode {mode}` failed (exit {:?})\n{}",
        cfg.init.status.code(),
        text(&cfg.init)
    );
    assert!(
        cfg.yaml().contains(&format!("mode: {mode}")),
        "fixture: `rivet init --mode {mode}` wrote an export of that mode\n{}",
        cfg.yaml()
    );
    cfg
}

/// The generated config runs as written; the default oracle grades what it delivered.
fn generated_config_runs(cfg: &InitConfig) {
    let out = cfg.cli(&["run"]);
    assert!(
        out.status.success(),
        "the config `rivet init` wrote does not run (exit {:?})\n{}\n--- config ---\n{}",
        out.status.code(),
        text(&out),
        cfg.yaml()
    );
}

/// RESULTS 19 and the sweep's general row: the config `rivet init --mode <mode>` writes for `table` runs as written.
fn init_config_runs(url: &str, table: &str, mode: &str) {
    generated_config_runs(&init_config(url, table, mode));
}

/// [`init_config_runs`] over the standard table of a batch engine.
fn init_batch_config_runs(engine: SqlEngine, mode: &str) {
    engine.alive();
    let (table, _guard) = engine.table("oc_init");
    engine.insert(&table, 1..=20, 120, Some(5));
    init_config_runs(engine.url(), &table, mode);
}

/// RESULTS 20: `check` names the strategy `run` then uses for `mode: chunked` with no chunk column.
fn check_names_the_strategy_run_uses(engine: SqlEngine) {
    let (table, _guard) = range_table(engine, "oc_nochunk", ROWS);
    // The table fits one default chunk: the planner may run it as one unchunked pass.
    let (check, chunk_parts) = check_then_run(engine.rig(&table).mode("chunked"));
    assert_eq!(
        check.contains("Strategy: chunked("),
        chunk_parts > 0,
        "`check` said `Strategy: chunked` and `run` exported {ROWS} rows as one unchunked part, or the reverse ({chunk_parts} chunk part(s))\n{check}"
    );
    assert!(
        !check.contains("column: ?"),
        "`check` names the column the run ranges on\n{check}"
    );
    // Eight chunks of five rows: the planner range-chunks on the primary key it resolved.
    let (check, chunk_parts) = check_then_run(
        engine
            .rig(&table)
            .mode("chunked")
            .export_line("chunk_size: 5"),
    );
    let key = check
        .split_once("Strategy: chunked(")
        .and_then(|(_, rest)| rest.split_once(", size=5)"))
        .map(|(key, _)| key.to_string())
        .unwrap_or_default();
    assert!(
        key.eq_ignore_ascii_case("id")
            && check.contains(&format!("Mode: chunked (column: {key}, size: 5)")),
        "`check` names the range chunking the run uses, on the primary key, in both lines\n{check}"
    );
    assert_eq!(chunk_parts, 8, "the run writes one part per chunk of five");
}

/// What `check` printed for `rig`, then how many range-chunk parts its `run` wrote (0 for one unchunked pass).
fn check_then_run(rig: Rig) -> (String, usize) {
    let check = text(&rig.cli(&["check"]));
    rig.run_ok();
    let parts: Vec<String> = files_below(&rig.out_dir())
        .iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        parquet_rows(&rig.out_dir()),
        ROWS as usize,
        "fixture: the run delivers the table"
    );
    let chunk_parts = parts.iter().filter(|n| n.contains("_chunk")).count();
    assert!(
        chunk_parts == 0 || chunk_parts == parts.len(),
        "a run is range-chunked or it is not: {parts:?}"
    );
    (check, chunk_parts)
}

/// RESULTS 15 (standby twin): a bounded CDC run refused on a PostgreSQL standby leaves nothing, and pointing at the primary then runs.
fn standby_refusal_leaves_nothing_and_its_remedy_runs() {
    let tbl = unique_name("oc_standby");
    let slot = unique_name("oc_standby_slot");
    let mut p = postgres::Client::connect(PG_STANDBY_PRIMARY_URL, postgres::NoTls)
        .expect("connect the cdc-standby primary");
    let mut sb = postgres::Client::connect(PG_STANDBY_URL, postgres::NoTls)
        .expect("connect the cdc-standby replica");
    let _slot = Slot::on(PG_STANDBY_URL, slot.clone());
    let _slot_primary = Slot::on(PG_STANDBY_PRIMARY_URL, slot.clone());
    p.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO {tbl} VALUES (1, 1), (2, 2)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(PG_STANDBY_PRIMARY_URL, tbl.clone());
    let t0 = std::time::Instant::now();
    let q = format!("SELECT COUNT(*) FROM {tbl}");
    while sb.query_one(&q, &[]).ok().map(|r| r.get::<_, i64>(0)) != Some(2) {
        assert!(
            t0.elapsed().as_secs() < 30,
            "fixture: the standby never held the table"
        );
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let nudger = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = p.execute("SELECT pg_log_standby_snapshot()", &[]);
                std::thread::sleep(std::time::Duration::from_millis(300));
            }
        })
    };
    let mut rig = Rig::pg_cdc_standby(&tbl, &slot).cdc("initial: snapshot");
    let verdict = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rig.refuses_twice_then(
            &["run"],
            &[],
            Refused::by_code(CDC_PREREQUISITE, 1),
            vec![Remedy::new(
                "point the source at the primary",
                Then::DeliversTheSource,
                |r| r.rebuilt(|r| r.source_url(PG_STANDBY_PRIMARY_URL)),
            )],
        )
    }));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    nudger.join().expect("the standby nudger");
    if let Err(panic) = verdict {
        std::panic::resume_unwind(panic);
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_run_without_the_checkpoint_beside_an_unfinished_run_is_refused_postgres() {
    stale_chunk_checkpoint(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_without_the_checkpoint_beside_an_unfinished_run_is_refused_mysql() {
    stale_chunk_checkpoint(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_without_the_checkpoint_beside_an_unfinished_run_is_refused_mssql() {
    stale_chunk_checkpoint(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn run_validate_fails_the_run_over_a_failed_validation_postgres() {
    run_validate_flag(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn run_validate_fails_the_run_over_a_failed_validation_mysql() {
    run_validate_flag(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn run_validate_fails_the_run_over_a_failed_validation_mssql() {
    run_validate_flag(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn run_validate_fails_the_run_over_a_failed_validation_oracle() {
    run_validate_flag(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_run_over_a_read_only_state_is_refused_and_runs_once_it_is_writable_postgres() {
    read_only_state(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_over_a_read_only_state_is_refused_and_runs_once_it_is_writable_mysql() {
    read_only_state(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_over_a_read_only_state_is_refused_and_runs_once_it_is_writable_mssql() {
    read_only_state(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_over_a_read_only_state_is_refused_and_runs_once_it_is_writable_oracle() {
    read_only_state(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_run_whose_cursor_write_fails_delivers_and_does_not_exit_0_postgres() {
    cursor_write_fails(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_whose_cursor_write_fails_delivers_and_does_not_exit_0_mysql() {
    cursor_write_fails(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_whose_cursor_write_fails_delivers_and_does_not_exit_0_mssql() {
    cursor_write_fails(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_whose_cursor_write_fails_delivers_and_does_not_exit_0_oracle() {
    cursor_write_fails(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (concurrent full runs), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_concurrent_full_run_does_not_double_the_prefix_postgres() {
    concurrent_full_runs(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (concurrent full runs), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_concurrent_full_run_does_not_double_the_prefix_mysql() {
    concurrent_full_runs(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (concurrent full runs), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_concurrent_full_run_does_not_double_the_prefix_mssql() {
    concurrent_full_runs(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (concurrent full runs), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_concurrent_full_run_does_not_double_the_prefix_oracle() {
    concurrent_full_runs(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (failed manifest before a write), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_never_connected_keeps_the_export_complete_postgres() {
    never_connected_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (failed manifest before a write), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_never_connected_keeps_the_export_complete_mysql() {
    never_connected_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (failed manifest before a write), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_never_connected_keeps_the_export_complete_mssql() {
    never_connected_sql(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (failed manifest before a write), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_never_connected_keeps_the_export_complete_oracle() {
    never_connected_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (--resume --force remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_resume_force_runs_over_a_complete_prefix_postgres() {
    resume_force(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (--resume --force remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_resume_force_runs_over_a_complete_prefix_mysql() {
    resume_force(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (--resume --force remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_resume_force_runs_over_a_complete_prefix_mssql() {
    resume_force(SqlEngine::Mssql);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_checkpointed_run_is_refused_by_code_postgres() {
    second_run_beside_a_live_checkpointed_one(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_checkpointed_run_is_refused_by_code_mysql() {
    second_run_beside_a_live_checkpointed_one(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_checkpointed_run_is_refused_by_code_mssql() {
    second_run_beside_a_live_checkpointed_one(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_refuses_a_table_that_does_not_exist_postgres() {
    plan_on_a_missing_table(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_refuses_a_password_the_source_rejects_postgres() {
    plan_with_a_wrong_password(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn plan_refuses_a_table_that_does_not_exist_mysql() {
    plan_on_a_missing_table(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn plan_refuses_a_password_the_source_rejects_mysql() {
    plan_with_a_wrong_password(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn plan_refuses_a_table_that_does_not_exist_mssql() {
    plan_on_a_missing_table(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn plan_refuses_a_password_the_source_rejects_mssql() {
    plan_with_a_wrong_password(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn plan_refuses_a_table_that_does_not_exist_oracle() {
    plan_on_a_missing_table(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn plan_refuses_a_password_the_source_rejects_oracle() {
    plan_with_a_wrong_password(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn an_unusable_state_url_is_refused_and_each_remedy_runs_postgres() {
    let (table, _guard) = id_v_table(SqlEngine::Pg, "oc_stateurl", 10);
    let state = ScratchStateDb::new("oc_stateurl");
    unusable_state_url_on(SqlEngine::Pg.rig(&table), Some(&state.url()));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_unusable_state_url_is_refused_and_each_remedy_runs_mysql() {
    unusable_state_url(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_unusable_state_url_is_refused_and_each_remedy_runs_mssql() {
    unusable_state_url(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unusable_state_url_is_refused_and_each_remedy_runs_oracle() {
    unusable_state_url(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc mysql-cdc; open defect (corrupt checkpoint remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_deleting_a_corrupt_cdc_checkpoint_accepts_a_new_anchor_mysql() {
    corrupt_checkpoint_remedy(CdcScenario::mysql_with(
        "oc_ckpt",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc mysql-cdc; open defect (uncoded corrupt-checkpoint refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_corrupt_cdc_checkpoint_is_refused_by_code_mysql() {
    corrupt_checkpoint_is_refused_by_code(CdcScenario::mysql_with(
        "oc_ckptc",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose mssql (CDC); open defect (corrupt checkpoint remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_deleting_a_corrupt_cdc_checkpoint_accepts_a_new_anchor_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    corrupt_checkpoint_remedy(CdcScenario::mssql_with(
        "oc_ckpt",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose mssql (CDC); open defect (uncoded corrupt-checkpoint refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_corrupt_cdc_checkpoint_is_refused_by_code_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    corrupt_checkpoint_is_refused_by_code(CdcScenario::mssql_with(
        "oc_ckptc",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc mysql-cdc; open defect (uncoded TRUNCATE refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cdc_truncate_is_refused_by_code_mysql() {
    truncate_is_refused_by_code(CdcScenario::mysql(
        "oc_trunc",
        "id BIGINT PRIMARY KEY, v BIGINT",
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc postgres-cdc; open defect (uncoded TRUNCATE refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cdc_truncate_is_refused_by_code_postgres() {
    truncate_is_refused_by_code(CdcScenario::pg(
        "oc_trunc",
        "id BIGINT PRIMARY KEY, v BIGINT",
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc postgres-cdc; open defect (uncoded dropped-slot refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_dropped_slot_is_refused_by_code_postgres() {
    pg_dropped_slot_is_refused_by_code(false);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_refused_cdc_run_on_a_missing_table_leaves_no_slot_postgres() {
    pg_cdc_missing_table_leaves_no_slot(true);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_refused_changes_only_cdc_run_on_a_missing_table_leaves_no_slot_postgres() {
    pg_cdc_missing_table_leaves_no_slot(false);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn doctor_is_not_green_where_the_cdc_run_refuses_wal_level_postgres() {
    doctor_agrees_on_a_server_without_logical_wal();
}

#[test]
#[ignore = "live+gate-only: docker compose up -d mongo; open defect (mongo page_size remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_removing_page_size_runs_after_the_heterogeneous_id_refusal_mongo() {
    mongo_heterogeneous_resume_remedy();
}

#[test]
#[ignore = "live+gate-only: docker compose up -d mongo; open defect (failed manifest before a write), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_never_connected_keeps_the_export_complete_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("oc_noconn");
    let _guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    MongoTest::connect(MONGO_PORT, &db).seed_int_id("t", 20);
    let url = MongoTest::url(MONGO_PORT, &db);
    never_connected(Rig::mongo_batch("t").source_url(&url), &url);
}

#[test]
#[ignore = "live: requires docker compose minio"]
fn validate_fails_on_a_missing_bucket_s3() {
    require_alive(LiveService::Minio);
    let rig = Rig::pg_batch("oc_nobucket").dest_s3(
        &unique_name("oc-nobucket").replace('_', "-"),
        "p",
        MINIO_ENDPOINT,
    );
    validate_on_a_missing_bucket(rig, MINIO_ENV);
}

#[test]
#[ignore = "live: requires docker compose minio"]
fn validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_s3() {
    ensure_minio_bucket(EMPTY_PREFIX_BUCKET);
    let rig = Rig::pg_batch("oc_emptyprefix").dest_s3(
        EMPTY_PREFIX_BUCKET,
        &unique_name("p"),
        MINIO_ENDPOINT,
    );
    validate_on_an_empty_prefix(rig, MINIO_ENV);
}

#[test]
#[ignore = "live: requires docker compose fake-gcs"]
fn validate_fails_on_a_missing_bucket_gcs() {
    require_alive(LiveService::FakeGcs);
    validate_on_a_missing_bucket(
        Rig::pg_batch("oc_nobucket").dest_gcs(&unique_name("oc_nobucket"), "p", FAKE_GCS_ENDPOINT),
        &[],
    );
}

#[test]
#[ignore = "live: requires docker compose fake-gcs"]
fn validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_gcs() {
    ensure_gcs_bucket(EMPTY_PREFIX_BUCKET);
    let rig = Rig::pg_batch("oc_emptyprefix").dest_gcs(
        EMPTY_PREFIX_BUCKET,
        &unique_name("p"),
        FAKE_GCS_ENDPOINT,
    );
    validate_on_an_empty_prefix(rig, &[]);
}

#[test]
#[ignore = "live: requires docker compose azurite"]
fn validate_fails_on_a_missing_bucket_azure() {
    require_alive(LiveService::Azurite);
    let rig =
        Rig::pg_batch("oc_nobucket").dest_azure(&unique_name("oc-nobucket").replace('_', "-"), "p");
    validate_on_a_missing_bucket(rig, &[("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)]);
}

#[test]
#[ignore = "live: requires docker compose azurite"]
fn validate_reads_an_empty_prefix_of_an_existing_bucket_as_legacy_azure() {
    ensure_azure_container(EMPTY_PREFIX_BUCKET);
    let rig = Rig::pg_batch("oc_emptyprefix").dest_azure(EMPTY_PREFIX_BUCKET, &unique_name("p"));
    validate_on_an_empty_prefix(rig, &[("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)]);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_without_the_checkpoint_beside_an_unfinished_run_is_refused_oracle() {
    stale_chunk_checkpoint(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (--resume --force remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_resume_force_runs_over_a_complete_prefix_oracle() {
    resume_force(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_checkpointed_run_is_refused_by_code_oracle() {
    second_run_beside_a_live_checkpointed_one(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (range chunking refuses NUMBER(19)), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_range_chunking_takes_a_number_19_key_oracle() {
    oracle_range_chunking_takes_a_number_19_key();
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn a_run_over_a_read_only_state_is_refused_and_runs_once_it_is_writable_mongo() {
    mongo_read_only_state();
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn run_validate_fails_the_run_over_a_failed_validation_mongo() {
    let (url, _m, _guard) = mongo_db("oc_validate", ROWS);
    run_validate_flag_on(Rig::mongo_batch("t").source_url(&url));
}

#[test]
#[ignore = "live+gate-only: docker compose up -d mongo; open defect (concurrent full runs), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_concurrent_full_run_does_not_double_the_prefix_mongo() {
    let (url, _m, _guard) = mongo_db("oc_twofull", MONGO_SLOW);
    two_full_runs(Rig::mongo_batch("t").source_url(&url), MONGO_SLOW, 150);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn resume_force_runs_over_a_complete_prefix_mongo() {
    mongo_resume_force();
}

#[test]
#[ignore = "live+gate-only: docker compose up -d mongo; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_resumable_run_is_refused_by_code_mongo() {
    mongo_second_run_beside_a_live_resumable_one();
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn plan_refuses_a_collection_that_does_not_exist_mongo() {
    let (url, _m, _guard) = mongo_db("oc_plan", 1);
    plan_on_an_unreadable_source(Rig::mongo_batch("absent").source_url(&url), 1);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn check_refuses_a_collection_that_does_not_exist_as_run_does_mongo() {
    let (url, _m, _guard) = mongo_db("oc_checkabsent", 1);
    let rig = Rig::mongo_batch("absent").source_url(&url);
    let (check, run) = (rig.cli(&["check"]), rig.run());
    let refusal = "does not exist — an export of it would succeed with 0 rows";
    assert!(
        !check.status.success() && text(&check).contains(refusal),
        "`check` is not green over a collection `run` refuses\n{}",
        text(&check)
    );
    assert_refused(
        &check,
        Refused::by_code("RIVET_SOURCE_COLLECTION_NOT_FOUND", 1),
    );
    assert!(
        !run.status.success() && text(&run).contains(refusal),
        "fixture: `run` refuses the absent collection\n{}",
        text(&run)
    );
    let present = Rig::mongo_batch("t").source_url(&url);
    assert!(
        present.cli(&["check"]).status.success(),
        "`check` stays green over a collection that exists"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn an_unusable_state_url_is_refused_and_each_remedy_runs_mongo() {
    let (url, _m, _guard) = mongo_db("oc_stateurl", 10);
    unusable_state_url_on(Rig::mongo_batch("t").source_url(&url), None);
}

#[test]
#[ignore = "live+gate-only: docker compose mongo-rs; open defect (corrupt checkpoint remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_deleting_a_corrupt_cdc_checkpoint_accepts_a_new_anchor_mongo() {
    corrupt_checkpoint_remedy(CdcScenario::mongo_with("oc_ckpt", |r, _| {
        r.cdc("initial: snapshot")
    }));
}

#[test]
#[ignore = "live+gate-only: docker compose mongo-rs; open defect (uncoded corrupt-checkpoint refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_corrupt_cdc_checkpoint_is_refused_by_code_mongo() {
    corrupt_checkpoint_is_refused_by_code(CdcScenario::mongo_with("oc_ckptc", |r, _| {
        r.cdc("initial: snapshot")
    }));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle (LogMiner); open defect (corrupt checkpoint remedy), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_deleting_a_corrupt_cdc_checkpoint_accepts_a_new_anchor_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    corrupt_checkpoint_remedy(CdcScenario::oracle_with("oc_ckpt", |r, _| {
        r.cdc("initial: snapshot")
    }));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle (LogMiner); open defect (uncoded corrupt-checkpoint refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_corrupt_cdc_checkpoint_is_refused_by_code_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    corrupt_checkpoint_is_refused_by_code(CdcScenario::oracle_with("oc_ckptc", |r, _| r));
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc postgres-cdc; open defect (uncoded corrupt-checkpoint refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_corrupt_cdc_checkpoint_is_refused_by_code_postgres() {
    corrupt_checkpoint_is_refused_by_code(CdcScenario::pg_with(
        "oc_ckptc",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc("initial: snapshot").relative_checkpoint("cdc.ckpt"),
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose mongo-rs; open defect (uncoded collection-drop refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cdc_collection_drop_is_refused_by_code_mongo() {
    let s = CdcScenario::mongo_with("oc_trunc", |r, _| r);
    truncate_is_refused_by_code(s);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_cdc_truncate_is_refused_by_code_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let s = CdcScenario::oracle_with("oc_trunc", |r, _| r);
    truncate_is_refused_by_code(s);
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc postgres-cdc; open defect (uncoded dropped-slot refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_dropped_slot_after_a_changes_run_is_refused_by_code_postgres() {
    pg_dropped_slot_is_refused_by_code(true);
}

#[test]
#[ignore = "live+gate-only: docker compose --profile replica; open defect (uncoded binlog-gap refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_binlogs_purged_past_the_checkpoint_before_the_first_changes_run_are_refused_by_code_mysql()
 {
    let _serial = cross_process_serial("mysql_replica");
    mysql_binlogs_purged_past_the_checkpoint(false);
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_change_table_cleaned_past_the_checkpoint_before_the_first_changes_run_is_refused_by_code_mssql()
 {
    let _serial = cross_process_serial("mssql_cdc");
    mssql_cleanup_past_the_checkpoint(false);
}

#[test]
#[ignore = "live+gate-only: docker compose --profile replica; open defect (uncoded binlog-gap refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_binlogs_purged_past_the_checkpoint_after_a_changes_run_are_refused_by_code_mysql() {
    let _serial = cross_process_serial("mysql_replica");
    mysql_binlogs_purged_past_the_checkpoint(true);
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_change_table_cleaned_past_the_checkpoint_after_a_changes_run_is_refused_by_code_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    mssql_cleanup_past_the_checkpoint(true);
}

#[test]
#[ignore = "live: requires docker compose --profile replica (mysql-primary :3308 → mysql-replica-nolog :3310)"]
fn doctor_is_not_green_where_the_cdc_run_refuses_a_replica_that_does_not_relog_mysql() {
    use mysql::prelude::Queryable as _;
    let _serial = cross_process_serial("mysql_replica");
    let table = unique_name("oc_nolog");
    let _table = ReplicatedTable(table.clone());
    mysql::Conn::new(REPLICA_PRIMARY)
        .expect("connect mysql-primary :3308")
        .query_drop(format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    doctor_agrees_where_the_stream_is_refused(
        Rig::mysql_cdc(&table).source_url(REPLICA_NOLOG_RIVET),
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn doctor_is_not_green_where_the_cdc_run_refuses_a_table_with_no_capture_instance_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("oc_nocap");
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, v BIGINT)"
    ));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: format!("dbo_{table}"),
    };
    doctor_agrees_where_the_stream_is_refused(Rig::mssql_cdc(&table, &format!("dbo_{table}")));
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn doctor_is_not_green_where_the_cdc_run_refuses_a_standalone_mongo() {
    let (url, _m, _guard) = mongo_db("oc_standalone", 3);
    doctor_agrees_where_the_stream_is_refused(Rig::mongo_cdc("t").source_url(&url));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn doctor_is_not_green_where_the_cdc_run_refuses_a_table_without_all_column_logging_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = OracleTable::create("oc_nolog", "id NUMBER(18) PRIMARY KEY, v NUMBER(18)");
    ora_exec(&format!("GRANT SELECT ON {} TO c##rivetcdc", t.name()));
    doctor_agrees_where_the_stream_is_refused(Rig::oracle_cdc(t.name()));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn cdc_to_stdout_emits_each_change_once_postgres() {
    cdc_stdout_emits_each_change_once(CdcScenario::pg_with(
        "oc_ndjson",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.relative_checkpoint("cdc.ckpt"),
    ));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn cdc_to_stdout_emits_each_change_once_mysql() {
    cdc_stdout_emits_each_change_once(CdcScenario::mysql_with(
        "oc_ndjson",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r,
    ));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn cdc_to_stdout_emits_each_change_once_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    cdc_stdout_emits_each_change_once(CdcScenario::mssql_with(
        "oc_ndjson",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, t| r.repoint(&format!("dbo.{t}")),
    ));
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn cdc_to_stdout_emits_each_change_once_mongo() {
    cdc_stdout_emits_each_change_once(CdcScenario::mongo_with("oc_ndjson", |r, _| r));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn cdc_to_stdout_emits_each_change_once_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    cdc_stdout_emits_each_change_once(CdcScenario::oracle_with("oc_ndjson", |r, _| r));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_config_init_writes_runs_full_postgres() {
    init_batch_config_runs(SqlEngine::Pg, "full");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_config_init_writes_runs_full_mysql() {
    init_batch_config_runs(SqlEngine::Mysql, "full");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_config_init_writes_runs_full_mssql() {
    init_batch_config_runs(SqlEngine::Mssql, "full");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_config_init_writes_runs_full_oracle() {
    init_batch_config_runs(SqlEngine::Oracle, "full");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_config_init_writes_runs_incremental_postgres() {
    init_batch_config_runs(SqlEngine::Pg, "incremental");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_config_init_writes_runs_incremental_mysql() {
    init_batch_config_runs(SqlEngine::Mysql, "incremental");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_config_init_writes_runs_incremental_mssql() {
    init_batch_config_runs(SqlEngine::Mssql, "incremental");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_config_init_writes_runs_incremental_oracle() {
    init_batch_config_runs(SqlEngine::Oracle, "incremental");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_config_init_writes_runs_chunked_postgres() {
    init_batch_config_runs(SqlEngine::Pg, "chunked");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_config_init_writes_runs_chunked_mysql() {
    init_batch_config_runs(SqlEngine::Mysql, "chunked");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_config_init_writes_runs_chunked_mssql() {
    init_batch_config_runs(SqlEngine::Mssql, "chunked");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_config_init_writes_runs_chunked_oracle() {
    init_batch_config_runs(SqlEngine::Oracle, "chunked");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn the_config_init_writes_runs_full_mongo() {
    let (url, _m, _guard) = mongo_db("oc_init", 20);
    init_config_runs(&url, "t", "full");
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn the_config_init_writes_runs_cdc_postgres() {
    let mut s = CdcScenario::pg_with("oc_initc", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| r);
    s.insert(1);
    let cfg = init_config(POSTGRES_CDC_URL, &s.table, "cdc");
    let _slot = cfg.field("slot").map(Slot::new);
    generated_config_runs(&cfg);
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn the_config_init_writes_runs_cdc_mysql() {
    let mut s = CdcScenario::mysql_with("oc_initc", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| r);
    s.insert(1);
    init_config_runs(MYSQL_CDC_URL, &s.table, "cdc");
}

#[test]
#[ignore = "live+gate-only: docker compose mssql (CDC); open defect (init derives the capture instance), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_init_cdc_writes_the_capture_instance_the_catalog_holds_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("oc_initc");
    let ci = format!("ci_{table}");
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, v BIGINT)"
    ));
    let _table = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    enable_cdc(&table, &ci);
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, 1)"));
    wait_for_capture(&ci, 1);
    let cfg = init_config(MSSQL_CDC_URL, &format!("dbo.{table}"), "cdc");
    assert_eq!(
        cfg.field("capture_instance").as_deref(),
        Some(ci.as_str()),
        "`rivet init --mode cdc` wrote a capture instance cdc.change_tables does not hold for dbo.{table}"
    );
    generated_config_runs(&cfg);
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn the_config_init_writes_runs_cdc_mongo() {
    let db = unique_name("oc_initc");
    let _guard = MongoDbGuard {
        port: 27018,
        db: db.clone(),
    };
    MongoTest::connect(27018, &db).seed_int_id("t", 3);
    init_config_runs(&MongoTest::url(27018, &db), "t", "cdc");
}

#[test]
#[ignore = "live+gate-only: the cdc-standby pair (dev/pytools/cdc_stand)"]
fn the_config_init_writes_runs_cdc_on_a_standby_postgres() {
    let tbl = unique_name("oc_inits");
    let mut p = postgres::Client::connect(PG_STANDBY_PRIMARY_URL, postgres::NoTls)
        .expect("connect the cdc-standby primary");
    p.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO {tbl} VALUES (1, 1)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(PG_STANDBY_PRIMARY_URL, tbl.clone());
    std::thread::sleep(std::time::Duration::from_secs(2));
    let cfg = init_config(PG_STANDBY_URL, &tbl, "cdc");
    let _slot = cfg.field("slot").map(|s| Slot::on(PG_STANDBY_URL, s));
    generated_config_runs(&cfg);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn check_names_the_strategy_run_uses_for_chunked_with_no_column_postgres() {
    check_names_the_strategy_run_uses(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn check_names_the_strategy_run_uses_for_chunked_with_no_column_mysql() {
    check_names_the_strategy_run_uses(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn check_names_the_strategy_run_uses_for_chunked_with_no_column_mssql() {
    check_names_the_strategy_run_uses(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_names_the_strategy_run_uses_for_chunked_with_no_column_oracle() {
    check_names_the_strategy_run_uses(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: the cdc-standby pair (dev/pytools/cdc_stand)"]
fn a_bounded_cdc_run_refused_on_a_standby_leaves_nothing_and_its_remedy_runs_postgres() {
    standby_refusal_leaves_nothing_and_its_remedy_runs();
}

#[test]
#[ignore = "live+gate-only: docker (a throwaway mongo replica set); open defect (oplog gone), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_oplog_rolled_past_the_checkpoint_before_the_first_changes_run_is_refused_by_code_mongo()
 {
    mongo_oplog_rolled_past_the_checkpoint(false);
}

#[test]
#[ignore = "live+gate-only: docker (a throwaway mongo replica set); open defect (oplog gone), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_oplog_rolled_past_the_checkpoint_after_a_changes_run_is_refused_by_code_mongo() {
    mongo_oplog_rolled_past_the_checkpoint(true);
}
