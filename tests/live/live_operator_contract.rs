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
    engine.alive();
    let (id, v, int) = (engine.col("id"), engine.col("v"), engine.int64());
    let (table, guard) = engine.create(tag, &format!("{id} {int} PRIMARY KEY, {v} {int} NOT NULL"));
    insert_ids(engine, &table, 1..=n);
    (table, guard)
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

/// Whether `dir` holds a parquet part yet.
fn has_a_part(dir: &std::path::Path) -> bool {
    files_below(dir)
        .iter()
        .any(|p| p.extension().is_some_and(|e| e == "parquet"))
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

/// `rig` reading ten rows every `ms` milliseconds, so a run stays alive long enough to meet another.
fn slowed(rig: Rig, ms: u32) -> Rig {
    rig.source_line("tuning:")
        .source_line("  batch_size: 10")
        .source_line(&format!("  throttle_ms: {ms}"))
}

// (a) exit 0 over a wrong destination

/// RESULTS 1: a checkpoint left by a crashed run is not resumed after a later run finished the export.
fn stale_chunk_checkpoint(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_stale", ROWS);
    let mut rig = range_checkpoint_rig(engine, &table);
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_chunk_complete:2");
    assert!(
        !crash.status.success(),
        "fixture: the first run crashes after its third chunk"
    );
    rig.replace_export_line("chunk_checkpoint", "chunk_checkpoint: false");
    rig.run_ok();
    let v = engine.col("v");
    engine.exec(&format!("UPDATE {table} SET {v} = {v} + 1000"));
    rig.replace_export_line("chunk_checkpoint", "chunk_checkpoint: true");
    let out = rig.run();
    if out.status.success() {
        let stale: Vec<i64> = rig
            .read_declared_parts()
            .iter()
            .flat_map(|b| {
                ids_of(std::slice::from_ref(b))
                    .into_iter()
                    .zip(int_column(b, "v"))
            })
            .filter(|(_, v)| *v < 1000)
            .map(|(id, _)| id)
            .collect();
        assert!(
            stale.is_empty(),
            "a run resumed a checkpoint older than the last finished run and exited 0: {} of {ROWS} rows carry the value from before the update",
            stale.len()
        );
    }
}

/// One integer column of a batch, whatever its width.
fn int_column(b: &arrow::record_batch::RecordBatch, name: &str) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let col = b
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column {name}"));
    let col =
        arrow::compute::cast(col, &arrow::datatypes::DataType::Int64).expect("an integer column");
    let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// RESULTS 2: `run --validate` does not exit 0 when its own validation failed.
fn run_validate_flag(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_validate", ROWS);
    let rig = engine.rig(&table).export_line("verify: content");
    let out = rig.run_args(&["--validate"]);
    let said = text(&out);
    assert!(
        said.contains("validated:"),
        "fixture: `--validate` reports a verdict:\n{said}"
    );
    assert!(
        !(out.status.success() && said.contains("validated: FAIL")),
        "`run --validate` exited 0 with `validated: FAIL`\n{said}"
    );
}

/// Makes the SQLite state files beside a config read-only until dropped.
struct ReadOnlyState(Vec<std::path::PathBuf>);

impl ReadOnlyState {
    fn beside(cfg: &std::path::Path) -> Self {
        use std::os::unix::fs::PermissionsExt as _;
        let files: Vec<_> = files_below(cfg.parent().unwrap())
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with(".rivet_state.db"))
            })
            .collect();
        assert!(
            !files.is_empty(),
            "fixture: a SQLite state beside the config"
        );
        for f in &files {
            std::fs::set_permissions(f, std::fs::Permissions::from_mode(0o400)).unwrap();
        }
        ReadOnlyState(files)
    }
}

impl Drop for ReadOnlyState {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt as _;
        for f in &self.0 {
            let _ = std::fs::set_permissions(f, std::fs::Permissions::from_mode(0o600));
        }
    }
}

/// RESULTS 3: an incremental run that cannot store its cursor does not exit 0.
fn read_only_state(engine: SqlEngine) {
    if state_url_under_test().is_some() {
        return skip_live(
            "a read-only SQLite state file; this pass grades Postgres state (RIVET_GATE_STATE_URL)",
        );
    }
    let (table, _guard) = id_v_table(engine, "oc_rostate", 10);
    let rig = engine
        .rig(&table)
        .mode("incremental")
        .export_line("cursor_column: id");
    rig.run_ok();
    insert_ids(engine, &table, 11..=13);
    let locked = ReadOnlyState::beside(&rig.config_path());
    let out = rig.run();
    drop(locked);
    assert!(
        !out.status.success(),
        "an incremental run that could not advance its cursor (read-only state) exited 0\n{}",
        text(&out)
    );
}

/// RESULTS 4: a second `full` run beside a live one is refused, or the prefix still holds each key once.
fn concurrent_full_runs(engine: SqlEngine) {
    const N: i64 = 200;
    let (table, _guard) = id_v_table(engine, "oc_twofull", N);
    let rig = slowed(engine.rig(&table), 200);
    let mut first = rig.spawn_args_env(&[], &[]);
    std::thread::sleep(std::time::Duration::from_millis(1500));
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

/// RESULTS 5: `validate` on a bucket that does not exist does not exit 0.
fn validate_on_a_missing_bucket(rig: Rig, envs: &[(&str, &str)]) {
    let out = rig.cli_env(&["validate"], envs);
    assert!(
        !out.status.success(),
        "`validate` on a bucket that does not exist exited 0{}\n{}",
        if text(&out).contains("legacy_run") {
            " with `status: legacy_run`"
        } else {
            ""
        },
        text(&out)
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
    let (table, _guard) = id_v_table(engine, "oc_resume", ROWS);
    let mut rig = range_checkpoint_rig(engine, &table);
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

/// RESULTS 15: a PostgreSQL CDC run refused for a table that does not exist leaves no replication slot.
fn pg_cdc_missing_table_leaves_no_slot() {
    let slot = unique_name("oc_noslot");
    let _guard = Slot::new(slot.clone());
    let rig = Rig::pg_cdc(&unique_name("oc_absent"), &slot)
        .cdc("initial: snapshot")
        .oracle_known_defect(
            "a failed run left: chunk-checkpoint",
            "known defect: a PostgreSQL CDC run refused for a table that does not exist has already stored its export_state row and created its slot; the refusal must come before both",
        );
    let out = rig.run();
    assert!(!out.status.success(), "fixture: the table does not exist");
    let left: i64 = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls)
        .expect("connect postgres-cdc")
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
}

// (d) the exit-code contract

/// RESULTS 11: a second run beside a live checkpointed run is refused as `RIVET_STATE_RUN_IN_PROGRESS` (exit 5).
fn second_run_beside_a_live_checkpointed_run(engine: SqlEngine) {
    let (table, _guard) = id_v_table(engine, "oc_live", 300);
    let rig = slowed(
        engine
            .rig(&table)
            .mode("chunked")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 20")
            .export_line("chunk_checkpoint: true"),
        400,
    );
    let mut first = rig.spawn_args_env(&[], &[]);
    let t0 = std::time::Instant::now();
    while !has_a_part(&rig.out_dir()) {
        assert!(
            first.try_wait().unwrap().is_none(),
            "fixture: the run exited before it was seen mid-export"
        );
        assert!(
            t0.elapsed().as_secs() < 60,
            "fixture: the run never reached its first part"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let second = rig.run();
    assert!(
        first.try_wait().unwrap().is_none(),
        "fixture: the first run is alive when the second answers"
    );
    assert!(first.wait().unwrap().success(), "the live run finishes");
    let delivered: usize = read_all_parts(&rig.out_dir())
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(
        delivered, 300,
        "the live run delivers every row beside the refused one"
    );
    assert_refused(&second, Refused::by_code("RIVET_STATE_RUN_IN_PROGRESS", 5));
}

/// Mac report C: a TRUNCATE inside the stream is refused as `RIVET_SOURCE_CDC_TRUNCATED` (exit 5), every cycle.
fn truncate_is_refused_by_code(mut s: CdcScenario) {
    s.insert(1);
    s.rig.run_ok();
    s.sql(&format!("TRUNCATE TABLE {}", s.table));
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
fn pg_dropped_slot_is_refused_by_code() {
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

/// RESULTS 10: `plan` over a table that does not exist fails and seals no artifact.
fn plan_on_a_missing_table(engine: SqlEngine) {
    engine.alive();
    let rig = engine.rig(&unique_name("oc_absent"));
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
    let out = engine.rig(&table).run_with_env(
        "RIVET_STATE_URL",
        "postgre://rivet:rivet@127.0.0.1:5433/rivet_nope",
    );
    assert!(
        !out.status.success(),
        "`RIVET_STATE_URL=postgre://...` (a scheme rivet does not know) ran with exit 0 on a SQLite state beside the config"
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (stale chunk checkpoint adopted), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_stale_chunk_checkpoint_is_not_adopted_after_a_finished_run_postgres() {
    stale_chunk_checkpoint(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (stale chunk checkpoint adopted), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_stale_chunk_checkpoint_is_not_adopted_after_a_finished_run_mysql() {
    stale_chunk_checkpoint(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (stale chunk checkpoint adopted), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_stale_chunk_checkpoint_is_not_adopted_after_a_finished_run_mssql() {
    stale_chunk_checkpoint(SqlEngine::Mssql);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (run --validate exit), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_validate_does_not_exit_0_over_a_failed_validation_postgres() {
    run_validate_flag(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (run --validate exit), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_validate_does_not_exit_0_over_a_failed_validation_mysql() {
    run_validate_flag(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (run --validate exit), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_validate_does_not_exit_0_over_a_failed_validation_mssql() {
    run_validate_flag(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (run --validate exit), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_validate_does_not_exit_0_over_a_failed_validation_oracle() {
    run_validate_flag(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (read-only state), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_cannot_store_its_cursor_does_not_exit_0_postgres() {
    read_only_state(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (read-only state), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_cannot_store_its_cursor_does_not_exit_0_mysql() {
    read_only_state(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (read-only state), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_cannot_store_its_cursor_does_not_exit_0_mssql() {
    read_only_state(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (read-only state), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_that_cannot_store_its_cursor_does_not_exit_0_oracle() {
    read_only_state(SqlEngine::Oracle);
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
    second_run_beside_a_live_checkpointed_run(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_checkpointed_run_is_refused_by_code_mysql() {
    second_run_beside_a_live_checkpointed_run(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (uncoded run-in-progress refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_run_beside_a_live_checkpointed_run_is_refused_by_code_mssql() {
    second_run_beside_a_live_checkpointed_run(SqlEngine::Mssql);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (plan green over an unreadable source), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_plan_fails_over_a_table_that_does_not_exist_postgres() {
    plan_on_a_missing_table(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (plan green over an unreadable source), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_plan_fails_over_a_table_that_does_not_exist_mysql() {
    plan_on_a_missing_table(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (plan green over an unreadable source), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_plan_fails_over_a_table_that_does_not_exist_mssql() {
    plan_on_a_missing_table(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (plan green over an unreadable source), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_plan_fails_over_a_table_that_does_not_exist_oracle() {
    plan_on_a_missing_table(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (state URL typo falls back to SQLite), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unusable_state_url_is_refused_postgres() {
    unusable_state_url(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (state URL typo falls back to SQLite), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unusable_state_url_is_refused_mysql() {
    unusable_state_url(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (state URL typo falls back to SQLite), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unusable_state_url_is_refused_mssql() {
    unusable_state_url(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (state URL typo falls back to SQLite), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unusable_state_url_is_refused_oracle() {
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
    corrupt_checkpoint_remedy(CdcScenario::mssql_with(
        "oc_ckpt",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r.cdc("initial: snapshot"),
    ));
}

#[test]
#[ignore = "live+gate-only: docker compose mssql (CDC); open defect (uncoded corrupt-checkpoint refusal), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_corrupt_cdc_checkpoint_is_refused_by_code_mssql() {
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
    pg_dropped_slot_is_refused_by_code();
}

#[test]
#[ignore = "live+gate-only: docker compose --profile cdc postgres-cdc; open defect (slot left by a refused run), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_refused_cdc_run_on_a_missing_table_leaves_no_slot_postgres() {
    pg_cdc_missing_table_leaves_no_slot();
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (doctor green where run refuses), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_doctor_is_not_green_where_the_cdc_run_refuses_wal_level_postgres() {
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
#[ignore = "live+gate-only: minio; open defect (validate green on a missing bucket), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_missing_bucket_s3() {
    require_alive(LiveService::Minio);
    let rig = Rig::pg_batch("oc_nobucket").dest_s3(
        &unique_name("oc-nobucket").replace('_', "-"),
        "p",
        MINIO_ENDPOINT,
    );
    validate_on_a_missing_bucket(
        rig,
        &[
            ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
            ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ],
    );
}

#[test]
#[ignore = "live+gate-only: fake-gcs; open defect (validate green on a missing bucket), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_missing_bucket_gcs() {
    require_alive(LiveService::FakeGcs);
    validate_on_a_missing_bucket(
        Rig::pg_batch("oc_nobucket").dest_gcs(&unique_name("oc_nobucket"), "p", FAKE_GCS_ENDPOINT),
        &[],
    );
}

#[test]
#[ignore = "live+gate-only: azurite; open defect (validate green on a missing bucket), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_missing_bucket_azure() {
    require_alive(LiveService::Azurite);
    let rig =
        Rig::pg_batch("oc_nobucket").dest_azure(&unique_name("oc-nobucket").replace('_', "-"), "p");
    validate_on_a_missing_bucket(rig, &[("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)]);
}
