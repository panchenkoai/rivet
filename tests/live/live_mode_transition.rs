//! Mode transitions (ADR-0033): what an incremental export inherits from the export's previous mode.

use crate::common::*;
use std::path::Path;

struct Stage(&'static str, &'static [&'static str]);

const FULL: Stage = Stage("full", &[]);
const TIME_WINDOW: Stage = Stage(
    "time_window",
    &["time_column: server_time", "days_window: 1"],
);
const RANGE_CHUNKED: Stage = Stage(
    "chunked",
    &[
        "chunk_column: id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
    ],
);
const KEYSET: Stage = Stage("chunked", &["chunk_by_key: id", "chunk_size: 4"]);
const KEYSET_CHECKPOINT: Stage = Stage(
    "chunked",
    &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
    ],
);
const PARALLEL_KEYSET: Stage = Stage(
    "chunked",
    &["chunk_by_key: id", "chunk_size: 4", "parallel: 2"],
);
const KEYSET_INCREMENTAL_ID: Stage = Stage(
    "chunked",
    &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
        "keyset_incremental: true",
    ],
);
const KEYSET_INCREMENTAL_EXT_ID: Stage = Stage(
    "chunked",
    &[
        "chunk_by_key: ext_id",
        "chunk_size: 4",
        "chunk_checkpoint: true",
        "keyset_incremental: true",
    ],
);
const PARALLEL_KEYSET_INCREMENTAL_ID: Stage = Stage(
    "chunked",
    &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "parallel: 2",
        "chunk_checkpoint: true",
        "keyset_incremental: true",
    ],
);
const PARALLEL_KEYSET_INCREMENTAL_EXT_ID: Stage = Stage(
    "chunked",
    &[
        "chunk_by_key: ext_id",
        "chunk_size: 4",
        "parallel: 2",
        "chunk_checkpoint: true",
        "keyset_incremental: true",
    ],
);
const INCREMENTAL_ID: Stage = Stage("incremental", &["cursor_column: id"]);
const INCREMENTAL_TIME: Stage = Stage("incremental", &["cursor_column: server_time"]);
const INCREMENTAL_TIME_SETTLED: Stage = Stage(
    "incremental",
    &["cursor_column: server_time", "settle: { after: 1h }"],
);
const INCREMENTAL_COALESCE: Stage = Stage(
    "incremental",
    &[
        "cursor_column: server_time",
        "cursor_fallback_column: updated_at",
        "incremental_cursor_mode: coalesce",
    ],
);

enum Expect {
    Continues,
    FullPass,
    Refused(&'static [&'static str]),
}

fn staged(rig: Rig, stage: &Stage, out: &Path) -> Rig {
    rig.restage(stage.0, stage.1).dest_path(out.to_path_buf())
}

fn transition(engine: SqlEngine, prior: Stage, next: Stage, expect: Expect) {
    transition_with(engine, prior, next, expect, |_| {});
}

/// Run `prior` over ids 1..=10, add ids 11..=13, restage the same rig to `next` and check its export.
fn transition_with(
    engine: SqlEngine,
    prior: Stage,
    next: Stage,
    expect: Expect,
    between: impl Fn(&Path),
) {
    engine.alive();
    let (table, _guard) = engine.table("mode_transition");
    engine.insert(&table, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let rig = staged(engine.rig(&table), &prior, first.path());
    rig.run_ok();
    assert_eq!(read_ids(first.path()), (1..=10).collect::<Vec<_>>());
    between(&rig.config_path().with_file_name(".rivet_state.db"));

    engine.insert(&table, 11..=13, 170, Some(10));
    let rig = staged(rig, &next, second.path());
    match expect {
        Expect::Continues => {
            let rig = continued(rig);
            rig.run_ok();
            assert_eq!(read_ids(second.path()), vec![11, 12, 13]);
        }
        Expect::FullPass => {
            rig.run_ok();
            assert_eq!(read_ids(second.path()), (1..=13).collect::<Vec<_>>());
        }
        Expect::Refused(names) => {
            let said = rig.run_expect_fail();
            for n in names {
                assert!(said.contains(n), "refusal must name {n}:\n{said}");
            }
            assert!(said.contains("state reset"), "{said}");
            assert!(read_ids(second.path()).is_empty(), "nothing exported");

            let reset = rig.cli(&["state", "reset", "--export", &table]);
            assert!(
                reset.status.success(),
                "{}",
                String::from_utf8_lossy(&reset.stderr)
            );
            rig.run_ok();
            assert_eq!(read_ids(second.path()), (1..=13).collect::<Vec<_>>());
        }
    }
}

/// A rig whose next run continues past rows the prior stage delivered to another destination.
fn continued(rig: Rig) -> Rig {
    rig.no_oracle(
        "the continued delta starts past rows the prior stage delivered to another destination",
    )
}

/// Export `_id` 1..=10 with `prior`, add 11..=13, switch to `page_size` + `resume`, check the first resumed run.
fn mongo_switch_to_resume(prior: Option<&str>, parallel: bool, expect: Expect) {
    require_alive(LiveService::Mongo);
    let db = unique_name("mt_mongo");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("t", 10);
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut rig = Rig::mongo_batch("t").source_url(&MongoTest::url(27017, &db));
    if let Some(opts) = prior {
        rig = rig.mongo(opts);
    }
    let lines: &[&str] = if parallel { &["parallel: 2"] } else { &[] };
    let rig = rig
        .restage("full", lines)
        .dest_path(first.path().to_path_buf());
    rig.run_ok();
    let ids = |dir: &Path| -> Vec<i64> {
        let mut v: Vec<i64> = dir_parquet_distinct_strings(dir, "_id")
            .iter()
            .map(|s| s.parse().expect("an integer _id"))
            .collect();
        v.sort();
        v
    };
    assert_eq!(ids(first.path()), (1..=10).collect::<Vec<_>>());

    for i in 11..=13 {
        m.upsert_set("t", i, "v", "new");
    }
    let rig = rig
        .mongo("page_size: 4, resume: true")
        .restage("full", &[])
        .dest_path(second.path().to_path_buf());
    match expect {
        Expect::Continues => {
            let rig = continued(rig);
            rig.run_ok();
            assert_eq!(ids(second.path()), vec![11, 12, 13]);
        }
        Expect::FullPass => {
            rig.run_ok();
            assert_eq!(ids(second.path()), (1..=13).collect::<Vec<_>>());
        }
        Expect::Refused(_) => unreachable!("no Mongo switch to resume is refused"),
    }
}

fn forget_cursor_column(state_db: &Path) {
    rusqlite::Connection::open(state_db)
        .unwrap()
        .execute("UPDATE export_state SET cursor_column = NULL", [])
        .unwrap();
}

fn full_then_incremental(e: SqlEngine) {
    transition(e, FULL, INCREMENTAL_ID, Expect::FullPass);
}

fn time_window_then_incremental(e: SqlEngine) {
    transition(e, TIME_WINDOW, INCREMENTAL_ID, Expect::FullPass);
}

fn range_chunked_then_incremental(e: SqlEngine) {
    transition(e, RANGE_CHUNKED, INCREMENTAL_ID, Expect::FullPass);
}

fn keyset_then_incremental_same_key(e: SqlEngine) {
    transition(e, KEYSET, INCREMENTAL_ID, Expect::Continues);
}

fn keyset_checkpoint_then_incremental_same_key(e: SqlEngine) {
    transition(e, KEYSET_CHECKPOINT, INCREMENTAL_ID, Expect::Continues);
}

fn parallel_keyset_then_incremental_same_key(e: SqlEngine) {
    transition(e, PARALLEL_KEYSET, INCREMENTAL_ID, Expect::Continues);
}

fn keyset_then_incremental_other_column(e: SqlEngine) {
    transition(
        e,
        KEYSET_CHECKPOINT,
        INCREMENTAL_TIME,
        Expect::Refused(&["`id`", "`server_time`"]),
    );
}

/// Both keyset runners refuse: the parallel one reads its anchor in its own call (`run_keyset_parallel`).
fn keyset_incremental_key_change(e: SqlEngine) {
    transition(
        e,
        KEYSET_INCREMENTAL_ID,
        KEYSET_INCREMENTAL_EXT_ID,
        Expect::Refused(&["`id`", "`ext_id`"]),
    );
    transition(
        e,
        PARALLEL_KEYSET_INCREMENTAL_ID,
        PARALLEL_KEYSET_INCREMENTAL_EXT_ID,
        Expect::Refused(&["`id`", "`ext_id`"]),
    );
}

fn incremental_then_keyset_incremental_same_key(e: SqlEngine) {
    transition(e, INCREMENTAL_ID, KEYSET_INCREMENTAL_ID, Expect::Continues);
}

fn incremental_cursor_change(e: SqlEngine) {
    transition(
        e,
        INCREMENTAL_ID,
        INCREMENTAL_TIME,
        Expect::Refused(&["`id`", "`server_time`"]),
    );
}

fn incremental_single_to_coalesce(e: SqlEngine) {
    transition(
        e,
        INCREMENTAL_TIME,
        INCREMENTAL_COALESCE,
        Expect::Refused(&["`server_time`", "coalesce(server_time,updated_at)"]),
    );
}

fn incremental_adding_settle(e: SqlEngine) {
    transition(
        e,
        INCREMENTAL_TIME,
        INCREMENTAL_TIME_SETTLED,
        Expect::Continues,
    );
}

/// MT6: a pre-v26 incremental cursor is attributed to the column the run's key
/// descriptor names; switching `cursor_column` without a reset is refused, never run
/// against the old column's value (exit 0, zero rows, on MySQL).
fn legacy_incremental_state_then_other_column(e: SqlEngine) {
    transition_with(
        e,
        INCREMENTAL_ID,
        INCREMENTAL_TIME,
        Expect::Refused(&["`id`", "`server_time`"]),
        forget_cursor_column,
    );
}

fn legacy_keyset_state_then_other_column(e: SqlEngine) {
    transition_with(
        e,
        KEYSET_CHECKPOINT,
        INCREMENTAL_TIME,
        Expect::Refused(&["`id`", "`server_time`"]),
        forget_cursor_column,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn full_then_incremental_mysql() {
    full_then_incremental(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_then_incremental_postgres() {
    full_then_incremental(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn full_then_incremental_mssql() {
    full_then_incremental(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn time_window_then_incremental_mysql() {
    time_window_then_incremental(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn time_window_then_incremental_postgres() {
    time_window_then_incremental(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn time_window_then_incremental_mssql() {
    time_window_then_incremental(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn range_chunked_then_incremental_mysql() {
    range_chunked_then_incremental(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn range_chunked_then_incremental_postgres() {
    range_chunked_then_incremental(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn range_chunked_then_incremental_mssql() {
    range_chunked_then_incremental(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_then_incremental_same_key_mysql() {
    keyset_then_incremental_same_key(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_then_incremental_same_key_postgres() {
    keyset_then_incremental_same_key(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_then_incremental_same_key_mssql() {
    keyset_then_incremental_same_key(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_checkpoint_then_incremental_same_key_mysql() {
    keyset_checkpoint_then_incremental_same_key(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_checkpoint_then_incremental_same_key_postgres() {
    keyset_checkpoint_then_incremental_same_key(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_checkpoint_then_incremental_same_key_mssql() {
    keyset_checkpoint_then_incremental_same_key(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn parallel_keyset_then_incremental_same_key_mysql() {
    parallel_keyset_then_incremental_same_key(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_keyset_then_incremental_same_key_postgres() {
    parallel_keyset_then_incremental_same_key(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn parallel_keyset_then_incremental_same_key_mssql() {
    parallel_keyset_then_incremental_same_key(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_then_incremental_other_column_mysql() {
    keyset_then_incremental_other_column(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_then_incremental_other_column_postgres() {
    keyset_then_incremental_other_column(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_then_incremental_other_column_mssql() {
    keyset_then_incremental_other_column(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_incremental_key_change_mysql() {
    keyset_incremental_key_change(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_incremental_key_change_postgres() {
    keyset_incremental_key_change(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_incremental_key_change_mssql() {
    keyset_incremental_key_change(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_then_keyset_incremental_same_key_mysql() {
    incremental_then_keyset_incremental_same_key(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_then_keyset_incremental_same_key_postgres() {
    incremental_then_keyset_incremental_same_key(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_then_keyset_incremental_same_key_mssql() {
    incremental_then_keyset_incremental_same_key(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_cursor_change_mysql() {
    incremental_cursor_change(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_cursor_change_postgres() {
    incremental_cursor_change(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_cursor_change_mssql() {
    incremental_cursor_change(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_single_to_coalesce_mysql() {
    incremental_single_to_coalesce(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_single_to_coalesce_postgres() {
    incremental_single_to_coalesce(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_single_to_coalesce_mssql() {
    incremental_single_to_coalesce(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_adding_settle_mysql() {
    incremental_adding_settle(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_adding_settle_postgres() {
    incremental_adding_settle(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_adding_settle_mssql() {
    incremental_adding_settle(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn legacy_keyset_state_then_other_column_mysql() {
    legacy_keyset_state_then_other_column(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn legacy_incremental_state_then_other_column_mysql() {
    legacy_incremental_state_then_other_column(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn legacy_incremental_state_then_other_column_postgres() {
    legacy_incremental_state_then_other_column(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn legacy_incremental_state_then_other_column_mssql() {
    legacy_incremental_state_then_other_column(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn legacy_keyset_state_then_other_column_postgres() {
    legacy_keyset_state_then_other_column(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn legacy_keyset_state_then_other_column_mssql() {
    legacy_keyset_state_then_other_column(SqlEngine::Mssql);
}

/// MT1 — a plain Mongo full scan stores no `_id`; the first `resume` run is a full pass.
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn full_then_resume_mongo() {
    mongo_switch_to_resume(None, false, Expect::FullPass);
}

/// MT2 — a `page_size` keyset records its final `_id`; `resume` continues past it.
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn keyset_then_resume_mongo() {
    mongo_switch_to_resume(Some("page_size: 4"), false, Expect::Continues);
}

/// The parallel `_id`-range reader records no `_id`; the first `resume` run is a full pass.
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn parallel_keyset_then_resume_mongo() {
    mongo_switch_to_resume(Some("page_size: 4"), true, Expect::FullPass);
}

/// Run after an identity change: true when it refused loudly, naming `state reset`.
fn refused(rig: &Rig) -> bool {
    let run = rig.run();
    if run.status.success() {
        return false;
    }
    let said = String::from_utf8_lossy(&run.stderr);
    assert!(
        said.contains("state reset"),
        "a refusal must name `state reset`:\n{said}"
    );
    true
}

/// P-06: a keyset run that crashed mid-way leaves a high-water an incremental run must not continue from.
fn crashed_keyset_then_incremental(e: SqlEngine) {
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let rig = staged(e.rig(&table), &KEYSET_CHECKPOINT, first.path());
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_keyset_page:0");
    assert!(!crash.status.success(), "the injected crash must stop it");

    let rig = staged(rig, &INCREMENTAL_ID, second.path());
    if !refused(&rig) {
        let ids = read_ids(second.path());
        assert!(
            ids == (1..=10).collect::<Vec<_>>(),
            "P-06: an incremental run continued from a crashed keyset run's high-water: it delivered ids {ids:?} of 1..=10 with exit 0"
        );
    }
}

/// P-22: a parallel keyset run resumed after a crash stores the highest key it delivered.
fn resumed_parallel_keyset_then_incremental(e: SqlEngine) {
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=300, 180, Some(10));
    e.insert(&table, 400..=400, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let parallel = Stage(
        "chunked",
        &[
            "chunk_by_key: id",
            "chunk_size: 4",
            "parallel: 4",
            "chunk_checkpoint: true",
        ],
    );
    let rig = staged(e.rig(&table), &parallel, first.path());
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:3");
    assert!(!crash.status.success(), "the injected crash must stop it");
    rig.run_ok();
    assert_eq!(
        read_ids(first.path()).len(),
        301,
        "the resumed run delivers every row"
    );

    let rig = continued(staged(rig, &INCREMENTAL_ID, second.path()));
    rig.run_ok();
    let again = read_ids(second.path());
    assert!(
        again.is_empty(),
        "P-22: a resumed parallel keyset run stored a cursor below its own maximum: incremental on the key re-delivered {} row(s) up to id {}",
        again.len(),
        again.last().unwrap_or(&0)
    );
}

/// An edited `query:` filter under the same FROM is another stream: refused, or its own rows in full.
fn incremental_query_filter_edited(e: SqlEngine) {
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=5, 180, Some(0));
    e.insert(&table, 6..=10, 180, Some(1));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let filtered =
        |spent: i32| format!("SELECT id, time_spent FROM {table} WHERE time_spent = {spent}");

    let rig = staged(
        e.rig(&table).query(&filtered(1)),
        &INCREMENTAL_ID,
        first.path(),
    );
    rig.run_ok();
    assert_eq!(read_ids(first.path()), (6..=10).collect::<Vec<_>>());

    let rig = staged(rig.query(&filtered(0)), &INCREMENTAL_ID, second.path());
    if !refused(&rig) {
        let ids = read_ids(second.path());
        assert!(
            ids == (1..=5).collect::<Vec<_>>(),
            "an edited query filter inherited the old filter's cursor: the run delivered ids {ids:?} of 1..=5 with exit 0"
        );
    }
}

struct PgSchema(String);

impl Drop for PgSchema {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
            let _ = c.batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.0));
        }
    }
}

#[test]
#[ignore = "live+gate-only: postgres; open defect P-02, acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_crashed_range_chunk_run_is_not_adopted_by_another_source_postgres() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=10, 180, Some(10));
    let other = pg_other_database_url();
    let _other_guard = pg_same_table_on(&other, &table, 30);
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let rig = staged(e.rig(&table), &RANGE_CHUNKED, first.path());
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_chunk_complete:0");
    assert!(!crash.status.success(), "the injected crash must stop it");

    let rig = rig
        .source_url(&other)
        .dest_path(second.path().to_path_buf());
    if !refused(&rig) {
        let ids = read_ids(second.path());
        assert!(
            ids == (1..=30).collect::<Vec<_>>(),
            "P-02: a same-named export of another source resumed a crashed chunk run: it delivered {} of 30 ids with exit 0",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live+gate-only: postgres; open defect P-06, acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_crashed_keyset_then_incremental_postgres() {
    crashed_keyset_then_incremental(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: postgres; open defect P-22, acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_resumed_parallel_keyset_then_incremental_postgres() {
    resumed_parallel_keyset_then_incremental(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: postgres; open defect P-18, acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_source_url_without_its_default_port_continues_postgres() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let rig = staged(e.rig(&table), &INCREMENTAL_ID, first.path());
    rig.run_ok();
    assert_eq!(read_ids(first.path()), (1..=10).collect::<Vec<_>>());

    e.insert(&table, 11..=13, 170, Some(10));
    let respelled = POSTGRES_URL.replace(":5432", "");
    assert_ne!(
        respelled, POSTGRES_URL,
        "the stand URL names the default port"
    );
    let rig = continued(staged(rig, &INCREMENTAL_ID, second.path()).source_url(&respelled));
    rig.run_ok();
    let ids = read_ids(second.path());
    assert!(
        ids == vec![11, 12, 13],
        "P-18: the source URL without its default port lost the cursor: the run delivered ids {ids:?}, the delta is [11, 12, 13]"
    );
}

#[test]
#[ignore = "live+gate-only: postgres; open defect (cursor identity, search_path), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_same_table_in_another_schema_is_another_stream_postgres() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=10, 180, Some(10));
    let schema = PgSchema(unique_name("mt_schema"));
    e.exec(&format!(
        "CREATE SCHEMA {s}; CREATE TABLE {s}.{table} (LIKE public.{table} INCLUDING ALL); \
         INSERT INTO {s}.{table} SELECT * FROM public.{table} WHERE id <= 7",
        s = schema.0
    ));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let rig = staged(e.rig(&table), &INCREMENTAL_ID, first.path());
    rig.run_ok();
    assert_eq!(read_ids(first.path()), (1..=10).collect::<Vec<_>>());

    let elsewhere = format!("{POSTGRES_URL}?options=-csearch_path%3D{}", schema.0);
    let rig = staged(rig, &INCREMENTAL_ID, second.path()).source_url(&elsewhere);
    if !refused(&rig) {
        let ids = read_ids(second.path());
        assert!(
            ids == (1..=7).collect::<Vec<_>>(),
            "the same table name in another schema inherited the cursor: the run delivered ids {ids:?} of 1..=7 with exit 0"
        );
    }
}

#[test]
#[ignore = "live+gate-only: postgres; open defect (cursor identity, edited query), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_incremental_query_filter_edited_postgres() {
    incremental_query_filter_edited(SqlEngine::Pg);
}
