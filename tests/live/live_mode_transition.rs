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
const PARALLEL_KEYSET_CHECKPOINT: Stage = Stage(
    "chunked",
    &[
        "chunk_by_key: id",
        "chunk_size: 4",
        "parallel: 2",
        "chunk_checkpoint: true",
    ],
);
const RANGE_CHUNKED_PLAIN: Stage = Stage("chunked", &["chunk_column: id", "chunk_size: 4"]);
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
    let (table, _guard) = engine.range_table("mode_transition");
    engine.insert(&table, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let rig = staged_for(engine, engine.rig(&table), &prior, first.path());
    rig.run_ok();
    assert_eq!(
        delivered_ids(engine, first.path()),
        (1..=10).collect::<Vec<_>>()
    );
    between(&rig.config_path().with_file_name(".rivet_state.db"));

    engine.insert(&table, 11..=13, 170, Some(10));
    let rig = staged_for(engine, rig, &next, second.path());
    match expect {
        Expect::Continues => {
            let rig = continued(rig);
            rig.run_ok();
            assert_eq!(delivered_ids(engine, second.path()), vec![11, 12, 13]);
        }
        Expect::FullPass => {
            rig.run_ok();
            assert_eq!(
                delivered_ids(engine, second.path()),
                (1..=13).collect::<Vec<_>>()
            );
        }
        Expect::Refused(names) => {
            let said = rig.run_expect_fail();
            for n in names {
                let n = &catalog_names(engine, n);
                assert!(said.contains(n.as_str()), "refusal must name {n}:\n{said}");
            }
            assert!(said.contains("state reset"), "{said}");
            assert!(
                delivered_ids(engine, second.path()).is_empty(),
                "nothing exported"
            );

            let reset = rig.cli(&["state", "reset", "--export", &table]);
            assert!(
                reset.status.success(),
                "{}",
                String::from_utf8_lossy(&reset.stderr)
            );
            rig.run_ok();
            assert_eq!(
                delivered_ids(engine, second.path()),
                (1..=13).collect::<Vec<_>>()
            );
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

/// The stream refusal's remedy for an export whose query or source was edited.
const RESTORE: &str = "restore what it read to continue from the stored progress";

/// The stream refusal's reset remedy for `export`, applied through the CLI.
fn reset_remedy<'a>(export: &'a str) -> crate::common::Remedy<'a> {
    crate::common::Remedy::new(
        &format!("`rivet state reset -c <config> --export {export}` starts `"),
        Then::DeliversTheSource,
        move |r| {
            let reset = r.cli(&["state", "reset", "--export", export]);
            let said = String::from_utf8_lossy(&reset.stderr);
            assert!(reset.status.success(), "{said}");
        },
    )
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
    let rig = staged_for(e, e.rig(&table), &parallel, first.path());
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:3");
    assert!(!crash.status.success(), "the injected crash must stop it");
    rig.run_ok();
    assert_eq!(
        delivered_ids(e, first.path()).len(),
        301,
        "the resumed run delivers every row"
    );

    let rig = continued(staged_for(e, rig, &INCREMENTAL_ID, second.path()));
    rig.run_ok();
    let again = delivered_ids(e, second.path());
    assert!(
        again.is_empty(),
        "P-22: a resumed parallel keyset run stored a cursor below its own maximum: incremental on the key re-delivered {} row(s) up to id {}",
        again.len(),
        again.last().unwrap_or(&0)
    );
}

/// An edited `query:` filter under the same FROM is another stream: refused twice by code; with `walk`, the old filter restored continues, a reset delivers the new filter's rows in full, another destination changes nothing.
fn incremental_query_filter_edited(e: SqlEngine, walk: bool) {
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=5, 180, Some(0));
    e.insert(&table, 6..=10, 180, Some(1));
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (first, second, third) = (dirs[0].path(), dirs[1].path(), dirs[2].path());
    let filtered =
        |spent: i32| format!("SELECT id, time_spent FROM {table} WHERE time_spent = {spent}");

    let rig = staged_for(e, e.rig(&table).query(&filtered(1)), &INCREMENTAL_ID, first);
    rig.run_ok();
    assert_eq!(delivered_ids(e, first), (6..=10).collect::<Vec<_>>());

    let mut rig = staged_for(e, rig.query(&filtered(0)), &INCREMENTAL_ID, second);
    if !walk {
        let (stored, now) = ("where time_spent = 1)", "where time_spent = 0)");
        return refused_twice_for_the_stream(&rig, second, stored, now, &|o| delivered_ids(e, o));
    }
    let said = rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        STREAM_REFUSED,
        vec![
            reset_remedy(&table),
            crate::common::Remedy::wrong(
                "pointed the export at another destination",
                Then::Refuses(STREAM_REFUSED),
                |r| r.rebuilt(|r| r.dest_path(third.to_path_buf())),
            ),
            crate::common::Remedy::new(RESTORE, Then::DeliversTheSource, |r| {
                r.rebuilt(|r| staged_for(e, r.query(&filtered(1)), &INCREMENTAL_ID, first))
            }),
        ],
    );
    for want in [
        "where time_spent = 1)",
        "where time_spent = 0)",
        "cursor `10`",
    ] {
        assert!(said.contains(want), "the refusal names {want}:\n{said}");
    }
    assert!(
        delivered_ids(e, third).is_empty(),
        "a refused run writes nothing"
    );
    assert_eq!(delivered_ids(e, first), (6..=10).collect::<Vec<_>>());
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
#[ignore = "live: requires docker compose postgres"]
fn resumed_parallel_keyset_then_incremental_postgres() {
    resumed_parallel_keyset_then_incremental(SqlEngine::Pg);
}

/// P-18: the same source spelled without its default port keeps its cursor.
fn source_url_without_its_default_port_continues(e: SqlEngine) {
    e.alive();
    let (table, _guard) = e.table("mode_transition");
    e.insert(&table, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let rig = staged_for(e, e.rig(&table), &INCREMENTAL_ID, first.path());
    rig.run_ok();
    assert_eq!(delivered_ids(e, first.path()), (1..=10).collect::<Vec<_>>());

    e.insert(&table, 11..=13, 170, Some(10));
    let respelled = e.url().replace(&format!(":{}/", e.default_port()), "/");
    assert_ne!(respelled, e.url(), "the stand URL names the default port");
    let rig = continued(staged_for(e, rig, &INCREMENTAL_ID, second.path()).source_url(&respelled));
    rig.run_ok();
    let ids = delivered_ids(e, second.path());
    assert!(
        ids == vec![11, 12, 13],
        "P-18: the source URL without its default port lost the cursor: the run delivered ids {ids:?}, the delta is [11, 12, 13]"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn source_url_without_its_default_port_continues_postgres() {
    source_url_without_its_default_port_continues(SqlEngine::Pg);
}

/// The same table name read through another `search_path` is another stream: refused twice by code; the old source restored continues, a reset delivers the other schema's rows in full, another destination changes nothing.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn same_table_in_another_schema_is_another_stream_postgres() {
    same_table_in_another_schema(true);
}

/// The refusal alone, no remedy walked: graded in full whatever the state backend.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn same_table_in_another_schema_is_refused_twice_postgres() {
    same_table_in_another_schema(false);
}

/// The refusal alone, no remedy walked: graded in full whatever the state backend.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_query_filter_edited_is_refused_twice_postgres() {
    incremental_query_filter_edited(SqlEngine::Pg, false);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_query_filter_edited_is_refused_twice_mysql() {
    incremental_query_filter_edited(SqlEngine::Mysql, false);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_query_filter_edited_is_refused_twice_mssql() {
    incremental_query_filter_edited(SqlEngine::Mssql, false);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_query_filter_edited_is_refused_twice_oracle() {
    incremental_query_filter_edited(SqlEngine::Oracle, false);
}

/// The body of the two other-schema cells; `walk` adds the remedies.
fn same_table_in_another_schema(walk: bool) {
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
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (first, second, third) = (dirs[0].path(), dirs[1].path(), dirs[2].path());
    let rig = staged_for(e, e.rig(&table), &INCREMENTAL_ID, first);
    rig.run_ok();
    assert_eq!(read_ids(first), (1..=10).collect::<Vec<_>>());

    let elsewhere = format!("{POSTGRES_URL}?options=-csearch_path%3D{}", schema.0);
    let mut rig = staged_for(e, rig, &INCREMENTAL_ID, second).source_url(&elsewhere);
    if !walk {
        let stored = format!("`{table} under the server's own search_path`");
        let now = format!("`{table} under search_path {}`", schema.0);
        return refused_twice_for_the_stream(&rig, second, &stored, &now, &read_ids);
    }
    let said = rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        STREAM_REFUSED,
        vec![
            reset_remedy(&table),
            crate::common::Remedy::wrong(
                "pointed the export at another destination",
                Then::Refuses(STREAM_REFUSED),
                |r| r.rebuilt(|r| r.dest_path(third.to_path_buf())),
            ),
            crate::common::Remedy::new(RESTORE, Then::DeliversTheSource, |r| {
                r.rebuilt(|r| staged_for(e, r, &INCREMENTAL_ID, first).source_url(POSTGRES_URL))
            }),
        ],
    );
    for want in [
        format!("`{table} under the server's own search_path`"),
        format!("`{table} under search_path {}`", schema.0),
    ] {
        assert!(said.contains(&want), "the refusal names {want}:\n{said}");
    }
    assert!(read_ids(third).is_empty(), "a refused run writes nothing");
    assert_eq!(read_ids(first), (1..=10).collect::<Vec<_>>());
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_query_filter_edited_postgres() {
    incremental_query_filter_edited(SqlEngine::Pg, true);
}

const STREAM_CODE: &str = "RIVET_STATE_CURSOR_STREAM_MISMATCH";
const STREAM_REFUSED: Refused = Refused::by_code(STREAM_CODE, 5);
/// The stream refusal's remedy for two exports that share a name.
const OWN_NAMES: &str = "two exports sharing a name in one state database need their own names";

/// Two runs in a row refuse with the stream code (exit 5) naming both streams, and write nothing to `out`.
fn refused_twice_for_the_stream(
    rig: &Rig,
    out: &Path,
    stored: &str,
    now: &str,
    ids: &dyn Fn(&Path) -> Vec<i64>,
) {
    for cycle in 1..=2 {
        let o = rig.run();
        let said = String::from_utf8_lossy(&o.stderr).to_string();
        assert_eq!(
            o.status.code(),
            Some(5),
            "cycle {cycle}: a refusal, not a run:\n{said}"
        );
        for want in [STREAM_CODE, stored, now, "state reset"] {
            assert!(
                said.contains(want),
                "cycle {cycle}: refusal must name {want}:\n{said}"
            );
        }
        assert!(ids(out).is_empty(), "cycle {cycle}: nothing exported");
        assert!(!out.join("_SUCCESS").exists(), "cycle {cycle}: no _SUCCESS");
    }
}

/// `stage` on this rig, with key and cursor columns named as the engine's catalog holds them (Oracle folds to upper case).
fn staged_for(engine: SqlEngine, rig: Rig, stage: &Stage, out: &Path) -> Rig {
    engine
        .staged(rig, stage.0, stage.1)
        .dest_path(out.to_path_buf())
}

/// `text` with the fixture's column names spelled as the engine's catalog holds them.
fn catalog_names(engine: SqlEngine, text: &str) -> String {
    if !engine.folds_upper() {
        return text.to_string();
    }
    ["server_time", "updated_at", "ext_id", "`id`"]
        .iter()
        .fold(text.to_string(), |t, n| t.replace(n, &n.to_uppercase()))
}

/// Sorted ids re-read from every part under `out`, whatever case and integer type the engine gives the column.
fn delivered_ids(engine: SqlEngine, out: &Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let col = if engine.folds_upper() { "ID" } else { "id" };
    let mut v = Vec::new();
    for b in read_all_parts(out) {
        let ids = arrow::compute::cast(
            b.column_by_name(col).expect("the id column"),
            &arrow::datatypes::DataType::Int64,
        )
        .expect("an integer id");
        let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        v.extend((0..ids.len()).map(|i| ids.value(i)));
    }
    v.sort();
    v
}

/// Export table A (ids 101..=110) with `stage`, repoint the SAME export at table B (ids 1..=10) without a reset.
fn stream_repoint(engine: SqlEngine, stage: Stage) {
    engine.alive();
    let (a, _ga) = engine.table("stream_a");
    let (b, _gb) = engine.table("stream_b");
    engine.insert(&a, 101..=110, 170, Some(10));
    engine.insert(&b, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ids = |out: &Path| delivered_ids(engine, out);

    let rig = staged_for(engine, engine.rig(&a), &stage, first.path());
    rig.run_ok();
    assert_eq!(ids(first.path()), (101..=110).collect::<Vec<_>>());

    let mut rig = staged_for(engine, rig.repoint(&b), &stage, second.path());
    let own = unique_name("own");
    let said = rig.refuses_twice_then(
        &["run"],
        &[],
        STREAM_REFUSED,
        vec![
            crate::common::Remedy::new(
                &format!("`rivet state reset -c <config> --export {a}` starts `"),
                Then::DeliversTheSource,
                |r| {
                    let reset = r.cli(&["state", "reset", "--export", &a]);
                    let said = String::from_utf8_lossy(&reset.stderr);
                    assert!(reset.status.success(), "{said}");
                },
            ),
            crate::common::Remedy::new(OWN_NAMES, Then::DeliversTheSource, |r| {
                r.rebuilt(|r| r.export_named(&own))
            }),
        ],
    );
    assert!(said.contains(&a) && said.contains(&b), "{said}");
    assert_eq!(ids(second.path()), (1..=10).collect::<Vec<_>>());
}

/// Two configs, one export name, one state, two tables: the second is refused until it has its own name. A SQLite state sits beside its config, so there the second config takes the first one's directory.
fn stream_shared_name(engine: SqlEngine, stage: Stage) {
    engine.alive();
    let (a, _ga) = engine.table("stream_a");
    let (b, _gb) = engine.table("stream_b");
    engine.insert(&a, 101..=110, 170, Some(10));
    engine.insert(&b, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ids = |out: &Path| delivered_ids(engine, out);
    let shared = unique_name("shared");

    let one = staged_for(
        engine,
        engine.rig(&a).export_named(&shared),
        &stage,
        first.path(),
    );
    one.run_ok();
    assert_eq!(ids(first.path()), (101..=110).collect::<Vec<_>>());

    let two = if state_url_under_test().is_some() {
        engine.rig(&b).export_named(&shared)
    } else {
        one.repoint(&b)
    };
    let mut two = staged_for(engine, two, &stage, second.path());
    let own = unique_name("own");
    let said = two.refuses_twice_then(
        &["run"],
        &[],
        STREAM_REFUSED,
        vec![crate::common::Remedy::new(
            OWN_NAMES,
            Then::DeliversTheSource,
            |r| r.rebuilt(|r| r.export_named(&own)),
        )],
    );
    assert!(said.contains(&a) && said.contains(&b), "{said}");
    assert_eq!(ids(second.path()), (1..=10).collect::<Vec<_>>());
}

/// The `rivet init` shape (`query:`): a column added to the SELECT keeps the cursor; another FROM table is refused.
fn stream_query_repoint(engine: SqlEngine) {
    engine.alive();
    let (a, _ga) = engine.table("stream_a");
    let (b, _gb) = engine.table("stream_b");
    engine.insert(&a, 101..=110, 170, Some(10));
    engine.insert(&b, 1..=10, 180, Some(10));
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let ids = |out: &Path| delivered_ids(engine, out);
    let stage = INCREMENTAL_ID;

    let rig = engine
        .rig(&a)
        .query(&format!("SELECT id, ext_id, server_time FROM {a}"));
    let rig = staged_for(engine, rig, &stage, dirs[0].path());
    rig.run_ok();
    assert_eq!(ids(dirs[0].path()), (101..=110).collect::<Vec<_>>());

    engine.insert(&a, 111..=113, 160, Some(10));
    let wider = rig.query(&format!(
        "select id, ext_id, server_time, time_spent   from {a}"
    ));
    let wider = continued(staged_for(engine, wider, &stage, dirs[1].path()));
    wider.run_ok();
    assert_eq!(
        ids(dirs[1].path()),
        vec![111, 112, 113],
        "the cursor is kept"
    );

    let other = wider.query(&format!("SELECT id, ext_id, server_time FROM {b}"));
    let other = staged_for(engine, other, &stage, dirs[2].path());
    refused_twice_for_the_stream(&other, dirs[2].path(), &a, &b, &ids);
    let reset = other.cli(&["state", "reset", "--export", &a]);
    assert!(
        reset.status.success(),
        "{}",
        String::from_utf8_lossy(&reset.stderr)
    );
    other.run_ok();
    assert_eq!(ids(dirs[2].path()), (1..=10).collect::<Vec<_>>());
}

/// Mongo `resume`: export collection `hi` (`_id` 101..=110), then read `lo` (`_id` 1..=10) under the same export name and state.
fn mongo_stream(shared_name: bool) {
    require_alive(LiveService::Mongo);
    let db = unique_name("mt_stream");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("lo", 10);
    for i in 101..=110 {
        m.upsert_set("hi", i, "v", "row");
    }
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ids = |dir: &Path| -> Vec<i64> {
        let mut v: Vec<i64> = dir_parquet_distinct_strings(dir, "_id")
            .iter()
            .map(|s| s.parse().expect("an integer _id"))
            .collect();
        v.sort();
        v
    };
    let name = unique_name("hi");
    let rig_for = |coll: &str, out: &Path| {
        Rig::mongo_batch(coll)
            .export_named(&name)
            .source_url(&MongoTest::url(27017, &db))
            .mongo("page_size: 4, resume: true")
            .dest_path(out.to_path_buf())
    };
    let rig = rig_for("hi", first.path());
    rig.run_ok();
    assert_eq!(ids(first.path()), (101..=110).collect::<Vec<_>>());

    let rig = if shared_name && state_url_under_test().is_some() {
        rig_for("lo", second.path())
    } else {
        rig.repoint("lo").dest_path(second.path().to_path_buf())
    };
    refused_twice_for_the_stream(&rig, second.path(), "hi", "lo", &ids);

    let rig = if shared_name {
        rig.export_named(&unique_name("own"))
    } else {
        let reset = rig.cli(&["state", "reset", "--export", &name]);
        assert!(
            reset.status.success(),
            "{}",
            String::from_utf8_lossy(&reset.stderr)
        );
        rig
    };
    rig.run_ok();
    assert_eq!(ids(second.path()), (1..=10).collect::<Vec<_>>());
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn resume_repointed_at_another_collection_mongo() {
    mongo_stream(false);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn resume_shared_name_another_collection_mongo() {
    mongo_stream(true);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_repointed_at_another_table_mysql() {
    stream_repoint(SqlEngine::Mysql, INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_repointed_at_another_table_postgres() {
    stream_repoint(SqlEngine::Pg, INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_repointed_at_another_table_mssql() {
    stream_repoint(SqlEngine::Mssql, INCREMENTAL_ID);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_repointed_at_another_table_oracle() {
    stream_repoint(SqlEngine::Oracle, INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn keyset_incremental_repointed_at_another_table_mysql() {
    stream_repoint(SqlEngine::Mysql, KEYSET_INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_incremental_repointed_at_another_table_postgres() {
    stream_repoint(SqlEngine::Pg, KEYSET_INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn keyset_incremental_repointed_at_another_table_mssql() {
    stream_repoint(SqlEngine::Mssql, KEYSET_INCREMENTAL_ID);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_incremental_repointed_at_another_table_oracle() {
    stream_repoint(SqlEngine::Oracle, KEYSET_INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn parallel_keyset_incremental_repointed_at_another_table_mysql() {
    stream_repoint(SqlEngine::Mysql, PARALLEL_KEYSET_INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_keyset_incremental_repointed_at_another_table_postgres() {
    stream_repoint(SqlEngine::Pg, PARALLEL_KEYSET_INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn parallel_keyset_incremental_repointed_at_another_table_mssql() {
    stream_repoint(SqlEngine::Mssql, PARALLEL_KEYSET_INCREMENTAL_ID);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn parallel_keyset_incremental_repointed_at_another_table_oracle() {
    stream_repoint(SqlEngine::Oracle, PARALLEL_KEYSET_INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_shared_name_another_table_mysql() {
    stream_shared_name(SqlEngine::Mysql, INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_shared_name_another_table_postgres() {
    stream_shared_name(SqlEngine::Pg, INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_shared_name_another_table_mssql() {
    stream_shared_name(SqlEngine::Mssql, INCREMENTAL_ID);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_shared_name_another_table_oracle() {
    stream_shared_name(SqlEngine::Oracle, INCREMENTAL_ID);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_query_repointed_at_another_table_mysql() {
    stream_query_repoint(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn incremental_query_repointed_at_another_table_postgres() {
    stream_query_repoint(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_query_repointed_at_another_table_mssql() {
    stream_query_repoint(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_query_repointed_at_another_table_oracle() {
    stream_query_repoint(SqlEngine::Oracle);
}

const INTERRUPTED_CODE: &str = "RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH";

enum Remedy {
    FinishTheRun,
    Reset,
}

/// P-06: a checkpointed run of `prior` crashes at `crash_at` with part of ids 1..=10 delivered, then the export becomes `mode: incremental` with no reset. `abandon` is the state subcommand the refusal names.
fn crashed_run_then_incremental(
    engine: SqlEngine,
    prior: Stage,
    crash_at: &str,
    abandon: &str,
    remedy: Remedy,
) {
    engine.alive();
    let (table, _guard) = engine.range_table("crashed_run");
    engine.insert(&table, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ids = |out: &Path| delivered_ids(engine, out);
    let source: Vec<i64> = (1..=10).collect();
    let keeps_a_high_water = prior.1.iter().any(|l| l.starts_with("chunk_by_key"));

    let run = staged_for(engine, engine.rig(&table), &prior, first.path());
    let crash = run.run_with_env("RIVET_TEST_PANIC_AT", crash_at);
    assert!(!crash.status.success(), "the first run must crash");
    assert_eq!(
        ids(first.path()),
        vec![1, 2, 3, 4],
        "ids 1..=4 landed first:
{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    let incremental = staged_for(engine, run, &INCREMENTAL_ID, second.path());
    for cycle in 1..=2 {
        let o = incremental.run();
        let said = String::from_utf8_lossy(&o.stderr).to_string();
        let got = ids(second.path());
        assert!(
            got.is_empty(),
            "cycle {cycle}: the incremental run delivered {} of {} source ids ({got:?}) past an \
             unfinished run, exit {:?}",
            got.len(),
            source.len(),
            o.status.code()
        );
        assert_eq!(o.status.code(), Some(5), "cycle {cycle}:\n{said}");
        let command = format!("rivet state {abandon} -c <config> --export {table}");
        for want in [
            INTERRUPTED_CODE,
            "of mode `",
            "runs as `incremental`",
            &command,
        ] {
            assert!(
                said.contains(want),
                "cycle {cycle}: must name {want}:\n{said}"
            );
        }
        assert!(!second.path().join("_SUCCESS").exists(), "cycle {cycle}");
    }

    match remedy {
        Remedy::FinishTheRun => {
            let run = staged_for(engine, incremental, &prior, first.path());
            run.run_ok();
            assert_eq!(ids(first.path()), source, "the interrupted run finished");
            engine.insert(&table, 11..=13, 170, Some(10));
            let incremental = staged_for(engine, run, &INCREMENTAL_ID, second.path());
            if keeps_a_high_water {
                continued(incremental).run_ok();
                assert_eq!(ids(second.path()), vec![11, 12, 13], "MT2");
            } else {
                incremental.run_ok();
                assert_eq!(ids(second.path()), (1..=13).collect::<Vec<_>>(), "MT1");
            }
        }
        Remedy::Reset => {
            let reset = incremental.cli(&["state", abandon, "--export", &table]);
            assert!(
                reset.status.success(),
                "{}",
                String::from_utf8_lossy(&reset.stderr)
            );
            incremental.run_ok();
            assert_eq!(ids(second.path()), source, "a full pass after the reset");
        }
    }
}

fn crashed_keyset_then_incremental(engine: SqlEngine, remedy: Remedy) {
    crashed_run_then_incremental(
        engine,
        KEYSET_CHECKPOINT,
        "after_keyset_page:0",
        "reset",
        remedy,
    );
}

fn crashed_range_chunk_then_incremental(engine: SqlEngine, remedy: Remedy) {
    crashed_run_then_incremental(
        engine,
        RANGE_CHUNKED,
        "after_chunk_complete:0",
        "reset-chunks",
        remedy,
    );
}

/// Sorted `(id, time_spent)` of every part under `out`, whatever the engine's integer widths and name case.
fn delivered_spent(engine: SqlEngine, out: &Path) -> Vec<(i64, i64)> {
    use arrow::array::{Array, Int64Array};
    let name = |col: &str| match engine.folds_upper() {
        true => col.to_uppercase(),
        false => col.to_string(),
    };
    let mut rows = Vec::new();
    for b in read_all_parts(out) {
        let ints = |col: &str| -> Vec<i64> {
            let cast = arrow::compute::cast(
                b.column_by_name(&name(col)).expect("the column"),
                &arrow::datatypes::DataType::Int64,
            )
            .expect("an integer column");
            let a = cast.as_any().downcast_ref::<Int64Array>().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect()
        };
        rows.extend(ints("id").into_iter().zip(ints("time_spent")));
    }
    rows.sort();
    rows
}

/// The standard table with a key range chunking accepts on every engine (Oracle refuses `NUMBER(19)`).
fn range_chunkable_table(engine: SqlEngine, prefix: &str) -> (String, Box<dyn std::any::Any>) {
    #[cfg(feature = "oracle")]
    if let SqlEngine::Oracle = engine {
        return engine.create(
            prefix,
            "id NUMBER(18) PRIMARY KEY, ext_id NUMBER(18) NOT NULL UNIQUE, \
             server_time TIMESTAMP(6) NOT NULL, updated_at TIMESTAMP(6) NULL, time_spent INT NULL",
        );
    }
    engine.table(prefix)
}

/// One checkpointed runner shape: its stage, the same stage with the checkpoint removed, where its first run crashes, and the state subcommand that abandons it.
struct Checkpointed {
    prior: Stage,
    plain: Stage,
    crash_at: &'static str,
    abandon: &'static str,
}

const RANGE_CHUNK_RUN: Checkpointed = Checkpointed {
    prior: RANGE_CHUNKED,
    plain: RANGE_CHUNKED_PLAIN,
    crash_at: "after_chunk_complete:0",
    abandon: "reset-chunks",
};
const KEYSET_RUN: Checkpointed = Checkpointed {
    prior: KEYSET_CHECKPOINT,
    plain: KEYSET,
    crash_at: "after_keyset_page:0",
    abandon: "reset",
};
const PARALLEL_KEYSET_RUN: Checkpointed = Checkpointed {
    prior: PARALLEL_KEYSET_CHECKPOINT,
    plain: PARALLEL_KEYSET,
    crash_at: "keyset_parallel_range_committed:0",
    abandon: "reset",
};

/// MT10: a checkpointed run crashes with part of ids 1..=10 delivered, then the export drops its checkpoint with no reset: refused twice, nothing written. After `remedy` the uncheckpointed run delivers the source as it is, and the checkpoint put back starts a fresh run over rows changed since.
fn crashed_run_then_checkpoint_removed(engine: SqlEngine, shape: Checkpointed, remedy: Remedy) {
    engine.alive();
    let (table, _guard) = range_chunkable_table(engine, "ckpt_removed");
    engine.insert(&table, 1..=10, 180, Some(10));
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let (crashed, plain_out, fresh) = (dirs[0].path(), dirs[1].path(), dirs[2].path());
    let ids = |out: &Path| delivered_ids(engine, out);
    let source: Vec<i64> = (1..=10).collect();

    let run = staged_for(engine, engine.rig(&table), &shape.prior, crashed);
    let crash = run.run_with_env("RIVET_TEST_PANIC_AT", shape.crash_at);
    assert!(!crash.status.success(), "the first run must crash");
    assert!(
        !ids(crashed).is_empty(),
        "the crashed run delivered a part first:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    let plain = staged_for(engine, run, &shape.plain, plain_out);
    for cycle in 1..=2 {
        let o = plain.run();
        let said = String::from_utf8_lossy(&o.stderr).to_string();
        let got = ids(plain_out);
        assert!(
            got.is_empty(),
            "cycle {cycle}: the run without the checkpoint delivered {} of {} source ids \
             ({got:?}) beside an unfinished run, exit {:?}",
            got.len(),
            source.len(),
            o.status.code()
        );
        assert_eq!(o.status.code(), Some(5), "cycle {cycle}:\n{said}");
        let command = format!("rivet state {} -c <config> --export {table}", shape.abandon);
        for want in [
            INTERRUPTED_CODE,
            "of mode `",
            "runs without the checkpoint that run was opened with",
            "restore the checkpoint setting (`chunk_checkpoint: true`",
            &command,
        ] {
            assert!(
                said.contains(want),
                "cycle {cycle}: must name {want}:\n{said}"
            );
        }
        assert!(!plain_out.join("_SUCCESS").exists(), "cycle {cycle}");
    }

    let plain = match remedy {
        Remedy::FinishTheRun => {
            let run = staged_for(engine, plain, &shape.prior, crashed);
            run.run_ok();
            assert_eq!(ids(crashed), source, "the interrupted run finished");
            staged_for(engine, run, &shape.plain, plain_out)
        }
        Remedy::Reset => {
            let reset = plain.cli(&["state", shape.abandon, "--export", &table]);
            assert!(
                reset.status.success(),
                "{}",
                String::from_utf8_lossy(&reset.stderr)
            );
            plain
        }
    };
    let change = |ids: &str, to: i64| {
        engine.exec(&format!(
            "UPDATE {table} SET time_spent = {to} WHERE id IN ({ids})"
        ))
    };
    let expect = |changed: &[(i64, i64)]| -> Vec<(i64, i64)> {
        let to = |id: i64| changed.iter().find(|c| c.0 == id).map_or(10, |c| c.1);
        (1..=10).map(|id| (id, to(id))).collect()
    };
    change("1, 5, 9", 99);
    plain.run_ok();
    assert_eq!(
        delivered_spent(engine, plain_out),
        expect(&[(1, 99), (5, 99), (9, 99)]),
        "the run without the checkpoint, once the unfinished run is settled"
    );

    change("2, 6, 10", 77);
    let again = staged_for(engine, plain, &shape.prior, fresh);
    let said = again.run_ok_capture();
    assert!(
        !said.contains("resuming it"),
        "no run is left to resume:\n{said}"
    );
    assert_eq!(
        delivered_spent(engine, fresh),
        expect(&[(1, 99), (5, 99), (9, 99), (2, 77), (6, 77), (10, 77)]),
        "the checkpoint back on reads the source as it is now"
    );
}

/// MT10 on MongoDB: a `resume: true` run crashes after its first page, then `resume` is removed.
fn crashed_resume_then_resume_removed_mongo(remedy: Remedy) {
    require_alive(LiveService::Mongo);
    let db = unique_name("mt_resume_off");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("t", 10);
    let (crashed, plain_out) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let strings = |dir: &Path, col: &str| dir_parquet_distinct_strings(dir, col);
    let ids = |dir: &Path| -> Vec<i64> {
        let mut v: Vec<i64> = strings(dir, "_id")
            .iter()
            .map(|s| s.parse().expect("an integer _id"))
            .collect();
        v.sort();
        v
    };
    let source: Vec<i64> = (1..=10).collect();
    let resumable = |rig: Rig| {
        rig.mongo("page_size: 4, resume: true")
            .restage("full", &[])
            .dest_path(crashed.path().to_path_buf())
    };
    let unresumable = |rig: Rig| {
        rig.mongo("page_size: 4")
            .restage("full", &[])
            .dest_path(plain_out.path().to_path_buf())
    };

    let run = resumable(
        Rig::mongo_batch("t")
            .source_url(&MongoTest::url(27017, &db))
            .export_named(&db),
    );
    let crash = run.run_with_env("RIVET_TEST_PANIC_AT", "after_keyset_page:0");
    assert!(!crash.status.success(), "the first run must crash");
    assert_eq!(ids(crashed.path()), vec![1, 2, 3, 4], "page 0 landed first");

    let plain = unresumable(run);
    for cycle in 1..=2 {
        let o = plain.run();
        let said = String::from_utf8_lossy(&o.stderr).to_string();
        let got = ids(plain_out.path());
        assert!(
            got.is_empty(),
            "cycle {cycle}: the run without `resume` delivered {} of {} source ids ({got:?}) \
             beside an unfinished run, exit {:?}",
            got.len(),
            source.len(),
            o.status.code()
        );
        assert_eq!(o.status.code(), Some(5), "cycle {cycle}:\n{said}");
        for want in [
            INTERRUPTED_CODE,
            "of mode `keyset`",
            "MongoDB's `source.mongo.resume: true`",
            &format!("rivet state reset -c <config> --export {db}"),
        ] {
            assert!(
                said.contains(want),
                "cycle {cycle}: must name {want}:\n{said}"
            );
        }
    }

    let plain = match remedy {
        Remedy::FinishTheRun => {
            let run = resumable(plain);
            run.run_ok();
            assert_eq!(ids(crashed.path()), source, "the interrupted run finished");
            unresumable(run)
        }
        Remedy::Reset => {
            let reset = plain.cli(&["state", "reset", "--export", &db]);
            assert!(
                reset.status.success(),
                "{}",
                String::from_utf8_lossy(&reset.stderr)
            );
            plain
        }
    };
    for id in [1, 5, 9] {
        m.upsert_set("t", id, "v", "changed");
    }
    plain.run_ok();
    assert_eq!(ids(plain_out.path()), source);
    let changed = strings(plain_out.path(), "document")
        .iter()
        .filter(|d| d.contains("changed"))
        .count();
    assert_eq!(
        changed, 3,
        "the run without `resume` reads the source as it is now"
    );
}

/// P-02: config A crashes after chunk 0; config B (same export name, table name and chunk settings, ANOTHER database) must deliver its own rows, twice; A then resumes its own run.
fn range_chunk_shared_name_another_source(engine: SqlEngine) {
    engine.alive();
    let other = engine.second_database("chunk_other");
    let (table, _guard) = engine.range_table("chunk_shared");
    engine.insert(&table, 1..=10, 180, Some(10));
    other.table_with(&table, 1..=30);
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let ids = |out: &Path| delivered_ids(engine, out);

    let a = staged_for(engine, engine.rig(&table), &RANGE_CHUNKED, dirs[0].path());
    let crash = a.run_with_env("RIVET_TEST_PANIC_AT", "after_chunk_complete:0");
    assert!(!crash.status.success(), "config A must crash");
    let partial = ids(dirs[0].path());
    assert!(
        !partial.is_empty() && partial.len() < 10,
        "A crashed mid-run: {partial:?}"
    );

    let mut b = a.source_url(&other.url());
    for (cycle, out) in [(1, dirs[1].path()), (2, dirs[2].path())] {
        b = staged_for(engine, b, &RANGE_CHUNKED, out);
        let o = b.run();
        let got = ids(out);
        assert_eq!(
            got,
            (1..=30).collect::<Vec<_>>(),
            "cycle {cycle}: B delivered {} of its 30 source ids, exit {:?}:\n{}",
            got.len(),
            o.status.code(),
            String::from_utf8_lossy(&o.stderr)
        );
        assert!(o.status.success(), "cycle {cycle}");
    }

    let a = staged_for(
        engine,
        b.source_url(engine.url()),
        &RANGE_CHUNKED,
        dirs[0].path(),
    );
    let said = a.run_ok_capture();
    assert!(
        said.contains("resuming it"),
        "A resumes its own run:\n{said}"
    );
    assert_eq!(ids(dirs[0].path()), (1..=10).collect::<Vec<_>>());
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_incremental_finish_the_run_postgres() {
    crashed_keyset_then_incremental(SqlEngine::Pg, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_incremental_reset_postgres() {
    crashed_keyset_then_incremental(SqlEngine::Pg, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_incremental_finish_the_run_mysql() {
    crashed_keyset_then_incremental(SqlEngine::Mysql, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_incremental_reset_mysql() {
    crashed_keyset_then_incremental(SqlEngine::Mysql, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_incremental_finish_the_run_mssql() {
    crashed_keyset_then_incremental(SqlEngine::Mssql, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_incremental_reset_mssql() {
    crashed_keyset_then_incremental(SqlEngine::Mssql, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_incremental_finish_the_run_oracle() {
    crashed_keyset_then_incremental(SqlEngine::Oracle, Remedy::FinishTheRun);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_incremental_reset_oracle() {
    crashed_keyset_then_incremental(SqlEngine::Oracle, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn range_chunk_shared_name_another_source_postgres() {
    range_chunk_shared_name_another_source(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn range_chunk_shared_name_another_source_mysql() {
    range_chunk_shared_name_another_source(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn range_chunk_shared_name_another_source_mssql() {
    range_chunk_shared_name_another_source(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunk_then_incremental_finish_the_run_postgres() {
    crashed_range_chunk_then_incremental(SqlEngine::Pg, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunk_then_incremental_reset_postgres() {
    crashed_range_chunk_then_incremental(SqlEngine::Pg, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunk_then_incremental_finish_the_run_mysql() {
    crashed_range_chunk_then_incremental(SqlEngine::Mysql, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunk_then_incremental_reset_mysql() {
    crashed_range_chunk_then_incremental(SqlEngine::Mysql, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunk_then_incremental_finish_the_run_mssql() {
    crashed_range_chunk_then_incremental(SqlEngine::Mssql, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunk_then_incremental_reset_mssql() {
    crashed_range_chunk_then_incremental(SqlEngine::Mssql, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunk_then_checkpoint_removed_finish_the_run_postgres() {
    crashed_run_then_checkpoint_removed(SqlEngine::Pg, RANGE_CHUNK_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunk_then_checkpoint_removed_finish_the_run_mysql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mysql, RANGE_CHUNK_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunk_then_checkpoint_removed_finish_the_run_mssql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mssql, RANGE_CHUNK_RUN, Remedy::FinishTheRun);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunk_then_checkpoint_removed_finish_the_run_oracle() {
    crashed_run_then_checkpoint_removed(SqlEngine::Oracle, RANGE_CHUNK_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_range_chunk_then_checkpoint_removed_reset_postgres() {
    crashed_run_then_checkpoint_removed(SqlEngine::Pg, RANGE_CHUNK_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_range_chunk_then_checkpoint_removed_reset_mysql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mysql, RANGE_CHUNK_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_range_chunk_then_checkpoint_removed_reset_mssql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mssql, RANGE_CHUNK_RUN, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunk_then_checkpoint_removed_reset_oracle() {
    crashed_run_then_checkpoint_removed(SqlEngine::Oracle, RANGE_CHUNK_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_checkpoint_removed_finish_the_run_postgres() {
    crashed_run_then_checkpoint_removed(SqlEngine::Pg, KEYSET_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_checkpoint_removed_finish_the_run_mysql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mysql, KEYSET_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_checkpoint_removed_finish_the_run_mssql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mssql, KEYSET_RUN, Remedy::FinishTheRun);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_checkpoint_removed_finish_the_run_oracle() {
    crashed_run_then_checkpoint_removed(SqlEngine::Oracle, KEYSET_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_keyset_then_checkpoint_removed_reset_postgres() {
    crashed_run_then_checkpoint_removed(SqlEngine::Pg, KEYSET_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_keyset_then_checkpoint_removed_reset_mysql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mysql, KEYSET_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_keyset_then_checkpoint_removed_reset_mssql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mssql, KEYSET_RUN, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_keyset_then_checkpoint_removed_reset_oracle() {
    crashed_run_then_checkpoint_removed(SqlEngine::Oracle, KEYSET_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_checkpoint_removed_finish_the_run_postgres() {
    crashed_run_then_checkpoint_removed(SqlEngine::Pg, PARALLEL_KEYSET_RUN, Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_parallel_keyset_then_checkpoint_removed_finish_the_run_mysql() {
    crashed_run_then_checkpoint_removed(
        SqlEngine::Mysql,
        PARALLEL_KEYSET_RUN,
        Remedy::FinishTheRun,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_parallel_keyset_then_checkpoint_removed_finish_the_run_mssql() {
    crashed_run_then_checkpoint_removed(
        SqlEngine::Mssql,
        PARALLEL_KEYSET_RUN,
        Remedy::FinishTheRun,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_parallel_keyset_then_checkpoint_removed_finish_the_run_oracle() {
    crashed_run_then_checkpoint_removed(
        SqlEngine::Oracle,
        PARALLEL_KEYSET_RUN,
        Remedy::FinishTheRun,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crashed_parallel_keyset_then_checkpoint_removed_reset_postgres() {
    crashed_run_then_checkpoint_removed(SqlEngine::Pg, PARALLEL_KEYSET_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn crashed_parallel_keyset_then_checkpoint_removed_reset_mysql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mysql, PARALLEL_KEYSET_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn crashed_parallel_keyset_then_checkpoint_removed_reset_mssql() {
    crashed_run_then_checkpoint_removed(SqlEngine::Mssql, PARALLEL_KEYSET_RUN, Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_parallel_keyset_then_checkpoint_removed_reset_oracle() {
    crashed_run_then_checkpoint_removed(SqlEngine::Oracle, PARALLEL_KEYSET_RUN, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn crashed_resume_then_resume_removed_finish_the_run_mongo() {
    crashed_resume_then_resume_removed_mongo(Remedy::FinishTheRun);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn crashed_resume_then_resume_removed_reset_mongo() {
    crashed_resume_then_resume_removed_mongo(Remedy::Reset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn full_then_incremental_oracle() {
    full_then_incremental(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn time_window_then_incremental_oracle() {
    time_window_then_incremental(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn range_chunked_then_incremental_oracle() {
    range_chunked_then_incremental(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_then_incremental_same_key_oracle() {
    keyset_then_incremental_same_key(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_checkpoint_then_incremental_same_key_oracle() {
    keyset_checkpoint_then_incremental_same_key(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn parallel_keyset_then_incremental_same_key_oracle() {
    parallel_keyset_then_incremental_same_key(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_then_incremental_other_column_oracle() {
    keyset_then_incremental_other_column(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_incremental_key_change_oracle() {
    keyset_incremental_key_change(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_then_keyset_incremental_same_key_oracle() {
    incremental_then_keyset_incremental_same_key(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_cursor_change_oracle() {
    incremental_cursor_change(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_single_to_coalesce_oracle() {
    incremental_single_to_coalesce(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_adding_settle_oracle() {
    incremental_adding_settle(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn legacy_keyset_state_then_other_column_oracle() {
    legacy_keyset_state_then_other_column(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn legacy_incremental_state_then_other_column_oracle() {
    legacy_incremental_state_then_other_column(SqlEngine::Oracle);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunk_then_incremental_finish_the_run_oracle() {
    crashed_range_chunk_then_incremental(SqlEngine::Oracle, Remedy::FinishTheRun);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn crashed_range_chunk_then_incremental_reset_oracle() {
    crashed_range_chunk_then_incremental(SqlEngine::Oracle, Remedy::Reset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn resumed_parallel_keyset_then_incremental_mysql() {
    resumed_parallel_keyset_then_incremental(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn resumed_parallel_keyset_then_incremental_mssql() {
    resumed_parallel_keyset_then_incremental(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn resumed_parallel_keyset_then_incremental_oracle() {
    resumed_parallel_keyset_then_incremental(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn source_url_without_its_default_port_continues_mysql() {
    source_url_without_its_default_port_continues(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn source_url_without_its_default_port_continues_mssql() {
    source_url_without_its_default_port_continues(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn source_url_without_its_default_port_continues_oracle() {
    source_url_without_its_default_port_continues(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn incremental_query_filter_edited_mysql() {
    incremental_query_filter_edited(SqlEngine::Mysql, true);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn incremental_query_filter_edited_mssql() {
    incremental_query_filter_edited(SqlEngine::Mssql, true);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn incremental_query_filter_edited_oracle() {
    incremental_query_filter_edited(SqlEngine::Oracle, true);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: requires the oracle-latin1 service"]
fn range_chunk_shared_name_another_source_oracle() {
    range_chunk_shared_name_another_source(SqlEngine::Oracle);
}
