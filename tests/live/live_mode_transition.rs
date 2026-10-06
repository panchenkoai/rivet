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

const STREAM_CODE: &str = "RIVET_STATE_CURSOR_STREAM_MISMATCH";

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
    let lines: Vec<String> = stage
        .1
        .iter()
        .map(|l| match l.split_once(": ") {
            Some((k @ ("chunk_by_key" | "cursor_column"), v)) if engine.folds_upper() => {
                format!("{k}: {}", v.to_uppercase())
            }
            _ => l.to_string(),
        })
        .collect();
    let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
    rig.restage(stage.0, &lines).dest_path(out.to_path_buf())
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

    let rig = staged_for(engine, rig.repoint(&b), &stage, second.path());
    refused_twice_for_the_stream(&rig, second.path(), &a, &b, &ids);

    let reset = rig.cli(&["state", "reset", "--export", &a]);
    assert!(
        reset.status.success(),
        "{}",
        String::from_utf8_lossy(&reset.stderr)
    );
    rig.run_ok();
    assert_eq!(ids(second.path()), (1..=10).collect::<Vec<_>>());
}

/// Two configs, one export name, one Postgres state, two tables: the second is refused until it has its own name.
fn stream_shared_name(engine: SqlEngine, stage: Stage) {
    if state_url_under_test().is_none() {
        return skip_live(
            "RIVET_GATE_STATE_URL unset: two configs share one state only on the Postgres backend",
        );
    }
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

    let two = staged_for(
        engine,
        engine.rig(&b).export_named(&shared),
        &stage,
        second.path(),
    );
    refused_twice_for_the_stream(&two, second.path(), &a, &b, &ids);

    let own = staged_for(
        engine,
        two.export_named(&unique_name("own")),
        &stage,
        second.path(),
    );
    own.run_ok();
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
        "SELECT id, ext_id, server_time, time_spent FROM {a} WHERE id > 0"
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
    if shared_name && state_url_under_test().is_none() {
        return skip_live(
            "RIVET_GATE_STATE_URL unset: two configs share one state only on the Postgres backend",
        );
    }
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

    let rig = if shared_name {
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
