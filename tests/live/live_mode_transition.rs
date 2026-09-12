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

fn keyset_incremental_key_change(e: SqlEngine) {
    transition(
        e,
        KEYSET_INCREMENTAL_ID,
        KEYSET_INCREMENTAL_EXT_ID,
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
#[ignore = "live: requires docker compose postgres"]
fn legacy_keyset_state_then_other_column_postgres() {
    legacy_keyset_state_then_other_column(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn legacy_keyset_state_then_other_column_mssql() {
    legacy_keyset_state_then_other_column(SqlEngine::Mssql);
}
