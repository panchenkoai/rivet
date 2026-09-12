//! Incremental settle window (`exports[].settle`).

use crate::common::*;
use std::path::Path;

fn settled_rig(engine: SqlEngine, table: &str, out: &Path, lines: &[&str]) -> Rig {
    engine
        .rig(table)
        .query(&format!("SELECT id, server_time, time_spent FROM {table}"))
        .restage("incremental", lines)
        .dest_path(out.to_path_buf())
}

fn settle_holds_back_young_rows_until_they_age(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = engine.table("settle_age");
    engine.insert(&table, 1..=20, 180, Some(10));
    engine.insert(&table, 21..=25, 10, None);
    let out = tempfile::tempdir().unwrap();
    let rig = settled_rig(
        engine,
        &table,
        out.path(),
        &["cursor_column: server_time", "settle: { after: 1h }"],
    );

    rig.run_ok();
    assert_eq!(read_ids(out.path()), (1..=20).collect::<Vec<_>>());

    engine.exec(&format!(
        "UPDATE {table} SET time_spent = 999, server_time = {} WHERE id > 20",
        engine.ago(61)
    ));
    rig.run_ok();
    let expected: Vec<(i64, Option<i32>)> = (1..=20)
        .map(|i| (i, Some(10)))
        .chain((21..=25).map(|i| (i, Some(999))))
        .collect();
    assert_eq!(read_id_spent(out.path()), expected);
}

fn settle_on_another_column_never_skips_an_out_of_order_row(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = engine.table("settle_ooo");
    engine.insert(&table, 1..=10, 180, Some(10));
    let out = tempfile::tempdir().unwrap();
    let rig = settled_rig(
        engine,
        &table,
        out.path(),
        &[
            "cursor_column: id",
            "settle: { after: 1h, column: server_time }",
        ],
    );

    rig.run_ok();
    assert_eq!(read_ids(out.path()), (1..=10).collect::<Vec<_>>());

    engine.insert(&table, 11..=11, 10, None);
    engine.insert(&table, 12..=12, 120, Some(10));
    rig.run_ok();
    assert_eq!(
        read_ids(out.path()),
        (1..=10).collect::<Vec<_>>(),
        "a settled row above a still-settling id must wait for it"
    );

    engine.exec(&format!(
        "UPDATE {table} SET time_spent = 999, server_time = {} WHERE id = 11",
        engine.ago(61)
    ));
    rig.run_ok();
    let mut expected: Vec<(i64, Option<i32>)> = (1..=10).map(|i| (i, Some(10))).collect();
    expected.push((11, Some(999)));
    expected.push((12, Some(10)));
    assert_eq!(read_id_spent(out.path()), expected);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn settle_holds_back_young_rows_until_they_age_mysql() {
    settle_holds_back_young_rows_until_they_age(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn settle_holds_back_young_rows_until_they_age_postgres() {
    settle_holds_back_young_rows_until_they_age(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn settle_holds_back_young_rows_until_they_age_mssql() {
    settle_holds_back_young_rows_until_they_age(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn settle_on_another_column_never_skips_an_out_of_order_row_mysql() {
    settle_on_another_column_never_skips_an_out_of_order_row(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn settle_on_another_column_never_skips_an_out_of_order_row_postgres() {
    settle_on_another_column_never_skips_an_out_of_order_row(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn settle_on_another_column_never_skips_an_out_of_order_row_mssql() {
    settle_on_another_column_never_skips_an_out_of_order_row(SqlEngine::Mssql);
}
