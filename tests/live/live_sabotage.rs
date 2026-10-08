//! Sabotage (docs/sabotage-matrix.yaml): deliberate failure as gate cells, one generator per row and
//! one cell per engine. After every scenario the only acceptable outcomes are exit 0 with the
//! destination equal to the source (the default oracle at the rig's seam), or a refusal that repeats
//! and whose named remedy leads there. A cell asserts that; one that fails today is an
//! `open_defect_*` cell acknowledged in dev/release_oracle/known_red.py.
//!
//! `refusal_*` rows walk the error-code registry through `Rig::refuses_twice_and_walks_out`: refuse,
//! refuse again, each remedy the text names, and one wrong remedy. `corrupt_*` rows damage what a
//! run reads about itself (`Rig::damage`, `Rig::edit_state`) and grade the next command through
//! `Rig::delivers_or_refuses`.

use crate::common::*;

pub(crate) const N: i64 = 40;
const MONGO_PORT: u16 = 27017;

const RANGE_CHECKPOINT: &[&str] = &[
    "chunk_column: id",
    "chunk_size: 5",
    "chunk_checkpoint: true",
];
const KEYSET_INCREMENTAL: &[&str] = &[
    "chunk_by_key: id",
    "chunk_size: 4",
    "chunk_checkpoint: true",
    "keyset_incremental: true",
];

/// The tasks of this export's chunk runs, for a state edit.
const ITS_TASKS: &str = "run_id IN (SELECT run_id FROM chunk_run WHERE export_name = '{export}')";

/// A fresh standard table holding ids `1..=N`, and its drop guard.
pub(crate) fn seeded(engine: SqlEngine, tag: &str) -> (String, Box<dyn std::any::Any>) {
    engine.alive();
    let (table, guard) = engine.range_table(tag);
    engine.insert(&table, 1..=N, 180, Some(10));
    (table, guard)
}

/// A full export of `N` documents of a fresh database on the standalone MongoDB, and its drop guard.
pub(crate) fn mongo_rig(tag: &str) -> (Rig, MongoDbGuard) {
    require_alive(LiveService::Mongo);
    let db = unique_name(tag);
    let guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    MongoTest::connect(MONGO_PORT, &db).seed_int_id("t", N);
    (
        Rig::mongo_batch("t").source_url(&MongoTest::url(MONGO_PORT, &db)),
        guard,
    )
}

/// Panic unless the invocation exited 0.
fn ok(out: std::process::Output) {
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `rivet state <verb> --export <this rig's export>`, which must succeed.
fn state(rig: &Rig, verb: &str, export: &str) {
    ok(rig.cli(&["state", verb, "--export", export]));
}

// refusal_*: the registry, refuse -> again -> each named remedy -> one wrong remedy

/// RIVET_STATE_CURSOR_OWNER_MISMATCH: an incremental export whose `cursor_column` was changed with no reset.
fn cursor_owner_mismatch(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, "sab_owner");
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let on = |rig: Rig, column: &str, out: &std::path::Path| {
        engine
            .staged(rig, "incremental", &[&format!("cursor_column: {column}")])
            .dest_path(out.to_path_buf())
    };
    let rig = on(engine.rig(&table), "id", first.path());
    rig.run_ok();
    engine.insert(&table, N + 1..=N + 3, 170, Some(10));
    let mut rig = on(rig, "ext_id", second.path());
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        Refused::by_code("RIVET_STATE_CURSOR_OWNER_MISMATCH", 5),
        vec![
            Remedy::new(
                &format!("`rivet state reset -c <config> --export {table}` starts"),
                Then::DeliversTheSource,
                |r| state(r, "reset", &table),
            ),
            Remedy::new(
                "or restore the previous cursor",
                Then::DeliversTheSource,
                |r| r.rebuilt(|r| on(r, "id", first.path())),
            ),
            Remedy::wrong(
                "runs `rivet state reset-chunks`, the sibling of the command the text names",
                Then::Refuses(Refused::by_code("RIVET_STATE_CURSOR_OWNER_MISMATCH", 5)),
                |r| state(r, "reset-chunks", &table),
            ),
        ],
    );
}

/// RIVET_STATE_CURSOR_STREAM_MISMATCH: an incremental export repointed at another table with no reset.
fn cursor_stream_mismatch(engine: SqlEngine) {
    engine.alive();
    let (a, _ga) = engine.table("sab_stream_a");
    let (b, _gb) = engine.table("sab_stream_b");
    engine.insert(&a, 101..=110, 170, Some(10));
    engine.insert(&b, 1..=10, 180, Some(10));
    let (first, second) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let staged = |rig: Rig, out: &std::path::Path| {
        engine
            .staged(rig, "incremental", &["cursor_column: id"])
            .dest_path(out.to_path_buf())
    };
    let rig = staged(engine.rig(&a), first.path());
    rig.run_ok();
    let mut rig = staged(rig.repoint(&b), second.path());
    let own = unique_name("sab_own");
    let refused = Refused::by_code("RIVET_STATE_CURSOR_STREAM_MISMATCH", 5);
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        refused,
        vec![
            Remedy::new(
                &format!("`rivet state reset -c <config> --export {a}` starts `"),
                Then::DeliversTheSource,
                |r| state(r, "reset", &a),
            ),
            Remedy::new("need their own names", Then::DeliversTheSource, |r| {
                r.rebuilt(|r| r.export_named(&own))
            }),
            Remedy::wrong(
                "runs `rivet state reset-chunks`, the sibling of the command the text names",
                Then::Refuses(refused),
                |r| state(r, "reset-chunks", &a),
            ),
        ],
    );
}

/// A range-chunked, checkpointed run of a fresh `N`-row table that crashed after its third chunk.
fn interrupted(engine: SqlEngine, tag: &str) -> (String, Rig, Box<dyn std::any::Any>) {
    let (table, guard) = seeded(engine, tag);
    let rig = engine.staged(engine.rig(&table), "chunked", RANGE_CHECKPOINT);
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "after_chunk_complete:2");
    assert!(
        !crash.status.success(),
        "fixture: the run crashes after its third chunk"
    );
    (table, rig, guard)
}

/// RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH: an unfinished range-chunk run, and the export now runs as `incremental`.
fn interrupted_run_owner_mismatch(engine: SqlEngine) {
    let (table, rig, _guard) = interrupted(engine, "sab_interrupted");
    let mut rig = engine.staged(rig, "incremental", &["cursor_column: id"]);
    let refused = Refused::by_code("RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH", 5);
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        refused,
        vec![
            Remedy::new(
                "restore the `chunked` settings and run once to finish run",
                Then::DeliversTheSource,
                |r| r.rebuilt(|r| engine.staged(r, "chunked", RANGE_CHECKPOINT)),
            ),
            Remedy::new(
                "abandons it and the next run starts with a full pass",
                Then::DeliversTheSource,
                |r| state(r, "reset-chunks", &table),
            ),
            Remedy::wrong(
                "runs `rivet state reset` where the text names `rivet state reset-chunks`",
                Then::Refuses(refused),
                |r| state(r, "reset", &table),
            ),
            Remedy::wrong(
                "empties the destination and runs again",
                Then::Refuses(refused),
                |r| {
                    std::fs::remove_dir_all(r.out_dir()).expect("empty the destination");
                    std::fs::create_dir_all(r.out_dir()).expect("the destination directory");
                },
            ),
        ],
    );
}

/// RIVET_STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED: an unfinished sequential keyset-incremental run, and the export now runs with `parallel: 2`.
fn keyset_sequential_anchor_unfinished(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, "sab_anchor");
    let rig = engine.staged(engine.rig(&table), "chunked", KEYSET_INCREMENTAL);
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", "keyset_after_data_complete");
    assert!(
        !crash.status.success(),
        "fixture: the run crashes after its last page"
    );
    let parallel: Vec<&str> = KEYSET_INCREMENTAL
        .iter()
        .copied()
        .chain(["parallel: 2"])
        .collect();
    let mut rig = engine.staged(rig, "chunked", &parallel);
    let refused = Refused::by_code("RIVET_STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED", 5);
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        refused,
        vec![
            Remedy::new(
                "Re-run once with `parallel: 1` to finish run",
                Then::DeliversTheSource,
                |r| r.rebuilt(|r| engine.staged(r, "chunked", KEYSET_INCREMENTAL)),
            ),
            Remedy::wrong(
                "runs `rivet state reset-chunks` and keeps `parallel: 2`",
                Then::Refuses(refused),
                |r| state(r, "reset-chunks", &table),
            ),
        ],
    );
}

/// RIVET_STATE_SCHEMA_NEWER: a state database a newer rivet migrated.
fn state_schema_newer(mut rig: Rig, export: &str) {
    if state_url_under_test().is_some() {
        return skip_live(
            "a SQLite state whose schema version one cell may raise; this pass grades the shared Postgres state (RIVET_GATE_STATE_URL)",
        );
    }
    rig.run_ok();
    rig.edit_state("UPDATE schema_version SET version = version + 100", 1);
    let refused = Refused::by_code("RIVET_STATE_SCHEMA_NEWER", 5);
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        refused,
        vec![
            Remedy::new(
                "point this one at a state DB it created",
                Then::DeliversTheSource,
                |r| {
                    let db = r.config_path().with_file_name(".rivet_state.db");
                    std::fs::remove_file(db).expect("remove the newer state database");
                },
            ),
            Remedy::wrong(
                "runs `rivet state reset` on the newer state database",
                Then::Refuses(refused),
                |r| {
                    let _ = r.cli(&["state", "reset", "--export", export]);
                },
            ),
        ],
    );
}

// corrupt_*: a complete export's manifest, marker and parts

/// A complete range-chunked export of a fresh `N`-row table in four parts.
fn complete(engine: SqlEngine, tag: &str) -> (Rig, Box<dyn std::any::Any>) {
    let (table, guard) = seeded(engine, tag);
    let rig = engine.staged(
        engine.rig(&table),
        "chunked",
        &["chunk_column: id", "chunk_size: 10"],
    );
    rig.run_ok();
    (rig, guard)
}

/// One way to damage a complete export.
#[derive(Clone, Copy)]
enum Harm {
    ManifestTruncated,
    ManifestNotJson,
    ManifestDropsAPart,
    ManifestNamesAGhostPart,
    ManifestRemoved,
    PartTruncated,
    PartReplacedByItsSibling,
    PartRemoved,
}

/// Damage `rig`'s complete export.
fn harm(rig: &Rig, how: Harm) {
    let manifest = rig.out_dir().join("manifest.json");
    let part = |n: usize| rig.manifest_parts()[n].clone();
    match how {
        Harm::ManifestTruncated => rig.damage(&manifest, Damage::Truncated),
        Harm::ManifestNotJson => rig.damage(&manifest, Damage::NotJson),
        Harm::ManifestDropsAPart => rig.damage(
            &manifest,
            Damage::Json(&|m| {
                m["parts"].as_array_mut().expect("parts").pop();
            }),
        ),
        Harm::ManifestNamesAGhostPart => rig.damage(
            &manifest,
            Damage::Json(&|m| {
                let parts = m["parts"].as_array_mut().expect("parts");
                let mut ghost = parts[0].clone();
                ghost["path"] = serde_json::json!("ghost.parquet");
                parts.push(ghost);
            }),
        ),
        Harm::ManifestRemoved => rig.damage(&manifest, Damage::Removed),
        Harm::PartTruncated => rig.damage(&part(0), Damage::Truncated),
        Harm::PartReplacedByItsSibling => rig.damage(&part(0), Damage::ReplacedBy(&part(1))),
        Harm::PartRemoved => rig.damage(&part(0), Damage::Removed),
    }
}

/// `validate` fails on the damaged export every time and names `finding`; the next run delivers the source and `validate` passes.
fn validate_sees(rig: Rig, how: Harm, finding: &str) {
    harm(&rig, how);
    rig.validate_fails(finding);
    rig.run_ok();
    ok(rig.cli(&["validate"]));
}

fn validate_sees_sql(engine: SqlEngine, how: Harm, finding: &str) {
    let (rig, _guard) = complete(engine, "sab_validate");
    validate_sees(rig, how, finding);
}

fn validate_sees_mongo(how: Harm, finding: &str) {
    let (rig, _guard) = mongo_rig("sab_validate");
    rig.run_ok();
    validate_sees(rig, how, finding);
}

fn state_schema_newer_mongo() {
    let (rig, _guard) = mongo_rig("sab_newer");
    state_schema_newer(rig, "t");
}

fn state_schema_newer_sql(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, "sab_newer");
    state_schema_newer(engine.rig(&table), &table);
}

/// A manifest whose marker is gone: the next run delivers the source and writes the marker again.
fn marker_removed(engine: SqlEngine) {
    let (rig, _guard) = complete(engine, "sab_marker");
    let marker = rig.out_dir().join("_SUCCESS");
    rig.damage(&marker, Damage::Removed);
    rig.run_ok();
    assert!(marker.is_file(), "the run wrote `_SUCCESS` again");
    ok(rig.cli(&["validate"]));
}

// corrupt_*: the checkpoint rows and parts of an unfinished range-chunk run

/// Edit the checkpoint of an unfinished run; the resume then delivers the source or refuses, and a delivered export validates.
fn resume_after(engine: SqlEngine, sabotage: impl FnOnce(&Rig)) {
    let (_table, rig, _guard) = interrupted(engine, "sab_resume");
    sabotage(&rig);
    if rig.delivers_or_refuses(&["run"], &[]) == Survived::Delivered {
        ok(rig.cli(&["validate"]));
    }
}

/// A pending chunk task marked `completed` in the checkpoint.
fn task_marked_completed(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.edit_state(
            &format!(
                "UPDATE chunk_task SET status = 'completed' WHERE chunk_index = 5 AND {ITS_TASKS}"
            ),
            1,
        )
    });
}

/// A pending chunk task whose upper bound was lowered, leaving a hole between two tasks.
fn task_bounds_edited(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.edit_state(
            &format!("UPDATE chunk_task SET end_key = '33' WHERE chunk_index = 6 AND {ITS_TASKS}"),
            1,
        )
    });
}

/// Every chunk task of the unfinished run deleted, its `chunk_run` row kept.
fn tasks_deleted(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.edit_state(&format!("DELETE FROM chunk_task WHERE {ITS_TASKS}"), 8)
    });
}

/// The `chunk_run` row of the unfinished run deleted, its tasks kept.
fn chunk_run_deleted(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.edit_state("DELETE FROM chunk_run WHERE export_name = '{export}'", 1)
    });
}

/// The unfinished `chunk_run` row marked `completed`.
fn chunk_run_marked_completed(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.edit_state(
            "UPDATE chunk_run SET status = 'completed' WHERE export_name = '{export}'",
            1,
        )
    });
}

/// The parts the crashed run committed, oldest first.
fn committed_parts(rig: &Rig) -> Vec<std::path::PathBuf> {
    let mut parts = files_with_extension(&rig.out_dir(), "parquet");
    parts.sort();
    assert_eq!(
        parts.len(),
        3,
        "fixture: the crashed run committed three parts"
    );
    parts
}

/// A part the unfinished run committed, truncated before the resume.
fn committed_part_truncated(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.damage(&committed_parts(rig)[0], Damage::Truncated)
    });
}

/// A part the unfinished run committed, deleted before the resume.
fn committed_part_removed(engine: SqlEngine) {
    resume_after(engine, |rig| {
        rig.damage(&committed_parts(rig)[0], Damage::Removed)
    });
}

/// A part the unfinished run committed, replaced by the bytes of its sibling before the resume.
fn committed_part_replaced(engine: SqlEngine) {
    resume_after(engine, |rig| {
        let parts = committed_parts(rig);
        rig.damage(&parts[0], Damage::ReplacedBy(&parts[1]))
    });
}

/// After a sabotage the export delivers the source, or is refused by code naming `remedy_text`, whose `remedy` then delivers it.
fn delivers_or_is_led_out(rig: &Rig, remedy_text: &str, remedy: impl FnOnce(&Rig)) {
    if let Survived::Refused(said) = rig.delivers_or_refuses(&["run"], &[]) {
        assert!(
            said.contains(remedy_text),
            "a refusal nothing leads out of: the text does not name `{remedy_text}`\n{said}"
        );
        remedy(rig);
        rig.run_ok();
    }
}

/// The stored plan fingerprint of the unfinished run edited: the resume is refused by code naming `rivet state reset-chunks`, which leads out.
fn plan_fingerprint_edited(engine: SqlEngine) {
    let (table, rig, _guard) = interrupted(engine, "sab_fingerprint");
    rig.edit_state(
        "UPDATE chunk_run SET plan_hash = 'deadbeef' WHERE export_name = '{export}'",
        1,
    );
    delivers_or_is_led_out(&rig, "rivet state reset-chunks", |r| {
        state(r, "reset-chunks", &table)
    });
}

// corrupt_*: the stored cursor of an incremental export

/// An incremental export of ids `1..=N` on `id`, then three more source rows; its cursor row is edited before the next run.
fn cursor_edited(engine: SqlEngine, value: &str) -> (String, Rig, Box<dyn std::any::Any>) {
    let (table, guard) = seeded(engine, "sab_cursor");
    let rig = engine.staged(engine.rig(&table), "incremental", &["cursor_column: id"]);
    rig.run_ok();
    engine.insert(&table, N + 1..=N + 3, 170, Some(10));
    rig.edit_state(
        &format!(
            "UPDATE export_state SET last_cursor_value = '{value}' WHERE export_name = '{{export}}'"
        ),
        1,
    );
    (table, rig, guard)
}

/// The cursor moved back, or forward past rows the export never delivered: the run delivers the source or refuses.
fn cursor_moved(engine: SqlEngine, value: i64) {
    let (_table, rig, _guard) = cursor_edited(engine, &value.to_string());
    rig.delivers_or_refuses(&["run"], &[]);
}

/// A cursor value that is not of the cursor column's type: the run delivers the source, or is refused by code naming `rivet state reset`, which leads out.
fn cursor_of_another_type(engine: SqlEngine) {
    let (table, rig, _guard) = cursor_edited(engine, "not-a-number");
    delivers_or_is_led_out(&rig, "rivet state reset", |r| state(r, "reset", &table));
}

// cells: one per engine, each named in docs/sabotage-matrix.yaml

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_changed_cursor_column_is_refused_and_walked_out_postgres() {
    cursor_owner_mismatch(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_changed_cursor_column_is_refused_and_walked_out_mysql() {
    cursor_owner_mismatch(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_changed_cursor_column_is_refused_and_walked_out_mssql() {
    cursor_owner_mismatch(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_changed_cursor_column_is_refused_and_walked_out_oracle() {
    cursor_owner_mismatch(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_repointed_incremental_export_is_refused_and_walked_out_postgres() {
    cursor_stream_mismatch(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_repointed_incremental_export_is_refused_and_walked_out_mysql() {
    cursor_stream_mismatch(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_repointed_incremental_export_is_refused_and_walked_out_mssql() {
    cursor_stream_mismatch(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_repointed_incremental_export_is_refused_and_walked_out_oracle() {
    cursor_stream_mismatch(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_unfinished_chunk_run_under_another_mode_is_refused_and_walked_out_postgres() {
    interrupted_run_owner_mismatch(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_unfinished_chunk_run_under_another_mode_is_refused_and_walked_out_mysql() {
    interrupted_run_owner_mismatch(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_unfinished_chunk_run_under_another_mode_is_refused_and_walked_out_mssql() {
    interrupted_run_owner_mismatch(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unfinished_chunk_run_under_another_mode_is_refused_and_walked_out_oracle() {
    interrupted_run_owner_mismatch(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_unfinished_sequential_keyset_run_under_parallel_is_refused_and_walked_out_postgres() {
    keyset_sequential_anchor_unfinished(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_unfinished_sequential_keyset_run_under_parallel_is_refused_and_walked_out_mysql() {
    keyset_sequential_anchor_unfinished(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_unfinished_sequential_keyset_run_under_parallel_is_refused_and_walked_out_mssql() {
    keyset_sequential_anchor_unfinished(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unfinished_sequential_keyset_run_under_parallel_is_refused_and_walked_out_oracle() {
    keyset_sequential_anchor_unfinished(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_newer_state_schema_is_refused_and_walked_out_postgres() {
    state_schema_newer_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_newer_state_schema_is_refused_and_walked_out_mysql() {
    state_schema_newer_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_newer_state_schema_is_refused_and_walked_out_mssql() {
    state_schema_newer_sql(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_newer_state_schema_is_refused_and_walked_out_mongo() {
    state_schema_newer_mongo();
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_newer_state_schema_is_refused_and_walked_out_oracle() {
    state_schema_newer_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_truncated_manifest_postgres() {
    validate_sees_sql(
        SqlEngine::Pg,
        Harm::ManifestTruncated,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_truncated_manifest_mysql() {
    validate_sees_sql(
        SqlEngine::Mysql,
        Harm::ManifestTruncated,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_truncated_manifest_mssql() {
    validate_sees_sql(
        SqlEngine::Mssql,
        Harm::ManifestTruncated,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn validate_sees_a_truncated_manifest_mongo() {
    validate_sees_mongo(
        Harm::ManifestTruncated,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_truncated_manifest_oracle() {
    validate_sees_sql(
        SqlEngine::Oracle,
        Harm::ManifestTruncated,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_manifest_that_is_not_json_postgres() {
    validate_sees_sql(
        SqlEngine::Pg,
        Harm::ManifestNotJson,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_manifest_that_is_not_json_mysql() {
    validate_sees_sql(
        SqlEngine::Mysql,
        Harm::ManifestNotJson,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_manifest_that_is_not_json_mssql() {
    validate_sees_sql(
        SqlEngine::Mssql,
        Harm::ManifestNotJson,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn validate_sees_a_manifest_that_is_not_json_mongo() {
    validate_sees_mongo(Harm::ManifestNotJson, "RIVET_VERIFY_MANIFEST_INCONSISTENT");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_manifest_that_is_not_json_oracle() {
    validate_sees_sql(
        SqlEngine::Oracle,
        Harm::ManifestNotJson,
        "RIVET_VERIFY_MANIFEST_INCONSISTENT",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_manifest_that_drops_a_part_postgres() {
    validate_sees_sql(SqlEngine::Pg, Harm::ManifestDropsAPart, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_manifest_that_drops_a_part_mysql() {
    validate_sees_sql(SqlEngine::Mysql, Harm::ManifestDropsAPart, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_manifest_that_drops_a_part_mssql() {
    validate_sees_sql(SqlEngine::Mssql, Harm::ManifestDropsAPart, "RIVET_VERIFY_");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_manifest_that_drops_a_part_oracle() {
    validate_sees_sql(SqlEngine::Oracle, Harm::ManifestDropsAPart, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_manifest_naming_a_part_that_is_not_there_postgres() {
    validate_sees_sql(
        SqlEngine::Pg,
        Harm::ManifestNamesAGhostPart,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_manifest_naming_a_part_that_is_not_there_mysql() {
    validate_sees_sql(
        SqlEngine::Mysql,
        Harm::ManifestNamesAGhostPart,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_manifest_naming_a_part_that_is_not_there_mssql() {
    validate_sees_sql(
        SqlEngine::Mssql,
        Harm::ManifestNamesAGhostPart,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn validate_sees_a_manifest_naming_a_part_that_is_not_there_mongo() {
    validate_sees_mongo(Harm::ManifestNamesAGhostPart, "RIVET_VERIFY_");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_manifest_naming_a_part_that_is_not_there_oracle() {
    validate_sees_sql(
        SqlEngine::Oracle,
        Harm::ManifestNamesAGhostPart,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (validate passes a prefix with no manifest), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_marker_without_a_manifest_postgres() {
    validate_sees_sql(SqlEngine::Pg, Harm::ManifestRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (validate passes a prefix with no manifest), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_marker_without_a_manifest_mysql() {
    validate_sees_sql(SqlEngine::Mysql, Harm::ManifestRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (validate passes a prefix with no manifest), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_marker_without_a_manifest_mssql() {
    validate_sees_sql(SqlEngine::Mssql, Harm::ManifestRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live+gate-only: docker compose mongo; open defect (validate passes a prefix with no manifest), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_validate_fails_on_a_marker_without_a_manifest_mongo() {
    validate_sees_mongo(Harm::ManifestRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_run_restores_a_removed_success_marker_postgres() {
    marker_removed(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_restores_a_removed_success_marker_mysql() {
    marker_removed(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_restores_a_removed_success_marker_mssql() {
    marker_removed(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_restores_a_removed_success_marker_oracle() {
    marker_removed(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_truncated_part_postgres() {
    validate_sees_sql(
        SqlEngine::Pg,
        Harm::PartTruncated,
        "RIVET_VERIFY_PART_SIZE_MISMATCH",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_truncated_part_mysql() {
    validate_sees_sql(
        SqlEngine::Mysql,
        Harm::PartTruncated,
        "RIVET_VERIFY_PART_SIZE_MISMATCH",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_truncated_part_mssql() {
    validate_sees_sql(
        SqlEngine::Mssql,
        Harm::PartTruncated,
        "RIVET_VERIFY_PART_SIZE_MISMATCH",
    );
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn validate_sees_a_truncated_part_mongo() {
    validate_sees_mongo(Harm::PartTruncated, "RIVET_VERIFY_PART_SIZE_MISMATCH");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_truncated_part_oracle() {
    validate_sees_sql(
        SqlEngine::Oracle,
        Harm::PartTruncated,
        "RIVET_VERIFY_PART_SIZE_MISMATCH",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_part_replaced_by_its_sibling_postgres() {
    validate_sees_sql(
        SqlEngine::Pg,
        Harm::PartReplacedByItsSibling,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_part_replaced_by_its_sibling_mysql() {
    validate_sees_sql(
        SqlEngine::Mysql,
        Harm::PartReplacedByItsSibling,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_part_replaced_by_its_sibling_mssql() {
    validate_sees_sql(
        SqlEngine::Mssql,
        Harm::PartReplacedByItsSibling,
        "RIVET_VERIFY_",
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_part_replaced_by_its_sibling_oracle() {
    validate_sees_sql(
        SqlEngine::Oracle,
        Harm::PartReplacedByItsSibling,
        "RIVET_VERIFY_",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_sees_a_removed_part_postgres() {
    validate_sees_sql(SqlEngine::Pg, Harm::PartRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_sees_a_removed_part_mysql() {
    validate_sees_sql(SqlEngine::Mysql, Harm::PartRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_sees_a_removed_part_mssql() {
    validate_sees_sql(SqlEngine::Mssql, Harm::PartRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn validate_sees_a_removed_part_mongo() {
    validate_sees_mongo(Harm::PartRemoved, "RIVET_VERIFY_");
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_sees_a_removed_part_oracle() {
    validate_sees_sql(SqlEngine::Oracle, Harm::PartRemoved, "RIVET_VERIFY_");
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_task_marked_completed_delivers_or_refuses_postgres() {
    task_marked_completed(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_task_marked_completed_delivers_or_refuses_mysql() {
    task_marked_completed(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_task_marked_completed_delivers_or_refuses_mssql() {
    task_marked_completed(SqlEngine::Mssql);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_edited_task_bounds_delivers_or_refuses_postgres() {
    task_bounds_edited(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_edited_task_bounds_delivers_or_refuses_mysql() {
    task_bounds_edited(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_edited_task_bounds_delivers_or_refuses_mssql() {
    task_bounds_edited(SqlEngine::Mssql);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_deleted_tasks_delivers_or_refuses_postgres() {
    tasks_deleted(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_deleted_tasks_delivers_or_refuses_mysql() {
    tasks_deleted(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_deleted_tasks_delivers_or_refuses_mssql() {
    tasks_deleted(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_resume_over_a_deleted_chunk_run_delivers_or_refuses_postgres() {
    chunk_run_deleted(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_resume_over_a_deleted_chunk_run_delivers_or_refuses_mysql() {
    chunk_run_deleted(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_resume_over_a_deleted_chunk_run_delivers_or_refuses_mssql() {
    chunk_run_deleted(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_resume_over_a_deleted_chunk_run_delivers_or_refuses_oracle() {
    chunk_run_deleted(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_resume_over_a_chunk_run_marked_completed_delivers_or_refuses_postgres() {
    chunk_run_marked_completed(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_resume_over_a_chunk_run_marked_completed_delivers_or_refuses_mysql() {
    chunk_run_marked_completed(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_resume_over_a_chunk_run_marked_completed_delivers_or_refuses_mssql() {
    chunk_run_marked_completed(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_resume_over_a_chunk_run_marked_completed_delivers_or_refuses_oracle() {
    chunk_run_marked_completed(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (the plan-fingerprint refusal has no code), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_edited_plan_fingerprint_delivers_or_is_refused_by_code_and_led_out_postgres() {
    plan_fingerprint_edited(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (the plan-fingerprint refusal has no code), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_edited_plan_fingerprint_delivers_or_is_refused_by_code_and_led_out_mysql() {
    plan_fingerprint_edited(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (the plan-fingerprint refusal has no code), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_edited_plan_fingerprint_delivers_or_is_refused_by_code_and_led_out_mssql() {
    plan_fingerprint_edited(SqlEngine::Mssql);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_truncated_committed_part_delivers_or_refuses_postgres() {
    committed_part_truncated(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_truncated_committed_part_delivers_or_refuses_mysql() {
    committed_part_truncated(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_truncated_committed_part_delivers_or_refuses_mssql() {
    committed_part_truncated(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_resume_over_a_removed_committed_part_delivers_or_refuses_postgres() {
    committed_part_removed(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_resume_over_a_removed_committed_part_delivers_or_refuses_mysql() {
    committed_part_removed(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_resume_over_a_removed_committed_part_delivers_or_refuses_mssql() {
    committed_part_removed(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_resume_over_a_removed_committed_part_delivers_or_refuses_oracle() {
    committed_part_removed(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_replaced_committed_part_delivers_or_refuses_postgres() {
    committed_part_replaced(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_replaced_committed_part_delivers_or_refuses_mysql() {
    committed_part_replaced(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_replaced_committed_part_delivers_or_refuses_mssql() {
    committed_part_replaced(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_cursor_moved_back_delivers_or_refuses_postgres() {
    cursor_moved(SqlEngine::Pg, 5);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_cursor_moved_back_delivers_or_refuses_mysql() {
    cursor_moved(SqlEngine::Mysql, 5);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_cursor_moved_back_delivers_or_refuses_mssql() {
    cursor_moved(SqlEngine::Mssql, 5);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_cursor_moved_back_delivers_or_refuses_oracle() {
    cursor_moved(SqlEngine::Oracle, 5);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a cursor moved forward skips rows), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cursor_moved_forward_delivers_or_refuses_postgres() {
    cursor_moved(SqlEngine::Pg, N + 2);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a cursor moved forward skips rows), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cursor_moved_forward_delivers_or_refuses_mysql() {
    cursor_moved(SqlEngine::Mysql, N + 2);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a cursor moved forward skips rows), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cursor_moved_forward_delivers_or_refuses_mssql() {
    cursor_moved(SqlEngine::Mssql, N + 2);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a cursor of another type is a raw driver error), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cursor_of_another_type_delivers_or_is_refused_by_code_and_led_out_postgres() {
    cursor_of_another_type(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_cursor_of_another_type_delivers_or_is_refused_by_code_and_led_out_mysql() {
    cursor_of_another_type(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a cursor of another type is a raw driver error), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_cursor_of_another_type_delivers_or_is_refused_by_code_and_led_out_mssql() {
    cursor_of_another_type(SqlEngine::Mssql);
}
