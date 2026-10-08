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
    mongo_rig_of(tag, N)
}

/// [`mongo_rig`] over `docs` documents.
fn mongo_rig_of(tag: &str, docs: i64) -> (Rig, MongoDbGuard) {
    require_alive(LiveService::Mongo);
    let db = unique_name(tag);
    let guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    MongoTest::connect(MONGO_PORT, &db).seed_int_id("t", docs);
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

/// A complete range-chunked export of a fresh `N`-row table in four parts (`chunk_count`: a row estimate at or below a bare `chunk_size` plans one snapshot part instead).
fn complete(engine: SqlEngine, tag: &str) -> (Rig, Box<dyn std::any::Any>) {
    let (table, guard) = seeded(engine, tag);
    let rig = engine.staged(
        engine.rig(&table),
        "chunked",
        &["chunk_column: id", "chunk_count: 4"],
    );
    let said = rig.run_ok_capture();
    let parts = rig.manifest_parts().len();
    assert_eq!(
        parts, 4,
        "fixture: {N} ids range-chunked with `chunk_count: 4` are four parts, the manifest names {parts}\n{said}"
    );
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
    let export = rig.export_name().to_string();
    state_schema_newer(rig, &export);
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

/// A plain re-run, which resumes an unfinished checkpointed run.
const RERUN: &[&str] = &["run"];
/// The explicit resume of an unfinished checkpointed run.
const RESUME: &[&str] = &["run", "--resume"];

/// Edit the checkpoint of an unfinished run; `argv` then delivers the source or refuses, and a delivered export validates.
fn continued_after(engine: SqlEngine, argv: &[&str], sabotage: impl FnOnce(&Rig)) {
    let (_table, rig, _guard) = interrupted(engine, "sab_resume");
    sabotage(&rig);
    if rig.delivers_or_refuses(argv, &[]) == Survived::Delivered {
        let ids = declared_ids(&rig);
        let distinct: std::collections::BTreeSet<i64> = ids.iter().copied().collect();
        assert!(
            argv != RESUME || ids == (1..=N).collect::<Vec<_>>(),
            "`rivet {}` exited 0 over a success manifest declaring {} row(s) and {} of {N} source ids",
            argv.join(" "),
            ids.len(),
            distinct.len()
        );
        ok(rig.cli(&["validate"]));
    }
}

/// Sorted ids of the parts the success manifests of `rig`'s destination declare: what an explicit resume is held to, since the default oracle grades it on the rows it delivered.
fn declared_ids(rig: &Rig) -> Vec<i64> {
    let mut ids: Vec<i64> = rig
        .read_declared_parts()
        .iter()
        .flat_map(|b| {
            let schema = b.schema();
            let i = schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case("id"))
                .expect("an id column");
            let col = b.column(i).clone();
            let col = col
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("a 64-bit id");
            col.values().to_vec()
        })
        .collect();
    ids.sort();
    ids
}

/// [`continued_after`] by a plain re-run.
fn resume_after(engine: SqlEngine, sabotage: impl FnOnce(&Rig)) {
    continued_after(engine, RERUN, sabotage);
}

/// A pending chunk task marked `completed` in the checkpoint.
fn task_marked_completed(engine: SqlEngine, argv: &[&str]) {
    continued_after(engine, argv, |rig| {
        rig.edit_state(
            &format!(
                "UPDATE chunk_task SET status = 'completed' WHERE chunk_index = 5 AND {ITS_TASKS}"
            ),
            1,
        )
    });
}

/// A pending chunk task whose upper bound was lowered, leaving a hole between two tasks.
fn task_bounds_edited(engine: SqlEngine, argv: &[&str]) {
    continued_after(engine, argv, |rig| {
        rig.edit_state(
            &format!("UPDATE chunk_task SET end_key = '33' WHERE chunk_index = 6 AND {ITS_TASKS}"),
            1,
        )
    });
}

/// Every chunk task of the unfinished run deleted, its `chunk_run` row kept.
fn tasks_deleted(engine: SqlEngine, argv: &[&str]) {
    continued_after(engine, argv, |rig| {
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
fn committed_part_truncated(engine: SqlEngine, argv: &[&str]) {
    continued_after(engine, argv, |rig| {
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
fn committed_part_replaced(engine: SqlEngine, argv: &[&str]) {
    continued_after(engine, argv, |rig| {
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

// takeaway_* and beside_*: a live run of each shape, met mid-export

/// Rows of a table a slowed run reads for about three seconds.
const LIVE_ROWS: i64 = 200;
/// Documents of a MongoDB collection whose export stays alive long enough to be met.
const MONGO_LIVE: i64 = 300_000;

/// The batch shapes a live run is met in.
#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Full,
    Incremental,
    Range,
    RangeCheckpoint,
    Keyset,
    KeysetCheckpoint,
    RangeCheckpointParallel,
    KeysetCheckpointParallel,
}

impl Shape {
    /// `rig` staged in this shape on `engine`.
    fn staged(self, engine: SqlEngine, rig: Rig) -> Rig {
        let (mode, lines): (&str, &[&str]) = match self {
            Shape::Full => ("full", &[]),
            Shape::Incremental => ("incremental", &["cursor_column: id"]),
            Shape::Range => ("chunked", &["chunk_column: id", "chunk_count: 10"]),
            Shape::RangeCheckpoint => (
                "chunked",
                &[
                    "chunk_column: id",
                    "chunk_size: 20",
                    "chunk_checkpoint: true",
                ],
            ),
            Shape::Keyset => ("chunked", &["chunk_by_key: id", "chunk_size: 20"]),
            Shape::KeysetCheckpoint => (
                "chunked",
                &[
                    "chunk_by_key: id",
                    "chunk_size: 20",
                    "chunk_checkpoint: true",
                ],
            ),
            Shape::RangeCheckpointParallel => (
                "chunked",
                &[
                    "chunk_column: id",
                    "chunk_size: 20",
                    "chunk_checkpoint: true",
                    "parallel: 2",
                ],
            ),
            Shape::KeysetCheckpointParallel => (
                "chunked",
                &[
                    "chunk_by_key: id",
                    "chunk_size: 20",
                    "chunk_checkpoint: true",
                    "parallel: 2",
                ],
            ),
        };
        engine.staged(rig, mode, lines)
    }

    /// Whether a live run of this shape is mid-export: a part committed, or (one part at the end) at once.
    fn mid_run(self, rig: &Rig) -> bool {
        matches!(self, Shape::Full | Shape::Incremental) || rig.has_a_part()
    }
}

/// A rig whose run a cell stops mid-export: what that run may leave for the next one, and what `also` (a keyset-checkpoint run's page high-water, its resume anchor in `export_state`; the schema a run observed before its state store filled up).
fn stopped_mid_run(rig: Rig, also: &[Leftover]) -> Rig {
    let mut leaves = vec![
        Leftover::OrphanPart,
        Leftover::FileLog,
        Leftover::ChunkCheckpoint,
    ];
    leaves.extend_from_slice(also);
    rig.a_failed_run_may_leave(
        &leaves,
        "the cell takes a resource away mid-export: the parts written and the checkpoint rows recorded before it stay for the next run",
    )
}

/// An export of a fresh `LIVE_ROWS`-row table in `shape`: the table, the rig and the drop guard.
fn shaped(engine: SqlEngine, tag: &str, shape: Shape) -> (String, Rig, Box<dyn std::any::Any>) {
    engine.alive();
    let (table, guard) = engine.range_table(tag);
    engine.insert(&table, 1..=LIVE_ROWS, 180, Some(10));
    let rig = shape.staged(engine, engine.rig(&table));
    (table, rig, guard)
}

/// [`shaped`], slowed so its run can be met, and declared as a run a cell stops mid-export.
fn live(engine: SqlEngine, tag: &str, shape: Shape) -> (String, Rig, Box<dyn std::any::Any>) {
    let (table, rig, guard) = shaped(engine, tag, shape);
    let anchor: &[Leftover] = match shape {
        Shape::KeysetCheckpoint => &[Leftover::ResumePoint],
        _ => &[],
    };
    (table, stopped_mid_run(rig, anchor).slowed(150), guard)
}

/// A resumable export of `MONGO_LIVE` documents of a fresh database on the standalone MongoDB.
fn mongo_live(tag: &str) -> (Rig, MongoTest, MongoDbGuard) {
    mongo_live_leaving(tag, &[])
}

/// [`mongo_live`] whose stopped run may `also` leave what [`stopped_mid_run`] names.
fn mongo_live_leaving(tag: &str, also: &[Leftover]) -> (Rig, MongoTest, MongoDbGuard) {
    require_alive(LiveService::Mongo);
    let db = unique_name(tag);
    let guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    let m = MongoTest::connect(MONGO_PORT, &db);
    m.seed_int_id("t", MONGO_LIVE);
    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(MONGO_PORT, &db))
        .mongo("page_size: 20000, resume: true");
    (stopped_mid_run(rig, also), m, guard)
}

/// With the resource back, a plain run delivers the source and the export validates.
fn recovers(rig: &Rig, envs: &[(&str, &str)]) {
    ok(rig.run_with_envs(envs));
    ok(rig.cli_env(&["validate"], envs));
}

/// A live run of `rig` loses a resource through `take`, which hands back what gives it back: the run delivers or fails loudly, so does every run while the resource is away, and with it back a run delivers the source.
fn taken_away<G>(
    rig: &Rig,
    envs: &[(&str, &str)],
    ready: impl Fn(&Rig) -> bool,
    take: impl FnOnce(&Rig) -> G,
    give_back: impl FnOnce(G),
) {
    let met = rig.beside_a_live_run(envs, ready, take);
    rig.delivered_or_failed_loudly("the run a resource was taken from", &met.run);
    for _ in 0..2 {
        rig.delivers_or_fails_loudly(&["run"], envs);
    }
    give_back(met.acted);
    recovers(rig, envs);
}

/// Every session of the login a live run reads as (a login the cell creates) killed on the server.
fn source_session_killed(engine: SqlEngine, shape: Shape) {
    let (table, rig, _guard) = live(engine, "sab_kill", shape);
    let reader = engine.reader(&table);
    let rig = rig.source_url(reader.url());
    let met = rig.beside_a_live_run(&[], |r| shape.mid_run(r), |_| reader.kill_sessions());
    rig.delivered_or_failed_loudly("the run whose source session was killed", &met.run);
    recovers(&rig, &[]);
}

/// The cursor of a live MongoDB export killed on the server.
fn source_session_killed_mongo() {
    let (rig, m, _guard) = mongo_live("sab_kill");
    let met = rig.beside_a_live_run(&[], Rig::has_a_part, |_| {
        let killed = m.kill_readers("t");
        assert!(
            killed > 0,
            "sabotage: no cursor on the collection appeared to kill"
        );
    });
    rig.delivered_or_failed_loudly("the run whose cursor was killed", &met.run);
    recovers(&rig, &[]);
}

/// The local destination made read-only once the live run has committed a part.
fn destination_read_only(rig: Rig) {
    taken_away(
        &rig,
        &[],
        Rig::has_a_part,
        |r| r.read_only(Local::Destination),
        ReadOnly::restore,
    );
}

fn destination_read_only_sql(engine: SqlEngine, shape: Shape) {
    let (_table, rig, _guard) = live(engine, "sab_rodest", shape);
    destination_read_only(rig);
}

fn destination_read_only_mongo() {
    let (rig, _m, _guard) = mongo_live("sab_rodest");
    destination_read_only(rig);
}

/// Whether this pass grades the shared Postgres state, where a cell on the SQLite state's own files (`what`) records a skip.
fn not_sqlite_state(what: &str) -> bool {
    let shared = state_url_under_test().is_some();
    if shared {
        skip_live(&format!(
            "{what}; this pass grades Postgres state (RIVET_GATE_STATE_URL)"
        ));
    }
    shared
}

/// The SQLite state files made read-only once the live run of `rig` has opened them and is `mid_run`.
fn state_read_only(rig: Rig, mid_run: impl Fn(&Rig) -> bool) {
    let opened =
        |r: &Rig| mid_run(r) && r.config_path().with_file_name(".rivet_state.db").is_file();
    taken_away(
        &rig,
        &[],
        opened,
        |r| r.read_only(Local::State),
        ReadOnly::restore,
    );
}

fn state_read_only_sql(engine: SqlEngine, shape: Shape) {
    if not_sqlite_state("a read-only SQLite state file") {
        return;
    }
    let (_table, rig, _guard) = live(engine, "sab_rostate", shape);
    state_read_only(rig, |r| shape.mid_run(r));
}

fn state_read_only_mongo() {
    if not_sqlite_state("a read-only SQLite state file") {
        return;
    }
    let (rig, _m, _guard) = mongo_live("sab_rostate");
    state_read_only(rig, Rig::has_a_part);
}

/// The reader's SELECT on the table revoked once the live run has committed a part.
fn select_revoked(engine: SqlEngine, shape: Shape) {
    let (table, rig, _guard) = live(engine, "sab_revoke", shape);
    let reader = engine.reader(&table);
    let rig = rig.source_url(reader.url());
    let met = rig.beside_a_live_run(&[], Rig::has_a_part, |_| reader.revoke());
    rig.delivered_or_failed_loudly("the run whose SELECT was revoked", &met.run);
    for _ in 0..2 {
        let again = rig.delivers_or_fails_loudly(&["run"], &[]);
        assert!(
            matches!(again, Stopped::Failed(_)),
            "a run with no SELECT on its table exited 0"
        );
    }
    reader.grant();
    recovers(&rig, &[]);
}

/// The object stores a bucket is removed from under a live run.
#[derive(Clone, Copy)]
enum Store {
    S3,
    Gcs,
    Azure,
}

/// The bucket of a live range-checkpoint export removed once it holds an object, then created again empty.
fn bucket_removed(store: Store) {
    let engine = SqlEngine::Pg;
    let (_table, rig, _guard) = live(engine, "sab_bucket", Shape::RangeCheckpoint);
    let bucket = unique_name("sab-bucket").replace('_', "-");
    let b = bucket.as_str();
    let (rig, envs): (Rig, Vec<(&str, &str)>) = match store {
        Store::S3 => {
            ensure_minio_bucket(b);
            (
                rig.dest_s3(b, "p", MINIO_ENDPOINT),
                vec![
                    ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
                    ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
                    ("AWS_EC2_METADATA_DISABLED", "true"),
                ],
            )
        }
        Store::Gcs => {
            ensure_gcs_bucket(b);
            (rig.dest_gcs(b, "p", FAKE_GCS_ENDPOINT), Vec::new())
        }
        Store::Azure => {
            ensure_azure_container(b);
            (
                rig.dest_azure(b, "p"),
                vec![("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)],
            )
        }
    };
    let holds_an_object = |_: &Rig| match store {
        Store::S3 => !minio_object_names(b, "p").is_empty(),
        Store::Gcs => !fake_gcs_names(b, "p").is_empty(),
        Store::Azure => !azure_blob_names(b, "p").is_empty(),
    };
    let remove = |_: &Rig| match store {
        Store::S3 => remove_minio_bucket(b),
        Store::Gcs => remove_gcs_bucket(b),
        Store::Azure => remove_azure_container(b),
    };
    let create = |()| match store {
        Store::S3 => ensure_minio_bucket(b),
        Store::Gcs => ensure_gcs_bucket(b),
        Store::Azure => ensure_azure_container(b),
    };
    taken_away(&rig, &envs, holds_an_object, remove, create);
    remove(&rig);
}

/// The Postgres state database of a live range-checkpoint export closed to connections, its sessions ended, then opened again.
fn postgres_state_cut_off() {
    let (_table, rig, _guard) = live(SqlEngine::Pg, "sab_pgstate", Shape::RangeCheckpoint);
    let db = ScratchStateDb::new("sab_state");
    let url = db.url();
    let envs = [("RIVET_STATE_URL", url.as_str())];
    taken_away(
        &rig,
        &envs,
        Rig::has_a_part,
        |_| db.cut_off(),
        |()| db.let_in(),
    );
}

/// RIVET_STATE_CHUNK_CHECKPOINT_GONE: a trigger in the SQLite state removes a chunk run's rows when its second task completes.
fn chunk_checkpoint_gone(engine: SqlEngine) {
    if not_sqlite_state("a SQLite state one cell arms with a trigger") {
        return;
    }
    let (table, _guard) = seeded(engine, "sab_gone");
    let rig = engine.staged(engine.rig(&table), "chunked", RANGE_CHECKPOINT);
    rig.run_ok();
    let mut rig = stopped_mid_run(rig, &[]);
    rig.edit_state(
        "CREATE TRIGGER sab_checkpoint_gone AFTER UPDATE OF status ON chunk_task \
         WHEN NEW.status = 'completed' AND NEW.chunk_index = 1 BEGIN \
         DELETE FROM chunk_task WHERE run_id = NEW.run_id; \
         DELETE FROM chunk_run WHERE run_id = NEW.run_id; END",
        0,
    );
    let refused = Refused::by_code("RIVET_STATE_CHUNK_CHECKPOINT_GONE", 5);
    rig.refuses_twice_and_walks_out(
        &["run"],
        &[],
        refused,
        vec![
            Remedy::new(
                "Run the export again: it starts a new chunk run",
                Then::DeliversTheSource,
                |r| r.edit_state("DROP TRIGGER sab_checkpoint_gone", 0),
            ),
            Remedy::wrong(
                "runs `rivet state reset-chunks` while what removes the rows is still there",
                Then::Refuses(refused),
                |r| state(r, "reset-chunks", &table),
            ),
        ],
    );
}

/// RIVET_STATE_RUN_IN_PROGRESS: `rivet state reset` beside a live checkpointed run of `rig`'s export, walked out by waiting and by stopping the process.
fn run_in_progress(live_rig: impl Fn() -> (String, Rig, Box<dyn std::any::Any>)) {
    let refused = Refused::by_code("RIVET_STATE_RUN_IN_PROGRESS", 5);
    for stop in [false, true] {
        let (export, mut rig, _guard) = live_rig();
        let twin = rig.twin();
        let mut run = twin.spawn_mid_run(&[], Rig::has_a_part);
        let pid = run.id();
        let reset = ["state", "reset", "--export", export.as_str()];
        let sibling = ["state", "reset-chunks", "--export", export.as_str()];
        let remedies = if stop {
            vec![
                Remedy::wrong(
                    "asks for stuck checkpoints to be cleared, which the live run's is not",
                    Then::DeliversTheSource,
                    |_| {},
                )
                .rerun_as(&["state", "reset-chunks", "--stuck-checkpoints"]),
                Remedy::new(
                    "or stop that process (its pid ends the run id), then repeat this command",
                    Then::DeliversTheSource,
                    |_| {
                        run.kill().expect("stop the live run");
                        assert!(!run.wait().expect("reap").success());
                    },
                )
                .in_place(),
            ]
        } else {
            vec![
                Remedy::wrong(
                    "runs `rivet state reset-chunks`, the sibling command, beside the live run",
                    Then::Refuses(refused),
                    |_| {},
                )
                .rerun_as(&sibling),
                Remedy::new("Wait for it to finish", Then::DeliversTheSource, |_| {
                    assert!(run.wait().expect("reap").success(), "the live run finishes");
                })
                .in_place(),
            ]
        };
        let said = rig.refuses_twice_and_walks_out(&reset, &[], refused, remedies);
        assert!(
            said.contains(&format!("_{pid}' is in progress")),
            "the refusal names a run id that ends in the live run's pid {pid}\n{said}"
        );
        recovers(&rig, &[]);
    }
}

/// A live resumable MongoDB export as [`live`] hands one back: its export name, rig and drop guard.
fn mongo_live_export(tag: &str) -> (String, Rig, Box<dyn std::any::Any>) {
    let (rig, _m, guard) = mongo_live(tag);
    (rig.export_name().to_string(), rig, Box::new(guard))
}

fn run_in_progress_sql(engine: SqlEngine) {
    run_in_progress(|| live(engine, "sab_live", Shape::RangeCheckpoint));
}

fn run_in_progress_mongo() {
    run_in_progress(|| mongo_live_export("sab_live"));
}

/// The lease file of a live checkpointed run deleted, then `rivet state reset`: each command delivers or refuses by code, and a run then delivers the source.
fn lease_file_removed(live_rig: impl FnOnce() -> (String, Rig, Box<dyn std::any::Any>)) {
    if not_sqlite_state(
        "the lease file of a SQLite state (a Postgres state's lease is an advisory lock)",
    ) {
        return;
    }
    let (table, rig, _guard) = live_rig();
    let met = rig.beside_a_live_run(&[], Rig::has_a_part, |r| {
        r.delete_lease_files();
        r.cli(&["state", "reset", "--export", &table])
    });
    rig.delivered_or_refused(
        "`rivet state reset` after the lease file was deleted",
        &met.acted,
    );
    rig.delivered_or_refused("the live run whose lease file was deleted", &met.run);
    recovers(&rig, &[]);
}

/// A second `rivet run` 700 ms into a live one of the same export (never in its first millisecond: docs/sabotage-matrix.yaml beside_run_same_instant): each delivers or refuses by code, and a run then delivers the source.
fn second_run_beside(engine: SqlEngine, shape: Shape) {
    let (_table, rig, _guard) = live(engine, "sab_two", shape);
    let met = rig.beside_a_live_run(
        &[],
        |r| shape.mid_run(r),
        |r| {
            std::thread::sleep(std::time::Duration::from_millis(700));
            r.run()
        },
    );
    rig.delivered_or_refused("the second run beside a live one", &met.acted);
    rig.delivered_or_refused("the live run a second one met", &met.run);
    recovers(&rig, &[]);
}

/// `rivet state reset` and `rivet state reset-chunks` beside a live incremental run that continues a stored cursor.
fn state_reset_beside(engine: SqlEngine) {
    let (table, _guard) = seeded(engine, "sab_reset");
    let rig = Shape::Incremental.staged(engine, engine.rig(&table));
    rig.run_ok();
    engine.insert(&table, N + 1..=N + LIVE_ROWS, 170, Some(10));
    let rig = rig.slowed(150);
    let met = rig.beside_a_live_run(
        &[],
        |_| true,
        |r| {
            [
                r.cli(&["state", "reset", "--export", &table]),
                r.cli(&["state", "reset-chunks", "--export", &table]),
            ]
        },
    );
    for out in &met.acted {
        rig.delivered_or_refused("a state command beside a live incremental run", out);
    }
    rig.delivered_or_refused("the live run a state command met", &met.run);
    recovers(&rig, &[]);
}

/// One command beside a live checkpointed run that continues a complete export: it delivers, refuses by code or fails verification; the run delivers or refuses by code; a run then delivers the source.
fn command_beside(engine: SqlEngine, tag: &str, command: &[&str]) {
    let (table, rig, _guard) = live(engine, tag, Shape::RangeCheckpoint);
    let argv: Vec<&str> = command
        .iter()
        .map(|a| if *a == "{export}" { table.as_str() } else { *a })
        .collect();
    let met = rig.beside_a_live_run(&[], Rig::has_a_part, |r| r.cli(&argv));
    let what = format!("`rivet {}` beside a live run", command.join(" "));
    rig.delivered_or_failed_loudly(&what, &met.acted);
    rig.delivered_or_refused("the live run a command met", &met.run);
    recovers(&rig, &[]);
}

// kill_*: the process killed at a named point of each runner

/// Crash `rig`'s run at `point` (a fault hook of src/test_hook.rs: a panic, or a hard exit of a parallel worker).
fn crashed_at(rig: &Rig, point: &str) {
    let crash = rig.run_with_env("RIVET_TEST_PANIC_AT", point);
    assert!(
        matches!(crash.status.code(), Some(101 | 70)),
        "fixture: the run crashes at `{point}`, it ended with {:?}\n{}",
        crash.status.code(),
        String::from_utf8_lossy(&crash.stderr)
    );
}

/// Between a crash at `point` and its recovery every state command answers, and `validate` passes or fails loudly.
fn state_answers(rig: &Rig, point: &str) {
    let export = rig.export_name().to_string();
    for argv in [
        vec!["state", "show"],
        vec!["state", "chunks", "--export", export.as_str()],
        vec!["state", "runs"],
    ] {
        let out = rig.cli(&argv);
        assert!(
            out.status.success(),
            "after a crash at `{point}` `rivet {}` did not answer\n{}",
            argv.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    rig.delivers_or_fails_loudly(&["validate"], &[]);
}

/// The process killed at each of `points` of a run in `shape`: the state commands answer, then a plain run delivers the source and the export validates.
fn killed_at(engine: SqlEngine, shape: Shape, points: &[&str]) {
    for point in points {
        let (_table, rig, _guard) = shaped(engine, "sab_killed", shape);
        crashed_at(&rig, point);
        state_answers(&rig, point);
        recovers(&rig, &[]);
    }
}

/// [`killed_at`] for a `mode: full` MongoDB export.
fn killed_at_mongo(points: &[&str]) {
    for point in points {
        let (rig, _guard) = mongo_rig("sab_killed");
        crashed_at(&rig, point);
        state_answers(&rig, point);
        recovers(&rig, &[]);
    }
}

/// After a crash and a source that moved on, the next run delivers the source as it is now or refuses by code, and a delivered export validates.
fn recovers_to_the_source_as_it_is(rig: &Rig) {
    if rig.delivers_or_refuses(RERUN, &[]) == Survived::Delivered {
        ok(rig.cli(&["validate"]));
    }
}

/// A checkpointed run in `shape` crashed at `point`, then three more source rows.
fn killed_then_source_grows(engine: SqlEngine, shape: Shape, point: &str) {
    let (table, rig, _guard) = shaped(engine, "sab_grows", shape);
    crashed_at(&rig, point);
    engine.insert(&table, LIVE_ROWS + 1..=LIVE_ROWS + 3, 170, Some(10));
    recovers_to_the_source_as_it_is(&rig);
}

/// A range-checkpoint run of an empty table crashed after its one task completed, then `N` source rows.
fn killed_over_an_empty_table_then_source_fills(engine: SqlEngine) {
    engine.alive();
    let (table, _guard) = engine.range_table("sab_fills");
    let rig = engine.staged(engine.rig(&table), "chunked", RANGE_CHECKPOINT);
    crashed_at(&rig, "after_chunk_complete:0");
    engine.insert(&table, 1..=N, 180, Some(10));
    recovers_to_the_source_as_it_is(&rig);
}

/// The first `chunk_task` insert of a chunk run fails (a trigger in the SQLite state): the run fails loudly twice and leaves no unfinished chunk run; with the trigger dropped a run delivers the source.
fn chunk_run_open_fails(engine: SqlEngine) {
    if not_sqlite_state("a SQLite state one cell arms with a trigger") {
        return;
    }
    let (table, _guard) = seeded(engine, "sab_open");
    let rig = engine.staged(engine.rig(&table), "chunked", RANGE_CHECKPOINT);
    rig.run_ok();
    rig.edit_state(
        "CREATE TRIGGER sab_open_fails BEFORE INSERT ON chunk_task \
         BEGIN SELECT RAISE(FAIL, 'sabotage: the state does not take this row'); END",
        0,
    );
    for _ in 0..2 {
        let how = rig.delivers_or_fails_loudly(&["run"], &[]);
        assert!(
            matches!(how, Stopped::Failed(_)),
            "a run whose chunk tasks the state did not take exited 0"
        );
        rig.edit_state(
            "DELETE FROM chunk_run WHERE export_name = '{export}' AND status = 'in_progress'",
            0,
        );
    }
    rig.edit_state("DROP TRIGGER sab_open_fails", 0);
    recovers(&rig, &[]);
}

// edit_*: a config edited between two runs, with no reset

/// One edit of an export's config between two runs.
#[derive(Clone, Copy, Debug)]
enum ConfigEdit {
    DestinationPath,
    Format,
    Compression,
}

/// `rig` after `edit`; a moved destination is `fresh`.
fn edited(rig: Rig, edit: ConfigEdit, fresh: &std::path::Path) -> Rig {
    match edit {
        ConfigEdit::DestinationPath => rig.dest_path(fresh.to_path_buf()),
        ConfigEdit::Format => rig.with_format("csv"),
        ConfigEdit::Compression => rig.export_line("compression: gzip"),
    }
}

/// After `edit` with no reset the run delivers the source or refuses by code; a delivery declares every one of `rows` source ids once where the config now points (parquet parts; a destination in another format is the default oracle's to read), and `validate` passes.
fn delivers_whole_or_refuses(rig: &Rig, edit: ConfigEdit, rows: i64) {
    if let Survived::Refused(_) = rig.delivers_or_refuses(RERUN, &[]) {
        return;
    }
    if !matches!(edit, ConfigEdit::Format) {
        let ids = declared_ids(rig);
        let distinct: std::collections::BTreeSet<i64> = ids.iter().copied().collect();
        assert!(
            ids == (1..=rows).collect::<Vec<_>>(),
            "after {edit:?} was edited `rivet run` exited 0 over a destination that declares {} row(s) and {} of {rows} source ids",
            ids.len(),
            distinct.len()
        );
    }
    ok(rig.cli(&["validate"]));
}

/// An incremental export that ran once, then `edit` and three more source rows.
fn edited_incremental(engine: SqlEngine, edit: ConfigEdit) {
    let (table, _guard) = seeded(engine, "sab_edit");
    let rig = Shape::Incremental.staged(engine, engine.rig(&table));
    rig.run_ok();
    engine.insert(&table, N + 1..=N + 3, 170, Some(10));
    let fresh = tempfile::tempdir().unwrap();
    let rig = edited(rig, edit, fresh.path());
    delivers_whole_or_refuses(&rig, edit, N + 3);
}

/// An unfinished range-checkpoint run, then `edit`.
fn edited_unfinished(engine: SqlEngine, edit: ConfigEdit) {
    let (_table, rig, _guard) = interrupted(engine, "sab_edit");
    let fresh = tempfile::tempdir().unwrap();
    let rig = edited(rig, edit, fresh.path());
    delivers_whole_or_refuses(&rig, edit, N);
}

// versions_*: the previous release and this build on one state and one prefix

/// This rig, its twin that runs the previous release, and a state database of the cell's own when the pass grades the shared Postgres state (which the old binary must not migrate back and forth under other cells).
fn two_versions(rig: Rig) -> Option<(Rig, Rig, Option<ScratchStateDb>)> {
    let old = rig.as_previous_release()?;
    let version = |r: &Rig| String::from_utf8_lossy(&r.cli(&["--version"]).stdout).to_string();
    assert_ne!(
        version(&old),
        version(&rig),
        "fixture: the previous release answers `--version` as this build does"
    );
    let state = state_url_under_test().map(|_| ScratchStateDb::new("sab_versions"));
    Some((rig, old, state))
}

/// The state URL of a cell's own state database, as the environment of each invocation.
fn on_state(url: &Option<String>) -> Vec<(&str, &str)> {
    url.iter()
        .map(|u| ("RIVET_STATE_URL", u.as_str()))
        .collect()
}

/// This build runs first, then the previous release on the same state and prefix (a downgrade): it delivers the source or refuses by code, and this build then delivers the source.
fn new_then_old(rig: Rig, grow: impl Fn(i64)) {
    let Some((new, old, state)) = two_versions(rig) else {
        return;
    };
    let url = state.as_ref().map(ScratchStateDb::url);
    let envs = on_state(&url);
    ok(new.run_with_envs(&envs));
    grow(1);
    old.delivers_or_refuses(&["run"], &envs);
    grow(2);
    recovers(&new, &envs);
}

/// The two binaries take turns on one export, the previous release first (a job still pinned to it during a rolling upgrade): it delivers before this build has run, each later turn delivers the source or refuses by code, and this build delivers last.
fn versions_alternate(rig: Rig, grow: impl Fn(i64)) {
    let Some((new, old, state)) = two_versions(rig) else {
        return;
    };
    let url = state.as_ref().map(ScratchStateDb::url);
    let envs = on_state(&url);
    ok(old.run_with_envs(&envs));
    grow(1);
    recovers(&new, &envs);
    grow(2);
    old.delivers_or_refuses(&["run"], &envs);
    grow(3);
    recovers(&new, &envs);
}

/// This build starts 700 ms into a live run of the previous release on a state the old binary created: each delivers the source or refuses by code, and this build then delivers the source.
fn new_beside_a_live_old_run(rig: Rig) {
    let Some((new, old, state)) = two_versions(rig) else {
        return;
    };
    let url = state.as_ref().map(ScratchStateDb::url);
    let envs = on_state(&url);
    let met = old.beside_a_live_run(
        &envs,
        |_| true,
        |_| {
            std::thread::sleep(std::time::Duration::from_millis(700));
            new.run_with_envs(&envs)
        },
    );
    new.delivered_or_refused(
        "this build beside a live run of the previous release",
        &met.acted,
    );
    old.delivered_or_refused("the live run of the previous release", &met.run);
    recovers(&new, &envs);
}

/// An incremental export of a fresh `N`-row table, and what adds three rows to it per step.
fn growing(engine: SqlEngine, tag: &str) -> (Rig, impl Fn(i64), Box<dyn std::any::Any>) {
    let (table, guard) = seeded(engine, tag);
    let rig = Shape::Incremental.staged(engine, engine.rig(&table));
    let grow = move |step: i64| {
        let from = N + 3 * (step - 1);
        engine.insert(&table, from + 1..=from + 3, 170, Some(10));
    };
    (rig, grow, guard)
}

fn new_then_old_sql(engine: SqlEngine) {
    let (rig, grow, _guard) = growing(engine, "sab_downgrade");
    new_then_old(rig, grow);
}

fn versions_alternate_sql(engine: SqlEngine) {
    let (rig, grow, _guard) = growing(engine, "sab_rolling");
    versions_alternate(rig, grow);
}

fn new_beside_a_live_old_run_sql(engine: SqlEngine) {
    let (_table, rig, _guard) = live(engine, "sab_oldlive", Shape::Incremental);
    new_beside_a_live_old_run(rig);
}

// exhaust_*: a resource that runs out under a live run

/// Two runs with the resource still exhausted both fail loudly.
fn fails_loudly_twice(rig: &Rig, what: &str) {
    for _ in 0..2 {
        let again = rig.delivers_or_fails_loudly(&["run"], &[]);
        assert!(matches!(again, Stopped::Failed(_)), "{what} exited 0");
    }
}

/// The volume under the local destination of a live run fills up once `mid_run`: the run delivers or fails loudly, every run into the full volume fails loudly, and with the space back a run delivers the source.
fn destination_fills_up(rig: Rig, mid_run: impl Fn(&Rig) -> bool) {
    let Some(volume) = TinyVolume::mounted(4) else {
        return;
    };
    let rig = rig.dest_path(volume.path().join("out"));
    let met = rig.beside_a_live_run(&[], mid_run, |_| volume.fill());
    rig.delivered_or_failed_loudly("the run whose destination filled up", &met.run);
    fails_loudly_twice(&rig, "a run into a destination with no space left");
    volume.free();
    recovers(&rig, &[]);
}

/// What a run whose state store filled up leaves beside a stopped run's parts.
const STATE_FULL: &[Leftover] = &[Leftover::ObservedSchema];

/// The volume under the SQLite state of a live run fills up once `mid_run`: the run delivers or fails loudly, so does every run while it is full, and with the space back a run delivers the source.
fn state_volume_fills_up(rig: Rig, mid_run: impl Fn(&Rig) -> bool) {
    let Some(volume) = TinyVolume::mounted(4) else {
        return;
    };
    let dir = volume.path().join("cfg");
    std::fs::create_dir(&dir).expect("mkdir the config directory");
    let rig = rig.config_dir(dir);
    taken_away(&rig, &[], mid_run, |_| volume.fill(), |()| volume.free());
}

fn state_volume_fills_up_sql(engine: SqlEngine) {
    if not_sqlite_state("a SQLite state on a volume that fills up") {
        return;
    }
    let (_table, rig, _guard) = shaped(engine, "sab_statefull", Shape::RangeCheckpoint);
    state_volume_fills_up(
        stopped_mid_run(rig, STATE_FULL).slowed(150),
        Rig::has_a_part,
    );
}

/// The most open files a capped run gets: stdin, stdout, stderr and five more, fewer than a run needs for its config, state, source and part.
const FEW_FILES: u64 = 8;

/// Every run capped at `FEW_FILES` open files fails loudly, never by a crash, and a run without the cap then delivers the source.
fn out_of_file_descriptors(rig: Rig) {
    let capped = rig.twin().open_files(FEW_FILES);
    fails_loudly_twice(&capped, "a run capped below the files it needs");
    recovers(&rig, &[]);
}

/// [`out_of_file_descriptors`] on the SQLite state: on a Postgres state the state client is the PostgreSQL client, whose crash under the cap is the PostgreSQL cell's open defect.
fn out_of_file_descriptors_on_sqlite_state(rig: Rig) {
    if not_sqlite_state(
        "a run out of file descriptors whose only PostgreSQL client would be its state",
    ) {
        return;
    }
    out_of_file_descriptors(rig);
}

// ddl_*: the source table changed under a live run

/// One change of the table a live run reads.
#[derive(Clone, Copy, PartialEq)]
enum Ddl {
    DropColumn,
    AlterColumnType,
    DropTable,
}

impl Ddl {
    /// The statement on `engine`.
    fn sql(self, engine: SqlEngine, table: &str) -> String {
        match (self, engine) {
            (Ddl::DropColumn, _) => format!("ALTER TABLE {table} DROP COLUMN time_spent"),
            (Ddl::DropTable, _) => format!("DROP TABLE {table}"),
            (Ddl::AlterColumnType, SqlEngine::Pg) => {
                format!("ALTER TABLE {table} ALTER COLUMN time_spent TYPE TEXT")
            }
            (Ddl::AlterColumnType, SqlEngine::Mysql) => {
                format!("ALTER TABLE {table} MODIFY time_spent VARCHAR(40) NULL")
            }
            (Ddl::AlterColumnType, SqlEngine::Mssql) => {
                format!("ALTER TABLE {table} ALTER COLUMN time_spent VARCHAR(40) NULL")
            }
            #[cfg(feature = "oracle")]
            (Ddl::AlterColumnType, SqlEngine::Oracle) => {
                unreachable!("ORA-01439: Oracle changes the type of an empty column only")
            }
        }
    }
}

/// `ddl` on the table of a live range-checkpoint run once it has committed a part, the statement back before the run ends: the run delivers or fails loudly; a dropped table fails every run loudly until it is there again with the rows it held; then a run delivers the source as it is now.
fn table_changed_under_it(engine: SqlEngine, ddl: Ddl) {
    let (table, rig, _guard) = live(engine, "sab_ddl", Shape::RangeCheckpoint);
    let (copy, _copy_guard) = engine.range_table("sab_ddl_copy");
    engine.exec(&format!("INSERT INTO {copy} SELECT * FROM {table}"));
    let met = rig.beside_a_live_run(&[], Rig::has_a_part, |_| {
        engine.exec(&ddl.sql(engine, &table))
    });
    let (run, ()) = met.answered_while_alive();
    rig.delivered_or_failed_loudly("the run whose table changed under it", &run);
    if ddl == Ddl::DropTable {
        fails_loudly_twice(&rig, "a run of a dropped table");
        engine.range_table_again(&table);
        engine.exec(&format!("INSERT INTO {table} SELECT * FROM {copy}"));
    }
    recovers(&rig, &[]);
}

/// The collection of a live MongoDB export dropped once it has committed a part, then seeded again with the documents it held: the run delivers or fails loudly, and a run then delivers the source.
fn collection_dropped_under_it() {
    let (rig, m, _guard) = mongo_live("sab_ddl");
    let met = rig.beside_a_live_run(&[], Rig::has_a_part, |_| m.drop_collection("t"));
    let (run, ()) = met.answered_while_alive();
    rig.delivered_or_failed_loudly("the run whose collection was dropped under it", &run);
    m.seed_int_id("t", MONGO_LIVE);
    recovers(&rig, &[]);
}

// clock_*: the state dated ahead of the host clock

/// A moment no run of this suite reaches.
const AHEAD: &str = "2099-01-01T00:00:00.000000+00:00";

/// Set `columns` of this export's rows in `table` to [`AHEAD`].
fn dated_ahead(rig: &Rig, table: &str, columns: &[&str]) {
    let set: Vec<String> = columns.iter().map(|c| format!("{c} = '{AHEAD}'")).collect();
    rig.edit_state(
        &format!(
            "UPDATE {table} SET {} WHERE export_name = '{{export}}'",
            set.join(", ")
        ),
        1,
    );
}

/// An incremental export ran once and every run its state records is dated ahead (the host clock stepped back, or the run was on a host whose clock ran fast); three more source rows: the run delivers the source or refuses by code, and a delivered export validates.
fn clock_back_incremental(engine: SqlEngine) {
    let (rig, grow, _guard) = growing(engine, "sab_clock");
    rig.run_ok();
    grow(1);
    dated_ahead(&rig, "run_status", &["started_at", "finished_at"]);
    dated_ahead(&rig, "export_metrics", &["run_at"]);
    recovers_to_the_source_as_it_is(&rig);
}

/// An unfinished range-checkpoint run whose `running` rows are dated ahead: the next run delivers the source or refuses by code, and a delivered export validates.
fn clock_back_unfinished(engine: SqlEngine) {
    let (_table, rig, _guard) = interrupted(engine, "sab_clock");
    dated_ahead(&rig, "run_status", &["started_at"]);
    dated_ahead(&rig, "export_metrics", &["run_at"]);
    dated_ahead(&rig, "chunk_run", &["created_at", "updated_at"]);
    recovers_to_the_source_as_it_is(&rig);
}

// hostile_*: values a naive reader mangles

/// How one run of a hostile value ended, and that the next run of it ends the same way: both deliver (the default oracle compares the value with the source, and the export validates) or both fail loudly.
fn delivered_or_failed_loudly_twice(rig: &Rig, what: &str) {
    let first = rig.delivers_or_fails_loudly(&["run"], &[]);
    let again = rig.delivers_or_fails_loudly(&["run"], &[]);
    let delivered = first == Stopped::Delivered;
    assert_eq!(
        delivered,
        again == Stopped::Delivered,
        "{what}: one run delivered and the next failed"
    );
    eprintln!(
        "hostile: {what}: {}",
        if delivered {
            "delivered"
        } else {
            "failed loudly"
        }
    );
    if delivered {
        ok(rig.cli(&["validate"]));
    }
}

/// Each `(column type, SQL literal)` alone in a table of its own, exported whole twice: delivered as the source holds it, or a loud failure both times.
fn hostile(engine: SqlEngine, values: &[(&str, &str)]) {
    engine.alive();
    for (column, value) in values {
        let columns = format!("id {} PRIMARY KEY, v {column}", engine.int64());
        let (table, _guard) = engine.create("sab_hostile", &columns);
        engine.exec(&format!("INSERT INTO {table} (id, v) VALUES (1, {value})"));
        let shown: String = value.chars().take(60).collect();
        delivered_or_failed_loudly_twice(&engine.rig(&table), &format!("{column} {shown}"));
    }
}

/// Each value alone in a collection of its own, exported whole twice: delivered as the source holds it, or a loud failure both times.
fn hostile_mongo(values: Vec<(&str, mongodb::bson::Bson)>) {
    require_alive(LiveService::Mongo);
    for (what, value) in values {
        let db = unique_name("sab_hostile");
        let _guard = MongoDbGuard {
            port: MONGO_PORT,
            db: db.clone(),
        };
        MongoTest::connect(MONGO_PORT, &db)
            .insert_many("t", vec![mongodb::bson::doc! { "_id": 1_i64, "v": value }]);
        let rig = Rig::mongo_batch("t").source_url(&MongoTest::url(MONGO_PORT, &db));
        delivered_or_failed_loudly_twice(&rig, what);
    }
}

/// A cell of `HUGE` bytes.
const HUGE: usize = 8 << 20;

/// Text with a NUL where the engine stores one, control characters, quotes and a backslash, characters outside the basic plane and right-to-left text, and the empty string.
fn hostile_text(engine: SqlEngine) -> Vec<(&'static str, &'static str)> {
    match engine {
        SqlEngine::Pg => vec![
            ("TEXT", r"E'a\tb\nc\rd'"),
            ("TEXT", r#"E'it''s "quoted" back\\slash'"#),
            ("TEXT", "'😀𝄞 שלום é'"),
            ("TEXT", "''"),
        ],
        SqlEngine::Mysql => vec![
            ("TEXT CHARACTER SET utf8mb4", "CONCAT('a', CHAR(0), 'b')"),
            (
                "TEXT CHARACTER SET utf8mb4",
                "CONCAT('a', CHAR(9), 'b', CHAR(10), 'c', CHAR(13), 'd')",
            ),
            (
                "TEXT CHARACTER SET utf8mb4",
                r#"CONCAT('it''s "quoted" back', CHAR(92), 'slash')"#,
            ),
            ("TEXT CHARACTER SET utf8mb4", "'😀𝄞 שלום é'"),
            ("TEXT CHARACTER SET utf8mb4", "''"),
        ],
        SqlEngine::Mssql => vec![
            ("NVARCHAR(MAX)", "N'a' + NCHAR(0) + N'b'"),
            (
                "NVARCHAR(MAX)",
                "N'a' + NCHAR(9) + N'b' + NCHAR(10) + N'c' + NCHAR(13) + N'd'",
            ),
            ("NVARCHAR(MAX)", r#"N'it''s "quoted" back\slash'"#),
            ("NVARCHAR(MAX)", "N'😀𝄞 שלום é'"),
            ("NVARCHAR(MAX)", "N''"),
        ],
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => vec![
            ("NVARCHAR2(200)", "'a' || CHR(0) || 'b'"),
            (
                "NVARCHAR2(200)",
                "'a' || CHR(9) || 'b' || CHR(10) || 'c' || CHR(13) || 'd'",
            ),
            ("NVARCHAR2(200)", r#"'it''s "quoted" back\slash'"#),
            (
                "NVARCHAR2(200)",
                r"UNISTR('\D83D\DE00\D834\DD1E \05E9\05DC\05D5\05DD \00E9')",
            ),
        ],
    }
}

/// Bytes that are not UTF-8, in a binary column and (where the engine has one) a single-byte text column.
fn hostile_bytes(engine: SqlEngine) -> Vec<(&'static str, &'static str)> {
    match engine {
        SqlEngine::Pg => vec![("BYTEA", r"'\x00fffe80'::bytea"), ("BYTEA", r"'\x'::bytea")],
        SqlEngine::Mysql => vec![
            ("VARBINARY(16)", "x'00FFFE80'"),
            ("VARBINARY(16)", "x''"),
            ("VARCHAR(16) CHARACTER SET latin1", "x'FFFE80'"),
        ],
        SqlEngine::Mssql => vec![
            ("VARBINARY(16)", "0x00FFFE80"),
            ("VARBINARY(16)", "0x"),
            ("VARCHAR(16)", "CHAR(255) + CHAR(254) + CHAR(128)"),
        ],
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => vec![("RAW(16)", "HEXTORAW('00FFFE80')")],
    }
}

/// One text cell of `HUGE` bytes.
fn hostile_huge(engine: SqlEngine) -> Vec<(&'static str, &'static str)> {
    match engine {
        SqlEngine::Pg => vec![("TEXT", "repeat('x', 8388608)")],
        SqlEngine::Mysql => vec![("LONGTEXT", "REPEAT('x', 8388608)")],
        SqlEngine::Mssql => vec![(
            "VARCHAR(MAX)",
            "REPLICATE(CAST('x' AS VARCHAR(MAX)), 8388608)",
        )],
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => {
            unreachable!("a CLOB past 4000 bytes takes a PL/SQL block, not one statement")
        }
    }
}

/// The ends of each numeric type, and the values that are not numbers.
fn hostile_numbers(engine: SqlEngine) -> Vec<(&'static str, &'static str)> {
    match engine {
        SqlEngine::Pg => vec![
            ("BIGINT", "-9223372036854775808"),
            ("BIGINT", "9223372036854775807"),
            ("NUMERIC(38,0)", "99999999999999999999999999999999999999"),
            (
                "NUMERIC(40,20)",
                "12345678901234567890.12345678901234567890",
            ),
            ("NUMERIC", "'NaN'"),
            ("DOUBLE PRECISION", "'NaN'"),
            ("DOUBLE PRECISION", "'Infinity'"),
            ("DOUBLE PRECISION", "'-Infinity'"),
            ("DOUBLE PRECISION", "1.7976931348623157e308"),
        ],
        SqlEngine::Mysql => vec![
            ("BIGINT", "-9223372036854775808"),
            ("BIGINT", "9223372036854775807"),
            ("BIGINT UNSIGNED", "18446744073709551615"),
            (
                "DECIMAL(65,0)",
                "99999999999999999999999999999999999999999999999999999999999999999",
            ),
            ("DOUBLE", "1.7976931348623157E308"),
        ],
        SqlEngine::Mssql => vec![
            ("BIGINT", "-9223372036854775808"),
            ("BIGINT", "9223372036854775807"),
            ("DECIMAL(38,0)", "99999999999999999999999999999999999999"),
            ("FLOAT", "1.79E308"),
            ("MONEY", "922337203685477.5807"),
        ],
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => vec![
            ("NUMBER(38)", "99999999999999999999999999999999999999"),
            ("NUMBER", "1E125"),
            ("BINARY_DOUBLE", "BINARY_DOUBLE_INFINITY"),
            ("BINARY_DOUBLE", "BINARY_DOUBLE_NAN"),
        ],
    }
}

/// The first and last moment each date type holds, and the values that are not moments.
fn hostile_dates(engine: SqlEngine) -> Vec<(&'static str, &'static str)> {
    match engine {
        SqlEngine::Pg => vec![
            ("DATE", "'0001-01-01'"),
            ("DATE", "'9999-12-31'"),
            ("DATE", "'infinity'"),
            ("TIMESTAMP", "'infinity'"),
            ("TIMESTAMP", "'-infinity'"),
            ("TIMESTAMP", "'294276-12-31 23:59:59.999999'"),
            ("TIMESTAMPTZ", "'0001-01-01 00:00:00+00'"),
        ],
        SqlEngine::Mysql => vec![
            ("DATE", "'1000-01-01'"),
            ("DATE", "'9999-12-31'"),
            ("DATETIME(6)", "'9999-12-31 23:59:59.999999'"),
            ("TIME", "'-838:59:59'"),
            ("YEAR", "1901"),
        ],
        SqlEngine::Mssql => vec![
            ("DATE", "'0001-01-01'"),
            ("DATE", "'9999-12-31'"),
            ("DATETIME2(7)", "'9999-12-31 23:59:59.9999999'"),
            ("DATETIMEOFFSET", "'0001-01-02 00:00:00 +14:00'"),
            ("DATETIME", "'1753-01-01'"),
        ],
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => vec![
            ("DATE", "DATE '9999-12-31'"),
            ("DATE", "DATE '-4712-01-01'"),
            ("TIMESTAMP(9)", "TIMESTAMP '9999-12-31 23:59:59.999999999'"),
        ],
    }
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
    task_marked_completed(SqlEngine::Pg, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_task_marked_completed_delivers_or_refuses_mysql() {
    task_marked_completed(SqlEngine::Mysql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_task_marked_completed_delivers_or_refuses_mssql() {
    task_marked_completed(SqlEngine::Mssql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_edited_task_bounds_delivers_or_refuses_postgres() {
    task_bounds_edited(SqlEngine::Pg, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_edited_task_bounds_delivers_or_refuses_mysql() {
    task_bounds_edited(SqlEngine::Mysql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_edited_task_bounds_delivers_or_refuses_mssql() {
    task_bounds_edited(SqlEngine::Mssql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_deleted_tasks_delivers_or_refuses_postgres() {
    tasks_deleted(SqlEngine::Pg, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_deleted_tasks_delivers_or_refuses_mysql() {
    tasks_deleted(SqlEngine::Mysql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_deleted_tasks_delivers_or_refuses_mssql() {
    tasks_deleted(SqlEngine::Mssql, RERUN);
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
    committed_part_truncated(SqlEngine::Pg, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_truncated_committed_part_delivers_or_refuses_mysql() {
    committed_part_truncated(SqlEngine::Mysql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_truncated_committed_part_delivers_or_refuses_mssql() {
    committed_part_truncated(SqlEngine::Mssql, RERUN);
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
    committed_part_replaced(SqlEngine::Pg, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_replaced_committed_part_delivers_or_refuses_mysql() {
    committed_part_replaced(SqlEngine::Mysql, RERUN);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_resume_over_a_replaced_committed_part_delivers_or_refuses_mssql() {
    committed_part_replaced(SqlEngine::Mssql, RERUN);
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

// cells: a resource taken away mid-run, two processes on one export, `--resume` over a damaged checkpoint

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_full_run_whose_source_session_is_killed_delivers_or_fails_loudly_postgres() {
    source_session_killed(SqlEngine::Pg, Shape::Full);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_full_run_whose_source_session_is_killed_delivers_or_fails_loudly_mysql() {
    source_session_killed(SqlEngine::Mysql, Shape::Full);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_full_run_whose_source_session_is_killed_delivers_or_fails_loudly_mssql() {
    source_session_killed(SqlEngine::Mssql, Shape::Full);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_full_run_whose_source_session_is_killed_delivers_or_fails_loudly_mongo() {
    source_session_killed_mongo();
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_full_run_whose_source_session_is_killed_delivers_or_fails_loudly_oracle() {
    source_session_killed(SqlEngine::Oracle, Shape::Full);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_incremental_run_whose_source_session_is_killed_delivers_or_fails_loudly_postgres() {
    source_session_killed(SqlEngine::Pg, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_incremental_run_whose_source_session_is_killed_delivers_or_fails_loudly_mysql() {
    source_session_killed(SqlEngine::Mysql, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_incremental_run_whose_source_session_is_killed_delivers_or_fails_loudly_mssql() {
    source_session_killed(SqlEngine::Mssql, Shape::Incremental);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_incremental_run_whose_source_session_is_killed_delivers_or_fails_loudly_oracle() {
    source_session_killed(SqlEngine::Oracle, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_run_whose_source_session_is_killed_delivers_or_fails_loudly_postgres() {
    source_session_killed(SqlEngine::Pg, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_run_whose_source_session_is_killed_delivers_or_fails_loudly_mysql() {
    source_session_killed(SqlEngine::Mysql, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_run_whose_source_session_is_killed_delivers_or_fails_loudly_mssql() {
    source_session_killed(SqlEngine::Mssql, Shape::Range);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_run_whose_source_session_is_killed_delivers_or_fails_loudly_oracle() {
    source_session_killed(SqlEngine::Oracle, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_postgres() {
    source_session_killed(SqlEngine::Pg, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_mysql() {
    source_session_killed(SqlEngine::Mysql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_mssql() {
    source_session_killed(SqlEngine::Mssql, Shape::RangeCheckpoint);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_oracle() {
    source_session_killed(SqlEngine::Oracle, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_keyset_run_whose_source_session_is_killed_delivers_or_fails_loudly_postgres() {
    source_session_killed(SqlEngine::Pg, Shape::Keyset);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_keyset_run_whose_source_session_is_killed_delivers_or_fails_loudly_mysql() {
    source_session_killed(SqlEngine::Mysql, Shape::Keyset);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_keyset_run_whose_source_session_is_killed_delivers_or_fails_loudly_mssql() {
    source_session_killed(SqlEngine::Mssql, Shape::Keyset);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_keyset_run_whose_source_session_is_killed_delivers_or_fails_loudly_oracle() {
    source_session_killed(SqlEngine::Oracle, Shape::Keyset);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_keyset_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_postgres() {
    source_session_killed(SqlEngine::Pg, Shape::KeysetCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_keyset_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_mysql() {
    source_session_killed(SqlEngine::Mysql, Shape::KeysetCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_keyset_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_mssql() {
    source_session_killed(SqlEngine::Mssql, Shape::KeysetCheckpoint);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_keyset_checkpoint_run_whose_source_session_is_killed_delivers_or_fails_loudly_oracle() {
    source_session_killed(SqlEngine::Oracle, Shape::KeysetCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_run_whose_destination_turns_read_only_delivers_or_fails_loudly_postgres() {
    destination_read_only_sql(SqlEngine::Pg, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_run_whose_destination_turns_read_only_delivers_or_fails_loudly_mysql() {
    destination_read_only_sql(SqlEngine::Mysql, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_run_whose_destination_turns_read_only_delivers_or_fails_loudly_mssql() {
    destination_read_only_sql(SqlEngine::Mssql, Shape::Range);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_run_whose_destination_turns_read_only_delivers_or_fails_loudly_oracle() {
    destination_read_only_sql(SqlEngine::Oracle, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_checkpoint_run_whose_destination_turns_read_only_delivers_or_fails_loudly_postgres() {
    destination_read_only_sql(SqlEngine::Pg, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_checkpoint_run_whose_destination_turns_read_only_delivers_or_fails_loudly_mysql() {
    destination_read_only_sql(SqlEngine::Mysql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_checkpoint_run_whose_destination_turns_read_only_delivers_or_fails_loudly_mssql() {
    destination_read_only_sql(SqlEngine::Mssql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_range_checkpoint_run_whose_destination_turns_read_only_delivers_or_fails_loudly_mongo() {
    destination_read_only_mongo();
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_checkpoint_run_whose_destination_turns_read_only_delivers_or_fails_loudly_oracle() {
    destination_read_only_sql(SqlEngine::Oracle, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn a_run_whose_s3_bucket_is_removed_delivers_or_fails_loudly_postgres() {
    bucket_removed(Store::S3);
}

#[test]
#[ignore = "live: requires docker compose postgres + fake-gcs"]
fn a_run_whose_gcs_bucket_is_removed_delivers_or_fails_loudly_postgres() {
    bucket_removed(Store::Gcs);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres + azurite; open defect (a removed Azure container is retried for minutes), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_whose_azure_bucket_is_removed_delivers_or_fails_loudly_postgres() {
    bucket_removed(Store::Azure);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_incremental_run_whose_state_turns_read_only_delivers_or_fails_loudly_postgres() {
    state_read_only_sql(SqlEngine::Pg, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_incremental_run_whose_state_turns_read_only_delivers_or_fails_loudly_mysql() {
    state_read_only_sql(SqlEngine::Mysql, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_incremental_run_whose_state_turns_read_only_delivers_or_fails_loudly_mssql() {
    state_read_only_sql(SqlEngine::Mssql, Shape::Incremental);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_incremental_run_whose_state_turns_read_only_delivers_or_fails_loudly_oracle() {
    state_read_only_sql(SqlEngine::Oracle, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_run_whose_state_turns_read_only_delivers_or_fails_loudly_postgres() {
    state_read_only_sql(SqlEngine::Pg, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_run_whose_state_turns_read_only_delivers_or_fails_loudly_mysql() {
    state_read_only_sql(SqlEngine::Mysql, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_run_whose_state_turns_read_only_delivers_or_fails_loudly_mssql() {
    state_read_only_sql(SqlEngine::Mssql, Shape::Range);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_run_whose_state_turns_read_only_delivers_or_fails_loudly_oracle() {
    state_read_only_sql(SqlEngine::Oracle, Shape::Range);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_checkpoint_run_whose_state_turns_read_only_delivers_or_fails_loudly_postgres() {
    state_read_only_sql(SqlEngine::Pg, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_checkpoint_run_whose_state_turns_read_only_delivers_or_fails_loudly_mysql() {
    state_read_only_sql(SqlEngine::Mysql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_checkpoint_run_whose_state_turns_read_only_delivers_or_fails_loudly_mssql() {
    state_read_only_sql(SqlEngine::Mssql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_range_checkpoint_run_whose_state_turns_read_only_delivers_or_fails_loudly_mongo() {
    state_read_only_mongo();
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_checkpoint_run_whose_state_turns_read_only_delivers_or_fails_loudly_oracle() {
    state_read_only_sql(SqlEngine::Oracle, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_run_whose_postgres_state_is_cut_off_delivers_or_fails_loudly_postgres() {
    postgres_state_cut_off();
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_run_whose_select_is_revoked_fails_loudly_and_recovers_postgres() {
    select_revoked(SqlEngine::Pg, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_whose_select_is_revoked_fails_loudly_and_recovers_mysql() {
    select_revoked(SqlEngine::Mysql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_whose_select_is_revoked_fails_loudly_and_recovers_mssql() {
    select_revoked(SqlEngine::Mssql, Shape::RangeCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_chunk_checkpoint_removed_under_its_run_is_refused_and_walked_out_postgres() {
    chunk_checkpoint_gone(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_chunk_checkpoint_removed_under_its_run_is_refused_and_walked_out_mysql() {
    chunk_checkpoint_gone(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_chunk_checkpoint_removed_under_its_run_is_refused_and_walked_out_mssql() {
    chunk_checkpoint_gone(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_chunk_checkpoint_removed_under_its_run_is_refused_and_walked_out_oracle() {
    chunk_checkpoint_gone(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_state_reset_beside_a_live_run_is_refused_and_walked_out_postgres() {
    run_in_progress_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_state_reset_beside_a_live_run_is_refused_and_walked_out_mysql() {
    run_in_progress_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_state_reset_beside_a_live_run_is_refused_and_walked_out_mssql() {
    run_in_progress_sql(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_state_reset_beside_a_live_run_is_refused_and_walked_out_mongo() {
    run_in_progress_mongo();
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_state_reset_beside_a_live_run_is_refused_and_walked_out_oracle() {
    run_in_progress_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_second_incremental_run_beside_a_live_one_delivers_or_refuses_postgres() {
    second_run_beside(SqlEngine::Pg, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_second_incremental_run_beside_a_live_one_delivers_or_refuses_mysql() {
    second_run_beside(SqlEngine::Mysql, Shape::Incremental);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_second_incremental_run_beside_a_live_one_delivers_or_refuses_mssql() {
    second_run_beside(SqlEngine::Mssql, Shape::Incremental);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_second_incremental_run_beside_a_live_one_delivers_or_refuses_oracle() {
    second_run_beside(SqlEngine::Oracle, Shape::Incremental);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (two concurrent range-chunked runs double the destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_range_run_beside_a_live_one_delivers_or_refuses_postgres() {
    second_run_beside(SqlEngine::Pg, Shape::Range);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (two concurrent range-chunked runs double the destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_range_run_beside_a_live_one_delivers_or_refuses_mysql() {
    second_run_beside(SqlEngine::Mysql, Shape::Range);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (two concurrent range-chunked runs double the destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_range_run_beside_a_live_one_delivers_or_refuses_mssql() {
    second_run_beside(SqlEngine::Mssql, Shape::Range);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (two concurrent keyset runs double the destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_keyset_run_beside_a_live_one_delivers_or_refuses_postgres() {
    second_run_beside(SqlEngine::Pg, Shape::Keyset);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (two concurrent keyset runs double the destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_keyset_run_beside_a_live_one_delivers_or_refuses_mysql() {
    second_run_beside(SqlEngine::Mysql, Shape::Keyset);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (two concurrent keyset runs double the destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_keyset_run_beside_a_live_one_delivers_or_refuses_mssql() {
    second_run_beside(SqlEngine::Mssql, Shape::Keyset);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a second run beside a live keyset-checkpoint run is refused with no code), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_keyset_checkpoint_run_beside_a_live_one_delivers_or_refuses_postgres() {
    second_run_beside(SqlEngine::Pg, Shape::KeysetCheckpoint);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a second run beside a live keyset-checkpoint run is refused with no code), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_keyset_checkpoint_run_beside_a_live_one_delivers_or_refuses_mysql() {
    second_run_beside(SqlEngine::Mysql, Shape::KeysetCheckpoint);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a second run beside a live keyset-checkpoint run is refused with no code), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_second_keyset_checkpoint_run_beside_a_live_one_delivers_or_refuses_mssql() {
    second_run_beside(SqlEngine::Mssql, Shape::KeysetCheckpoint);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_state_reset_beside_a_live_incremental_run_delivers_or_refuses_postgres() {
    state_reset_beside(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_state_reset_beside_a_live_incremental_run_delivers_or_refuses_mysql() {
    state_reset_beside(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_state_reset_beside_a_live_incremental_run_delivers_or_refuses_mssql() {
    state_reset_beside(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_state_reset_beside_a_live_incremental_run_delivers_or_refuses_oracle() {
    state_reset_beside(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_deleted_lease_file_beside_a_live_run_delivers_or_refuses_postgres() {
    lease_file_removed(|| live(SqlEngine::Pg, "sab_lease", Shape::RangeCheckpoint));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_deleted_lease_file_beside_a_live_run_delivers_or_refuses_mysql() {
    lease_file_removed(|| live(SqlEngine::Mysql, "sab_lease", Shape::RangeCheckpoint));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_deleted_lease_file_beside_a_live_run_delivers_or_refuses_mssql() {
    lease_file_removed(|| live(SqlEngine::Mssql, "sab_lease", Shape::RangeCheckpoint));
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_deleted_lease_file_beside_a_live_run_delivers_or_refuses_mongo() {
    lease_file_removed(|| mongo_live_export("sab_lease"));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_deleted_lease_file_beside_a_live_run_delivers_or_refuses_oracle() {
    lease_file_removed(|| live(SqlEngine::Oracle, "sab_lease", Shape::RangeCheckpoint));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_beside_a_live_run_delivers_or_fails_loudly_postgres() {
    command_beside(SqlEngine::Pg, "sab_validate", &["validate"]);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_beside_a_live_run_delivers_or_fails_loudly_mysql() {
    command_beside(SqlEngine::Mysql, "sab_validate", &["validate"]);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_beside_a_live_run_delivers_or_fails_loudly_mssql() {
    command_beside(SqlEngine::Mssql, "sab_validate", &["validate"]);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_beside_a_live_run_delivers_or_fails_loudly_oracle() {
    command_beside(SqlEngine::Oracle, "sab_validate", &["validate"]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn reconcile_beside_a_live_run_delivers_or_fails_loudly_postgres() {
    command_beside(
        SqlEngine::Pg,
        "sab_reconcile",
        &["reconcile", "--export", "{export}"],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn reconcile_beside_a_live_run_delivers_or_fails_loudly_mysql() {
    command_beside(
        SqlEngine::Mysql,
        "sab_reconcile",
        &["reconcile", "--export", "{export}"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn reconcile_beside_a_live_run_delivers_or_fails_loudly_mssql() {
    command_beside(
        SqlEngine::Mssql,
        "sab_reconcile",
        &["reconcile", "--export", "{export}"],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn reconcile_beside_a_live_run_delivers_or_fails_loudly_oracle() {
    command_beside(
        SqlEngine::Oracle,
        "sab_reconcile",
        &["reconcile", "--export", "{export}"],
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (repair --execute beside a live run takes its chunks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_repair_beside_a_live_run_delivers_or_fails_loudly_postgres() {
    command_beside(
        SqlEngine::Pg,
        "sab_repair",
        &["repair", "--export", "{export}", "--execute"],
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (repair --execute beside a live run takes its chunks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_repair_beside_a_live_run_delivers_or_fails_loudly_mysql() {
    command_beside(
        SqlEngine::Mysql,
        "sab_repair",
        &["repair", "--export", "{export}", "--execute"],
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (repair --execute beside a live run takes its chunks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_repair_beside_a_live_run_delivers_or_fails_loudly_mssql() {
    command_beside(
        SqlEngine::Mssql,
        "sab_repair",
        &["repair", "--export", "{export}", "--execute"],
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (run --resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_task_marked_completed_delivers_or_refuses_postgres() {
    task_marked_completed(SqlEngine::Pg, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (run --resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_task_marked_completed_delivers_or_refuses_mysql() {
    task_marked_completed(SqlEngine::Mysql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (run --resume trusts an edited chunk_task row), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_task_marked_completed_delivers_or_refuses_mssql() {
    task_marked_completed(SqlEngine::Mssql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (run --resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_edited_task_bounds_delivers_or_refuses_postgres() {
    task_bounds_edited(SqlEngine::Pg, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (run --resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_edited_task_bounds_delivers_or_refuses_mysql() {
    task_bounds_edited(SqlEngine::Mysql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (run --resume trusts edited chunk_task bounds), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_edited_task_bounds_delivers_or_refuses_mssql() {
    task_bounds_edited(SqlEngine::Mssql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (run --resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_deleted_tasks_delivers_or_refuses_postgres() {
    tasks_deleted(SqlEngine::Pg, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (run --resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_deleted_tasks_delivers_or_refuses_mysql() {
    tasks_deleted(SqlEngine::Mysql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (run --resume over a chunk_run with no tasks), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_deleted_tasks_delivers_or_refuses_mssql() {
    tasks_deleted(SqlEngine::Mssql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (run --resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_truncated_committed_part_delivers_or_refuses_postgres() {
    committed_part_truncated(SqlEngine::Pg, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (run --resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_truncated_committed_part_delivers_or_refuses_mysql() {
    committed_part_truncated(SqlEngine::Mysql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (run --resume reuses a truncated committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_truncated_committed_part_delivers_or_refuses_mssql() {
    committed_part_truncated(SqlEngine::Mssql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (run --resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_replaced_committed_part_delivers_or_refuses_postgres() {
    committed_part_replaced(SqlEngine::Pg, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (run --resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_replaced_committed_part_delivers_or_refuses_mysql() {
    committed_part_replaced(SqlEngine::Mysql, RESUME);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (run --resume reuses a replaced committed part), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_run_resume_over_a_replaced_committed_part_delivers_or_refuses_mssql() {
    committed_part_replaced(SqlEngine::Mssql, RESUME);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_full_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::Full,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_full_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::Full,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_full_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::Full,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_full_run_killed_at_each_point_recovers_mongo() {
    killed_at_mongo(&["after_file_write", "after_manifest_update"]);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_full_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::Full,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_incremental_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::Incremental,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
            "after_cursor_commit",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_incremental_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::Incremental,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
            "after_cursor_commit",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_incremental_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::Incremental,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
            "after_cursor_commit",
        ],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_incremental_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::Incremental,
        &[
            "after_source_read",
            "after_file_write",
            "after_manifest_update",
            "after_cursor_commit",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::Range,
        &["after_file_write", "after_manifest_update"],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::Range,
        &["after_file_write", "after_manifest_update"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::Range,
        &["after_file_write", "after_manifest_update"],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::Range,
        &["after_file_write", "after_manifest_update"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_checkpoint_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::RangeCheckpoint,
        &[
            "after_chunk_file:2",
            "after_chunk_complete:2",
            "after_chunk_complete:9",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_checkpoint_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::RangeCheckpoint,
        &[
            "after_chunk_file:2",
            "after_chunk_complete:2",
            "after_chunk_complete:9",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_checkpoint_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::RangeCheckpoint,
        &[
            "after_chunk_file:2",
            "after_chunk_complete:2",
            "after_chunk_complete:9",
            "after_manifest_update",
        ],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_checkpoint_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::RangeCheckpoint,
        &[
            "after_chunk_file:2",
            "after_chunk_complete:2",
            "after_chunk_complete:9",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_range_checkpoint_parallel_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::RangeCheckpointParallel,
        &["after_chunk_file:2", "after_chunk_complete:2"],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_range_checkpoint_parallel_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::RangeCheckpointParallel,
        &["after_chunk_file:2", "after_chunk_complete:2"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_range_checkpoint_parallel_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::RangeCheckpointParallel,
        &["after_chunk_file:2", "after_chunk_complete:2"],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_range_checkpoint_parallel_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::RangeCheckpointParallel,
        &["after_chunk_file:2", "after_chunk_complete:2"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_keyset_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::Keyset,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_keyset_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::Keyset,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_keyset_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::Keyset,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_keyset_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::Keyset,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_keyset_checkpoint_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::KeysetCheckpoint,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_keyset_checkpoint_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::KeysetCheckpoint,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_keyset_checkpoint_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::KeysetCheckpoint,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_keyset_checkpoint_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::KeysetCheckpoint,
        &[
            "keyset_after_open_before_first_page",
            "after_keyset_page:2",
            "keyset_after_data_complete",
            "after_manifest_update",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_keyset_checkpoint_parallel_run_killed_at_each_point_recovers_postgres() {
    killed_at(
        SqlEngine::Pg,
        Shape::KeysetCheckpointParallel,
        &["keyset_parallel_range_committed:0"],
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_keyset_checkpoint_parallel_run_killed_at_each_point_recovers_mysql() {
    killed_at(
        SqlEngine::Mysql,
        Shape::KeysetCheckpointParallel,
        &["keyset_parallel_range_committed:0"],
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_keyset_checkpoint_parallel_run_killed_at_each_point_recovers_mssql() {
    killed_at(
        SqlEngine::Mssql,
        Shape::KeysetCheckpointParallel,
        &["keyset_parallel_range_committed:0"],
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_keyset_checkpoint_parallel_run_killed_at_each_point_recovers_oracle() {
    killed_at(
        SqlEngine::Oracle,
        Shape::KeysetCheckpointParallel,
        &["keyset_parallel_range_committed:0"],
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a resumed range-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_range_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_postgres()
{
    killed_then_source_grows(
        SqlEngine::Pg,
        Shape::RangeCheckpoint,
        "after_chunk_complete:2",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a resumed range-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_range_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_mysql() {
    killed_then_source_grows(
        SqlEngine::Mysql,
        Shape::RangeCheckpoint,
        "after_chunk_complete:2",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a resumed range-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_range_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_mssql() {
    killed_then_source_grows(
        SqlEngine::Mssql,
        Shape::RangeCheckpoint,
        "after_chunk_complete:2",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a resumed parallel range-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_range_checkpoint_parallel_run_killed_before_the_source_grows_delivers_or_refuses_postgres()
 {
    killed_then_source_grows(
        SqlEngine::Pg,
        Shape::RangeCheckpointParallel,
        "after_chunk_complete:2",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a resumed parallel range-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_range_checkpoint_parallel_run_killed_before_the_source_grows_delivers_or_refuses_mysql()
 {
    killed_then_source_grows(
        SqlEngine::Mysql,
        Shape::RangeCheckpointParallel,
        "after_chunk_complete:2",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a resumed parallel range-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_range_checkpoint_parallel_run_killed_before_the_source_grows_delivers_or_refuses_mssql()
 {
    killed_then_source_grows(
        SqlEngine::Mssql,
        Shape::RangeCheckpointParallel,
        "after_chunk_complete:2",
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_postgres() {
    killed_then_source_grows(
        SqlEngine::Pg,
        Shape::KeysetCheckpoint,
        "after_keyset_page:2",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_mysql() {
    killed_then_source_grows(
        SqlEngine::Mysql,
        Shape::KeysetCheckpoint,
        "after_keyset_page:2",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_mssql() {
    killed_then_source_grows(
        SqlEngine::Mssql,
        Shape::KeysetCheckpoint,
        "after_keyset_page:2",
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_keyset_checkpoint_run_killed_before_the_source_grows_delivers_or_refuses_oracle() {
    killed_then_source_grows(
        SqlEngine::Oracle,
        Shape::KeysetCheckpoint,
        "after_keyset_page:2",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a resumed parallel keyset-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_keyset_checkpoint_parallel_run_killed_before_the_source_grows_delivers_or_refuses_postgres()
 {
    killed_then_source_grows(
        SqlEngine::Pg,
        Shape::KeysetCheckpointParallel,
        "keyset_parallel_range_committed:1",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a resumed parallel keyset-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_keyset_checkpoint_parallel_run_killed_before_the_source_grows_delivers_or_refuses_mysql()
 {
    killed_then_source_grows(
        SqlEngine::Mysql,
        Shape::KeysetCheckpointParallel,
        "keyset_parallel_range_committed:1",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a resumed parallel keyset-checkpoint run misses rows that arrived after the crash), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_keyset_checkpoint_parallel_run_killed_before_the_source_grows_delivers_or_refuses_mssql()
 {
    killed_then_source_grows(
        SqlEngine::Mssql,
        Shape::KeysetCheckpointParallel,
        "keyset_parallel_range_committed:1",
    );
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a crashed run of an empty table resumes to a 0-row success), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_killed_over_an_empty_table_before_it_fills_delivers_or_refuses_postgres() {
    killed_over_an_empty_table_then_source_fills(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a crashed run of an empty table resumes to a 0-row success), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_killed_over_an_empty_table_before_it_fills_delivers_or_refuses_mysql() {
    killed_over_an_empty_table_then_source_fills(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a crashed run of an empty table resumes to a 0-row success), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_killed_over_an_empty_table_before_it_fills_delivers_or_refuses_mssql() {
    killed_over_an_empty_table_then_source_fills(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_chunk_run_whose_tasks_the_state_refuses_leaves_no_run_and_recovers_postgres() {
    chunk_run_open_fails(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_chunk_run_whose_tasks_the_state_refuses_leaves_no_run_and_recovers_mysql() {
    chunk_run_open_fails(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_chunk_run_whose_tasks_the_state_refuses_leaves_no_run_and_recovers_mssql() {
    chunk_run_open_fails(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_chunk_run_whose_tasks_the_state_refuses_leaves_no_run_and_recovers_oracle() {
    chunk_run_open_fails(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (an incremental export continues its cursor into a moved destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_incremental_export_whose_destination_path_is_edited_delivers_the_source_or_refuses_postgres()
 {
    edited_incremental(SqlEngine::Pg, ConfigEdit::DestinationPath);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (an incremental export continues its cursor into a moved destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_incremental_export_whose_destination_path_is_edited_delivers_the_source_or_refuses_mysql()
 {
    edited_incremental(SqlEngine::Mysql, ConfigEdit::DestinationPath);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (an incremental export continues its cursor into a moved destination), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_incremental_export_whose_destination_path_is_edited_delivers_the_source_or_refuses_mssql()
 {
    edited_incremental(SqlEngine::Mssql, ConfigEdit::DestinationPath);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_or_refuses_postgres() {
    edited_unfinished(SqlEngine::Pg, ConfigEdit::DestinationPath);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_or_refuses_mysql() {
    edited_unfinished(SqlEngine::Mysql, ConfigEdit::DestinationPath);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_or_refuses_mssql() {
    edited_unfinished(SqlEngine::Mssql, ConfigEdit::DestinationPath);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unfinished_run_whose_destination_path_is_edited_delivers_the_source_or_refuses_oracle() {
    edited_unfinished(SqlEngine::Oracle, ConfigEdit::DestinationPath);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (an incremental export continues its cursor in another format), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_incremental_export_whose_format_is_edited_delivers_the_source_or_refuses_postgres()
 {
    edited_incremental(SqlEngine::Pg, ConfigEdit::Format);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (an incremental export continues its cursor in another format), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_incremental_export_whose_format_is_edited_delivers_the_source_or_refuses_mysql() {
    edited_incremental(SqlEngine::Mysql, ConfigEdit::Format);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (an incremental export continues its cursor in another format), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_incremental_export_whose_format_is_edited_delivers_the_source_or_refuses_mssql() {
    edited_incremental(SqlEngine::Mssql, ConfigEdit::Format);
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (an unfinished run resumes in another format), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unfinished_run_whose_format_is_edited_delivers_the_source_or_refuses_postgres() {
    edited_unfinished(SqlEngine::Pg, ConfigEdit::Format);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (an unfinished run resumes in another format), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unfinished_run_whose_format_is_edited_delivers_the_source_or_refuses_mysql() {
    edited_unfinished(SqlEngine::Mysql, ConfigEdit::Format);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (an unfinished run resumes in another format), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_an_unfinished_run_whose_format_is_edited_delivers_the_source_or_refuses_mssql() {
    edited_unfinished(SqlEngine::Mssql, ConfigEdit::Format);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_incremental_export_whose_compression_is_edited_delivers_the_source_or_refuses_postgres() {
    edited_incremental(SqlEngine::Pg, ConfigEdit::Compression);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_incremental_export_whose_compression_is_edited_delivers_the_source_or_refuses_mysql() {
    edited_incremental(SqlEngine::Mysql, ConfigEdit::Compression);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_incremental_export_whose_compression_is_edited_delivers_the_source_or_refuses_mssql() {
    edited_incremental(SqlEngine::Mssql, ConfigEdit::Compression);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_incremental_export_whose_compression_is_edited_delivers_the_source_or_refuses_oracle() {
    edited_incremental(SqlEngine::Oracle, ConfigEdit::Compression);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_unfinished_run_whose_compression_is_edited_delivers_the_source_or_refuses_postgres() {
    edited_unfinished(SqlEngine::Pg, ConfigEdit::Compression);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_unfinished_run_whose_compression_is_edited_delivers_the_source_or_refuses_mysql() {
    edited_unfinished(SqlEngine::Mysql, ConfigEdit::Compression);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_unfinished_run_whose_compression_is_edited_delivers_the_source_or_refuses_mssql() {
    edited_unfinished(SqlEngine::Mssql, ConfigEdit::Compression);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unfinished_run_whose_compression_is_edited_delivers_the_source_or_refuses_oracle() {
    edited_unfinished(SqlEngine::Oracle, ConfigEdit::Compression);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_previous_release_after_this_build_delivers_or_refuses_postgres() {
    new_then_old_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_previous_release_after_this_build_delivers_or_refuses_mysql() {
    new_then_old_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_previous_release_after_this_build_delivers_or_refuses_mssql() {
    new_then_old_sql(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_previous_release_after_this_build_delivers_or_refuses_oracle() {
    new_then_old_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn the_previous_release_after_this_build_delivers_or_refuses_mongo() {
    {
        let (rig, _guard) = mongo_rig("sab_downgrade");
        new_then_old(rig, |_| ());
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_two_releases_taking_turns_deliver_or_refuse_postgres() {
    versions_alternate_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_two_releases_taking_turns_deliver_or_refuse_mysql() {
    versions_alternate_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_two_releases_taking_turns_deliver_or_refuse_mssql() {
    versions_alternate_sql(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_two_releases_taking_turns_deliver_or_refuse_oracle() {
    versions_alternate_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn the_two_releases_taking_turns_deliver_or_refuse_mongo() {
    {
        let (rig, _guard) = mongo_rig("sab_rolling");
        versions_alternate(rig, |_| ());
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn this_build_beside_a_live_run_of_the_previous_release_delivers_or_refuses_postgres() {
    new_beside_a_live_old_run_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn this_build_beside_a_live_run_of_the_previous_release_delivers_or_refuses_mysql() {
    new_beside_a_live_old_run_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn this_build_beside_a_live_run_of_the_previous_release_delivers_or_refuses_mssql() {
    new_beside_a_live_old_run_sql(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn this_build_beside_a_live_run_of_the_previous_release_delivers_or_refuses_oracle() {
    new_beside_a_live_old_run_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_run_whose_destination_volume_fills_up_delivers_or_fails_loudly_postgres() {
    {
        let (_table, rig, _guard) = live(SqlEngine::Pg, "sab_full", Shape::RangeCheckpoint);
        destination_fills_up(rig, Rig::has_a_part);
    }
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_whose_destination_volume_fills_up_delivers_or_fails_loudly_mysql() {
    {
        let (_table, rig, _guard) = live(SqlEngine::Mysql, "sab_full", Shape::RangeCheckpoint);
        destination_fills_up(rig, Rig::has_a_part);
    }
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_whose_destination_volume_fills_up_delivers_or_fails_loudly_mssql() {
    {
        let (_table, rig, _guard) = live(SqlEngine::Mssql, "sab_full", Shape::RangeCheckpoint);
        destination_fills_up(rig, Rig::has_a_part);
    }
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_whose_destination_volume_fills_up_delivers_or_fails_loudly_oracle() {
    {
        let (_table, rig, _guard) = live(SqlEngine::Oracle, "sab_full", Shape::RangeCheckpoint);
        destination_fills_up(rig, Rig::has_a_part);
    }
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_run_whose_destination_volume_fills_up_delivers_or_fails_loudly_mongo() {
    {
        let (rig, _m, _guard) = mongo_live("sab_full");
        destination_fills_up(rig, Rig::has_a_part);
    }
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a run whose state volume is full leaves what a failed run may not), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_whose_state_volume_fills_up_delivers_or_fails_loudly_postgres() {
    state_volume_fills_up_sql(SqlEngine::Pg);
}

#[test]
#[ignore = "live+gate-only: docker compose mysql; open defect (a run whose state volume is full leaves what a failed run may not), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_whose_state_volume_fills_up_delivers_or_fails_loudly_mysql() {
    state_volume_fills_up_sql(SqlEngine::Mysql);
}

#[test]
#[ignore = "live+gate-only: docker compose mssql; open defect (a run whose state volume is full leaves what a failed run may not), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_whose_state_volume_fills_up_delivers_or_fails_loudly_mssql() {
    state_volume_fills_up_sql(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live+gate-only: docker compose oracle; open defect (a run whose state volume is full leaves what a failed run may not), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_whose_state_volume_fills_up_delivers_or_fails_loudly_oracle() {
    state_volume_fills_up_sql(SqlEngine::Oracle);
}

#[test]
#[ignore = "live+gate-only: docker compose mongo; open defect (a run whose state volume is full leaves what a failed run may not), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_whose_state_volume_fills_up_delivers_or_fails_loudly_mongo() {
    {
        if not_sqlite_state("a SQLite state on a volume that fills up") {
            return;
        }
        let (rig, _m, _guard) = mongo_live_leaving("sab_statefull", STATE_FULL);
        state_volume_fills_up(rig, Rig::has_a_part);
    }
}

#[test]
#[ignore = "live+gate-only: docker compose postgres; open defect (a PostgreSQL run out of file descriptors panics), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_a_run_out_of_file_descriptors_fails_loudly_and_the_next_delivers_postgres() {
    {
        let (_table, rig, _guard) = shaped(SqlEngine::Pg, "sab_fds", Shape::RangeCheckpoint);
        out_of_file_descriptors(rig);
    }
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_run_out_of_file_descriptors_fails_loudly_and_the_next_delivers_mysql() {
    {
        let (_table, rig, _guard) = shaped(SqlEngine::Mysql, "sab_fds", Shape::RangeCheckpoint);
        out_of_file_descriptors_on_sqlite_state(rig);
    }
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_run_out_of_file_descriptors_fails_loudly_and_the_next_delivers_mssql() {
    {
        let (_table, rig, _guard) = shaped(SqlEngine::Mssql, "sab_fds", Shape::RangeCheckpoint);
        out_of_file_descriptors_on_sqlite_state(rig);
    }
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_out_of_file_descriptors_fails_loudly_and_the_next_delivers_oracle() {
    {
        let (_table, rig, _guard) = shaped(SqlEngine::Oracle, "sab_fds", Shape::RangeCheckpoint);
        out_of_file_descriptors_on_sqlite_state(rig);
    }
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_run_out_of_file_descriptors_fails_loudly_and_the_next_delivers_mongo() {
    {
        let (rig, _guard) = mongo_rig("sab_fds");
        out_of_file_descriptors_on_sqlite_state(rig);
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_column_dropped_under_a_live_run_delivers_or_fails_loudly_postgres() {
    table_changed_under_it(SqlEngine::Pg, Ddl::DropColumn);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_column_dropped_under_a_live_run_delivers_or_fails_loudly_mysql() {
    table_changed_under_it(SqlEngine::Mysql, Ddl::DropColumn);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_column_dropped_under_a_live_run_delivers_or_fails_loudly_mssql() {
    table_changed_under_it(SqlEngine::Mssql, Ddl::DropColumn);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_column_dropped_under_a_live_run_delivers_or_fails_loudly_oracle() {
    table_changed_under_it(SqlEngine::Oracle, Ddl::DropColumn);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_column_retyped_under_a_live_run_delivers_or_fails_loudly_postgres() {
    table_changed_under_it(SqlEngine::Pg, Ddl::AlterColumnType);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_column_retyped_under_a_live_run_delivers_or_fails_loudly_mysql() {
    table_changed_under_it(SqlEngine::Mysql, Ddl::AlterColumnType);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_column_retyped_under_a_live_run_delivers_or_fails_loudly_mssql() {
    table_changed_under_it(SqlEngine::Mssql, Ddl::AlterColumnType);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_table_dropped_under_a_live_run_delivers_or_fails_loudly_postgres() {
    table_changed_under_it(SqlEngine::Pg, Ddl::DropTable);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_table_dropped_under_a_live_run_delivers_or_fails_loudly_mysql() {
    table_changed_under_it(SqlEngine::Mysql, Ddl::DropTable);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_table_dropped_under_a_live_run_delivers_or_fails_loudly_mssql() {
    table_changed_under_it(SqlEngine::Mssql, Ddl::DropTable);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_table_dropped_under_a_live_run_delivers_or_fails_loudly_oracle() {
    table_changed_under_it(SqlEngine::Oracle, Ddl::DropTable);
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_table_dropped_under_a_live_run_delivers_or_fails_loudly_mongo() {
    collection_dropped_under_it();
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_incremental_export_whose_state_is_dated_ahead_delivers_or_refuses_postgres() {
    clock_back_incremental(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_incremental_export_whose_state_is_dated_ahead_delivers_or_refuses_mysql() {
    clock_back_incremental(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_incremental_export_whose_state_is_dated_ahead_delivers_or_refuses_mssql() {
    clock_back_incremental(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_incremental_export_whose_state_is_dated_ahead_delivers_or_refuses_oracle() {
    clock_back_incremental(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_unfinished_run_whose_state_is_dated_ahead_delivers_or_refuses_postgres() {
    clock_back_unfinished(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn an_unfinished_run_whose_state_is_dated_ahead_delivers_or_refuses_mysql() {
    clock_back_unfinished(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn an_unfinished_run_whose_state_is_dated_ahead_delivers_or_refuses_mssql() {
    clock_back_unfinished(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unfinished_run_whose_state_is_dated_ahead_delivers_or_refuses_oracle() {
    clock_back_unfinished(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn hostile_text_is_delivered_or_fails_loudly_postgres() {
    hostile(SqlEngine::Pg, &hostile_text(SqlEngine::Pg));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn hostile_text_is_delivered_or_fails_loudly_mysql() {
    hostile(SqlEngine::Mysql, &hostile_text(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn hostile_text_is_delivered_or_fails_loudly_mssql() {
    hostile(SqlEngine::Mssql, &hostile_text(SqlEngine::Mssql));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn hostile_text_is_delivered_or_fails_loudly_oracle() {
    hostile(SqlEngine::Oracle, &hostile_text(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn hostile_text_is_delivered_or_fails_loudly_mongo() {
    hostile_mongo(vec![
        (
            "a string with a NUL",
            mongodb::bson::Bson::String("a\0b".into()),
        ),
        (
            "control characters",
            mongodb::bson::Bson::String("a\tb\nc\rd".into()),
        ),
        (
            "outside the basic plane",
            mongodb::bson::Bson::String("😀𝄞 שלום é".into()),
        ),
        (
            "the empty string",
            mongodb::bson::Bson::String(String::new()),
        ),
    ]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn bytes_that_are_not_utf8_are_delivered_or_fail_loudly_postgres() {
    hostile(SqlEngine::Pg, &hostile_bytes(SqlEngine::Pg));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn bytes_that_are_not_utf8_are_delivered_or_fail_loudly_mysql() {
    hostile(SqlEngine::Mysql, &hostile_bytes(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn bytes_that_are_not_utf8_are_delivered_or_fail_loudly_mssql() {
    hostile(SqlEngine::Mssql, &hostile_bytes(SqlEngine::Mssql));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn bytes_that_are_not_utf8_are_delivered_or_fail_loudly_oracle() {
    hostile(SqlEngine::Oracle, &hostile_bytes(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn bytes_that_are_not_utf8_are_delivered_or_fail_loudly_mongo() {
    hostile_mongo(vec![(
        "bytes that are not UTF-8",
        mongodb::bson::Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: vec![0x00, 0xff, 0xfe, 0x80],
        }),
    )]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_huge_cell_is_delivered_or_fails_loudly_postgres() {
    hostile(SqlEngine::Pg, &hostile_huge(SqlEngine::Pg));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_huge_cell_is_delivered_or_fails_loudly_mysql() {
    hostile(SqlEngine::Mysql, &hostile_huge(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_huge_cell_is_delivered_or_fails_loudly_mssql() {
    hostile(SqlEngine::Mssql, &hostile_huge(SqlEngine::Mssql));
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn a_huge_cell_is_delivered_or_fails_loudly_mongo() {
    hostile_mongo(vec![(
        "a string of HUGE bytes",
        mongodb::bson::Bson::String("x".repeat(HUGE)),
    )]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_ends_of_each_numeric_type_are_delivered_or_fail_loudly_postgres() {
    hostile(SqlEngine::Pg, &hostile_numbers(SqlEngine::Pg));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_ends_of_each_numeric_type_are_delivered_or_fail_loudly_mysql() {
    hostile(SqlEngine::Mysql, &hostile_numbers(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_ends_of_each_numeric_type_are_delivered_or_fail_loudly_mssql() {
    hostile(SqlEngine::Mssql, &hostile_numbers(SqlEngine::Mssql));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_ends_of_each_numeric_type_are_delivered_or_fail_loudly_oracle() {
    hostile(SqlEngine::Oracle, &hostile_numbers(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn the_ends_of_each_numeric_type_are_delivered_or_fail_loudly_mongo() {
    hostile_mongo(vec![
        ("the least int64", mongodb::bson::Bson::Int64(i64::MIN)),
        ("the greatest int64", mongodb::bson::Bson::Int64(i64::MAX)),
        ("NaN", mongodb::bson::Bson::Double(f64::NAN)),
        ("infinity", mongodb::bson::Bson::Double(f64::INFINITY)),
        ("the greatest double", mongodb::bson::Bson::Double(f64::MAX)),
    ]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_ends_of_each_date_type_are_delivered_or_fail_loudly_postgres() {
    hostile(SqlEngine::Pg, &hostile_dates(SqlEngine::Pg));
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_ends_of_each_date_type_are_delivered_or_fail_loudly_mysql() {
    hostile(SqlEngine::Mysql, &hostile_dates(SqlEngine::Mysql));
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_ends_of_each_date_type_are_delivered_or_fail_loudly_mssql() {
    hostile(SqlEngine::Mssql, &hostile_dates(SqlEngine::Mssql));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_ends_of_each_date_type_are_delivered_or_fail_loudly_oracle() {
    hostile(SqlEngine::Oracle, &hostile_dates(SqlEngine::Oracle));
}

#[test]
#[ignore = "live: requires docker compose mongo"]
fn the_ends_of_each_date_type_are_delivered_or_fail_loudly_mongo() {
    hostile_mongo(vec![
        (
            "the first date",
            mongodb::bson::Bson::DateTime(mongodb::bson::DateTime::MIN),
        ),
        (
            "the last date",
            mongodb::bson::Bson::DateTime(mongodb::bson::DateTime::MAX),
        ),
        (
            "the year 1",
            mongodb::bson::Bson::DateTime(mongodb::bson::DateTime::from_millis(
                -62_135_596_800_000,
            )),
        ),
        (
            "the year 9999",
            mongodb::bson::Bson::DateTime(mongodb::bson::DateTime::from_millis(
                253_402_300_799_999,
            )),
        ),
    ]);
}
