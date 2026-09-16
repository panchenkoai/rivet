//! `cdc.backfill` by reference — the baseline leg borrows an existing batch
//! export's READ RECIPE, and every later run must capture only the delta.
//!
//! Three questions, each of which a single-run test cannot answer:
//!
//! 1. The recipe is read ONCE per full run (the run loop skips it, because the
//!    CDC export runs it after the anchor) — and still exports on its own when
//!    named with `-e`.
//! 2. One cycle: anchor → baseline → delta, with the baseline NOT re-read on the
//!    second run.
//! 3. Repeated runs against a REAL warehouse: run 2, 3, 4 with the same config
//!    must append only what changed, and `<table>__changes` must only ever GROW
//!    (the load into a changelog we did not truncate is always an append).
//!
//! Gated `#[ignore]` like every `live_*` suite. Run with:
//!     docker compose --profile cdc up -d mysql-cdc
//!     cargo test --test live_suite cdc_backfill -- --ignored
//! The BigQuery test additionally needs `BIGQUERY_TEST_PROJECT` +
//! `RIVET_TEST_GCS_BUCKET` (optional `RIVET_TEST_BQ_DATASET`, default
//! `rivet_e2e`) and an authenticated `bq` CLI; it SKIPS without them, so anyone
//! can point it at their own project.

use crate::common::MysqlCdcTable as Table;
use crate::common::*;
use mysql::prelude::Queryable;

fn conn() -> mysql::PooledConn {
    mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn")
}

/// A fresh `(id, v)` table on the CDC stand holding ids `1..=n`.
fn seeded(prefix: &str, n: i64) -> (String, Table) {
    let mut c = conn();
    let tbl = unique_name(prefix);
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"
    ))
    .expect("create table");
    let guard = Table(tbl.clone());
    let rows: Vec<String> = (1..=n).map(|i| format!("({i}, {i})")).collect();
    c.query_drop(format!(
        "INSERT INTO {tbl} (id, v) VALUES {}",
        rows.join(", ")
    ))
    .expect("seed rows");
    (tbl, guard)
}

/// A CDC export over `tbl` whose baseline is the batch export `baseline`, which
/// reads the same table. `backfill: auto` pairs them by table name.
fn backfill_rig(tbl: &str) -> Rig {
    Rig::mysql_cdc(tbl)
        .cdc("backfill: auto")
        .also_batch_export("baseline", tbl, "full")
}

// Parts are counted with `common::parquet::total_parquet_rows` — DIRECTLY under a
// dir, not recursive: the baseline lands in the `snapshot/` CHILD of the CDC
// export's own prefix, and telling the two legs apart is the point here.

/// The baseline's prefix inside the CDC export's own destination (variant A:
/// the recipe is a READ recipe, not a second load target).
fn snapshot_dir(rig: &Rig) -> std::path::PathBuf {
    rig.out_dir().join("snapshot")
}

fn run_ok_with_log(rig: &Rig) -> String {
    let out = rig.run_args_env(&[], &[("RUST_LOG", "info")]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "rivet run failed:\n{said}");
    said
}

/// The run loop must read a captured table ONCE per full run. The recipe export
/// exists to describe HOW to read the table; the CDC export runs it after the
/// anchor, so running it again from the loop would read the whole table a second
/// time in one invocation — twice the source pressure, into a prefix nothing
/// consumes.
///
/// The oracle is the DELIVERED OUTCOME, not just the skip message: the recipe's
/// own prefix stays empty while the baseline lands in the CDC export's
/// `snapshot/`. A test asserting only the log line would pass against a run that
/// logged the skip and exported anyway.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn a_backfill_recipe_is_read_once_per_run_and_still_exports_when_named() {
    let (tbl, _guard) = seeded("rivet_bf_skip", 5);
    let rig = backfill_rig(&tbl);

    let said = run_ok_with_log(&rig);
    assert!(
        said.contains("skipped — it is the backfill recipe"),
        "the run must say why it skipped the recipe:\n{said}"
    );
    assert!(
        said.contains("baseline"),
        "the skip line must name the recipe:\n{said}"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir_for("baseline")),
        0,
        "the recipe's OWN prefix must stay empty — it is a read recipe, not a second export"
    );
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)),
        5,
        "the baseline belongs in the CDC export's snapshot prefix"
    );

    // Named explicitly, the operator still gets it: the skip is a whole-config
    // rule, never a disabled export.
    let out = rig.run_args(&["-e", "baseline"]);
    assert!(
        out.status.success(),
        "`rivet run -e baseline` failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir_for("baseline")),
        5,
        "named explicitly, the recipe exports into its own prefix"
    );
}

/// The same rule on the OTHER whole-config entry point.
///
/// `rivet apply <cfg.yaml>` runs every export wave by wave, and it shipped
/// WITHOUT the recipe filter that `rivet run` had — so the table was read twice
/// per invocation: once by the recipe into a prefix `rivet load` deliberately
/// skips, and once by the CDC export's own baseline leg. One entry point
/// honouring a rule the other ignores is the bug this pins.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn apply_skips_the_backfill_recipe_exactly_as_run_does() {
    let (tbl, _guard) = seeded("rivet_bf_apply", 5);
    let rig = backfill_rig(&tbl);

    let cfg = rig.config_path();
    let out = rig.apply_env(&cfg, &[], &[]);
    assert!(
        out.status.success(),
        "rivet apply failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir_for("baseline")),
        0,
        "apply must not run the recipe as an ordinary export — that is the second full read"
    );
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)),
        5,
        "the baseline still lands, pulled by the CDC export after its anchor"
    );
}

/// `rivet validate` must certify the baseline a `backfill:` stream wrote.
///
/// It derived "this stream has a snapshot dataset" from `initial:` alone, so a
/// backfilled baseline — the largest dataset in the export — was never verified
/// and its parts were reported as stray in the change prefix, exit 0.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn validate_certifies_the_backfilled_baseline_and_does_not_call_it_stray() {
    let (tbl, _guard) = seeded("rivet_bf_validate", 5);
    let rig = backfill_rig(&tbl);
    rig.run_ok();
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)),
        5,
        "inert fixture: validate would certify an EMPTY baseline and prove nothing"
    );

    // The report goes beside the CONFIG, never under the destination — a file
    // under the prefix would itself be the stray object this test forbids.
    let report = rig.config_path().with_file_name("validate.json");
    let out = rig.cli(&[
        "validate",
        "--format",
        "json",
        "--output",
        report.to_str().expect("utf-8 path"),
    ]);
    assert!(
        out.status.success(),
        "rivet validate failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report).expect("report written"))
            .expect("report is JSON");
    let exports = json["exports"].as_array().expect("exports array");
    let snapshot = exports
        .iter()
        .find(|e| e["export_name"] == format!("{tbl}/snapshot"))
        .unwrap_or_else(|| panic!("the baseline dataset must get its own verdict; got: {json}"));
    assert_eq!(
        snapshot["verification"]["passed"], true,
        "the baseline must verify: {snapshot}"
    );
    assert!(
        json["warnings"].as_array().is_some_and(Vec::is_empty),
        "the baseline parts must not be reported as stray: {}",
        json["warnings"]
    );
    // The recipe never ran, so there is nothing at its prefix to certify: a
    // verdict on it can only ever say "no manifest", noise that hides a real one.
    assert!(
        !exports.iter().any(|e| e["export_name"] == "baseline"),
        "the recipe is a read recipe, not a dataset to verify: {json}"
    );
}

/// A source re-query — the oracle that shares nothing with the capture path.
fn query_one(sql: &str) -> i64 {
    conn()
        .query_first::<i64, _>(sql)
        .expect("source query")
        .expect("one row")
}

/// One full cycle, and the half a single run cannot show: the SECOND run must
/// not re-read the baseline, and must capture only what changed after the
/// anchor. Capture-works is not resume-works.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn a_backfill_cycle_anchors_then_captures_only_the_delta_on_the_next_run() {
    let (tbl, _guard) = seeded("rivet_bf_cycle", 5);
    let rig = backfill_rig(&tbl);

    rig.run_ok();
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)) as i64,
        query_one(&format!("SELECT COUNT(*) FROM {tbl}")),
        "run 1 backfills the whole table through the recipe — graded against the SOURCE"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        0,
        "nothing changed after the anchor, so the delta leg captures nothing"
    );

    let mut c = conn();
    c.query_drop(format!(
        "INSERT INTO {tbl} (id, v) VALUES (6,6),(7,7),(8,8)"
    ))
    .expect("insert");
    c.query_drop(format!("UPDATE {tbl} SET v = 99 WHERE id = 1"))
        .expect("update");

    rig.run_ok();
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)),
        5,
        "the baseline must NOT be re-read — a completed snapshot is resume evidence"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        4,
        "only the delta: three inserts and one update"
    );
}

/// A baseline leg that CRASHED mid-way must finish on the next plain run.
///
/// The recipe the docs bless for "a table with only a non-unique index" is
/// `mode: chunked` + `chunk_checkpoint: true`. Its in-progress run is recorded
/// under the leg's synthesized name, and the chunked runner refused the next run
/// with two commands — `--export <leg> --resume` and `state reset-chunks --export
/// <leg>` — that both reject a name absent from the config. Every later run
/// failed identically while the anchor, already taken, pinned the log. The leg is
/// rivet's own; resuming it is rivet's job, not a command for the operator.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn a_crashed_chunked_baseline_finishes_on_the_next_plain_run() {
    let (tbl, _guard) = seeded("rivet_bf_crash", 150);
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("backfill: auto")
        .also_batch_export("baseline", &tbl, "chunked")
        .also_export_line("chunk_column: id")
        .also_export_line("chunk_size: 50")
        .also_export_line("chunk_checkpoint: true");

    // Run 1 dies right after the baseline's first chunk is recorded complete.
    let crash = rig.run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "after_chunk_complete:0")]);
    assert!(!crash.status.success(), "the crash run must not exit 0");
    let landed = total_parquet_rows(&snapshot_dir(&rig));
    assert!(
        landed > 0 && landed < 150,
        "inert fixture: expected a PARTIAL baseline, got {landed} rows"
    );

    // Run 2 is the operator's plain re-run — no flags, no synthesized names.
    let out = rig.run_args(&[]);
    assert!(
        out.status.success(),
        "a plain re-run must finish the baseline, not demand a command it will then reject:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)),
        150,
        "the baseline is complete: chunk 0 once, the rest resumed — no gap, no duplicate"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        0,
        "nothing changed, so no delta"
    );
}

/// The type-conflict refusal must hold on EVERY run, not only the one that
/// builds the leg.
///
/// A column the recipe and the CDC export type differently is refused — but
/// the check ran inside the leg builder, which runs only for tables still
/// PENDING a baseline. A conflict added after the baseline landed was never
/// seen: the same config was refused on a fresh state DB and accepted on an
/// established one, and the stream then wrote the second type into the log
/// the baseline had written with the first.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn a_type_conflict_added_after_the_baseline_is_still_refused() {
    let mut c = conn();
    let tbl = unique_name("rivet_bf_conflict");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v DECIMAL(10,2))"
    ))
    .expect("create table");
    let _guard = Table(tbl.clone());
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 1.50), (2, 2.50), (3, 3.50)"
    ))
    .expect("seed");

    let mut rig = Rig::mysql_cdc(&tbl)
        .cdc("backfill: auto")
        .also_batch_export("baseline", &tbl, "full")
        .also_export_line("columns: { v: \"decimal(10,2)\" }");
    rig.run_ok();
    assert_eq!(
        total_parquet_rows(&snapshot_dir(&rig)),
        3,
        "inert fixture: the baseline must have landed before the conflict is added"
    );

    // Run 2: the CDC export now declares the SAME column differently.
    rig.amend_export_lines(&["columns: { v: \"decimal(12,4)\" }"]);
    let said = rig.run_expect_fail();
    assert!(
        said.contains("cannot have two types"),
        "the conflict must be refused whether or not a baseline is pending:\n{said}"
    );
}

/// The baseline and the stream write ONE `__changes`, so a column must have one
/// type in both — and the warehouse DDL must be typed from that one.
///
/// The recipe's `columns:` shaped the baseline's Parquet, but the stream and the
/// recorded load spec resolved types from the CDC export's own `columns:` alone.
/// A `decimal(12,4)` declared on the recipe — the placement the conflict refusal
/// itself recommends — produced a `decimal(12,4)` baseline and a `decimal(10,2)`
/// delta in one log, with the DDL typed from the second.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn the_recipe_column_types_reach_the_stream_and_the_recorded_spec() {
    let mut c = conn();
    let tbl = unique_name("rivet_bf_types");
    c.query_drop(format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v DECIMAL(10,2))"
    ))
    .expect("create table");
    let _guard = Table(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 1.50), (2, 2.50)"))
        .expect("seed");

    let rig = Rig::mysql_cdc(&tbl)
        .cdc("backfill: auto")
        .also_batch_export("baseline", &tbl, "full")
        .also_export_line("columns: { v: \"decimal(12,4)\" }")
        .top_line("load: { target: bigquery, project: p, dataset: d, pk: [id] }");
    rig.run_ok();
    let baseline_v = parquet_column_type(&snapshot_dir(&rig), "v");

    c.query_drop(format!("INSERT INTO {tbl} VALUES (3, 3.50)"))
        .expect("a change to capture");
    rig.run_ok();
    let delta_v = parquet_column_type(&rig.out_dir(), "v");
    assert_eq!(
        baseline_v, delta_v,
        "one log, one type per column: the baseline wrote {baseline_v:?}, the stream {delta_v:?}"
    );

    let spec = StateDb::next_to_config(&rig.config_path()).load_spec_types(&tbl, None);
    let v_spec = spec
        .iter()
        .find(|(name, _)| name == "v")
        .map(|(_, t)| t.clone())
        .expect("`v` in the recorded spec");
    assert!(
        v_spec.contains("12") && v_spec.contains('4'),
        "the spec the DDL is typed from must carry the recipe's declared type; got {v_spec}"
    );
}

/// `rivet plan` over a config that MIXES a CDC export with a batch one must plan
/// the batch export and skip the stream — `rivet check` already does exactly
/// that ("CDC exports are not plannable"), while `plan` took every export and
/// aborted the whole command on the first `mode: cdc`, so `--annotate-waves`
/// (which refuses `--export`) was unusable on any mixed config.
#[test]
#[ignore = "live: requires docker compose --profile cdc up -d mysql-cdc"]
fn plan_skips_a_cdc_export_instead_of_failing_the_whole_config() {
    let (tbl, _guard) = seeded("rivet_bf_plan_cdc", 3);
    let (other, _guard2) = seeded("rivet_bf_plan_batch", 3);
    let rig = Rig::mysql_cdc(&tbl).also_batch_export("other", &other, "full");
    let out_path = rig.config_path().with_file_name("plan.json");
    let out = rig.cli(&[
        "plan",
        "--format",
        "json",
        "--output",
        out_path.to_str().expect("utf-8 path"),
    ]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "a CDC export must be SKIPPED by plan, not fail the whole config:\n{said}"
    );
    assert!(
        said.contains(&tbl) && said.to_lowercase().contains("skip"),
        "the skip must be said, naming the stream:\n{said}"
    );
    assert!(
        out_path.exists(),
        "the batch export's plan must still be written"
    );
}

fn load_ok(rig: &Rig) {
    let out = rig.cli(&["load"]);
    assert!(
        out.status.success(),
        "rivet load failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn count(text: String) -> i64 {
    text.parse().expect("a count")
}

/// Rows the warehouse has ACCUMULATED for this export: the changelog once it
/// exists, the plain table before the first delta renames it into one.
fn loaded_rows(bq: &BqLive, table: &str, changes: &str) -> i64 {
    if bq.read_bq_table_type(changes).is_some() {
        count(bq.read_bq_count(changes))
    } else {
        count(bq.read_bq_count(table))
    }
}

/// Repeated runs of ONE config against a real warehouse: every run after the
/// first must append ONLY what changed, and the changelog must only ever grow.
///
/// This is the question a local part-count cannot answer — the parts are
/// per-run, so re-reading the whole table looks identical to a delta until the
/// rows land in a table that accumulates. Run 3 (nothing changed) and the
/// repeated `rivet load` with no new run are the two ways a re-read or a
/// double-load would show up as growth.
#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds (BIGQUERY_TEST_PROJECT, RIVET_TEST_GCS_BUCKET)"]
fn repeated_runs_of_one_config_append_only_the_delta_to_the_changelog() {
    let Some(bq) = BqLive::from_env("cdc_bf_repeat") else {
        return;
    };
    let (tbl, _guard) = seeded("rivet_bf_bq", 5);
    let changes = format!("{tbl}__changes");
    let _cleanup = bq.cleanup(&[&tbl, &changes]);

    let rig = backfill_rig(&tbl)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id]"));

    // Run 1 — anchor + baseline through the recipe.
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        loaded_rows(&bq, &tbl, &changes),
        5,
        "the baseline is the whole table, loaded once"
    );

    // Run 2 — three inserts and one update, nothing else.
    let mut c = conn();
    c.query_drop(format!(
        "INSERT INTO {tbl} (id, v) VALUES (6,6),(7,7),(8,8)"
    ))
    .expect("insert");
    c.query_drop(format!("UPDATE {tbl} SET v = 99 WHERE id = 1"))
        .expect("update");
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        loaded_rows(&bq, &tbl, &changes),
        9,
        "run 2 appends the 4 changed rows and NOT the whole table again"
    );
    assert_eq!(
        count(bq.read_bq_count(&tbl)),
        8,
        "the current-state view is one row per key"
    );

    // Run 3 — nothing changed since run 2.
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        loaded_rows(&bq, &tbl, &changes),
        9,
        "a run with no changes must append nothing"
    );

    // A second `rivet load` with no new run: the ledger already recorded these
    // run ids, so the same parts must not be loaded twice.
    load_ok(&rig);
    assert_eq!(
        loaded_rows(&bq, &tbl, &changes),
        9,
        "re-loading without a new run must append nothing (the load ledger)"
    );

    // Run 4 — two more inserts, to prove the stream is still live after the
    // idle cycle rather than merely quiet.
    c.query_drop(format!("INSERT INTO {tbl} (id, v) VALUES (9,9),(10,10)"))
        .expect("insert");
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        loaded_rows(&bq, &tbl, &changes),
        11,
        "only the two new rows are appended"
    );
    assert_eq!(
        count(bq.read_bq_count(&tbl)),
        10,
        "the view follows the new keys"
    );
}
