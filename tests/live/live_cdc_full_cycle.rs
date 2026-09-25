//! The full CDC cycle on every engine, end to end, in the operator's order:
//! preflight → anchor + baseline (backfill through a batch recipe) → load →
//! changes → load 2 (the delta only, into the buffer) → compact → a crash ON THE
//! CDC LEG → plain re-run → load 3 → an idle run and a load with nothing new.
//!
//! Two oracles, neither sharing code with rivet's write path: the warehouse
//! read over BigQuery's REST API, the source through a re-query on the engine. Every
//! step reads back what landed — a 0-row success cannot pass.
//!
//! Needs the CDC stand (`docker compose --profile cdc up -d`) and the warehouse
//! env (`BIGQUERY_TEST_PROJECT`, `RIVET_TEST_GCS_BUCKET`, optional
//! `RIVET_TEST_BQ_DATASET`); it SKIPS without the latter, so anyone can point it
//! at their own project. The step-by-step operator version of this scenario is
//! `docs/cdc-full-cycle.md`.

use crate::common::*;

fn load_ok(rig: &Rig) {
    let out = rig.cli(&["load"]);
    assert!(
        out.status.success(),
        "rivet load failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `rivet compact`: merge the buffer into the base and drop it (a no-op without one).
fn compact_ok(rig: &Rig) {
    let out = rig.cli(&["compact"]);
    assert!(
        out.status.success(),
        "rivet compact failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn count(bq: &BqLive, table: &str) -> i64 {
    bq.read_bq_count(table).parse().expect("a count")
}

/// Rows in the `__changes` buffer — 0 when there is none (compacted, or nothing loaded).
fn buffered(bq: &BqLive, changes: &str) -> i64 {
    if bq.read_bq_table_type(changes).is_some() {
        count(bq, changes)
    } else {
        0
    }
}

/// The base's LIVE state must equal the source, row for row on the key.
///
/// Base + buffer layout (the default with a `backfill:` recipe): the base is a
/// physical table whose deleted keys stay as rows flagged `__is_deleted`, so live
/// state is `WHERE NOT __is_deleted` — and it is only current after `compact`.
fn assert_state_is_source(bq: &BqLive, table: &str, scn: &mut CdcScenario, step: &str) {
    let source = scn.count();
    let row = &bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT {pk}) AS d FROM `{}.{}.{table}` \
         WHERE NOT __is_deleted",
        bq.project,
        bq.dataset,
        pk = scn.pk()
    ))[0];
    let n: i64 = row["n"].as_str().expect("count").parse().expect("a count");
    let d: i64 = row["d"].as_str().expect("count").parse().expect("a count");
    assert_eq!(n, source, "{step}: the live state must equal the source");
    assert_eq!(d, n, "{step}: one row per key");
}

/// Shape every engine's rig the same way: a CDC export whose baseline is the
/// batch recipe `baseline` over the same table, a live GCS destination, and a
/// `load:` block keyed on the scenario's primary key.
fn shaped(rig: Rig, table: &str, pk: &str, bq: &BqLive) -> Rig {
    rig.cdc("backfill: auto")
        .also_batch_export("baseline", table, "full")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(&format!(", pk: [{pk}]")))
}

/// The scenario itself — one body, four engines.
fn full_cycle(mut scn: CdcScenario, bq: BqLive) {
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);

    // Rows that exist BEFORE the anchor: the baseline's whole job.
    for id in 1..=5 {
        scn.insert(id);
    }
    scn.settle();

    // 0. Preflight: a `mode: cdc` export is a log reader, not a table scan.
    let check = scn.rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "rivet check must pass on a well-formed CDC config:\n{}",
        String::from_utf8_lossy(&check.stderr)
    );

    // 1. Anchor + baseline in ONE run, then the first load and a compaction.
    //
    // The log may already hold more than the baseline here: an engine whose
    // anchor exists BEFORE the seed (PostgreSQL's slot is created by the
    // scenario) streams the seed rows too — at-least-once, into the buffer. The
    // compaction folds them, so step 2's buffer holds exactly its own delta.
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(
        count(&bq, &table),
        5,
        "run 1: the baseline must reach the base"
    );
    compact_ok(&scn.rig);
    assert_state_is_source(&bq, &table, &mut scn, "run 1");

    // 2. Changes → run 2 → load 2: exactly the delta, in the buffer; compact.
    for id in 6..=8 {
        scn.insert(id);
    }
    scn.update(1);
    scn.delete(2);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(
        buffered(&bq, &changes),
        5,
        "run 2 buffers the 5 changed rows (3 inserts, 1 update, 1 delete) and nothing else"
    );
    compact_ok(&scn.rig);
    assert_eq!(buffered(&bq, &changes), 0, "compact drops the buffer");
    assert_state_is_source(&bq, &table, &mut scn, "run 2");

    // 3. A crash on the CDC leg AFTER the part is flushed, BEFORE the checkpoint
    //    advances: the next plain run must re-read the un-acked changes. The
    //    buffer may then hold them twice (at-least-once); the compacted base must not.
    scn.insert(9);
    scn.insert(10);
    scn.settle();
    let crashed = scn.rig.run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the injected crash must fail the run — a run that exits 0 saw NO change to flush:\n{}{}",
        String::from_utf8_lossy(&crashed.stdout),
        String::from_utf8_lossy(&crashed.stderr)
    );
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert!(
        buffered(&bq, &changes) >= 2,
        "run 3 (after the crash) must deliver rows 9 and 10 — nothing lost"
    );
    compact_ok(&scn.rig);
    assert_state_is_source(
        &bq,
        &table,
        &mut scn,
        "run 3 after cdc_after_flush_before_ack",
    );

    // 4. A crash AFTER the ack: the change is durable and acknowledged, so the
    //    next run must NOT re-read it — the base stays exact either way.
    scn.insert(11);
    scn.settle();
    let crashed = scn
        .rig
        .run_args_env(&[], &[("RIVET_TEST_PANIC_AT", "cdc_after_ack")]);
    assert!(
        !crashed.status.success(),
        "the injected crash must fail the run — a run that exits 0 saw NO change to ack:\n{}{}",
        String::from_utf8_lossy(&crashed.stdout),
        String::from_utf8_lossy(&crashed.stderr)
    );
    scn.rig.run_ok();
    load_ok(&scn.rig);
    compact_ok(&scn.rig);
    assert_state_is_source(&bq, &table, &mut scn, "run 4 after cdc_after_ack");

    // 5. Idle: a run with no changes and a load with no new run buffer nothing.
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(
        buffered(&bq, &changes),
        0,
        "an idle run must append nothing"
    );
    load_ok(&scn.rig);
    assert_eq!(
        buffered(&bq, &changes),
        0,
        "a load with no new run must append nothing (the load ledger)"
    );
    compact_ok(&scn.rig);
    assert_state_is_source(&bq, &table, &mut scn, "idle");
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn full_cdc_cycle_mysql() {
    let Some(bq) = BqLive::from_env("cycle_mysql") else {
        return;
    };
    let scn = CdcScenario::mysql_with("cycle_my", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shaped(r, t, "id", &bq)
    });
    full_cycle(scn, bq);
}

#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + BigQuery creds"]
fn full_cdc_cycle_postgres() {
    let Some(bq) = BqLive::from_env("cycle_pg") else {
        return;
    };
    let scn = CdcScenario::pg_with("cycle_pg", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        shaped(r, t, "id", &bq)
    });
    full_cycle(scn, bq);
}

#[test]
#[ignore = "live: requires mssql-cdc (SQL Server Agent) + BigQuery creds"]
fn full_cdc_cycle_mssql() {
    let Some(bq) = BqLive::from_env("cycle_ms") else {
        return;
    };
    let _serial = cross_process_serial("mssql_cdc");
    let scn = CdcScenario::mssql_with("cycle_ms", "id INT PRIMARY KEY, v INT", |r, t| {
        shaped(r, t, "id", &bq)
    });
    full_cycle(scn, bq);
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn full_cdc_cycle_mongo() {
    let Some(bq) = BqLive::from_env("cycle_mg") else {
        return;
    };
    require_alive(LiveService::MongoRs);
    let scn = CdcScenario::mongo_with("cycle_mg", |r, t| shaped(r, t, "_id", &bq));
    full_cycle(scn, bq);
}
