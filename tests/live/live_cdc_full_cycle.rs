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

/// A truncate whose pre-truncate part was flushed but never loaded: the remedy, followed as printed with a BigQuery load, leaves only the source's rows live.
#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + BigQuery creds"]
fn rebaseline_remedy_after_a_truncate_leaves_no_removed_key_live_in_bigquery_postgres() {
    use postgres::NoTls;
    let Some(bq) = BqLive::from_env("rb_trunc_pg") else {
        return;
    };
    let table = unique_name("rb_trunc_pg");
    let slot = unique_name("rb_trunc_pg_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id BIGINT PRIMARY KEY, v INT); \
         INSERT INTO {table} VALUES (1, 1), (2, 2), (3, 3)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    let rig = Rig::pg_cdc(&table, &slot)
        .cdc("initial: snapshot")
        .a_failed_run_may_leave(
            &[Leftover::OrphanPart, Leftover::FileLog, Leftover::CdcFlush],
            "the stream delivers the rows it read before the TRUNCATE it refuses",
        )
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id]"));
    rig.run_ok();
    load_ok(&rig);
    c.execute(&format!("INSERT INTO {table} VALUES (4, 4)"), &[])
        .unwrap();
    c.execute(&format!("TRUNCATE {table}"), &[]).unwrap();
    c.execute(&format!("INSERT INTO {table} VALUES (5, 5)"), &[])
        .unwrap();
    let parts_before = bq.gcs_objects().len();
    let said = rig.run_expect_fail();
    assert!(
        said.contains("was TRUNCATEd") && said.contains(REBASELINE_REMEDY),
        "the refusal names the re-baseline remedy:\n{said}"
    );
    assert!(
        bq.gcs_objects().len() > parts_before,
        "fixture: the refused run flushed the change before the truncate as a part"
    );
    let past: String = c
        .query_one(
            &format!(
                "WITH p AS (SELECT lsn, data FROM pg_logical_slot_peek_changes('{slot}', NULL, NULL)) \
                 SELECT min(lsn)::text FROM p WHERE data LIKE 'COMMIT%' \
                 AND lsn > (SELECT max(lsn) FROM p WHERE data LIKE '%{table}%: TRUNCATE:%')"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    c.execute(
        &format!("SELECT pg_replication_slot_advance('{slot}', '{past}')"),
        &[],
    )
    .unwrap();
    bq.delete_objects("");
    assert!(
        clear_cdc_snapshot(
            &rig.config_path(),
            rig.export_name(),
            &format!("{}/{}", bq.bucket, bq.prefix)
        ) > 0,
        "fixture: the export had a `cdc_snapshot` row to clear"
    );
    bq.exec(&format!(
        "TRUNCATE TABLE `{}.{}.{changes}`",
        bq.project, bq.dataset
    ));
    rig.run_ok();
    load_ok(&rig);
    let live: Vec<String> = bq
        .read_bq_rows(&format!(
            "SELECT CAST(id AS STRING) AS id FROM `{}.{}.{table}` ORDER BY id",
            bq.project, bq.dataset
        ))
        .iter()
        .map(|r| r["id"].as_str().expect("an id").to_string())
        .collect();
    assert_eq!(
        live,
        vec!["5".to_string()],
        "only the row written after the truncate is live; the flushed but unloaded \
         pre-truncate insert (4) and the truncated rows (1-3) are not"
    );
}

/// Two configs of one cluster sharing a PostgreSQL slot name, run in turn: the second consumes
/// changes from the slot the first one reads, and the first one's next run is refused (the
/// changes between its checkpoint and the slot went to the other reader) instead of continuing
/// past them with exit 0.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn a_slot_another_config_advanced_is_refused_instead_of_losing_its_changes() {
    let tbl = unique_name("rivet_slot_shared");
    let slot = unique_name("rivet_shared_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("pg");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT)"
    ))
    .expect("table");
    let _t = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let ckpts = tempfile::tempdir().unwrap();
    let config = |name: &str| {
        Rig::pg_cdc(&tbl, &slot)
            .export_named(name)
            .cdc_line(&format!(
                "checkpoint: {}",
                ckpts.path().join(format!("{name}.ckpt")).display()
            ))
    };
    let first = config(&unique_name("cfg_a"));
    let second = config(&unique_name("cfg_b"));
    first.run_ok();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 1)"))
        .unwrap();
    first.run_ok();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (2, 2)"))
        .unwrap();
    second.run_ok();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (3, 3)"))
        .unwrap();
    let out = first.run_args(&[]);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_LOG_GAP", 5));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("another reader consumed"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A PostgreSQL stream the released previous version (`RIVET_PREV_RELEASE_BIN`) left with its
/// slot past its checkpoint: a change-free transaction on another table between its runs made it
/// release the empty span by advancing the slot before saving the checkpoint (fixed in #649).
/// This binary's first run takes the slot's position once (a warning names both), delivers, and
/// writes the checkpoint in its own form; a later advance by a real foreign reader is refused
/// RIVET_SOURCE_CDC_LOG_GAP on every run (main: refused on the first upgraded run already).
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc + RIVET_PREV_RELEASE_BIN"]
fn an_upgraded_stream_whose_old_release_advanced_its_slot_past_the_checkpoint_continues() {
    let tbl = unique_name("rivet_pg_upgrade");
    let other = unique_name("rivet_pg_upgrade_other");
    let slot = unique_name("rivet_upgrade_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("pg");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT); \
         CREATE TABLE {other} (id BIGINT PRIMARY KEY)"
    ))
    .expect("tables");
    let _t = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let _o = PgTable::adopt_on(POSTGRES_CDC_URL, other.clone());
    let ckpts = tempfile::tempdir().unwrap();
    let ckpt = ckpts.path().join("cdc.ckpt");
    let rig = Rig::pg_cdc(&tbl, &slot).checkpoint_path(ckpt.clone());
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    old.run_ok();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 1)"))
        .unwrap();
    old.run_ok();
    // A captured change followed by a transaction the stream does not capture: the run that
    // yields the change acks it, and the next one releases the trailing empty span by advancing
    // the slot without saving the checkpoint (measured on the released 0.32.1).
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (10, 10); INSERT INTO {other} VALUES (1)"
    ))
    .unwrap();
    old.run_ok();
    old.run_ok();
    let lsn = |c: &mut postgres::Client| -> (String, String) {
        let slot_at: String = c
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .unwrap()
            .get(0);
        let saved: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
        (slot_at, saved["lsn"].as_str().unwrap().to_string())
    };
    let (slot_at, saved) = lsn(&mut c);
    assert_ne!(
        slot_at, saved,
        "fixture: the old release left its slot past its checkpoint"
    );

    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (2, 2)"))
        .unwrap();
    let out = rig.run_args(&[]);
    let said = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "the first upgraded run continues:\n{said}"
    );
    assert!(
        said.contains(&format!("is confirmed at {slot_at}, past {saved}")),
        "one warning names both positions:\n{said}"
    );
    let rewritten: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
    assert_eq!(rewritten["confirmed_first"], true, "{rewritten}");
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (3, 3)"))
        .unwrap();
    rig.run_ok();

    // A real foreign reader now: refused, on every run.
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (4, 4)"))
        .unwrap();
    c.execute(
        "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())",
        &[&slot],
    )
    .unwrap();
    for _ in 0..2 {
        let out = rig.run_args(&[]);
        assert_refused(&out, Refused::by_code("RIVET_SOURCE_CDC_LOG_GAP", 5));
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("another reader consumed"),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// The way back: a checkpoint this binary wrote (marked `confirmed_first`) is read by the
/// released previous version (`RIVET_PREV_RELEASE_BIN`), which continues the stream from it into the
/// same destination: the rig oracle grades that destination against the source.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc + RIVET_PREV_RELEASE_BIN"]
fn the_previous_release_reads_a_checkpoint_this_one_wrote() {
    let tbl = unique_name("rivet_pg_wayback");
    let slot = unique_name("rivet_wayback_slot");
    let _slot = Slot::new(slot.clone());
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("pg");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v BIGINT)"
    ))
    .expect("table");
    let _t = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let ckpts = tempfile::tempdir().unwrap();
    let ckpt = ckpts.path().join("cdc.ckpt");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_cdc(&tbl, &slot)
        .checkpoint_path(ckpt.clone())
        .dest_path(out.path().join("new"));
    rig.run_ok();
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 1)"))
        .unwrap();
    rig.run_ok();
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&ckpt).unwrap()).unwrap();
    assert_eq!(written["confirmed_first"], true, "{written}");
    // The previous release on a state of its own (it cannot open this one's newer schema).
    let back = tempfile::tempdir().unwrap();
    let Some(old) = Rig::pg_cdc(&tbl, &slot)
        .checkpoint_path(ckpt.clone())
        .config_dir(back.path().to_path_buf())
        .dest_path(out.path().join("new"))
        .as_previous_release()
    else {
        return;
    };
    c.batch_execute(&format!("INSERT INTO {tbl} VALUES (2, 2)"))
        .unwrap();
    let ran = old.run_args(&[]);
    assert!(
        ran.status.success(),
        "{}",
        String::from_utf8_lossy(&ran.stderr)
    );
}
