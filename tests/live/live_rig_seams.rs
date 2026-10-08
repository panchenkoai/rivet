//! The Rig's cross-command seams, each on a state green on main: `run_after_doctor` (doctor and
//! the run it predicts agree) on every CDC engine, and `pg_cdc_standby` + `run_nudged` taking an
//! `initial: snapshot` stream from the cdc-standby replica. Oracles: the rig oracle on every run
//! (source against the declared parts), the manifests, and the standby's own catalog.

use crate::common::*;

/// Doctor is green on a fresh stream and the run it predicts succeeds (its rows graded by the rig oracle).
fn doctor_green_then_run_ok(rig: &Rig) {
    let (green, run) = rig.run_after_doctor();
    assert!(
        green && run.status.success(),
        "a fresh stream: doctor all_ok={green}, run ok={}:\n{}",
        run.status.success(),
        String::from_utf8_lossy(&run.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn doctor_and_the_run_it_predicts_agree_on_a_fresh_postgres_stream() {
    let mut s = CdcScenario::pg_with("seam_doc_pg", "id INT PRIMARY KEY, v INT", |r, _| r);
    s.insert(1);
    doctor_green_then_run_ok(&s.rig);
    assert_eq!(
        read_cdc_changes(&s.rig.out_dir()).len(),
        1,
        "the run delivered the one change"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn doctor_and_the_run_it_predicts_agree_on_a_fresh_mysql_stream() {
    let mut s = CdcScenario::mysql_with("seam_doc_my", "id INT PRIMARY KEY, v INT", |r, _| r);
    s.rig.run_ok();
    s.insert(1);
    doctor_green_then_run_ok(&s.rig);
    assert_eq!(
        read_cdc_changes(&s.rig.out_dir()).len(),
        1,
        "the run delivered the one change"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mssql-cdc with SQL Server Agent"]
fn doctor_and_the_run_it_predicts_agree_on_a_fresh_sql_server_stream() {
    let mut s = CdcScenario::mssql_with("seam_doc_ms", "id INT PRIMARY KEY, v INT", |r, _| r);
    s.rig.run_ok();
    s.insert(1);
    s.settle();
    doctor_green_then_run_ok(&s.rig);
    assert_eq!(
        read_cdc_changes(&s.rig.out_dir()).len(),
        1,
        "the run delivered the one change"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn doctor_and_the_run_it_predicts_agree_on_a_fresh_mongo_stream() {
    let mut s = CdcScenario::mongo_with("seam_doc_mg", |r, _| r);
    s.rig.run_ok();
    s.insert(1);
    doctor_green_then_run_ok(&s.rig);
    assert_eq!(
        read_mongo_cdc_changes(&s.rig.out_dir()).len(),
        1,
        "the run delivered the one change"
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn doctor_and_the_run_it_predicts_agree_on_a_fresh_oracle_stream() {
    let t = crate::live_cdc_oracledb::cdc_table(
        "seam_doc_or",
        "id NUMBER(18) PRIMARY KEY, v NUMBER(18)",
    );
    let rig = Rig::oracle_cdc(t.name());
    rig.run_ok();
    ora_exec(&format!("INSERT INTO {} VALUES (1, 1)", t.name()));
    doctor_green_then_run_ok(&rig);
    assert_eq!(
        read_cdc_changes(&rig.out_dir()).len(),
        1,
        "the run delivered the one change"
    );
}

/// The cdc-standby replica serves an `initial: snapshot` stream run continuously: the baseline,
/// then a change made on the primary, from a slot the standby holds.
#[test]
#[ignore = "live+gate-only: requires the cdc-standby profile — python3 -m dev.pytools.cdc_stand standby (pg-cdc-primary :5437 → pg-cdc-standby :5436)"]
fn an_initial_snapshot_stream_runs_continuously_from_a_standby() {
    let mut p = postgres::Client::connect(PG_STANDBY_PRIMARY_URL, postgres::NoTls)
        .expect("fixture: the cdc-standby primary (:5437) is up");
    let mut sb = postgres::Client::connect(PG_STANDBY_URL, postgres::NoTls)
        .expect("fixture: the cdc-standby replica (:5436) is up");
    let tbl = unique_name("seam_standby_snap");
    let slot = unique_name("seam_standby_slot");
    let _slot = Slot::on(PG_STANDBY_URL, slot.clone());
    p.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); \
         INSERT INTO {tbl} VALUES (1, 10), (2, 20)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(PG_STANDBY_PRIMARY_URL, tbl.clone());
    let replicated = |sb: &mut postgres::Client, want: i64| {
        for _ in 0..60 {
            let q = format!("SELECT COUNT(*) FROM {tbl}");
            if sb.query_one(&q, &[]).ok().map(|r| r.get::<_, i64>(0)) == Some(want) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        panic!("fixture: the standby never held {want} rows of {tbl}");
    };
    replicated(&mut sb, 2);
    let rig = Rig::pg_cdc_standby(&tbl, &slot)
        .continuous()
        .cdc("initial: snapshot");
    let say = |o: &std::process::Output| String::from_utf8_lossy(&o.stderr).to_string();
    let first = rig.run_nudged(&[]);
    assert!(
        first.status.success(),
        "the first standby run:\n{}",
        say(&first)
    );
    let held: bool = sb
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(held, "the stream's slot is on the standby, not the primary");
    p.batch_execute(&format!("INSERT INTO {tbl} VALUES (3, 30)"))
        .unwrap();
    replicated(&mut sb, 3);
    let second = rig.run_nudged(&[]);
    assert!(
        second.status.success(),
        "the second standby run:\n{}",
        say(&second)
    );
    assert_eq!(
        manifest_rows(&rig.out_dir().join("snapshot")),
        2,
        "the baseline leg holds the rows the standby had before the stream opened"
    );
    assert_eq!(
        manifest_rows(&rig.out_dir()),
        1,
        "the second run delivered the change made on the primary"
    );
}

/// `Remedy::in_place` after an earlier remedy already lifted the refusal is not a walk: the rig panics instead of grading a remedy that changed nothing.
#[test]
#[ignore = "live: requires docker compose postgres"]
#[should_panic(expected = "the refused state did not come back before remedy")]
fn an_in_place_remedy_after_the_refusal_was_lifted_is_not_graded() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let sqlite = [("RIVET_STATE_URL", "")];
    let mut rig = Rig::pg_batch(table.name());
    assert!(rig.run_with_envs(&sqlite).status.success());
    let db = rig.config_path().with_file_name(".rivet_state.db");
    rusqlite::Connection::open(&db)
        .and_then(|c| c.execute("UPDATE schema_version SET version = version + 100", []))
        .expect("a newer state schema");
    let fresh_state = "point this one at a state DB it created";
    rig.refuses_twice_then(
        &["run"],
        &sqlite,
        Refused::by_code("RIVET_STATE_SCHEMA_NEWER", 5),
        vec![
            Remedy::new(fresh_state, Then::DeliversTheSource, |_| {
                std::fs::remove_file(&db).expect("remove the newer state database");
            }),
            Remedy::new(fresh_state, Then::DeliversTheSource, |_| {}).in_place(),
        ],
    );
}
