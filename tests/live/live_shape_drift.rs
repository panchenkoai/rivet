//! Shape-drift warn (`shape_drift_warn_factor`) on EVERY runner.
//!
//! Each case exports a table whose `payload` values are one byte wide, widens a
//! single row in a MIDDLE chunk / page / range to 200 bytes, and exports again:
//! the second run must warn. The wide row is in neither the first nor the last
//! sink, so a runner that fed only one end of its run stays RED.

use crate::common::*;
use postgres::NoTls;

struct PgCleanup(String);
impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

const WARN: &str = "shape drift in column 'payload'";

/// Run the export twice around a widening of a middle row; return run 2's stderr.
fn pg_second_run_stderr(tag: &str, text_key: bool, mode: &str, lines: &[&str]) -> String {
    require_alive(LiveService::Postgres);
    let table = unique_name(tag);
    let key = if text_key {
        "k TEXT PRIMARY KEY"
    } else {
        "k BIGINT PRIMARY KEY"
    };
    let key_val = if text_key {
        "'k' || lpad(g::text, 4, '0')"
    } else {
        "g"
    };
    // Row 8: chunk 2 of 4 (size 5), page 3 of 7 (size 3) — never an end sink.
    let middle = if text_key { "'k0008'" } else { "8" };
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} ({key}, payload TEXT NOT NULL);
         INSERT INTO {table} SELECT {key_val}, 'p' FROM generate_series(1, 20) g;"
    ))
    .unwrap();
    let _guard = PgCleanup(table.clone());

    let export = unique_name(&format!("{tag}_exp"));
    let out = tempfile::tempdir().unwrap();
    let mut rig = Rig::pg_batch(&table)
        .mode(mode)
        .export_named(&export)
        .dest_path(out.path().to_path_buf());
    for l in lines {
        rig = rig.export_line(l);
    }
    let r1 = rig.run_args(&["--export", &export]);
    assert!(
        r1.status.success(),
        "run 1 must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    c.batch_execute(&format!(
        "UPDATE {table} SET payload = repeat('x', 200) WHERE k = {middle};"
    ))
    .unwrap();
    let r2 = rig.run_args(&["--export", &export]);
    let stderr = String::from_utf8_lossy(&r2.stderr).into_owned();
    assert!(r2.status.success(), "run 2 must succeed; stderr:\n{stderr}");
    stderr
}

fn assert_warns(runner: &str, stderr: &str) {
    assert!(
        stderr.contains(WARN),
        "{runner}: a 200× growth of `payload` in a middle sink must warn `{WARN}`; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_single_runner() {
    let e = pg_second_run_stderr("shape_single", false, "full", &[]);
    assert_warns("single", &e);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_keyset_runner() {
    let lines = ["chunk_by_key: k", "chunk_size: 3"];
    let e = pg_second_run_stderr("shape_keyset", true, "chunked", &lines);
    assert_warns("keyset", &e);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_parallel_keyset_runner() {
    let lines = ["chunk_by_key: k", "chunk_size: 3", "parallel: 4"];
    let e = pg_second_run_stderr("shape_keyset_par", true, "chunked", &lines);
    assert_warns("keyset-parallel", &e);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_chunked_runner() {
    let lines = ["chunk_column: k", "chunk_size: 5"];
    let e = pg_second_run_stderr("shape_chunked", false, "chunked", &lines);
    assert_warns("chunked", &e);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_parallel_chunked_runner() {
    let lines = ["chunk_column: k", "chunk_size: 5", "parallel: 2"];
    let e = pg_second_run_stderr("shape_chunked_par", false, "chunked", &lines);
    assert_warns("chunked-parallel", &e);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_checkpoint_runner() {
    let lines = ["chunk_column: k", "chunk_size: 5", "chunk_checkpoint: true"];
    let e = pg_second_run_stderr("shape_ckpt", false, "chunked", &lines);
    assert_warns("chunked-checkpoint", &e);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn shape_drift_warns_on_the_parallel_checkpoint_runner() {
    let lines = [
        "chunk_column: k",
        "chunk_size: 5",
        "chunk_checkpoint: true",
        "parallel: 2",
    ];
    let e = pg_second_run_stderr("shape_ckpt_par", false, "chunked", &lines);
    assert_warns("chunked-checkpoint-parallel", &e);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn shape_drift_warns_on_the_mongo_parallel_runner() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mshape");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("bench", 4000);
    let rig = Rig::mongo_batch("bench")
        .source_url(&MongoTest::url(27017, &db))
        .mongo("page_size: 1000")
        .export_line("parallel: 4");
    rig.run_ok();
    // _id 1500: the second of four worker ranges — neither end.
    m.upsert_set("bench", 1500, "wide", &"x".repeat(5000));
    let r2 = rig.run_args(&[]);
    let stderr = String::from_utf8_lossy(&r2.stderr);
    assert!(r2.status.success(), "run 2 must succeed; stderr:\n{stderr}");
    assert!(
        stderr.contains("shape drift in column 'document'"),
        "mongo-parallel: a 5000-byte document in a middle worker's range must warn; stderr:\n{stderr}"
    );
    m.drop_database();
}
