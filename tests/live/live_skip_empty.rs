//! `skip_empty: true` on EVERY runner: a run that delivers nothing is recorded
//! as `skipped` (with a reason), never as `success`.

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

/// `(status, skip_reason)` of the latest metrics row for `export`.
pub(crate) fn latest_status(rig: &Rig, export: &str) -> (String, Option<String>) {
    let db = rig.config_path().parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .query_row(
            "SELECT status, skip_reason FROM export_metrics \
             WHERE export_name = ?1 ORDER BY id DESC LIMIT 1",
            [export],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("a metrics row for the export")
}

/// Export an EMPTY table with `skip_empty: true`; assert the run is `skipped`.
fn pg_empty_run_is_skipped(tag: &str, text_key: bool, mode: &str, lines: &[&str]) {
    require_alive(LiveService::Postgres);
    let table = unique_name(tag);
    let key = if text_key { "TEXT" } else { "BIGINT" };
    pg_connect()
        .batch_execute(&format!(
            "CREATE TABLE {table} (k {key} PRIMARY KEY, payload TEXT NOT NULL);"
        ))
        .unwrap();
    let _guard = PgCleanup(table.clone());

    let export = unique_name(&format!("{tag}_exp"));
    let out = tempfile::tempdir().unwrap();
    let mut rig = Rig::pg_batch(&table)
        .mode(mode)
        .export_named(&export)
        .export_line("skip_empty: true")
        .dest_path(out.path().to_path_buf());
    for l in lines {
        rig = rig.export_line(l);
    }
    let r = rig.run_args(&["--export", &export]);
    assert!(
        r.status.success(),
        "{tag}: an empty export under skip_empty must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    assert_eq!(
        latest_status(&rig, &export),
        (
            "skipped".to_string(),
            Some("source returned 0 rows".to_string())
        ),
        "{tag}: a run that delivered nothing under skip_empty must be recorded as skipped"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_single_runner() {
    pg_empty_run_is_skipped("skip_single", false, "full", &[]);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_keyset_runner() {
    pg_empty_run_is_skipped(
        "skip_keyset",
        true,
        "chunked",
        &["chunk_by_key: k", "chunk_size: 3"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_parallel_keyset_runner() {
    pg_empty_run_is_skipped(
        "skip_keyset_par",
        true,
        "chunked",
        &["chunk_by_key: k", "chunk_size: 3", "parallel: 4"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_chunked_runner() {
    pg_empty_run_is_skipped(
        "skip_chunked",
        false,
        "chunked",
        &["chunk_column: k", "chunk_size: 5"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_parallel_chunked_runner() {
    pg_empty_run_is_skipped(
        "skip_chunked_par",
        false,
        "chunked",
        &["chunk_column: k", "chunk_size: 5", "parallel: 2"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_checkpoint_runner() {
    pg_empty_run_is_skipped(
        "skip_ckpt",
        false,
        "chunked",
        &["chunk_column: k", "chunk_size: 5", "chunk_checkpoint: true"],
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn skip_empty_skips_on_the_parallel_checkpoint_runner() {
    pg_empty_run_is_skipped(
        "skip_ckpt_par",
        false,
        "chunked",
        &[
            "chunk_column: k",
            "chunk_size: 5",
            "chunk_checkpoint: true",
            "parallel: 2",
        ],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn skip_empty_skips_on_the_mongo_parallel_runner() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mskip");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("bench", 1);
    m.delete_one("bench", 1);
    let rig = Rig::mongo_batch("bench")
        .source_url(&MongoTest::url(27017, &db))
        .mongo("page_size: 1000")
        .export_line("parallel: 4")
        .export_line("skip_empty: true");
    let r = rig.run_args(&[]);
    assert!(
        r.status.success(),
        "an empty parallel-Mongo export under skip_empty must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    assert_eq!(latest_status(&rig, "bench").0, "skipped");
    m.drop_database();
}
