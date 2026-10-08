//! Cells for guarantees the docs state in so many words and no test held: one generator per
//! sentence, one cell per engine the sentence covers. A cell asserts what the sentence says; one
//! that fails today is an `open_defect_*` cell acknowledged in dev/release_oracle/known_red.py.
//!
//! Oracles: the destination re-read file by file, the plan artifact as written, and a TCP
//! listener standing where the source would be.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::*;

const ROWS: i64 = 12;
const MONGO_PORT: u16 = 27017;

/// Every file under `dir` whose name satisfies `keep`, sorted.
fn files_under(dir: &Path, keep: &dyn Fn(&str) -> bool) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if keep(&p.file_name().unwrap_or_default().to_string_lossy()) {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// The schema and the sorted rows of every part in `parts`, each cell as Arrow renders it.
fn events(parts: &[PathBuf]) -> (Vec<String>, Vec<Vec<String>>) {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let (mut schema, mut rows) = (Vec::new(), Vec::new());
    for part in parts {
        let file = std::fs::File::open(part).unwrap();
        for batch in ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
        {
            let b = batch.unwrap();
            schema = b
                .schema()
                .fields()
                .iter()
                .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                .collect();
            for r in 0..b.num_rows() {
                rows.push(
                    b.columns()
                        .iter()
                        .map(|c| arrow::util::display::array_value_to_string(c, r).unwrap())
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
    rows.sort();
    (schema, rows)
}

/// A CDC run over two new rows killed at `hook`: the scenario, and the directory the killed run wrote into.
fn killed_cdc_run(mut s: CdcScenario, hook: &str) -> (CdcScenario, PathBuf) {
    s.rig.run_ok();
    let failed_dir = s.rig.resume_into_fresh_dest();
    s.insert(1);
    s.insert(2);
    s.settle();
    let failed = s.rig.run_with_envs(&[("RIVET_TEST_PANIC_AT", hook)]);
    assert!(!failed.status.success(), "fixture: the run dies at {hook}");
    (s, failed_dir)
}

/// docs/reference/cdc.md: a failed CDC run leaves its parts and no `manifest.json` / `_SUCCESS`,
/// and the event it wrote is re-delivered byte-identical by the next run.
fn a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike(s: CdcScenario) {
    let (mut s, failed_dir) = killed_cdc_run(s, "cdc_after_flush_before_ack");
    let parts = files_under(&failed_dir, &|n| n.ends_with(".parquet"));
    let (schema, written) = events(&parts);
    assert_eq!(
        written.len(),
        2,
        "a failed CDC run leaves its durable parts in the destination: {parts:?}"
    );
    assert_eq!(
        files_under(&failed_dir, &|n| n == "manifest.json" || n == "_SUCCESS"),
        Vec::<PathBuf>::new(),
        "a failed CDC run leaves no manifest.json and no _SUCCESS"
    );

    let again_dir = s.rig.resume_into_fresh_dest();
    s.rig.run_ok();
    let again = files_under(&again_dir, &|n| n.ends_with(".parquet"));
    assert_eq!(
        events(&again),
        (schema, written),
        "the re-delivered events are byte-identical to the ones the failed run wrote"
    );
}

/// The same sentence one step later: a run killed after its checkpoint and before the acknowledgement is a failed run too.
fn a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker(s: CdcScenario) {
    let (_s, failed_dir) = killed_cdc_run(s, "cdc_after_checkpoint_before_ack");
    assert_eq!(
        files_under(&failed_dir, &|n| n.ends_with(".parquet")).len(),
        1,
        "fixture: the killed run wrote its part"
    );
    assert_eq!(
        files_under(&failed_dir, &|n| n == "manifest.json" || n == "_SUCCESS"),
        Vec::<PathBuf>::new(),
        "a failed CDC run leaves no manifest.json and no _SUCCESS"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker_postgres() {
    a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker(CdcScenario::pg_with(
        "guar_ack",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r,
    ));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker_mysql() {
    a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker(CdcScenario::mysql_with(
        "guar_ack",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, _| r,
    ));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker(CdcScenario::mssql_with(
        "guar_ack",
        "id BIGINT PRIMARY KEY, v BIGINT",
        |r, t| r.repoint(&format!("dbo.{t}")),
    ));
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker_mongo() {
    a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker(CdcScenario::mongo_with(
        "guar_ack",
        |r, _| r,
    ));
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    a_cdc_run_killed_after_its_checkpoint_leaves_no_manifest_or_marker(CdcScenario::oracle_with(
        "guar_ack",
        |r, _| r,
    ));
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike_postgres() {
    a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike(
        CdcScenario::pg_with("guar_fail", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| r),
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc"]
fn a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike_mysql() {
    a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike(
        CdcScenario::mysql_with("guar_fail", "id BIGINT PRIMARY KEY, v BIGINT", |r, _| r),
    );
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike(
        CdcScenario::mssql_with("guar_fail", "id BIGINT PRIMARY KEY, v BIGINT", |r, t| {
            r.repoint(&format!("dbo.{t}"))
        }),
    );
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike_mongo() {
    a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike(
        CdcScenario::mongo_with("guar_fail", |r, _| r),
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle (LogMiner)"]
fn a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    a_failed_cdc_run_leaves_parts_without_a_manifest_and_redelivers_them_alike(
        CdcScenario::oracle_with("guar_fail", |r, _| r),
    );
}

/// `url` with its host and port replaced by `127.0.0.1:<port>`.
fn at_port(url: &str, port: u16) -> String {
    let start = url
        .rfind('@')
        .map_or_else(|| url.find("://").expect("a URL") + 3, |at| at + 1);
    let rest = &url[start..];
    let end = rest.find(['/', '?', ';']).unwrap_or(rest.len());
    format!("{}127.0.0.1:{port}{}", &url[..start], &rest[end..])
}

/// A listener on a free local port that counts the connections it accepts and closes each at once.
fn recording_listener() -> (u16, Arc<AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(AtomicUsize::new(0));
    let count = seen.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            count.fetch_add(1, Ordering::SeqCst);
            drop(conn);
        }
    });
    (port, seen)
}

/// A local port nothing listens on.
fn closed_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// docs/cloud-destinations.md: `rivet validate` never queries the source. The verdict over a
/// complete export is the same with the source unreachable, and a listener standing where the
/// source would be records no connection from it.
fn validate_never_queries_the_source(mut rig: Rig, url: &str) {
    rig.run_ok();
    let verdict = |o: &std::process::Output| {
        (
            o.status.code(),
            String::from_utf8_lossy(&o.stdout).into_owned(),
        )
    };
    let honest = rig.cli(&["validate"]);
    assert_eq!(
        honest.status.code(),
        Some(0),
        "fixture: validate passes over the complete export\n{}",
        String::from_utf8_lossy(&honest.stderr)
    );

    let dead = at_port(url, closed_port());
    rig.rebuilt(|r| r.source_url(&dead));
    assert_eq!(
        verdict(&rig.cli(&["validate"])),
        verdict(&honest),
        "validate gives the same verdict with the source unreachable"
    );

    let (port, seen) = recording_listener();
    let watched = at_port(url, port);
    rig.rebuilt(|r| r.source_url(&watched));
    let blind = rig.cli(&["validate"]);
    let during_validate = seen.load(Ordering::SeqCst);
    assert_eq!(
        verdict(&blind),
        verdict(&honest),
        "validate gives the same verdict with a listener in the source's place"
    );
    assert_eq!(
        during_validate, 0,
        "validate opened {during_validate} connection(s) to the source"
    );
    let check = rig.cli(&["check"]);
    assert!(
        seen.load(Ordering::SeqCst) > 0 && !check.status.success(),
        "fixture: a command that does read the source reaches the listener, so the zero above \
         is a measurement"
    );
}

/// A complete chunked export of a fresh `(id, v)` table on `engine`, unrun.
fn sql_export(engine: SqlEngine, tag: &str) -> (Rig, Box<dyn std::any::Any>) {
    engine.alive();
    let (table, guard) = engine.table(tag);
    engine.insert(&table, 1..=ROWS, 180, Some(10));
    (engine.rig(&table), guard)
}

/// A fresh database on the standalone MongoDB with `ROWS` documents in `t`: its URL and drop guard.
fn mongo_db(tag: &str) -> (String, MongoDbGuard) {
    require_alive(LiveService::Mongo);
    let db = unique_name(tag);
    let guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    MongoTest::connect(MONGO_PORT, &db).seed_int_id("t", ROWS);
    (MongoTest::url(MONGO_PORT, &db), guard)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_never_queries_the_source_postgres() {
    let (rig, _guard) = sql_export(SqlEngine::Pg, "guar_val");
    validate_never_queries_the_source(rig, SqlEngine::Pg.url());
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn validate_never_queries_the_source_mysql() {
    let (rig, _guard) = sql_export(SqlEngine::Mysql, "guar_val");
    validate_never_queries_the_source(rig, SqlEngine::Mysql.url());
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn validate_never_queries_the_source_mssql() {
    let (rig, _guard) = sql_export(SqlEngine::Mssql, "guar_val");
    validate_never_queries_the_source(rig, SqlEngine::Mssql.url());
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn validate_never_queries_the_source_oracle() {
    let (rig, _guard) = sql_export(SqlEngine::Oracle, "guar_val");
    validate_never_queries_the_source(rig, SqlEngine::Oracle.url());
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn validate_never_queries_the_source_mongo() {
    let (url, _guard) = mongo_db("guar_val");
    validate_never_queries_the_source(Rig::mongo_batch("t").source_url(&url), &url);
}

/// docs/reference/cli.md: the `RIVET_STATE_URL` value is not embedded in plan artifacts or config
/// files. A plan sealed under a state URL carries none of its database, its marker or its address.
fn a_sealed_plan_carries_no_state_url(rig: Rig) {
    let state = ScratchStateDb::new("guar_plan");
    let marker = unique_name("guar_state_marker");
    let url = format!("{}?application_name={marker}", state.url());
    let dir = tempfile::tempdir().unwrap();
    let plan = dir.path().join("plan.json");
    let out = rig.plan_json_env(&plan, &[], &[("RIVET_STATE_URL", &url)]);
    assert!(
        out.status.success(),
        "fixture: the plan seals\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let written: Vec<PathBuf> = files_under(rig.config_path().parent().unwrap(), &|n| {
        n.ends_with(".yaml") || n.ends_with(".json")
    })
    .into_iter()
    .chain([plan.clone()])
    .collect();
    let leaks: Vec<String> = written
        .iter()
        .flat_map(|f| {
            let text = std::fs::read_to_string(f).unwrap_or_default();
            [marker.as_str(), state.name.as_str(), "127.0.0.1:5433"]
                .into_iter()
                .filter(move |needle| text.contains(needle))
                .map(move |needle| format!("{}: {needle}", f.display()))
        })
        .collect();
    assert_eq!(
        leaks,
        Vec::<String>::new(),
        "the state URL reached a plan artifact or a config file"
    );
    assert!(
        std::fs::metadata(&plan).is_ok_and(|m| m.len() > 0),
        "fixture: the plan artifact was written and read"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_sealed_plan_carries_no_state_url_postgres() {
    let (rig, _guard) = sql_export(SqlEngine::Pg, "guar_plan");
    a_sealed_plan_carries_no_state_url(rig);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_sealed_plan_carries_no_state_url_mysql() {
    let (rig, _guard) = sql_export(SqlEngine::Mysql, "guar_plan");
    a_sealed_plan_carries_no_state_url(rig);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_sealed_plan_carries_no_state_url_mssql() {
    let (rig, _guard) = sql_export(SqlEngine::Mssql, "guar_plan");
    a_sealed_plan_carries_no_state_url(rig);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_sealed_plan_carries_no_state_url_oracle() {
    let (rig, _guard) = sql_export(SqlEngine::Oracle, "guar_plan");
    a_sealed_plan_carries_no_state_url(rig);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn a_sealed_plan_carries_no_state_url_mongo() {
    let (url, _guard) = mongo_db("guar_plan");
    a_sealed_plan_carries_no_state_url(Rig::mongo_batch("t").source_url(&url));
}

/// `at_port` swaps the authority of each URL shape the stand uses and nothing else.
#[test]
fn at_port_replaces_only_the_host_and_port() {
    assert_eq!(
        at_port("sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet", 9),
        "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:9/rivet"
    );
    assert_eq!(
        at_port("mongodb://127.0.0.1:27017/db?directConnection=true", 9),
        "mongodb://127.0.0.1:9/db?directConnection=true"
    );
    assert_eq!(
        at_port("postgresql://u:p@db.internal:5432/x", 9),
        "postgresql://u:p@127.0.0.1:9/x"
    );
}
