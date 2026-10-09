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

const WIRE_ROWS: i64 = 4000;
const WIRE_PAGE: i64 = 200;
const WIRE_PAGES: u64 = (WIRE_ROWS / WIRE_PAGE) as u64;

/// The bytes of each answer the source gave to one request, over every connection through a [`wire_to`] forwarder.
type Answers = Arc<std::sync::Mutex<Vec<u64>>>;

/// Every byte the clients sent through a [`wire_to`] forwarder, in the order it read them.
type Asked = Arc<std::sync::Mutex<Vec<u8>>>;

/// Copy `from` to `to` on a thread, reporting each read to `seen` before it is passed on (a request closes the answer before the source can start the next one) and the end of the stream as an empty read.
fn pipe(
    mut from: std::net::TcpStream,
    mut to: std::net::TcpStream,
    mut seen: impl FnMut(&[u8]) + Send + 'static,
) {
    use std::io::{Read, Write};
    std::thread::spawn(move || {
        let mut buf = [0u8; 65536];
        while let Ok(n) = from.read(&mut buf) {
            if n == 0 {
                break;
            }
            seen(&buf[..n]);
            if to.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        let _ = to.shutdown(std::net::Shutdown::Both);
        let _ = from.shutdown(std::net::Shutdown::Both);
        seen(&[]);
    });
}

/// A loopback forwarder to local `port`: its own port, the size of each answer the source gives through it, and what the clients sent.
fn wire_to(port: u16) -> (u16, Answers, Asked) {
    // DuckDB's MySQL reader, which the rig's oracle attaches with, refuses a port above 65353.
    let listener = std::iter::repeat_with(|| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .find(|l| l.local_addr().unwrap().port() <= 65353)
        .unwrap();
    let via = listener.local_addr().unwrap().port();
    let (answers, asked) = (Answers::default(), Asked::default());
    let (all, sent) = (answers.clone(), asked.clone());
    std::thread::spawn(move || {
        for down in listener.incoming().flatten() {
            let Ok(up) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
                continue;
            };
            let open = Arc::new(AtomicUsize::new(0));
            let close = |all: &Answers, open: &AtomicUsize| {
                let answer = open.swap(0, Ordering::SeqCst) as u64;
                if answer > 0 {
                    all.lock().unwrap().push(answer);
                }
            };
            let (asking, answered) = ((all.clone(), open.clone()), (all.clone(), open));
            let sent = sent.clone();
            pipe(
                down.try_clone().unwrap(),
                up.try_clone().unwrap(),
                move |read| {
                    if !read.is_empty() {
                        sent.lock().unwrap().extend_from_slice(read);
                        close(&asking.0, &asking.1);
                    }
                },
            );
            pipe(up, down, move |read| {
                if read.is_empty() {
                    close(&answered.0, &answered.1);
                } else {
                    answered.1.fetch_add(read.len(), Ordering::SeqCst);
                }
            });
        }
    });
    (via, answers, asked)
}

/// The FETCH statements a PostgreSQL client sent inside each transaction that declared rivet's cursor, up to its COMMIT.
fn fetches_per_cursor_transaction(asked: &[u8]) -> Vec<usize> {
    let find = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).position(|w| w == needle);
    let (mut out, mut rest) = (Vec::new(), asked);
    while let Some(at) = find(rest, b"DECLARE _rivet") {
        rest = &rest[at + 1..];
        let end = find(rest, b"COMMIT\0").unwrap_or(rest.len());
        out.push(rest[..end].windows(6).filter(|w| w == b"FETCH ").count());
        rest = &rest[end..];
    }
    out
}

/// The longest single answer and the bytes of all of them.
fn longest_and_total(answers: &[u64]) -> (u64, u64) {
    (
        answers.iter().copied().max().unwrap_or(0),
        answers.iter().sum(),
    )
}

/// Whether the longest answer is one page of a `pages`-page read: at most half again over an even share of every byte the source sent.
fn is_one_page(longest: u64, total: u64, pages: u64) -> bool {
    longest * pages * 2 <= total * 3
}

/// What `mode: full` holds on the source in one statement: a `batch_size` page, a fetch array the driver sizes (measured at 4.5% and 7.5% of the table on two servers), or the table.
#[derive(Clone, Copy)]
enum Holds {
    OnePage,
    #[cfg(feature = "oracle")]
    LessThanHalf,
    TheTable,
}

/// Whether the longest answer carried more than half of every byte the source sent: the table in one statement, not a fetch at a time.
fn is_most_of_the_table(longest: u64, total: u64) -> bool {
    longest * 2 > total
}

/// Run `rig` with its source behind a forwarder to `port`: the size of each answer the source gave the rivet process and every byte the process sent, once the run delivered `WIRE_ROWS` rows.
fn a_run_on_the_wire(rig: Rig, url: &str, port: u16) -> (Vec<u64>, Vec<u8>) {
    let (via, answers, asked) = wire_to(port);
    let rig = rig.source_url(&at_port(url, via));
    let mut run = rig.spawn_args_env(&[], &[]);
    // The rig's oracle reads the source through the same URL before and after the process.
    answers.lock().unwrap().clear();
    std::process::Child::wait(&mut run).expect("rivet ran");
    let seen = (
        answers.lock().unwrap().clone(),
        asked.lock().unwrap().clone(),
    );
    assert!(
        run.wait().expect("rivet ran").success(),
        "fixture: the run succeeds"
    );
    let rows = events(&files_with_extension(&rig.out_dir(), "parquet"))
        .1
        .len();
    assert_eq!(rows, WIRE_ROWS as usize, "fixture: the run read the table");
    seen
}

/// The longest answer the source gave a run of `rig` behind a forwarder to `port`, and the bytes of all of them.
fn longest_answer_of_a_run(rig: Rig, url: &str, port: u16) -> (u64, u64) {
    let (longest, total) = longest_and_total(&a_run_on_the_wire(rig, url, port).0);
    eprintln!("longest answer {longest} of {total} bytes");
    assert!(
        total > WIRE_ROWS as u64 * 8,
        "fixture: the forwarder carried the table ({total} bytes)"
    );
    (longest, total)
}

/// A fresh `WIRE_ROWS`-row table on `engine`, every row the same width on the wire.
fn wire_table(engine: SqlEngine) -> (String, Box<dyn std::any::Any>) {
    engine.alive();
    let (table, guard) = engine.range_table("guar_page");
    for lo in (1000..1000 + WIRE_ROWS).step_by(500) {
        engine.insert(&table, lo..=lo + 499, 180, Some(10));
    }
    (table, guard)
}

/// docs/why/source-safe-under-load.md: the longest query rivet holds open on the source is a
/// single page. Each paged shape reads a 20-page table through a forwarder; no single answer of
/// the source may be longer than one page's share of the bytes it sent.
fn the_longest_statement_of_a_paged_export_is_one_page(engine: SqlEngine) {
    let (table, _guard) = wire_table(engine);
    let size = format!("chunk_size: {WIRE_PAGE}");
    for key in ["chunk_by_key: id", "chunk_column: id"] {
        let rig = engine.staged(engine.rig(&table), "chunked", &[key, &size]);
        let (longest, total) = longest_answer_of_a_run(rig, engine.url(), engine.default_port());
        assert!(
            is_one_page(longest, total, WIRE_PAGES),
            "{key}: the longest statement held on the source answered {longest} of {total} \
             bytes, more than one page of {WIRE_PAGES}"
        );
    }
}

/// docs/partitioning.md against the same sentence: what `mode: full` holds on the source. An
/// engine read through a cursor answers one fetch at a time; the others answer the whole table
/// to a single statement.
fn mode_full_documents_the_longest_statement_it_holds(engine: SqlEngine, holds: Holds) {
    let (table, _guard) = wire_table(engine);
    let rig = engine
        .rig(&table)
        .export_line(&format!("tuning: {{batch_size: {WIRE_PAGE}}}"));
    let (longest, total) = longest_answer_of_a_run(rig, engine.url(), engine.default_port());
    let held = match holds {
        Holds::OnePage => is_one_page(longest, total, WIRE_PAGES),
        #[cfg(feature = "oracle")]
        Holds::LessThanHalf => !is_most_of_the_table(longest, total),
        Holds::TheTable => is_most_of_the_table(longest, total),
    };
    assert!(
        held,
        "mode: full answered {longest} of {total} bytes to its longest statement"
    );
}

/// docs/why/source-safe-under-load.md: on PostgreSQL `mode: full` reads the whole table inside
/// one transaction, and a chunked export opens one per page. Counted in what the rivet process
/// sent: the transactions that declare its cursor, and the FETCH statements inside each.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn mode_full_reads_in_one_transaction_and_chunked_in_one_per_page_postgres() {
    let engine = SqlEngine::Pg;
    let (table, _guard) = wire_table(engine);
    let full = engine
        .rig(&table)
        .export_line(&format!("tuning: {{batch_size: {WIRE_PAGE}}}"));
    let asked = a_run_on_the_wire(full, engine.url(), engine.default_port()).1;
    let per = fetches_per_cursor_transaction(&asked);
    assert!(
        per.len() == 1 && per[0] as u64 >= WIRE_PAGES,
        "mode: full read the table in {} cursor transactions holding {per:?} FETCH statements, \
         not in one holding all {WIRE_PAGES} pages",
        per.len()
    );
    let size = format!("chunk_size: {WIRE_PAGE}");
    for key in ["chunk_by_key: id", "chunk_column: id"] {
        let rig = engine.staged(engine.rig(&table), "chunked", &[key, &size]);
        let asked = a_run_on_the_wire(rig, engine.url(), engine.default_port()).1;
        let per = fetches_per_cursor_transaction(&asked);
        assert!(
            per.len() as u64 >= WIRE_PAGES,
            "{key}: {} cursor transactions for {WIRE_PAGES} pages, not one per page ({per:?})",
            per.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn the_longest_statement_of_a_paged_export_is_one_page_postgres() {
    the_longest_statement_of_a_paged_export_is_one_page(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn the_longest_statement_of_a_paged_export_is_one_page_mysql() {
    the_longest_statement_of_a_paged_export_is_one_page(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn the_longest_statement_of_a_paged_export_is_one_page_mssql() {
    the_longest_statement_of_a_paged_export_is_one_page(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_longest_statement_of_a_paged_export_is_one_page_oracle() {
    the_longest_statement_of_a_paged_export_is_one_page(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn mode_full_documents_the_longest_statement_it_holds_postgres() {
    mode_full_documents_the_longest_statement_it_holds(SqlEngine::Pg, Holds::OnePage);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mode_full_documents_the_longest_statement_it_holds_mysql() {
    mode_full_documents_the_longest_statement_it_holds(SqlEngine::Mysql, Holds::TheTable);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mode_full_documents_the_longest_statement_it_holds_mssql() {
    mode_full_documents_the_longest_statement_it_holds(SqlEngine::Mssql, Holds::TheTable);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn mode_full_documents_the_longest_statement_it_holds_oracle() {
    mode_full_documents_the_longest_statement_it_holds(SqlEngine::Oracle, Holds::LessThanHalf);
}

/// A fresh database on the standalone MongoDB with `WIRE_ROWS` documents of `pad` bytes in `t`: its URL and drop guard.
fn wire_collection(pad: usize) -> (String, MongoDbGuard) {
    require_alive(LiveService::Mongo);
    let db = unique_name("guar_page");
    let guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    MongoTest::connect(MONGO_PORT, &db).append_padded("t", 1..=WIRE_ROWS as usize, pad);
    (MongoTest::url(MONGO_PORT, &db), guard)
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn the_longest_statement_of_a_paged_export_is_one_page_mongo() {
    // 2 kB documents: a page outweighs the server's status answers.
    let (url, _guard) = wire_collection(2000);
    let rig = Rig::mongo_batch("t").mongo(&format!("page_size: {WIRE_PAGE}"));
    let (longest, total) = longest_answer_of_a_run(rig, &url, MONGO_PORT);
    assert!(
        is_one_page(longest, total, WIRE_PAGES),
        "page_size: the longest statement held on the source answered {longest} of {total} \
         bytes, more than one page of {WIRE_PAGES}"
    );
}

/// The most a MongoDB server puts in one reply (16 MiB of documents and its envelope).
const MONGO_REPLY: u64 = 16 * 1024 * 1024 + 64 * 1024;

/// MongoDB sets no cursor batch size: `mode: full` holds one server reply of up to 16 MiB per statement, neither a `batch_size` page nor the collection.
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mode_full_documents_the_longest_statement_it_holds_mongo() {
    let (url, _guard) = wire_collection(10_000);
    let rig = Rig::mongo_batch("t").export_line(&format!("tuning: {{batch_size: {WIRE_PAGE}}}"));
    let (longest, total) = longest_answer_of_a_run(rig, &url, MONGO_PORT);
    assert!(
        total > 2 * MONGO_REPLY
            && longest <= MONGO_REPLY
            && !is_one_page(longest, total, WIRE_PAGES),
        "mode: full answered {longest} of {total} bytes to its longest statement"
    );
}

/// The backend behind the transaction pooler and every setting of its session.
fn pooled_session() -> (i32, Vec<(String, String)>) {
    let mut c = postgres::Client::connect(PGBOUNCER_URL, postgres::NoTls).expect("pgbouncer");
    let pid = c.query_one("SELECT pg_backend_pid()", &[]).unwrap().get(0);
    let settings = c
        .query("SELECT name, setting FROM pg_settings ORDER BY name", &[])
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    (pid, settings)
}

/// docs/concepts.md: Postgres session state is never leaked into the pool. Each batch shape runs
/// through a transaction-mode pooler with one server connection, delivers the table, and leaves
/// every setting of that connection as it found it.
#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn every_batch_shape_through_a_transaction_pooler_leaves_its_session_as_it_was_postgres() {
    let _alone = pgbouncer_alone();
    let engine = SqlEngine::Pg;
    engine.alive();
    let (table, _guard) = engine.range_table("guar_pool");
    engine.insert(&table, 1..=ROWS, 180, Some(10));
    let before = pooled_session();
    let shapes: [(&str, &[&str]); 4] = [
        ("full", &[]),
        ("chunked", &["chunk_by_key: id", "chunk_size: 5"]),
        ("chunked", &["chunk_column: id", "chunk_size: 5"]),
        ("incremental", &["cursor_column: id"]),
    ];
    for (mode, lines) in shapes {
        let rig = engine
            .staged(engine.rig(&table), mode, lines)
            .source_url(PGBOUNCER_URL)
            .export_line("tuning: {statement_timeout_s: 300, lock_timeout_s: 30}");
        rig.run_ok();
        assert_eq!(
            read_ids(&rig.out_dir()),
            (1..=ROWS).collect::<Vec<_>>(),
            "{mode} {lines:?}: every row arrives through the pooler"
        );
        assert_eq!(
            pooled_session(),
            before,
            "{mode} {lines:?}: the run changed the session of the pooled connection"
        );
    }
}

/// The forwarder closes an answer at the next request, and one page is told from two.
#[test]
fn an_answer_ends_at_the_next_request_and_two_pages_are_not_one() {
    use std::io::{Read, Write};
    let server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = server.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let (mut conn, _) = server.accept().unwrap();
        let mut ask = [0u8; 1];
        for answer in [300usize, 7] {
            conn.read_exact(&mut ask).unwrap();
            conn.write_all(&vec![0u8; answer]).unwrap();
        }
    });
    let (via, answers, asked) = wire_to(port);
    let mut client = std::net::TcpStream::connect(("127.0.0.1", via)).unwrap();
    for answer in [300usize, 7] {
        client.write_all(b"?").unwrap();
        client.read_exact(&mut vec![0u8; answer]).unwrap();
    }
    client.write_all(b"?").unwrap();
    drop(client);
    for _ in 0..200 {
        if answers.lock().unwrap().len() == 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(*answers.lock().unwrap(), vec![300, 7]);
    assert!(asked.lock().unwrap().starts_with(b"??"));
    let one =
        b"Q BEGIN\0 Q DECLARE _rivet .. P FETCH 2 FROM _rivet P FETCH 2 FROM _rivet Q COMMIT\0";
    assert_eq!(fetches_per_cursor_transaction(one), vec![2]);
    assert_eq!(
        fetches_per_cursor_transaction(&[&one[..], b" P FETCH 9 ", &one[..]].concat()),
        vec![2, 2],
        "a FETCH outside a cursor transaction is not counted"
    );
    assert!(fetches_per_cursor_transaction(b"Q BEGIN\0 Q SELECT 1 Q COMMIT\0").is_empty());
    assert_eq!(longest_and_total(&[300, 7]), (300, 307));
    assert!(is_one_page(1000, 20_000, 20) && is_one_page(1500, 20_000, 20));
    assert!(!is_one_page(2000, 20_000, 20), "two pages are not one");
    assert!(
        !is_one_page(20_000, 20_000, 20),
        "the whole table is not one page"
    );
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
