//! Suspected silent-loss paths from the 2026-09-26 contract audit. Each test states
//! the contract as "the run either delivers what the source holds, or fails loudly
//! naming why" — a green exit over a divergent destination is the failure.

use crate::common::*;
use mysql::prelude::Queryable as _;

const MYSQL_CDC_ROOT_URL: &str = "mysql://root:rivet@127.0.0.1:3307/rivet";
const MONGO_RS_PORT: u16 = 27018;

/// Combined stdout+stderr of a finished run.
fn said(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A MySQL connection on the CDC instance as `url`'s user.
fn mysql_cdc_conn(url: &str) -> mysql::PooledConn {
    mysql::Pool::new(url)
        .and_then(|p| p.get_conn())
        .expect("connect mysql-cdc")
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn mysql_cdc_a_statement_logged_insert_is_captured_or_refused_never_dropped() {
    let tbl = unique_name("aud_stmt");
    let mut c = mysql_cdc_conn(MYSQL_CDC_URL);
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl);
    rig.run_ok(); // anchor

    // One session switched to STATEMENT logging — a per-session setting the
    // server's global ROW format does not prevent.
    let mut root = mysql_cdc_conn(MYSQL_CDC_ROOT_URL);
    root.query_drop("SET SESSION binlog_format = 'STATEMENT'")
        .unwrap();
    root.query_drop(format!("INSERT INTO rivet.{tbl} VALUES (1, 10)"))
        .unwrap();
    let file: String = root
        .query_first::<mysql::Row, _>("SHOW BINARY LOG STATUS")
        .ok()
        .flatten()
        .or_else(|| {
            root.query_first::<mysql::Row, _>("SHOW MASTER STATUS")
                .ok()
                .flatten()
        })
        .and_then(|r| r.get(0))
        .expect("binlog file");
    let events: Vec<(String, String)> = root
        .query_map(
            format!("SHOW BINLOG EVENTS IN '{file}'"),
            |r: mysql::Row| {
                (
                    r.get::<String, _>(2).unwrap_or_default(),
                    r.get::<String, _>(5).unwrap_or_default(),
                )
            },
        )
        .unwrap();
    assert!(
        events
            .iter()
            .any(|(kind, info)| kind == "Query"
                && info.contains(&format!("INSERT INTO rivet.{tbl}"))),
        "fixture: the insert must be in the binlog as a statement, not as row events"
    );

    let out = rig.run();
    if out.status.success() {
        let delivered = if files_with_extension(&rig.out_dir(), "parquet").is_empty() {
            Default::default()
        } else {
            duckdb_declared_dir_id_set(&rig.out_dir())
        };
        assert_eq!(
            delivered,
            [1].into_iter().collect(),
            "the run exited 0, so the statement-logged insert must be in the output:\n{}",
            said(&out)
        );
    } else {
        assert!(
            said(&out).to_lowercase().contains("statement"),
            "a refusal must name the statement-logged event:\n{}",
            said(&out)
        );
    }
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn mysql_cdc_a_dropped_captured_table_is_refused_like_a_truncate() {
    let tbl = unique_name("aud_drop");
    let mut c = mysql_cdc_conn(MYSQL_CDC_URL);
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl);
    rig.run_ok(); // anchor
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        [1].into_iter().collect(),
        "fixture: the row is delivered before the drop"
    );

    c.query_drop(format!("DROP TABLE {tbl}")).unwrap();
    let out = rig.run();
    assert!(
        !out.status.success(),
        "the source table is gone while the destination still holds row 1 — a green \
         run here is the divergence a TRUNCATE is refused for:\n{}",
        said(&out)
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_a_dropped_captured_collection_is_refused_not_skipped() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("aud_mdrop");
    let m = MongoTest::connect(MONGO_RS_PORT, &db);
    m.drop_collection("t");

    let rig = Rig::mongo_cdc("t")
        .source_url(&MongoTest::url(MONGO_RS_PORT, &db))
        .duckdb_oracle();
    rig.run_ok(); // anchor
    m.upsert_set("t", 1, "v", "a");
    rig.run_ok();
    assert!(
        duckdb_declared_distinct_set(rig.oracle_dir(), "_id").contains("1"),
        "fixture: the document is delivered before the drop"
    );

    m.drop_collection("t");
    let out = rig.run();
    assert!(
        !out.status.success(),
        "the collection is gone while the destination still holds document 1 — a green \
         run here silently diverges:\n{}",
        said(&out)
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres (en_US collation)"]
fn parallel_keyset_incremental_on_a_text_key_takes_keys_the_collation_ranks_higher() {
    require_alive(LiveService::Postgres);
    let table = unique_name("aud_txtkey");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id TEXT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT 'a' || lpad(g::text, 4, '0'), g FROM generate_series(1, 1000) g;
         INSERT INTO {table} SELECT 'b' || lpad(g::text, 4, '0'), g FROM generate_series(1, 1000) g;"
    ))
    .unwrap();
    let _tbl = PgTable::adopt(table.clone());
    // The fixture only bites where the database orders 'C…' after 'b…' and bytes do not.
    let ranks_higher: bool = c.query_one("SELECT 'C0001' > 'b1000'", &[]).unwrap().get(0);
    assert!(
        ranks_higher,
        "fixture: this database's collation must rank 'C…' above 'b…'"
    );

    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200");
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), "count(DISTINCT id)"),
        2000,
        "fixture: run 1 exports every key"
    );

    c.batch_execute(&format!(
        "INSERT INTO {table} SELECT 'C' || lpad(g::text, 4, '0'), g FROM generate_series(1, 500) g"
    ))
    .unwrap();
    rig.run_ok();
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), "count(*) FILTER (WHERE id LIKE 'C%')"),
        500,
        "the 500 keys the source ranks above the anchor must be exported, not judged \
         'nothing new' by a byte compare"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_incremental_on_a_legacy_datetime_cursor_takes_the_next_row_once() {
    require_alive(LiveService::Mssql);
    let table = unique_name("aud_dtcur");
    mssql_exec(&format!(
        "IF OBJECT_ID('{table}') IS NOT NULL DROP TABLE {table}"
    ));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, updated_at DATETIME NOT NULL);
         INSERT INTO {table} VALUES (1, '2024-01-01T10:00:00.123'), (2, '2024-01-01T10:00:00.457');"
    ));

    let rig = Rig::mssql_batch(&table)
        .query(&format!("SELECT id, updated_at FROM {table}"))
        .mode("incremental")
        .export_line("cursor_column: updated_at");
    rig.run_ok();
    mssql_exec(&format!(
        "INSERT INTO {table} VALUES (3, '2024-01-01T10:00:01.500')"
    ));
    let out = rig.run();
    let text = said(&out);
    mssql_exec(&format!("DROP TABLE {table}"));
    assert!(
        out.status.success(),
        "an incremental run over a DATETIME cursor must not fail on its own saved boundary:\n{text}"
    );
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), "count(*)"),
        3,
        "rows 1 and 2 once, then row 3 once — no re-export and no skip"
    );
    assert_eq!(
        duckdb_declared_dir_id_set(&rig.out_dir()),
        [1, 2, 3].into_iter().collect()
    );
}

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog ROW)"]
fn mysql_cdc_a_dropped_and_recreated_captured_table_is_refused_not_merged() {
    let tbl = unique_name("aud_recreate");
    let mut c = mysql_cdc_conn(MYSQL_CDC_URL);
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl);
    rig.run_ok(); // anchor
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    rig.run_ok();

    // The table is replaced under the same name: row 1 no longer exists anywhere.
    c.query_drop(format!("DROP TABLE {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 20)"))
        .unwrap();
    let out = rig.run();
    assert!(
        !out.status.success(),
        "the source now holds only row 2 while the destination keeps row 1 as live — a \
         green run merges two tables' histories:\n{}",
        said(&out)
    );
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_cdc_export_altered_between_runs_loads_into_bigquery_or_refuses_by_column() {
    let Some(bq) = BqLive::from_env("aud_mixed") else {
        panic!("BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset: this cell cannot run");
    };
    let tbl = unique_name("aud_mixed");
    let changes = format!("{tbl}__changes");
    let _cleanup = bq.cleanup(&[&tbl, &changes]);
    let mut c = mysql_cdc_conn(MYSQL_CDC_URL);
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id]"));
    rig.run_ok(); // anchor
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    rig.run_ok();
    let first = rig.cli(&["load"]);
    assert!(
        first.status.success(),
        "fixture: the first load:\n{}",
        said(&first)
    );

    // The source changes shape; the next part carries a new column and a wider type.
    c.query_drop(format!(
        "ALTER TABLE {tbl} ADD COLUMN w VARCHAR(20), MODIFY v BIGINT"
    ))
    .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 30000000000, 'new')"))
        .unwrap();
    rig.run_ok();
    let second = rig.cli(&["load"]);
    let text = said(&second);
    if !second.status.success() {
        assert!(
            text.contains("`v`") || text.contains(" v ") || text.contains("'v'"),
            "a refused load must name the column whose type moved:\n{text}"
        );
        return;
    }
    let rows = bq.read_bq_rows(&format!(
        "SELECT CAST(id AS STRING) AS id, CAST(v AS STRING) AS v, w FROM `{}.{}.{tbl}` ORDER BY id",
        bq.project, bq.dataset
    ));
    let got: Vec<(String, String, Option<String>)> = rows
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap_or_default().to_string(),
                r["v"].as_str().unwrap_or_default().to_string(),
                r["w"].as_str().map(str::to_string),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".to_string(), "10".to_string(), None),
            (
                "2".to_string(),
                "30000000000".to_string(),
                Some("new".to_string())
            ),
        ],
        "the load exited 0, so both rows must be in BigQuery with their values:\n{text}"
    );
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn compact_after_a_source_alter_carries_the_new_column_into_the_base() {
    let Some(bq) = BqLive::from_env("aud_compact") else {
        panic!("BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset: this cell cannot run");
    };
    let tbl = unique_name("aud_cmpct");
    let changes = format!("{tbl}__changes");
    let _cleanup = bq.cleanup(&[&tbl, &changes]);
    let mut c = mysql_cdc_conn(MYSQL_CDC_URL);
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    // Base + buffer layout: a batch recipe backfills the base, CDC fills the buffer.
    let rig = Rig::mysql_cdc(&tbl)
        .cdc("backfill: auto")
        .also_batch_export("baseline", &tbl, "full")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id]"));
    rig.run_ok();
    for step in [&["load"][..], &["compact"][..]] {
        let o = rig.cli(step);
        assert!(o.status.success(), "fixture: {step:?}:\n{}", said(&o));
    }

    c.query_drop(format!("ALTER TABLE {tbl} ADD COLUMN w VARCHAR(20)"))
        .unwrap();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (2, 20, 'new')"))
        .unwrap();
    rig.run_ok();
    let load = rig.cli(&["load"]);
    assert!(load.status.success(), "the second load:\n{}", said(&load));
    let compact = rig.cli(&["compact"]);
    let text = said(&compact);
    assert!(
        compact.status.success(),
        "a column added at the source must reach the base; a raw warehouse query error is \
         not a refusal, and every later compact would fail the same way:\n{text}"
    );
    let rows = bq.read_bq_rows(&format!(
        "SELECT CAST(id AS STRING) AS id, w FROM `{}.{}.{tbl}` WHERE NOT __is_deleted ORDER BY id",
        bq.project, bq.dataset
    ));
    let got: Vec<(String, Option<String>)> = rows
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap_or_default().to_string(),
                r["w"].as_str().map(str::to_string),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".to_string(), None),
            ("2".to_string(), Some("new".to_string()))
        ],
        "compact exited 0, so the base must hold row 2 with its new column:\n{text}"
    );
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn cleanup_source_never_deletes_parts_an_extract_committed_during_the_load() {
    let Some(bq) = BqLive::from_env("aud_cleanup") else {
        panic!("BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset: this cell cannot run");
    };
    let tbl = unique_name("aud_clean");
    let changes = format!("{tbl}__changes");
    let _cleanup = bq.cleanup(&[&tbl, &changes]);
    let mut c = mysql_cdc_conn(MYSQL_CDC_URL);
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, v INT)"))
        .unwrap();
    let _guard = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id], cleanup_source: true"));
    let cfg = rig.config_path();
    rig.run_ok(); // anchor
    for id in 1..=50 {
        c.query_drop(format!("INSERT INTO {tbl} VALUES ({id}, {id})"))
            .unwrap();
    }
    rig.run_ok();

    // The load decides whether a run is writing just before it appends, then spends
    // seconds in BigQuery jobs and deletes the prefix at the end. An extract that
    // starts after that decision and commits before the delete is the race. The
    // decision's moment is not observable from outside, so the extract's start is
    // swept until one attempt lands after it (the load then prints no "a run is
    // writing" note) and before the load ends.
    let mut next_id = 51;
    let mut raced = false;
    for delay_s in [4u64, 7, 10, 13] {
        let started = std::time::Instant::now();
        let load = std::process::Command::new(rivet_bin())
            .args(["load", "-c", cfg.to_str().unwrap()])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn rivet load");
        std::thread::sleep(std::time::Duration::from_secs(delay_s));
        c.query_drop(format!("INSERT INTO {tbl} VALUES ({next_id}, {next_id})"))
            .unwrap();
        next_id += 1;
        rig.run_ok();
        let run_committed_at = started.elapsed();
        let load_out = load.wait_with_output().expect("load finishes");
        let load_took = started.elapsed();
        let text = said(&load_out);
        assert!(
            load_out.status.success(),
            "fixture: the racing load:\n{text}"
        );
        let saw_the_run = text.contains("a run is writing");
        eprintln!(
            "delay {delay_s}s: extract committed at {run_committed_at:?}, load ended at \
             {load_took:?}, load saw the run: {saw_the_run}"
        );
        if !saw_the_run && run_committed_at < load_took {
            raced = true;
            break;
        }
    }
    assert!(
        raced,
        "fixture is inert: no attempt committed an extract inside the load's window, so this \
         run proves nothing about the cleanup"
    );

    // Everything the stream has acknowledged must still reach the warehouse.
    rig.run_ok();
    let last = rig.cli(&["load"]);
    assert!(last.status.success(), "the final load:\n{}", said(&last));
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(DISTINCT id) AS n FROM `{}.{}.{tbl}`",
        bq.project, bq.dataset
    ));
    let n: i64 = rows[0]["n"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        n,
        next_id - 1,
        "every row an extract committed during a load must reach the warehouse; a cleanup \
         that deleted its part after the stream acknowledged it loses the row for good"
    );
}
