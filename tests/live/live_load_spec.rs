//! The load spec a successful `rivet run` records for `rivet load` (ADR-0034 D1).

use crate::common::*;

const LOAD: &str = "load: { target: bigquery, project: p, dataset: d }";
const MONGO_PORT: u16 = 27017;

/// Export a table keyed on `(b_key, a_key)` and check the recorded columns and key order.
fn records_the_key_in_key_order(e: SqlEngine) {
    e.alive();
    let (table, _guard) = e.create(
        "load_spec",
        "a_key INT NOT NULL, b_key INT NOT NULL, v VARCHAR(20) NULL, PRIMARY KEY (b_key, a_key)",
    );
    e.exec(&format!(
        "INSERT INTO {table} (a_key, b_key, v) VALUES (1, 2, 'x')"
    ));
    let rig = e.rig(&table).top_line(LOAD);
    rig.run_ok();

    let (columns, key) = StateDb::next_to_config(&rig.config_path())
        .load_spec(&table, None)
        .expect("a successful run records the load spec");
    assert_eq!(columns, ["a_key", "b_key", "v"]);
    assert_eq!(key, Some(vec!["b_key".to_string(), "a_key".to_string()]));
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn run_records_the_load_spec_key_in_key_order_mysql() {
    records_the_key_in_key_order(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn run_records_the_load_spec_key_in_key_order_postgres() {
    records_the_key_in_key_order(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn run_records_the_load_spec_key_in_key_order_mssql() {
    records_the_key_in_key_order(SqlEngine::Mssql);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn run_records_the_load_spec_of_a_mongo_collection() {
    require_alive(LiveService::Mongo);
    let db = unique_name("load_spec_mg");
    MongoTest::connect(MONGO_PORT, &db).seed_int_id("c", 3);
    let rig = Rig::mongo_batch("c")
        .source_url(&MongoTest::url(MONGO_PORT, &db))
        .top_line(LOAD);
    rig.run_ok();

    let (columns, key) = StateDb::next_to_config(&rig.config_path())
        .load_spec("c", None)
        .expect("a successful run records the load spec");
    assert_eq!(columns, ["_id", "document"]);
    assert_eq!(key, Some(vec!["_id".to_string()]));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_query_export_records_its_columns_and_no_key() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("load_spec_query");
    e.insert(&table, 1..=3, 10, None);
    let rig = e
        .rig(&table)
        .query(&format!("SELECT id, time_spent FROM {table}"))
        .top_line(LOAD);
    rig.run_ok();

    let (columns, key) = StateDb::next_to_config(&rig.config_path())
        .load_spec(&table, None)
        .expect("a successful run records the load spec");
    assert_eq!(columns, ["id", "time_spent"]);
    assert_eq!(key, None);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_config_without_a_load_block_records_no_load_spec() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("load_spec_none");
    e.insert(&table, 1..=3, 10, None);
    let rig = e.rig(&table);
    rig.run_ok();

    assert_eq!(read_ids(&rig.out_dir()), vec![1, 2, 3]);
    assert!(
        StateDb::next_to_config(&rig.config_path())
            .load_spec(&table, None)
            .is_none()
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_failed_run_records_no_load_spec() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("load_spec_failed");
    e.insert(&table, 1..=3, 10, None);
    let rig = e
        .rig(&table)
        .top_line(LOAD)
        .unwritable_dest_path(std::path::PathBuf::from("/dev/null/rivet-out"));
    rig.run_expect_fail();

    assert!(
        StateDb::next_to_config(&rig.config_path())
            .load_spec(&table, None)
            .is_none()
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

/// `rivet load` must fail; returns what it said.
fn load_fails(rig: &Rig) -> String {
    let out = rig.cli(&["load"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "rivet load must fail:\n{said}");
    said
}

/// A Postgres table keyed on `(b_key, a_key)` with 40 rows.
fn keyed_pg_table(prefix: &str) -> (String, Box<dyn std::any::Any>) {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, guard) = e.create(
        prefix,
        "a_key INT NOT NULL, b_key INT NOT NULL, v VARCHAR(20) NULL, PRIMARY KEY (b_key, a_key)",
    );
    e.exec(&format!(
        "INSERT INTO {table} (a_key, b_key, v) SELECT g, g % 7, 'v' || g FROM generate_series(1, 40) g"
    ));
    (table, guard)
}

fn distinct_a_keys(bq: &BqLive, table: &str) -> String {
    bq.read_bq_rows(&format!(
        "SELECT COUNT(DISTINCT a_key) AS n FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ))[0]["n"]
        .as_str()
        .expect("count")
        .to_string()
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_full_load_clusters_on_the_recorded_key_and_overwrites_its_own_table() {
    let Some(bq) = BqLive::from_env("bq_full_auto") else {
        return;
    };
    let (table, _guard) = keyed_pg_table("bq_full_auto");
    let _cleanup = bq.cleanup(&[&table]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_clustering(&table), ["b_key", "a_key"]);

    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_clustering(&table), ["b_key", "a_key"]);
    assert_eq!(distinct_a_keys(&bq, &table), "40");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_full_load_refuses_its_table_when_the_clustering_changed() {
    let Some(bq) = BqLive::from_env("bq_full_recluster") else {
        return;
    };
    let (table, _guard) = keyed_pg_table("bq_full_recluster");
    let _cleanup = bq.cleanup(&[&table]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", cluster_by: []"));
    rig.run_ok();
    load_ok(&rig);
    assert!(bq.read_bq_clustering(&table).is_empty());

    let rig = rig
        .clear_top_lines()
        .top_line(&bq.load_line(", cluster_by: [v]"));
    rig.run_ok();
    let said = load_fails(&rig);
    assert!(
        said.contains("clustered on nothing") && said.contains("`v`"),
        "the refusal names the difference:\n{said}"
    );
    assert!(bq.read_bq_clustering(&table).is_empty(), "untouched");
    assert_eq!(distinct_a_keys(&bq, &table), "40");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_incremental_first_run_lands_as_a_table_and_the_first_delta_starts_the_changelog() {
    let Some(bq) = BqLive::from_env("bq_inc_first") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("bq_inc_first");
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    e.insert(&table, 1..=10, 10, Some(1));
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("BASE TABLE"));
    assert_eq!(bq.read_bq_table_type(&changes), None, "no change log yet");
    assert_eq!(bq.read_bq_count(&table), "10");

    e.insert(&table, 11..=13, 5, Some(2));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("VIEW"));
    assert_eq!(bq.read_bq_count(&changes), "13", "no row is loaded twice");
    assert_eq!(bq.read_bq_clustering(&changes), ["id"]);
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ));
    assert_eq!(
        (rows[0]["n"].as_str(), rows[0]["d"].as_str()),
        (Some("13"), Some("13"))
    );

    let reset = rig.cli(&["state", "reset", "--export", &table]);
    assert!(reset.status.success());
    rig.run_ok();
    let said = load_fails(&rig);
    assert!(
        said.contains("a full pass cannot be appended"),
        "a whole-table run onto the change log is refused:\n{said}"
    );
    assert_eq!(bq.read_bq_count(&changes), "13", "untouched");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn init_records_the_key_a_query_export_cannot_capture() {
    let (table, _guard) = keyed_pg_table("init_query_key");
    let rig = SqlEngine::Pg
        .rig(&table)
        .query(&format!("SELECT a_key, b_key, v FROM {table}"))
        .top_line(LOAD);
    let cfg = rig.config_path();
    let scaffold = cfg.with_file_name("init.yaml");
    let out = run_rivet(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--table",
        &table,
        "--mode",
        "incremental",
        "--output",
        scaffold.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "rivet init failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        std::fs::read_to_string(&scaffold)
            .unwrap()
            .contains("query:"),
        "the scaffold must be a query: export"
    );
    rig.run_ok();

    let (columns, key) = StateDb::next_to_config(&cfg)
        .load_spec(&table, None)
        .expect("the run records the columns");
    assert_eq!(columns, ["a_key", "b_key", "v"]);
    assert_eq!(key, Some(vec!["b_key".to_string(), "a_key".to_string()]));
}

/// A Postgres `(id PK, d DATE, v)` table with ids `1..=n` over five dates.
fn dated_pg_table(prefix: &str, n: i64) -> (String, Box<dyn std::any::Any>) {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, guard) = e.create(
        prefix,
        "id INT NOT NULL PRIMARY KEY, d DATE NOT NULL, v VARCHAR(20) NULL",
    );
    add_dated_rows(&table, 1, n);
    (table, guard)
}

fn add_dated_rows(table: &str, from: i64, to: i64) {
    SqlEngine::Pg.exec(&format!(
        "INSERT INTO {table} (id, d, v) SELECT g, DATE '2026-09-01' + (g % 5)::int, 'v' || g \
         FROM generate_series({from}, {to}) g"
    ));
}

/// Create `table` in BigQuery by hand, partitioned on `d` and clustered on `v`.
fn hand_partitioned(bq: &BqLive, table: &str, options: &str) {
    bq.exec(&format!(
        "CREATE TABLE `{}.{}.{table}` (id INT64, d DATE, v STRING) \
         PARTITION BY d CLUSTER BY v{options}",
        bq.project, bq.dataset
    ));
}

fn distinct_ids(bq: &BqLive, table: &str) -> String {
    bq.read_bq_rows(&format!(
        "SELECT COUNT(DISTINCT id) AS n FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ))[0]["n"]
        .as_str()
        .expect("count")
        .to_string()
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_full_load_refuses_a_table_it_did_not_load() {
    let Some(bq) = BqLive::from_env("bq_foreign") else {
        return;
    };
    let (table, _guard) = dated_pg_table("bq_foreign", 30);
    let _cleanup = bq.cleanup(&[&table]);
    hand_partitioned(&bq, &table, "");
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    let said = load_fails(&rig);
    assert!(
        said.contains("no record of rivet loading it"),
        "the refusal says why:\n{said}"
    );

    assert_eq!(bq.read_bq_partitioning(&table).as_deref(), Some("d"));
    assert_eq!(bq.read_bq_clustering(&table), ["v"]);
    assert_eq!(bq.read_bq_count(&table), "0", "untouched");
}

/// Re-create `table` under its own name, partitioned on `d`, clustered on `v`, with `options`.
fn reshape_by_hand(bq: &BqLive, table: &str, options: &str) {
    let fq = |t: &str| format!("`{}.{}.{t}`", bq.project, bq.dataset);
    bq.exec(&format!(
        "CREATE TABLE {tmp} PARTITION BY d CLUSTER BY v{options} AS SELECT * FROM {t}; \
         DROP TABLE {t}; ALTER TABLE {tmp} RENAME TO {table};",
        tmp = fq(&format!("{table}__reshaped")),
        t = fq(table)
    ));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_changelog_keeps_the_shape_of_the_table_it_grew_from() {
    let Some(bq) = BqLive::from_env("bq_inherit_spec") else {
        return;
    };
    let (table, _guard) = dated_pg_table("bq_inherit_spec", 30);
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes, &format!("{table}__reshaped")]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .restage("chunked", &["chunk_by_key: id", "chunk_size: 4"])
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    reshape_by_hand(&bq, &table, " OPTIONS(require_partition_filter = true)");

    add_dated_rows(&table, 31, 35);
    let rig = rig.restage("incremental", &["cursor_column: id"]);
    rig.run_ok();
    load_ok(&rig);

    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("VIEW"));
    assert_eq!(
        bq.read_bq_count(&changes),
        "35",
        "the rows the table held, plus the delta"
    );
    assert_eq!(bq.read_bq_partitioning(&changes).as_deref(), Some("d"));
    assert_eq!(bq.read_bq_clustering(&changes), ["v"]);
    assert_eq!(
        bq.read_bq_option(&changes, "require_partition_filter")
            .as_deref(),
        Some("false"),
        "the view reads all of `__changes`, so it cannot require a partition filter"
    );
    assert_eq!(distinct_ids(&bq, &table), "35");
}
