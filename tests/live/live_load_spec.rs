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

/// A table an earlier release created unclustered (no `cluster_by` then), met by a config
/// whose `cluster_by` now defaults to `auto` (= the key): nothing is written, so the load
/// follows the table — it overwrites it and keeps it unclustered — instead of refusing
/// every such table in the fleet after an upgrade. A written `cluster_by` still refuses
/// (the test above).
#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_full_load_follows_the_clustering_of_the_table_it_overwrites() {
    let Some(bq) = BqLive::from_env("bq_full_auto_cluster") else {
        return;
    };
    let (table, _guard) = keyed_pg_table("bq_full_auto_cluster");
    let _cleanup = bq.cleanup(&[&table]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", cluster_by: []"));
    rig.run_ok();
    load_ok(&rig);
    assert!(bq.read_bq_clustering(&table).is_empty());

    let rig = rig.clear_top_lines().top_line(&bq.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    assert!(
        bq.read_bq_clustering(&table).is_empty(),
        "an unwritten cluster_by keeps the table's own clustering"
    );
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

    // A whole-table run onto the view (after `state reset`) joins the log: appended,
    // the view still reads one current row per key. Pre-fix this refused, and a
    // stateless cycle re-selecting the first run was wedged the same way.
    let reset = rig.cli(&["state", "reset", "--export", &table]);
    assert!(reset.status.success());
    e.insert(&table, 14..=15, 5, Some(3));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("VIEW"));
    assert_eq!(
        bq.read_bq_count(&changes),
        "28",
        "13 + the 15-row whole pass"
    );
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ));
    assert_eq!(
        (rows[0]["n"].as_str(), rows[0]["d"].as_str()),
        (Some("15"), Some("15")),
        "the view is the current state"
    );
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

/// The refusal holds on every attempt: the table is created by hand in exactly the shape
/// rivet would give it (nothing but the ownership guard can refuse it), and the second
/// `rivet load` — the scheduler's retry — still refuses, leaves it untouched, and adopts
/// nothing. RED against the pre-fix ledger, whose `failed` row for the first refusal made
/// the table rivet's own for the second.
#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_load_refuses_a_table_it_did_not_load_on_every_attempt() {
    let Some(bq) = BqLive::from_env("bq_foreign_retry") else {
        return;
    };
    let (table, _guard) = dated_pg_table("bq_foreign_retry", 30);
    let _cleanup = bq.cleanup(&[&table]);
    bq.exec(&format!(
        "CREATE TABLE `{}.{}.{table}` (id INT64, d DATE, v STRING)",
        bq.project, bq.dataset
    ));
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    for attempt in 1..=2 {
        let said = load_fails(&rig);
        assert!(
            said.contains("no record of rivet loading it"),
            "attempt {attempt} refuses for the same reason:\n{said}"
        );
        assert_eq!(
            bq.read_bq_count(&table),
            "0",
            "attempt {attempt}: untouched"
        );
        assert_eq!(
            bq.read_bq_table_type(&format!("{table}__changes")),
            None,
            "attempt {attempt}: nothing adopted"
        );
    }
    let loads = StateDb::next_to_config(&rig.config_path())
        .load_statuses(&format!("{}.{}.{table}", bq.project, bq.dataset));
    assert_eq!(
        loads,
        ["refused", "refused"],
        "both stops are on the record"
    );
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

/// A Postgres table with a column of each partitionable type, ids `1..=n` over five days.
fn temporal_pg_table(prefix: &str, n: i64) -> (String, Box<dyn std::any::Any>) {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, guard) = e.create(
        prefix,
        "id INT NOT NULL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, dt TIMESTAMP NOT NULL, \
         d DATE NOT NULL, n INT NOT NULL, v VARCHAR(20) NULL",
    );
    add_temporal_rows(&table, 1, n);
    (table, guard)
}

fn add_temporal_rows(table: &str, from: i64, to: i64) {
    SqlEngine::Pg.exec(&format!(
        "INSERT INTO {table} (id, ts, dt, d, n, v) SELECT g, \
         TIMESTAMPTZ '2026-09-01 10:00:00+00' + (g % 5) * INTERVAL '1 day', \
         TIMESTAMP '2026-09-01 10:00:00' + (g % 5) * INTERVAL '1 day', \
         DATE '2026-09-01' + (g % 5)::int, g * 7, 'v' || g FROM generate_series({from}, {to}) g"
    ));
}

fn partition_line(bq: &BqLive, block: &str) -> String {
    bq.load_line(&format!(", partition: {block}"))
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_full_load_partitions_by_day_and_keeps_the_options_in_step() {
    let Some(bq) = BqLive::from_env("bq_part_day") else {
        return;
    };
    let (table, _guard) = temporal_pg_table("bq_part_day", 30);
    let _cleanup = bq.cleanup(&[&table]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&partition_line(
            &bq,
            "{ column: ts, granularity: day, expiration_days: 400, require_filter: true }",
        ));
    rig.run_ok();
    load_ok(&rig);
    let daily = Some(("DAY".to_string(), Some("ts".to_string())));
    assert_eq!(bq.read_bq_time_partitioning(&table), daily);
    assert_eq!(bq.read_bq_partition_expiration_days(&table), Some(400.0));
    assert!(bq.read_bq_requires_partition_filter(&table));
    assert_eq!(bq.read_bq_clustering(&table), ["id"]);
    let all = "ts >= TIMESTAMP '2000-01-01'";
    assert_eq!(bq.read_bq_count_where(&table, all), "30");

    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        bq.read_bq_count_where(&table, all),
        "30",
        "overwrites its own table"
    );

    let rig = rig.clear_top_lines().top_line(&partition_line(
        &bq,
        "{ column: ts, granularity: day, expiration_days: 30 }",
    ));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_time_partitioning(&table), daily);
    assert_eq!(
        bq.read_bq_partition_expiration_days(&table),
        Some(30.0),
        "options change in place"
    );
    assert!(!bq.read_bq_requires_partition_filter(&table));
    assert_eq!(bq.read_bq_count(&table), "30");

    let rig = rig
        .clear_top_lines()
        .top_line(&partition_line(&bq, "{ column: ts, granularity: month }"));
    rig.run_ok();
    let said = load_fails(&rig);
    assert!(
        said.contains("partitioned by `ts` by day, the load declares `ts` by month"),
        "the refusal names the difference:\n{said}"
    );
    assert_eq!(bq.read_bq_time_partitioning(&table), daily, "untouched");
    assert_eq!(bq.read_bq_count(&table), "30");
}

/// Run + load `block`'s partition on a fresh 20-row table and return the table's metadata.
fn partition_lands(bq: &BqLive, label: &str, block: &str) -> serde_json::Value {
    let (table, _guard) = temporal_pg_table(label, 20);
    let _cleanup = bq.cleanup(&[&table]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&partition_line(bq, block));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_count(&table), "20", "{label}");
    bq.read_bq_meta(&table)
}

fn column_partitions_at_every_granularity(label: &str, column: &str, granularities: &[&str]) {
    let Some(bq) = BqLive::from_env(label) else {
        return;
    };
    for g in granularities {
        let meta = partition_lands(
            &bq,
            &format!("{label}_{g}"),
            &format!("{{ column: {column}, granularity: {g} }}"),
        );
        assert_eq!(
            time_partitioning(&meta),
            Some((g.to_uppercase(), Some(column.to_string()))),
            "{column} by {g}"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_timestamp_partitions_land_at_every_granularity() {
    column_partitions_at_every_granularity("bq_part_ts", "ts", &["hour", "day", "month", "year"]);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_datetime_partitions_land_at_every_granularity() {
    column_partitions_at_every_granularity("bq_part_dt", "dt", &["hour", "day", "month", "year"]);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_date_partitions_land_by_day_month_and_year() {
    column_partitions_at_every_granularity("bq_part_d", "d", &["day", "month", "year"]);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_range_and_load_time_partitions_land_as_declared() {
    let Some(bq) = BqLive::from_env("bq_part_misc") else {
        return;
    };
    let meta = partition_lands(
        &bq,
        "bq_part_range",
        "{ range: { column: n, start: 0, end: 1000, interval: 100 } }",
    );
    assert_eq!(
        meta["rangePartitioning"]["field"].as_str(),
        Some("n"),
        "{meta}"
    );
    assert_eq!(
        meta["rangePartitioning"]["range"]["interval"].as_str(),
        Some("100")
    );
    for g in ["hour", "day", "month", "year"] {
        let meta = partition_lands(
            &bq,
            &format!("bq_part_ing_{g}"),
            &format!("{{ ingestion: {g} }}"),
        );
        assert_eq!(
            time_partitioning(&meta),
            Some((g.to_uppercase(), None)),
            "load time by {g}"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_changelog_inherits_the_partition_rivet_gave_the_table() {
    let Some(bq) = BqLive::from_env("bq_part_inherit") else {
        return;
    };
    let (table, _guard) = temporal_pg_table("bq_part_inherit", 30);
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    let rig = SqlEngine::Pg
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&partition_line(
            &bq,
            "{ column: ts, granularity: day, require_filter: true }",
        ));
    rig.run_ok();
    load_ok(&rig);
    let daily = Some(("DAY".to_string(), Some("ts".to_string())));
    assert_eq!(bq.read_bq_time_partitioning(&table), daily);
    assert!(bq.read_bq_requires_partition_filter(&table));

    add_temporal_rows(&table, 31, 35);
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("VIEW"));
    assert_eq!(bq.read_bq_time_partitioning(&changes), daily);
    assert!(
        !bq.read_bq_requires_partition_filter(&changes),
        "the view reads all of the log"
    );
    assert_eq!(bq.read_bq_clustering(&changes), ["id"]);
    assert_eq!(bq.read_bq_count(&changes), "35");
    assert_eq!(distinct_ids(&bq, &table), "35");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_hourly_partitions_over_the_job_cap_are_refused_before_the_load() {
    let Some(bq) = BqLive::from_env("bq_part_cap") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.create(
        "bq_part_cap",
        "id INT NOT NULL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL",
    );
    let _cleanup = bq.cleanup(&[&table]);
    e.exec(&format!(
        "INSERT INTO {table} (id, ts) SELECT g, TIMESTAMPTZ '2026-01-01 10:00:00+00' + \
         (g - 1) * INTERVAL '1 day' FROM generate_series(1, 200) g"
    ));
    let rig = e
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&partition_line(
            &bq,
            "{ column: ts, granularity: hour, expiration_days: 30 }",
        ));
    rig.run_ok();
    let said = load_fails(&rig);
    assert!(
        said.contains("about 4777 hour partitions of `ts`"),
        "the refusal counts the partitions:\n{said}"
    );
    assert!(
        said.contains("use `granularity: day` (about 200)"),
        "and names the granularity that fits:\n{said}"
    );
    assert_eq!(bq.read_bq_table_type(&table), None, "no job ran");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_changelog_follows_a_written_cluster_by_and_repartitions_only_on_rebuild() {
    let Some(bq) = BqLive::from_env("bq_part_drift") else {
        return;
    };
    let (table, _guard) = temporal_pg_table("bq_part_drift", 30);
    let changes = format!("{table}__changes");
    let (old, rebuild) = (format!("{changes}__old"), format!("{changes}__rebuild"));
    let _cleanup = bq.cleanup(&[&table, &changes, &old, &rebuild]);
    let daily = Some(("DAY".to_string(), Some("ts".to_string())));
    let rig = SqlEngine::Pg
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&partition_line(&bq, "{ column: ts, granularity: day }"));
    rig.run_ok();
    load_ok(&rig);
    add_temporal_rows(&table, 31, 35);
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_clustering(&changes), ["id"]);
    assert_eq!(bq.read_bq_time_partitioning(&changes), daily);

    add_temporal_rows(&table, 36, 40);
    let rig = rig
        .clear_top_lines()
        .top_line(&bq.load_line(", cluster_by: [v], partition: { column: ts, granularity: day }"));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        bq.read_bq_clustering(&changes),
        ["v"],
        "a written cluster_by re-clusters the log in place"
    );
    assert_eq!(bq.read_bq_count(&changes), "40");

    add_temporal_rows(&table, 41, 45);
    let rig = rig.clear_top_lines().top_line(
        &bq.load_line(", cluster_by: [v], partition: { column: ts, granularity: month }"),
    );
    rig.run_ok();
    let said = load_fails(&rig);
    assert!(
        said.contains("partitioned by `ts` by day, the load declares `ts` by month")
            && said.contains("--rebuild-changelog"),
        "the refusal names both keys and the way out:\n{said}"
    );
    assert_eq!(bq.read_bq_time_partitioning(&changes), daily, "untouched");
    assert_eq!(bq.read_bq_count(&changes), "40", "untouched");

    let out = rig.cli(&["load", "--rebuild-changelog"]);
    assert!(
        out.status.success(),
        "rivet load --rebuild-changelog failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        bq.read_bq_time_partitioning(&changes),
        Some(("MONTH".to_string(), Some("ts".to_string())))
    );
    assert_eq!(bq.read_bq_clustering(&changes), ["v"]);
    assert_eq!(
        bq.read_bq_count(&changes),
        "45",
        "the rebuilt log plus the delta"
    );
    assert_eq!(distinct_ids(&bq, &table), "45");
    assert_eq!(
        bq.read_bq_table_type(&old),
        None,
        "the swap left nothing behind"
    );
    assert_eq!(bq.read_bq_table_type(&rebuild), None);
}

/// The documented switch: an export runs `mode: full` for a while, then becomes
/// incremental. MT1 says `full` stores no cursor, so the first incremental run re-reads
/// the whole table and lands as the table again (an overwrite of rivet's own); only the
/// FIRST DELTA turns that table into `<table>__changes` — by renaming it, so its rows
/// become the baseline and nothing is copied or re-loaded — and the old name becomes the
/// current-state view.
#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_incremental_never_overwrites_an_existing_table_and_adopts_it_as_the_log() {
    let Some(bq) = BqLive::from_env("bq_full_to_inc") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("bq_full_to_inc");
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    e.insert(&table, 1..=10, 10, Some(1));

    // 1. `mode: full` → a plain table.
    let rig = e
        .rig(&table)
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("BASE TABLE"));
    let after_full = bq.read_bq_count(&table);
    assert_eq!(
        after_full, "10",
        "the full load's rows, counted in BigQuery"
    );
    assert_eq!(bq.read_bq_table_type(&changes), None, "no change log yet");

    // 2. Switch to incremental (MT1): no cursor was stored, so this run re-reads the whole
    //    table. The table already exists, so it is NOT overwritten — it is renamed into the
    //    change log and the pass is appended to it. Row 5 is deleted at the source first,
    //    so the pass cannot carry it: finding it afterwards proves the earlier rows were
    //    kept rather than replaced.
    e.insert(&table, 11..=13, 5, Some(2));
    SqlEngine::Pg.exec(&format!("DELETE FROM {table} WHERE id = 5"));
    let rig = rig.restage("incremental", &["cursor_column: id"]);
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(
        bq.read_bq_table_type(&table).as_deref(),
        Some("VIEW"),
        "the existing table became the change log and the name became the view"
    );
    let after_whole_pass = bq.read_bq_count(&changes);
    assert_eq!(
        after_whole_pass, "22",
        "10 kept as the baseline + the 12-row whole pass — nothing was overwritten"
    );
    assert_eq!(
        bq.read_bq_count_where(&changes, "id = 5"),
        "1",
        "the row deleted at the source survives in the log: the table was not replaced"
    );
    assert_eq!(
        StateDb::next_to_config(&rig.config_path()).cursor_column(&table),
        Some("id".to_string()),
        "and the cursor is recorded under its column"
    );

    // 3. An ordinary delta appends on top.
    e.insert(&table, 14..=16, 5, Some(3));
    rig.run_ok();
    load_ok(&rig);
    assert_eq!(bq.read_bq_table_type(&table).as_deref(), Some("VIEW"));

    // Counted in BigQuery, split by leg: what the full load left as the baseline (ids
    // 1..=10), what the whole pass re-read (ids 1..=13 less the deleted 5) and the delta.
    let log_total = bq.read_bq_count(&changes);
    let adopted = bq.read_bq_count_where(&changes, "id <= 13");
    let via_incremental = bq.read_bq_count_where(&changes, "id >= 14");
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d FROM `{}.{}.{changes}`",
        bq.project, bq.dataset
    ));
    let view = bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ));
    eprintln!(
        "BQ COUNTS full_baseline={after_full} log_after_switch={after_whole_pass} \
         log_total={log_total} baseline_plus_pass={adopted} via_delta={via_incremental} \
         log_distinct={:?} view={:?}",
        rows[0]["d"].as_str(),
        view[0]["n"].as_str()
    );
    assert_eq!(
        (adopted.as_str(), via_incremental.as_str()),
        ("22", "3"),
        "10 baseline + 12 whole pass, then only the delta on top"
    );
    assert_eq!(
        log_total, "25",
        "the log keeps every leg: nothing was overwritten at any point"
    );
    assert_eq!(
        (rows[0]["n"].as_str(), rows[0]["d"].as_str()),
        (Some("25"), Some("16")),
        "the overlap is at-least-once; the view is what dedups it"
    );
    assert_eq!(
        (view[0]["n"].as_str(), view[0]["d"].as_str()),
        (Some("16"), Some("16")),
        "the view is the current state, one row per key"
    );
}

/// `mode: incremental` from the very first run, onto a table that is ALREADY in the
/// dataset and that rivet's ledger knows nothing about. The whole-table first pass would
/// overwrite it and the first delta would rename it into the change log, so both are
/// refused — on every attempt, not just the first (the refusal is journaled `refused`,
/// which never makes the table rivet's own).
#[test]
#[ignore = "live: requires docker compose up -d postgres + BigQuery creds"]
fn bigquery_incremental_onto_a_table_already_in_the_dataset_is_refused_every_time() {
    let Some(bq) = BqLive::from_env("bq_inc_existing") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("bq_inc_existing");
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    e.insert(&table, 1..=10, 10, Some(1));
    // Someone else's table, with the export's exact columns: nothing but the ownership
    // guard can refuse it.
    bq.exec(&format!(
        "CREATE TABLE `{}.{}.{table}` (id INT64, ext_id INT64, server_time DATETIME, \
         updated_at DATETIME, time_spent INT64)",
        bq.project, bq.dataset
    ));

    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""));
    rig.run_ok();
    for attempt in 1..=2 {
        let said = load_fails(&rig);
        assert!(
            said.contains("no record of rivet loading it"),
            "attempt {attempt} refuses for the same reason:\n{said}"
        );
        assert_eq!(
            bq.read_bq_count(&table),
            "0",
            "attempt {attempt}: untouched"
        );
        assert_eq!(
            bq.read_bq_table_type(&table).as_deref(),
            Some("BASE TABLE"),
            "attempt {attempt}: still their table, not a view"
        );
        assert_eq!(
            bq.read_bq_table_type(&changes),
            None,
            "attempt {attempt}: nothing adopted"
        );
    }
    let loads = StateDb::next_to_config(&rig.config_path())
        .load_statuses(&format!("{}.{}.{table}", bq.project, bq.dataset));
    assert_eq!(
        loads,
        ["refused", "refused"],
        "a stop before the write is journaled `refused`, never `failed`"
    );
}
