//! A history wider than one BigQuery job may write (5,000 daily partitions), read by
//! KEYSET over an autoincrement key — the partner shape: `id` grows with `created_at`,
//! so each Parquet file is date-local although nothing was chunked by date. The load
//! must land DAY partitions in several jobs, not refuse and not degrade to `month`.
//!
//! Three cells, one fixture (5,000 rows, one row per day since 2000-01-01, five files
//! of 1,000 rows):
//!   * batch `mode: full`-shaped keyset export → `<table>` via staging + CLONE, two
//!     jobs (files 1–4 span exactly 4,000 days), DAY partitions, no staging left;
//!   * the same table with the dates PERMUTED (every file spans all 5,000 days) →
//!     refused BEFORE any job, naming the file; the ledger says `refused`;
//!   * the CDC shape (`backfill: auto` with the keyset recipe) → the baseline lands in
//!     the BASE `<table>` via staging + CLONE in two jobs, DAY partitions, every row
//!     `__is_deleted = false`; no buffer yet.
//!
//! Oracles: `bq` (count, distinct partitions, SUM(id), the table's partitioning), the
//! SQLite ledger beside the config, the loader's own stderr only for the job count it
//! CLAIMS (cross-checked against what BigQuery holds). Needs the MySQL stands and the
//! warehouse env; SKIPS without them.

use crate::common::*;
use mysql::prelude::Queryable as _;

const ROWS: i64 = 5000;
/// Multiplier coprime with 5000: `(id * 7919) % 5000` is a permutation of 0..5000.
const SHUFFLE: i64 = 7919;

/// One row per day from 2000-01-01; `permuted` breaks the id ↔ date correlation so
/// every keyset file spans the whole history.
fn seed(conn: &mut mysql::PooledConn, table: &str, permuted: bool) {
    let day = if permuted {
        format!("(n * {SHUFFLE}) % {ROWS}")
    } else {
        "n - 1".to_string()
    };
    conn.query_drop("SET SESSION cte_max_recursion_depth = 10000")
        .expect("cte depth");
    conn.query_drop(format!(
        "INSERT INTO {table} (id, v, created_at) \
         WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < {ROWS}) \
         SELECT n, n, DATE_ADD('2000-01-01', INTERVAL {day} DAY) FROM s"
    ))
    .expect("seed 5000 dated rows");
}

fn keyset_recipe_lines(rig: Rig) -> Rig {
    rig.also_export_line("chunk_by_key: id")
        .also_export_line("chunk_size: 1000")
        .also_export_line("chunk_checkpoint: true")
}

/// `(rows, distinct DAY partitions, SUM(id))` of a warehouse table, read through `bq`.
fn warehouse_profile(bq: &BqLive, table: &str, where_sql: &str) -> (i64, i64, i64) {
    let row = &bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT DATE(created_at)) AS p, IFNULL(SUM(id), 0) AS s \
         FROM `{}.{}.{table}` {where_sql}",
        bq.project, bq.dataset
    ))[0];
    let get = |k: &str| -> i64 { row[k].as_str().expect(k).parse().expect("a number") };
    (get("n"), get("p"), get("s"))
}

fn expected_sum() -> i64 {
    ROWS * (ROWS + 1) / 2
}

#[test]
#[ignore = "live: requires mysql + BigQuery creds"]
fn a_wide_history_read_by_keyset_loads_daily_partitions_in_batches() {
    let Some(bq) = BqLive::from_env("pbatch") else {
        return;
    };
    let mut conn = mysql_connect();
    let table = unique_name("pbatch");
    conn.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v INT, created_at DATETIME NOT NULL)"
    ))
    .expect("create");
    let _guard = MysqlTable::adopt(table.clone());
    seed(&mut conn, &table, false);
    let _cleanup = bq.cleanup(&[&table, &format!("{table}__staging")]);

    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 1000")
        .export_line("chunk_checkpoint: true")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", partition: { column: created_at, granularity: day }"));
    rig.run_ok();

    let out = rig.cli(&["load"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "the load must succeed in batches:\n{said}"
    );
    assert!(
        said.contains("5 files in 2 load jobs"),
        "files 1-4 span exactly 4,000 days and fit one job; file 5 needs the second:\n{said}"
    );

    let (n, partitions, sum) = warehouse_profile(&bq, &table, "");
    assert_eq!(n, ROWS, "every row landed once");
    assert_eq!(
        partitions, ROWS,
        "one DAY partition per row — never coarsened to month"
    );
    assert_eq!(sum, expected_sum(), "the rows are these rows");
    assert_eq!(
        bq.read_bq_time_partitioning(&table),
        Some(("DAY".to_string(), Some("created_at".to_string()))),
        "the table keeps the declared DAY partitioning through the CLONE"
    );
    assert!(
        bq.read_bq_table_type(&format!("{table}__staging"))
            .is_none(),
        "the staging table is dropped after the CLONE"
    );
}

#[test]
#[ignore = "live: requires mysql + BigQuery creds"]
fn a_file_spanning_the_whole_history_is_refused_by_name_before_any_job() {
    let Some(bq) = BqLive::from_env("pwide") else {
        return;
    };
    let mut conn = mysql_connect();
    let table = unique_name("pwide");
    conn.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v INT, created_at DATETIME NOT NULL)"
    ))
    .expect("create");
    let _guard = MysqlTable::adopt(table.clone());
    seed(&mut conn, &table, true);
    let _cleanup = bq.cleanup(&[&table, &format!("{table}__staging")]);

    let rig = Rig::mysql_batch(&table)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 1000")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", partition: { column: created_at, granularity: day }"));
    rig.run_ok();

    let out = rig.cli(&["load"]);
    let err = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !out.status.success(),
        "a file no batching splits must be refused:\n{err}"
    );
    assert!(
        // 5,000 residues over 1,000-row files: each file spans ~4,999 days.
        err.contains("alone spans about 499")
            && err.contains("day partitions of `created_at`")
            && err.contains("no batching splits one file")
            && err.contains("use `granularity: month` (about 16"),
        "{err}"
    );
    assert!(
        bq.read_bq_table_type(&table).is_none()
            && bq
                .read_bq_table_type(&format!("{table}__staging"))
                .is_none(),
        "refused BEFORE any job: nothing in the warehouse"
    );
    let ledger = StateDb::next_to_config(&rig.config_path());
    assert_eq!(
        ledger.load_statuses(&format!("{}.{}.{table}", bq.project, bq.dataset)),
        vec!["refused".to_string()],
        "the ledger records a refusal, never a failure that makes the target rivet's own"
    );
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_cdc_baseline_over_a_wide_history_lands_in_the_base_in_batches() {
    let Some(bq) = BqLive::from_env("pcdc") else {
        return;
    };
    let mut scn = CdcScenario::mysql_with(
        "pcdc",
        "id BIGINT PRIMARY KEY, v INT, created_at DATETIME NOT NULL",
        |r, t| {
            keyset_recipe_lines(
                r.cdc("backfill: auto")
                    .also_batch_export("baseline", t, "chunked"),
            )
            .dest_gcs_live(&bq.bucket, &bq.prefix)
            .top_line(
                &bq.load_line(", pk: [id], partition: { column: created_at, granularity: day }"),
            )
        },
    );
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    let seed_sql = format!(
        "INSERT INTO {table} (id, v, created_at) \
         WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < {ROWS}) \
         SELECT n, n, DATE_ADD('2000-01-01', INTERVAL n - 1 DAY) FROM s"
    );
    scn.sql("SET SESSION cte_max_recursion_depth = 10000");
    scn.sql(&seed_sql);

    // Anchor + the keyset baseline (five date-local files), then the load.
    scn.rig.run_ok();
    let out = scn.rig.cli(&["load"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "the CDC baseline must land in the base in batches:\n{said}"
    );
    assert!(
        said.contains("5 files in 2 load jobs") && said.contains("layout=base+buffer"),
        "{said}"
    );

    let (live, partitions, live_sum) = warehouse_profile(&bq, &table, "WHERE NOT __is_deleted");
    assert_eq!(
        (live, live_sum),
        (ROWS, expected_sum()),
        "the base holds the baseline once, every row live"
    );
    assert_eq!(partitions, ROWS, "DAY partitions on the base");
    assert_eq!(
        bq.read_bq_time_partitioning(&table),
        Some(("DAY".to_string(), Some("created_at".to_string())))
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "no buffer until the stream captures a change"
    );
}
