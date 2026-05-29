//! Live end-to-end coverage for keyset (seek) pagination (OPT-4).
//!
//! The planner/query/runner logic is unit-tested; this pins the *behavior* on a
//! real MySQL table with a non-integer (UUID-shaped `VARCHAR`) primary key:
//! chunked mode auto-selects keyset, pages the table by the unique key, and the
//! union of all page files reproduces the source key set exactly — no row
//! skipped or duplicated at a `WHERE key > last` page boundary.
//!
//! Run: `docker compose up -d mysql && cargo test --test live_keyset -- --ignored`.

mod common;
use common::*;

use std::collections::BTreeSet;

use arrow::array::{Array, StringArray};
use mysql::prelude::Queryable;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Drop the test table on exit even if an assertion fails.
struct DropTable(String);
impl Drop for DropTable {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

fn read_uid_set(dir: &std::path::Path) -> (usize, BTreeSet<String>) {
    let mut count = 0usize;
    let mut keys = BTreeSet::new();
    for path in files_with_extension(dir, "parquet") {
        let bytes = std::fs::read(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let uid = batch
                .column_by_name("uid")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..uid.len() {
                count += 1;
                keys.insert(uid.value(i).to_string());
            }
        }
    }
    (count, keys)
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_varchar_pk_roundtrips_full_keyset_across_pages() {
    require_alive(LiveService::Mysql);

    const N: usize = 3000;
    let table = unique_name("keyset_rt");
    let _guard = DropTable(table.clone());

    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    // Seed N rows with a non-integer PK so range chunking is impossible and the
    // planner must auto-select keyset. A recursive CTE keeps seeding to one
    // round-trip; bump the recursion ceiling above the default 1000.
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {N}) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    // Chunked mode, no chunk_column / chunk_by_key → auto-keyset on the unique
    // varchar PK. chunk_size 500 → 6 pages, so page boundaries are exercised.
    let export = unique_name("keyset_rt_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_size: 500\n    format: parquet\n    \
         compression: zstd\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Multiple page files (one per keyset page).
    let files = files_with_extension(out_dir.path(), "parquet");
    assert!(
        files.len() >= 2,
        "expected multiple keyset page files for {N} rows at chunk_size 500, got {}",
        files.len()
    );

    // The union of all pages must reproduce the source key set exactly — no row
    // dropped or duplicated at a `WHERE uid > last` boundary.
    let (count, keys) = read_uid_set(out_dir.path());
    let expected: BTreeSet<String> = (1..=N).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        count, N,
        "row count must round-trip exactly (no dupes/skips)"
    );
    assert_eq!(
        keys, expected,
        "the exported key set must equal the source key set"
    );
}
