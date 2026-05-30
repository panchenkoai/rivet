//! Property-based type round-trip (OPT-3, live MySQL).
//!
//! The enumerated four-reader matrix proves the *listed* types; this proves the
//! **value mapping over generated inputs**. The `UNSIGNED BIGINT → Decimal128`
//! class was found "by example" in 0.7.8 — exactly the kind of integer-width /
//! signedness bug a property test catches. Here we generate random in-range
//! values across the risky classes (incl. `u64` values above `i64::MAX`), seed
//! a live MySQL table, export through rivet → Parquet, read it back, and assert
//! every value survives. proptest shrinks any failure to a minimal row set.
//!
//! Scope: MySQL value mapping for the integer-width / text / bool classes
//! (where signedness/precision bugs live). Random *schemas* over the full type
//! universe + Postgres are documented follow-ups.

use crate::common::*;
use mysql::prelude::Queryable;
use proptest::prelude::*;

use super::helpers::{MysqlCleanup, read_parquet_batches};

use arrow::array::{
    Array, BooleanArray, Int16Array, Int32Array, Int64Array, StringArray, UInt64Array,
};

#[derive(Debug, Clone)]
struct GenRow {
    tinyint_u: u8,
    c_int: i32,
    c_bigint: i64,
    c_bigint_u: u64,
    c_text: String,
    c_bool: bool,
}

fn row_strategy() -> impl Strategy<Value = GenRow> {
    // `any::<u64>()` deliberately includes values above i64::MAX — the exact
    // class the `UNSIGNED BIGINT` mapping must preserve. Text is printable
    // ASCII so the round-trip isolates the numeric/boolean mapping from charset
    // concerns (a separate matrix covers wide/unicode text).
    (
        any::<u8>(),
        any::<i32>(),
        any::<i64>(),
        any::<u64>(),
        "[ -~]{0,48}",
        any::<bool>(),
    )
        .prop_map(
            |(tinyint_u, c_int, c_bigint, c_bigint_u, c_text, c_bool)| GenRow {
                tinyint_u,
                c_int,
                c_bigint,
                c_bigint_u,
                c_text,
                c_bool,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    #[test]
    #[ignore = "live: requires docker compose up -d mysql"]
    fn mysql_value_roundtrip(rows in proptest::collection::vec(row_strategy(), 1..16)) {
        require_alive(LiveService::Mysql);

        let table = unique_name("type_rt_prop");
        let _guard = MysqlCleanup(table.clone());

        let mut conn = mysql_connect();
        conn.query_drop(format!("DROP TABLE IF EXISTS {table}")).unwrap();
        conn.query_drop(format!(
            "CREATE TABLE {table} ( \
               id INT PRIMARY KEY, \
               c_tinyint_u TINYINT UNSIGNED, \
               c_int INT, \
               c_bigint BIGINT, \
               c_bigint_u BIGINT UNSIGNED, \
               c_text TEXT, \
               c_bool BOOLEAN )"
        ))
        .unwrap();

        for (i, r) in rows.iter().enumerate() {
            conn.exec_drop(
                format!(
                    "INSERT INTO {table} \
                     (id, c_tinyint_u, c_int, c_bigint, c_bigint_u, c_text, c_bool) \
                     VALUES (?,?,?,?,?,?,?)"
                ),
                (
                    i as i32,
                    r.tinyint_u as u32,
                    r.c_int,
                    r.c_bigint,
                    r.c_bigint_u,
                    r.c_text.as_str(),
                    r.c_bool as i32,
                ),
            )
            .unwrap();
        }

        let export_name = unique_name("prop_my");
        let cfg_dir = tempfile::tempdir().unwrap();
        let out_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export_name}\n    \
             query: >-\n      SELECT id, c_tinyint_u, c_int, c_bigint, c_bigint_u, c_text, c_bool \
             FROM {table} ORDER BY id\n    mode: full\n    format: parquet\n    compression: zstd\n    \
             destination:\n      type: local\n      path: {out}\n",
            out = out_dir.path().display(),
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let out = run_rivet_export(&cfg, &export_name);
        prop_assert!(
            out.status.success(),
            "rivet export failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        let files = files_with_extension(out_dir.path(), "parquet");
        prop_assert_eq!(files.len(), 1, "expected exactly one parquet file");
        let (_schema, batches) = read_parquet_batches(&files[0]);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        prop_assert_eq!(total, rows.len(), "row count mismatch");

        // Compare every value in id order against the generated input.
        let mut idx = 0usize;
        for b in &batches {
            let tu = b.column_by_name("c_tinyint_u").unwrap().as_any().downcast_ref::<Int16Array>().unwrap();
            let ci = b.column_by_name("c_int").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let cb = b.column_by_name("c_bigint").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            let cbu = b.column_by_name("c_bigint_u").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
            let ct = b.column_by_name("c_text").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let cbool = b.column_by_name("c_bool").unwrap().as_any().downcast_ref::<BooleanArray>().unwrap();
            for j in 0..b.num_rows() {
                let r = &rows[idx];
                prop_assert_eq!(tu.value(j), r.tinyint_u as i16, "c_tinyint_u row {}", idx);
                prop_assert_eq!(ci.value(j), r.c_int, "c_int row {}", idx);
                prop_assert_eq!(cb.value(j), r.c_bigint, "c_bigint row {}", idx);
                prop_assert_eq!(cbu.value(j), r.c_bigint_u, "c_bigint_u row {} (the UNSIGNED class)", idx);
                prop_assert_eq!(ct.value(j), r.c_text.as_str(), "c_text row {}", idx);
                prop_assert_eq!(cbool.value(j), r.c_bool, "c_bool row {}", idx);
                idx += 1;
            }
        }
    }
}
