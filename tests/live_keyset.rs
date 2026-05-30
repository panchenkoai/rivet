//! Live end-to-end coverage for keyset (seek) pagination (OPT-4).
//!
//! The planner/query/runner logic is unit-tested; this pins the *behavior* on
//! real tables with non-integer primary keys:
//!
//! - MySQL `VARCHAR(40)` PK (the original varchar shape).
//! - PostgreSQL `UUID` PK (the most common non-integer PK in production).
//! - MySQL `CHAR(36)` UUID PK (UUID storage as text in MySQL).
//!
//! For each shape: chunked mode auto-selects keyset, pages the table by the
//! unique key, and the union of all page files reproduces the source key set
//! exactly — no row skipped or duplicated at a `WHERE key > last` page
//! boundary.
//!
//! Run: `docker compose up -d postgres mysql && cargo test --test live_keyset -- --ignored`.

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

/// PostgreSQL UUID PK is the most common non-integer PK in production
/// (`id UUID PRIMARY KEY DEFAULT gen_random_uuid()` is the canonical
/// shape after `gen_random_uuid()` landed in core). The documented path
/// for PG UUID PK is **`mode: full`** — PG's `DECLARE CURSOR` snapshot
/// is already RAM-bounded, so a snapshot SELECT does not OOM the client
/// on a large UUID-PK table. See ADR-0020 for the explicit reasoning
/// (and the asymmetric MySQL story: MySQL has no server-side cursor,
/// so it auto-falls-through to keyset on non-int PK per OPT-4).
///
/// This test pins:
/// - `mode: full` accepts a UUID-PK table (introspection must report
///   the column existence; type-mapping must produce `arrow.uuid`
///   extension type metadata so parquet-rs emits native
///   `LogicalType::Uuid`),
/// - the export round-trips every UUID byte-for-byte through
///   `FixedSizeBinary(16)` → canonical UUID string decode.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn snapshot_pg_uuid_pk_roundtrips_full_uuid_set() {
    require_alive(LiveService::Postgres);

    const N: usize = 3000;
    let table = unique_name("pg_snap_uuid_rt");

    struct PgDropTable(String);
    impl Drop for PgDropTable {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = PgDropTable(table.clone());

    let mut c = pg_connect();
    // Deterministic UUIDs from a hand-formatted octet string — keeps the
    // expected BTreeSet computable without reading back from source.
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id UUID PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} (id, payload)
         SELECT
            ('00000000-0000-0000-0000-' || LPAD(to_hex(g), 12, '0'))::uuid,
            g
         FROM generate_series(1, {N}) g;"
    ))
    .unwrap();

    let export = unique_name("pg_snap_uuid_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: postgres\n  url: \"{POSTGRES_URL}\"\nexports:\n  - name: {export}\n    \
         table: public.{table}\n    mode: full\n    format: parquet\n    compression: zstd\n    \
         destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "PG UUID snapshot export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Single file expected — mode: full produces one part.
    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(
        files.len(),
        1,
        "mode:full must produce exactly one parquet file; got {:?}",
        files
    );

    // Read back the UUID column as FixedSizeBinary(16) and decode to
    // canonical UUID strings; pins the wire path PG `uuid` → Arrow
    // `FixedSizeBinary(16)` with `arrow.uuid` extension type metadata
    // (per ADR-0014 §"UUID / JSON / Binary") → parquet
    // `LogicalType::Uuid`.
    let (count, keys) = read_uuid_set_fixed(out_dir.path(), "id");
    let expected: BTreeSet<String> = (1..=N)
        .map(|n| format!("00000000-0000-0000-0000-{n:012x}"))
        .collect();
    assert_eq!(
        count, N,
        "row count must round-trip exactly (no dupes/skips)"
    );
    assert_eq!(
        keys, expected,
        "the exported UUID set must equal the source UUID set"
    );
}

/// MySQL UUID PK shape via `CHAR(36)` — the standard MySQL idiom for
/// UUID storage (MySQL has no native UUID type; `BINARY(16)` is also
/// common but loses textual key semantics). Mirrors the PG UUID test
/// above: introspection must pick the CHAR(36) column as a usable
/// keyset key, the runtime path must build `WHERE id > '<uuid>'` with
/// correct quoting under MySQL's `?` placeholder protocol, and the
/// pages must round-trip exactly.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_mysql_uuid_pk_roundtrips_full_keyset_across_pages() {
    require_alive(LiveService::Mysql);

    const N: usize = 3000;
    let table = unique_name("mysql_keyset_uuid_rt");
    let _guard = DropTable(table.clone());

    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (id CHAR(36) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    // Deterministic UUIDs from a recursive CTE, same shape as the PG test:
    // `00000000-0000-0000-0000-<n hex padded to 12>`. Keeps the row-count
    // assertion stable and the expected BTreeSet computable without reading
    // back the source.
    conn.query_drop(format!(
        "INSERT INTO {table} (id, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {N}) \
         SELECT CONCAT('00000000-0000-0000-0000-', LPAD(HEX(n), 12, '0')), n FROM seq"
    ))
    .unwrap();

    let export = unique_name("mysql_keyset_uuid_exp");
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
        "MySQL UUID keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert!(
        files.len() >= 2,
        "expected multiple keyset page files for {N} rows at chunk_size 500, got {}",
        files.len()
    );

    let (count, keys) = read_uid_set_named(out_dir.path(), "id");
    // MySQL HEX() returns uppercase hex; lowercase below would mismatch if not
    // normalized. We use lowercase on both sides for consistency with the PG
    // expectation and the BTreeSet ordering.
    let expected: BTreeSet<String> = (1..=N)
        .map(|n| format!("00000000-0000-0000-0000-{:012X}", n))
        .collect();
    assert_eq!(
        count, N,
        "row count must round-trip exactly (no dupes/skips)"
    );
    assert_eq!(
        keys, expected,
        "the exported UUID set must equal the source UUID set"
    );
}

/// Companion to `snapshot_pg_uuid_pk_roundtrips_full_uuid_set` for the
/// **explicit `chunk_by_key:` path**. PG's planner intentionally does not
/// auto-keyset for non-int PKs (the "PG keeps refusing" branch in
/// `plan/build.rs::resolve_chunked_strategy`; see ADR-0020), so the
/// operator's escape hatch for "PG, UUID PK, chunked keyset" is *explicit*
/// `chunk_by_key: id`. Before the sink runtime gained the
/// `FixedSizeBinary(16)` arm in `extract_last_cursor_value`, this path
/// failed at page 0 with "unsupported type" — this test pins the fix and
/// closes layer 2 of the gap documented in ADR-0020.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn keyset_pg_uuid_pk_via_explicit_chunk_by_key_roundtrips_full_set() {
    require_alive(LiveService::Postgres);

    const N: usize = 3000;
    let table = unique_name("pg_keyset_uuid_explicit");

    struct PgDropTable(String);
    impl Drop for PgDropTable {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = PgDropTable(table.clone());

    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id UUID PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} (id, payload)
         SELECT
            ('00000000-0000-0000-0000-' || LPAD(to_hex(g), 12, '0'))::uuid,
            g
         FROM generate_series(1, {N}) g;"
    ))
    .unwrap();

    let export = unique_name("pg_keyset_uuid_explicit_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    // `chunk_by_key: id` opts into keyset paging on a UUID column despite
    // PG's planner default of refusing auto-resolution for non-int PKs.
    // chunk_size 500 → 6 pages, exercising every page boundary.
    let yaml = format!(
        "source:\n  type: postgres\n  url: \"{POSTGRES_URL}\"\nexports:\n  - name: {export}\n    \
         table: public.{table}\n    mode: chunked\n    chunk_by_key: id\n    chunk_size: 500\n    \
         format: parquet\n    compression: zstd\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "PG UUID keyset (explicit chunk_by_key) must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ≥2 page files prove pagination ran.
    let files = files_with_extension(out_dir.path(), "parquet");
    assert!(
        files.len() >= 2,
        "expected multiple keyset page files for {N} rows at chunk_size 500, got {}",
        files.len()
    );

    // Exact UUID set round-trip across the pages — the page boundary
    // value (`E'<uuid>'` literal cast on the server) survives the
    // FixedSizeBinary(16) → UUID-string → next-page WHERE clause cycle.
    let (count, keys) = read_uuid_set_fixed(out_dir.path(), "id");
    let expected: BTreeSet<String> = (1..=N)
        .map(|n| format!("00000000-0000-0000-0000-{n:012x}"))
        .collect();
    assert_eq!(
        count, N,
        "row count must round-trip exactly across keyset pages (no dupes/skips at boundaries)"
    );
    assert_eq!(
        keys, expected,
        "the exported UUID set must equal the source UUID set"
    );
}

/// Variant of `read_uid_set` that takes the column name as an argument so
/// the new UUID tests can read `id` while the original varchar test stays
/// on `uid` without churn.
fn read_uid_set_named(dir: &std::path::Path, col: &str) -> (usize, BTreeSet<String>) {
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
            let arr = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column '{col}' missing from parquet output"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| {
                    panic!("column '{col}' must decode as utf8 string (UUID is stored as text)")
                });
            for i in 0..arr.len() {
                count += 1;
                keys.insert(arr.value(i).to_string());
            }
        }
    }
    (count, keys)
}

/// Variant of `read_uid_set_named` that decodes `FixedSizeBinary(16)`
/// columns (PG `uuid` mapping per ADR-0014) into canonical UUID strings
/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. The set comparison stays
/// comparable to MySQL's `CHAR(36)` → `Utf8` path.
fn read_uuid_set_fixed(dir: &std::path::Path, col: &str) -> (usize, BTreeSet<String>) {
    use arrow::array::FixedSizeBinaryArray;

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
            let arr = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column '{col}' missing from parquet output"))
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap_or_else(|| {
                    panic!(
                        "column '{col}' must decode as FixedSizeBinary(16) — PG uuid maps there \
                         per ADR-0014"
                    )
                });
            for i in 0..arr.len() {
                count += 1;
                let bytes = arr.value(i);
                // Canonical 8-4-4-4-12 hex representation.
                let s = format!(
                    "{:02x}{:02x}{:02x}{:02x}-\
                     {:02x}{:02x}-\
                     {:02x}{:02x}-\
                     {:02x}{:02x}-\
                     {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0],
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                    bytes[5],
                    bytes[6],
                    bytes[7],
                    bytes[8],
                    bytes[9],
                    bytes[10],
                    bytes[11],
                    bytes[12],
                    bytes[13],
                    bytes[14],
                    bytes[15],
                );
                keys.insert(s);
            }
        }
    }
    (count, keys)
}
