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
//! Run: `docker compose up -d postgres mysql && cargo test --test live_suite -- --ignored`.

use crate::common::*;

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

/// Form B on the keyset runner (round-9 gap fix): the manifest must RECORD the
/// per-column value checksums — previously the sink computed them per page then
/// DROPPED them, so `rivet validate`'s Form-B re-read was a silent no-op on keyset
/// (a large-table path). Assert (1) the manifest carries a non-empty
/// column_checksums array (RED before the run-wide harvest), and (2) `rivet
/// validate` re-reads the parts and PASSES — proving the recorded XOR-combined
/// checksums actually match the parquet, not just that the array is populated.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_export_records_form_b_checksums_and_validate_passes() {
    require_alive(LiveService::Postgres);
    let table = unique_name("keyset_formb");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (k TEXT PRIMARY KEY, v INT NOT NULL, note TEXT);
         INSERT INTO {table} SELECT 'k' || lpad(g::text, 6, '0'), g, 'n' || g \
         FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let export = unique_name("keyset_formb_exp");
    // TEXT key + chunk_by_key → keyset, chunk_size 500 → 4 pages (cross-page XOR).
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: k\n    chunk_size: 500\n    \
         format: parquet\n    destination: {{type: local, path: {out}}}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    // (1) The manifest records Form B checksums (RED before the harvest — empty).
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out_dir.path().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    let checksums = manifest["column_checksums"].as_array();
    assert!(
        checksums.is_some_and(|a| !a.is_empty()),
        "keyset manifest must record Form B column_checksums, got: {}",
        manifest["column_checksums"]
    );

    // (2) validate (default depth Full → runs the Form B re-read) re-reads the
    // parts; the recorded checksums must MATCH.
    let v = std::process::Command::new(RIVET_BIN)
        .args([
            "validate",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet validate");
    assert!(
        v.status.success(),
        "rivet validate must PASS — the recorded Form B checksums must match the re-read parts; \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
}

/// v18 failure-forensics on the KEYSET runner: a keyset export must persist the
/// self-sufficient debug columns on its `export_metrics` row + a schema-at-open
/// `export_schema` row, so a keyset FAILURE is legible WITHOUT re-querying the
/// source. Uses 2000 rows / chunk 500 = 4 EXACT pages, so the run exits via the
/// empty-seek `else break` — the path that recorded `cursor_max = null` before the
/// fix (RED there). Guards, in one live run, four gaps closed this round:
/// cursor_max (both loop exits), server_context_json (source limits), the enriched
/// key_descriptor_json (strategy + key + db_type), and schema-at-open.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_export_persists_v18_forensics_columns() {
    require_alive(LiveService::Postgres);
    let table = unique_name("keyset_forensics");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let export = unique_name("keyset_forensics_exp");
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: id\n    chunk_size: 500\n    \
         format: parquet\n    destination: {{type: local, path: {out}}}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    let db = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db).expect("open state db");
    let (cursor_max, server_ctx, key_desc, error_class): (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT cursor_max, server_context_json, key_descriptor_json, error_class \
             FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT 1",
            [&export],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("an export_metrics row must exist after the run");

    // cursor_max = the max key reached, captured on the empty-seek `else break`
    // (2000 / 500 = 4 exact pages). RED (null) before that exit set cursor_high.
    assert_eq!(
        cursor_max.as_deref(),
        Some("2000"),
        "cursor_max must be the table's max key (via the exact-multiple else-break exit)"
    );
    // server_context captured at OPEN — the source limits that explain a timeout.
    let sc = server_ctx.expect("server_context_json must be captured at open");
    assert!(
        sc.contains("postgres") && sc.contains("statement_timeout"),
        "server_context: {sc}"
    );
    // key descriptor: strategy + key + the resolved source native type.
    let kd = key_desc.expect("key_descriptor_json must be set on keyset");
    assert!(
        kd.contains("\"strategy\":\"keyset\"") && kd.contains("\"key\":\"id\""),
        "kd: {kd}"
    );
    assert!(
        kd.contains("\"db_type\""),
        "key_descriptor must carry the key's db_type (schema-at-open resolved it): {kd}"
    );
    assert!(
        error_class.is_none(),
        "a successful run has no error_class: {error_class:?}"
    );

    // schema-at-open: export_schema carries the columns even though (here) the run
    // succeeded — the point is the row exists from the OPEN probe, not finalize.
    let schema_cols: String = conn
        .query_row(
            "SELECT columns_json FROM export_schema WHERE export_name = ?1",
            [&export],
            |r| r.get(0),
        )
        .expect("schema-at-open must record an export_schema row");
    assert!(
        schema_cols.contains("\"id\""),
        "schema-at-open must list the columns: {schema_cols}"
    );
}

/// Cross-shape manifest guard on the KEYSET runner: a batch keyset export must
/// refuse to overwrite a prior CDC manifest at the same prefix (they would
/// silently destroy each other's audit trail). Every batch runner calls
/// guard_manifest_mode; this proves the keyset column of the runner-coverage
/// matrix, the sibling of the checkpoint gap the graph surfaced.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn keyset_export_refuses_to_clobber_a_cdc_manifest() {
    require_alive(LiveService::Postgres);
    let table = unique_name("keyset_guard");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (k TEXT PRIMARY KEY, v INT NOT NULL);
         INSERT INTO {table} SELECT 'k' || lpad(g::text, 6, '0'), g \
         FROM generate_series(1, 100) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    // A prior CDC run's manifest already sits at the destination prefix.
    std::fs::write(
        out_dir.path().join("manifest.json"),
        br#"{"manifest_version":1,"run_id":"prior-cdc","mode":"cdc","parts":[]}"#,
    )
    .unwrap();
    let export = unique_name("keyset_guard_exp");
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: k\n    chunk_size: 50\n    \
         format: parquet\n    destination: {{type: local, path: {out}}}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let r = run_rivet_export(&cfg, &export);
    assert!(
        !r.status.success(),
        "keyset export must REFUSE to overwrite a CDC manifest"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&r.stdout),
        String::from_utf8_lossy(&r.stderr)
    );
    assert!(
        combined.contains("already holds a 'cdc' manifest"),
        "must name the cross-shape collision; got:\n{combined}"
    );
}

/// Form B on the CHUNKED (range) runner — same round-9 gap + same run-wide XOR
/// harvest as keyset, but a different runner (exec.rs, sequential + parallel).
/// An integer chunk_column routes to range chunking, not keyset.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_export_records_form_b_checksums_and_validate_passes() {
    require_alive(LiveService::Postgres);
    let table = unique_name("chunked_formb");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v INT NOT NULL, note TEXT);
         INSERT INTO {table} SELECT g, g * 2, 'n' || g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let export = unique_name("chunked_formb_exp");
    // Integer chunk_column → range chunking (the chunked runner), chunk_size 500.
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_column: id\n    chunk_size: 500\n    \
         format: parquet\n    destination: {{type: local, path: {out}}}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "chunked export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out_dir.path().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    assert!(
        manifest["column_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "chunked manifest must record Form B column_checksums, got: {}",
        manifest["column_checksums"]
    );

    let v = std::process::Command::new(RIVET_BIN)
        .args([
            "validate",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet validate");
    assert!(
        v.status.success(),
        "rivet validate must PASS on the chunked export — recorded Form B must match the re-read; \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
}

/// Form B on the CHECKPOINT (resumable `chunk_checkpoint: true`) chunked runner —
/// a SEPARATE runner (chunked/sequential_checkpoint.rs) the graph surfaced as a
/// distinct 300+-line function that the first Form B pass (exec.rs only) MISSED.
/// The resumable path must record Form B too.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_checkpoint_export_records_form_b_checksums_and_validate_passes() {
    require_alive(LiveService::Postgres);
    let table = unique_name("ckpt_formb");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v INT NOT NULL, note TEXT);
         INSERT INTO {table} SELECT g, g * 2, 'n' || g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let export = unique_name("ckpt_formb_exp");
    // chunk_checkpoint: true routes to the CHECKPOINT runner (not exec.rs).
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_column: id\n    chunk_size: 500\n    \
         chunk_checkpoint: true\n    format: parquet\n    destination: {{type: local, path: {out}}}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "chunked-checkpoint export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out_dir.path().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    assert!(
        manifest["column_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "chunked-CHECKPOINT manifest must record Form B column_checksums, got: {}",
        manifest["column_checksums"]
    );

    let v = std::process::Command::new(RIVET_BIN)
        .args([
            "validate",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet validate");
    assert!(
        v.status.success(),
        "rivet validate must PASS on the checkpoint chunked export — recorded Form B must match; \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
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

/// PARALLEL keyset (feat/parallel-keyset, iteration 1): `parallel: N` fans N
/// ROW-percentile-range workers, each keyset-paging a disjoint `(lo, hi]` slice
/// concurrently. The union of every worker's pages must reproduce the source key
/// set EXACTLY — parity is STRUCTURAL (the N−1 boundaries partition the key into
/// half-open intervals whose union is the whole key space), so it holds no matter
/// how skewed the sample is.
///
/// Two assertions, and the FIRST is the self-oracle that makes the second mean
/// something: a plain SEQUENTIAL keyset run passes the union check too, so the
/// test would be vacuous without proving parallel actually ran. The `pk_w{id}`
/// part-name stamp (only the parallel runner emits it) IS that proof — assert it
/// before the union, so a silent fall-back to sequential fails here, not slips by.
///
/// RED proof (run before committing): mutate the boundary in
/// `build_keyset_query_bounded` from `AND key <= upper` to `AND key < upper` —
/// each of the N−1 boundary keys then falls in NO worker's half-open range (its
/// owning worker excludes it as `< hi`, the next excludes it as `> lo`), so the
/// count drops from 3000 to 2997 and the distinct-set names the 3 dropped keys.
/// A VARCHAR PK is deliberate: it exercises the string-literal boundary escaping
/// (`escape_mysql_literal`, quotes) the integer path never hits.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_parallel_reads_every_row_once_across_workers() {
    require_alive(LiveService::Mysql);

    const N: usize = 3000;
    let table = unique_name("keyset_par");
    let _guard = DropTable(table.clone());

    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {N}) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    // Auto-keyset on the VARCHAR PK + `parallel: 4`. chunk_size 500 → each ~750-row
    // worker still pages twice, so BOTH the inter-worker boundary and the
    // intra-worker `WHERE uid > last` boundary are exercised in one run.
    let export = unique_name("keyset_par_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    parallel: 4\n    chunk_size: 500\n    \
         format: parquet\n    compression: zstd\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "parallel keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // (1) SELF-ORACLE: the parallel runner is the only path that stamps `pk_w{id}`
    // into part names. Assert it ran — else the union check below is satisfied by a
    // silent sequential fall-back and proves nothing about parallelism.
    let parts = files_with_extension(out_dir.path(), "parquet");
    let parallel_parts = parts
        .iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains("_pk_w"))
        })
        .count();
    assert!(
        parallel_parts >= 2,
        "expected the parallel runner's `pk_w{{id}}` parts (≥2 workers); got files: {:?}",
        parts
            .iter()
            .filter_map(|p| p.file_name().and_then(|n| n.to_str()))
            .collect::<Vec<_>>()
    );

    // (2) STRUCTURAL PARITY: the union of every worker's pages is the whole key set,
    // no row dropped at a boundary (RED at `< hi`: 2997/3000) or duplicated across
    // two workers' overlapping ranges (would inflate count past N).
    let (count, keys) = read_uid_set(out_dir.path());
    let expected: BTreeSet<String> = (1..=N).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        count, N,
        "row count must round-trip exactly across all workers (no boundary drop/dupe)"
    );
    assert_eq!(
        keys, expected,
        "the union of all parallel workers' keys must equal the source key set"
    );
}

/// PARALLEL keyset on a PostgreSQL native `uuid` PK — the canonical
/// `id UUID PRIMARY KEY` shape. The boundary probe (`query_scalar`) must READ the
/// uuid to sample percentiles; without the uuid arm (M2) it returns None, the
/// sampler gets zero boundaries, and `parallel: N` silently COLLAPSES to a single
/// worker (data complete, but the fan-out absent). Asserts the run fanned out to
/// ≥2 `pk_w{id}` workers AND round-trips every uuid — RED before the query_scalar
/// uuid arm (1 worker → 1 part-family).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn keyset_parallel_pg_uuid_key_fans_out_not_collapses() {
    require_alive(LiveService::Postgres);
    const N: usize = 4000;
    let table = unique_name("pg_par_uuid");
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
         SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid, g
         FROM generate_series(1, {N}) g;"
    ))
    .unwrap();

    let export = unique_name("pg_par_uuid_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\nexports:\n  - name: {export}\n    \
         table: public.{table}\n    mode: chunked\n    chunk_by_key: id\n    parallel: 4\n    \
         chunk_size: 500\n    format: parquet\n    destination: {{type: local, path: {out}}}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "PG uuid parallel keyset must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    // Fan-out proof: ≥2 distinct pk_w{id} workers (a collapse = only pk_w0).
    let workers: BTreeSet<String> = files_with_extension(out_dir.path(), "parquet")
        .iter()
        .filter_map(|p| p.file_name().and_then(|n| n.to_str()).map(String::from))
        .filter_map(|n| {
            n.split("_pk_w")
                .nth(1)
                .and_then(|s| s.split('_').next())
                .map(|w| w.to_string())
        })
        .collect();
    assert!(
        workers.len() >= 2,
        "PG uuid parallel keyset must fan out to ≥2 workers, got workers {workers:?} — the \
         boundary probe collapsed to a single worker (query_scalar uuid arm missing)"
    );

    // Completeness: every uuid round-trips (PG's first-party parallel completeness
    // check — the gap the bughunt flagged as absent).
    let (count, keys) = read_uuid_set_fixed(out_dir.path(), "id");
    let expected: BTreeSet<String> = (1..=N)
        .map(|n| format!("00000000-0000-0000-0000-{n:012x}"))
        .collect();
    assert_eq!(count, N, "row count must round-trip exactly");
    assert_eq!(keys, expected, "the uuid set must equal the source");
}

/// PARALLEL keyset CRASH-RECOVERY (feat/parallel-keyset iteration 2). A parallel
/// run with `chunk_checkpoint: true` that crashes AFTER one range commits (its
/// parts durable in file_log, its `keyset_range` row `done=1`) but before finalize
/// must, on resume, produce a COMPLETE destination manifest: the done range's parts
/// are REHYDRATED from file_log (not re-read — it is skipped), the crashed ranges
/// are re-run from their `lo` (their run_id-named partial parts overwritten), and
/// the union is every row exactly once.
///
/// Discriminator = the DESTINATION manifest's `row_count` (not a parquet glob — the
/// committed parquet survives on disk regardless, so a re-read cannot see an
/// orphaned-from-the-manifest part). RED against removing the resume rehydrate in
/// `run_keyset_parallel`: the done range's pre-crash parts are then absent from the
/// manifest (row_count < N) — the manifest-authoritative loader would silently drop
/// them. That the mutant goes RED is ALSO the proof the done range was SKIPPED, not
/// re-read: if it were re-read from source, rehydrate would be immaterial.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_parallel_crash_resume_writes_a_complete_destination_manifest() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_par_crash");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 2000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    // parallel: 4 + chunk_checkpoint → per-range crash-recovery. Small chunk_size so
    // each ~500-row range pages several times (a crash mid-range is mid-page-set).
    let export = unique_name("keyset_par_crash_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    parallel: 4\n    \
         chunk_checkpoint: true\n    chunk_size: 200\n    format: parquet\n    \
         destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Run 1: HARD-EXIT (process dies) right after range 0's atomic checkpoint commit
    // — range 0 is durably `done`, ranges 1-3 wrote parts to disk but never committed.
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:0")
        .output()
        .expect("spawn rivet");
    assert!(
        !crash.status.success(),
        "the injected hard-exit must make run 1 fail"
    );

    // Resume: skips the done range (rehydrates it), re-runs the rest.
    let resume = run_rivet_export(&cfg, &export);
    assert!(
        resume.status.success(),
        "resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // MANIFEST-DRIVEN completeness: the destination manifest must declare ALL 2000
    // rows — the pre-crash done range must be rehydrated, not orphaned.
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out_dir.path().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    assert_eq!(
        m["row_count"].as_i64(),
        Some(2000),
        "destination manifest must declare every row (done range not orphaned); got {}",
        m["row_count"]
    );

    // And the physical union is complete with no duplicate keys (the crashed
    // ranges' partial parts were overwritten by the re-run, not accumulated).
    let (count, keys) = read_uid_set(out_dir.path());
    let expected: BTreeSet<String> = (1..=2000).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        count, 2000,
        "parquet union must hold every row exactly once"
    );
    assert_eq!(
        keys, expected,
        "the union must equal the full source key set"
    );
}

/// PARALLEL keyset INCREMENTAL (feat/parallel-keyset iteration 3). A parallel run
/// with `keyset_incremental` seeks past the persisted anchor and ADVANCES it at
/// success; a CLEAN re-run pulls only keys past the high-water mark, across N
/// workers (each range floored at the anchor, the last ceiling'd at the source max
/// pinned at open). Discriminator = the TOTAL row count across all part files (a
/// set dedups, so the union of keys can't tell a re-read from an incremental
/// resume): 1000 → 1000 → 1500, never 2000 / 2500 (a full re-read). RED against
/// the anchor not advancing (planner disabling it, or the runner not updating it):
/// run 2 would re-read all 1000 → total 2000.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_parallel_incremental_second_run_captures_only_new_keys() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_par_inc");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    let seed = |conn: &mut mysql::PooledConn, lo: usize, hi: usize| {
        conn.query_drop(format!(
            "INSERT INTO {table} (uid, payload) \
             WITH RECURSIVE seq AS (SELECT {lo} n UNION ALL SELECT n+1 FROM seq WHERE n < {hi}) \
             SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
        ))
        .unwrap();
    };
    seed(&mut conn, 1, 1000);

    let export = unique_name("keyset_par_inc_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    parallel: 4\n    \
         chunk_checkpoint: true\n    keyset_incremental: true\n    chunk_size: 200\n    \
         format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let run = |label: &str| {
        let out = run_rivet_export(&cfg, &export);
        assert!(
            out.status.success(),
            "{label} failed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    // Run 1: all 1000, anchor persisted at the max key (id-001000).
    run("run 1");
    let (count1, _) = read_uid_set(out_dir.path());
    assert_eq!(count1, 1000, "run 1 must export all seeded rows");

    // Run 2 on the UNCHANGED source: anchor floor = id-001000 → ZERO new rows; the
    // total across files stays 1000 (a re-read would double it to 2000).
    run("run 2 (unchanged)");
    let (count2, _) = read_uid_set(out_dir.path());
    assert_eq!(
        count2, 1000,
        "unchanged incremental re-run must add zero rows (got {count2}) — 2000 = a full re-read"
    );

    // Insert 500 rows with HIGHER keys, resume: only those 500 are read across the
    // N workers.
    seed(&mut conn, 1001, 1500);
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(out_dir.path());
    assert_eq!(
        count3, 1500,
        "incremental must add ONLY the 500 new keys (got {count3}); 2500 = a full re-read"
    );
    let expected: BTreeSet<String> = (1..=1500).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        keys3, expected,
        "the union of all runs must equal the full source key set"
    );
}

/// Two-run keyset RESUME (`chunk_checkpoint: true` — OPT-4 + Phase 2). Run 1
/// exports the whole key set and persists the high-water key; an UNCHANGED
/// re-run exports ZERO new rows; after inserting rows with higher keys, the next
/// run exports ONLY those — it resumes from the last committed key rather than
/// re-reading the table. The SQL analogue of Mongo keyset resume, enabled by
/// threading `chunk_checkpoint` into the SQL `KeysetPlan` (was hardcoded
/// `checkpoint: false` in `plan::build`).
///
/// Discriminator = the TOTAL row count across all page files: with resume it is
/// 1000 → 1000 → 1500 (run 3 adds only the 500 new keys); without resume a
/// re-read would inflate it to 2500. The union-of-keys alone cannot tell the two
/// apart (a set dedups), so we assert the running row TOTAL, not just the keys —
/// the exact "capture-works ≠ resume-works" trap the two-run test exists to close.
/// Round-5: a keyset checkpoint crash before the terminal manifest leaves committed
/// pages durably on the destination (parquet + file_log) but with NO manifest; the
/// resume continues from the cursor and skips them, so finalize used to write a
/// manifest of ONLY the resume's pages — orphaning the pre-crash pages from the
/// manifest-authoritative loader (silent loss). This reads the DESTINATION
/// manifest.json (not a parquet glob) and asserts it declares EVERY row. RED before
/// the resume_run_id + file_log rehydration.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_checkpoint_crash_resume_writes_a_complete_destination_manifest() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_m5");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    let export = unique_name("keyset_m5_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Crash after page 0 commits (300 rows durable, cursor advanced, no manifest).
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "after_keyset_page:0")
        .output()
        .expect("spawn rivet");
    assert!(!crash.status.success(), "crash run must exit non-zero");

    // Resume (keyset checkpoint auto-resumes from the cursor).
    let resume = run_rivet_export(&cfg, &export);
    assert!(
        resume.status.success(),
        "resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    // MANIFEST-DRIVEN: the destination manifest.json must declare all 1000 rows —
    // page 0 (pre-crash) must be rehydrated, not orphaned.
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out_dir.path().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    assert_eq!(
        m["row_count"].as_i64(),
        Some(1000),
        "destination manifest must declare every row (pre-crash page not orphaned); got {}",
        m["row_count"]
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_checkpoint_resume_second_run_captures_only_new_keys() {
    require_alive(LiveService::Mysql);

    let table = unique_name("keyset_ckpt");
    let _guard = DropTable(table.clone());

    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    let seed = |conn: &mut mysql::PooledConn, lo: usize, hi: usize| {
        conn.query_drop(format!(
            "INSERT INTO {table} (uid, payload) \
             WITH RECURSIVE seq AS (SELECT {lo} n UNION ALL SELECT n+1 FROM seq WHERE n < {hi}) \
             SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
        ))
        .unwrap();
    };
    seed(&mut conn, 1, 1000);

    // Explicit keyset key + checkpoint + keyset_incremental. Same cfg dir across
    // runs so the `.rivet_state.db` (written next to the config) is shared → run
    // 2/3 continue. `keyset_incremental: true` is the append-only opt-in: since
    // the crash-recovery/incremental split, chunk_checkpoint ALONE would re-read
    // the whole table on a clean re-run; this flag is what makes a clean re-run
    // pull only new keys (the behaviour this test asserts).
    let export = unique_name("keyset_ckpt_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         keyset_incremental: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = |label: &str| {
        let out = run_rivet_export(&cfg, &export);
        assert!(
            out.status.success(),
            "{label} failed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        // Distinct millisecond part stamp so a resumed run's parts never clobber
        // the prior run's (run-uniqueness rule).
        // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
        // back-to-back sub-second runs must not collide — sleeping here would
        // mask exactly that regression (matrix audit: sleep-masked class).
    };

    // Run 1: exports all 1000, persists high-water key id-001000.
    run("run 1");
    let (count1, _) = read_uid_set(out_dir.path());
    assert_eq!(count1, 1000, "run 1 must export all seeded rows");

    // Run 2 on the UNCHANGED source: resume floor is id-001000 → ZERO new rows,
    // no file written; the total across files stays 1000 (no re-read).
    run("run 2 (unchanged)");
    let (count2, _) = read_uid_set(out_dir.path());
    assert_eq!(
        count2, 1000,
        "unchanged resume must add zero rows (got {count2}) — a re-read would double it"
    );

    // Insert 500 rows with HIGHER keys, then resume: only those 500 are read.
    seed(&mut conn, 1001, 1500);
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(out_dir.path());
    assert_eq!(
        count3, 1500,
        "resume must add ONLY the 500 new keys (got {count3}); 2500 would mean a full re-read"
    );
    let expected: BTreeSet<String> = (1..=1500).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        keys3, expected,
        "the union of all runs must equal the full source key set"
    );
    // `read_uid_set` is a parquet re-read; assert the dest manifest COPIES
    // (`manifest-<run>.json`, reconcile's artifact) span every run — a resumed
    // run clobbering a prior manifest is silent to the parquet re-read.
    assert_eq!(
        dir_manifest_copy_total_rows(out_dir.path()),
        1500,
        "run-unique manifest copies must sum run 1 (1000) + run 3 (500); a clobbered manifest is silent to the parquet re-read"
    );
}

/// SAFETY (crash-recovery ⇄ incremental split): keyset `chunk_checkpoint: true`
/// WITHOUT `keyset_incremental` must FULLY re-read on a CLEAN re-run — never
/// silently skip already-exported rows. Before the split, chunk_checkpoint
/// implied incremental-by-key, so a clean re-run of a MUTABLE table skipped every
/// row whose key had already passed (silent staleness — the production footgun
/// that kept `init` from defaulting keyset checkpoint on). A crash still resumes
/// (via the in-progress run_id — see `..._crash_resume_...`); a clean completion
/// clears that run_id, so a plain re-run starts fresh. This is the RED-proof:
/// against the old conflated behaviour the second run reads 1000 (skip), not 2000.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_checkpoint_without_incremental_rereads_on_a_clean_rerun() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_safe");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    // chunk_checkpoint: true but NO keyset_incremental → crash-recovery ONLY.
    let export = unique_name("keyset_safe_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let run = |label: &str| {
        let out = run_rivet_export(&cfg, &export);
        assert!(
            out.status.success(),
            "{label} failed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    // Run 1 completes cleanly → the in-progress run_id is cleared at finalize.
    run("run 1");
    let (count1, _) = read_uid_set(out_dir.path());
    assert_eq!(count1, 1000, "run 1 must export all seeded rows");

    // Run 2 on the UNCHANGED source: a CLEAN re-run without keyset_incremental
    // must re-read all 1000 again (full pass) — the parquet re-read total doubles
    // to 2000. A total of 1000 would mean checkpoint silently implied incremental
    // (the pre-split staleness bug this test guards).
    run("run 2 (clean re-run)");
    let (count2, keys2) = read_uid_set(out_dir.path());
    assert_eq!(
        count2, 2000,
        "clean re-run must FULLY re-read (2000 rows across both runs); {count2} == 1000 would be a silent incremental skip"
    );
    let expected: BTreeSet<String> = (1..=1000).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        keys2, expected,
        "the key set stays the 1000 source keys (a re-read, not new data)"
    );
}

/// #1 (CRITICAL, adversarial-hunt): a FRESH non-incremental keyset run that crashes
/// AFTER open but BEFORE its first page commit must, on recovery, re-read from the
/// START — never resume from a PRIOR completed run's stale high-water mark. Before
/// the cursor-clear fix, Run 2 (fresh) set resume_run_id but left last_cursor_value
/// at Run 1's final key; a crash before the first commit meant Run 3 loaded that
/// stale key, issued `WHERE key > <max>`, read 0 rows, and wrote a SUCCESSFUL empty
/// export — the entire table silently skipped. RED against the pre-fix build.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_fresh_run_crash_before_first_page_does_not_skip_the_table() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_stale");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    let export = unique_name("keyset_stale_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Run 1: full export of all 1000; on data-completion resume_run_id is cleared
    // and last_cursor_value = the final key id-001000.
    let r1 = run_rivet_export(&cfg, &export);
    assert!(
        r1.status.success(),
        "run 1 stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let (count1, _) = read_uid_set(out_dir.path());
    assert_eq!(count1, 1000, "run 1 must export all 1000");

    // Run 2: a FRESH run that crashes right after open (no page committed).
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "keyset_after_open_before_first_page")
        .output()
        .expect("spawn rivet");
    assert!(!crash.status.success(), "crash run must exit non-zero");

    // Run 3: recovery. It MUST re-read all 1000 (total across run1+run3 = 2000).
    // A total of 1000 means Run 3 resumed from Run 1's stale cursor and skipped the
    // whole table — the critical silent-loss bug.
    let r3 = run_rivet_export(&cfg, &export);
    assert!(
        r3.status.success(),
        "run 3 stderr:\n{}",
        String::from_utf8_lossy(&r3.stderr)
    );
    let (count3, keys3) = read_uid_set(out_dir.path());
    assert_eq!(
        count3, 2000,
        "recovery must re-read all 1000 (total 2000); {count3} == 1000 means Run 3 resumed from a STALE cursor and silently skipped the whole table"
    );
    let expected: BTreeSet<String> = (1..=1000).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(keys3, expected, "the full source key set must be present");
}

/// #3 (MEDIUM, adversarial-hunt): a keyset run that commits ALL its data then fails
/// (a post-data gate, or any late failure) must NOT leave a resume anchor — the next
/// run is a fresh full pass, never a crash-recovery that skips rows updated since.
/// Data-completion clears resume_run_id, so a subsequent failure cannot strand it.
/// RED against a build that only cleared at finalize (skipped on failure).
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_failure_after_data_complete_does_not_resume_and_skip_on_the_next_run() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_gate");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    let export = unique_name("keyset_gate_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Run 1: commits ALL 1000, then fails AFTER data-completion (a stand-in for a
    // post-data gate rejection). Data-completion has already cleared resume_run_id.
    let fail = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "keyset_after_data_complete")
        .output()
        .expect("spawn rivet");
    assert!(
        !fail.status.success(),
        "the post-data failure must exit non-zero"
    );

    // Run 2: a deliberate full re-run. It MUST re-read all 1000 (total 2000), NOT
    // resume from the high-water mark. A total of 1000 means Run 1's stale
    // resume_run_id made Run 2 a crash-recovery that skipped the whole table.
    let r2 = run_rivet_export(&cfg, &export);
    assert!(
        r2.status.success(),
        "run 2 stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let (count2, _) = read_uid_set(out_dir.path());
    assert_eq!(
        count2, 2000,
        "the re-run after a post-data failure must FULLY re-read (total 2000); {count2} == 1000 means a stale resume anchor silently skipped the table"
    );
}

/// Round-2 hunt HIGH: the INCREMENTAL keyset variant must KEEP its resume anchor
/// at data-complete (unlike non-incremental, which clears it) so a crash in the
/// [data-complete → finalize_manifest] window rehydrates the committed pages
/// instead of orphaning them. The committed parquet SURVIVES on disk either way,
/// so this asserts on the MANIFEST (dir_manifest_copy_total_rows), not a parquet
/// re-read — the loss is manifest-level and invisible to a data re-read. RED
/// against an unconditional data-complete clear_resume_run_id (manifest = 0).
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_incremental_crash_before_finalize_rehydrates_not_orphans_the_manifest() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_inc_orphan");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), n FROM seq"
    ))
    .unwrap();

    // Incremental keyset (append-only opt-in). chunk_checkpoint implied by the
    // planner, but set explicitly too for clarity.
    let export = unique_name("keyset_inc_orphan_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mysql\n  url: \"{MYSQL_URL}\"\nexports:\n  - name: {export}\n    \
         table: {table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         keyset_incremental: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Run 1: commits all 1000 pages under R1, then crashes AFTER data-complete but
    // BEFORE finalize_manifest — so NO destination manifest for R1 is written.
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "keyset_after_data_complete")
        .output()
        .expect("spawn rivet");
    assert!(!crash.status.success(), "crash run must exit non-zero");

    // Run 2 (incremental): must REHYDRATE R1's committed pages into a complete
    // manifest (1000 rows), not orphan them. The parquet survives on disk
    // regardless, so we assert on the run-unique manifest copies. 0 means R1's
    // committed pages were orphaned — the manifest-authoritative loader would
    // silently drop them.
    let r2 = run_rivet_export(&cfg, &export);
    assert!(
        r2.status.success(),
        "run 2 stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    assert_eq!(
        dir_manifest_copy_total_rows(out_dir.path()),
        1000,
        "an incremental crash before finalize must rehydrate all 1000 committed rows into the manifest; 0 means the pages were orphaned (silent manifest-level row loss)"
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

/// PostgreSQL twin of `keyset_checkpoint_resume_second_run_captures_only_new_keys`.
/// The resume logic in `pipeline::keyset` is engine-agnostic (persist the max key
/// to `export_state`, read it back next run), but per the project's resume
/// discipline "one engine passing proves nothing about another" — so pin it on PG
/// too, via explicit `chunk_by_key` (PG does not auto-keyset). Same discriminator:
/// the running row TOTAL is 800 → 800 → 1200, never 2000.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn keyset_checkpoint_resume_pg_second_run_captures_only_new_keys() {
    require_alive(LiveService::Postgres);

    let table = unique_name("pg_keyset_ckpt");
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
        "CREATE TABLE {table} (uid TEXT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} (uid, payload)
         SELECT 'id-' || LPAD(g::text, 6, '0'), g FROM generate_series(1, 800) g;"
    ))
    .unwrap();

    let export = unique_name("pg_keyset_ckpt_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: postgres\n  url: \"{POSTGRES_URL}\"\nexports:\n  - name: {export}\n    \
         table: public.{table}\n    mode: chunked\n    chunk_by_key: uid\n    chunk_checkpoint: true\n    \
         keyset_incremental: true\n    \
         chunk_size: 300\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = |label: &str| {
        let out = run_rivet_export(&cfg, &export);
        assert!(
            out.status.success(),
            "{label} failed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
        // back-to-back sub-second runs must not collide — sleeping here would
        // mask exactly that regression (matrix audit: sleep-masked class).
    };

    run("run 1");
    assert_eq!(read_uid_set(out_dir.path()).0, 800, "run 1 exports all 800");

    run("run 2 (unchanged)");
    assert_eq!(
        read_uid_set(out_dir.path()).0,
        800,
        "unchanged resume adds zero rows — a re-read would double it"
    );

    c.batch_execute(&format!(
        "INSERT INTO {table} (uid, payload) \
         SELECT 'id-' || LPAD(g::text, 6, '0'), g FROM generate_series(801, 1200) g;"
    ))
    .unwrap();
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(out_dir.path());
    assert_eq!(
        count3, 1200,
        "resume adds ONLY the 400 new keys (got {count3}); 2000 would mean a full re-read"
    );
    let expected: BTreeSet<String> = (1..=1200).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        keys3, expected,
        "union of all runs equals the full source key set"
    );
    // Dest manifest copies (reconcile's artifact), not just the parquet re-read.
    assert_eq!(
        dir_manifest_copy_total_rows(out_dir.path()),
        1200,
        "run-unique manifest copies must sum run 1 (800) + run 3 (400); a clobbered manifest is silent to the parquet re-read"
    );
}

/// SQL Server twin of the keyset-resume two-run test. Per the "one engine passing
/// proves nothing about another" resume discipline, pin `chunk_by_key` +
/// `chunk_checkpoint` resume on MSSQL too. Same running-row-TOTAL discriminator
/// (1000 → 1000 → 1500, never 2500). Keys are 6-digit zero-padded so lexical
/// keyset order == numeric order.
#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn keyset_checkpoint_resume_mssql_second_run_captures_only_new_keys() {
    require_alive(LiveService::Mssql);

    let table = unique_name("ms_keyset_ckpt");
    struct MsDrop(String);
    impl Drop for MsDrop {
        fn drop(&mut self) {
            mssql_drop_table(&self.0);
        }
    }
    let _guard = MsDrop(format!("dbo.{table}"));

    mssql_exec(&format!(
        "CREATE TABLE dbo.{table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload INT NOT NULL)"
    ));
    let seed = |lo: i64, hi: i64| {
        mssql_exec(&format!(
            "INSERT INTO dbo.{table} (uid, payload) \
             SELECT RIGHT('000000' + CAST(value AS VARCHAR(10)), 6), value \
             FROM GENERATE_SERIES(CAST({lo} AS BIGINT), CAST({hi} AS BIGINT))"
        ));
    };
    seed(1, 1000);

    let export = unique_name("ms_keyset_ckpt_exp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        "source:\n  type: mssql\n  url: \"{MSSQL_URL}\"\n  tls:\n    accept_invalid_certs: true\n\
         exports:\n  - name: {export}\n    table: dbo.{table}\n    mode: chunked\n    \
         chunk_by_key: uid\n    chunk_checkpoint: true\n    keyset_incremental: true\n    \
         chunk_size: 300\n    format: parquet\n    \
         destination:\n      type: local\n      path: {out}\n",
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = |label: &str| {
        let out = run_rivet_export(&cfg, &export);
        assert!(
            out.status.success(),
            "{label} failed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
        // back-to-back sub-second runs must not collide — sleeping here would
        // mask exactly that regression (matrix audit: sleep-masked class).
    };

    run("run 1");
    assert_eq!(
        read_uid_set(out_dir.path()).0,
        1000,
        "run 1 exports all 1000"
    );

    run("run 2 (unchanged)");
    assert_eq!(
        read_uid_set(out_dir.path()).0,
        1000,
        "unchanged resume adds zero rows — a re-read would double it"
    );

    seed(1001, 1500);
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(out_dir.path());
    assert_eq!(
        count3, 1500,
        "resume adds ONLY the 500 new keys (got {count3}); 2500 would mean a full re-read"
    );
    let expected: BTreeSet<String> = (1..=1500).map(|n| format!("{n:06}")).collect();
    assert_eq!(
        keys3, expected,
        "union of all runs equals the full source key set"
    );
    // Dest manifest copies (reconcile's artifact), not just the parquet re-read.
    assert_eq!(
        dir_manifest_copy_total_rows(out_dir.path()),
        1500,
        "run-unique manifest copies must sum run 1 (1000) + run 3 (500); a clobbered manifest is silent to the parquet re-read"
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
