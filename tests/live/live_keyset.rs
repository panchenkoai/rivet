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

/// `(rows, distinct keys)` read by DuckDB — an INDEPENDENT codec.
///
/// `read_uid_set` below decodes with rivet's own arrow/parquet crate, so an
/// encode fault cancels itself: the writer and the reader agree because they
/// are the same code. Six completeness-claiming tests in this file rested on
/// it alone (the batch oracle gate named them), and each now cross-checks with
/// this. Manifest-DECLARED parts, so a crashed attempt's orphans cannot inflate
/// the count — which matters here, since half the callers crash on purpose.
fn duckdb_uid_counts(dir: &std::path::Path) -> (i64, i64) {
    (
        duckdb_declared_dir_scalar(dir, "count(*)"),
        duckdb_declared_dir_scalar(dir, "count(DISTINCT uid)"),
    )
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
    // chunk_size 1000 over 2000 rows, NOT 500: each part must span MORE THAN ONE
    // read batch (PROBE_BATCH_SIZE = 500). At one batch per part the write-side
    // fold is applied exactly once from zero, so `0 ^ s` and `0 + s` are equal and
    // the test cannot tell the two folds apart. Every Form B fixture here used
    // 500 and all of them stayed green through a fold change that broke the
    // write/read agreement for every export past 500 rows — measured: 500 rows
    // exit 0, 501 exit 3.
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
    let export = unique_name("keyset_formb_exp");
    // TEXT key + chunk_by_key → keyset, chunk_size 500 → 4 pages (cross-page XOR).
    let rig = Rig::pg_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 1000");
    let cfg = rig.config_path();
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    // (1) The manifest records Form B checksums (RED before the harvest — empty).
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(rig.out_dir().join("manifest.json")).expect("manifest.json"),
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
    let v = rig.cli(&["validate", "--export", &export]);
    assert!(
        v.status.success(),
        "rivet validate must PASS — the recorded Form B checksums must match the re-read parts; \
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
}

/// The NEGATIVE control Form B never had on the batch path.
///
/// Every batch Form B test until now proved the POSITIVE direction: the manifest
/// records checksums and a clean `validate` passes. That leaves the question the
/// mechanism exists to answer unasked — would a corrupted part be DETECTED? CDC
/// has its negative control (`live_cdc_mbt.rs` tampers the recorded sum); batch
/// had none, on any runner (audit 2026-08-17).
///
/// The tamper is on the DATA, not on the manifest, and deliberately so. Editing
/// the recorded checksum proves `validate` compares two numbers; corrupting the
/// PART proves it detects the thing an operator actually fears. It is also the
/// harder case to pass: the rewritten file must stay a VALID parquet with the
/// SAME row count, or `validate` fails for a second reason and the test would be
/// measuring the corruption's clumsiness rather than Form B — the exact
/// "fails-for-another-reason" shape this session found in three other places.
/// Both of those are asserted before the verdict is read.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_corrupted_part_fails_validate_on_the_value_checksum() {
    use arrow::array::{Int64Array, RecordBatch};
    use parquet::arrow::ArrowWriter;

    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(500);
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(table.name())
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 200")
        .dest_path(out.path().to_path_buf());
    let r = rig.run_args(&[]);
    assert!(
        r.status.success(),
        "the export must succeed before anything is corrupted; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    // The mechanism must be ARMED: no recorded checksums, nothing to detect
    // with, and the tamper below would prove nothing.
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(out.path().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    assert!(
        manifest["column_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "Form B must be recorded or this test grades nothing: {}",
        manifest["column_checksums"]
    );
    let clean = rig.cli(&["validate", "--export", table.name()]);
    assert!(
        clean.status.success(),
        "validate must PASS before the tamper, or the failure after it means nothing"
    );

    // Rewrite one part with a single `amount`… no: `id` is the keyed column, so
    // change a NON-key value column. One cell, same schema, same row count.
    let part = files_with_extension(out.path(), "parquet")
        .into_iter()
        .next()
        .expect("at least one part");
    let before = std::fs::read(&part).unwrap();
    let batches: Vec<RecordBatch> = {
        let f = std::fs::File::open(&part).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .map(|b| b.unwrap())
            .collect()
    };
    let rows_before: usize = batches.iter().map(|b| b.num_rows()).sum();
    let schema = batches[0].schema();
    let idx = schema
        .index_of("id")
        .expect("the fixture's keyed column is `id`");
    let tampered: Vec<RecordBatch> = batches
        .iter()
        .enumerate()
        .map(|(bi, b)| {
            if bi != 0 {
                return b.clone();
            }
            let col = b
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64");
            // +1 on ONE cell: the row count, the schema and every other column
            // are untouched, so only a VALUE oracle can see it.
            let mut v: Vec<i64> = col.iter().map(|x| x.unwrap_or(0)).collect();
            v[0] += 1;
            let mut cols = b.columns().to_vec();
            cols[idx] = std::sync::Arc::new(Int64Array::from(v));
            RecordBatch::try_new(b.schema(), cols).unwrap()
        })
        .collect();
    {
        let f = std::fs::File::create(&part).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        for b in &tampered {
            w.write(b).unwrap();
        }
        w.close().unwrap();
    }

    // Re-encoding changes the FILE SIZE, and validate checks size first: the
    // first run of this test failed on
    // `[RIVET_VERIFY_PART_SIZE_MISMATCH] manifest 4371, dest 9813` and never
    // reached the value leg — the "fails for another reason" trap, on my own
    // test. Patching the recorded size is not cheating, it is the POINT: Form B
    // exists for corruption the size and count checks cannot see, so isolating
    // it means neutralising the gates that would fire first. (Locally only size
    // applies — validate reports "0 md5, 1 size-only" for a local destination.)
    let part_name = part.file_name().unwrap().to_string_lossy().into_owned();
    let new_size = std::fs::metadata(&part).unwrap().len();
    let mpath = out.path().join("manifest.json");
    let mut m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&mpath).unwrap()).unwrap();
    let mut patched = false;
    for p in m["parts"].as_array_mut().expect("parts array") {
        if p["path"].as_str().is_some_and(|x| x.ends_with(&part_name)) {
            p["size_bytes"] = serde_json::json!(new_size);
            patched = true;
        }
    }
    assert!(patched, "the tampered part must be named in the manifest");
    let manifest_bytes = serde_json::to_string_pretty(&m).unwrap().into_bytes();
    std::fs::write(&mpath, &manifest_bytes).unwrap();
    // `_SUCCESS` carries a fingerprint OF THE MANIFEST BYTES, so editing the
    // manifest staled it and validate stopped there instead
    // (`[RIVET_VERIFY_SUCCESS_STALE]`). Re-stamped with the product's own
    // helper. That is three gates now standing between crude corruption and the
    // value leg — size, marker freshness, and (on a store that surfaces one)
    // md5. Each had to be satisfied to ask the question Form B answers, which is
    // worth knowing on its own: the value checksum is the LAST line, not the
    // first, and everything ahead of it works.
    std::fs::write(
        out.path().join("_SUCCESS"),
        rivet::manifest::success_marker_body(&manifest_bytes),
    )
    .unwrap();

    // NON-INERTNESS, both halves: the file changed, and it is still a readable
    // parquet with the same row count. Without these two, a `validate` failure
    // below could be a parse error or a count mismatch wearing Form B's clothes.
    assert_ne!(
        before,
        std::fs::read(&part).unwrap(),
        "the tamper must actually change the file"
    );
    let rows_after: usize = {
        let f = std::fs::File::open(&part).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum()
    };
    assert_eq!(
        rows_before, rows_after,
        "the corrupted part must still hold the SAME number of rows — otherwise \
         validate can fail on the count and Form B is never exercised"
    );

    let bad = rig.cli(&["validate", "--export", table.name()]);
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&bad.stdout),
        String::from_utf8_lossy(&bad.stderr)
    );
    assert!(
        !bad.status.success(),
        "a part whose VALUES were altered must fail validate — this is the whole \
         point of recording per-column checksums; got:\n{err}"
    );
    assert!(
        err.to_lowercase().contains("checksum") || err.contains("VALUE_CHECKSUM"),
        "and it must fail on the VALUE leg, not incidentally: {err}"
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
    let export = unique_name("keyset_forensics_exp");
    let rig = Rig::pg_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 1000");
    let cfg = rig.config_path();
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
    let export = unique_name("keyset_guard_exp");
    let rig = Rig::pg_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 50");
    let cfg = rig.config_path();
    // A prior CDC run's manifest already sits at the destination prefix. Written
    // AFTER config_path(), which is what materialises the destination dir.
    std::fs::write(
        rig.out_dir().join("manifest.json"),
        br#"{"manifest_version":1,"run_id":"prior-cdc","mode":"cdc","parts":[]}"#,
    )
    .unwrap();
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
    let export = unique_name("chunked_formb_exp");
    // Integer chunk_column → range chunking (the chunked runner), chunk_size 500.
    let rig = Rig::pg_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000");
    let cfg = rig.config_path();
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "chunked export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(rig.out_dir().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    assert!(
        manifest["column_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "chunked manifest must record Form B column_checksums, got: {}",
        manifest["column_checksums"]
    );

    let v = rig.cli(&["validate", "--export", &export]);
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
    let export = unique_name("ckpt_formb_exp");
    // chunk_checkpoint: true routes to the CHECKPOINT runner (not exec.rs).
    let rig = Rig::pg_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line("chunk_checkpoint: true");
    let cfg = rig.config_path();
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "chunked-checkpoint export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(rig.out_dir().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    assert!(
        manifest["column_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "chunked-CHECKPOINT manifest must record Form B column_checksums, got: {}",
        manifest["column_checksums"]
    );

    let v = rig.cli(&["validate", "--export", &export]);
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_size: 1000")
        .export_line("compression: zstd");
    let cfg = rig.config_path();
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Multiple page files (one per keyset page).
    let files = files_with_extension(&rig.out_dir(), "parquet");
    assert!(
        files.len() >= 2,
        "expected multiple keyset page files for {N} rows at chunk_size 500, got {}",
        files.len()
    );

    // The union of all pages must reproduce the source key set exactly — no row
    // dropped or duplicated at a `WHERE uid > last` boundary.
    let (count, keys) = read_uid_set(&rig.out_dir());
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000")
        .export_line("compression: zstd");
    let cfg = rig.config_path();
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "parallel keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // (1) SELF-ORACLE: the parallel runner is the only path that stamps `pk_w{id}`
    // into part names. Assert it ran — else the union check below is satisfied by a
    // silent sequential fall-back and proves nothing about parallelism.
    let parts = files_with_extension(&rig.out_dir(), "parquet");
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
    let (count, keys) = read_uid_set(&rig.out_dir());
    let expected: BTreeSet<String> = (1..=N).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        count, N,
        "row count must round-trip exactly across all workers (no boundary drop/dupe)"
    );
    assert_eq!(
        keys, expected,
        "the union of all parallel workers' keys must equal the source key set"
    );

    // The same completeness claim through an INDEPENDENT codec: `read_uid_set`
    // above decodes with rivet's own arrow/parquet crate, so an encode fault
    // cancels itself. DuckDB shares none of it, and reads the manifest-DECLARED
    // parts so a crashed attempt's orphans cannot inflate the count.
    let (dk_rows, dk_keys) = duckdb_uid_counts(&rig.out_dir());
    assert_eq!(
        dk_rows, dk_keys,
        "every key exactly once, by DuckDB: {dk_rows} rows over {dk_keys} distinct \
         keys — loss and duplication share a total when both happen"
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
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 4")
        .export_line("chunk_size: 1000");
    let cfg = rig.config_path();
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "PG uuid parallel keyset must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    // Fan-out proof: ≥2 distinct pk_w{id} workers (a collapse = only pk_w0).
    let workers: BTreeSet<String> = files_with_extension(&rig.out_dir(), "parquet")
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
    let (count, keys) = read_uuid_set_fixed(&rig.out_dir(), "id");
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 200");
    let cfg = rig.config_path();

    // Run 1: HARD-EXIT (process dies) right after range 0's atomic checkpoint commit
    // — range 0 is durably `done`, ranges 1-3 wrote parts to disk but never committed.
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:0")],
    );
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
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    assert_eq!(
        m["row_count"].as_i64(),
        Some(2000),
        "destination manifest must declare every row (done range not orphaned); got {}",
        m["row_count"]
    );

    // And the physical union is complete with no duplicate keys (the crashed
    // ranges' partial parts were overwritten by the re-run, not accumulated).
    let (count, keys) = read_uid_set(&rig.out_dir());
    let expected: BTreeSet<String> = (1..=2000).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        count, 2000,
        "parquet union must hold every row exactly once"
    );
    assert_eq!(
        keys, expected,
        "the union must equal the full source key set"
    );

    // The same completeness claim through an INDEPENDENT codec: `read_uid_set`
    // above decodes with rivet's own arrow/parquet crate, so an encode fault
    // cancels itself. DuckDB shares none of it, and reads the manifest-DECLARED
    // parts so a crashed attempt's orphans cannot inflate the count.
    let (dk_rows, dk_keys) = duckdb_uid_counts(&rig.out_dir());
    assert_eq!(
        dk_rows, dk_keys,
        "every key exactly once, by DuckDB: {dk_rows} rows over {dk_keys} distinct \
         keys — loss and duplication share a total when both happen"
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 200");
    let cfg = rig.config_path();
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
    let (count1, _) = read_uid_set(&rig.out_dir());
    assert_eq!(count1, 1000, "run 1 must export all seeded rows");

    // Run 2 on the UNCHANGED source: anchor floor = id-001000 → ZERO new rows; the
    // total across files stays 1000 (a re-read would double it to 2000).
    run("run 2 (unchanged)");
    let (count2, _) = read_uid_set(&rig.out_dir());
    assert_eq!(
        count2, 1000,
        "unchanged incremental re-run must add zero rows (got {count2}) — 2000 = a full re-read"
    );

    // Insert 500 rows with HIGHER keys, resume: only those 500 are read across the
    // N workers.
    seed(&mut conn, 1001, 1500);
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(&rig.out_dir());
    assert_eq!(
        count3, 1500,
        "incremental must add ONLY the 500 new keys (got {count3}); 2500 = a full re-read"
    );
    let expected: BTreeSet<String> = (1..=1500).map(|n| format!("id-{n:06}")).collect();
    assert_eq!(
        keys3, expected,
        "the union of all runs must equal the full source key set"
    );

    // The same completeness claim through an INDEPENDENT codec: `read_uid_set`
    // above decodes with rivet's own arrow/parquet crate, so an encode fault
    // cancels itself. DuckDB shares none of it, and reads the manifest-DECLARED
    // parts so a crashed attempt's orphans cannot inflate the count.
    let (dk_rows, dk_keys) = duckdb_uid_counts(&rig.out_dir());
    assert_eq!(
        dk_rows, dk_keys,
        "every key exactly once, by DuckDB: {dk_rows} rows over {dk_keys} distinct \
         keys — loss and duplication share a total when both happen"
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

    // Crash after page 0 commits (300 rows durable, cursor advanced, no manifest).
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_keyset_page:0")],
    );
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
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    assert_eq!(
        m["row_count"].as_i64(),
        Some(1000),
        "destination manifest must declare every row (pre-crash page not orphaned); got {}",
        m["row_count"]
    );

    // The same completeness claim through an INDEPENDENT codec: `read_uid_set`
    // above decodes with rivet's own arrow/parquet crate, so an encode fault
    // cancels itself. DuckDB shares none of it, and reads the manifest-DECLARED
    // parts so a crashed attempt's orphans cannot inflate the count.
    let (dk_rows, dk_keys) = duckdb_uid_counts(&rig.out_dir());
    assert_eq!(
        dk_rows, dk_keys,
        "every key exactly once, by DuckDB: {dk_rows} rows over {dk_keys} distinct \
         keys — loss and duplication share a total when both happen"
    );
}

/// Convergence round-1 finding #3 (MEASURED): a sequential-keyset `chunk_checkpoint`
/// resume DUPLICATES a page when the crash lands in the `after_manifest_update` window —
/// the file_log row is written but the cursor is NOT yet advanced (commit.rs:352). On resume
/// the pre-crash page's part is rehydrated from file_log (a keyset name has no chunk-nonce, so
/// the superseded-attempt filter is skipped and it is kept), AND the loop re-reads from the
/// un-advanced cursor and writes the SAME rows under a NEW per-invocation stamp — so the
/// manifest declares that page TWICE. The existing crash test above uses `after_keyset_page:0`
/// (AFTER the advance), so it never exercised this window. This crashes at
/// `after_manifest_update` and asserts the destination manifest declares EXACTLY the source
/// row count — it currently declares MORE (the measured duplication). RED against the bug.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_checkpoint_crash_in_manifest_window_must_not_duplicate_a_page() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_dup");
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

    let export = unique_name("keyset_dup_exp");
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

    // Crash in the I3 window: page 0's file_log row is written, cursor NOT advanced.
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_manifest_update")],
    );
    assert!(!crash.status.success(), "crash run must exit non-zero");

    // Resume (NO panic env) — the cursor was never advanced, so it re-reads page 0.
    let resume = run_rivet_export(&cfg, &export);
    assert!(
        resume.status.success(),
        "resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    let declared = m["row_count"].as_i64().unwrap_or(-1);
    assert_eq!(
        declared,
        1000,
        "the destination manifest must declare EXACTLY the 1000 source rows — it declared \
         {declared} ({} duplicated), the after_manifest_update rehydrate+re-read dup",
        declared - 1000
    );
}

/// v25 cursor-atomic checkpoint under a CHANGED config on resume. The convergence-round-3
/// review posited a multi-part-ROTATION dup (a keyset page re-read into a DIFFERENT number of
/// parts). HONEST SCOPE: that is not readily reachable for keyset — `max_file_size` rotates on
/// FLUSHED parquet bytes, and a sub-row-group page (chunk_size ≪ ~1M) never flushes, so it stays
/// a SINGLE part (documented in audit_maxfile as a no-op). So this test exercises the reachable
/// proxy: a resume whose config CHANGED between the crash and the resume (`max_file_size` 8KB →
/// 128KB), which the seek_tag+dedup band-aid alone could mis-handle if the part shape ever did
/// change. The v25 reconcile makes it moot — resume reconciles `last` from the committed part's
/// cursor_high and NEVER re-reads the committed page, whatever the config. Manifest must declare
/// EXACTLY the source rows.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_checkpoint_resume_survives_a_changed_max_file_size_config() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_mp");
    let _guard = DropTable(table.clone());
    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    // HIGH-ENTROPY payload (concatenated SHA2 digests) so a 300-row page does NOT compress away —
    // it must exceed a small max_file_size and genuinely rotate into MULTIPLE parts. A repetitive
    // payload compresses to a single part and would test nothing (fixture below the rotation
    // threshold). The value is generated ONCE at INSERT and stored, so re-reads are identical.
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload VARCHAR(600) NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 1000) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), \
                CONCAT(SHA2(n, 512), SHA2(CONCAT(n,'a'), 512), \
                       SHA2(CONCAT(n,'b'), 512), SHA2(CONCAT(n,'c'), 512)) \
         FROM seq"
    ))
    .unwrap();

    let export = unique_name("keyset_mp_exp");
    let mut rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 300")
        .export_line("max_file_size: 8KB"); // forces each 300-row page to rotate into several parts
    let cfg = rig.config_path();

    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "after_manifest_update")],
    );
    assert!(!crash.status.success(), "crash run must exit non-zero");

    // RESUME with a DIFFERENT max_file_size so the re-read would rotate the same page into a
    // DIFFERENT number of parts — the exact multi-part-ROTATION the seek_tag+dedup band-aid cannot
    // dedup (the re-read's part paths no longer match the rehydrated ones). Only the v25
    // cursor-atomic reconcile survives it, because it never re-reads the committed page at all.
    // Through the rig's sanctioned mutation path — a raw fs::write patch would
    // now be refused by config_path's hand-edit guard (and was only ever safe
    // here because the resume ran through a raw helper, not the rig).
    rig.replace_export_line("max_file_size:", "max_file_size: 128KB");

    let resume = run_rivet_export(&cfg, &export);
    assert!(
        resume.status.success(),
        "resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );

    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(rig.out_dir().join("manifest.json")).unwrap())
            .expect("destination manifest.json must exist + parse");
    let declared = m["row_count"].as_i64().unwrap_or(-1);
    assert_eq!(
        declared,
        1000,
        "a crash-resume whose config changed (max_file_size 8KB → 128KB) must declare EXACTLY the \
         1000 source rows — it declared {declared} ({} duplicated); the v25 reconcile skips the \
         committed page regardless of the resume config",
        declared - 1000
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

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
    let (count1, _) = read_uid_set(&rig.out_dir());
    assert_eq!(count1, 1000, "run 1 must export all seeded rows");

    // Run 2 on the UNCHANGED source: resume floor is id-001000 → ZERO new rows,
    // no file written; the total across files stays 1000 (no re-read).
    run("run 2 (unchanged)");
    let (count2, _) = read_uid_set(&rig.out_dir());
    assert_eq!(
        count2, 1000,
        "unchanged resume must add zero rows (got {count2}) — a re-read would double it"
    );

    // Insert 500 rows with HIGHER keys, then resume: only those 500 are read.
    seed(&mut conn, 1001, 1500);
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(&rig.out_dir());
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
        dir_manifest_copy_total_rows(&rig.out_dir()),
        1500,
        "run-unique manifest copies must sum run 1 (1000) + run 3 (500); a clobbered manifest is silent to the parquet re-read"
    );

    // The same completeness claim through an INDEPENDENT codec: `read_uid_set`
    // above decodes with rivet's own arrow/parquet crate, so an encode fault
    // cancels itself. DuckDB shares none of it, and reads the manifest-DECLARED
    // parts so a crashed attempt's orphans cannot inflate the count.
    let (dk_rows, dk_keys) = duckdb_uid_counts(&rig.out_dir());
    assert_eq!(
        dk_rows, dk_keys,
        "every key exactly once, by DuckDB: {dk_rows} rows over {dk_keys} distinct \
         keys — loss and duplication share a total when both happen"
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();
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
    let (count1, _) = read_uid_set(&rig.out_dir());
    assert_eq!(count1, 1000, "run 1 must export all seeded rows");

    // Run 2 on the UNCHANGED source: a CLEAN re-run without keyset_incremental
    // must re-read all 1000 again (full pass) — the parquet re-read total doubles
    // to 2000. A total of 1000 would mean checkpoint silently implied incremental
    // (the pre-split staleness bug this test guards).
    run("run 2 (clean re-run)");
    let (count2, keys2) = read_uid_set(&rig.out_dir());
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

    // Run 1: full export of all 1000; on data-completion resume_run_id is cleared
    // and last_cursor_value = the final key id-001000.
    let r1 = run_rivet_export(&cfg, &export);
    assert!(
        r1.status.success(),
        "run 1 stderr:\n{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let (count1, _) = read_uid_set(&rig.out_dir());
    assert_eq!(count1, 1000, "run 1 must export all 1000");

    // Run 2: a FRESH run that crashes right after open (no page committed).
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "keyset_after_open_before_first_page")],
    );
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
    let (count3, keys3) = read_uid_set(&rig.out_dir());
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

    // Run 1: commits ALL 1000, then fails AFTER data-completion (a stand-in for a
    // post-data gate rejection). Data-completion has already cleared resume_run_id.
    let fail = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "keyset_after_data_complete")],
    );
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
    let (count2, _) = read_uid_set(&rig.out_dir());
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

    // Run 1: commits all 1000 pages under R1, then crashes AFTER data-complete but
    // BEFORE finalize_manifest — so NO destination manifest for R1 is written.
    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "keyset_after_data_complete")],
    );
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
        dir_manifest_copy_total_rows(&rig.out_dir()),
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
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .export_named(&export)
        .mode("full")
        .export_line("compression: zstd");
    let cfg = rig.config_path();
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "PG UUID snapshot export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Single file expected — mode: full produces one part.
    let files = files_with_extension(&rig.out_dir(), "parquet");
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
    let (count, keys) = read_uuid_set_fixed(&rig.out_dir(), "id");
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
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_size: 1000")
        .export_line("compression: zstd");
    let cfg = rig.config_path();
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "MySQL UUID keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(&rig.out_dir(), "parquet");
    assert!(
        files.len() >= 2,
        "expected multiple keyset page files for {N} rows at chunk_size 500, got {}",
        files.len()
    );

    let (count, keys) = read_uid_set_named(&rig.out_dir(), "id");
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
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

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
    assert_eq!(read_uid_set(&rig.out_dir()).0, 800, "run 1 exports all 800");

    run("run 2 (unchanged)");
    assert_eq!(
        read_uid_set(&rig.out_dir()).0,
        800,
        "unchanged resume adds zero rows — a re-read would double it"
    );

    c.batch_execute(&format!(
        "INSERT INTO {table} (uid, payload) \
         SELECT 'id-' || LPAD(g::text, 6, '0'), g FROM generate_series(801, 1200) g;"
    ))
    .unwrap();
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(&rig.out_dir());
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
        dir_manifest_copy_total_rows(&rig.out_dir()),
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
    let rig = Rig::mssql_batch(&format!("dbo.{table}"))
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 300");
    let cfg = rig.config_path();

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
        read_uid_set(&rig.out_dir()).0,
        1000,
        "run 1 exports all 1000"
    );

    run("run 2 (unchanged)");
    assert_eq!(
        read_uid_set(&rig.out_dir()).0,
        1000,
        "unchanged resume adds zero rows — a re-read would double it"
    );

    seed(1001, 1500);
    run("run 3 (after insert)");
    let (count3, keys3) = read_uid_set(&rig.out_dir());
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
        dir_manifest_copy_total_rows(&rig.out_dir()),
        1500,
        "run-unique manifest copies must sum run 1 (1000) + run 3 (500); a clobbered manifest is silent to the parquet re-read"
    );

    // The same completeness claim through an INDEPENDENT codec: `read_uid_set`
    // above decodes with rivet's own arrow/parquet crate, so an encode fault
    // cancels itself. DuckDB shares none of it, and reads the manifest-DECLARED
    // parts so a crashed attempt's orphans cannot inflate the count.
    let (dk_rows, dk_keys) = duckdb_uid_counts(&rig.out_dir());
    assert_eq!(
        dk_rows, dk_keys,
        "every key exactly once, by DuckDB: {dk_rows} rows over {dk_keys} distinct \
         keys — loss and duplication share a total when both happen"
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
    // `chunk_by_key: id` opts into keyset paging on a UUID column despite
    // PG's planner default of refusing auto-resolution for non-int PKs.
    // chunk_size 500 → 6 pages, exercising every page boundary.
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 1000")
        .export_line("compression: zstd");
    let cfg = rig.config_path();
    let out = run_rivet_export(&cfg, &export);
    assert!(
        out.status.success(),
        "PG UUID keyset (explicit chunk_by_key) must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ≥2 page files prove pagination ran.
    let files = files_with_extension(&rig.out_dir(), "parquet");
    assert!(
        files.len() >= 2,
        "expected multiple keyset page files for {N} rows at chunk_size 500, got {}",
        files.len()
    );

    // Exact UUID set round-trip across the pages — the page boundary
    // value (`E'<uuid>'` literal cast on the server) survives the
    // FixedSizeBinary(16) → UUID-string → next-page WHERE clause cycle.
    let (count, keys) = read_uuid_set_fixed(&rig.out_dir(), "id");
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

/// ARRAY columns must reach the value checksum — measured gap, not a hypothesis.
///
/// Stubbing `CellSource::list` to `None` passed the whole `--lib` cycle AND the
/// whole live suite, including every existing Form-B test. The cause was
/// upstream of all of them: `seeds/common/postgres.sql` declared no array column
/// at all, so no export ever carried a LIST cell through the checksum path, and
/// such a column contributed NOTHING to the source-side sum with nobody to
/// notice.
///
/// The same missing fixture is why `_rivet_row_hash` shipped collapsing
/// `['a, b']` with `['a','b']`, and `[NULL]` with `[""]` and `[]` (fixed
/// 2026-08-01; its injectivity now has unit coverage of its own). Arrays landed
/// 2026-05-12 and reached neither mechanism — one gap, two blind oracles.
///
/// The seed's `array_matrix` carries one column per element type
/// `list_elem_covered` claims to check, with distinct per-position values so an
/// ordering or index bug cannot hide behind equal elements, inner NULLs, and an
/// EMPTY array beside a NULL array.
#[test]
#[ignore = "live: requires docker compose up -d postgres with the golden seed"]
fn array_columns_reach_the_value_checksum() {
    require_alive(LiveService::Postgres);
    // The FIXTURE precondition, checked first and named in full when missing.
    //
    // This test's `#[ignore]` says it needs "docker compose up -d postgres with
    // the golden seed" — and that was UNREACHABLE by the documented command
    // until 2026-08-05: the root compose mounts `dev/postgres/init.sql`, not
    // `seeds/common/postgres.sql`, and the former carried no array column. The
    // table reached a stand only if somebody applied the golden SQL by hand, so
    // on a clean checkout this failed with an export error about a missing
    // relation — indistinguishable, to a reader, from a product regression.
    //
    // Init scripts run ONLY on an empty data directory, so pulling the fix does
    // not repair an EXISTING stand. Hence the command, spelled out.
    if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
        let present: bool = c
            .query_one("SELECT to_regclass('public.array_matrix') IS NOT NULL", &[])
            .map(|r| r.get(0))
            .unwrap_or(false);
        assert!(
            present,
            "fixture `array_matrix` is absent — a STAND problem, not a rivet one.\n\
             It lives in dev/postgres/init.sql, which docker runs only on a FRESH data \
             directory. Apply it to a running stand with:\n  \
             docker exec -i rivet-postgres-1 psql -U rivet -d rivet < dev/postgres/init.sql"
        );
    }
    let export = unique_name("array_matrix_exp");
    let rig = Rig::pg_batch("array_matrix")
        .export_named(&export)
        .mode("full");
    let cfg = rig.config_path();
    let r = run_rivet_export(&cfg, &export);
    assert!(
        r.status.success(),
        "array_matrix export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(rig.out_dir().join("manifest.json")).expect("manifest.json"),
    )
    .expect("parse manifest");
    let checksums = manifest["column_checksums"]
        .as_array()
        .expect("column_checksums must be recorded");
    let named: Vec<&str> = checksums
        .iter()
        .filter_map(|c| c["name"].as_str())
        .collect();
    for col in ["bools", "i16s", "i32s", "i64s", "f32s", "f64s", "texts"] {
        assert!(
            named.contains(&col),
            "array column `{col}` must be COVERED by the value checksum — a LIST cell \
             that contributes nothing is a silently weakened integrity oracle; got {named:?}"
        );
    }

    // And the two sides must agree on them: `validate` re-reads the parts and
    // recomputes side B against the recorded side A.
    let v = rig.cli(&["validate", "--export", &export]);
    assert!(
        v.status.success(),
        "validate must re-verify the ARRAY columns' checksums; stderr:\n{}",
        String::from_utf8_lossy(&v.stderr)
    );
}

/// `rivet validate --depth full` — the command the reconcile report TELLS the
/// operator to run, never once run by the suite.
///
/// Its only reference anywhere in the tree is inside an assertion STRING
/// (`live_mysql_reconcile_repair.rs`: the report must point at "rivet validate
/// --depth full"). That report says, correctly, that reconcile compares the
/// source to a number rivet RECORDED and that this is the check which re-reads
/// the files — so the remediation the product hands out led to a code path no
/// test executed. A remediation hint has to work from the state it is offered in.
///
/// `Full` is `Sample` plus the Form B value-checksum re-read, which DOWNLOADS the
/// parts. Both halves are asserted: it passes on a clean export, and it FAILS on
/// a part whose bytes were altered while every lighter level still passes — the
/// difference between the depths is the whole point of naming one in a hint.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn validate_depth_full_rereads_the_parts_the_lighter_depths_never_open() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(300);
    let rig = Rig::pg_batch(table.name())
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 100");
    rig.run_ok();

    // Clean: every depth passes.
    for depth in ["light", "sample", "full"] {
        let out = rig.cli(&["validate", "--depth", depth]);
        assert!(
            out.status.success(),
            "validate --depth {depth} must pass on a clean export; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Corrupt a part's BYTES without touching its size: a value-level fault, which
    // is exactly what the lighter depths cannot see (they check the manifest, the
    // `_SUCCESS` marker, part presence and size — none of which move).
    let parts = files_with_extension(&rig.out_dir(), "parquet");
    assert!(!parts.is_empty(), "the export must have written a part");
    let victim = &parts[0];
    let before = std::fs::metadata(victim).expect("stat part").len();
    let mut bytes = std::fs::read(victim).expect("read part");
    // Flip bits in the middle of the data region, away from the header/footer, so
    // the file stays a readable parquet whose VALUES changed.
    let mid = bytes.len() / 2;
    for b in bytes.iter_mut().skip(mid).take(64) {
        *b ^= 0xFF;
    }
    std::fs::write(victim, &bytes).expect("write corrupted part");
    assert_eq!(
        std::fs::metadata(victim).expect("re-stat part").len(),
        before,
        "the corruption must not change the SIZE — otherwise the lighter depths catch it \
         on size alone and this test proves nothing about depth"
    );

    // The DEPTH distinction, measured rather than assumed: whatever the lighter
    // levels do on this exact corruption is recorded below, because "full fails"
    // alone would be satisfied by a failure for any reason at all.
    let light = rig.cli(&["validate", "--depth", "light"]);
    let sample = rig.cli(&["validate", "--depth", "sample"]);
    // Measured 2026-08-18: light=true sample=true, full=false. `sample` adds a
    // prefix listing (presence, size, surplus) and a byte flip moves none of
    // those, which is what makes `--depth full` genuinely load-bearing rather than
    // a slower spelling of the same check. Its verdict is REPORTED, not asserted:
    // pinning "sample passes" would forbid ever strengthening it, and this test's
    // subject is the depth the report recommends, not the ceiling of the one below.
    eprintln!(
        "depth verdicts on a byte-corrupted part: light={} sample={}",
        light.status.success(),
        sample.status.success()
    );
    assert!(
        light.status.success(),
        "`light` reads the manifest, `_SUCCESS` and self-consistency — none of which a \
         byte flip moves. If it fails, the fixture broke something else and the depth \
         comparison below is meaningless; stderr:\n{}",
        String::from_utf8_lossy(&light.stderr)
    );

    let full = rig.cli(&["validate", "--depth", "full"]);
    assert!(
        !full.status.success(),
        "validate --depth full must FAIL on a part whose bytes changed — it is the depth \
         that re-reads them, and it is what `rivet reconcile` tells the operator to run \
         when its own verdict is only rivet's recorded count; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&full.stdout),
        String::from_utf8_lossy(&full.stderr)
    );
}

/// PART-NAME NON-DOUBLING (field-run finding). The keyset part-name format
/// prepends the export name, and the run_id it used for run-uniqueness is
/// itself `<export>_<stamp>` — so real runs wrote `<export>_<export>_<stamp>_
/// pk_w…`, the export name TWICE (the chunked/mongo siblings key off a fresh
/// stamp and never doubled). Through the rig: a PARALLEL keyset export (the
/// pk_w path, keyset.rs:605) whose part basenames must carry the export name
/// exactly ONCE. RED before run_scoped_tag stripped the redundant prefix.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn keyset_part_names_do_not_double_the_export_name() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(2000);
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::pg_batch(table.name())
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 200")
        .export_line("parallel: 4")
        .dest_path(out.path().to_path_buf());
    assert!(
        rig.run_args(&[]).status.success(),
        "keyset export must succeed before its part names can be checked"
    );

    let parts = files_with_extension(out.path(), "parquet");
    assert!(!parts.is_empty(), "keyset run produced no parts to name");

    let doubled = format!("{}_{}", table.name(), table.name());
    for p in &parts {
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        // The part is a parallel-keyset part (proves we exercised the pk_w path).
        assert!(
            name.contains("_pk_w"),
            "expected parallel-keyset (pk_w) parts; got {name}"
        );
        assert!(
            !name.contains(&doubled),
            "part name doubles the export name — '{doubled}' appears in '{name}' \
             (the run_id-carries-the-export bug; run_scoped_tag must strip it)"
        );
        // ...and the export name is still present exactly once (dir + name both
        // carry it, so a reader can attribute a stray file to its export).
        assert!(
            name.starts_with(&format!("{}_", table.name())),
            "part name must still lead with the export name once: {name}"
        );
    }
}

/// Round-5 lifecycle HIGH, keyset flavor: a committed page part deleted from
/// the destination BETWEEN attempts (a gc pass after `state finish-run`, a
/// foreign-host gc) must make the resume REFUSE — keyset has no per-part
/// re-export (the cursor has moved past the page), so the pre-fix behavior
/// re-declared the deleted file sight-unseen: Success + `_SUCCESS` naming
/// parquet that does not exist, the page's rows silently absent. The refusal
/// names the remedy (`state reset-chunks` / fresh prefix). RED against
/// disabling the destination probe in `rehydrate_keyset_pages_probed`.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn keyset_resume_refuses_when_a_committed_page_was_deleted_between_attempts() {
    require_alive(LiveService::Mysql);
    let table = unique_name("keyset_gone_page");
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

    let export = unique_name("keyset_gone_exp");
    let rig = Rig::mysql_batch(&table)
        .export_named(&export)
        .mode("chunked")
        .export_line("chunk_by_key: uid")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 200");
    let cfg = rig.config_path();

    let crash = rig.run_args_env(
        &["--export", &export],
        &[("RIVET_TEST_PANIC_AT", "keyset_parallel_range_committed:0")],
    );
    assert!(!crash.status.success());
    assert!(
        String::from_utf8_lossy(&crash.stderr).contains("keyset_parallel_range_committed"),
        "the crash must be OUR injected panic; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );

    // The between-attempts gc: every durable parquet vanishes — including the
    // done range's committed pages, the ones the resume would re-declare.
    let out = rig.out_dir();
    let mut deleted = 0;
    for e in std::fs::read_dir(&out).unwrap().flatten() {
        let path = e.path();
        if path.extension().is_some_and(|x| x == "parquet") {
            std::fs::remove_file(&path).unwrap();
            deleted += 1;
        }
    }
    assert!(
        deleted > 0,
        "fixture: the crash must have left durable parquet"
    );

    let resume = run_rivet_export(&cfg, &export);
    assert!(
        !resume.status.success(),
        "resume must REFUSE to declare deleted pages, not finalize Success over a hole"
    );
    let err = String::from_utf8_lossy(&resume.stderr);
    assert!(
        err.contains("GONE from the destination"),
        "the refusal must name the cause; stderr:\n{err}"
    );
    assert!(
        err.contains("state reset") && !err.contains("reset-chunks"),
        "the refusal must name the WORKING remedy — round-6 live-proved \
         reset-chunks clears tables keyset never writes, stranding the operator \
         in a refusal loop; stderr:\n{err}"
    );
    assert!(
        !out.join("_SUCCESS").is_file(),
        "no completion marker may exist after the refusal"
    );

    // THE REMEDY MUST WORK from the refused state (a hint is a testable claim):
    // follow it verbatim, then the next run must do a fresh full pass.
    let reset = std::process::Command::new(env!("CARGO_BIN_EXE_rivet"))
        .args([
            "state",
            "reset",
            "-c",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .unwrap();
    assert!(
        reset.status.success(),
        "the named remedy must run clean; stderr:\n{}",
        String::from_utf8_lossy(&reset.stderr)
    );
    let rerun = run_rivet_export(&cfg, &export);
    assert!(
        rerun.status.success(),
        "after the remedy the export must complete; stderr:\n{}",
        String::from_utf8_lossy(&rerun.stderr)
    );
    // INDEPENDENT delivery proof (harness audit): manifest.json row_count is
    // rivet's own counter — a run that miscounts agrees with itself. DuckDB
    // reads the parquet with a codec rivet does not share — over the
    // MANIFEST-DECLARED parts, not a glob: the REFUSED resume exported its
    // missing ranges before refusing at finalize, and those unmanifested
    // orphans (measured: 499 rows) are the gc_orphans case, not delivered
    // data. A glob read 2499 here and graded the fixture, not the remedy.
    assert_eq!(
        duckdb_declared_dir_scalar(&out, "count(*)"),
        2000,
        "the remedied run must deliver the whole table — graded by DuckDB over \
         the declared parts"
    );
    assert_eq!(
        duckdb_declared_dir_scalar(&out, "count(DISTINCT uid)"),
        2000,
        "every key exactly once — loss and duplication are different failures \
         and a bare count cannot tell them apart"
    );
}
