//! The SIX-WAY census for the BATCH side — the instrument the batch suites did
//! not have.
//!
//! Measured 2026-08-29 over 594 live capture tests: the CDC side graded 35% of
//! its claims with an independent oracle, the batch side 23%, and the gap was
//! not discipline — `Rig::row_census` REFUSED every batch rig outright (its
//! reader map was CDC-instance-only), so no batch test could ask the question
//! at all. With the main-stand readers wired, one DuckDB session that shares no
//! code with rivet now compares six numbers:
//!
//!   source · delivered parquet · export_metrics · file_log · MANIFEST · DISTINCT
//!
//! The last two are new and each closes a shape the four-way census could not
//! see. The MANIFEST is what a consumer actually reads — a run can deliver the
//! right parquet and declare a different total, and nothing compared those.
//! DISTINCT tells LOSS from DUPLICATION: the same total can hide both at once,
//! which is exactly how a keyset retry once shipped 751 rows / 750 distinct.

use crate::common::*;

/// A clean full export: every leg agrees, and the fixture is not inert.
#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn batch_full_export_census_agrees_on_all_six_legs_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("bcensus");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
         INSERT INTO {table} SELECT g, 'v' || g FROM generate_series(1, 500) g;"
    ))
    .unwrap();

    let rig = Rig::pg_batch(&format!("public.{table}"))
        .census_oracle()
        .census_key("id");
    rig.run_ok();

    let census = rig.row_census();
    assert_eq!(
        census.source, 500,
        "fixture is not inert: the source must really hold 500 rows — a census \
         over an empty table agrees with everything: {census:?}"
    );
    assert!(
        census.agrees(),
        "every leg must agree — source, delivered parquet, both ledgers, the \
         MANIFEST a consumer reads, and DISTINCT on each side: {census:?}"
    );
    assert_eq!(
        census.manifest, 500,
        "the manifest declares what a consumer will read, and nothing compared \
         it to the parquet before this census: {census:?}"
    );
    assert_eq!(
        census.delivered_distinct,
        Some(500),
        "no duplication: a matching total with fewer distinct keys is the retry \
         shape this leg exists for: {census:?}"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// The DISTINCT leg BITES: a destination carrying a duplicated key must fail
/// `agrees()` even though every COUNT still matches.
///
/// Built by hand rather than by inducing a retry: the point is to prove the
/// instrument reacts, and a fixture that needs a real race to reproduce would
/// grade the race instead. The duplicate is appended as a second part, which is
/// exactly what an un-overwritten retry attempt leaves behind.
#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn batch_census_distinct_leg_catches_a_duplicated_key_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("bcensus_dup");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
         INSERT INTO {table} SELECT g, 'v' || g FROM generate_series(1, 100) g;"
    ))
    .unwrap();

    let rig = Rig::pg_batch(&format!("public.{table}"))
        .census_oracle()
        .census_key("id");
    rig.run_ok();
    let clean = rig.row_census();
    assert!(
        clean.agrees(),
        "precondition: the clean run agrees: {clean:?}"
    );

    // The fixture must hold every COUNT equal and move only DISTINCT — otherwise
    // the assertion below passes for a second reason and grades nothing. A first
    // cut simply copied a part beside itself: `delivered` then rose to 200 and
    // even the OLD four-leg `agrees()` failed on source != delivered, so the
    // mutant that removes the DISTINCT leg still passed (measured — the test was
    // vacuous for the very leg it names).
    //
    // So: drop one key and duplicate another, in place. 100 rows before, 100
    // after, 99 distinct — the silent shape, where a row was lost AND a row
    // duplicated and every total still agrees.
    let parts = files_with_extension(&rig.out_dir(), "parquet");
    let first = parts.first().expect("the run wrote a part");
    let name = first.file_name().unwrap().to_string_lossy().into_owned();
    let container = rig.oracle_container_out();
    let _ = duckdb_run_sql_json(&format!(
        "COPY (SELECT * FROM read_parquet('{container}/{name}') WHERE id <> 100 \
               UNION ALL \
               SELECT * FROM read_parquet('{container}/{name}') WHERE id = 1) \
         TO '{container}/{name}.rewritten' (FORMAT parquet)"
    ));
    std::fs::rename(first.with_extension("parquet.rewritten"), first)
        .expect("swap in the rewritten part");

    let dup = rig.row_census();
    assert_eq!(
        (
            dup.source,
            dup.delivered,
            dup.metrics,
            dup.file_log,
            dup.manifest
        ),
        (100, 100, 100, 100, 100),
        "the fixture must leave every COUNT equal — otherwise this test grades \
         the count legs, not the DISTINCT one: {dup:?}"
    );
    assert!(
        dup.delivered > dup.delivered_distinct.unwrap_or(0),
        "the destination now holds a duplicated key, and the census must SEE it: {dup:?}"
    );
    assert!(
        !dup.agrees(),
        "a census that still 'agrees' over duplicated rows is the four-way \
         census's blind spot, which is why the DISTINCT leg exists: {dup:?}"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// The MANIFEST leg BITES: a destination whose manifest declares a total the
/// parquet does not hold must fail `agrees()`, even though every other leg
/// still matches.
///
/// This is what a CONSUMER reads. `rivet load` and every cross-boundary reader
/// take the manifest's word for what was delivered; the four-way census never
/// asked it anything, so a run could ship correct parquet and declare a wrong
/// total with nothing to notice. The fixture edits the declared `row_count`
/// only — the parts, the ledgers and the source are untouched, so a green
/// result here means the leg is absent, not that the world is sound.
#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn batch_census_manifest_leg_catches_a_declared_total_the_parquet_denies_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("bcensus_man");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
         INSERT INTO {table} SELECT g, 'v' || g FROM generate_series(1, 100) g;"
    ))
    .unwrap();

    let rig = Rig::pg_batch(&format!("public.{table}"))
        .census_oracle()
        .census_key("id");
    rig.run_ok();
    let clean = rig.row_census();
    assert!(
        clean.agrees(),
        "precondition: the clean run agrees: {clean:?}"
    );
    assert_eq!(
        clean.manifest, 100,
        "precondition: the manifest declares 100"
    );

    // Rewrite ONLY the declared total, through the one resolver that decides
    // which manifests count.
    let mans = declared_manifests(&rig.out_dir());
    let target = mans.first().expect("the run wrote a manifest");
    let text = std::fs::read_to_string(target).unwrap();
    let mut doc: serde_json::Value = serde_json::from_str(&text).unwrap();
    doc["row_count"] = serde_json::json!(99);
    std::fs::write(target, serde_json::to_string(&doc).unwrap()).unwrap();

    let lied = rig.row_census();
    assert_eq!(
        (lied.source, lied.delivered, lied.metrics, lied.file_log),
        (100, 100, 100, 100),
        "the fixture must move ONLY the manifest — otherwise this grades another \
         leg: {lied:?}"
    );
    assert_eq!(
        lied.manifest, 99,
        "the census must read the declared total: {lied:?}"
    );
    assert!(
        !lied.agrees(),
        "a manifest that declares a total the parquet denies is what a consumer \
         will act on, and the census must refuse it: {lied:?}"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// THE THIRD COMPARISON POINT: what the BUCKET holds, against what the ledger
/// recorded and the manifest declared.
///
/// Every census leg before this one reads the host — the source, the local
/// parquet, the state DB. A cloud export's artifacts live somewhere no test
/// asked about: `file_log` said N, the manifest declared N, and whether the
/// bucket held N was simply never a question. The store readers differ per
/// emulator (minio and azurite glob natively; fake-gcs is read-only to DuckDB
/// because its JSON API 404s the HEAD httpfs issues — measured), so the leg
/// names its store rather than pretending one path fits all.
#[test]
#[ignore = "live: requires docker compose up -d postgres minio duckdb"]
fn batch_export_to_minio_agrees_across_store_manifest_and_ledger_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("bstore");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
         INSERT INTO {table} SELECT g, 'v' || g FROM generate_series(1, 300) g;"
    ))
    .unwrap();

    let bucket = "rivet-census";
    ensure_minio_bucket(bucket);
    let prefix = unique_name("store_census");
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .census_oracle()
        .dest_s3(bucket, &prefix, MINIO_ENDPOINT);
    // The credentials the rendered config names — the rig writes
    // `access_key_env: RIVET_TEST_MINIO_AK`, so the run needs them on its env.
    let out = rig.run_with_envs(&[
        ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
        ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
    ]);
    assert!(
        out.status.success(),
        "the export must succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The BUCKET, read by DuckDB from inside the stand network.
    let store = duckdb_store_census(ObjectStore::Minio, bucket, &prefix, &[]);
    assert_eq!(
        store.rows, 300,
        "the bucket must hold every source row — this is what a cross-boundary \
         reader will find, and no test asked it before: {store:?}"
    );
    assert!(store.files > 0, "fixture is not inert: {store:?}");

    // …against what rivet RECORDED and DECLARED. The state DB sits beside the
    // config in the shared workdir even for a cloud destination.
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.total_rows,
        Some(store.rows),
        "the ledger's total and the bucket's contents must be the same number: \
         ledger {:?} vs store {}",
        m.total_rows,
        store.rows
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// The FOURTH comparison point: the row hash, checked against an INDEPENDENT
/// reading of its spec rather than against rivet recomputing its own.
///
/// `rivet validate` re-derives `_rivet_row_hash` with the product's own
/// `enrich`, which answers "did the data change" and cannot answer "is the
/// canonical form injective" — both sides share the definition. Render id v1
/// shipped non-injective twice and lived four months inside exactly that
/// arrangement.
///
/// The fixture is hostile on purpose: values that carry the v1 separator
/// (`\x1f`), empty strings and NULLs, arranged
/// so a boundary-forging pair sits in the data — `('a\x1f', 'b')` next to
/// `('a', '\x1fb')`, which v1 hashed identically. A green result means the
/// shipped canonical image distinguishes them AND that an independent
/// implementation of the spec agrees with rivet row for row.
///
/// The OTHER half of the v1 family — a value whose rendering IS the null marker
/// (a lone `\x00`) — cannot be expressed here: PostgreSQL refuses a NUL byte in
/// `text` outright ("null character not permitted"), which is a real engine
/// constraint rather than a fixture choice. It is graded offline instead, by
/// `hash_distinguishes_null_from_a_value_rendering_as_the_null_marker`
/// (src/enrich.rs), where the batch is built through Arrow directly.
#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn batch_row_hash_agrees_with_an_independent_reading_of_the_spec_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("bhash");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, a TEXT, b TEXT);
         INSERT INTO {table} VALUES
           (1, 'a' || chr(31), 'b'),
           (2, 'a',            chr(31) || 'b'),
           (3, NULL,           'x'),
           (4, '',             'x'),
           (5, 'plain',        'value');"
    ))
    .unwrap();

    let rig = Rig::pg_batch(&format!("public.{table}"))
        .census_oracle()
        .export_line("meta_columns: { row_hash: true }");
    rig.run_ok();

    // The values themselves must survive the trip, or the hash comparison is
    // over data that never carried the hostile bytes.
    let census = rig.row_census();
    assert_eq!(
        census.delivered, 5,
        "the five hostile rows must land: {census:?}"
    );

    let checked = row_hash_matches_independent_spec(&rig.out_dir(), &["id", "a", "b"]);
    assert!(
        checked,
        "the independent hasher did not run (no `xxhash` on the host) — this \
         test grades nothing without it, and falling back on rivet's own \
         recomputation would be the self-oracle the helper refuses"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}

/// METABASE COHERENCE: rivet's own records must describe the run that actually
/// happened — checked against the artifacts, not against each other.
///
/// The metabase has 17 tables. A survey on 2026-08-29 found `export_metrics`
/// and `file_log` read by ~20 test files each, and THREE tables read by nobody:
/// `export_shape`, `run_aggregate`, `strategy_snapshot`. `export_shape` is the
/// sharp one — rivet writes a row per column on EVERY ordinary export (present
/// in every state DB on the stand), the value-growth warning is built on it,
/// and no test had ever compared it to the data it describes.
///
/// Three claims, each against an artifact:
///   1. `run_status` is TERMINAL for a finished run — a stale `running` freezes
///      the prefix for the gc's active-run signal.
///   2. `export_metrics` records exactly ONE row for the run.
///   3. `export_shape.max_byte_len` per column EQUALS the widest value that
///      column carries in the delivered parquet, measured by DuckDB.
#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn batch_metabase_records_cohere_with_the_artifacts_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("bmeta");
    let mut c = pg_connect();
    // Widths chosen so each column has a DIFFERENT max: a fixture where every
    // column is the same width cannot tell a per-column read from a constant.
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, short TEXT, wide TEXT);
         INSERT INTO {table} VALUES
           (1, 'ab',   'abcdefghij'),
           (2, 'a',    'abc'),
           (3, NULL,   'abcdefg');"
    ))
    .unwrap();

    let rig = Rig::pg_batch(&format!("public.{table}")).census_oracle();
    rig.run_ok();

    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());

    // 1. terminal status
    assert_eq!(
        db.run_status_of(&run_id).as_deref(),
        Some("success"),
        "a finished run must record a TERMINAL status — a stale `running` row \
         freezes this prefix for the gc's active-run signal"
    );

    // 2. exactly one metrics row
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.total_rows,
        Some(3),
        "the run's own metrics row must describe this run: {m:?}"
    );

    // 3. export_shape vs the delivered parquet, per column, by DuckDB
    let shape = db.shape_rows(rig.export_name());
    assert!(
        !shape.is_empty(),
        "rivet writes export_shape on every export — an empty read means the \
         table moved and this check would pass vacuously"
    );
    for (col, recorded) in &shape {
        let actual = duckdb_dir_scalar(
            &rig.out_dir(),
            // `strlen` is DuckDB's BYTE length (verified: 'привет' -> 12),
            // which is what `max_byte_len` names; `length` counts characters.
            &format!("coalesce(max(strlen(CAST({col} AS VARCHAR))), 0)"),
            None,
        );
        assert_eq!(
            *recorded, actual,
            "export_shape says column `{col}` peaked at {recorded} bytes; the \
             delivered parquet says {actual}. Nothing compared these before — \
             the value-growth warning is built on the recorded number"
        );
    }
    // …and the per-column maxima must actually DIFFER, or the assertion above
    // would pass against a constant.
    let widths: Vec<i64> = shape.iter().map(|(_, w)| *w).collect();
    assert!(
        widths
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > 1,
        "fixture must give the columns different widths, or a per-column read \
         and a constant are indistinguishable: {shape:?}"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}
