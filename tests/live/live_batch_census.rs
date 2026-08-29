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
