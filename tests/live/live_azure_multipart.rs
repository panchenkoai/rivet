//! Wave 3 Azure-multipart: a `max_file_size` rotation must reach Azure Blob
//! storage as MULTIPLE blobs under NON-COLLIDING keys.
//!
//! Azure rides the SAME `CloudDestination::write` + `part_indexed_name` path as
//! S3/GCS (see audit_cloud_multipart.rs), so a `_p{idx}`-suffix regression would
//! overwrite p0 with p1 at one blob key here too. Azure had ZERO live coverage
//! of any kind (no emulator in the stack) — this adds the emulator (Azurite) and
//! the same independent re-read: list the blobs (assert distinct `_p0`/`_p1`
//! keys), download every blob over anonymous HTTP, assert total rows == seeded.
//!
//! Infra: the `azurite` service in docker-compose.yaml + the `az` CLI on PATH
//! (used ONLY to provision the public-read container; the blob list/download
//! read-back goes over plain anonymous HTTP via reqwest — see the note in
//! `ensure_azure_container`). The well-known Azurite dev account is used; its
//! key is fixed, not a secret.
//!
//! Run: `docker compose up -d postgres azurite && \
//!       cargo test --test live_suite -- --ignored`

use crate::common::*;

const N: i64 = 1_000;

/// A fresh, valid Azure container name per run (lowercase alnum + hyphens, no
/// underscores). A unique name also means `az storage container create` always
/// hits the fresh-create path (which returns JSON) rather than the
/// already-exists path (which returns XML and trips a known `az`/expat bug on
/// some installs).
fn unique_container() -> String {
    unique_name("rivetazmp").replace('_', "-").to_lowercase()
}

/// Seed `(id BIGINT PK, payload TEXT)` with DISTINCT ~1 KB payloads so parquet
/// row groups grow past the cap (identical values dictionary-encode to nothing).
fn seed_distinct_payload() -> (String, PgTable) {
    let name = unique_name("rivet_azure_mp");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)"
    ))
    .expect("create azure-mp table");
    c.batch_execute(&format!(
        "INSERT INTO {name} (id, payload) \
         SELECT g, repeat(md5(g::text), 32) FROM generate_series(1, {N}) g"
    ))
    .expect("seed azure-mp rows");
    (name.clone(), PgTable::adopt(name))
}

#[test]
#[ignore = "live: requires docker compose postgres + azurite, and the az CLI on PATH"]
fn cloud_multipart_azure_rotation_distinct_keys_and_all_rows() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Azurite);

    let container = unique_container();
    ensure_azure_container(&container);
    let prefix = unique_name("azmp");
    let (table, _g) = seed_distinct_payload();

    let rig = Rig::pg_batch(&prefix)
        .query(&format!("SELECT id, payload FROM {table}"))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 500")
        .export_line("compression: none")
        .export_line("max_file_size: 64KB")
        .export_line("parquet:")
        .export_line("  row_group_strategy: fixed_rows")
        .export_line("  row_group_rows: 100")
        .export_line("tuning: {batch_size: 100}")
        .dest_azure(&container, &prefix);
    let run = rig.run_args_env(
        &["--export", &prefix],
        &[("RIVET_TEST_AZURITE_KEY", AZURITE_KEY)],
    );
    assert!(
        run.status.success(),
        "azure multipart export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    // Re-read the destination over plain anonymous HTTP (the container is
    // public-read; rivet wrote it authenticated). List Blobs returns XML.
    let http = reqwest::blocking::Client::new();
    let list_url =
        format!("{AZURITE_ENDPOINT}/{container}?restype=container&comp=list&prefix={prefix}");
    let xml = http
        .get(&list_url)
        .send()
        .expect("azure list request")
        .text()
        .expect("azure list body");
    let keys = azure_blob_names_from_list_xml(&xml);

    let parquet: Vec<&String> = keys.iter().filter(|k| k.ends_with(".parquet")).collect();
    assert!(
        parquet.len() >= 2,
        "azure: rotation must produce >=2 parquet blobs; got {parquet:?}\nlist xml:\n{xml}"
    );
    assert!(
        parquet.iter().any(|k| k.contains("_p0.")) && parquet.iter().any(|k| k.contains("_p1.")),
        "azure: a rotated chunk must land under distinct _p0 AND _p1 keys (part_indexed_name \
         non-collision); a missing _p1 means p1 overwrote p0 at one key. keys: {parquet:?}"
    );

    // Download every blob and sum rows.
    let mut total = 0usize;
    for key in &parquet {
        let blob_url = format!("{AZURITE_ENDPOINT}/{container}/{key}");
        let bytes = http
            .get(&blob_url)
            .send()
            .expect("azure blob download")
            .bytes()
            .expect("azure blob body")
            .to_vec();
        total += parquet_rows_from_bytes(bytes);
    }
    assert_eq!(
        total, N as usize,
        "azure: total rows across all downloaded blobs must equal {N} (a lost/overwritten part \
         under-counts); got {total}"
    );

    // The same claim through an INDEPENDENT codec: DuckDB reads the container
    // over azure:// and shares none of the arrow/parquet code the byte-level
    // downloads above decode with. Capability verified live before relying on
    // it (glob + read_parquet against azurite: 70 files, 4.2M rows).
    let store = duckdb_store_census(ObjectStore::Azurite, &container, &prefix, &[]);
    assert_eq!(
        store.rows, N,
        "azure, read by DuckDB: the container must hold every row: {store:?}"
    );
}
