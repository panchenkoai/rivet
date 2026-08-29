//! Wave 3 cloud-multipart: a `max_file_size` rotation must reach the CLOUD
//! destination as MULTIPLE objects under NON-COLLIDING keys.
//!
//! `commit::write_sink_parts` names rotated parts via `part_indexed_name`
//! (commit.rs:187): a rotated chunk's parts get a `_p{idx}` suffix
//! (`…_chunk0_p0.parquet`, `…_chunk0_p1.parquet`, …). The cloud write path
//! (`CloudDestination::write`, src/destination/cloud.rs) is shared by S3 / GCS /
//! Azure and keys the object solely by that name. So a `part_indexed_name`
//! regression — dropping the `_p` suffix, or passing `count = 1` so no suffix is
//! added — would make every rotated part of a chunk PUT to the SAME key,
//! silently overwriting `p0` with `p1` at the wire. Row counters stay
//! self-consistent; the destination just loses a part.
//!
//! These tests INDEPENDENTLY re-read the cloud objects (list the keys, download
//! the bytes) and assert: (1) rotation produced distinct `_p0` AND `_p1` keys
//! for a chunk (proving non-colliding multipart upload — not one overwritten
//! key), and (2) the total rows across all downloaded objects == seeded (no part
//! was lost to an overwrite). The seed uses DISTINCT ~1 KB payloads so the
//! parquet row groups actually exceed the cap (identical values dictionary-encode
//! to ~nothing and would defeat rotation).
//!
//! `part_indexed_name` is backend-agnostic, so S3 (verified end-to-end via
//! `mc cat`) plus GCS (verified via the fake-gcs HTTP API) cover the shared
//! regression; Azure rides the same code path (see live_azure_multipart.rs).
//!
//! Run: `docker compose up -d postgres minio fake-gcs && \
//!       cargo test --test audit_cloud_multipart -- --ignored`

use crate::common::*;

const N: i64 = 1_000;

/// Seed `(id BIGINT PK, payload TEXT)` with N rows of DISTINCT ~1 KB payloads
/// (`md5(g)` repeated 32× ≈ 1 KB, unique per row) so parquet row groups grow
/// past the cap instead of dictionary-compressing to nothing.
fn seed_distinct_payload() -> (String, PgTable) {
    let name = unique_name("rivet_cloud_mp");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)"
    ))
    .expect("create cloud-mp table");
    c.batch_execute(&format!(
        "INSERT INTO {name} (id, payload) \
         SELECT g, repeat(md5(g::text), 32) FROM generate_series(1, {N}) g"
    ))
    .expect("seed cloud-mp rows");
    (name.clone(), PgTable::adopt(name))
}

/// The chunked-export config body that forces rotation: chunk_size 500 → 2
/// chunks, batch_size 100 so `maybe_split` runs every 100 rows (it only rotates
/// BETWEEN flushes — one batch per chunk would never split mid-chunk), aligned
/// with row_group_rows 100 so each ~100 KB closed group exceeds the 64 KB cap →
/// maybe_split rotates every group → `_p0`, `_p1`, … per chunk.
fn chunked_cloud_rig(table: &str, export: &str) -> Rig {
    Rig::pg_batch(table)
        .export_named(export)
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
}

/// Assert the listed keys prove a rotation reached the wire: at least two
/// `.parquet` objects, including a chunk split into `_p0` AND `_p1`.
fn assert_rotated_keys(keys: &[String], ctx: &str) {
    let parquet: Vec<&String> = keys.iter().filter(|k| k.ends_with(".parquet")).collect();
    assert!(
        parquet.len() >= 2,
        "{ctx}: rotation must produce >=2 parquet objects at the destination; got {parquet:?}"
    );
    assert!(
        parquet.iter().any(|k| k.contains("_p0.")) && parquet.iter().any(|k| k.contains("_p1.")),
        "{ctx}: a rotated chunk must land under distinct _p0 AND _p1 keys (part_indexed_name \
         non-collision); a missing _p1 means p1 overwrote p0 at one key. keys: {parquet:?}"
    );
}

// ── S3 / MinIO ───────────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn cloud_multipart_s3_rotation_distinct_keys_and_all_rows() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);

    let bucket = "rivet-qa-multipart";
    ensure_minio_bucket(bucket);
    let prefix = unique_name("s3mp");
    let (table, _g) = seed_distinct_payload();

    let rig = chunked_cloud_rig(&table, &prefix).dest_s3(bucket, &prefix, MINIO_ENDPOINT);
    let env = [
        ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
        ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];
    let run = rig.run_args_env(&["--export", &prefix], &env);
    assert!(
        run.status.success(),
        "s3 multipart export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    // List FULL keys from the bucket root (rivet uses `prefix` as a string
    // prefix, not a directory, so the object key is `<prefix><filename>` with no
    // separator). Filter to this run's unique prefix.
    let list_script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc ls --recursive local/{bucket}"
    );
    let ls = std::process::Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &list_script])
        .output()
        .expect("mc ls");
    assert!(
        ls.status.success(),
        "mc ls failed: {}",
        String::from_utf8_lossy(&ls.stderr)
    );
    let listing = String::from_utf8_lossy(&ls.stdout);
    // `mc ls --recursive` prints "<date> <time> <size> <key>"; the key is the
    // last whitespace-separated token (full key relative to the bucket root).
    let keys: Vec<String> = listing
        .lines()
        .filter_map(|l| l.split_whitespace().last())
        .map(|k| k.to_string())
        .filter(|k| k.contains(&prefix))
        .collect();
    assert_rotated_keys(&keys, "s3");

    // Download each object via `mc cat` (raw bytes to stdout) and sum rows.
    let mut total = 0usize;
    for key in keys.iter().filter(|k| k.ends_with(".parquet")) {
        let cat_script = format!(
            "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
             mc cat local/{bucket}/{key}"
        );
        let cat = std::process::Command::new("docker")
            .args(["compose", "exec", "-T", "minio", "sh", "-c", &cat_script])
            .output()
            .expect("mc cat");
        assert!(
            cat.status.success(),
            "mc cat {key} failed: {}",
            String::from_utf8_lossy(&cat.stderr)
        );
        total += parquet_rows_from_bytes(cat.stdout);
    }
    assert_eq!(
        total, N as usize,
        "s3: total rows across all downloaded objects must equal {N} (a lost/overwritten part \
         would under-count); got {total}"
    );

    // …and the same claim through an INDEPENDENT codec. Everything above
    // decodes with `parquet_rows_from_bytes` — rivet's own arrow/parquet crate,
    // which cancels an encode fault the way any self-read does. DuckDB reads
    // the bucket over s3:// from inside the stand network and shares none of
    // that code; the batch oracle gate asks for exactly this on a name that
    // claims "all_rows" (2026-08-29).
    let store = duckdb_store_census(ObjectStore::Minio, bucket, &prefix, &[]);
    assert_eq!(
        store.rows, N,
        "s3, read by DuckDB: the bucket must hold every row: {store:?}"
    );
}

// ── GCS / fake-gcs ─────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + fake-gcs"]
fn cloud_multipart_gcs_rotation_distinct_keys_and_all_rows() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::FakeGcs);

    let bucket = "rivet-qa-multipart-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("gcsmp");
    let (table, _g) = seed_distinct_payload();

    let rig = chunked_cloud_rig(&table, &prefix).dest_gcs(bucket, &prefix, FAKE_GCS_ENDPOINT);
    let run = rig.run_args(&["--export", &prefix]);
    assert!(
        run.status.success(),
        "gcs multipart export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let http = reqwest::blocking::Client::new();
    // List object names under the prefix (GCS JSON API).
    let list_url = format!("{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o?prefix={prefix}");
    let body = http
        .get(&list_url)
        .send()
        .expect("gcs list request")
        .text()
        .expect("gcs list body");
    let json: serde_json::Value = serde_json::from_str(&body).expect("gcs list json");
    let keys: Vec<String> = json["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|o| o["name"].as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    assert_rotated_keys(&keys, "gcs");

    // Download each object (media endpoint) and sum rows. Object names contain
    // '/', percent-encoded as %2F in the path segment.
    let mut total = 0usize;
    for key in keys.iter().filter(|k| k.ends_with(".parquet")) {
        let enc = key.replace('/', "%2F");
        let media_url = format!("{FAKE_GCS_ENDPOINT}/storage/v1/b/{bucket}/o/{enc}?alt=media");
        let bytes = http
            .get(&media_url)
            .send()
            .expect("gcs media request")
            .bytes()
            .expect("gcs media body")
            .to_vec();
        total += parquet_rows_from_bytes(bytes);
    }
    assert_eq!(
        total, N as usize,
        "gcs: total rows across all downloaded objects must equal {N} (a lost/overwritten part \
         would under-count); got {total}"
    );
}
