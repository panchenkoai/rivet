//! Destination parity tests — local vs S3 (MinIO) vs GCS (fake-gcs).
//!
//! QA backlog Task 6.3.  Export the same Postgres dataset through each
//! supported destination backend and assert the output is consistent.
//! Every destination carries its own auth / commit-protocol oddities; here
//! we pin the *row-count* contract so a regression in any backend surfaces
//! immediately.
//!
//! File-level assertions (naming, manifest) are intentionally left to
//! per-backend tests — the parity check is the minimum bar.

use crate::common::*;

fn parquet_rows(path: &std::path::Path) -> usize {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let bytes = std::fs::read(path).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

/// Export `query` through the destination the `dest` closure attaches to the
/// base rig, with environment variables `env`. Returns `(status_success,
/// stdout, stderr)` so the caller can surface cloud-specific errors cleanly.
fn export(
    query: &str,
    dest: impl FnOnce(Rig) -> Rig,
    env: &[(&str, &str)],
) -> (bool, String, String) {
    let export_name = unique_name("qa63");
    let rig = dest(Rig::pg_batch(&export_name).query(query));
    let out = rig.run_args_env(&["--export", &export_name], env);
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn local_destination_produces_one_parquet_with_all_rows() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(25);
    let out_dir = tempfile::tempdir().unwrap();

    let (ok, _stdout, stderr) = export(
        &format!("SELECT id, name FROM {}", table.name()),
        |r| r.dest_path(out_dir.path().to_path_buf()),
        &[],
    );
    assert!(ok, "rivet local failed; stderr:\n{stderr}");

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1, "local must produce exactly one file");
    assert_eq!(parquet_rows(&files[0]), 25);
    // …and by an INDEPENDENT codec: `parquet_rows` decodes with rivet's own
    // arrow/parquet crate, which cancels an encode fault. The batch oracle gate
    // asks for this on a name that claims "all_rows".
    assert_eq!(
        duckdb_total_parquet_rows(out_dir.path()),
        25,
        "local: DuckDB must count the same 25 rows"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn s3_minio_destination_produces_one_parquet_with_all_rows() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);

    let bucket = "rivet-qa-parity";
    ensure_minio_bucket(bucket);
    let prefix = unique_name("qa63s3");
    let table = seed_pg_numeric_table(25);

    let env = [
        ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
        ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
        // opendal S3 backend requires these:
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];
    let (ok, _stdout, stderr) = export(
        &format!("SELECT id, name FROM {}", table.name()),
        |r| r.dest_s3(bucket, &prefix, MINIO_ENDPOINT),
        &env,
    );
    assert!(ok, "rivet s3 (MinIO) failed; stderr:\n{stderr}");

    // Enumerate what MinIO received via `mc ls` inside the container.  This
    // keeps assertions independent of opendal internals and avoids staging
    // a mirror directory via `mc cp` (which is fragile across tmp paths).
    let ls = minio_mc(&format!(
        "mc ls --recursive local/{bucket}/{prefix} 2>/dev/null"
    ))
    .output()
    .expect("mc ls");
    assert!(
        ls.status.success(),
        "mc ls inside minio failed: stderr:\n{}",
        String::from_utf8_lossy(&ls.stderr)
    );
    let listing = String::from_utf8_lossy(&ls.stdout);
    let parquet_count = listing.matches(".parquet").count();
    assert_eq!(
        parquet_count, 1,
        "s3 (MinIO) must produce exactly one parquet under the prefix;\nmc ls output:\n{listing}"
    );
    // "all rows" needs a CONTENT oracle, not file presence: download the
    // object(s) and count the physical rows (matrix audit: wrong-artifact).
    assert_eq!(
        minio_parquet_total_rows(bucket, &prefix),
        25,
        "the downloaded s3 parquet must hold every seeded row"
    );

    // The BUCKET's contents, by an INDEPENDENT codec. Everything above counts
    // OBJECTS through `mc ls` — presence, not rows — so a run that wrote one
    // object holding nothing passed a test whose name claims "all_rows".
    let store = duckdb_store_census(ObjectStore::Minio, bucket, &prefix, &[]);
    assert_eq!(
        store.rows, 25,
        "s3, read by DuckDB: the bucket must hold all 25 rows: {store:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + fake-gcs"]
fn gcs_fake_destination_produces_one_parquet_with_all_rows() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::FakeGcs);

    let bucket = "rivet-qa-parity-gcs";
    ensure_gcs_bucket(bucket);
    let prefix = unique_name("qa63gcs");
    let table = seed_pg_numeric_table(25);

    let (ok, _stdout, stderr) = export(
        &format!("SELECT id, name FROM {}", table.name()),
        |r| r.dest_gcs(bucket, &prefix, FAKE_GCS_ENDPOINT),
        &[],
    );
    assert!(ok, "rivet gcs (fake-gcs) failed; stderr:\n{stderr}");

    // Enumerate objects via fake-gcs HTTP API (GET /storage/v1/b/<bucket>/o?prefix=<prefix>).
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let mut s = TcpStream::connect("127.0.0.1:4443").unwrap();
    let req = format!(
        "GET /storage/v1/b/{bucket}/o?prefix={prefix} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    assert!(
        resp.contains("\"kind\": \"storage#objects\"") || resp.contains("\"items\""),
        "unexpected fake-gcs list response:\n{resp}"
    );
    // Count parquet entries — each object has a `"name":"<key>"` field.
    let parquet_count = resp.matches(".parquet").count();
    assert!(
        parquet_count >= 1,
        "fake-gcs bucket must contain at least one .parquet; response:\n{resp}"
    );
    // "all rows" needs a CONTENT oracle, not file presence: download the
    // object(s) and count the physical rows (matrix audit: wrong-artifact).
    assert_eq!(
        fake_gcs_parquet_total_rows(bucket, &prefix),
        25,
        "the downloaded gcs parquet must hold every seeded row"
    );

    // The BUCKET's contents, by an INDEPENDENT codec. The listing above proves
    // an object EXISTS; it says nothing about rows. DuckDB cannot list
    // fake-gcs (its JSON API 404s the HEAD httpfs issues), so the object names
    // come from the store's own API and DuckDB reads them by URL.
    let objects: Vec<String> = fake_gcs_object_names(bucket, &prefix)
        .into_iter()
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    assert!(
        !objects.is_empty(),
        "fixture: the bucket must hold a parquet"
    );
    let store = duckdb_store_census(ObjectStore::FakeGcs, bucket, &prefix, &objects);
    assert_eq!(
        store.rows, 25,
        "gcs, read by DuckDB: the bucket must hold all 25 rows: {store:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + minio + fake-gcs"]
fn destination_parity_row_counts_match_across_local_s3_gcs() {
    // The crown-jewel parity check: same seed, three exports, one assertion.
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);
    require_alive(LiveService::FakeGcs);

    const ROWS: i64 = 30;
    let table = seed_pg_numeric_table(ROWS);
    let base_query = format!("SELECT id, name FROM {}", table.name());

    // 1. Local
    let local_dir = tempfile::tempdir().unwrap();
    {
        let (ok, _, err) = export(
            &base_query,
            |r| r.dest_path(local_dir.path().to_path_buf()),
            &[],
        );
        assert!(ok, "local failed: {err}");
    }
    let local_rows: usize = files_with_extension(local_dir.path(), "parquet")
        .iter()
        .map(|p| parquet_rows(p))
        .sum();

    // 2. S3 (MinIO)
    let s3_bucket = "rivet-qa-parity";
    ensure_minio_bucket(s3_bucket);
    let s3_prefix = unique_name("qa63_parity_s3");
    {
        let env = [
            ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
            ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
            ("AWS_EC2_METADATA_DISABLED", "true"),
        ];
        let (ok, _, err) = export(
            &base_query,
            |r| r.dest_s3(s3_bucket, &s3_prefix, MINIO_ENDPOINT),
            &env,
        );
        assert!(ok, "s3 failed: {err}");
    }
    // Count rows by sampling the manifest via rivet state files instead of
    // re-downloading.  Simpler: trust that rivet wrote what the RunSummary
    // reported via `--validate` — invoke exports with validate flag; since
    // we already ran once without it, do a cheap check: mc ls.
    let s3_count_out = minio_mc(&format!(
        "mc ls local/{s3_bucket}/{s3_prefix} 2>/dev/null | wc -l"
    ))
    .output()
    .expect("mc ls");
    let s3_file_count: usize = String::from_utf8_lossy(&s3_count_out.stdout)
        .trim()
        .parse()
        .unwrap_or(0);

    // 3. GCS (fake-gcs)
    let gcs_bucket = "rivet-qa-parity-gcs";
    ensure_gcs_bucket(gcs_bucket);
    let gcs_prefix = unique_name("qa63_parity_gcs");
    {
        let (ok, _, err) = export(
            &base_query,
            |r| r.dest_gcs(gcs_bucket, &gcs_prefix, FAKE_GCS_ENDPOINT),
            &[],
        );
        assert!(ok, "gcs failed: {err}");
    }

    // TRUE parity: the same physical row count downloaded back from every
    // backend. The prior version counted FILES for s3 ("mc ls | wc -l") and
    // settled for "no error" on gcs — trusting rivet's own summary is exactly
    // the self-oracle the audit flagged; a backend silently writing a short
    // parquet passed. Every leg now downloads and counts rows.
    assert_eq!(
        local_rows, ROWS as usize,
        "local backend row count mismatch"
    );
    assert!(
        s3_file_count >= 1,
        "s3 backend must have at least one object under {s3_prefix}"
    );
    assert_eq!(
        minio_parquet_total_rows(s3_bucket, &s3_prefix),
        ROWS as usize,
        "s3 (MinIO) downloaded row count must equal the seed"
    );
    assert_eq!(
        fake_gcs_parquet_total_rows(gcs_bucket, &gcs_prefix),
        ROWS as usize,
        "gcs (fake-gcs) downloaded row count must equal the seed"
    );
}

/// A relative `destination.path` is anchored to the config's directory: one config delivers to one place whatever directory rivet is started from (P-27).
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_relative_destination_path_follows_the_config() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("dest_relative");
    e.insert(&table, 1..=10, 180, Some(10));
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .unwritable_dest_path(std::path::PathBuf::from("./out"));
    let ran = |dir: &std::path::Path| {
        let run = rig.run_in_dir(dir);
        assert!(
            run.status.success(),
            "{}",
            String::from_utf8_lossy(&run.stderr)
        );
    };
    ran(a.path());
    e.insert(&table, 11..=13, 170, Some(10));
    ran(b.path());

    let beside_config = rig.config_path();
    let held = |dir: &std::path::Path| {
        let out = dir.join("out");
        if out.is_dir() {
            read_ids(&out)
        } else {
            Vec::new()
        }
    };
    let (first, second) = (held(a.path()), held(b.path()));
    let beside = held(beside_config.parent().unwrap());
    let whole = (1..=13).collect::<Vec<_>>();
    assert_eq!(
        beside, whole,
        "P-27: one config with a relative destination.path delivered ids {first:?} under the first working directory and {second:?} under the second: the directory beside the config holds {beside:?}, not the 13 source rows"
    );
    assert!(
        first.is_empty() && second.is_empty(),
        "nothing lands under a working directory: {first:?} / {second:?}"
    );
}

/// Upgrade: the previous release, started from directory A with `path: ./out`, wrote under A; this build started from A refuses by code, twice and through `validate` alike, with nothing written in either place; the printed remedy, moving the files beside the config, lets the next run continue the cursor there, every source row once (P-27).
#[test]
#[ignore = "live: requires docker compose postgres and RIVET_PREV_RELEASE_BIN"]
fn a_relative_destination_path_moved_by_the_upgrade_is_refused_until_the_files_follow() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("dest_rel_upgrade");
    e.insert(&table, 1..=10, 180, Some(10));
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .unwritable_dest_path(std::path::PathBuf::from("./out"));
    let beside = rig.config_path().parent().unwrap().join("out");
    // Directory A under the rig's own, so the remedy's move is one rename.
    let a = tempfile::tempdir_in(beside.parent().unwrap()).unwrap();
    let under_cwd = a.path().join("out");
    let mut rig = rig.in_dir(a.path());
    let Some(old) = rig.as_previous_release() else {
        return;
    };
    // A state database of the cell's own when the pass grades the shared Postgres state, which the previous release must not meet migrated past it.
    let state = state_url_under_test().map(|_| ScratchStateDb::new("dest_rel_upgrade"));
    let url = state.as_ref().map(ScratchStateDb::url);
    let envs: Vec<(&str, &str)> = url
        .iter()
        .map(|u| ("RIVET_STATE_URL", u.as_str()))
        .collect();
    let run = old.run_with_envs(&envs);
    assert!(
        run.status.success(),
        "the previous release: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let first = (1..=10).collect::<Vec<_>>();
    assert_eq!(
        read_ids(&under_cwd),
        first,
        "the previous release delivers under the working directory"
    );
    e.insert(&table, 11..=13, 170, Some(10));

    let refused = Refused::by_code("RIVET_DEST_RELATIVE_PATH_MOVED", 5);
    let text = rig.refuses_twice_and_walks_out(
        &["run"],
        &envs,
        refused,
        vec![
            Remedy::wrong(
                "runs `rivet validate`, which meets the same refusal",
                Then::Refuses(refused),
                |_| {},
            )
            .rerun_as(&["validate"])
            .in_place(),
            Remedy::new(
                &format!("move the files to {}", beside.display()),
                Then::DeliversTheSource,
                |_| {
                    assert!(!beside.exists(), "nothing written beside the config");
                    assert_eq!(
                        read_ids(&under_cwd),
                        first,
                        "nothing written under the working directory"
                    );
                    std::fs::rename(&under_cwd, &beside).unwrap();
                },
            )
            .in_place(),
        ],
    );
    assert!(
        text.contains(under_cwd.to_str().unwrap()),
        "the refusal names the working-directory place:\n{text}"
    );
    assert_eq!(
        read_ids(&beside),
        (1..=13).collect::<Vec<_>>(),
        "the destination holds every source row once"
    );
    assert!(
        !under_cwd.exists(),
        "nothing lands under the working directory again"
    );
}
