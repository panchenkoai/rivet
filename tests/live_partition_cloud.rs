//! Live: value-based partitioning of `content_items` to object storage
//! (MinIO / fake-gcs), verified end-to-end **with `--reconcile`**.
//!
//! `--reconcile` implies `--validate` (ADR-0013), so a green run proves, per
//! partition: source `COUNT(*)` of that day's rows == exported rows, AND the
//! manifest + `_SUCCESS` at the cloud prefix verify. We then list the bucket to
//! confirm the Hive `created_at=YYYY-MM-DD/` layout actually landed.
//!
//! `content_items` is the pre-seeded bench fixture (`cargo run --bin seed`);
//! this test does not seed or drop it. The 3-day window and the expected
//! partition labels are derived from the table's actual earliest day, so the
//! test adapts to whatever date distribution the seed produced instead of
//! hard-coding calendar dates. Each test is `#[ignore]`.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;

use common::*;

/// Derive a small, data-backed test window from `content_items`: the earliest
/// day present, plus the next two. Returns the SQL `WHERE` predicate and the
/// `created_at=YYYY-MM-DD` partition labels that actually have rows in it.
///
/// Panics with an actionable message when `content_items` is absent or empty
/// (it is a `seed.rs` fixture, not seeded here).
fn content_items_window() -> (String, Vec<String>) {
    let mut c = pg_connect();
    let min_day: String = c
        .query_one(
            "SELECT min(created_at)::date::text FROM content_items WHERE created_at IS NOT NULL",
            &[],
        )
        .expect("content_items must exist — run `cargo run --bin seed`")
        .get::<_, Option<String>>(0)
        .expect("content_items is empty — run `cargo run --bin seed`");

    let start = chrono::NaiveDate::parse_from_str(&min_day, "%Y-%m-%d").expect("parse min day");
    let end = start
        .checked_add_days(chrono::Days::new(3))
        .expect("min day + 3");
    let where_clause = format!("created_at >= '{start}' AND created_at < '{end}'");

    let days: Vec<String> = c
        .query(
            &format!(
                "SELECT DISTINCT created_at::date::text AS d FROM content_items \
                 WHERE {where_clause} ORDER BY d"
            ),
            &[],
        )
        .expect("query distinct partition days")
        .iter()
        .map(|r| format!("created_at={}", r.get::<_, String>(0)))
        .collect();

    assert!(
        !days.is_empty(),
        "content_items has no rows near its earliest day ({min_day}); run `cargo run --bin seed`"
    );
    (where_clause, days)
}

/// Run a partitioned `content_items` export with `--reconcile` through
/// `destination_yaml`, filtered to `window`. Returns `(success, stderr)`.
fn run_partitioned_reconcile(
    window: &str,
    destination_yaml: &str,
    env: &[(&str, &str)],
) -> (bool, String) {
    let export_name = unique_name("ci_part_cloud");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, title, created_at FROM content_items WHERE {window}"
    partition_by: created_at
    partition_granularity: day
    format: parquet
    {destination_yaml}
"#
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let mut cmd = std::process::Command::new(RIVET_BIN);
    cmd.args([
        "run",
        "--config",
        cfg_path.to_str().unwrap(),
        "--export",
        &export_name,
        "--reconcile",
    ]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("spawn rivet");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
#[ignore = "live: requires docker compose postgres + minio (content_items pre-seeded)"]
fn partition_content_items_to_s3_minio_with_reconcile() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);
    let (window, days) = content_items_window();

    let bucket = "rivet-qa-parity";
    ensure_minio_bucket(bucket);
    let base = unique_name("ci_part_s3");

    let dest = format!(
        r#"destination:
      type: s3
      bucket: {bucket}
      prefix: {base}/{{partition}}/
      region: us-east-1
      endpoint: {MINIO_ENDPOINT}
      access_key_env: RIVET_TEST_MINIO_AK
      secret_key_env: RIVET_TEST_MINIO_SK"#
    );
    let env = [
        ("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY),
        ("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];

    let (ok, stderr) = run_partitioned_reconcile(&window, &dest, &env);
    assert!(
        ok,
        "partitioned content_items → S3 with --reconcile failed (reconcile/validate mismatch?):\n{stderr}"
    );

    // List the bucket and assert the Hive layout: one `created_at=DAY/` per day,
    // each with a parquet.
    let script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc ls --recursive local/{bucket}/{base} 2>/dev/null"
    );
    let ls = std::process::Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &script])
        .output()
        .expect("mc ls");
    let listing = String::from_utf8_lossy(&ls.stdout);
    for day in &days {
        assert!(
            listing.contains(day),
            "S3 bucket missing partition `{day}`;\nmc ls:\n{listing}"
        );
    }
    assert!(
        listing.matches(".parquet").count() >= days.len(),
        "S3 must hold >= {} parquet (one per day partition);\nmc ls:\n{listing}",
        days.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + fake-gcs (content_items pre-seeded)"]
fn partition_content_items_to_gcs_with_reconcile() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::FakeGcs);
    let (window, days) = content_items_window();

    let bucket = "rivet-qa-parity-gcs";
    ensure_gcs_bucket(bucket);
    let base = unique_name("ci_part_gcs");

    let dest = format!(
        r#"destination:
      type: gcs
      bucket: {bucket}
      prefix: {base}/{{partition}}/
      endpoint: {FAKE_GCS_ENDPOINT}
      allow_anonymous: true"#
    );

    let (ok, stderr) = run_partitioned_reconcile(&window, &dest, &[]);
    assert!(
        ok,
        "partitioned content_items → GCS with --reconcile failed (reconcile/validate mismatch?):\n{stderr}"
    );

    // Enumerate objects via the fake-gcs list API and assert the Hive layout.
    let mut s = TcpStream::connect("127.0.0.1:4443").unwrap();
    let req = format!(
        "GET /storage/v1/b/{bucket}/o?prefix={base} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    // fake-gcs URL-encodes the `=` in object names (`created_at%3D2023-01-01`).
    for day in &days {
        let encoded = day.replace('=', "%3D");
        assert!(
            resp.contains(&encoded),
            "GCS bucket missing partition `{day}` (looked for `{encoded}`);\nlist response:\n{resp}"
        );
    }
    assert!(
        resp.matches(".parquet").count() >= days.len(),
        "GCS must hold >= {} parquet (one per day partition);\nlist response:\n{resp}",
        days.len()
    );
}
