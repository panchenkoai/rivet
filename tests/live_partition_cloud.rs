//! Live: value-based partitioning of `content_items` to object storage
//! (MinIO / fake-gcs), verified end-to-end **with `--reconcile`**.
//!
//! `--reconcile` implies `--validate` (ADR-0013), so a green run proves, per
//! partition: source `COUNT(*)` of that day's rows == exported rows, AND the
//! manifest + `_SUCCESS` at the cloud prefix verify. We then list the bucket to
//! confirm the Hive `created_at=YYYY-MM-DD/` layout actually landed.
//!
//! `content_items` is the pre-seeded bench fixture (`cargo run --bin seed`);
//! this test does not seed or drop it. Each test is `#[ignore]`.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;

use common::*;

/// 3-day window of `content_items.created_at` → exactly 3 day partitions.
const WINDOW: &str = "created_at >= '2023-01-01' AND created_at < '2023-01-04'";
const PART_DAYS: [&str; 3] = [
    "created_at=2023-01-01",
    "created_at=2023-01-02",
    "created_at=2023-01-03",
];

/// Skip-guard: `content_items` must be present and cover the window. Panics with
/// an actionable message (the table is a `seed.rs` fixture, not seeded here).
fn require_content_items_window() {
    let mut c = pg_connect();
    let count: i64 = c
        .query_one(
            &format!("SELECT COUNT(*) FROM content_items WHERE {WINDOW}"),
            &[],
        )
        .expect("content_items must exist — run `cargo run --bin seed`")
        .get(0);
    assert!(
        count > 0,
        "content_items has no rows in the test window ({WINDOW}); run `cargo run --bin seed`"
    );
}

/// Run a partitioned `content_items` export with `--reconcile` through
/// `destination_yaml`. Returns `(success, stderr)`.
fn run_partitioned_reconcile(destination_yaml: &str, env: &[(&str, &str)]) -> (bool, String) {
    let export_name = unique_name("ci_part_cloud");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, title, created_at FROM content_items WHERE {WINDOW}"
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
    require_content_items_window();

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

    let (ok, stderr) = run_partitioned_reconcile(&dest, &env);
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
    for day in PART_DAYS {
        assert!(
            listing.contains(day),
            "S3 bucket missing partition `{day}`;\nmc ls:\n{listing}"
        );
    }
    assert!(
        listing.matches(".parquet").count() >= 3,
        "S3 must hold >= 3 parquet (one per day partition);\nmc ls:\n{listing}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + fake-gcs (content_items pre-seeded)"]
fn partition_content_items_to_gcs_with_reconcile() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::FakeGcs);
    require_content_items_window();

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

    let (ok, stderr) = run_partitioned_reconcile(&dest, &[]);
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
    for day in PART_DAYS {
        let encoded = day.replace('=', "%3D");
        assert!(
            resp.contains(&encoded),
            "GCS bucket missing partition `{day}` (looked for `{encoded}`);\nlist response:\n{resp}"
        );
    }
    assert!(
        resp.matches(".parquet").count() >= 3,
        "GCS must hold >= 3 parquet (one per day partition);\nlist response:\n{resp}"
    );
}
