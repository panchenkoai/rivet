//! Live: value-based partitioning to object storage (MinIO / fake-gcs),
//! verified end-to-end **with `--reconcile`**.
//!
//! `--reconcile` implies `--validate` (ADR-0013), so a green run proves, per
//! partition: source `COUNT(*)` of that day's rows == exported rows, AND the
//! manifest + `_SUCCESS` at the cloud prefix verify. We then list the bucket to
//! confirm the Hive `created_at=YYYY-MM-DD/` layout actually landed.
//!
//! Self-contained: each test seeds its own small fixture table (a
//! content-items-shaped `id / title / created_at`) and drops it on teardown —
//! it does NOT depend on the heavy pre-seeded `content_items`, which the live
//! CI job deliberately does not seed (`--skip content_export`). Each test is
//! `#[ignore]`.

use std::io::{Read, Write};
use std::net::TcpStream;

use crate::common::*;
use postgres::NoTls;

/// Deterministic 3-day fixture → exactly three day partitions.
const PART_DAYS: [&str; 3] = [
    "created_at=2023-01-01",
    "created_at=2023-01-02",
    "created_at=2023-01-03",
];

struct PgCleanup(String);

impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Seed a small `id / title / created_at` table spanning the three PART_DAYS
/// (2 + 2 + 1 rows). Returns the table name and a guard that drops it.
fn seed_partition_source() -> (String, PgCleanup) {
    let table = unique_name("ci_part_src");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, title TEXT, created_at TIMESTAMP);
         INSERT INTO {table} (id, title, created_at) VALUES
           (1, 'a', '2023-01-01 09:00:00'),
           (2, 'b', '2023-01-01 18:00:00'),
           (3, 'c', '2023-01-02 09:00:00'),
           (4, 'd', '2023-01-02 18:00:00'),
           (5, 'e', '2023-01-03 09:00:00');",
    ))
    .expect("seed partition source table");
    (table.clone(), PgCleanup(table))
}

/// Run a partitioned export of `table` (by `created_at`, day) with `--reconcile`
/// through `destination_yaml`. Returns `(success, stderr)`.
fn run_partitioned_reconcile(
    table: &str,
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
    query: "SELECT id, title, created_at FROM {table}"
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
#[ignore = "live: requires docker compose postgres + minio"]
fn partition_to_s3_minio_with_reconcile() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);
    let (table, _guard) = seed_partition_source();

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

    let (ok, stderr) = run_partitioned_reconcile(&table, &dest, &env);
    assert!(
        ok,
        "partitioned export → S3 with --reconcile failed (reconcile/validate mismatch?):\n{stderr}"
    );

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
        listing.matches(".parquet").count() >= PART_DAYS.len(),
        "S3 must hold >= {} parquet (one per day partition);\nmc ls:\n{listing}",
        PART_DAYS.len()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + fake-gcs"]
fn partition_to_gcs_with_reconcile() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::FakeGcs);
    let (table, _guard) = seed_partition_source();

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

    let (ok, stderr) = run_partitioned_reconcile(&table, &dest, &[]);
    assert!(
        ok,
        "partitioned export → GCS with --reconcile failed (reconcile/validate mismatch?):\n{stderr}"
    );

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
        resp.matches(".parquet").count() >= PART_DAYS.len(),
        "GCS must hold >= {} parquet (one per day partition);\nlist response:\n{resp}",
        PART_DAYS.len()
    );
}
