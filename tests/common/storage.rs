//! Idempotent bucket provisioning for the object-storage destinations
//! (MinIO for S3, fake-gcs for GCS).

#![allow(dead_code)]

use std::net::TcpStream;
use std::process::Command;

use super::env::{LiveService, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, require_alive};

/// Idempotently create `bucket` in the local MinIO instance via `mc` inside
/// the running container.  Does nothing if the bucket already exists.
///
/// Implementation: `docker compose exec -T minio sh -c "mc alias set ... && mc mb -p local/<bucket>"`.
/// Uses `-T` so cargo does not fight with the container for a TTY.  Panics
/// with an actionable message if `docker` is not on PATH — live tests need
/// it anyway.
pub fn ensure_minio_bucket(bucket: &str) {
    require_alive(LiveService::Minio);
    let script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc mb -p local/{bucket} >/dev/null 2>&1 || true"
    );
    let status = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &script])
        .status()
        .expect(
            "failed to spawn `docker compose exec minio` — \
             live tests for S3/MinIO require docker CLI on PATH",
        );
    assert!(
        status.success(),
        "`mc mb local/{bucket}` inside minio container failed with {status}"
    );
}

/// Idempotently create `bucket` in the fake-gcs server via its HTTP API.
/// The server exposes a create-bucket endpoint that does not require auth.
pub fn ensure_gcs_bucket(bucket: &str) {
    require_alive(LiveService::FakeGcs);
    use std::io::{Read, Write};
    let mut s = TcpStream::connect("127.0.0.1:4443").expect("connect fake-gcs");
    let body = format!(r#"{{"name":"{bucket}"}}"#);
    let req = format!(
        "POST /storage/v1/b?project=test HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).expect("write gcs req");
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    // 200 / 201 (fresh create) or 409 (already exists) — both acceptable.
    let status_ok = resp.starts_with("HTTP/1.0 200")
        || resp.starts_with("HTTP/1.0 201")
        || resp.starts_with("HTTP/1.1 200")
        || resp.starts_with("HTTP/1.1 201")
        || resp.contains(" 409 ");
    assert!(
        status_ok,
        "fake-gcs bucket create returned unexpected response:\n{resp}"
    );
}
