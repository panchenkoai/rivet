//! Idempotent bucket provisioning for the object-storage destinations
//! (MinIO for S3, fake-gcs for GCS).

#![allow(dead_code)]

use std::net::TcpStream;
use std::process::Command;

use super::env::{
    AZURITE_CONN_STRING, LiveService, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, require_alive,
};

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

/// Idempotently create `container` in the local Azurite emulator via the `az`
/// CLI + the well-known dev connection string, with CONTAINER-level public
/// read access. opendal's Azblob backend does not create the container, so
/// tests must provision it first; the public-access level lets the test re-read
/// the blobs over plain anonymous HTTP (rivet still WRITES with the account
/// key — public access only affects anonymous reads). Requires the `az` CLI on
/// PATH (Azure Storage emulator tests are dev-machine only).
///
/// `--public-access` is used (rather than `az storage blob` read-back) because
/// some `az` builds ship a Python without the `expat` XML module and choke on
/// the XML that blob list/download return; an anonymous reqwest GET sidesteps
/// the CLI entirely for the read path.
pub fn ensure_azure_container(container: &str) {
    require_alive(LiveService::Azurite);
    let out = Command::new("az")
        .args([
            "storage",
            "container",
            "create",
            "--name",
            container,
            "--public-access",
            "container",
            "--connection-string",
            AZURITE_CONN_STRING,
        ])
        .output()
        .expect(
            "failed to spawn `az` — Azure/Azurite live tests require the Azure CLI on PATH \
             (brew install azure-cli)",
        );
    // `az container create` is idempotent: it returns {\"created\": true|false}
    // and exits 0 whether the container was freshly made or already existed.
    assert!(
        out.status.success(),
        "`az storage container create --name {container}` against Azurite failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
