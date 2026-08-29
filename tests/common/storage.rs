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

/// Download every `.parquet` under `bucket/prefix` from MinIO (via `mc cat`
/// inside the container) and sum their row counts — the independent oracle an
/// "all rows" claim on an S3 destination needs. An `mc ls | wc -l` file count
/// proves presence, not content (matrix audit: wrong-artifact class).
pub fn minio_parquet_total_rows(bucket: &str, prefix: &str) -> usize {
    // List at BUCKET level: `mc ls` prints names relative to the listed path,
    // and that differs between a directory-style prefix (`prefix/file`) and
    // rivet's string-style concatenation (`prefixfile`). Bucket-level names are
    // always full keys; filter by prefix ourselves.
    let ls_script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc ls --recursive local/{bucket} 2>/dev/null"
    );
    let ls = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &ls_script])
        .output()
        .expect("mc ls");
    assert!(ls.status.success(), "mc ls failed");
    let mut total = 0usize;
    for line in String::from_utf8_lossy(&ls.stdout).lines() {
        // `mc ls` line: `[date] [time TZ] [size] [class] name` — name is last,
        // and for a STRING prefix (rivet concatenates `{prefix}{file}`, no `/`)
        // it is the full BUCKET-relative key, so cat under the bucket, not the
        // prefix (double-prefixing 404s).
        let Some(name) = line.split_whitespace().last() else {
            continue;
        };
        if !name.starts_with(prefix) || !name.ends_with(".parquet") {
            continue;
        }
        let cat_script = format!(
            "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
             mc cat local/{bucket}/{name}"
        );
        let cat = Command::new("docker")
            .args(["compose", "exec", "-T", "minio", "sh", "-c", &cat_script])
            .output()
            .expect("mc cat");
        assert!(cat.status.success(), "mc cat {name} failed");
        total += super::parquet_rows_from_bytes(cat.stdout);
    }
    total
}

/// Same independent oracle for fake-gcs: list the objects via the JSON API,
/// then `GET ?alt=media` each `.parquet` and sum the row counts.
pub fn fake_gcs_parquet_total_rows(bucket: &str, prefix: &str) -> usize {
    use std::io::{Read, Write};
    let http = |req: String| -> Vec<u8> {
        let mut s = TcpStream::connect("127.0.0.1:4443").expect("connect fake-gcs");
        s.write_all(req.as_bytes()).expect("write fake-gcs req");
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf);
        // Strip the HTTP/1.0 header block: body starts after the first CRLFCRLF.
        let sep = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("http header separator");
        buf.split_off(sep + 4)
    };
    let list = http(format!(
        "GET /storage/v1/b/{bucket}/o?prefix={prefix} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    ));
    let list: serde_json::Value = serde_json::from_slice(&list).expect("fake-gcs list JSON");
    let items = list["items"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    // An empty LISTING is a harness bug (wrong prefix/bucket), never evidence
    // of zero rows: the GCS all-features test probed a prefix missing its
    // export-name segment and this helper answered an honest-but-wrong 0
    // (2026-08-29). Objects exist whenever the capture ran — refuse to grade
    // a world the prefix cannot see.
    assert!(
        !items.is_empty(),
        "fake_gcs_parquet_total_rows: prefix `{prefix}` in bucket `{bucket}` \
         matched no objects at all — a wrong prefix reads as an empty export; \
         fix the probe, don't trust the zero"
    );
    let mut total = 0usize;
    for item in items {
        let name = item["name"].as_str().expect("object name");
        if !name.ends_with(".parquet") {
            continue;
        }
        // Object names carry `/`; the JSON API path wants them %2F-escaped.
        let escaped = name.replace('/', "%2F");
        let bytes = http(format!(
            "GET /storage/v1/b/{bucket}/o/{escaped}?alt=media HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        ));
        total += super::parquet_rows_from_bytes(bytes);
    }
    total
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

/// Pull every object under `prefix` into a LOCAL directory, preserving base
/// names, and return how many were written.
///
/// This exists so a cloud destination can be graded by the SAME oracle as a
/// local one. The alternative — a store-specific "what was delivered" reader —
/// is a second definition of delivered, and it drifts on the first fix: the
/// local read-back was corrected to count only manifest-DECLARED parts (a crash
/// leaves orphans no manifest names) while the cloud reader kept summing every
/// object under the prefix, so resume cells on s3/gcs read 2000 rows from a
/// 1000-row table. Pull the prefix, then run `dir_manifest_copy_id_set` /
/// `dir_manifest_copy_total_rows` over it exactly as the local tests do.
///
/// Base names are preserved because that is what a manifest's `parts[].path`
/// resolves to; a collision would silently drop an object, so it PANICS instead.
pub fn minio_pull_prefix(bucket: &str, prefix: &str, into: &std::path::Path) -> usize {
    std::fs::create_dir_all(into).expect("create pull dir");
    let ls_script = format!(
        "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
         mc ls --recursive local/{bucket} 2>/dev/null"
    );
    let ls = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c", &ls_script])
        .output()
        .expect("mc ls");
    assert!(ls.status.success(), "mc ls failed");
    let mut pulled = 0usize;
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for line in String::from_utf8_lossy(&ls.stdout).lines() {
        let Some(name) = line.split_whitespace().last() else {
            continue;
        };
        if !name.starts_with(prefix) {
            continue;
        }
        let base = name.rsplit('/').next().unwrap_or(name).to_string();
        assert!(
            seen.insert(base.clone()),
            "two objects under {prefix} share the base name {base} — flattening would \
             silently drop one, and the manifest oracle resolves parts by base name"
        );
        let cat_script = format!(
            "mc alias set local http://127.0.0.1:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && \
             mc cat local/{bucket}/{name}"
        );
        let cat = Command::new("docker")
            .args(["compose", "exec", "-T", "minio", "sh", "-c", &cat_script])
            .output()
            .expect("mc cat");
        assert!(cat.status.success(), "mc cat {name} failed");
        std::fs::write(into.join(&base), cat.stdout).expect("write pulled object");
        pulled += 1;
    }
    pulled
}

/// Blob names under `prefix` in an azurite container, via anonymous HTTP
/// `List Blobs` (the container is provisioned public-read by
/// [`ensure_azure_container`]).
///
/// Shared rather than private to one test: `live_azure_multipart.rs` grew this
/// XML walk locally, and a second azure test copying it would be the two-readers
/// problem the cloud read-back already paid for once — a store-specific "what
/// was delivered" drifts on the first fix.
pub fn azure_blob_names(container: &str, prefix: &str) -> Vec<String> {
    let url = format!(
        "{}/{container}?restype=container&comp=list&prefix={prefix}",
        super::env::AZURITE_ENDPOINT
    );
    let xml = reqwest::blocking::Client::new()
        .get(&url)
        .send()
        .expect("azure list request")
        .text()
        .expect("azure list body");
    azure_blob_names_from_list_xml(&xml)
}

/// Extract blob names from an Azure "List Blobs" XML body: each blob is
/// `<Blob><Name>…</Name>…</Blob>`. Split out so a caller that already holds the
/// XML (a test asserting on the listing itself) parses it the same way.
pub fn azure_blob_names_from_list_xml(xml: &str) -> Vec<String> {
    xml.split("<Name>")
        .skip(1)
        .filter_map(|seg| seg.split("</Name>").next())
        .map(String::from)
        .collect()
}

/// The independent read-back oracle for azurite: download every `.parquet` blob
/// under `prefix` and sum its row count — CONTENT, not object presence, which is
/// the distinction the destination matrix's round-trip row asks for.
pub fn azure_parquet_total_rows(container: &str, prefix: &str) -> usize {
    let http = reqwest::blocking::Client::new();
    azure_blob_names(container, prefix)
        .iter()
        .filter(|k| k.ends_with(".parquet"))
        .map(|key| {
            let bytes = http
                .get(format!(
                    "{}/{container}/{key}",
                    super::env::AZURITE_ENDPOINT
                ))
                .send()
                .expect("azure blob download")
                .bytes()
                .expect("azure blob body")
                .to_vec();
            super::parquet_rows_from_bytes(bytes)
        })
        .sum()
}
